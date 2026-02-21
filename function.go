package function

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// ── Configuration ────────────────────────────────────────────────────
// Config is loaded from S3 using the function name as the key.
// e.g. FUNCTION_TARGET=pglog-line1 → reads s3://{bucket}/pglog-line1.json
//
// S3 config file format:
//
//	{
//	  "table": "uns_log",
//	  "topics": [
//	    "v1.0/acme/factory1/mixing/line1/temperature",
//	    "v1.0/acme/factory1/mixing/line1/pressure",
//	    "v1.0/acme/factory1/mixing/line1/speed"
//	  ]
//	}

// ── UNS Topic Parsing ───────────────────────────────────────────────
// Topics follow the UNS Framework (unsframework.com) ISA-95 hierarchy:
//   v1.0/{enterprise}/{site}/{area}/{line}/{tag...}
//
// All metadata is derived from the topic path — no manual config needed.

type pglogConfig struct {
	Table  string   `json:"table"`
	Topics []string `json:"topics"`
}

type unsFields struct {
	Enterprise string
	Site       string
	Area       string
	Line       string
	Tag        string
}

var (
	ctx       = context.Background()
	cache     *redis.Client
	db        *pgxpool.Pool
	s3Client  *s3.Client
	keyPrefix string

	// Config cache
	configMu      sync.RWMutex
	cachedConfig  *pglogConfig
	configFetched time.Time
	configTTL     = 30 * time.Second

	// Last snapshot for change detection
	lastSnapshot   map[string]string
	lastSnapshotMu sync.Mutex
)

func init() {
	// ── Cache connection ─────────────────────────────────────────────
	cacheURL := envOrDefault("CACHE_URL", "redis://fnkit-cache:6379")
	keyPrefix = envOrDefault("CACHE_KEY_PREFIX", "uns")

	opts, err := redis.ParseURL(cacheURL)
	if err != nil {
		log.Fatalf("[pglog] Failed to parse CACHE_URL: %v", err)
	}
	cache = redis.NewClient(opts)

	if err := cache.Ping(ctx).Err(); err != nil {
		log.Printf("[pglog] Warning: cache not reachable at %s: %v", cacheURL, err)
	} else {
		log.Printf("[pglog] Connected to cache at %s", cacheURL)
	}

	// ── Postgres connection ──────────────────────────────────────────
	dbURL := envOrDefault("DATABASE_URL", "postgres://fnkit:fnkit@fnkit-postgres:5432/fnkit?sslmode=disable")
	db, err = pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("[pglog] Failed to create Postgres pool: %v", err)
	}

	if err := db.Ping(ctx); err != nil {
		log.Printf("[pglog] Warning: Postgres not reachable: %v", err)
	} else {
		log.Printf("[pglog] Connected to Postgres")
	}

	// ── S3 client ────────────────────────────────────────────────────
	s3Endpoint := envOrDefault("S3_ENDPOINT", "")
	s3Region := envOrDefault("S3_REGION", "us-east-1")
	s3AccessKey := envOrDefault("S3_ACCESS_KEY", "")
	s3SecretKey := envOrDefault("S3_SECRET_KEY", "")

	s3Opts := []func(*s3.Options){
		func(o *s3.Options) {
			o.Region = s3Region
			o.UsePathStyle = true
		},
	}

	if s3Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(s3Endpoint)
		})
	}

	if s3AccessKey != "" && s3SecretKey != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.Credentials = credentials.NewStaticCredentialsProvider(s3AccessKey, s3SecretKey, "")
		})
	}

	s3Client = s3.New(s3.Options{}, s3Opts...)
	log.Printf("[pglog] S3 client configured (bucket: %s)", envOrDefault("S3_BUCKET", ""))

	// ── Initialize last snapshot ─────────────────────────────────────
	lastSnapshot = make(map[string]string)

	// ── Register HTTP function ───────────────────────────────────────
	// The function name matches FUNCTION_TARGET, which is also the S3 config key.
	functionName := envOrDefault("FUNCTION_TARGET", "pglog")
	functions.HTTP(functionName, pglogHandler)
	log.Printf("[pglog] Registered HTTP function: %s", functionName)
}

// ── HTTP Handler ─────────────────────────────────────────────────────
// POST /pglog (or whatever FUNCTION_TARGET is set to)
//
// 1. Loads config from S3 (cached 30s)
// 2. Reads all configured topics from Valkey cache
// 3. Detects changes (current vs previous via uns:data/uns:prev keys)
// 4. If any topic changed → INSERT snapshot row to Postgres
// 5. Returns JSON summary

func pglogHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// 1. Load config from S3
	config, err := loadConfig()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("Failed to load config: %v", err),
		})
		return
	}

	if len(config.Topics) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{
			"error": "No topics configured",
		})
		return
	}

	// 2. Ensure table exists
	if err := ensureTable(config.Table); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("Failed to ensure table: %v", err),
		})
		return
	}

	// 3. Read all topics from cache
	snapshot, err := readTopicsFromCache(config.Topics)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("Failed to read cache: %v", err),
		})
		return
	}

	// 4. Detect changes
	changed := detectChanges(config.Topics, snapshot)

	if len(changed) == 0 {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"logged":  false,
			"message": "No changes detected",
			"topics":  len(config.Topics),
		})
		return
	}

	// 5. Build values JSONB (tag → value for all topics)
	values := buildValuesJSON(config.Topics, snapshot)

	// 6. Parse UNS fields from first topic (all share the same prefix)
	uns := parseTopic(config.Topics[0])

	// 7. INSERT row
	changedTag := changed[0] // the first changed tag for the trigger column
	if err := insertRow(config.Table, uns, changedTag, values, changed); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("Failed to insert row: %v", err),
		})
		return
	}

	// 8. Update last snapshot
	updateLastSnapshot(config.Topics, snapshot)

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"logged":  true,
		"table":   config.Table,
		"changed": changed,
		"values":  values,
		"uns": map[string]string{
			"enterprise": uns.Enterprise,
			"site":       uns.Site,
			"area":       uns.Area,
			"line":       uns.Line,
		},
	})
}

// ── S3 Config Loading ────────────────────────────────────────────────

func loadConfig() (*pglogConfig, error) {
	configMu.RLock()
	if cachedConfig != nil && time.Since(configFetched) < configTTL {
		cfg := cachedConfig
		configMu.RUnlock()
		return cfg, nil
	}
	configMu.RUnlock()

	configMu.Lock()
	defer configMu.Unlock()

	// Double-check after acquiring write lock
	if cachedConfig != nil && time.Since(configFetched) < configTTL {
		return cachedConfig, nil
	}

	bucket := envOrDefault("S3_BUCKET", "")
	if bucket == "" {
		return nil, fmt.Errorf("S3_BUCKET not configured")
	}

	// Config key = FUNCTION_TARGET (container name)
	configKey := envOrDefault("FUNCTION_TARGET", "pglog") + ".json"

	result, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(configKey),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to read s3://%s/%s: %w", bucket, configKey, err)
	}
	defer result.Body.Close()

	body, err := io.ReadAll(result.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read S3 response body: %w", err)
	}

	var config pglogConfig
	if err := json.Unmarshal(body, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config JSON: %w", err)
	}

	if config.Table == "" {
		config.Table = "uns_log"
	}

	cachedConfig = &config
	configFetched = time.Now()
	log.Printf("[pglog] Loaded config from s3://%s/%s (%d topics, table: %s)",
		bucket, configKey, len(config.Topics), config.Table)

	return &config, nil
}

// ── Cache Reading ────────────────────────────────────────────────────

type topicSnapshot struct {
	Current  string
	Previous string
}

func readTopicsFromCache(topics []string) (map[string]*topicSnapshot, error) {
	pipe := cache.Pipeline()

	for _, topic := range topics {
		pipe.Get(ctx, fmt.Sprintf("%s:data:%s", keyPrefix, topic))
		pipe.Get(ctx, fmt.Sprintf("%s:prev:%s", keyPrefix, topic))
	}

	results, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		// Pipeline may return errors for individual commands; that's OK
	}
	_ = err

	snapshot := make(map[string]*topicSnapshot)
	for i, topic := range topics {
		offset := i * 2
		current := ""
		previous := ""

		if offset < len(results) {
			if val, err := results[offset].(*redis.StringCmd).Result(); err == nil {
				current = val
			}
		}
		if offset+1 < len(results) {
			if val, err := results[offset+1].(*redis.StringCmd).Result(); err == nil {
				previous = val
			}
		}

		snapshot[topic] = &topicSnapshot{
			Current:  current,
			Previous: previous,
		}
	}

	return snapshot, nil
}

// ── Change Detection ─────────────────────────────────────────────────
// Compares current cache values against the last logged snapshot.
// Returns list of tag names that changed.

func detectChanges(topics []string, snapshot map[string]*topicSnapshot) []string {
	lastSnapshotMu.Lock()
	defer lastSnapshotMu.Unlock()

	var changed []string
	for _, topic := range topics {
		tag := parseTopic(topic).Tag
		snap := snapshot[topic]
		if snap == nil {
			continue
		}

		lastVal, exists := lastSnapshot[topic]
		if !exists || lastVal != snap.Current {
			if snap.Current != "" {
				changed = append(changed, tag)
			}
		}
	}

	return changed
}

func updateLastSnapshot(topics []string, snapshot map[string]*topicSnapshot) {
	lastSnapshotMu.Lock()
	defer lastSnapshotMu.Unlock()

	for _, topic := range topics {
		if snap := snapshot[topic]; snap != nil && snap.Current != "" {
			lastSnapshot[topic] = snap.Current
		}
	}
}

// ── Values Builder ───────────────────────────────────────────────────
// Builds a map of tag → parsed value for all topics (the full snapshot).

func buildValuesJSON(topics []string, snapshot map[string]*topicSnapshot) map[string]interface{} {
	values := make(map[string]interface{})

	for _, topic := range topics {
		tag := parseTopic(topic).Tag
		snap := snapshot[topic]
		if snap == nil || snap.Current == "" {
			values[tag] = nil
			continue
		}

		// Try to parse as JSON, fall back to raw string
		var parsed interface{}
		if err := json.Unmarshal([]byte(snap.Current), &parsed); err != nil {
			values[tag] = snap.Current
		} else {
			values[tag] = parsed
		}
	}

	return values
}

// ── UNS Topic Parsing ───────────────────────────────────────────────
// Parses UNS Framework topic path into ISA-95 hierarchy fields.
// v1.0/{enterprise}/{site}/{area}/{line}/{tag...}

func parseTopic(topic string) unsFields {
	parts := strings.Split(topic, "/")

	fields := unsFields{
		Enterprise: "unknown",
		Site:       "unknown",
		Area:       "unknown",
		Line:       "unknown",
		Tag:        "unknown",
	}

	// parts[0] = version (e.g. "v1.0")
	if len(parts) >= 2 {
		fields.Enterprise = parts[1]
	}
	if len(parts) >= 3 {
		fields.Site = parts[2]
	}
	if len(parts) >= 4 {
		fields.Area = parts[3]
	}
	if len(parts) >= 5 {
		fields.Line = parts[4]
	}
	if len(parts) >= 6 {
		// Tag can be multi-level (e.g. "cell1/temperature")
		fields.Tag = strings.Join(parts[5:], "/")
	}

	return fields
}

// ── Postgres ─────────────────────────────────────────────────────────

func ensureTable(table string) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id          BIGSERIAL    PRIMARY KEY,
			logged_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
			enterprise  TEXT         NOT NULL,
			site        TEXT         NOT NULL,
			area        TEXT         NOT NULL,
			line        TEXT         NOT NULL,
			tag         TEXT         NOT NULL,
			values      JSONB        NOT NULL,
			changed     TEXT[]       NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_%s_time ON %s (logged_at);
		CREATE INDEX IF NOT EXISTS idx_%s_line ON %s (enterprise, site, area, line);
	`, table, table, table, table, table)

	_, err := db.Exec(ctx, query)
	return err
}

func insertRow(table string, uns unsFields, tag string, values map[string]interface{}, changed []string) error {
	valuesJSON, err := json.Marshal(values)
	if err != nil {
		return fmt.Errorf("failed to marshal values: %w", err)
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (enterprise, site, area, line, tag, values, changed)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, table)

	_, err = db.Exec(ctx, query,
		uns.Enterprise,
		uns.Site,
		uns.Area,
		uns.Line,
		tag,
		valuesJSON,
		changed,
	)

	if err != nil {
		return fmt.Errorf("failed to insert row: %w", err)
	}

	log.Printf("[pglog] Logged row to %s: %s/%s/%s/%s tag=%s changed=%v",
		table, uns.Enterprise, uns.Site, uns.Area, uns.Line, tag, changed)

	return nil
}

// ── Helpers ──────────────────────────────────────────────────────────

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}
