package function

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// ── Configuration ────────────────────────────────────────────────────
// Config is loaded from Valkey cache using the function name as the key.
// e.g. FUNCTION_TARGET=uns-log-line1 → reads fnkit:config:uns-log-line1
//
// Config format (stored as JSON string in Valkey):
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
//
// ── Wildcard Topics ─────────────────────────────────────────────────
// Topics in config may contain MQTT-style wildcards:
//   + matches exactly one level    e.g. v1.0/acme/factory1/+/line1/temperature
//   # matches zero or more levels  e.g. v1.0/acme/factory1/mixing/#
//
// Wildcards are resolved at runtime against the uns:topics registry
// (populated by uns-framework). Concrete topics pass through unchanged.

type unsLogConfig struct {
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
	keyPrefix string

	// Config cache
	configMu      sync.RWMutex
	cachedConfig  *unsLogConfig
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
		log.Fatalf("[uns-log] Failed to parse CACHE_URL: %v", err)
	}
	cache = redis.NewClient(opts)

	if err := cache.Ping(ctx).Err(); err != nil {
		log.Printf("[uns-log] Warning: cache not reachable at %s: %v", cacheURL, err)
	} else {
		log.Printf("[uns-log] Connected to cache at %s", cacheURL)
	}

	// ── Postgres connection ──────────────────────────────────────────
	dbURL := envOrDefault("DATABASE_URL", "postgres://fnkit:fnkit@fnkit-postgres:5432/fnkit?sslmode=disable")
	db, err = pgxpool.New(ctx, dbURL)
	if err != nil {
		log.Fatalf("[uns-log] Failed to create Postgres pool: %v", err)
	}

	if err := db.Ping(ctx); err != nil {
		log.Printf("[uns-log] Warning: Postgres not reachable: %v", err)
	} else {
		log.Printf("[uns-log] Connected to Postgres")
	}

	// ── Initialize last snapshot ─────────────────────────────────────
	lastSnapshot = make(map[string]string)

	// ── Register HTTP function ───────────────────────────────────────
	// The function name matches FUNCTION_TARGET, which is also the config key.
	functionName := envOrDefault("FUNCTION_TARGET", "uns-log")
	functions.HTTP(functionName, unsLogHandler)
	log.Printf("[uns-log] Registered HTTP function: %s", functionName)
}

// ── HTTP Handler ─────────────────────────────────────────────────────
// POST /uns-log (or whatever FUNCTION_TARGET is set to)
//
// 1. Loads config from Valkey cache (cached 30s)
// 2. Reads all configured topics from Valkey cache
// 3. Detects changes (current vs previous via uns:data/uns:prev keys)
// 4. If any topic changed → INSERT snapshot row to Postgres
// 5. Returns JSON summary

func unsLogHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// 1. Load config from Valkey
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

	// 2. Resolve wildcard topics against the uns:topics registry
	topics, err := resolveTopics(config.Topics)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("Failed to resolve topics: %v", err),
		})
		return
	}

	if len(topics) == 0 {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"logged":  false,
			"message": "No topics matched (wildcards resolved to empty set)",
			"patterns": config.Topics,
		})
		return
	}

	// 3. Ensure table exists
	if err := ensureTable(config.Table); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("Failed to ensure table: %v", err),
		})
		return
	}

	// 4. Read all topics from cache
	snapshot, err := readTopicsFromCache(topics)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("Failed to read cache: %v", err),
		})
		return
	}

	// 5. Detect changes
	changed := detectChanges(topics, snapshot)

	if len(changed) == 0 {
		writeJSON(w, http.StatusOK, map[string]interface{}{
			"logged":  false,
			"message": "No changes detected",
			"topics":  len(topics),
		})
		return
	}

	// 6. Build values JSONB (tag → value for all topics)
	values := buildValuesJSON(topics, snapshot)

	// 7. Parse UNS fields from first topic (all share the same prefix)
	uns := parseTopic(topics[0])

	// 8. INSERT row
	changedTag := changed[0] // the first changed tag for the trigger column
	if err := insertRow(config.Table, uns, changedTag, values, changed); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{
			"error": fmt.Sprintf("Failed to insert row: %v", err),
		})
		return
	}

	// 9. Update last snapshot
	updateLastSnapshot(topics, snapshot)

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

// ── Config Loading (from Valkey) ─────────────────────────────────────

func loadConfig() (*unsLogConfig, error) {
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

	// Config key = fnkit:config:<FUNCTION_TARGET>
	configKey := "fnkit:config:" + envOrDefault("FUNCTION_TARGET", "uns-log")

	raw, err := cache.Get(ctx, configKey).Result()
	if err == redis.Nil {
		return nil, fmt.Errorf("config not found at key %s — set it with: docker exec fnkit-cache valkey-cli SET %s '<json>'", configKey, configKey)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to read config from cache key %s: %w", configKey, err)
	}

	var config unsLogConfig
	if err := json.Unmarshal([]byte(raw), &config); err != nil {
		return nil, fmt.Errorf("failed to parse config JSON: %w", err)
	}

	if config.Table == "" {
		config.Table = "uns_log"
	}

	cachedConfig = &config
	configFetched = time.Now()
	log.Printf("[uns-log] Loaded config from %s (%d topics, table: %s)",
		configKey, len(config.Topics), config.Table)

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

	log.Printf("[uns-log] Logged row to %s: %s/%s/%s/%s tag=%s changed=%v",
		table, uns.Enterprise, uns.Site, uns.Area, uns.Line, tag, changed)

	return nil
}

// ── Wildcard Topic Resolution ────────────────────────────────────────
// Resolves MQTT-style wildcard patterns against the uns:topics registry.
// Concrete topics (no wildcards) pass through unchanged.
// Patterns containing + or # are matched against all known topics.

// resolveTopics takes the configured topic list (which may contain wildcards)
// and returns a deduplicated list of concrete topics.
func resolveTopics(configTopics []string) ([]string, error) {
	var concrete []string
	var patterns []string

	for _, t := range configTopics {
		if strings.Contains(t, "+") || strings.Contains(t, "#") {
			patterns = append(patterns, t)
		} else {
			concrete = append(concrete, t)
		}
	}

	// No wildcards — return as-is (fast path, no cache lookup needed)
	if len(patterns) == 0 {
		return concrete, nil
	}

	// Fetch all known topics from the uns:topics registry
	topicsKey := fmt.Sprintf("%s:topics", keyPrefix)
	allTopics, err := cache.SMembers(ctx, topicsKey).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to read topic registry (%s): %w", topicsKey, err)
	}

	// Match each registered topic against wildcard patterns
	seen := make(map[string]bool)
	for _, t := range concrete {
		seen[t] = true
	}

	for _, topic := range allTopics {
		if seen[topic] {
			continue
		}
		for _, pattern := range patterns {
			if matchMQTTPattern(pattern, topic) {
				concrete = append(concrete, topic)
				seen[topic] = true
				break
			}
		}
	}

	log.Printf("[uns-log] Resolved %d patterns + %d concrete → %d topics",
		len(patterns), len(configTopics)-len(patterns), len(concrete))

	return concrete, nil
}

// matchMQTTPattern checks if a topic matches an MQTT wildcard pattern.
//
// Rules (per MQTT spec):
//   - "+" matches exactly one topic level
//   - "#" matches zero or more levels and must be the last segment
//
// Examples:
//
//	matchMQTTPattern("v1.0/acme/+/mixing/line1/temperature", "v1.0/acme/factory1/mixing/line1/temperature") → true
//	matchMQTTPattern("v1.0/acme/factory1/#", "v1.0/acme/factory1/mixing/line1/temperature")                 → true
//	matchMQTTPattern("v1.0/acme/factory1/#", "v1.0/acme/factory1")                                          → true
func matchMQTTPattern(pattern, topic string) bool {
	patternParts := strings.Split(pattern, "/")
	topicParts := strings.Split(topic, "/")

	for i, pp := range patternParts {
		// "#" matches everything from here onward (must be last segment)
		if pp == "#" {
			return true
		}

		// Ran out of topic levels but pattern still has more
		if i >= len(topicParts) {
			return false
		}

		// "+" matches exactly one level (any value)
		if pp == "+" {
			continue
		}

		// Literal match required
		if pp != topicParts[i] {
			return false
		}
	}

	// Pattern consumed — topic must also be fully consumed
	return len(patternParts) == len(topicParts)
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
