# pglog — PostgreSQL Change Logger

A Go HTTP function that reads [UNS Framework](https://www.unsframework.com) topic data from the shared Valkey cache (populated by [mqttuns](../mqttuns/)) and logs snapshot rows to PostgreSQL when any value changes.

## How It Works

```
POST /pglog (via gateway)
    │
    ▼
┌─────────────────────────────────────────────┐
│  pglog (Go HTTP function)                   │
│                                             │
│  1. Fetch config from S3 (cached 30s)       │
│     → topic list, table name                │
│                                             │
│  2. Read all topics from Valkey cache       │
│     → uns:data:<topic> for current values   │
│     → uns:prev:<topic> for change detection │
│                                             │
│  3. Compare current vs last logged snapshot │
│     → if ANY changed: build full row        │
│     → unchanged values copied forward       │
│                                             │
│  4. INSERT into PostgreSQL                  │
│     → auto-creates table on first run       │
│                                             │
│  5. Return JSON summary                     │
└─────────────────────────────────────────────┘
         │              │              │
         ▼              ▼              ▼
   S3 (config)    fnkit-cache    PostgreSQL
                  (Valkey)
```

## Config in S3

Config is stored in S3 — **not** in `.env` files. The function reads its config from S3 using `FUNCTION_TARGET` as the key:

```
FUNCTION_TARGET=pglog-line1  →  reads s3://{bucket}/pglog-line1.json
FUNCTION_TARGET=pglog-line2  →  reads s3://{bucket}/pglog-line2.json
```

### Config file format

```json
{
  "table": "uns_log",
  "topics": [
    "v1.0/acme/factory1/mixing/line1/temperature",
    "v1.0/acme/factory1/mixing/line1/pressure",
    "v1.0/acme/factory1/mixing/line1/speed"
  ]
}
```

That's it — **everything else is derived from the UNS topic path**:

| UNS Level    | Parsed From | Example    |
| ------------ | ----------- | ---------- |
| `enterprise` | `parts[1]`  | acme       |
| `site`       | `parts[2]`  | factory1   |
| `area`       | `parts[3]`  | mixing     |
| `line`       | `parts[4]`  | line1      |
| `tag`        | `parts[5:]` | temperature |

Upload config with the fnkit S3 CLI:

```bash
fnkit s3 upload pglog-line1.json pglog-line1.json
```

## PostgreSQL Table

Auto-created on first run:

```sql
CREATE TABLE IF NOT EXISTS uns_log (
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
```

### Example rows

```
id | logged_at                | enterprise | site     | area   | line  | tag         | values                                              | changed
1  | 2026-02-21T15:10:44Z     | acme       | factory1 | mixing | line1 | temperature | {"temperature": 23.1, "pressure": 1.2, "speed": 45} | {temperature}
2  | 2026-02-21T15:10:48Z     | acme       | factory1 | mixing | line1 | pressure    | {"temperature": 23.1, "pressure": 1.5, "speed": 45} | {pressure}
3  | 2026-02-21T15:10:50Z     | acme       | factory1 | mixing | line1 | temperature | {"temperature": 24.0, "pressure": 1.5, "speed": 45} | {temperature}
```

Every row is a **complete snapshot** — unchanged values are copied forward.

## Quick Start

```bash
# Ensure fnkit-network, cache, and mqttuns are running
docker network create fnkit-network 2>/dev/null || true
fnkit cache start
# (mqttuns should already be running and populating cache)
# (PostgreSQL should be accessible)

# Upload config to S3
fnkit s3 init --bucket fnkit-config --endpoint http://minio:9000
fnkit s3 upload pglog.json pglog.json

# Build and start
docker compose up -d

# Check logs
docker logs -f pglog

# Trigger a log run
curl http://localhost:8080/pglog
```

## Multiple Instances

Deploy the same image multiple times with different `FUNCTION_TARGET` values. Each reads its own config from S3:

```yaml
services:
  pglog-line1:
    build: .
    container_name: pglog-line1
    environment:
      - FUNCTION_TARGET=pglog-line1
      # ... same S3/Postgres/Cache config

  pglog-line2:
    build: .
    container_name: pglog-line2
    environment:
      - FUNCTION_TARGET=pglog-line2
      # ... same S3/Postgres/Cache config
```

```bash
# Upload separate configs
fnkit s3 upload line1-config.json pglog-line1.json
fnkit s3 upload line2-config.json pglog-line2.json

# Trigger via gateway
curl http://localhost:8080/pglog-line1
curl http://localhost:8080/pglog-line2
```

## API Response

### Change detected (row logged)

```json
{
  "logged": true,
  "table": "uns_log",
  "changed": ["temperature"],
  "values": {
    "temperature": 23.1,
    "pressure": 1.2,
    "speed": 45
  },
  "uns": {
    "enterprise": "acme",
    "site": "factory1",
    "area": "mixing",
    "line": "line1"
  }
}
```

### No changes

```json
{
  "logged": false,
  "message": "No changes detected",
  "topics": 3
}
```

## Configuration

Environment variables (connections only — topic config lives in S3):

| Variable           | Default                                                          | Description                        |
| ------------------ | ---------------------------------------------------------------- | ---------------------------------- |
| `FUNCTION_TARGET`  | `pglog`                                                          | Function name = S3 config key      |
| `S3_ENDPOINT`      |                                                                  | S3-compatible endpoint (MinIO etc) |
| `S3_BUCKET`        | `fnkit-config`                                                   | S3 bucket for config files         |
| `S3_REGION`        | `us-east-1`                                                      | S3 region                          |
| `S3_ACCESS_KEY`    |                                                                  | S3 access key                      |
| `S3_SECRET_KEY`    |                                                                  | S3 secret key                      |
| `DATABASE_URL`     | `postgres://fnkit:fnkit@fnkit-postgres:5432/fnkit?sslmode=disable` | PostgreSQL connection string       |
| `CACHE_URL`        | `redis://fnkit-cache:6379`                                       | Valkey/Redis connection            |
| `CACHE_KEY_PREFIX` | `uns`                                                            | Cache key prefix (match mqttuns)   |

## UNS Framework

The [Unified Namespace (UNS) Framework](https://www.unsframework.com) organises enterprise data in a hierarchical MQTT topic structure following ISA-95:

```
v1.0/{enterprise}/{site}/{area}/{line}/{tag}
```

All hierarchy metadata is parsed directly from the topic path — no manual mapping needed.

## Built With

- [fnkit](https://github.com/maxbaines/fnkit) — scaffolded with `fnkit go pglog`
- [functions-framework-go](https://github.com/GoogleCloudPlatform/functions-framework-go) — HTTP function framework
- [go-redis](https://github.com/redis/go-redis) — Valkey/Redis client
- [pgx](https://github.com/jackc/pgx) — PostgreSQL driver
- [aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2) — S3 client
