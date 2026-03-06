# uns-log — PostgreSQL Change Logger

A Go HTTP function that reads [UNS Framework](https://www.unsframework.com) topic data from the shared Valkey cache (populated by [uns-framework](../uns-framework/)) and logs snapshot rows to PostgreSQL when any value changes.

## How It Works

```
POST /uns-log (via gateway)
    │
    ▼
┌─────────────────────────────────────────────┐
│  uns-log (Go HTTP function)                   │
│                                             │
│  1. Fetch config from Valkey (cached 30s)   │
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
         │              │
         ▼              ▼
   fnkit-cache      PostgreSQL
   (Valkey)
```

## Config in Valkey

Config is stored in the shared Valkey cache — **not** in `.env` files or S3. The function reads its config using `FUNCTION_TARGET` as the key:

```
FUNCTION_TARGET=uns-log-line1  →  reads fnkit:config:uns-log-line1
FUNCTION_TARGET=uns-log-line2  →  reads fnkit:config:uns-log-line2
```

### Config format

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

Topics can be **explicit paths** or **MQTT-style wildcard patterns** (see [Wildcard Topics](#wildcard-topics) below).

That's it — **everything else is derived from the UNS topic path**:

| UNS Level    | Parsed From | Example     |
| ------------ | ----------- | ----------- |
| `enterprise` | `parts[1]`  | acme        |
| `site`       | `parts[2]`  | factory1    |
| `area`       | `parts[3]`  | mixing      |
| `line`       | `parts[4]`  | line1       |
| `tag`        | `parts[5:]` | temperature |

Set config with valkey-cli:

```bash
docker exec fnkit-cache valkey-cli SET fnkit:config:uns-log '{"table":"uns_log","topics":["v1.0/acme/factory1/mixing/line1/temperature","v1.0/acme/factory1/mixing/line1/pressure","v1.0/acme/factory1/mixing/line1/speed"]}'
```

## Wildcard Topics

Topics in config support standard MQTT wildcard patterns, resolved at runtime against the `uns:topics` registry (populated by [uns-framework](../uns-framework/)).

| Wildcard | Meaning | Example |
| -------- | ------- | ------- |
| `+` | Matches **exactly one** topic level | `v1.0/acme/factory1/+/line1/status` |
| `#` | Matches **zero or more** levels (must be last) | `v1.0/acme/factory1/mixing/#` |

### Examples

```bash
# Log status for ALL lines in area1 (+ = single-level wildcard)
docker exec fnkit-cache valkey-cli SET fnkit:config:uns-log \
  '{"table":"uns_log","topics":["v1.0/acme/factory1/area1/+/status"]}'

# Log everything under cnc-01 (# = multi-level wildcard)
docker exec fnkit-cache valkey-cli SET fnkit:config:uns-log-cnc01 \
  '{"table":"uns_log","topics":["v1.0/acme/factory1/area1/cnc-01/#"]}'

# Mix concrete topics and wildcards
docker exec fnkit-cache valkey-cli SET fnkit:config:uns-log-mixed \
  '{"table":"uns_log","topics":["v1.0/acme/factory1/area1/cnc-01/#","v1.0/acme/factory1/area1/line5/temperature"]}'
```

### How it works

1. Concrete topics (no `+` or `#`) pass through unchanged — no extra lookup needed
2. Wildcard patterns trigger a read of the `uns:topics` SET from cache (the full topic registry)
3. Each registered topic is matched against the wildcard patterns
4. The resolved concrete topic list is used for cache reading, change detection, and logging

Wildcards are resolved on **every invocation**, so new topics that appear in the registry (e.g. a new sensor comes online) are automatically picked up without restarting or reconfiguring.

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
# Ensure fnkit-network, cache, and uns-framework are running
docker network create fnkit-network 2>/dev/null || true
fnkit cache start
# (uns-framework should already be running and populating cache)
# (PostgreSQL should be accessible)

# Set config in Valkey
docker exec fnkit-cache valkey-cli SET fnkit:config:uns-log '{"table":"uns_log","topics":["v1.0/acme/factory1/mixing/line1/temperature"]}'

# Build and start
docker compose up -d

# Check logs
docker logs -f uns-log

# Trigger a log run
curl http://localhost:8080/uns-log
```

## Multiple Instances

Deploy the same image multiple times with different `FUNCTION_TARGET` values. Each reads its own config from Valkey:

```yaml
services:
  uns-log-line1:
    build: .
    container_name: uns-log-line1
    environment:
      - FUNCTION_TARGET=uns-log-line1
      # ... same Postgres/Cache config

  uns-log-line2:
    build: .
    container_name: uns-log-line2
    environment:
      - FUNCTION_TARGET=uns-log-line2
      # ... same Postgres/Cache config
```

```bash
# Set separate configs in Valkey
docker exec fnkit-cache valkey-cli SET fnkit:config:uns-log-line1 '{"table":"uns_log","topics":[...]}'
docker exec fnkit-cache valkey-cli SET fnkit:config:uns-log-line2 '{"table":"uns_log","topics":[...]}'

# Trigger via gateway
curl http://localhost:8080/uns-log-line1
curl http://localhost:8080/uns-log-line2
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

Environment variables (connections only — topic config lives in Valkey):

| Variable           | Default                                                            | Description                            |
| ------------------ | ------------------------------------------------------------------ | -------------------------------------- |
| `FUNCTION_TARGET`  | `uns-log`                                                          | Function name = Valkey config key      |
| `DATABASE_URL`     | `postgres://fnkit:fnkit@fnkit-postgres:5432/fnkit?sslmode=disable` | PostgreSQL connection string           |
| `CACHE_URL`        | `redis://fnkit-cache:6379`                                         | Valkey/Redis connection                |
| `CACHE_KEY_PREFIX` | `uns`                                                              | Cache key prefix (match uns-framework) |

## UNS Framework

The [Unified Namespace (UNS) Framework](https://www.unsframework.com) organises enterprise data in a hierarchical MQTT topic structure following ISA-95:

```
v1.0/{enterprise}/{site}/{area}/{line}/{tag}
```

All hierarchy metadata is parsed directly from the topic path — no manual mapping needed.

## Built With

- [fnkit](https://github.com/maxbaines/fnkit) — scaffolded with `fnkit go uns-log`
- [functions-framework-go](https://github.com/GoogleCloudPlatform/functions-framework-go) — HTTP function framework
- [go-redis](https://github.com/redis/go-redis) — Valkey/Redis client
- [pgx](https://github.com/jackc/pgx) — PostgreSQL driver
