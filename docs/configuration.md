# Configuration Reference

This document provides a complete reference for all L.S.D configuration options, including environment variables, database settings, and advanced tuning parameters.

## Table of Contents

- [Configuration Methods](#configuration-methods)
- [Environment Variables](#environment-variables)
- [Database Configuration](#database-configuration)
- [Cache Configuration](#cache-configuration)
- [ClickHouse Configuration](#clickhouse-configuration)
- [CDC Pipeline Configuration](#cdc-pipeline-configuration)
- [Security Configuration](#security-configuration)
- [Rate Limiting](#rate-limiting)
- [Logging Configuration](#logging-configuration)
- [Advanced Tuning](#advanced-tuning)

## Configuration Methods

L.S.D supports multiple configuration methods:

### 1. Environment Variables (Recommended)

```bash
export DATABASE_URL="postgresql://..."
export PORT=5000
go run ./cmd/api
```

### 2. .env File

Create a `.env` file in the project root:

```bash
DATABASE_URL=postgresql://user:pass@host:5432/db
PORT=5000
SESSION_SECRET=your-secret-key
```

Load with `godotenv` (automatic in development):

```go
// Automatically loaded in main.go
godotenv.Load()
```

### 3. Configuration File

For complex setups, use a YAML configuration file:

```yaml
# config.yaml
database:
  url: postgresql://user:pass@host:5432/db
  max_connections: 50

server:
  port: 5000
  read_timeout: 30s
  write_timeout: 30s

cache:
  redis_addr: localhost:6379
  ttl: 30s
```

## Environment Variables

### Required Variables

| Variable | Type | Description | Example |
|----------|------|-------------|---------|
| `DATABASE_URL` | string | PostgreSQL connection string | `postgresql://user:pass@host:5432/db` |
| `SESSION_SECRET` | string | Secret key for JWT signing (min 32 chars) | `your-super-secret-key-here` |

### Server Configuration

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `PORT` | int | `5000` | HTTP server port |
| `HOST` | string | `0.0.0.0` | Server bind address |
| `READ_TIMEOUT` | duration | `30s` | HTTP read timeout |
| `WRITE_TIMEOUT` | duration | `30s` | HTTP write timeout |
| `IDLE_TIMEOUT` | duration | `120s` | HTTP idle timeout |
| `GRACEFUL_SHUTDOWN_TIMEOUT` | duration | `30s` | Graceful shutdown wait time |

### Example Configuration

```bash
# ═══════════════════════════════════════════════════════════
# Core Server Settings
# ═══════════════════════════════════════════════════════════
PORT=5000
HOST=0.0.0.0
READ_TIMEOUT=30s
WRITE_TIMEOUT=30s

# ═══════════════════════════════════════════════════════════
# Security
# ═══════════════════════════════════════════════════════════
SESSION_SECRET=change-this-to-a-secure-random-string-at-least-32-characters-long
```

## Database Configuration

### Connection String Format

```
postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]
```

### Required

| Variable | Type | Description |
|----------|------|-------------|
| `DATABASE_URL` | string | Full PostgreSQL connection string |

### Optional Database Parameters

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `DB_MAX_CONNECTIONS` | int | `50` | Maximum pool connections |
| `DB_MIN_CONNECTIONS` | int | `5` | Minimum idle connections |
| `DB_MAX_CONN_LIFETIME` | duration | `1h` | Max connection lifetime |
| `DB_MAX_CONN_IDLE_TIME` | duration | `30m` | Max idle time before close |
| `DB_HEALTH_CHECK_PERIOD` | duration | `1m` | Health check interval |

### Connection String Parameters

Add these to your `DATABASE_URL` for fine-grained control:

```
postgresql://user:pass@host:5432/db?sslmode=require&connect_timeout=10&statement_timeout=30000
```

| Parameter | Values | Description |
|-----------|--------|-------------|
| `sslmode` | `disable`, `require`, `verify-ca`, `verify-full` | SSL/TLS mode |
| `connect_timeout` | seconds | Connection timeout |
| `statement_timeout` | milliseconds | Query timeout |
| `application_name` | string | Application identifier |
| `search_path` | string | Schema search path |

### PgBouncer Compatibility

L.S.D is fully compatible with PgBouncer in **transaction pooling mode**:

```bash
# Point DATABASE_URL to PgBouncer
DATABASE_URL=postgresql://user:pass@pgbouncer:6432/lsd?sslmode=disable
```

⚠️ **Note**: Session-level features (prepared statements across transactions) are automatically handled.

## Cache Configuration

### Redis Settings

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `REDIS_ADDR` | string | `localhost:6379` | Redis server address |
| `REDIS_PASSWORD` | string | `""` | Redis password |
| `REDIS_DB` | int | `0` | Redis database number |
| `CACHE_TTL_SECONDS` | int | `30` | Default cache TTL |
| `CACHE_ENABLED` | bool | `true` | Enable/disable caching |

### Cache Behavior

```bash
# Enable caching with 30-second TTL
REDIS_ADDR=localhost:6379
CACHE_TTL_SECONDS=30

# Disable caching (no Redis required)
CACHE_ENABLED=false
```

### Cache Key Patterns

| Resource Type | Key Pattern | Example |
|---------------|-------------|---------|
| Table list | `tables:list` | `tables:list` |
| Table schema | `tables:schema:{table}` | `tables:schema:users` |
| Records list | `records:{table}:{cursor}:{filters}` | `records:users:abc123:` |
| Single record | `record:{table}:{pk}` | `record:users:42` |
| Search results | `search:{table}:{query}` | `search:users:john` |

### Cache Invalidation

Cache is automatically invalidated:

- **TTL expiration**: After `CACHE_TTL_SECONDS`
- **Manual**: Via admin API endpoint (future)
- **Write operations**: When implemented (future)

## ClickHouse Configuration

### Connection Settings

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `CLICKHOUSE_ADDR` | string | `""` | ClickHouse server address |
| `CLICKHOUSE_DATABASE` | string | `default` | Database name |
| `CLICKHOUSE_USER` | string | `default` | Username |
| `CLICKHOUSE_PASSWORD` | string | `""` | Password |
| `CLICKHOUSE_ASYNC` | bool | `false` | Async insert mode |
| `CLICKHOUSE_DEBUG` | bool | `false` | Enable debug logging |

### Search Configuration

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SEARCH_ENGINE` | string | `auto` | Search engine: `auto`, `clickhouse`, `postgresql` |
| `SEARCH_LIMIT` | int | `50` | Maximum search results |
| `SEARCH_MIN_LENGTH` | int | `2` | Minimum query length |

### Example

```bash
# Enable ClickHouse search acceleration
CLICKHOUSE_ADDR=localhost:9000
CLICKHOUSE_DATABASE=lsd_search
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=secure_password

# Force ClickHouse (fail if unavailable)
SEARCH_ENGINE=clickhouse

# Or force PostgreSQL fallback
SEARCH_ENGINE=postgresql
```

## CDC Pipeline Configuration

The Change Data Capture pipeline syncs data from PostgreSQL to ClickHouse.

### Basic Settings

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `CDC_ENABLED` | bool | `true` | Enable/disable CDC |
| `CDC_SYNC_INTERVAL_SECONDS` | int | `30` | Sync interval in seconds |
| `CDC_BATCH_SIZE` | int | `1000` | Records per batch |

### Table Selection

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `CLICKHOUSE_SYNC_TABLES` | string | `""` | Comma-separated tables (empty = all) |

### Example

```bash
# Sync all tables every 30 seconds
CDC_ENABLED=true
CDC_SYNC_INTERVAL_SECONDS=30
CDC_BATCH_SIZE=25000

# Sync only specific tables
CLICKHOUSE_SYNC_TABLES=users,orders,products

# Disable CDC
CDC_ENABLED=false
```

### CDC Requirements

For CDC to work, tables must have:

1. **Primary key**: Used for tracking changes
2. **`updated_at` column**: Timestamp for incremental sync

```sql
-- Add updated_at if missing
ALTER TABLE your_table ADD COLUMN updated_at TIMESTAMP DEFAULT NOW();

-- Create auto-update trigger
CREATE TRIGGER trigger_updated_at
BEFORE UPDATE ON your_table
FOR EACH ROW EXECUTE FUNCTION update_timestamp();
```

## Security Configuration

### JWT Settings

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SESSION_SECRET` | string | **Required** | JWT signing secret |
| `JWT_ACCESS_TOKEN_TTL` | duration | `15m` | Access token lifetime |
| `JWT_REFRESH_TOKEN_TTL` | duration | `7d` | Refresh token lifetime |
| `JWT_ISSUER` | string | `lsd-api` | Token issuer |

### Session Settings

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SESSION_MAX_AGE` | duration | `7d` | Session lifetime |
| `SESSION_SECURE` | bool | `false` | HTTPS only cookies |
| `SESSION_HTTP_ONLY` | bool | `true` | HTTP-only cookies |
| `SESSION_SAME_SITE` | string | `lax` | SameSite policy |

### CORS Settings

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `CORS_ALLOWED_ORIGINS` | string | `*` | Allowed origins (comma-separated) |
| `CORS_ALLOWED_METHODS` | string | `GET,POST,PUT,DELETE,OPTIONS` | Allowed methods |
| `CORS_ALLOWED_HEADERS` | string | `*` | Allowed headers |
| `CORS_MAX_AGE` | duration | `86400` | Preflight cache duration |

### Example Security Config

```bash
# Production security settings
SESSION_SECRET=your-production-secret-min-32-characters
JWT_ACCESS_TOKEN_TTL=15m
JWT_REFRESH_TOKEN_TTL=168h

# HTTPS only
SESSION_SECURE=true

# CORS for specific origins
CORS_ALLOWED_ORIGINS=https://app.example.com,https://admin.example.com
```

## Rate Limiting

### Basic Settings

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `RATE_LIMIT_ENABLED` | bool | `true` | Enable rate limiting |
| `RATE_LIMIT_RPS` | int | `100` | Requests per second per client |

### Rate Limits by Client Type

| Client Type | Rate Limit | Notes |
|-------------|------------|-------|
| Anonymous | 100 req/min | IP-based |
| Authenticated User | 1,000 req/min | User ID-based |
| API Key (AI Agent) | 5,000 req/min | Key-based |

### Example

```bash
# Custom rate limits
RATE_LIMIT_RPS=100
RATE_LIMIT_WINDOW=60s

# Disable rate limiting (not recommended)
RATE_LIMIT_ENABLED=false
```

### Rate Limit Headers

Every response includes rate limit information:

```
X-RateLimit-Limit: 100
X-RateLimit-Remaining: 95
X-RateLimit-Reset: 1704067200
```

## Logging Configuration

### Settings

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `LOG_LEVEL` | string | `info` | Log level: `debug`, `info`, `warn`, `error` |
| `LOG_FORMAT` | string | `json` | Output format: `json`, `text` |
| `LOG_OUTPUT` | string | `stdout` | Output: `stdout`, `stderr`, file path |

### Log Levels

| Level | Description |
|-------|-------------|
| `debug` | Detailed debugging information |
| `info` | General operational messages |
| `warn` | Warning conditions |
| `error` | Error conditions |

### Example

```bash
# Development logging
LOG_LEVEL=debug
LOG_FORMAT=text

# Production logging
LOG_LEVEL=info
LOG_FORMAT=json
LOG_OUTPUT=/var/log/lsd/api.log
```

## Advanced Tuning

### Query Builder Settings

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `QUERY_TIMEOUT` | duration | `30s` | Query execution timeout |
| `MAX_PAGE_SIZE` | int | `50` | Maximum records per page |
| `DEFAULT_PAGE_SIZE` | int | `20` | Default records per page |

### Schema Discovery

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `SCHEMA_REFRESH_INTERVAL` | duration | `5m` | Schema refresh interval |
| `SCHEMA_CACHE_SIZE` | int | `1000` | Cached schemas |

### Connection Pool Tuning

```bash
# High-traffic production settings
DB_MAX_CONNECTIONS=100
DB_MIN_CONNECTIONS=20
DB_MAX_CONN_LIFETIME=30m
DB_MAX_CONN_IDLE_TIME=10m
DB_HEALTH_CHECK_PERIOD=30s
```

### Performance Tuning Example

```bash
# Production configuration for 2-4 TB database
DATABASE_URL=postgresql://user:pass@host:5432/db
DB_MAX_CONNECTIONS=100
DB_MIN_CONNECTIONS=20

REDIS_ADDR=localhost:6379
CACHE_TTL_SECONDS=60

CLICKHOUSE_ADDR=clickhouse:9000
CDC_SYNC_INTERVAL_SECONDS=60
CDC_BATCH_SIZE=50000

RATE_LIMIT_RPS=500
QUERY_TIMEOUT=60s
MAX_PAGE_SIZE=50
```

---

**Next**: [Development Guide](development.md) | [Deployment Guide](deployment.md)
