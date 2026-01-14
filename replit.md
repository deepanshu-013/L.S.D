# High-Performance Dynamic Go API

## Overview
A production-grade Go API that automatically adapts to any PostgreSQL database schema by discovering tables and columns at runtime. Designed for large-scale databases (2-4 TB) serving web GUI, Telegram bot, and WhatsApp bot clients with fast, safe, and scalable data access. Features ClickHouse integration for sub-second multi-column text search on billions of rows.

## Architecture

### Core Technologies
- **Go 1.24** - Primary backend language
- **PostgreSQL** - Single source of truth database
- **ClickHouse** - Search acceleration layer (optional)
- **Redis** - Caching layer with 30-second TTL
- **pgx** - PostgreSQL driver with connection pooling
- **clickhouse-go/v2** - ClickHouse driver

### Key Features
- **Dynamic Schema Discovery** - Auto-discovers tables, columns, primary keys, and indexes at startup
- **Keyset (cursor-based) pagination** - O(1) performance at any scale
- **Type-aware cursor encoding** - Correctly handles timestamps, integers, decimals, and text
- **ClickHouse search acceleration** - Sub-second multi-column text search with ngram indexes
- **CDC pipeline** - Automatic PostgreSQL → ClickHouse sync with delete handling
- **Redis caching** with 30-second TTL
- **Rate limiting** per client IP
- **Whitelisted filters/sort** - Only leading indexed columns allowed
- **Strict input validation**
- **Context-based query timeouts**
- **Graceful fallback** - Uses PostgreSQL search when ClickHouse is unavailable

## Project Structure
```
cmd/
  api/
    main.go           # Application entry point
internal/
  cache/
    redis.go          # Redis caching layer
  clickhouse/
    connection.go     # ClickHouse connection management
    search_repository.go  # Search index with ngram support
    cdc.go            # CDC pipeline for PostgreSQL sync
  config/
    config.go         # Configuration management
  database/
    pool.go           # PostgreSQL connection pool
    dynamic_repository.go  # Dynamic data access layer
  handlers/
    dynamic_handlers.go    # Dynamic REST API handlers
    bot_handlers.go        # Telegram/WhatsApp webhook handlers
  middleware/
    middleware.go     # Rate limiting, logging, CORS, etc.
  models/
    models.go         # Data models and types
  schema/
    loader.go         # Schema discovery from PostgreSQL metadata
    query_builder.go  # Type-aware query builder with cursor pagination
  services/
    record_service.go # Business logic layer
web/
  static/
    app.js            # Frontend JavaScript
    style.css         # Frontend styles
  templates/
    index.html        # Web GUI template
```

## API Endpoints

### Dynamic REST API
- `GET /api/tables` - List all discovered tables
- `GET /api/tables/{table}/schema` - Get table schema and columns
- `GET /api/tables/{table}/records` - List records with pagination
  - Query params: `cursor`, `limit` (max 50), `sort_by`, `sort_dir`, plus any indexed column for filtering
- `GET /api/tables/{table}/records/{pk}` - Get single record by primary key
- `GET /api/tables/{table}/search?q=term&engine=auto` - Multi-column search
  - `engine` param: `auto` (default), `clickhouse`, or `postgresql` for manual engine selection
- `GET /api/tables/{table}/stats` - Get table statistics (uses estimated count)
- `GET /api/cdc/status` - CDC pipeline status with per-table sync metrics
- `GET /api/health` - Health check (includes Redis and ClickHouse status)

### Webhooks
- `POST /webhook/telegram` - Telegram bot webhook
- `GET/POST /webhook/whatsapp` - WhatsApp bot webhook

## Bot Commands
- `/list [table]` - List records from a table
- `/search <table> <term>` - Search records
- `/get <table> <id>` - Get record details
- `/stats` - API statistics
- `/help` - Show help message

## Query Rules (Enforced)
- Never uses SELECT * (always explicit columns)
- Never uses OFFSET pagination
- Always uses LIMIT (hard max: 50)
- Always uses keyset pagination
- Only leading indexed columns allowed for sorting/filtering
- Streaming rows with pgx.Rows

## Search Architecture
- **ClickHouse (when available)**: Uses ReplacingMergeTree with ngram indexes for fast multi-column text search
- **PostgreSQL fallback**: Uses ILIKE queries across text columns
- **Cursor validation**: Prevents pagination state bleed across different searches
- **Multi-token support**: "RAHUL VERMA" searches for both tokens across all text columns

## CDC Pipeline
- Automatic sync from PostgreSQL to ClickHouse every 30 seconds (configurable)
- Tracks updates via updated_at/modified_at columns
- Handles deletes via ReplacingMergeTree tombstones (is_deleted flag)
- Configurable tables via CLICKHOUSE_SYNC_TABLES environment variable

## Schema Discovery
At startup, the API queries PostgreSQL metadata to discover:
- All user tables (excluding pg_catalog, information_schema)
- Column names, types, and nullability
- Primary key columns (ordered)
- Leading indexed columns (for safe sorting/filtering)
- Text columns for search (varchar, text)

## Environment Variables
- `DATABASE_URL` - PostgreSQL connection string (required)
- `REDIS_ADDR` - Redis address (default: localhost:6379)
- `PORT` - Server port (default: 5000)
- `CACHE_TTL_SECONDS` - Cache TTL (default: 30)
- `RATE_LIMIT_RPS` - Rate limit per second (default: 100)
- `SESSION_SECRET` - Session secret for security
- `CLICKHOUSE_ADDR` - ClickHouse address (optional, enables search acceleration)
- `CLICKHOUSE_DATABASE` - ClickHouse database name (default: default)
- `CLICKHOUSE_USER` - ClickHouse username (default: default)
- `CLICKHOUSE_PASSWORD` - ClickHouse password (optional)
- `CLICKHOUSE_SYNC_TABLES` - Comma-separated list of tables to sync (default: all)
- `CDC_SYNC_INTERVAL_SECONDS` - CDC sync interval (default: 30)
- `CDC_BATCH_SIZE` - Records per CDC batch (default: 1000)

## Recent Changes
- 2026-01-14: Fixed cursor validation to prevent pagination bleed across searches
- 2026-01-14: Fixed CDC delete handling with ReplacingMergeTree tombstones
- 2026-01-14: Improved error logging and PostgreSQL search fallback
- 2026-01-14: Added ClickHouse integration for accelerated multi-column search
- 2026-01-14: Implemented CDC pipeline for PostgreSQL → ClickHouse sync
- 2026-01-14: Fixed stats endpoint with pg_class.reltuples estimation
- 2026-01-13: Implemented dynamic schema discovery with leading index detection
- 2026-01-13: Fixed composite cursor pagination for multi-column primary keys
- 2026-01-13: Restricted sorting/filtering to leading indexed columns only
- 2026-01-13: Initial implementation with full feature set

## Running the Application
The workflow starts Redis and the Go API server:
```bash
redis-server --daemonize yes --port 6379; go run ./cmd/api
```

## Deployment Notes
- Designed for PgBouncer compatibility (transaction pooling)
- Supports read replicas without code changes
- Horizontal scaling ready
- Supports partitioned PostgreSQL tables
- Works with any database schema without code changes
- ClickHouse is optional - API works without it
- CDC pipeline runs in background, gracefully handles ClickHouse unavailability
