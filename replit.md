# High-Performance Go API

## Overview
A production-grade Go API designed for large-scale PostgreSQL databases (2+ TB). It serves web GUI, Telegram bot, and WhatsApp bot clients with fast, safe, and scalable data access.

## Architecture

### Core Technologies
- **Go 1.24** - Primary backend language
- **PostgreSQL** - Single source of truth database
- **Redis** - Caching layer with read-first strategy
- **pgx** - PostgreSQL driver with connection pooling

### Key Features
- Keyset (cursor-based) pagination - O(1) performance at any scale
- Redis caching with 30-second TTL
- Rate limiting per client IP
- Whitelisted filters and sort columns
- Strict input validation
- Context-based query timeouts

## Project Structure
```
cmd/
  api/
    main.go           # Application entry point
internal/
  cache/
    redis.go          # Redis caching layer
  config/
    config.go         # Configuration management
  database/
    pool.go           # PostgreSQL connection pool
    repository.go     # Data access layer with keyset pagination
  handlers/
    api_handlers.go   # REST API handlers
    bot_handlers.go   # Telegram/WhatsApp webhook handlers
  middleware/
    middleware.go     # Rate limiting, logging, CORS, etc.
  models/
    models.go         # Data models and types
  pagination/
    cursor.go         # Opaque cursor encoding/decoding
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

### REST API
- `GET /api/records` - List records with pagination
  - Query params: `cursor`, `limit` (max 50), `sort_by`, `sort_dir`, `category`, `status`
- `GET /api/records/{id}` - Get single record by ID
- `GET /api/search?q=term` - Search records by name
- `GET /api/stats` - Get database statistics
- `GET /api/health` - Health check

### Webhooks
- `POST /webhook/telegram` - Telegram bot webhook
- `POST /webhook/whatsapp` - WhatsApp bot webhook

## Bot Commands
- `/list [category]` - List records
- `/search <term>` - Search records
- `/get <id>` - Get record details
- `/stats` - Database statistics
- `/help` - Show help message

## Query Rules (Enforced)
- Never uses SELECT *
- Never uses OFFSET pagination
- Always uses LIMIT (hard max: 50)
- Always uses keyset pagination
- Queries use indexed columns only
- Streaming rows with pgx.Rows

## Indexes
```sql
CREATE INDEX idx_records_category ON records(category);
CREATE INDEX idx_records_status ON records(status);
CREATE INDEX idx_records_created_at ON records(created_at);
CREATE INDEX idx_records_name ON records(name);
CREATE INDEX idx_records_category_created_at ON records(category, created_at);
CREATE INDEX idx_records_status_id ON records(status, id);
```

## Environment Variables
- `DATABASE_URL` - PostgreSQL connection string (required)
- `REDIS_ADDR` - Redis address (default: localhost:6379)
- `PORT` - Server port (default: 5000)
- `CACHE_TTL_SECONDS` - Cache TTL (default: 30)
- `RATE_LIMIT_RPS` - Rate limit per second (default: 100)
- `SESSION_SECRET` - Session secret for security

## Recent Changes
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
