# Setup Guide

This guide walks you through installing and configuring L.S.D from scratch. Whether you're running locally for development or preparing for production deployment, follow these steps.

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation Methods](#installation-methods)
- [Database Setup](#database-setup)
- [Configuration](#configuration)
- [Running the Application](#running-the-application)
- [Verifying Your Installation](#verifying-your-installation)
- [Troubleshooting](#troubleshooting)

## Prerequisites

### Required

| Software | Version | Purpose |
|----------|---------|---------|
| **Go** | 1.24+ | Backend runtime |
| **PostgreSQL** | 15+ | Primary database |
| **Git** | Latest | Version control |

### Optional (Recommended)

| Software | Version | Purpose |
|----------|---------|---------|
| **Redis** | 7+ | Response caching |
| **ClickHouse** | 24+ | Search acceleration |
| **Docker** | Latest | Containerized deployment |
| **PgBouncer** | 1.22+ | Connection pooling |

### System Requirements

| Component | Minimum | Recommended |
|-----------|---------|-------------|
| CPU | 2 cores | 4+ cores |
| RAM | 4 GB | 16+ GB |
| Disk | 20 GB SSD | 100+ GB NVMe |
| Network | 100 Mbps | 1 Gbps |

## Installation Methods

### Method 1: From Source (Recommended)

```bash
# Clone the repository
git clone https://github.com/Daveshvats/L.S.D.git
cd L.S.D

# Download dependencies
go mod download

# Verify build
go build -o bin/api ./cmd/api
```

### Method 2: Using Docker

```bash
# Build the image
docker build -t lsd-api:latest .

# Run with Docker Compose
docker-compose up -d
```

Create a `docker-compose.yml` file:

```yaml
version: '3.8'

services:
  api:
    build: .
    ports:
      - "5000:5000"
    environment:
      - DATABASE_URL=postgresql://postgres:password@postgres:5432/lsd
      - REDIS_ADDR=redis:6379
      - CLICKHOUSE_ADDR=clickhouse:9000
    depends_on:
      - postgres
      - redis
      - clickhouse

  postgres:
    image: postgres:15
    environment:
      - POSTGRES_DB=lsd
      - POSTGRES_PASSWORD=password
    volumes:
      - postgres_data:/var/lib/postgresql/data

  redis:
    image: redis:7-alpine
    volumes:
      - redis_data:/data

  clickhouse:
    image: clickhouse/clickhouse-server:latest
    volumes:
      - clickhouse_data:/var/lib/clickhouse

volumes:
  postgres_data:
  redis_data:
  clickhouse_data:
```

### Method 3: Using Replit

The repository includes `.replit` configuration for instant deployment on Replit:

1. Fork the repository to your Replit account
2. Set environment secrets in Replit
3. Click "Run" - the project will start automatically

## Database Setup

### PostgreSQL Configuration

L.S.D works with any existing PostgreSQL database. For optimal performance:

```sql
-- Recommended PostgreSQL settings (postgresql.conf)
shared_buffers = 256MB
work_mem = 64MB
maintenance_work_mem = 256MB
effective_cache_size = 768MB
random_page_cost = 1.1
effective_io_concurrency = 200
```

### Required Tables Setup

L.S.D requires authentication tables. Run the provided schema:

```bash
# Connect to your database
psql -h localhost -U postgres -d your_database

# Run the schema file
\i docs/database_schema.sql
```

This creates:

| Table | Purpose |
|-------|---------|
| `users` | User accounts and credentials |
| `sessions` | Active user sessions |
| `api_keys` | API key management for AI agents |

### Existing Database Integration

If connecting to an existing database:

1. **No schema changes required** - L.S.D only reads from your tables
2. **Primary keys required** - Each table must have a primary key
3. **Recommended columns** - Add `updated_at` for CDC support

```sql
-- Add updated_at column for CDC tracking (optional)
ALTER TABLE your_table
ADD COLUMN updated_at TIMESTAMP DEFAULT NOW();

-- Create trigger for auto-updating
CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_timestamp
BEFORE UPDATE ON your_table
FOR EACH ROW
EXECUTE FUNCTION update_timestamp();
```

### ClickHouse Setup (Optional)

For accelerated search, set up ClickHouse:

```sql
-- Create database
CREATE DATABASE IF NOT EXISTS lsd_search;

-- Tables are created automatically by CDC pipeline
-- Or create manually for specific configurations:

CREATE TABLE lsd_search.search_index (
    id String,
    table_name String,
    search_text String,
    updated_at DateTime,
    is_deleted UInt8 DEFAULT 0,
    INDEX ngram_idx search_text TYPE ngrambf_v1(4, 65536, 2, 0) GRANULARITY 1
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (table_name, id);
```

### Redis Setup (Optional)

Redis requires no special configuration:

```bash
# Install Redis
sudo apt-get install redis-server  # Ubuntu/Debian
brew install redis                  # macOS

# Start Redis
redis-server --daemonize yes --port 6379

# Verify connection
redis-cli ping  # Should return PONG
```

## Configuration

### Environment Variables

Copy the example configuration:

```bash
cp .env.example .env
```

Edit `.env` with your settings:

```bash
# ═══════════════════════════════════════════════════════════
# REQUIRED: PostgreSQL Connection
# ═══════════════════════════════════════════════════════════
DATABASE_URL=postgresql://username:password@host:5432/database_name

# ═══════════════════════════════════════════════════════════
# Server Configuration
# ═══════════════════════════════════════════════════════════
PORT=5000
SESSION_SECRET=your-secure-random-string-at-least-32-characters

# ═══════════════════════════════════════════════════════════
# Redis Cache (Optional)
# ═══════════════════════════════════════════════════════════
REDIS_ADDR=localhost:6379
CACHE_TTL_SECONDS=30

# ═══════════════════════════════════════════════════════════
# Rate Limiting
# ═══════════════════════════════════════════════════════════
RATE_LIMIT_RPS=100

# ═══════════════════════════════════════════════════════════
# ClickHouse (Optional - Enables Search Acceleration)
# ═══════════════════════════════════════════════════════════
CLICKHOUSE_ADDR=localhost:9000
CLICKHOUSE_DATABASE=default
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=

# ═══════════════════════════════════════════════════════════
# CDC Pipeline Configuration
# ═══════════════════════════════════════════════════════════
CDC_SYNC_INTERVAL_SECONDS=30
CDC_BATCH_SIZE=25000
```

### Configuration Priority

Settings are loaded in this order (later overrides earlier):

1. Default values in code
2. `.env` file
3. Environment variables
4. Command-line flags (if applicable)

## Running the Application

### Development Mode

```bash
# Start Redis first (if using caching)
redis-server --daemonize yes --port 6379

# Run with hot reload (using air)
air

# Or run directly
go run ./cmd/api
```

### Production Mode

```bash
# Build the binary
go build -ldflags="-s -w" -o bin/api ./cmd/api

# Run the compiled binary
./bin/api
```

### Using systemd (Linux)

Create a service file `/etc/systemd/system/lsd-api.service`:

```ini
[Unit]
Description=L.S.D API Server
After=network.target postgresql.service redis.service

[Service]
Type=simple
User=lsd
WorkingDirectory=/opt/lsd
ExecStart=/opt/lsd/bin/api
Restart=on-failure
RestartSec=5
Environment=DATABASE_URL=postgresql://...
Environment=REDIS_ADDR=localhost:6379

[Install]
WantedBy=multi-user.target
```

Enable and start:

```bash
sudo systemctl enable lsd-api
sudo systemctl start lsd-api
sudo systemctl status lsd-api
```

## Verifying Your Installation

### Health Check

```bash
# Check API health
curl http://localhost:5000/api/health
```

Expected response:

```json
{
  "status": "healthy",
  "postgres": "connected",
  "redis": "connected",
  "clickhouse": "connected",
  "tables_discovered": 15
}
```

### List Discovered Tables

```bash
curl http://localhost:5000/api/tables
```

### Get Table Schema

```bash
curl http://localhost:5000/api/tables/users/schema
```

### Test Pagination

```bash
curl "http://localhost:5000/api/tables/users/records?limit=5"
```

### Test Search

```bash
curl "http://localhost:5000/api/tables/users/search?q=john"
```

### Access Web Dashboard

Open `http://localhost:5000` in your browser to access the built-in dashboard.

## Troubleshooting

### Common Issues

#### Database Connection Failed

```
Error: connection refused
```

**Solutions:**

1. Verify PostgreSQL is running: `pg_isready -h localhost`
2. Check `DATABASE_URL` format is correct
3. Ensure firewall allows connections on port 5432
4. Verify credentials are correct

```bash
# Test connection manually
psql "postgresql://username:password@host:5432/database"
```

#### Redis Connection Failed

```
Error: dial tcp [::1]:6379: connect: connection refused
```

**Solutions:**

1. Start Redis: `redis-server --daemonize yes`
2. Check Redis status: `redis-cli ping`
3. Verify `REDIS_ADDR` is correct
4. Disable Redis caching if not needed (API will work without it)

#### ClickHouse Connection Failed

```
Warning: ClickHouse unavailable, using PostgreSQL fallback
```

**This is not an error** - the API gracefully falls back to PostgreSQL for search. To enable ClickHouse:

1. Install and start ClickHouse
2. Configure `CLICKHOUSE_ADDR`
3. Restart the API

#### Schema Discovery Returns Empty

```
{"tables": []}
```

**Solutions:**

1. Verify database has user tables (not just system catalogs)
2. Check database user has `SELECT` permissions on `information_schema`
3. Run: `GRANT USAGE ON SCHEMA information_schema TO your_user;`

```sql
-- Check tables exist
SELECT table_name FROM information_schema.tables
WHERE table_schema NOT IN ('pg_catalog', 'information_schema');
```

#### Rate Limit Exceeded

```
HTTP 429: Too Many Requests
```

**Solutions:**

1. Wait and retry (limits reset every minute)
2. Increase `RATE_LIMIT_RPS` in configuration
3. Use authentication for higher limits (authenticated users: 1000 req/min)
4. Use API keys for highest limits (AI agents: 5000 req/min)

### Debug Mode

Enable verbose logging:

```bash
# Set log level
export LOG_LEVEL=debug
go run ./cmd/api
```

### Getting Help

1. **Check logs**: Application logs provide detailed error messages
2. **GitHub Issues**: [github.com/Daveshvats/L.S.D/issues](https://github.com/Daveshvats/L.S.D/issues)
3. **Documentation**: Review other docs in the `/docs` folder

---

**Next**: [Configuration Reference](configuration.md) | [Architecture Guide](architecture.md)
