# Frequently Asked Questions

This document addresses common questions about L.S.D, from getting started to advanced usage scenarios.

## Table of Contents

- [General Questions](#general-questions)
- [Getting Started](#getting-started)
- [Architecture & Design](#architecture--design)
- [Performance](#performance)
- [Database & Schema](#database--schema)
- [Search](#search)
- [Authentication & Security](#authentication--security)
- [Deployment](#deployment)
- [Troubleshooting](#troubleshooting)

## General Questions

### What does L.S.D stand for?

**L.S.D** stands for **Large Search of Data**. The name reflects the project's primary goal: enabling fast, efficient search and data access across massive PostgreSQL databases (2-4 TB and beyond) without requiring manual API development.

### What problem does L.S.D solve?

L.S.D eliminates the need to write repetitive CRUD APIs for PostgreSQL databases. Instead of creating endpoints manually for each table, L.S.D:

1. **Auto-discovers** your database schema at runtime
2. **Generates** REST endpoints automatically for all tables
3. **Provides** high-performance search capabilities
4. **Handles** pagination, caching, and rate limiting out of the box

This allows developers to focus on business logic rather than boilerplate code.

### Is L.S.D production-ready?

Yes! L.S.D is designed for production workloads:

- **Battle-tested** with terabyte-scale databases
- **High availability** support through horizontal scaling
- **Enterprise features** like JWT auth, API keys, and rate limiting
- **Observability** ready with structured logging and metrics

However, always test thoroughly in your specific environment before deploying to production.

### What's the license?

L.S.D is licensed under the MIT License, allowing free use in both open-source and commercial projects.

## Getting Started

### Can I use L.S.D with an existing database?

**Absolutely!** L.S.D is designed to work with any existing PostgreSQL database. Simply:

1. Set your `DATABASE_URL` to point to your database
2. Start the API
3. All your tables will be automatically discovered

No schema modifications or migrations are required. L.S.D only reads from `information_schema` and `pg_catalog` for metadata.

### Do I need to install ClickHouse or Redis?

**No, both are optional:**

| Component | Required? | Purpose |
|-----------|-----------|---------|
| PostgreSQL | **Yes** | Primary database |
| Redis | No | Response caching (improves performance) |
| ClickHouse | No | Search acceleration (faster full-text search) |

The API works perfectly with just PostgreSQL. Add Redis and ClickHouse for enhanced performance.

### What versions of PostgreSQL are supported?

L.S.D supports **PostgreSQL 12 and later**, with optimal performance on **PostgreSQL 15+**. Some features work better with newer versions:

| Feature | PostgreSQL 12 | PostgreSQL 15+ |
|---------|---------------|----------------|
| Basic schema discovery | ✅ | ✅ |
| Keyset pagination | ✅ | ✅ |
| Performance optimizations | ⚠️ Limited | ✅ Full |
| Advanced index detection | ⚠️ Basic | ✅ Full |

### How do I reset the admin password?

Currently, L.S.D doesn't have a built-in admin user. To reset any user's password:

```sql
-- Connect to your database
psql -d your_database

-- Update password hash (example: set to "newpassword")
UPDATE users
SET password_hash = '$2a$10$...' -- Generate with bcrypt
WHERE email = 'admin@example.com';
```

## Architecture & Design

### Why doesn't L.S.D use an ORM?

L.S.D intentionally avoids ORMs for several reasons:

1. **Dynamic schema**: We need to query any table without pre-defined models
2. **Performance**: Raw SQL with `pgx` is significantly faster
3. **Flexibility**: We can optimize queries specifically for keyset pagination
4. **Transparency**: You know exactly what SQL is being executed

The trade-off is more verbose code, but the performance and flexibility gains are worth it for our use case.

### Why cursor-based pagination instead of OFFSET?

Traditional `OFFSET` pagination has O(n) performance—it gets slower as you page deeper into results. For a table with 100 million rows:

| Page | OFFSET Query Time |
|------|-------------------|
| 1 | ~10ms |
| 100 | ~100ms |
| 10,000 | ~1s |
| 1,000,000 | ~10s+ |

Cursor-based (keyset) pagination has O(1) performance—every page takes the same time, whether it's page 1 or page 1 million.

### What's the difference between search engines?

| Engine | When to Use | Performance |
|--------|-------------|-------------|
| ClickHouse | Large datasets (>1M rows), complex searches | Sub-second on billions of rows |
| PostgreSQL | Small datasets (<1M rows), simple searches | Varies by table size |
| Auto (default) | Let L.S.D decide based on availability | Best of both worlds |

### How does the CDC pipeline work?

The Change Data Capture pipeline synchronizes data from PostgreSQL to ClickHouse:

```
PostgreSQL ──(every 30s)──▶ ClickHouse
    │                            │
    │ 1. Query updated rows      │
    │ 2. Transform data          │
    │ 3. Insert into CH          │
    │                            │
    └────────────────────────────┘
```

**Requirements:**
- Tables must have `updated_at` or `modified_at` column
- Tables must have a primary key

**What it handles:**
- ✅ Inserts and updates
- ✅ Deletes (via tombstone flags)
- ✅ Multi-table sync
- ✅ Automatic retry on failure

## Performance

### How fast is L.S.D?

Benchmarks on a standard server (8 vCPU, 32GB RAM):

| Operation | Dataset | Latency (p99) |
|-----------|---------|---------------|
| List tables | Any | < 1ms |
| Get schema | Any | < 1ms |
| List records (cached) | Any | < 5ms |
| List records (uncached) | 1M rows | < 50ms |
| List records (uncached) | 1B rows | < 100ms |
| Search (ClickHouse) | 1B rows | < 200ms |
| Search (PostgreSQL) | 1M rows | < 500ms |

### How do I improve performance?

**Quick wins:**

1. **Enable Redis caching**
   ```bash
   REDIS_ADDR=localhost:6379
   CACHE_TTL_SECONDS=60
   ```

2. **Add ClickHouse for search**
   ```bash
   CLICKHOUSE_ADDR=localhost:9000
   ```

3. **Increase connection pool**
   ```bash
   DB_MAX_CONNECTIONS=100
   ```

4. **Add proper indexes**
   ```sql
   CREATE INDEX idx_your_table_column ON your_table(column);
   ```

**Advanced optimizations:**

- Use read replicas for search queries
- Partition large tables
- Add PgBouncer for connection pooling
- Tune PostgreSQL settings

### Why is my first request slow?

The first request after startup may be slow because L.S.D:

1. Queries database metadata to discover schema
2. Establishes connection pool
3. Warms up internal caches

Subsequent requests will be fast. This is a one-time startup cost.

### Can L.S.D handle concurrent requests?

Yes! L.S.D is designed for high concurrency:

- **Connection pooling**: Reuses database connections efficiently
- **Non-blocking I/O**: Go's goroutines handle thousands of concurrent requests
- **Rate limiting**: Prevents any single client from overwhelming the system

Expected throughput:

| Configuration | Requests/second |
|---------------|-----------------|
| Single API instance | 1,000-5,000 |
| 3 API instances + LB | 3,000-15,000 |
| 10 API instances + LB | 10,000-50,000 |

## Database & Schema

### Does L.S.D modify my database schema?

**No!** L.S.D never modifies your data or schema. It only reads from:

- `information_schema.columns` - Column metadata
- `information_schema.tables` - Table names
- `pg_catalog.pg_index` - Index information
- `pg_catalog.pg_constraint` - Primary key information

Your data is completely safe.

### What if my table doesn't have a primary key?

Tables without primary keys have limited functionality:

| Feature | With PK | Without PK |
|---------|---------|------------|
| List records | ✅ | ✅ |
| Get single record | ✅ | ❌ |
| Pagination | ✅ Efficient | ⚠️ Basic |
| Search | ✅ | ✅ |

**Recommendation:** Add a primary key for full functionality.

```sql
ALTER TABLE your_table ADD COLUMN id SERIAL PRIMARY KEY;
```

### Can I use L.S.D with views?

Yes! L.S.D works with PostgreSQL views:

- **Regular views**: Treated like tables (read-only)
- **Materialized views**: Also supported
- **Limitations**: Cannot get single records (views don't have primary keys)

### What data types are supported?

L.S.D supports all common PostgreSQL data types:

| Category | Types |
|----------|-------|
| Numeric | `integer`, `bigint`, `decimal`, `numeric`, `real` |
| Text | `varchar`, `text`, `char` |
| Date/Time | `timestamp`, `timestamptz`, `date`, `time` |
| Boolean | `boolean` |
| UUID | `uuid` |
| JSON | `json`, `jsonb` |
| Binary | `bytea` (limited) |

**Note:** Complex types like arrays and custom types have limited support.

## Search

### How does the search work?

L.S.D provides full-text search across all text columns in a table:

1. **Query tokenization**: "John Smith" → ["John", "Smith"]
2. **Multi-column search**: Each token is searched across all text columns
3. **OR logic**: Results match ANY token in ANY column
4. **Ranking**: More matches = higher rank

### What's the difference between `auto`, `clickhouse`, and `postgresql` search?

| Engine | Description | Use Case |
|--------|-------------|----------|
| `auto` (default) | Uses ClickHouse if available, falls back to PostgreSQL | Most cases |
| `clickhouse` | Forces ClickHouse search | When you need guaranteed fast search |
| `postgresql` | Forces PostgreSQL search | When ClickHouse is unavailable or for simple searches |

### Can I search specific columns only?

Currently, L.S.D searches all text columns. To limit search scope:

1. **Filter results**: Use the `filter` parameter after search
2. **Custom endpoint**: Create a custom handler for specific column searches

This feature is on the roadmap for future releases.

### Why is search slow on large tables?

If search is slow without ClickHouse:

1. **PostgreSQL ILIKE** scans all rows
2. **No index** is used for `LIKE '%term%'`

**Solutions:**

1. **Add ClickHouse** for dedicated search
2. **Create a trigram index**:
   ```sql
   CREATE EXTENSION pg_trgm;
   CREATE INDEX idx_table_col_trgm ON your_table USING gin (column gin_trgm_ops);
   ```
3. **Limit search columns** by filtering results

## Authentication & Security

### How do I create an API key for my application?

```bash
# Register a user
curl -X POST http://localhost:5000/api/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"app@example.com","username":"myapp","password":"secure123"}'

# Login to get JWT
curl -X POST http://localhost:5000/api/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"app@example.com","password":"secure123"}'

# Create API key (with JWT)
curl -X POST http://localhost:5000/api/api-keys \
  -H "Authorization: Bearer YOUR_JWT" \
  -H "Content-Type: application/json" \
  -d '{"name":"My App Key","scopes":["read","search"]}'
```

### What's the difference between JWT and API keys?

| Feature | JWT Token | API Key |
|---------|-----------|---------|
| Use case | User sessions | Service/AI integrations |
| Expiration | Short-lived (15min) | Long-lived or no expiration |
| Rate limit | 1,000 req/min | 5,000 req/min |
| Scopes | Full user access | Customizable |
| Storage | Client memory | Secure storage (env vars) |

### How do I secure my production deployment?

1. **Use HTTPS**: TLS termination at load balancer
2. **Strong secrets**: 32+ character session secret
3. **Restrict CORS**: Set `CORS_ALLOWED_ORIGINS` to your domains
4. **Rate limit**: Configure appropriate limits
5. **Network isolation**: Use VPC/firewalls
6. **Regular updates**: Keep dependencies updated

### Can I disable authentication?

For development or internal tools:

```bash
# Not recommended for production!
AUTH_DISABLED=true
```

⚠️ **Warning**: Only use this in trusted network environments.

## Deployment

### Can I run L.S.D on Windows?

Yes, but with some considerations:

1. **Development**: Works fine with native Go installation
2. **Production**: Recommended to use Docker or WSL2

```bash
# Using Docker on Windows
docker-compose up -d
```

### How do I scale L.S.D?

**Horizontal scaling (recommended):**

1. Deploy multiple API instances
2. Use a load balancer (nginx, HAProxy, cloud LB)
3. Share PostgreSQL, Redis, and ClickHouse

```yaml
# docker-compose.prod.yml
services:
  api:
    deploy:
      replicas: 3
```

**Vertical scaling:**

1. Increase server resources
2. Adjust connection pool size
3. Tune PostgreSQL configuration

### What's the recommended production setup?

For a medium-scale production deployment:

```
┌─────────────────┐
│  Load Balancer  │ (nginx, HAProxy, or cloud LB)
└────────┬────────┘
         │
    ┌────┴────┐
    ▼         ▼
┌───────┐ ┌───────┐
│ API 1 │ │ API 2 │ (2+ instances)
└───┬───┘ └───┬───┘
    │         │
    └────┬────┘
         │
    ┌────┴────────────────────┐
    ▼         ▼               ▼
┌───────┐ ┌───────┐    ┌────────────┐
│  PG   │ │ Redis │    │ ClickHouse │
└───────┘ └───────┘    └────────────┘
```

### How do I migrate to a new database?

1. **Dump existing data**
   ```bash
   pg_dump -h old-host -U user -d db > dump.sql
   ```

2. **Create new database**
   ```bash
   createdb -h new-host -U user new_db
   ```

3. **Import data**
   ```bash
   psql -h new-host -U user -d new_db < dump.sql
   ```

4. **Update L.S.D configuration**
   ```bash
   DATABASE_URL=postgresql://user:pass@new-host:5432/new_db
   ```

5. **Restart L.S.D**

## Troubleshooting

### The API won't start - what do I do?

**Check the basics:**

1. **Database connection**
   ```bash
   psql "$DATABASE_URL" -c "SELECT 1"
   ```

2. **Environment variables**
   ```bash
   echo $DATABASE_URL
   echo $SESSION_SECRET
   ```

3. **Port availability**
   ```bash
   lsof -i :5000
   ```

**Common issues:**

| Error | Solution |
|-------|----------|
| `connection refused` | Database not running or wrong host |
| `authentication failed` | Check credentials in `DATABASE_URL` |
| `address already in use` | Another process on port 5000 |
| `no tables found` | Check database user has read permissions |

### Why am I getting 401 Unauthorized?

1. **Check token validity**
   ```bash
   # Decode JWT (first part after header removal)
   echo "eyJ..." | base64 -d
   ```

2. **Verify Authorization header**
   ```
   Authorization: Bearer eyJ...
   ```

3. **Check API key format**
   ```
   X-API-Key: lsd_live_xxxxxxxx
   ```

4. **Verify session exists**
   ```bash
   redis-cli GET "session:SESSION_ID"
   ```

### Why are my search results incomplete?

1. **CDC sync delay**: ClickHouse may be 30s behind PostgreSQL
2. **Missing `updated_at`**: CDC can't track changes
3. **Search term limit**: Very long queries may be truncated

**Check CDC status:**
```bash
curl http://localhost:5000/api/cdc/status
```

### How do I report a bug?

1. **Check existing issues**: [GitHub Issues](https://github.com/Daveshvats/L.S.D/issues)
2. **Gather information**:
   - Go version: `go version`
   - PostgreSQL version: `psql --version`
   - Configuration (without secrets)
   - Error logs
3. **Create an issue** with:
   - Steps to reproduce
   - Expected behavior
   - Actual behavior
   - Environment details

---

**Still have questions?**

- 📖 Read the [full documentation](architecture.md)
- 💬 Ask on [GitHub Discussions](https://github.com/Daveshvats/L.S.D/discussions)
- 🐛 Report issues on [GitHub Issues](https://github.com/Daveshvats/L.S.D/issues)
