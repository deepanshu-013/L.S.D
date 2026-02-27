# Development Guide

Welcome to the L.S.D development guide! This document covers everything you need to contribute to the project, from setting up your development environment to understanding coding standards and submitting pull requests.

## Table of Contents

- [Development Setup](#development-setup)
- [Project Structure](#project-structure)
- [Coding Standards](#coding-standards)
- [Testing](#testing)
- [Adding New Features](#adding-new-features)
- [API Development](#api-development)
- [Database Migrations](#database-migrations)
- [Debugging](#debugging)
- [Contributing](#contributing)

## Development Setup

### Prerequisites

Ensure you have these tools installed:

| Tool | Version | Installation |
|------|---------|--------------|
| Go | 1.24+ | [go.dev/dl](https://go.dev/dl/) |
| Docker | Latest | [docker.com](https://docker.com) |
| golangci-lint | Latest | `go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest` |
| air | Latest | `go install github.com/cosmtrek/air@latest` |

### Clone and Setup

```bash
# Clone your fork
git clone https://github.com/YOUR_USERNAME/L.S.D.git
cd L.S.D

# Add upstream remote
git remote add upstream https://github.com/Daveshvats/L.S.D.git

# Install dependencies
go mod download

# Install development tools
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
go install github.com/cosmtrek/air@latest
```

### Development Environment

Create a `.env` file for local development:

```bash
# .env.local (don't commit this!)
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/lsd_dev
REDIS_ADDR=localhost:6379
CLICKHOUSE_ADDR=localhost:9000
PORT=5000
SESSION_SECRET=dev-secret-key-do-not-use-in-production
LOG_LEVEL=debug
LOG_FORMAT=text
```

### Start Development Services

Using Docker Compose for local dependencies:

```bash
# Start PostgreSQL, Redis, and ClickHouse
docker-compose -f docker-compose.dev.yml up -d

# Or run services individually
docker run -d --name postgres -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres:15
docker run -d --name redis -p 6379:6379 redis:7-alpine
docker run -d --name clickhouse -p 9000:9000 clickhouse/clickhouse-server:latest
```

### Run in Development Mode

```bash
# Using air for hot reload
air

# Or run directly
go run ./cmd/api
```

## Project Structure

```
L.S.D/
├── cmd/
│   └── api/
│       └── main.go              # Application entry point
├── internal/
│   ├── auth/                    # Authentication logic
│   │   ├── jwt.go               # JWT token handling
│   │   ├── api_key.go           # API key validation
│   │   └── middleware.go        # Auth middleware
│   ├── cache/                   # Caching layer
│   │   └── redis.go             # Redis implementation
│   ├── clickhouse/              # ClickHouse integration
│   │   ├── connection.go        # Connection management
│   │   ├── search.go            # Search queries
│   │   └── cdc.go               # CDC operations
│   ├── config/                  # Configuration
│   │   └── config.go            # Config loading
│   ├── database/                # Database operations
│   │   ├── pool.go              # Connection pool
│   │   └── repository.go        # Query repository
│   ├── handlers/                # HTTP handlers
│   │   ├── dynamic.go           # Dynamic REST handlers
│   │   ├── auth.go              # Auth endpoints
│   │   └── bot.go               # Bot webhook handlers
│   ├── middleware/              # HTTP middleware
│   │   ├── rate_limit.go        # Rate limiting
│   │   ├── logging.go           # Request logging
│   │   └── cors.go              # CORS handling
│   ├── models/                  # Data models
│   │   └── models.go            # Struct definitions
│   ├── pagination/              # Cursor pagination
│   │   ├── cursor.go            # Cursor encoding/decoding
│   │   └── encoder.go           # Type-aware encoding
│   ├── pipeline/                # CDC pipeline
│   │   └── cdc.go               # Pipeline orchestration
│   ├── schema/                  # Schema discovery
│   │   ├── loader.go            # Schema loading
│   │   └── query_builder.go     # Dynamic query building
│   ├── services/                # Business logic
│   │   └── record_service.go    # Record operations
│   └── utils/                   # Utilities
│       └── utils.go             # Helper functions
├── web/                         # Frontend assets
│   ├── index.html               # Main page
│   ├── dashboard.html           # Dashboard
│   ├── login.html               # Login page
│   ├── register.html            # Registration
│   ├── app.js                   # Frontend JavaScript
│   ├── style.css                # Styles
│   └── swagger.yaml             # OpenAPI spec
├── docs/                        # Documentation
│   ├── database_schema.sql      # Auth tables schema
│   └── env.example              # Environment template
├── go.mod                       # Go module definition
├── go.sum                       # Dependency checksums
├── .gitignore                   # Git ignore rules
├── .env.example                 # Environment template
└── README.md                    # Project README
```

## Coding Standards

### Go Style Guide

Follow the [Effective Go](https://go.dev/doc/effective_go) guidelines and these project-specific rules:

#### Naming Conventions

```go
// Good: Descriptive, exported names
type TableSchema struct {
    Name       string
    Columns    []Column
    PrimaryKey []string
}

// Good: Unexported for internal use
type queryBuilder struct {
    schema *TableSchema
}

// Good: Interface with -er suffix
type Searcher interface {
    Search(table, query string) ([]Record, error)
}
```

#### Error Handling

```go
// Good: Wrap errors with context
func (s *Service) GetRecord(ctx context.Context, table, id string) (*Record, error) {
    record, err := s.repo.FindByID(ctx, table, id)
    if err != nil {
        return nil, fmt.Errorf("failed to get record from %s: %w", table, err)
    }
    return record, nil
}

// Good: Check errors immediately
if err := db.Ping(ctx); err != nil {
    log.Fatalf("database connection failed: %v", err)
}
```

#### Context Usage

```go
// Good: Always pass context as first parameter
func (r *Repository) Query(ctx context.Context, query string, args ...any) (Rows, error) {
    return r.db.Query(ctx, query, args...)
}

// Good: Use context for timeouts
ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
result, err := service.Query(ctx, query)
```

### Code Organization

```go
// internal/handlers/dynamic.go

package handlers

import (
    "context"
    "net/http"

    "github.com/go-chi/chi/v5"
)

// Handler holds dependencies for HTTP handlers
type Handler struct {
    db         Database
    cache      Cache
    schema     SchemaLoader
    clickhouse ClickHouseClient
}

// NewHandler creates a new handler instance
func NewHandler(db Database, cache Cache, schema SchemaLoader, ch ClickHouseClient) *Handler {
    return &Handler{
        db:         db,
        cache:      cache,
        schema:     schema,
        clickhouse: ch,
    }
}

// ListTables handles GET /api/tables
func (h *Handler) ListTables(w http.ResponseWriter, r *http.Request) {
    // Implementation
}

// ListRecords handles GET /api/tables/{table}/records
func (h *Handler) ListRecords(w http.ResponseWriter, r *http.Request) {
    // Implementation
}
```

### Comments and Documentation

```go
// SchemaLoader discovers and caches database schema information.
// It queries PostgreSQL metadata tables to auto-discover tables,
// columns, primary keys, and indexes at runtime.
//
// Example usage:
//
//     loader := schema.NewLoader(dbPool)
//     tables, err := loader.Discover()
//     if err != nil {
//         log.Fatal(err)
//     }
type SchemaLoader struct {
    db    *pgxpool.Pool
    cache map[string]*TableSchema
}

// Discover queries the database metadata and returns all user tables
// with their column definitions, primary keys, and indexed columns.
// The results are cached in memory for subsequent lookups.
func (l *SchemaLoader) Discover() (map[string]*TableSchema, error) {
    // Implementation
}
```

## Testing

### Running Tests

```bash
# Run all tests
go test ./...

# Run with coverage
go test -cover ./...

# Run specific package
go test ./internal/handlers/...

# Run with verbose output
go test -v ./...

# Run integration tests (requires Docker)
go test -tags=integration ./...
```

### Writing Unit Tests

```go
// internal/schema/loader_test.go

package schema

import (
    "context"
    "testing"

    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

func TestLoader_Discover(t *testing.T) {
    // Setup
    db := setupTestDB(t)
    defer db.Close()

    loader := NewLoader(db)

    // Execute
    tables, err := loader.Discover()

    // Assert
    require.NoError(t, err)
    assert.NotEmpty(t, tables)

    // Verify specific table
    usersTable, exists := tables["users"]
    assert.True(t, exists)
    assert.NotEmpty(t, usersTable.Columns)
    assert.NotEmpty(t, usersTable.PrimaryKey)
}

func TestLoader_GetTableSchema(t *testing.T) {
    t.Run("existing table", func(t *testing.T) {
        // Test case
    })

    t.Run("non-existent table", func(t *testing.T) {
        // Test case
    })
}
```

### Integration Tests

```go
// internal/handlers/dynamic_test.go

//go:build integration

package handlers

import (
    "net/http"
    "net/http/httptest"
    "testing"
)

func TestHandler_ListRecords_Integration(t *testing.T) {
    // Setup full stack
    handler := setupIntegrationHandler(t)
    server := httptest.NewServer(handler)
    defer server.Close()

    // Make request
    resp, err := http.Get(server.URL + "/api/tables/users/records?limit=10")
    require.NoError(t, err)
    defer resp.Body.Close()

    // Assert
    assert.Equal(t, http.StatusOK, resp.StatusCode)
}
```

### Mocking

```go
// internal/services/record_service_test.go

package services

// MockRepository implements Repository interface for testing
type MockRepository struct {
    FindByIDFunc func(ctx context.Context, table, id string) (*Record, error)
    ListFunc     func(ctx context.Context, table string, opts ListOptions) ([]Record, error)
}

func (m *MockRepository) FindByID(ctx context.Context, table, id string) (*Record, error) {
    return m.FindByIDFunc(ctx, table, id)
}

func (m *MockRepository) List(ctx context.Context, table string, opts ListOptions) ([]Record, error) {
    return m.ListFunc(ctx, table, opts)
}

func TestRecordService_Get(t *testing.T) {
    mockRepo := &MockRepository{
        FindByIDFunc: func(ctx context.Context, table, id string) (*Record, error) {
            return &Record{ID: id, Data: map[string]any{"name": "test"}}, nil
        },
    }

    service := NewRecordService(mockRepo)

    // Test the service
    record, err := service.Get(context.Background(), "users", "123")

    assert.NoError(t, err)
    assert.Equal(t, "123", record.ID)
}
```

## Adding New Features

### Adding a New API Endpoint

1. **Add the handler** in `internal/handlers/`:

```go
// internal/handlers/custom.go

func (h *Handler) CustomEndpoint(w http.ResponseWriter, r *http.Request) {
    // 1. Parse request
    var req CustomRequest
    if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
        respondError(w, http.StatusBadRequest, "invalid request")
        return
    }

    // 2. Validate input
    if err := validateCustomRequest(&req); err != nil {
        respondError(w, http.StatusBadRequest, err.Error())
        return
    }

    // 3. Call service
    result, err := h.service.CustomOperation(r.Context(), &req)
    if err != nil {
        respondError(w, http.StatusInternalServerError, err.Error())
        return
    }

    // 4. Respond
    respondJSON(w, http.StatusOK, result)
}
```

2. **Register the route** in `main.go`:

```go
r.Route("/api/custom", func(r chi.Router) {
    r.Use(middleware.Auth)
    r.Post("/", h.CustomEndpoint)
})
```

3. **Add tests**:

```go
func TestHandler_CustomEndpoint(t *testing.T) {
    // Test cases
}
```

4. **Update OpenAPI spec** in `web/swagger.yaml`:

```yaml
/api/custom:
  post:
    summary: Custom endpoint
    tags:
      - Custom
    requestBody:
      required: true
      content:
        application/json:
          schema:
            $ref: '#/components/schemas/CustomRequest'
    responses:
      '200':
        description: Success
```

### Adding a New Internal Package

1. Create the package directory under `internal/`
2. Define interfaces and types in `models.go`
3. Implement the core logic
4. Add comprehensive tests
5. Wire up dependencies in `main.go`

## API Development

### Query Building Guidelines

```go
// Always use parameterized queries
func (qb *QueryBuilder) BuildSelect(table string, cursor *Cursor, limit int) string {
    query := fmt.Sprintf(`
        SELECT %s
        FROM %s
        WHERE %s
        ORDER BY %s
        LIMIT %d
    `,
        strings.Join(qb.columns, ", "),
        table,
        qb.buildWhereClause(cursor),
        qb.buildOrderBy(),
        limit+1, // Fetch one extra for pagination
    )
    return query
}

// Never concatenate user input into queries
// BAD:
// query := fmt.Sprintf("SELECT * FROM %s WHERE id = %s", table, userInput)

// GOOD:
// query := "SELECT * FROM $1 WHERE id = $2"
// db.Query(ctx, query, table, userInput)
```

### Response Helpers

```go
// Use consistent response formats
func respondJSON(w http.ResponseWriter, status int, data interface{}) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(status)
    json.NewEncoder(w).Encode(data)
}

func respondError(w http.ResponseWriter, status int, message string) {
    respondJSON(w, status, ErrorResponse{
        Error:   http.StatusText(status),
        Message: message,
    })
}

func respondPaginated(w http.ResponseWriter, records []Record, nextCursor string) {
    respondJSON(w, http.StatusOK, PaginatedResponse{
        Data:       records,
        NextCursor: nextCursor,
        Limit:      len(records),
    })
}
```

## Database Migrations

### Creating Migrations

For auth system changes, create migration files:

```sql
-- docs/migrations/001_add_user_roles.sql

-- Add role column to users
ALTER TABLE users ADD COLUMN IF NOT EXISTS role VARCHAR(20) DEFAULT 'user';

-- Add constraint
ALTER TABLE users ADD CONSTRAINT valid_role
CHECK (role IN ('user', 'admin', 'service'));

-- Create index
CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);
```

### Running Migrations

```bash
# Using psql
psql -f docs/migrations/001_add_user_roles.sql

# Or use a migration tool like golang-migrate
migrate -path docs/migrations -database $DATABASE_URL up
```

## Debugging

### Enable Debug Logging

```bash
export LOG_LEVEL=debug
export LOG_FORMAT=text
go run ./cmd/api
```

### Using Delve Debugger

```bash
# Install delve
go install github.com/go-delve/delve/cmd/dlv@latest

# Debug
dlv debug ./cmd/api
```

### Common Debug Patterns

```go
// Debug query building
func (qb *QueryBuilder) BuildSelect(table string, cursor *Cursor, limit int) string {
    query := qb.buildSelectQuery(table, cursor, limit)
    log.Debugf("Built query for table %s: %s", table, query)
    return query
}

// Debug cache hits/misses
func (c *Cache) Get(ctx context.Context, key string) ([]byte, bool) {
    val, err := c.client.Get(ctx, key).Bytes()
    if err != nil {
        log.Debugf("Cache miss for key: %s", key)
        return nil, false
    }
    log.Debugf("Cache hit for key: %s", key)
    return val, true
}
```

## Contributing

### Pull Request Process

1. **Fork and branch**: Create a feature branch from `main`
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make changes**: Follow coding standards

3. **Write tests**: Maintain or improve coverage

4. **Run linting**:
   ```bash
   golangci-lint run
   ```

5. **Run tests**:
   ```bash
   go test ./...
   ```

6. **Commit**: Use conventional commit messages
   ```
   feat: add support for composite foreign keys
   fix: resolve cursor validation for multi-column PKs
   docs: update API documentation for search endpoint
   ```

7. **Push and PR**:
   ```bash
   git push origin feature/your-feature-name
   ```

8. **Review**: Address review feedback

### Commit Message Format

Follow [Conventional Commits](https://conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `test`, `chore`

### Code Review Checklist

- [ ] Code follows project style guide
- [ ] All tests pass
- [ ] New features have tests
- [ ] Documentation updated
- [ ] No security vulnerabilities
- [ ] No performance regressions
- [ ] Backward compatible (or documented breaking changes)

---

**Next**: [Deployment Guide](deployment.md) | [FAQ](faq.md)
