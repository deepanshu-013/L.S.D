# System Architecture Diagrams

This document contains all Mermaid diagrams for the L.S.D project.

## 1. System Architecture Overview

```mermaid
graph TB
    subgraph "Client Layer"
        WEB[🌐 Web Dashboard]
        TG[📱 Telegram Bot]
        WA[📱 WhatsApp Bot]
        API[🔌 External APIs]
        AGENT[🤖 AI Agents]
    end

    subgraph "API Gateway Layer"
        AUTH[🔐 Authentication<br/>JWT + API Keys]
        RATE[⏱️ Rate Limiter<br/>100-5000 req/min]
        CORS[🔄 CORS Handler]
    end

    subgraph "Application Layer"
        subgraph "Core Services"
            SCHEMA[📋 Schema Discovery]
            HANDLERS[🎯 Dynamic Handlers]
            SEARCH[🔍 Search Engine]
        end
        subgraph "Pipeline Services"
            CDC[🔄 CDC Pipeline]
            SYNC[📊 Data Sync]
        end
    end

    subgraph "Caching Layer"
        REDIS[⚡ Redis Cache<br/>30s TTL]
    end

    subgraph "Data Layer"
        PG[🐘 PostgreSQL<br/>Primary Database]
        CH[🏭 ClickHouse<br/>Search Index]
    end

    WEB --> AUTH
    TG --> AUTH
    WA --> AUTH
    API --> AUTH
    AGENT --> AUTH

    AUTH --> RATE
    RATE --> CORS
    CORS --> HANDLERS

    HANDLERS --> SCHEMA
    HANDLERS --> SEARCH
    HANDLERS --> REDIS

    SCHEMA --> PG
    SEARCH --> CH
    SEARCH -.->|Fallback| PG
    CDC --> PG
    CDC --> CH
    SYNC --> CDC

    REDIS -.->|Cache Miss| PG

    style PG fill:#336791,color:#fff
    style CH fill:#ffcc00,color:#000
    style REDIS fill:#dc382d,color:#fff
    style AUTH fill:#4a5568,color:#fff
```

## 2. Data Flow Diagram

```mermaid
flowchart LR
    subgraph Request
        A[Client Request] --> B{Authenticated?}
        B -->|No| C[401 Unauthorized]
        B -->|Yes| D{Rate Limited?}
        D -->|Yes| E[429 Too Many Requests]
        D -->|No| F[Process Request]
    end

    subgraph Processing
        F --> G{Cache Hit?}
        G -->|Yes| H[Return Cached Response]
        G -->|No| I[Query Database]
        I --> J{ClickHouse Available?}
        J -->|Search Query| K[ClickHouse Search]
        J -->|Regular Query| L[PostgreSQL Query]
        J -->|No| L
        K --> M[Merge Results]
        L --> M
        M --> N[Cache Response]
        N --> O[Return Response]
    end

    style K fill:#ffcc00,color:#000
    style L fill:#336791,color:#fff
    style H fill:#dc382d,color:#fff
```

## 3. API Request Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant GW as API Gateway
    participant Auth as Auth Service
    participant H as Handler
    participant Cache as Redis Cache
    participant PG as PostgreSQL
    participant CH as ClickHouse

    C->>GW: HTTP Request + Auth Header
    GW->>Auth: Validate Token/API Key

    alt Invalid Auth
        Auth-->>C: 401 Unauthorized
    end

    Auth->>H: Forward Request

    alt Cache Hit
        H->>Cache: Check Cache
        Cache-->>H: Cached Data
        H-->>C: 200 OK (Cached)
    else Cache Miss
        H->>Cache: Check Cache
        Cache-->>H: Miss

        alt Search Request
            H->>CH: Search Query
            CH-->>H: Search Results
        else Regular Request
            H->>PG: SQL Query
            PG-->>H: Data
        end

        H->>Cache: Store in Cache
        H-->>C: 200 OK (Fresh)
    end
```

## 4. CDC Pipeline Flow

```mermaid
flowchart TB
    subgraph "PostgreSQL Source"
        PG_TABLE[(Table Data)]
        PG_TRIGGER[📝 Updated At<br/>Trigger]
    end

    subgraph "CDC Pipeline"
        SCHEDULER[⏰ Scheduler<br/>30s Interval]
        EXTRACT[📤 Extract Changes]
        TRANSFORM[🔄 Transform Data]
        LOAD[📥 Load to ClickHouse]
    end

    subgraph "ClickHouse Target"
        CH_TABLE[(ReplacingMergeTree)]
        TOMBSTONE[💀 Tombstone<br/>is_deleted flag]
    end

    PG_TABLE --> PG_TRIGGER
    PG_TRIGGER --> |updated_at| SCHEDULER
    SCHEDULER --> EXTRACT
    EXTRACT --> |Delta Records| TRANSFORM
    TRANSFORM --> LOAD
    LOAD --> CH_TABLE
    LOAD --> TOMBSTONE

    style PG_TABLE fill:#336791,color:#fff
    style CH_TABLE fill:#ffcc00,color:#000
    style SCHEDULER fill:#4a5568,color:#fff
```

## 5. Authentication Flow

```mermaid
sequenceDiagram
    participant U as User
    participant API as API Server
    participant DB as PostgreSQL
    participant Redis as Redis

    Note over U,Redis: Registration Flow
    U->>API: POST /api/auth/register
    API->>API: Hash Password
    API->>DB: Create User
    API->>API: Generate Verification Token
    API-->>U: 201 Created + Email

    Note over U,Redis: Login Flow
    U->>API: POST /api/auth/login
    API->>DB: Find User
    API->>API: Verify Password
    API->>API: Generate JWT + Refresh Token
    API->>Redis: Store Session
    API-->>U: 200 OK + Tokens

    Note over U,Redis: Token Refresh Flow
    U->>API: POST /api/auth/refresh
    API->>Redis: Validate Refresh Token
    API->>API: Generate New Access Token
    API-->>U: 200 OK + New Token
```

## 6. Search Architecture

```mermaid
graph TB
    subgraph "Search Request"
        Q[Search Query]
        TOK[Tokenize Query]
        MULTI[Multi-Token Search]
    end

    subgraph "Search Engines"
        subgraph "ClickHouse Primary"
            NGRAM[Ngram Index]
            FUZZY[Fuzzy Matching]
            RANK[Result Ranking]
        end
        subgraph "PostgreSQL Fallback"
            ILIKE[ILIKE Query]
            SCAN[Full Table Scan]
        end
    end

    subgraph "Result Processing"
        MERGE[Merge Results]
        VALID[Validate Cursor]
        PAG[Apply Pagination]
        RESP[Return Response]
    end

    Q --> TOK
    TOK --> MULTI
    MULTI --> NGRAM
    NGRAM --> FUZZY
    FUZZY --> RANK
    RANK --> MERGE

    NGRAM -.->|Unavailable| ILIKE
    ILIKE --> SCAN
    SCAN --> MERGE

    MERGE --> VALID
    VALID --> PAG
    PAG --> RESP

    style NGRAM fill:#ffcc00,color:#000
    style ILIKE fill:#336791,color:#fff
```

## 7. Schema Discovery Process

```mermaid
flowchart TB
    START[🚀 Server Startup]
    CONNECT[📡 Connect to PostgreSQL]

    subgraph "Discovery Process"
        TABLES[📋 Discover Tables]
        COLUMNS[📝 Discover Columns]
        PK[🔑 Identify Primary Keys]
        IDX[📊 Find Indexed Columns]
        TEXT[🔤 Identify Text Columns]
    end

    subgraph "Schema Cache"
        CACHE[💾 In-Memory Schema]
        VALID[✅ Validation Rules]
    end

    START --> CONNECT
    CONNECT --> TABLES
    TABLES --> COLUMNS
    COLUMNS --> PK
    PK --> IDX
    IDX --> TEXT
    TEXT --> CACHE
    CACHE --> VALID

    style START fill:#4a5568,color:#fff
    style CACHE fill:#dc382d,color:#fff
```

## 8. Cursor Pagination Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant H as Handler
    participant QB as Query Builder
    participant DB as Database

    C->>H: GET /records?limit=10

    Note over H: No cursor - First page
    H->>QB: Build query (no cursor)
    QB->>DB: SELECT ... ORDER BY pk LIMIT 11
    DB-->>QB: 11 rows
    QB->>QB: Encode cursor from row 10
    QB-->>H: 10 rows + next_cursor
    H-->>C: Response with cursor

    Note over C: Next page request
    C->>H: GET /records?cursor=xxx&limit=10
    H->>QB: Decode cursor, build query
    QB->>DB: SELECT ... WHERE pk > cursor_value ORDER BY pk LIMIT 11
    DB-->>QB: 11 rows
    QB->>QB: Encode new cursor
    QB-->>H: 10 rows + next_cursor
    H-->>C: Response with cursor
```

## 9. Rate Limiting Architecture

```mermaid
flowchart LR
    subgraph "Request Processing"
        REQ[Incoming Request]
        IP[Extract Client IP]
        KEY[Generate Rate Key]
    end

    subgraph "Rate Limiter"
        CHECK{Check Rate}
        COUNT[Request Count]
        WINDOW[Sliding Window]
    end

    subgraph "Actions"
        ALLOW[✅ Allow Request]
        DENY[❌ 429 Response]
        HEADERS[Add Rate Headers]
    end

    REQ --> IP
    IP --> KEY
    KEY --> CHECK
    CHECK -->|Under Limit| COUNT
    COUNT --> WINDOW
    WINDOW --> ALLOW
    ALLOW --> HEADERS

    CHECK -->|Over Limit| DENY
    DENY --> HEADERS

    style CHECK fill:#4a5568,color:#fff
    style ALLOW fill:#48bb78,color:#fff
    style DENY fill:#f56565,color:#fff
```

## 10. Component Interaction Map

```mermaid
graph TB
    subgraph "Entry Points"
        MAIN[main.go]
        WEBHOOK[Webhook Handlers]
    end

    subgraph "Internal Packages"
        AUTH_PKG[internal/auth]
        CACHE_PKG[internal/cache]
        CH_PKG[internal/clickhouse]
        CONFIG_PKG[internal/config]
        DB_PKG[internal/database]
        HAND_PKG[internal/handlers]
        MID_PKG[internal/middleware]
        MODEL_PKG[internal/models]
        PAG_PKG[internal/pagination]
        PIPE_PKG[internal/pipeline]
        SCHEMA_PKG[internal/schema]
        SVC_PKG[internal/services]
        UTIL_PKG[internal/utils]
    end

    MAIN --> HAND_PKG
    MAIN --> SVC_PKG
    MAIN --> CONFIG_PKG

    WEBHOOK --> HAND_PKG

    HAND_PKG --> SVC_PKG
    HAND_PKG --> SCHEMA_PKG
    HAND_PKG --> MID_PKG

    SVC_PKG --> DB_PKG
    SVC_PKG --> CACHE_PKG
    SVC_PKG --> CH_PKG

    MID_PKG --> AUTH_PKG
    MID_PKG --> CACHE_PKG

    SCHEMA_PKG --> DB_PKG
    PAG_PKG --> SCHEMA_PKG
    PIPE_PKG --> CH_PKG
    PIPE_PKG --> DB_PKG

    style MAIN fill:#00ADD8,color:#fff
    style SVC_PKG fill:#4a5568,color:#fff
```
