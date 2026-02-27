# Deployment Guide

This guide covers deploying L.S.D to production environments, from single-server setups to distributed architectures. Follow these best practices for a secure, scalable, and reliable deployment.

## Table of Contents

- [Deployment Architecture Options](#deployment-architecture-options)
- [Prerequisites](#prerequisites)
- [Docker Deployment](#docker-deployment)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Traditional Server Deployment](#traditional-server-deployment)
- [Cloud Provider Guides](#cloud-provider-guides)
- [Production Configuration](#production-configuration)
- [Monitoring & Observability](#monitoring--observability)
- [Security Hardening](#security-hardening)
- [Backup & Recovery](#backup--recovery)
- [Scaling Strategies](#scaling-strategies)

## Deployment Architecture Options

### Single Server (Small Scale)

Suitable for: Development, staging, small production workloads (< 100 req/s)

```
┌─────────────────────────────────────────┐
│            Single Server                 │
│  ┌─────────────────────────────────┐    │
│  │         L.S.D API               │    │
│  │         (Port 5000)             │    │
│  └─────────────────────────────────┘    │
│  ┌─────────────────────────────────┐    │
│  │         PostgreSQL              │    │
│  │         (Port 5432)             │    │
│  └─────────────────────────────────┘    │
│  ┌─────────────────────────────────┐    │
│  │         Redis                   │    │
│  │         (Port 6379)             │    │
│  └─────────────────────────────────┘    │
└─────────────────────────────────────────┘
```

### Multi-Service (Medium Scale)

Suitable for: Medium production workloads (100-1000 req/s)

```
┌──────────────────┐     ┌──────────────────┐
│   Load Balancer  │────▶│   L.S.D API #1   │
│   (nginx/HAProxy)│     │   (Port 5000)    │
└──────────────────┘     └──────────────────┘
         │               ┌──────────────────┐
         └──────────────▶│   L.S.D API #2   │
                         │   (Port 5000)    │
                         └──────────────────┘
                                │
         ┌──────────────────────┼──────────────────────┐
         ▼                      ▼                      ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│   PostgreSQL    │   │     Redis       │   │   ClickHouse    │
│   (Primary)     │   │   (Cluster)     │   │   (Cluster)     │
└─────────────────┘   └─────────────────┘   └─────────────────┘
```

### Distributed (Large Scale)

Suitable for: Large production workloads (1000+ req/s, TB-scale data)

```
                    ┌──────────────────┐
                    │   CDN / WAF      │
                    └──────────────────┘
                            │
                    ┌──────────────────┐
                    │   Load Balancer  │
                    │   (TLS Termination)
                    └──────────────────┘
                            │
        ┌───────────────────┼───────────────────┐
        ▼                   ▼                   ▼
┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│  L.S.D API #1 │   │  L.S.D API #2 │   │  L.S.D API #N │
│  (K8s Pod)    │   │  (K8s Pod)    │   │  (K8s Pod)    │
└───────────────┘   └───────────────┘   └───────────────┘
        │                   │                   │
        └───────────────────┼───────────────────┘
                            │
    ┌───────────────────────┼───────────────────────┐
    ▼                       ▼                       ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ PostgreSQL  │     │   Redis     │     │ ClickHouse  │
│ (Primary +  │     │  (Cluster)  │     │  (Cluster)  │
│  Replicas)  │     │             │     │             │
└─────────────┘     └─────────────┘     └─────────────┘
```

## Prerequisites

### Production Checklist

Before deploying to production:

- [ ] PostgreSQL 15+ with proper configuration
- [ ] Redis 7+ for caching (optional but recommended)
- [ ] ClickHouse 24+ for search acceleration (optional)
- [ ] TLS/SSL certificates
- [ ] Monitoring infrastructure
- [ ] Backup strategy
- [ ] Log aggregation
- [ ] Rate limiting configured
- [ ] Security hardening applied

### Resource Requirements

| Component | Minimum | Recommended | Large Scale |
|-----------|---------|-------------|-------------|
| API Server | 2 vCPU, 4GB | 4 vCPU, 8GB | 8+ vCPU, 16+ GB |
| PostgreSQL | 4 vCPU, 16GB | 8 vCPU, 32GB | 16+ vCPU, 64+ GB |
| Redis | 2 vCPU, 4GB | 4 vCPU, 8GB | 4+ vCPU, 16+ GB |
| ClickHouse | 4 vCPU, 16GB | 8 vCPU, 32GB | 16+ vCPU, 128+ GB |

## Docker Deployment

### Dockerfile

```dockerfile
# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Dependencies
COPY go.mod go.sum ./
RUN go mod download

# Build
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o api ./cmd/api

# Runtime stage
FROM alpine:3.19

RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Copy binary
COPY --from=builder /app/api .
COPY --from=builder /app/web ./web

# Create non-root user
RUN adduser -D -g '' appuser
USER appuser

EXPOSE 5000

CMD ["./api"]
```

### Docker Compose (Production)

```yaml
# docker-compose.prod.yml
version: '3.8'

services:
  api:
    build:
      context: .
      dockerfile: Dockerfile
    restart: always
    ports:
      - "5000:5000"
    environment:
      - DATABASE_URL=postgresql://lsd:${DB_PASSWORD}@postgres:5432/lsd
      - REDIS_ADDR=redis:6379
      - CLICKHOUSE_ADDR=clickhouse:9000
      - SESSION_SECRET=${SESSION_SECRET}
      - LOG_LEVEL=info
      - LOG_FORMAT=json
    depends_on:
      postgres:
        condition: service_healthy
      redis:
        condition: service_healthy
    healthcheck:
      test: ["CMD", "wget", "-q", "--spider", "http://localhost:5000/api/health"]
      interval: 30s
      timeout: 10s
      retries: 3
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 4G
        reservations:
          cpus: '1'
          memory: 2G

  postgres:
    image: postgres:15-alpine
    restart: always
    environment:
      - POSTGRES_DB=lsd
      - POSTGRES_USER=lsd
      - POSTGRES_PASSWORD=${DB_PASSWORD}
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./docs/database_schema.sql:/docker-entrypoint-initdb.d/01-schema.sql
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U lsd"]
      interval: 10s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7-alpine
    restart: always
    command: redis-server --appendonly yes --maxmemory 2gb --maxmemory-policy allkeys-lru
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 5s
      retries: 5

  clickhouse:
    image: clickhouse/clickhouse-server:latest
    restart: always
    environment:
      - CLICKHOUSE_DB=lsd_search
      - CLICKHOUSE_USER=lsd
      - CLICKHOUSE_PASSWORD=${CLICKHOUSE_PASSWORD}
    volumes:
      - clickhouse_data:/var/lib/clickhouse
    ulimits:
      nofile:
        soft: 262144
        hard: 262144

  nginx:
    image: nginx:alpine
    restart: always
    ports:
      - "80:80"
      - "443:443"
    volumes:
      - ./nginx.conf:/etc/nginx/nginx.conf:ro
      - ./certs:/etc/nginx/certs:ro
    depends_on:
      - api

volumes:
  postgres_data:
  redis_data:
  clickhouse_data:
```

### Deploy with Docker Compose

```bash
# Create environment file
cp .env.example .env.prod
# Edit .env.prod with production values

# Deploy
docker-compose -f docker-compose.prod.yml --env-file .env.prod up -d

# View logs
docker-compose -f docker-compose.prod.yml logs -f api

# Scale API
docker-compose -f docker-compose.prod.yml up -d --scale api=3
```

## Kubernetes Deployment

### Namespace and Secrets

```yaml
# k8s/namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: lsd

---
# k8s/secrets.yaml
apiVersion: v1
kind: Secret
metadata:
  name: lsd-secrets
  namespace: lsd
type: Opaque
stringData:
  DATABASE_URL: "postgresql://lsd:password@postgres:5432/lsd"
  SESSION_SECRET: "your-production-secret"
  REDIS_ADDR: "redis:6379"
  CLICKHOUSE_ADDR: "clickhouse:9000"
```

### ConfigMap

```yaml
# k8s/configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: lsd-config
  namespace: lsd
data:
  PORT: "5000"
  LOG_LEVEL: "info"
  LOG_FORMAT: "json"
  CACHE_TTL_SECONDS: "30"
  RATE_LIMIT_RPS: "500"
  CDC_SYNC_INTERVAL_SECONDS: "30"
```

### Deployment

```yaml
# k8s/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: lsd-api
  namespace: lsd
spec:
  replicas: 3
  selector:
    matchLabels:
      app: lsd-api
  template:
    metadata:
      labels:
        app: lsd-api
    spec:
      containers:
        - name: api
          image: lsd-api:latest
          imagePullPolicy: Always
          ports:
            - containerPort: 5000
          envFrom:
            - configMapRef:
                name: lsd-config
            - secretRef:
                name: lsd-secrets
          resources:
            requests:
              memory: "2Gi"
              cpu: "1"
            limits:
              memory: "4Gi"
              cpu: "2"
          livenessProbe:
            httpGet:
              path: /api/health
              port: 5000
            initialDelaySeconds: 30
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /api/health
              port: 5000
            initialDelaySeconds: 5
            periodSeconds: 5
      affinity:
        podAntiAffinity:
          preferredDuringSchedulingIgnoredDuringExecution:
            - weight: 100
              podAffinityTerm:
                labelSelector:
                  matchLabels:
                    app: lsd-api
                topologyKey: kubernetes.io/hostname
```

### Service and Ingress

```yaml
# k8s/service.yaml
apiVersion: v1
kind: Service
metadata:
  name: lsd-api
  namespace: lsd
spec:
  selector:
    app: lsd-api
  ports:
    - port: 80
      targetPort: 5000
  type: ClusterIP

---
# k8s/ingress.yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: lsd-ingress
  namespace: lsd
  annotations:
    kubernetes.io/ingress.class: nginx
    cert-manager.io/cluster-issuer: letsencrypt-prod
spec:
  tls:
    - hosts:
        - api.yourdomain.com
      secretName: lsd-tls
  rules:
    - host: api.yourdomain.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: lsd-api
                port:
                  number: 80
```

### Deploy to Kubernetes

```bash
# Apply configurations
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/secrets.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/service.yaml
kubectl apply -f k8s/ingress.yaml

# Check status
kubectl get pods -n lsd
kubectl get services -n lsd

# View logs
kubectl logs -f deployment/lsd-api -n lsd

# Scale
kubectl scale deployment lsd-api --replicas=5 -n lsd
```

## Traditional Server Deployment

### System Setup (Ubuntu/Debian)

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install dependencies
sudo apt install -y postgresql redis-server nginx

# Install Go
wget https://go.dev/dl/go1.24.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.24.linux-amd64.tar.gz
export PATH=$PATH:/usr/local/go/bin

# Create application user
sudo useradd -r -s /bin/false lsd

# Create directories
sudo mkdir -p /opt/lsd/{bin,web,logs}
sudo chown -R lsd:lsd /opt/lsd
```

### Build and Install

```bash
# Clone and build
git clone https://github.com/Daveshvats/L.S.D.git
cd L.S.D

# Build binary
CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /opt/lsd/bin/api ./cmd/api

# Copy web assets
cp -r web/* /opt/lsd/web/

# Set permissions
sudo chown -R lsd:lsd /opt/lsd
```

### systemd Service

```ini
# /etc/systemd/system/lsd-api.service
[Unit]
Description=L.S.D API Server
After=network.target postgresql.service redis.service
Wants=postgresql.service redis.service

[Service]
Type=simple
User=lsd
Group=lsd
WorkingDirectory=/opt/lsd
ExecStart=/opt/lsd/bin/api
Restart=on-failure
RestartSec=5

# Environment
EnvironmentFile=/opt/lsd/.env

# Security
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/opt/lsd/logs

# Resource limits
LimitNOFILE=65535
MemoryMax=4G

[Install]
WantedBy=multi-user.target
```

### nginx Reverse Proxy

```nginx
# /etc/nginx/sites-available/lsd-api
upstream lsd_api {
    server 127.0.0.1:5000;
    keepalive 32;
}

server {
    listen 80;
    server_name api.yourdomain.com;
    return 301 https://$server_name$request_uri;
}

server {
    listen 443 ssl http2;
    server_name api.yourdomain.com;

    # SSL
    ssl_certificate /etc/letsencrypt/live/api.yourdomain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/api.yourdomain.com/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256;
    ssl_prefer_server_ciphers off;

    # Security headers
    add_header X-Frame-Options DENY;
    add_header X-Content-Type-Options nosniff;
    add_header X-XSS-Protection "1; mode=block";
    add_header Strict-Transport-Security "max-age=31536000; includeSubDomains";

    # Rate limiting
    limit_req_zone $binary_remote_addr zone=api:10m rate=100r/s;

    location / {
        limit_req zone=api burst=200 nodelay;
        proxy_pass http://lsd_api;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_set_header Connection "";

        # Timeouts
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        proxy_read_timeout 60s;
    }

    # Health check (no rate limit)
    location /api/health {
        proxy_pass http://lsd_api;
        access_log off;
    }
}
```

### Enable and Start

```bash
# Enable services
sudo systemctl enable lsd-api
sudo systemctl enable nginx

# Start services
sudo systemctl start lsd-api
sudo systemctl start nginx

# Check status
sudo systemctl status lsd-api
```

## Cloud Provider Guides

### AWS ECS

```yaml
# ecs-task-definition.json
{
  "family": "lsd-api",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "1024",
  "memory": "4096",
  "containerDefinitions": [
    {
      "name": "api",
      "image": "your-ecr-repo/lsd-api:latest",
      "portMappings": [
        {
          "containerPort": 5000,
          "protocol": "tcp"
        }
      ],
      "environment": [
        {"name": "PORT", "value": "5000"},
        {"name": "LOG_LEVEL", "value": "info"}
      ],
      "secrets": [
        {"name": "DATABASE_URL", "valueFrom": "arn:aws:secretsmanager:..."},
        {"name": "SESSION_SECRET", "valueFrom": "arn:aws:secretsmanager:..."}
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/lsd-api",
          "awslogs-region": "us-east-1",
          "awslogs-stream-prefix": "ecs"
        }
      }
    }
  ]
}
```

### Google Cloud Run

```bash
# Build and push to GCR
gcloud builds submit --tag gcr.io/PROJECT_ID/lsd-api

# Deploy
gcloud run deploy lsd-api \
  --image gcr.io/PROJECT_ID/lsd-api \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars "PORT=5000" \
  --set-secrets "DATABASE_URL=database-url:latest,SESSION_SECRET=session-secret:latest"
```

## Production Configuration

### Environment Variables

```bash
# Production .env
DATABASE_URL=postgresql://lsd:SECURE_PASSWORD@postgres-host:5432/lsd
REDIS_ADDR=redis-host:6379
CLICKHOUSE_ADDR=clickhouse-host:9000

# Server
PORT=5000
HOST=0.0.0.0

# Security
SESSION_SECRET=CHANGE-THIS-TO-A-SECURE-RANDOM-STRING-AT-LEAST-32-CHARS
JWT_ACCESS_TOKEN_TTL=15m
JWT_REFRESH_TOKEN_TTL=168h
SESSION_SECURE=true

# Performance
DB_MAX_CONNECTIONS=100
CACHE_TTL_SECONDS=60
RATE_LIMIT_RPS=500

# Logging
LOG_LEVEL=info
LOG_FORMAT=json
```

## Monitoring & Observability

### Prometheus Metrics

```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'lsd-api'
    static_configs:
      - targets: ['lsd-api:5000']
    metrics_path: '/metrics'
```

### Grafana Dashboard

Key metrics to monitor:

| Metric | Description | Alert Threshold |
|--------|-------------|-----------------|
| `lsd_requests_total` | Total requests | - |
| `lsd_request_duration_seconds` | Request latency | p99 > 500ms |
| `lsd_db_connections_active` | Active DB connections | > 80% pool |
| `lsd_cache_hit_ratio` | Cache effectiveness | < 50% |
| `lsd_search_duration_seconds` | Search latency | p99 > 1s |

### Log Aggregation

```yaml
# filebeat.yml
filebeat.inputs:
  - type: log
    paths:
      - /opt/lsd/logs/*.log
    json.keys_under_root: true

output.elasticsearch:
  hosts: ["elasticsearch:9200"]
  index: "lsd-api-%{+yyyy.MM.dd}"
```

## Security Hardening

### Checklist

- [ ] TLS 1.2+ enforced
- [ ] Strong session secret (32+ chars)
- [ ] Database credentials rotated
- [ ] Rate limiting enabled
- [ ] CORS restricted to known origins
- [ ] Security headers configured
- [ ] Regular security updates
- [ ] API keys have minimal scopes
- [ ] Network isolation (VPC/firewalls)

### Firewall Rules

```bash
# UFW (Ubuntu)
sudo ufw default deny incoming
sudo ufw default allow outgoing
sudo ufw allow 22/tcp      # SSH
sudo ufw allow 80/tcp      # HTTP
sudo ufw allow 443/tcp     # HTTPS
sudo ufw enable
```

## Backup & Recovery

### PostgreSQL Backup

```bash
# Daily backup script
#!/bin/bash
BACKUP_DIR="/backup/postgres"
DATE=$(date +%Y%m%d)

pg_dump -h localhost -U lsd -d lsd | gzip > $BACKUP_DIR/lsd_$DATE.sql.gz

# Keep last 30 days
find $BACKUP_DIR -name "*.gz" -mtime +30 -delete
```

### Redis Persistence

```bash
# redis.conf
save 900 1
save 300 10
save 60 10000
appendonly yes
```

## Scaling Strategies

### Vertical Scaling

Increase resources for existing servers:

- Upgrade CPU/RAM
- Increase connection pool size
- Add SSD storage

### Horizontal Scaling

Add more API instances:

- Load balancer distributes traffic
- Shared PostgreSQL/Redis/ClickHouse
- Stateless API servers

### Database Scaling

- **Read replicas**: Direct read traffic to replicas
- **Partitioning**: Split large tables
- **Connection pooling**: PgBouncer for connection management

---

**Next**: [FAQ](faq.md) | [Architecture Guide](architecture.md)
