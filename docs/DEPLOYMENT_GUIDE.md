# DataBridge AI Deployment Guide

This guide covers deploying DataBridge AI Librarian (Hierarchy Builder) and Researcher (Analytics Engine) to production environments.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Quick Start](#quick-start)
3. [Docker Deployment](#docker-deployment)
4. [Environment Configuration](#environment-configuration)
5. [Production Checklist](#production-checklist)
6. [Service Architecture](#service-architecture)
7. [Monitoring & Health Checks](#monitoring--health-checks)
8. [Backup & Recovery](#backup--recovery)
9. [Security Configuration](#security-configuration)
10. [Troubleshooting](#troubleshooting)

---

## Prerequisites

### Required Software
- Docker Engine 24.0+ ([Install Docker](https://docs.docker.com/get-docker/))
- Docker Compose 2.20+ ([Install Compose](https://docs.docker.com/compose/install/))
- Git (for cloning the repository)

### Hardware Requirements

| Environment | CPU | RAM | Storage |
|-------------|-----|-----|---------|
| Development | 2 cores | 4 GB | 20 GB |
| Staging | 4 cores | 8 GB | 50 GB |
| Production | 8+ cores | 16+ GB | 100+ GB SSD |

### Network Requirements
- Outbound HTTPS (443) for external APIs (Notion, data warehouses)
- Internal ports: 5432 (PostgreSQL), 6379 (Redis), 8000-8001 (Services)

---

## Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/databridge-ai/databridge.git
cd databridge/docker
```

### 2. Run Quick Start Script
```bash
# Development environment
./scripts/quickstart.sh dev

# Production environment
./scripts/quickstart.sh prod
```

### 3. Verify Services
```bash
./scripts/healthcheck.sh
```

---

## Docker Deployment

### Development Environment

```bash
cd docker

# Start infrastructure only (PostgreSQL, Redis, ChromaDB)
docker-compose -f docker-compose.dev.yml up -d

# Start all services including Librarian and Researcher
docker-compose -f docker-compose.dev.yml --profile full up -d

# View logs
docker-compose -f docker-compose.dev.yml logs -f

# Stop services
docker-compose -f docker-compose.dev.yml down
```

### Production Environment

```bash
cd docker

# Setup environment (generates secure passwords)
./scripts/setup-env.sh prod

# Review and customize .env file
nano .env

# Start all services
docker-compose -f docker-compose.prod.yml up -d

# Wait for services to be healthy
python scripts/deploy.py health --env prod
```

### Using the Deployment Script

```bash
# Start services
python scripts/deploy.py up --env prod --build --wait

# Check status
python scripts/deploy.py status --env prod

# View logs
python scripts/deploy.py logs --env prod --services databridge-v4 --tail 100

# Restart a service
python scripts/deploy.py restart --env prod --services databridge-v4

# Stop services
python scripts/deploy.py down --env prod
```

---

## Environment Configuration

### Environment Variables

Create a `.env` file in the `docker/` directory:

```bash
# Database
POSTGRES_USER=databridge
POSTGRES_PASSWORD=<secure-password>
POSTGRES_DB=databridge_analytics
POSTGRES_PORT=5432

# Redis
REDIS_PASSWORD=<secure-password>
REDIS_PORT=6379

# ChromaDB
CHROMADB_PORT=8001

# Librarian Configuration
V3_PORT=8000
V3_ENCRYPTION_KEY=<32-byte-hex-key>
V3_IMAGE=ghcr.io/databridge-ai/databridge-v3:latest

# Researcher Configuration
V4_PORT=8001
V4_ENCRYPTION_KEY=<32-byte-hex-key>
V4_IMAGE=ghcr.io/databridge-ai/databridge-v4:latest

# Optional: Notion Integration
NOTION_API_KEY=<your-notion-api-key>

# Logging
LOG_LEVEL=INFO
```

### Generating Secure Keys

```bash
# Generate encryption keys
openssl rand -hex 32

# Generate passwords
openssl rand -base64 24 | tr -dc 'a-zA-Z0-9' | head -c 24
```

### Configuration by Service

#### PostgreSQL
| Variable | Description | Default |
|----------|-------------|---------|
| POSTGRES_USER | Database user | postgres |
| POSTGRES_PASSWORD | Database password | (required) |
| POSTGRES_DB | Database name | databridge_analytics |

#### Librarian Hierarchy Builder
| Variable | Description | Default |
|----------|-------------|---------|
| V3_ENCRYPTION_KEY | Fernet encryption key | (required) |
| CHROMADB_HOST | ChromaDB hostname | chromadb |
| REDIS_URL | Redis connection URL | redis://redis:6379/0 |

#### Researcher Analytics Engine
| Variable | Description | Default |
|----------|-------------|---------|
| V4_ENCRYPTION_KEY | API key encryption | (required) |
| POSTGRESQL_HOST | PostgreSQL hostname | postgres |
| LIBRARIAN_API_URL | Librarian service URL | http://databridge-librarian:8000 |
| NOTION_API_KEY | Notion API key | (optional) |

---

## Production Checklist

### Pre-Deployment

- [ ] **Environment file** configured with secure passwords
- [ ] **Encryption keys** generated and stored securely
- [ ] **Database backups** scheduled
- [ ] **Firewall rules** configured (only required ports open)
- [ ] **TLS certificates** obtained (if using HTTPS)
- [ ] **Resource limits** verified (CPU, memory)
- [ ] **Storage** sufficient for databases and logs

### Deployment

- [ ] Pull latest Docker images
- [ ] Run database migrations (if any)
- [ ] Start services in correct order
- [ ] Verify health checks pass
- [ ] Test API connectivity
- [ ] Verify Librarian-Researcher integration

### Post-Deployment

- [ ] Monitor logs for errors
- [ ] Set up alerting
- [ ] Document deployment version
- [ ] Test backup/restore procedure
- [ ] Update documentation

---

## Service Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      External Network                            │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────┐    ┌──────────────┐                           │
│  │   Nginx/LB   │    │  Claude/MCP  │                           │
│  │   (Optional) │    │   Client     │                           │
│  └──────┬───────┘    └──────┬───────┘                           │
│         │                    │                                   │
├─────────┴────────────────────┴───────────────────────────────────┤
│                      Internal Network                            │
│                                                                  │
│  ┌──────────────────┐    ┌──────────────────┐                   │
│  │   DataBridge Librarian  │◄───│   DataBridge Researcher  │                   │
│  │  (Port 8000)     │    │   (Port 8001)    │                   │
│  │  Hierarchy       │    │   Analytics      │                   │
│  └────────┬─────────┘    └──────┬───────────┘                   │
│           │                      │                               │
│           ▼                      ▼                               │
│  ┌──────────────────────────────────────────┐                   │
│  │              ChromaDB                     │                   │
│  │           (Vector Store)                  │                   │
│  └──────────────────────────────────────────┘                   │
│           │                      │                               │
│           ▼                      ▼                               │
│  ┌─────────────────┐    ┌─────────────────┐                     │
│  │     Redis       │    │   PostgreSQL    │                     │
│  │    (Cache)      │    │   (Analytics)   │                     │
│  └─────────────────┘    └─────────────────┘                     │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

### Port Mapping

| Service | Container Port | Host Port | Description |
|---------|----------------|-----------|-------------|
| PostgreSQL | 5432 | 5432 | Analytics database |
| Redis | 6379 | 6379 | Cache and sessions |
| ChromaDB | 8000 | 8001 | Vector embeddings |
| Librarian Hierarchy | 8000 | 8000 | MCP server |
| Researcher Analytics | 8001 | 8001 | MCP server |

---

## Monitoring & Health Checks

### Health Check Endpoints

```bash
# Librarian Hierarchy Builder
curl http://localhost:8000/health

# Researcher Analytics Engine
curl http://localhost:8001/health

# ChromaDB
curl http://localhost:8001/api/v1/heartbeat

# PostgreSQL (via docker)
docker exec databridge-postgres-prod pg_isready

# Redis (via docker)
docker exec databridge-redis-prod redis-cli ping
```

### Using the Health Check Script

```bash
./docker/scripts/healthcheck.sh prod
```

### Docker Health Checks

All services have built-in Docker health checks. View status:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

### Monitoring Recommendations

1. **Log Aggregation**: Send logs to Elasticsearch/Loki
2. **Metrics**: Export to Prometheus/Grafana
3. **Alerting**: Set up PagerDuty/Slack alerts for:
   - Service unhealthy > 5 minutes
   - Error rate > 1%
   - Memory usage > 80%
   - Disk usage > 80%

---

## Backup & Recovery

### Creating Backups

```bash
# Full backup
./docker/scripts/backup.sh /path/to/backup/dir

# Using deployment script
python docker/scripts/deploy.py backup --env prod --output /backups
```

### Backup Contents

| File | Description |
|------|-------------|
| postgres_TIMESTAMP.sql.gz | PostgreSQL full dump |
| redis_TIMESTAMP.rdb | Redis snapshot |
| librarian_databridge_TIMESTAMP.db | Librarian SQLite database |
| backup_TIMESTAMP.json | Backup manifest |

### Restoring from Backup

```bash
# Restore from specific timestamp
./docker/scripts/restore.sh /path/to/backup/dir 20260130_120000

# Verify services after restore
./docker/scripts/healthcheck.sh prod
```

### Backup Schedule (Recommended)

| Frequency | Retention | Purpose |
|-----------|-----------|---------|
| Hourly | 24 hours | Point-in-time recovery |
| Daily | 7 days | Daily rollback |
| Weekly | 4 weeks | Weekly snapshots |
| Monthly | 12 months | Compliance/audit |

---

## Security Configuration

### Network Security

1. **Internal Network Isolation**
   - Production uses separate internal network
   - Only necessary ports exposed

2. **Firewall Rules**
   ```bash
   # Allow only required ports
   ufw allow 8000/tcp  # Librarian (or use reverse proxy)
   ufw allow 8001/tcp  # Researcher (or use reverse proxy)
   ```

### TLS/SSL (Recommended)

Use nginx reverse proxy for TLS termination:

```nginx
# /docker/nginx/nginx.conf
server {
    listen 443 ssl;
    server_name databridge.example.com;

    ssl_certificate /etc/nginx/ssl/cert.pem;
    ssl_certificate_key /etc/nginx/ssl/key.pem;

    location /v3/ {
        proxy_pass http://databridge-v3:8000/;
    }

    location /v4/ {
        proxy_pass http://databridge-v4:8001/;
    }
}
```

### API Key Management

```bash
# Generate API key via Researcher service
curl -X POST http://localhost:8001/api/keys/generate \
  -H "Content-Type: application/json" \
  -d '{"description": "Production API Key", "scopes": ["read", "write"]}'
```

### Secrets Management

For production, consider:
- HashiCorp Vault
- AWS Secrets Manager
- Azure Key Vault
- Docker Secrets

---

## Troubleshooting

### Common Issues

#### Services Won't Start

```bash
# Check logs
docker-compose -f docker-compose.prod.yml logs

# Check resource constraints
docker stats

# Verify environment variables
docker-compose -f docker-compose.prod.yml config
```

#### Connection Refused

```bash
# Verify service is running
docker ps

# Check internal network
docker network inspect databridge-internal-prod

# Test internal connectivity
docker exec databridge-v4-prod ping postgres
```

#### Database Connection Errors

```bash
# Check PostgreSQL logs
docker logs databridge-postgres-prod

# Test connection
docker exec -it databridge-postgres-prod psql -U databridge -d databridge_analytics
```

#### Out of Memory

```bash
# Check memory usage
docker stats --no-stream

# Increase memory limits in docker-compose.yml
deploy:
  resources:
    limits:
      memory: 4G
```

#### Disk Space Full

```bash
# Check disk usage
docker system df

# Clean up unused resources
docker system prune -a

# Remove old backups
find /backups -mtime +30 -delete
```

### Log Locations

| Service | Log Location |
|---------|--------------|
| Docker containers | `docker logs <container>` |
| Librarian Application | `/app/logs/librarian.log` (in container) |
| Researcher Application | `/app/logs/researcher.log` (in container) |
| PostgreSQL | `docker logs databridge-postgres-prod` |

### Support

- GitHub Issues: https://github.com/databridge-ai/databridge/issues
- Documentation: https://docs.databridge-ai.com

---

## Appendix

### Sample docker-compose Override

For customizations, create `docker-compose.override.yml`:

```yaml
version: "3.9"

services:
  databridge-v4:
    environment:
      - LOG_LEVEL=DEBUG
    volumes:
      - ./custom-config:/app/config:ro
```

### Kubernetes Deployment

For Kubernetes deployment, see `docs/KUBERNETES_GUIDE.md` (coming soon).

---

*Document Version: 1.0.0 | Last Updated: 2026-01-30*
