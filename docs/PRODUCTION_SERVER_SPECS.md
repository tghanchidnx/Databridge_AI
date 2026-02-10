# DataBridge AI v0.40.0 — Production Server Specs & Installation Guide

> Complete requirements and step-by-step installation instructions for deploying DataBridge AI in a production environment.

---

## Table of Contents

1. [Server Hardware Requirements](#1-server-hardware-requirements)
2. [Operating System](#2-operating-system)
3. [Required Software](#3-required-software)
4. [Python Environment Setup](#4-python-environment-setup)
5. [Database Installation](#5-database-installation)
6. [Docker Deployment (Recommended)](#6-docker-deployment-recommended)
7. [Bare-Metal Deployment (Alternative)](#7-bare-metal-deployment-alternative)
8. [Environment Variables](#8-environment-variables)
9. [Network & Port Configuration](#9-network--port-configuration)
10. [Optional Components](#10-optional-components)
11. [SSL/TLS Configuration](#11-ssltls-configuration)
12. [Post-Install Verification](#12-post-install-verification)
13. [Tier-Specific Dependencies](#13-tier-specific-dependencies)

---

## 1. Server Hardware Requirements

### Minimum Specs by Deployment Size

| Spec | Small (Dev/Test) | Medium (Team/Staging) | Large (Production) |
|------|------------------|-----------------------|--------------------|
| **CPU** | 2 cores | 4 cores | 8+ cores |
| **RAM** | 4 GB | 8 GB | 16+ GB |
| **Storage** | 20 GB SSD | 50 GB SSD | 100+ GB NVMe SSD |
| **Network** | 100 Mbps | 1 Gbps | 1+ Gbps |

### Storage Breakdown (Production)

| Component | Recommended Size | Notes |
|-----------|-----------------|-------|
| OS + Software | 20 GB | Base system, Docker, Python |
| MySQL Database | 20-50 GB | Hierarchy data, user data |
| PostgreSQL Database | 10-30 GB | Analytics data |
| Redis | 512 MB - 1 GB | In-memory cache |
| ChromaDB (Vector Store) | 5-20 GB | Embeddings for GraphRAG |
| Application Logs | 10-20 GB | Rotate with logrotate |
| Backups | 50+ GB | Database dumps, snapshots |
| **Total Recommended** | **~150-200 GB** | With backup headroom |

### Resource Limits per Container (Production Docker)

| Container | CPU Limit | Memory Limit | Memory Reserved |
|-----------|-----------|-------------|-----------------|
| PostgreSQL | 2 cores | 2 GB | 512 MB |
| ChromaDB | 1 core | 2 GB | 512 MB |
| Redis | 0.5 core | 768 MB | 128 MB |
| Librarian (Hierarchy) | 1 core | 1 GB | 256 MB |
| Researcher (Analytics) | 2 cores | 2 GB | 512 MB |
| Nginx (Reverse Proxy) | 0.5 core | 256 MB | — |
| NestJS Backend | 1 core | 1 GB | 256 MB |
| React Frontend | 0.5 core | 512 MB | — |
| MySQL | 1 core | 2 GB | 512 MB |

---

## 2. Operating System

### Supported

| OS | Version | Notes |
|----|---------|-------|
| **Ubuntu Server** (Recommended) | 22.04 LTS / 24.04 LTS | Best Docker support |
| **Debian** | 12 (Bookworm) | Stable alternative |
| **RHEL / Rocky Linux** | 8.x / 9.x | Enterprise environments |
| **Amazon Linux** | 2023 | AWS deployments |
| **Windows Server** | 2022 | Docker Desktop or WSL2 required |

### Recommended: Ubuntu 22.04 LTS

```bash
# Update system
sudo apt update && sudo apt upgrade -y

# Install essential utilities
sudo apt install -y curl wget git unzip software-properties-common \
    build-essential libssl-dev libffi-dev apt-transport-https \
    ca-certificates gnupg lsb-release
```

---

## 3. Required Software

### 3.1 Docker Engine (24.0+) & Docker Compose (2.20+)

**This is the recommended deployment method.** All services run as containers.

```bash
# Remove old Docker versions
sudo apt remove -y docker docker-engine docker.io containerd runc 2>/dev/null

# Add Docker's official GPG key
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
    sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

# Add Docker repository
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
    https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" | \
    sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

# Install Docker
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io \
    docker-buildx-plugin docker-compose-plugin

# Add your user to docker group (avoids sudo)
sudo usermod -aG docker $USER
newgrp docker

# Verify
docker --version          # Should be 24.0+
docker compose version    # Should be 2.20+
```

### 3.2 Python (3.10, 3.11, or 3.12)

Required for the MCP server and CLI tools.

```bash
# Install Python 3.12 (Ubuntu 22.04+)
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt update
sudo apt install -y python3.12 python3.12-venv python3.12-dev python3.12-distutils

# Install pip
curl -sS https://bootstrap.pypa.io/get-pip.py | python3.12

# Verify
python3.12 --version   # Should be 3.12.x
pip3 --version
```

### 3.3 Node.js 20 LTS

Required for the NestJS backend and React frontend (web UI).

```bash
# Install via NodeSource
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs

# Verify
node --version   # Should be v20.x.x
npm --version    # Should be 10.x.x
```

### 3.4 Git

```bash
sudo apt install -y git
git --version   # Should be 2.34+
```

---

## 4. Python Environment Setup

### 4.1 Create Virtual Environment

```bash
# Clone the repository
git clone https://github.com/tghanchidnx/Databridge_AI.git
cd Databridge_AI

# Create virtual environment
python3.12 -m venv .venv
source .venv/bin/activate

# Upgrade pip
pip install --upgrade pip setuptools wheel
```

### 4.2 Install Core Dependencies (Community Edition)

```bash
# Core install (106 tools)
pip install -e .

# Or with all optional dependencies
pip install -e ".[all]"
```

#### Core Dependencies Installed

| Package | Version | Purpose |
|---------|---------|---------|
| fastmcp | >= 2.0.0 | MCP server framework |
| pydantic | >= 2.0.0 | Data validation & settings |
| pydantic-settings | >= 2.0.0 | Environment variable management |
| pandas | >= 2.0.0 | Data manipulation & analysis |
| sqlalchemy | >= 2.0.0 | Database ORM & connections |
| rapidfuzz | >= 3.0.0 | Fuzzy string matching |

#### Optional Dependency Groups

```bash
# PDF processing
pip install -e ".[pdf]"
# Installs: pypdf >= 3.0.0

# OCR capabilities
pip install -e ".[ocr]"
# Installs: pytesseract >= 0.3.10, Pillow >= 10.0.0

# Snowflake connectivity
pip install -e ".[snowflake]"
# Installs: snowflake-connector-python >= 3.0.0

# Everything
pip install -e ".[all]"
```

### 4.3 Install Pro Dependencies (Licensed — 284 tools)

Requires a valid license key (`DB-PRO-...`).

```bash
# Pro package (requires GitHub Packages access)
pip install databridge-ai-pro

# Pro with all extras
pip install "databridge-ai-pro[all]"
```

#### Additional Pro Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| snowflake-connector-python | >= 3.0.0 | Snowflake data warehouse |
| networkx | >= 3.0.0 | Graph analysis & lineage |
| sentence-transformers | >= 2.0.0 | Semantic embeddings |
| chromadb | >= 0.4.0 | Vector database (GraphRAG) |
| langchain | >= 0.1.0 | LLM chain orchestration |
| snowflake-snowpark-python | >= 1.0.0 | Cortex AI features |
| prometheus-client | >= 0.17.0 | Observability metrics |

---

## 5. Database Installation

### 5.1 MySQL 8.0 (Backend — Primary)

Used by the NestJS backend for hierarchy data, users, and connections.

**Docker (recommended):**
```bash
# Handled automatically by docker-compose — see Section 6
```

**Bare-metal:**
```bash
sudo apt install -y mysql-server-8.0

# Secure installation
sudo mysql_secure_installation

# Create database and user
sudo mysql -u root -p <<'SQL'
CREATE DATABASE databridge_ai_database
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;
CREATE USER 'databridge'@'%' IDENTIFIED BY '<YOUR_SECURE_PASSWORD>';
GRANT ALL PRIVILEGES ON databridge_ai_database.* TO 'databridge'@'%';
SET GLOBAL max_connections = 500;
FLUSH PRIVILEGES;
SQL
```

**Connection:** `mysql://databridge:<password>@localhost:3306/databridge_ai_database`

### 5.2 PostgreSQL 16 (Analytics Engine)

Used by the Researcher service for analytics data.

**Bare-metal:**
```bash
sudo apt install -y postgresql-16

sudo -u postgres psql <<'SQL'
CREATE USER databridge WITH PASSWORD '<YOUR_SECURE_PASSWORD>';
CREATE DATABASE databridge_analytics OWNER databridge;
GRANT ALL PRIVILEGES ON DATABASE databridge_analytics TO databridge;
SQL
```

**Connection:** `postgresql://databridge:<password>@localhost:5432/databridge_analytics`

### 5.3 Redis 7 (Cache & Sessions)

```bash
# Bare-metal
sudo apt install -y redis-server

# Edit config for production
sudo sed -i 's/^# requirepass .*/requirepass <YOUR_REDIS_PASSWORD>/' /etc/redis/redis.conf
sudo sed -i 's/^# maxmemory .*/maxmemory 512mb/' /etc/redis/redis.conf
sudo sed -i 's/^# maxmemory-policy .*/maxmemory-policy allkeys-lru/' /etc/redis/redis.conf
sudo sed -i 's/^appendonly no/appendonly yes/' /etc/redis/redis.conf

sudo systemctl restart redis-server
sudo systemctl enable redis-server
```

### 5.4 ChromaDB (Vector Store — Pro)

Required for GraphRAG semantic search and embeddings.

```bash
# Docker (recommended)
docker run -d --name chromadb \
    -p 8001:8000 \
    -v chromadb_data:/chroma/chroma \
    -e IS_PERSISTENT=true \
    -e ANONYMIZED_TELEMETRY=false \
    ghcr.io/chroma-core/chroma:latest
```

---

## 6. Docker Deployment (Recommended)

### 6.1 Full-Stack (MySQL + NestJS + React + Redis)

```bash
cd Databridge_AI/infrastructure

# Create .env file (see Section 8 for all variables)
cp .env.example .env
nano .env  # Edit all secrets!

# Start all services
docker compose up -d

# Verify health
docker compose ps
# All containers should show "healthy" or "running"
```

**Services started:**

| Container | Port | Purpose |
|-----------|------|---------|
| databridge-mysql | 3308 | MySQL 8.0 database |
| databridge-redis | 6381 | Redis 7 cache |
| databridge-backend | 3002 | NestJS API |
| databridge-frontend | 8080 (HTTP), 8443 (HTTPS) | React frontend |

### 6.2 Production Stack (Librarian + Researcher + Infra)

```bash
cd Databridge_AI/docker

# Generate secure environment
./scripts/setup-env.sh prod

# Edit generated .env
nano .env

# Start production services
docker compose -f docker-compose.prod.yml up -d

# With nginx reverse proxy
docker compose -f docker-compose.prod.yml --profile with-nginx up -d

# Verify all services healthy
docker compose -f docker-compose.prod.yml ps
```

**Services started:**

| Container | Port | Purpose |
|-----------|------|---------|
| databridge-postgres-prod | 5432 | PostgreSQL 16 |
| databridge-chromadb-prod | 8001 | Vector store |
| databridge-redis-prod | 6379 | Redis cache |
| databridge-librarian-prod | 8000 | Hierarchy Builder MCP |
| databridge-researcher-prod | 8001 | Analytics Engine MCP |
| databridge-nginx-prod (optional) | 80, 443 | Reverse proxy + TLS |

### 6.3 Using start_services.py (All-in-One Launcher)

```bash
# Start Docker services + verify health
python start_services.py

# Start everything (Docker + MCP + Dashboard)
python start_services.py --all

# Check status of all services
python start_services.py --status

# Stop everything
python start_services.py --stop
```

---

## 7. Bare-Metal Deployment (Alternative)

If not using Docker, install each component directly on the server.

### 7.1 MCP Server

```bash
source .venv/bin/activate

# Run the MCP server
python src/server.py

# Or with FastMCP inspector (development)
fastmcp dev src/server.py  # Accessible on port 6274
```

### 7.2 UI Dashboard

```bash
# Flask-based dashboard on port 5050
python run_ui.py
```

### 7.3 NestJS Backend

```bash
cd infrastructure/backend
npm install
npx prisma generate
npx prisma migrate deploy
npm run build
npm run start:prod
```

### 7.4 React Frontend

```bash
cd infrastructure/frontend
npm install
npm run build
# Serve the dist/ folder with nginx or any static file server
```

### 7.5 Process Management (systemd)

Create systemd service files to keep services running:

```ini
# /etc/systemd/system/databridge-mcp.service
[Unit]
Description=DataBridge AI MCP Server
After=network.target mysql.service redis.service

[Service]
Type=simple
User=databridge
WorkingDirectory=/opt/databridge
Environment=PATH=/opt/databridge/.venv/bin:/usr/bin
ExecStart=/opt/databridge/.venv/bin/python src/server.py
Restart=always
RestartSec=5
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable databridge-mcp
sudo systemctl start databridge-mcp
sudo systemctl status databridge-mcp
```

---

## 8. Environment Variables

### 8.1 MCP Server (.env in project root)

```bash
# ============================================================
# DataBridge AI - MCP Server Configuration
# ============================================================

# Database (SQLite for local, or connection string for remote)
DATABASE_URL=sqlite:///data/local.db

# OCR (optional — install Tesseract first)
TESSERACT_PATH=/usr/bin/tesseract

# AI Provider Keys (optional — for LLM features)
OPENAI_API_KEY=sk-...
ANTHROPIC_API_KEY=sk-ant-...

# Fuzzy Matching
FUZZY_THRESHOLD=80
MAX_ROWS_DISPLAY=10

# NestJS Backend Sync
NESTJS_BACKEND_URL=http://localhost:3002/api
NESTJS_API_KEY=<your-api-key>
NESTJS_SYNC_ENABLED=true

# Cortex Agent (Pro)
CORTEX_DEFAULT_MODEL=mistral-large
CORTEX_MAX_REASONING_STEPS=10
CORTEX_CONSOLE_ENABLED=true

# License Key
DATABRIDGE_LICENSE_KEY=DB-PRO-YOURCOMPANY-20270209-a1b2c3d4e5f6
DATABRIDGE_LICENSE_SECRET=<your-license-secret>
```

### 8.2 Backend (infrastructure/.env)

```bash
# ============================================================
# DataBridge AI Backend - Docker Environment
# ============================================================

# --- MySQL ---
MYSQL_ROOT_PASSWORD=<STRONG_PASSWORD_HERE>
MYSQL_DATABASE=databridge_ai_database
MYSQL_USER=databridge
MYSQL_PASSWORD=<STRONG_PASSWORD_HERE>

# --- Redis ---
REDIS_PASSWORD=<STRONG_PASSWORD_HERE>

# --- JWT & Security (CHANGE ALL IN PRODUCTION) ---
JWT_SECRET=<64_CHAR_RANDOM_STRING>
JWT_EXPIRATION=7d
REFRESH_TOKEN_SECRET=<64_CHAR_RANDOM_STRING>
REFRESH_TOKEN_EXPIRATION=30d
ENCRYPTION_KEY=<64_CHAR_RANDOM_STRING>

# --- Microsoft OAuth (optional) ---
MICROSOFT_CLIENT_ID=
MICROSOFT_CLIENT_SECRET=
MICROSOFT_TENANT_ID=common

# --- Azure AD (optional) ---
AZURE_TENANT_ID=
AZURE_CLIENT_ID=
AZURE_CLIENT_SECRET=

# --- Snowflake (optional) ---
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USERNAME=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=

# --- AI ---
AI_PROVIDER=anthropic
OPENAI_API_KEY=
ANTHROPIC_API_KEY=

# --- Rate Limiting ---
THROTTLE_TTL=60
THROTTLE_LIMIT=100

# --- File Upload ---
MAX_FILE_SIZE=10485760

# --- CORS ---
CORS_ORIGINS=https://yourdomain.com
```

### 8.3 Production Docker (docker/.env)

```bash
# ============================================================
# DataBridge AI Production - Docker Environment
# ============================================================

# --- PostgreSQL ---
POSTGRES_USER=databridge
POSTGRES_PASSWORD=<STRONG_PASSWORD_HERE>
POSTGRES_DB=databridge_analytics

# --- Redis ---
REDIS_PASSWORD=<STRONG_PASSWORD_HERE>

# --- Encryption Keys ---
LIBRARIAN_ENCRYPTION_KEY=<32_BYTE_HEX_KEY>
RESEARCHER_ENCRYPTION_KEY=<32_BYTE_HEX_KEY>

# --- ChromaDB Auth (optional) ---
CHROMA_AUTH_PROVIDER=
CHROMA_AUTH_CREDENTIALS=

# --- Notion (optional) ---
NOTION_API_KEY=

# --- Logging ---
LOG_LEVEL=INFO
```

### Generating Secure Secrets

```bash
# Generate 64-char random strings (JWT, encryption keys)
openssl rand -hex 32

# Generate 32-byte hex key
openssl rand -hex 16

# Generate password
openssl rand -base64 24 | tr -dc 'a-zA-Z0-9' | head -c 24
```

---

## 9. Network & Port Configuration

### All Ports Used

| Port | Service | Protocol | Direction | Required |
|------|---------|----------|-----------|----------|
| 80 | Nginx HTTP | TCP | Inbound | Optional (redirect to 443) |
| 443 | Nginx HTTPS | TCP | Inbound | Recommended |
| 3002 | NestJS Backend API | TCP | Internal | Yes |
| 3306/3308 | MySQL | TCP | Internal | Yes |
| 5050 | Flask UI Dashboard | TCP | Inbound | Optional |
| 5432 | PostgreSQL | TCP | Internal | Yes (Prod) |
| 6274 | FastMCP Inspector | TCP | Internal | Dev only |
| 6379/6381 | Redis | TCP | Internal | Yes |
| 8000 | Librarian MCP Server | TCP | Inbound | Yes (Prod) |
| 8001 | Researcher / ChromaDB | TCP | Inbound | Yes (Prod/Pro) |
| 8080 | Frontend HTTP | TCP | Inbound | Yes |
| 8443 | Frontend HTTPS | TCP | Inbound | Yes |

### Firewall Rules (UFW Example)

```bash
# Allow only external-facing ports
sudo ufw allow 443/tcp    comment "HTTPS"
sudo ufw allow 80/tcp     comment "HTTP redirect"
sudo ufw allow 8443/tcp   comment "Frontend HTTPS"
sudo ufw allow 8080/tcp   comment "Frontend HTTP"

# Block direct database access from outside
sudo ufw deny 3306/tcp
sudo ufw deny 3308/tcp
sudo ufw deny 5432/tcp
sudo ufw deny 6379/tcp
sudo ufw deny 6381/tcp

sudo ufw enable
```

### Internal Docker Networks

| Network | Subnet | Purpose |
|---------|--------|---------|
| databridge-backend-network | 172.22.0.0/16 | Backend communication |
| databridge-frontend-network | 172.23.0.0/16 | Frontend public access |
| databridge-internal-prod | Bridge (internal) | Production infra (no external) |
| databridge-external-prod | Bridge | Production external-facing |

---

## 10. Optional Components

### 10.1 Tesseract OCR

Required for OCR features (`ocr_image` tool, PDF image extraction).

```bash
# Ubuntu/Debian
sudo apt install -y tesseract-ocr tesseract-ocr-eng

# Verify
tesseract --version   # Should be 4.x or 5.x
which tesseract       # Note this path for TESSERACT_PATH env var
```

### 10.2 Poppler (PDF Utilities)

Required for advanced PDF-to-image conversion.

```bash
sudo apt install -y poppler-utils
```

### 10.3 Snowflake Connectivity

```bash
# Install Snowflake connector
pip install "snowflake-connector-python>=3.0.0"

# For Cortex AI (Pro only)
pip install "snowflake-snowpark-python>=1.0.0"

# RSA key authentication (optional, recommended for production)
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out snowflake_private.p8 -nocrypt
openssl rsa -in snowflake_private.p8 -pubout -out snowflake_public.pub
# Then set the public key in Snowflake: ALTER USER ... SET RSA_PUBLIC_KEY='...'
```

### 10.4 Sentence Transformers (Pro — Embeddings)

```bash
pip install "sentence-transformers>=2.0.0"

# Models will auto-download on first use:
# - all-MiniLM-L6-v2 (lightweight, ~80MB)
# - all-mpnet-base-v2 (higher quality, ~420MB)
```

### 10.5 spaCy NLP

```bash
pip install spacy
python -m spacy download en_core_web_sm
```

### 10.6 dbt (Data Build Tool)

```bash
pip install "dbt-core>=1.5.0"
# Plus adapter for your warehouse:
pip install dbt-snowflake   # or dbt-postgres, dbt-bigquery, etc.
```

### 10.7 Prometheus Monitoring (Pro)

```bash
pip install "prometheus-client>=0.17.0"
# Metrics exported by the Observability module
# Scrape from your Prometheus instance
```

---

## 11. SSL/TLS Configuration

### Self-Signed (Development/Testing)

```bash
mkdir -p infrastructure/frontend/ssl
openssl req -x509 -nodes -days 365 \
    -newkey rsa:2048 \
    -keyout infrastructure/frontend/ssl/key.pem \
    -out infrastructure/frontend/ssl/cert.pem \
    -subj "/CN=localhost"
```

### Production (Let's Encrypt)

```bash
# Install certbot
sudo apt install -y certbot

# Obtain certificate
sudo certbot certonly --standalone -d yourdomain.com

# Certificates stored at:
# /etc/letsencrypt/live/yourdomain.com/fullchain.pem
# /etc/letsencrypt/live/yourdomain.com/privkey.pem

# Auto-renewal
sudo systemctl enable certbot.timer
```

### Nginx Reverse Proxy Config

```nginx
server {
    listen 443 ssl;
    server_name yourdomain.com;

    ssl_certificate     /etc/letsencrypt/live/yourdomain.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/yourdomain.com/privkey.pem;
    ssl_protocols       TLSv1.2 TLSv1.3;

    # Frontend
    location / {
        proxy_pass http://localhost:8080;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }

    # Backend API
    location /api/ {
        proxy_pass http://localhost:3002/api/;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }

    # Librarian MCP
    location /mcp/librarian/ {
        proxy_pass http://localhost:8000/;
    }

    # Researcher MCP
    location /mcp/researcher/ {
        proxy_pass http://localhost:8001/;
    }
}

server {
    listen 80;
    server_name yourdomain.com;
    return 301 https://$host$request_uri;
}
```

---

## 12. Post-Install Verification

### Health Check Commands

```bash
# Docker services
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

# MySQL
docker exec databridge-mysql mysqladmin ping -h localhost

# PostgreSQL
docker exec databridge-postgres-prod pg_isready -U databridge

# Redis
docker exec databridge-redis redis-cli ping

# ChromaDB
curl -s http://localhost:8001/api/v1/heartbeat

# NestJS Backend
curl -s http://localhost:3002/api/health

# MCP Server (if running)
python -c "from src.server import mcp; print(f'Tools: {len(mcp._tool_manager._tools)}')"

# UI Dashboard
curl -s http://localhost:5050/ | head -1

# All-in-one status check
python start_services.py --status
```

### License Verification

```bash
# Generate a license key
python scripts/generate_license.py PRO YOURCOMPANY 365

# Test the license system
python scripts/test_license_system.py

# Set the key in environment
export DATABRIDGE_LICENSE_KEY=DB-PRO-YOURCOMPANY-20270209-<signature>
```

### Run Test Suite

```bash
source .venv/bin/activate
pip install -e ".[dev]"
pytest tests/ -v
```

---

## 13. Tier-Specific Dependencies

### Community Edition (Free — 106 tools)

```
Python 3.10+
fastmcp >= 2.0.0
pydantic >= 2.0.0
pydantic-settings >= 2.0.0
pandas >= 2.0.0
sqlalchemy >= 2.0.0
rapidfuzz >= 3.0.0
Optional: pypdf, pytesseract, Pillow
```

### Pro Edition (Licensed — 284 tools)

Everything in CE, plus:
```
snowflake-connector-python >= 3.0.0
networkx >= 3.0.0
sentence-transformers >= 2.0.0
Optional: chromadb, langchain, snowflake-snowpark-python, prometheus-client
```

Infrastructure: ChromaDB (vector store), Snowflake (optional)

### Enterprise Edition (Custom — 348+ tools)

Everything in Pro, plus:
```
All optional dependencies
Custom agent configurations
White-label deployment support
On-premise infrastructure
Dedicated support SLA
```

---

## Quick Reference: Minimum Viable Production

For a production deployment running the **full stack**, you need at minimum:

| Component | Version | Install Method |
|-----------|---------|----------------|
| Ubuntu Server | 22.04 LTS | Base OS |
| Docker Engine | 24.0+ | apt (Docker repo) |
| Docker Compose | 2.20+ | Docker plugin |
| Python | 3.10+ | apt / deadsnakes PPA |
| Node.js | 20 LTS | Containerized (no host install needed) |
| MySQL | 8.0 | Docker container |
| Redis | 7 | Docker container |
| Git | 2.34+ | apt |

**Optional but recommended:**
- PostgreSQL 16 (analytics)
- ChromaDB (GraphRAG / Pro)
- Tesseract OCR
- Nginx (reverse proxy + TLS)
- Certbot (Let's Encrypt SSL)

---

*Document Version: 1.0.0 | DataBridge AI v0.40.0 | Last Updated: 2026-02-09*
