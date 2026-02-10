# DataBridge AI

**Docker deployment for AI/UX enhanced data reconciliation and hierarchy management.**

## Overview

This is the Docker-based deployment of DataBridge AI for AI/UX enhanced data reconciliation and hierarchy management.

## Architecture

| Service | Container Port | Host Port | Notes |
|---------|----------------|-----------|-------|
| MySQL | 3306 | **3308** | Database: `databridge_ai_database` |
| NestJS Backend | 3000 | **3002** | API: `http://localhost:3002/api` |
| React Frontend | 5174 | **8080/8443** | UI: `http://localhost:8080` |
| Redis | 6379 | **6381** | Cache storage |
| Python MCP | stdio | N/A | Server: `DataBridgeAI` |

## Folder Structure

```
infrastructure/
├── frontend/          # React + Vite frontend (port 5174)
├── backend/           # NestJS backend (port 3002)
├── database/          # MySQL init scripts
├── mcp-server/        # Python MCP server
│   └── src/           # MCP tools and modules
├── docker-compose.yml # Docker orchestration
├── .env               # Environment config
└── README.md          # This file
```

## Quick Start

### 1. Start Services

```bash
cd T:\Users\telha\Databridge_AI_Source\infrastructure
docker-compose up -d
```

### 2. Verify Services

```bash
# Check all containers are running
docker-compose ps

# Check backend health
curl http://localhost:3002/api/health

# Open frontend
start http://localhost:8080
```

### 3. Run MCP Server (for Claude integration)

```bash
cd infrastructure/mcp-server
pip install -r requirements.txt
python -m src.server
```

## Service Ports

| Service | Internal Port | Host Port |
|---------|---------|---------|
| MySQL | 3307 | 3308 |
| Backend | 3001 | 3002 |
| Frontend HTTP | 80 | 8080 |
| Frontend HTTPS | 443 | 8443 |
| Redis | 6380 | 6381 |

## Environment Configuration

Key environment variables in `.env`:

```env
# Database
MYSQL_DATABASE=databridge_ai_database
MYSQL_USER=databridge
MYSQL_PASSWORD=DataBridge2026!

# Backend
NESTJS_BACKEND_URL=http://localhost:3002/api
API_KEYS=dev-key-1,dev-key-2

# Frontend
VITE_API_URL=/api
VITE_APP_NAME=DataBridge AI
```

## Features Roadmap

### Phase 1: Foundation & Quick Wins
- [ ] Loading skeletons
- [ ] Toast notifications with undo
- [ ] Keyboard shortcuts system
- [ ] Floating quick actions toolbar

### Phase 2: Mapping Coverage & Visualization
- [ ] Mapping coverage heatmap
- [ ] Breadcrumb navigation
- [ ] Progress dashboard

### Phase 3: CSV Import Intelligence
- [ ] Smart CSV analyzer
- [ ] Format auto-detection
- [ ] Split-screen diff view

### Phase 4: AI-Powered Features
- [ ] Mapping suggestions with Claude
- [ ] Formula auto-generation
- [ ] Natural language hierarchy builder
- [ ] Embedded AI chat assistant

### Phase 5: Advanced Features
- [ ] Anomaly detection
- [ ] Visual canvas builder
- [ ] Template gallery

## Development

### Running in Development Mode

```bash
# Backend (with hot reload)
cd infrastructure/backend
npm install
npm run start:dev

# Frontend (with hot reload)
cd infrastructure/frontend
npm install
npm run dev
```

### Database Migration

```bash
cd infrastructure/backend
npx prisma migrate dev
npx prisma generate
```

### Running Tests

```bash
# Backend tests
cd infrastructure/backend
npm run test

# Frontend tests
cd infrastructure/frontend
npm run test

# MCP server tests
cd infrastructure/mcp-server
pytest
```

## Troubleshooting

### Port Already in Use

If a port is already in use:

```bash
# Find process using port
netstat -ano | findstr :3002

# Kill process
taskkill /PID <pid> /F
```

### Docker Cleanup

```bash
# Stop and remove containers
docker-compose down

# Remove volumes (CAUTION: deletes data)
docker-compose down -v

# Rebuild images
docker-compose build --no-cache
```

### Database Connection Issues

```bash
# Connect to MySQL
mysql -h localhost -P 3308 -u databridge -p

# Check database exists
SHOW DATABASES;
USE databridge_ai_database;
```

## Contributing

1. Create feature branch from `main`
2. Implement changes in infrastructure folder
3. Test services independently
4. Submit PR

## License

Proprietary - DataNexum

---

*Created: January 2026*
*Version: 2.0.0*
