#!/bin/bash
# DataBridge AI Quick Start Script
# Usage: ./quickstart.sh [dev|prod]
#
# Quick setup and start of DataBridge AI.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"
ENV="${1:-dev}"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "${BLUE}🚀 DataBridge AI Quick Start${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Environment: $ENV"
echo "Docker Dir:  $DOCKER_DIR"
echo ""

# Check Docker
echo "Checking prerequisites..."
if ! command -v docker &> /dev/null; then
    echo -e "${RED}❌ Docker is not installed.${NC}"
    echo "Please install Docker: https://docs.docker.com/get-docker/"
    exit 1
fi
echo -e "${GREEN}✅${NC} Docker is installed"

if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}❌ Docker Compose is not installed.${NC}"
    echo "Please install Docker Compose: https://docs.docker.com/compose/install/"
    exit 1
fi
echo -e "${GREEN}✅${NC} Docker Compose is installed"

# Check if Docker daemon is running
if ! docker info &> /dev/null; then
    echo -e "${RED}❌ Docker daemon is not running.${NC}"
    echo "Please start Docker Desktop or the Docker service."
    exit 1
fi
echo -e "${GREEN}✅${NC} Docker daemon is running"

echo ""

# Setup environment if needed
ENV_FILE="$DOCKER_DIR/.env"
if [ ! -f "$ENV_FILE" ]; then
    echo "Setting up environment..."
    "$SCRIPT_DIR/setup-env.sh" "$ENV"
    echo ""
fi

# Select compose file
if [ "$ENV" = "prod" ]; then
    COMPOSE_FILE="$DOCKER_DIR/docker-compose.prod.yml"
else
    COMPOSE_FILE="$DOCKER_DIR/docker-compose.dev.yml"
fi

if [ ! -f "$COMPOSE_FILE" ]; then
    echo -e "${RED}❌ Compose file not found: $COMPOSE_FILE${NC}"
    exit 1
fi

# Start services
echo "Starting DataBridge AI services..."
echo ""

cd "$DOCKER_DIR"

if [ "$ENV" = "dev" ]; then
    # Dev: Start infrastructure only by default
    echo "Starting infrastructure services (PostgreSQL, Redis, ChromaDB)..."
    docker-compose -f "$COMPOSE_FILE" up -d postgres redis chromadb

    echo ""
    echo "To start all services including V3 and V4:"
    echo "  docker-compose -f $COMPOSE_FILE --profile full up -d"
else
    # Prod: Start all services
    docker-compose -f "$COMPOSE_FILE" up -d
fi

# Wait for services to start
echo ""
echo "Waiting for services to initialize..."
sleep 10

# Health check
echo ""
echo "Running health checks..."
"$SCRIPT_DIR/healthcheck.sh" "$ENV" || true

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "${GREEN}✅ DataBridge AI is running!${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "Service URLs:"
echo "  • PostgreSQL:  localhost:5432"
echo "  • Redis:       localhost:6379"
echo "  • ChromaDB:    http://localhost:8001"
if [ "$ENV" = "prod" ]; then
    echo "  • V3 Builder:  http://localhost:8000"
    echo "  • V4 Analytics: http://localhost:8001"
fi
echo ""
echo "Useful commands:"
echo "  • View logs:    docker-compose -f $COMPOSE_FILE logs -f"
echo "  • Stop:         docker-compose -f $COMPOSE_FILE down"
echo "  • Health check: $SCRIPT_DIR/healthcheck.sh"
echo "  • Backup:       $SCRIPT_DIR/backup.sh"
echo ""
