#!/bin/bash
# DataBridge AI Health Check Script
# Usage: ./healthcheck.sh [environment]
#
# Checks the health of all DataBridge AI services.

set -e

ENV="${1:-dev}"
TIMEOUT="${2:-5}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo ""
echo "🏥 DataBridge AI Health Check ($ENV)"
echo "========================================"
echo ""

# Function to check HTTP endpoint
check_http() {
    local name="$1"
    local url="$2"

    if curl -sf --max-time "$TIMEOUT" "$url" > /dev/null 2>&1; then
        echo -e "${GREEN}✅${NC} $name: Healthy"
        return 0
    else
        echo -e "${RED}❌${NC} $name: Unhealthy"
        return 1
    fi
}

# Function to check TCP port
check_tcp() {
    local name="$1"
    local host="$2"
    local port="$3"

    if nc -z -w "$TIMEOUT" "$host" "$port" > /dev/null 2>&1; then
        echo -e "${GREEN}✅${NC} $name: Healthy (port $port)"
        return 0
    else
        echo -e "${RED}❌${NC} $name: Unhealthy (port $port)"
        return 1
    fi
}

# Track overall status
ALL_HEALTHY=true

# Check PostgreSQL
if ! check_tcp "PostgreSQL" "localhost" "5432"; then
    ALL_HEALTHY=false
fi

# Check Redis
if ! check_tcp "Redis" "localhost" "6379"; then
    ALL_HEALTHY=false
fi

# Check ChromaDB
if ! check_http "ChromaDB" "http://localhost:8001/api/v1/heartbeat"; then
    ALL_HEALTHY=false
fi

# Check V3 Hierarchy Builder
if ! check_http "V3 Hierarchy Builder" "http://localhost:8000/health"; then
    ALL_HEALTHY=false
fi

# Check V4 Analytics Engine
if ! check_http "V4 Analytics Engine" "http://localhost:8001/health"; then
    ALL_HEALTHY=false
fi

echo ""
echo "========================================"

if [ "$ALL_HEALTHY" = true ]; then
    echo -e "${GREEN}✅ All services are healthy!${NC}"
    exit 0
else
    echo -e "${RED}⚠️  Some services are unhealthy.${NC}"
    exit 1
fi
