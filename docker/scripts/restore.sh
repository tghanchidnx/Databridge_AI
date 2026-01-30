#!/bin/bash
# DataBridge AI Restore Script
# Usage: ./restore.sh <backup_dir> [timestamp]
#
# Restores DataBridge AI databases from backup.

set -e

BACKUP_DIR="${1:?Usage: ./restore.sh <backup_dir> [timestamp]}"
TIMESTAMP="${2:-$(ls -t "$BACKUP_DIR"/backup_*.json 2>/dev/null | head -1 | sed 's/.*backup_//' | sed 's/.json//')}"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo ""
echo "📥 DataBridge AI Restore"
echo "========================="
echo "Backup Dir: $BACKUP_DIR"
echo "Timestamp: $TIMESTAMP"
echo ""

# Check manifest
MANIFEST="$BACKUP_DIR/backup_$TIMESTAMP.json"
if [ ! -f "$MANIFEST" ]; then
    echo -e "${RED}❌ Manifest not found: $MANIFEST${NC}"
    echo ""
    echo "Available backups:"
    ls -la "$BACKUP_DIR"/backup_*.json 2>/dev/null || echo "  No backups found"
    exit 1
fi

echo "Manifest found. Proceeding with restore..."
echo ""

# Confirm
read -p "⚠️  This will overwrite existing data. Continue? [y/N] " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Restore cancelled."
    exit 0
fi

echo ""

# Restore PostgreSQL
POSTGRES_BACKUP="$BACKUP_DIR/postgres_$TIMESTAMP.sql.gz"
if [ -f "$POSTGRES_BACKUP" ]; then
    echo "📦 Restoring PostgreSQL..."

    # Try production container first, then dev
    if gunzip -c "$POSTGRES_BACKUP" | docker exec -i databridge-postgres-prod psql -U postgres databridge_analytics 2>/dev/null; then
        echo -e "${GREEN}✅${NC} PostgreSQL restored"
    elif gunzip -c "$POSTGRES_BACKUP" | docker exec -i databridge-postgres psql -U postgres databridge_analytics 2>/dev/null; then
        echo -e "${GREEN}✅${NC} PostgreSQL restored (dev)"
    else
        echo -e "${YELLOW}⚠️${NC} PostgreSQL restore failed"
    fi
else
    echo -e "${YELLOW}⚠️${NC} PostgreSQL backup not found: $POSTGRES_BACKUP"
fi

# Restore Redis
REDIS_BACKUP="$BACKUP_DIR/redis_$TIMESTAMP.rdb"
if [ -f "$REDIS_BACKUP" ]; then
    echo "📦 Restoring Redis..."

    # Stop Redis, copy file, restart
    if docker cp "$REDIS_BACKUP" databridge-redis-prod:/data/dump.rdb 2>/dev/null; then
        docker restart databridge-redis-prod 2>/dev/null || true
        echo -e "${GREEN}✅${NC} Redis restored"
    elif docker cp "$REDIS_BACKUP" databridge-redis:/data/dump.rdb 2>/dev/null; then
        docker restart databridge-redis 2>/dev/null || true
        echo -e "${GREEN}✅${NC} Redis restored (dev)"
    else
        echo -e "${YELLOW}⚠️${NC} Redis restore failed"
    fi
else
    echo -e "${YELLOW}⚠️${NC} Redis backup not found: $REDIS_BACKUP"
fi

# Restore V3 SQLite
V3_BACKUP="$BACKUP_DIR/v3_databridge_$TIMESTAMP.db"
if [ -f "$V3_BACKUP" ]; then
    echo "📦 Restoring V3 SQLite..."

    if docker cp "$V3_BACKUP" databridge-v3-prod:/app/data/databridge.db 2>/dev/null; then
        echo -e "${GREEN}✅${NC} V3 SQLite restored"
    elif docker cp "$V3_BACKUP" databridge-v3:/app/data/databridge.db 2>/dev/null; then
        echo -e "${GREEN}✅${NC} V3 SQLite restored (dev)"
    else
        echo -e "${YELLOW}⚠️${NC} V3 SQLite restore failed"
    fi
else
    echo -e "${YELLOW}⚠️${NC} V3 SQLite backup not found: $V3_BACKUP"
fi

echo ""
echo "========================="
echo -e "${GREEN}✅ Restore completed!${NC}"
echo ""
echo "Please verify your services are running correctly:"
echo "  ./healthcheck.sh"
