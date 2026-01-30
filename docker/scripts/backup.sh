#!/bin/bash
# DataBridge AI Backup Script
# Usage: ./backup.sh [output_dir]
#
# Creates backups of all DataBridge AI databases.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"
OUTPUT_DIR="${1:-$DOCKER_DIR/backups}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo ""
echo "💾 DataBridge AI Backup"
echo "========================"
echo "Timestamp: $TIMESTAMP"
echo "Output: $OUTPUT_DIR"
echo ""

# Create backup directory
mkdir -p "$OUTPUT_DIR"

# Backup PostgreSQL
echo "📦 Backing up PostgreSQL..."
POSTGRES_BACKUP="$OUTPUT_DIR/postgres_$TIMESTAMP.sql.gz"

if docker exec databridge-postgres-prod pg_dump -U postgres databridge_analytics | gzip > "$POSTGRES_BACKUP" 2>/dev/null; then
    echo -e "${GREEN}✅${NC} PostgreSQL backup: $POSTGRES_BACKUP"
else
    # Try dev container
    docker exec databridge-postgres pg_dump -U postgres databridge_analytics | gzip > "$POSTGRES_BACKUP" 2>/dev/null || {
        echo -e "${YELLOW}⚠️${NC} PostgreSQL backup failed (container may not be running)"
    }
fi

# Backup Redis
echo "📦 Backing up Redis..."
REDIS_BACKUP="$OUTPUT_DIR/redis_$TIMESTAMP.rdb"

if docker exec databridge-redis-prod cat /data/dump.rdb > "$REDIS_BACKUP" 2>/dev/null; then
    echo -e "${GREEN}✅${NC} Redis backup: $REDIS_BACKUP"
else
    # Try dev container
    docker exec databridge-redis cat /data/dump.rdb > "$REDIS_BACKUP" 2>/dev/null || {
        echo -e "${YELLOW}⚠️${NC} Redis backup failed (container may not be running)"
    }
fi

# Backup V3 SQLite database
echo "📦 Backing up V3 SQLite..."
V3_BACKUP="$OUTPUT_DIR/v3_databridge_$TIMESTAMP.db"

if docker cp databridge-v3-prod:/app/data/databridge.db "$V3_BACKUP" 2>/dev/null; then
    echo -e "${GREEN}✅${NC} V3 SQLite backup: $V3_BACKUP"
else
    # Try dev container
    docker cp databridge-v3:/app/data/databridge.db "$V3_BACKUP" 2>/dev/null || {
        echo -e "${YELLOW}⚠️${NC} V3 SQLite backup failed (container may not be running)"
    }
fi

# Create manifest
MANIFEST="$OUTPUT_DIR/backup_$TIMESTAMP.json"
cat > "$MANIFEST" << EOF
{
    "timestamp": "$TIMESTAMP",
    "date": "$(date -Iseconds)",
    "files": {
        "postgresql": "postgres_$TIMESTAMP.sql.gz",
        "redis": "redis_$TIMESTAMP.rdb",
        "v3_sqlite": "v3_databridge_$TIMESTAMP.db"
    },
    "version": "1.0.0"
}
EOF
echo -e "${GREEN}✅${NC} Manifest: $MANIFEST"

# Calculate total size
TOTAL_SIZE=$(du -sh "$OUTPUT_DIR"/*_$TIMESTAMP* 2>/dev/null | tail -1 | cut -f1)

echo ""
echo "========================"
echo -e "${GREEN}✅ Backup completed!${NC}"
echo "Total size: $TOTAL_SIZE"
echo "Location: $OUTPUT_DIR"
echo ""

# List backups
echo "Backup files:"
ls -lh "$OUTPUT_DIR"/*_$TIMESTAMP* 2>/dev/null || true
