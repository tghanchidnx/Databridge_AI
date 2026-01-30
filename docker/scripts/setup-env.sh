#!/bin/bash
# DataBridge AI Environment Setup Script
# Usage: ./setup-env.sh [environment]
#
# Sets up environment configuration for DataBridge AI deployment.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER_DIR="$(dirname "$SCRIPT_DIR")"
ENV="${1:-prod}"

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo ""
echo "⚙️  DataBridge AI Environment Setup"
echo "===================================="
echo "Environment: $ENV"
echo ""

ENV_FILE="$DOCKER_DIR/.env"
ENV_EXAMPLE="$DOCKER_DIR/.env.example"

# Check if .env already exists
if [ -f "$ENV_FILE" ]; then
    echo -e "${YELLOW}⚠️  .env file already exists.${NC}"
    read -p "Overwrite? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Setup cancelled."
        exit 0
    fi
fi

# Generate secure passwords
generate_password() {
    openssl rand -base64 24 | tr -dc 'a-zA-Z0-9' | head -c 24
}

POSTGRES_PASSWORD=$(generate_password)
REDIS_PASSWORD=$(generate_password)
V3_ENCRYPTION_KEY=$(openssl rand -hex 32)
V4_ENCRYPTION_KEY=$(openssl rand -hex 32)

# Create .env file
cat > "$ENV_FILE" << EOF
# DataBridge AI Environment Configuration
# Generated: $(date -Iseconds)
# Environment: $ENV

# ============================================
# PostgreSQL Configuration
# ============================================
POSTGRES_USER=databridge
POSTGRES_PASSWORD=$POSTGRES_PASSWORD
POSTGRES_DB=databridge_analytics
POSTGRES_PORT=5432

# ============================================
# Redis Configuration
# ============================================
REDIS_PASSWORD=$REDIS_PASSWORD
REDIS_PORT=6379

# ============================================
# ChromaDB Configuration
# ============================================
CHROMADB_PORT=8001
# CHROMA_AUTH_PROVIDER=
# CHROMA_AUTH_CREDENTIALS=

# ============================================
# DataBridge V3 Configuration
# ============================================
V3_PORT=8000
V3_ENCRYPTION_KEY=$V3_ENCRYPTION_KEY
V3_IMAGE=ghcr.io/databridge-ai/databridge-v3:latest

# ============================================
# DataBridge V4 Configuration
# ============================================
V4_PORT=8001
V4_ENCRYPTION_KEY=$V4_ENCRYPTION_KEY
V4_IMAGE=ghcr.io/databridge-ai/databridge-v4:latest

# ============================================
# Notion Integration (Optional)
# ============================================
# NOTION_API_KEY=

# ============================================
# Logging
# ============================================
LOG_LEVEL=INFO
EOF

echo -e "${GREEN}✅${NC} Environment file created: $ENV_FILE"

# Show generated credentials
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "${BLUE}Generated Credentials (save securely!)${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "PostgreSQL:"
echo "  User:     databridge"
echo "  Password: $POSTGRES_PASSWORD"
echo "  Database: databridge_analytics"
echo ""
echo "Redis:"
echo "  Password: $REDIS_PASSWORD"
echo ""
echo "Encryption Keys:"
echo "  V3: $V3_ENCRYPTION_KEY"
echo "  V4: $V4_ENCRYPTION_KEY"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo -e "${GREEN}✅ Environment setup complete!${NC}"
echo ""
echo "Next steps:"
echo "  1. Review and customize .env file"
echo "  2. Start services: python deploy.py up --env $ENV"
echo "  3. Check health: python deploy.py health"
