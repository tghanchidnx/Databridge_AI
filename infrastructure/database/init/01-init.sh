#!/bin/bash
# ============================================================================
# MySQL Initialization Script
# This script runs automatically when MySQL container starts for first time
# ============================================================================
# IMPORTANT: These credentials must match docker-compose.yml MySQL environment
# Values from docker-compose.yml:
#   MYSQL_ROOT_PASSWORD=DataBridge2026!Root
#   MYSQL_DATABASE=databridge_ai_database
#   MYSQL_USER=databridge
#   MYSQL_PASSWORD=DataBridge2026!
# ============================================================================

set -e

echo "=========================================="
echo "DataBridge AI MySQL Initialization"
echo "=========================================="

# Create database if not exists
mysql -u root -p"$MYSQL_ROOT_PASSWORD" <<-EOSQL
    CREATE DATABASE IF NOT EXISTS \${MYSQL_DATABASE} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    
    -- Grant privileges to application user
    GRANT ALL PRIVILEGES ON \${MYSQL_DATABASE}.* TO '\${MYSQL_USER}'@'%';
    
    -- Create additional users if needed
    CREATE USER IF NOT EXISTS 'readonly'@'%' IDENTIFIED BY 'ReadOnly2025!';
    GRANT SELECT ON \${MYSQL_DATABASE}.* TO 'readonly'@'%';
    
    FLUSH PRIVILEGES;
    
    USE \${MYSQL_DATABASE};
    
    -- Verify database
    SELECT 'Database created successfully' AS status;
EOSQL

echo "=========================================="
echo "MySQL initialization completed!"
echo "Database: $MYSQL_DATABASE"
echo "User: $MYSQL_USER"
echo "=========================================="
