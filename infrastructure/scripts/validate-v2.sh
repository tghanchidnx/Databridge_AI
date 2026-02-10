#!/bin/bash
# DataBridge AI V2 - Validation Script
# Usage: ./validate-v2.sh [token]

API_URL="http://localhost:3002/api"
FRONTEND_URL="http://localhost:5174"
TOKEN="${1:-}"

echo ""
echo "========================================"
echo "  DataBridge AI V2 - Validation Tool   "
echo "========================================"
echo ""

# 1. Backend Health Check
echo "[1/5] Backend Health Check..."
HEALTH=$(curl -s --connect-timeout 5 "$API_URL/health" 2>/dev/null)
if echo "$HEALTH" | grep -q '"status":"ok"'; then
    echo "      Status: OK"
    echo "      $(echo $HEALTH | grep -o '"database":{[^}]*}')"
else
    echo "      Status: FAILED - Backend not responding"
    echo "      Run: cd v2/backend && npm run start:dev"
fi

# 2. Frontend Check
echo ""
echo "[2/5] Frontend Check..."
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 5 "$FRONTEND_URL" 2>/dev/null)
if [ "$HTTP_CODE" = "200" ]; then
    echo "      Status: OK (HTTP 200)"
else
    echo "      Status: FAILED - Frontend not responding"
    echo "      Run: cd v2/frontend && npm run dev"
fi

# 3. Database Check
echo ""
echo "[3/5] MySQL Database Check (port 3308)..."
if command -v mysql &> /dev/null; then
    if mysql -h localhost -P 3308 -u root -proot -e "SELECT 1" &>/dev/null; then
        echo "      Status: OK"
    else
        echo "      Status: FAILED - Cannot connect"
    fi
else
    echo "      Status: Skipped (mysql client not found)"
fi

# 4. API Endpoints
echo ""
echo "[4/5] API Endpoints Check..."
if [ -z "$TOKEN" ]; then
    echo "      Skipped (no token provided)"
    echo "      Tip: Run with token as argument"
else
    # Test projects
    PROJECTS=$(curl -s -H "Authorization: Bearer $TOKEN" "$API_URL/hierarchy-projects" 2>/dev/null)
    if echo "$PROJECTS" | grep -q "id"; then
        COUNT=$(echo "$PROJECTS" | grep -o '"id"' | wc -l)
        echo "      Projects API: OK ($COUNT projects)"
    else
        echo "      Projects API: FAILED"
    fi

    # Test reference tables
    TABLES=$(curl -s -H "Authorization: Bearer $TOKEN" "$API_URL/smart-hierarchy/reference-tables" 2>/dev/null)
    if echo "$TABLES" | grep -q "statusCode.*401"; then
        echo "      Reference Tables API: Auth Failed"
    elif echo "$TABLES" | grep -q "id"; then
        COUNT=$(echo "$TABLES" | grep -o '"id"' | wc -l)
        echo "      Reference Tables API: OK ($COUNT tables)"
    else
        echo "      Reference Tables API: Check response"
    fi
fi

# 5. File System Check
echo ""
echo "[5/5] File System Check..."
UPLOAD_PATH="v2/UploadFiles"
if [ -d "$UPLOAD_PATH" ]; then
    CSV_COUNT=$(ls -1 "$UPLOAD_PATH"/*.csv 2>/dev/null | wc -l)
    echo "      Upload folder: OK ($CSV_COUNT CSV files)"

    DIM_FILES=$(ls -1 "$UPLOAD_PATH"/dim*.csv "$UPLOAD_PATH"/DIM*.csv 2>/dev/null)
    if [ -n "$DIM_FILES" ]; then
        echo "      DIM files found:"
        echo "$DIM_FILES" | while read f; do echo "        - $(basename $f)"; done
    fi
else
    echo "      Upload folder: Not found"
fi

echo ""
echo "========================================"
echo "  Validation Complete                  "
echo "========================================"
echo ""
echo "Quick Commands:"
echo "  Start Backend:  cd v2/backend && npm run start:dev"
echo "  Start Frontend: cd v2/frontend && npm run dev"
echo "  Open App:       xdg-open http://localhost:5174"
echo ""
