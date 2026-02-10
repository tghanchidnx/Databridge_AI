# DataBridge AI V2 - Continued Documentation

> **Last Updated:** 2026-01-27
> **Version:** 2.0.0
> **Ports:** Backend: 3002 | Frontend: 5174 | MySQL: 3308

---

## Table of Contents

1. [Change Log](#change-log)
2. [Features Overview](#features-overview)
3. [API Endpoints Reference](#api-endpoints-reference)
4. [CLI Testing Commands](#cli-testing-commands)
5. [User Guide](#user-guide)
6. [MCP Tools Reference](#mcp-tools-reference)
7. [Troubleshooting](#troubleshooting)

---

## Change Log

### 2026-01-27 (Latest) - Smart Value Matching & Display Columns

#### Features Added:
1. **Automatic Value Matching**
   - When a mapping has a `source_uid` value, automatically checks if it exists in the reference table
   - If value exists: Shows "Value Matched" with green checkmark, auto-selects the value
   - If value doesn't exist: Shows "Value Not Found" error with option to select a different value
   - Message: "Value doesn't exist, please select or add it to your source table"

2. **Smart Column Mapping Flow**
   - If a mapping's column is not found in reference tables, pencil icon shows amber color
   - Step 1: Select a reference table to map to
   - Step 2: Select a column from that table to map the source column
   - Step 3: Select filter values with multi-select checkboxes
   - Option to apply mapping to all similar column names in hierarchy
   - Column mappings stored in localStorage for persistence

3. **Display Columns as Children**
   - Select additional columns to display as children under each selected value
   - DisplayColumnValues component fetches and displays related data
   - Values shown with column name and value under each filter selection
   - Display column configuration stored in localStorage

4. **UI States**
   - `value-matched`: Green checkmark, value auto-selected, can add display columns
   - `value-not-found`: Red X, warning message, shows available values to select
   - `select-values`: Standard multi-select checkbox list
   - `select-table`: List of reference tables to choose from
   - `select-column`: List of columns from selected table

#### Files Modified:
- `v2/frontend/src/pages/HierarchyViewerPage.tsx` - Value matching, display columns, multi-step flow

---

### 2026-01-27 - Reference Tables & Hierarchy Viewer Enhancements

#### Features Added:
1. **Reference Tables System**
   - Upload CSV files as virtual dimension/reference tables
   - Store in database with column type detection
   - Query tables with filtering and distinct values
   - CRUD operations for reference tables

2. **Hierarchy Viewer Enhancements**
   - Tree expand/collapse with parent-child resolution
   - Mappings shown as children with colored tags (column, table, schema, database)
   - Pencil icon opens Dialog to select filter values from reference tables
   - Multi-select checkboxes for value selection
   - Save and Cancel buttons in selection dialog
   - **Selected values displayed as children under each mapping** (with green checkmarks)
   - "Apply to all hierarchies with same table" option
   - Selections persist in database per user/project/hierarchy

3. **Session Persistence**
   - Project selection saved to localStorage
   - Persists across page refreshes
   - Shows placeholder if saved project no longer exists

4. **CLI Validation Scripts**
   - `v2/scripts/validate-v2.ps1` - PowerShell validation script
   - `v2/scripts/validate-v2.sh` - Bash validation script
   - Quick commands for testing all V2 services

#### Files Modified:
- `v2/backend/prisma/schema.prisma` - Added ReferenceTable, HierarchyViewerSelection models
- `v2/backend/src/modules/smart-hierarchy/services/reference-table.service.ts` - NEW
- `v2/backend/src/modules/smart-hierarchy/controllers/smart-hierarchy.controller.ts` - Added endpoints
- `v2/backend/src/modules/smart-hierarchy/smart-hierarchy.module.ts` - Added service
- `v2/frontend/src/services/api/hierarchy/reference-table.service.ts` - NEW
- `v2/frontend/src/components/hierarchy-knowledge-base/dialogs/ReferenceTablesDialog.tsx` - NEW
- `v2/frontend/src/pages/HierarchyViewerPage.tsx` - Major updates

---

## Features Overview

### 1. Hierarchy Knowledge Base
- Create and manage hierarchy projects
- Import/export hierarchies via CSV
- Visual tree editor with drag-and-drop
- Source mapping configuration
- Formula groups and calculations
- Filter conditions

### 2. Reference Tables
- Upload dimension/lookup CSV files
- Auto-detect column types (string, number, boolean, date)
- Query with filtering and pagination
- Get distinct values for dropdowns
- Link to hierarchy viewer selections

### 3. Connections Management
- Snowflake, MySQL, PostgreSQL, SQL Server support
- Test connection functionality
- Browse databases, schemas, tables, columns
- Schema metadata caching

### 4. Hierarchy Viewer
- Read-only tree visualization
- Expandable/collapsible nodes
- Search and filter
- Mapping display with metadata tags
- Reference table value selection
- Session-persistent project selection

---

## API Endpoints Reference

### Health Check
```
GET /api/health
```

### Projects
```
GET    /api/hierarchy-projects           # List all projects
POST   /api/hierarchy-projects           # Create project
GET    /api/hierarchy-projects/:id       # Get project details
PUT    /api/hierarchy-projects/:id       # Update project
DELETE /api/hierarchy-projects/:id       # Delete project
```

### Smart Hierarchy
```
GET    /api/smart-hierarchy/project/:projectId              # List hierarchies
POST   /api/smart-hierarchy                                  # Create hierarchy
GET    /api/smart-hierarchy/:id                              # Get hierarchy
PUT    /api/smart-hierarchy/:id                              # Update hierarchy
DELETE /api/smart-hierarchy/:id                              # Delete hierarchy
POST   /api/smart-hierarchy/import/csv                       # Import CSV
GET    /api/smart-hierarchy/export/csv/:projectId            # Export CSV
```

### Reference Tables
```
POST   /api/smart-hierarchy/reference-tables                 # Create from CSV
GET    /api/smart-hierarchy/reference-tables                 # List all tables
GET    /api/smart-hierarchy/reference-tables/:tableName      # Get table info
POST   /api/smart-hierarchy/reference-tables/:tableName/query # Query table
GET    /api/smart-hierarchy/reference-tables/:tableName/distinct/:columnName # Distinct values
DELETE /api/smart-hierarchy/reference-tables/:tableName      # Delete table
```

### Viewer Selections
```
POST   /api/smart-hierarchy/viewer-selections                # Save selection
GET    /api/smart-hierarchy/viewer-selections/:projectId     # Get selections
DELETE /api/smart-hierarchy/viewer-selections/:projectId/:hierarchyId/:tableName/:columnName
```

### Connections
```
GET    /api/connections                   # List connections
POST   /api/connections                   # Create connection
GET    /api/connections/:id               # Get connection
PUT    /api/connections/:id               # Update connection
DELETE /api/connections/:id               # Delete connection
POST   /api/connections/:id/test          # Test connection
GET    /api/connections/:id/databases     # List databases
GET    /api/connections/:id/schemas       # List schemas
GET    /api/connections/:id/tables        # List tables
GET    /api/connections/:id/columns       # List columns
```

---

## CLI Testing Commands

### Prerequisites
```powershell
# Set base URL
$API_URL = "http://localhost:3002/api"

# Get auth token (replace with actual login)
$TOKEN = "your-jwt-token-here"

# Common headers
$HEADERS = @{
    "Content-Type" = "application/json"
    "Authorization" = "Bearer $TOKEN"
}
```

### Health Check
```powershell
# Check if backend is running
curl http://localhost:3002/api/health

# Check frontend
curl http://localhost:5174 -o nul -w "%{http_code}"
```

### Project Management
```powershell
# List all projects
curl -X GET "$API_URL/hierarchy-projects" -H "Authorization: Bearer $TOKEN"

# Create a new project
curl -X POST "$API_URL/hierarchy-projects" `
  -H "Content-Type: application/json" `
  -H "Authorization: Bearer $TOKEN" `
  -d '{"name": "Test Project", "description": "CLI created project"}'

# Get project by ID
curl -X GET "$API_URL/hierarchy-projects/{project-id}" -H "Authorization: Bearer $TOKEN"

# Delete project
curl -X DELETE "$API_URL/hierarchy-projects/{project-id}" -H "Authorization: Bearer $TOKEN"
```

### Reference Tables
```powershell
# List reference tables
curl -X GET "$API_URL/smart-hierarchy/reference-tables" -H "Authorization: Bearer $TOKEN"

# Upload a reference table from CSV file
$csvContent = Get-Content -Path "v2/UploadFiles/dim_product.csv" -Raw
$body = @{
    name = "dim_product"
    displayName = "Product Dimension"
    csvContent = $csvContent
    sourceFile = "dim_product.csv"
} | ConvertTo-Json

curl -X POST "$API_URL/smart-hierarchy/reference-tables" `
  -H "Content-Type: application/json" `
  -H "Authorization: Bearer $TOKEN" `
  -d $body

# Get distinct values from a column
curl -X GET "$API_URL/smart-hierarchy/reference-tables/dim_product/distinct/PRODUCT_CODE" `
  -H "Authorization: Bearer $TOKEN"

# Query reference table
curl -X POST "$API_URL/smart-hierarchy/reference-tables/dim_product/query" `
  -H "Content-Type: application/json" `
  -H "Authorization: Bearer $TOKEN" `
  -d '{"tableName": "dim_product", "limit": 10}'

# Delete reference table
curl -X DELETE "$API_URL/smart-hierarchy/reference-tables/dim_product" `
  -H "Authorization: Bearer $TOKEN"
```

### Hierarchy Operations
```powershell
# List hierarchies in a project
curl -X GET "$API_URL/smart-hierarchy/project/{project-id}" -H "Authorization: Bearer $TOKEN"

# Import hierarchy from CSV
curl -X POST "$API_URL/smart-hierarchy/import/csv" `
  -H "Authorization: Bearer $TOKEN" `
  -F "hierarchyFile=@v2/UploadFiles/hierarchy.csv" `
  -F "mappingFile=@v2/UploadFiles/mapping.csv" `
  -F "projectId={project-id}"

# Export hierarchy to CSV
curl -X GET "$API_URL/smart-hierarchy/export/csv/{project-id}" `
  -H "Authorization: Bearer $TOKEN" `
  -o "exported_hierarchy.zip"
```

### Connection Testing
```powershell
# List connections
curl -X GET "$API_URL/connections" -H "Authorization: Bearer $TOKEN"

# Test a connection
curl -X POST "$API_URL/connections/{connection-id}/test" -H "Authorization: Bearer $TOKEN"

# Get databases for a connection
curl -X GET "$API_URL/connections/{connection-id}/databases" -H "Authorization: Bearer $TOKEN"
```

### Bash Equivalents
```bash
# Set variables
API_URL="http://localhost:3002/api"
TOKEN="your-jwt-token-here"

# Health check
curl -s http://localhost:3002/api/health | jq .

# List projects
curl -s -X GET "$API_URL/hierarchy-projects" \
  -H "Authorization: Bearer $TOKEN" | jq .

# Upload reference table
curl -s -X POST "$API_URL/smart-hierarchy/reference-tables" \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d "{
    \"name\": \"dim_account\",
    \"displayName\": \"Account Dimension\",
    \"csvContent\": \"$(cat v2/UploadFiles/DIM_ACCOUNT.csv)\",
    \"sourceFile\": \"DIM_ACCOUNT.csv\"
  }" | jq .

# Get distinct values
curl -s -X GET "$API_URL/smart-hierarchy/reference-tables/dim_account/distinct/ACCOUNT_CODE" \
  -H "Authorization: Bearer $TOKEN" | jq .
```

### Quick Validation Script
```powershell
# validate-v2.ps1 - Quick validation of V2 services

Write-Host "=== DataBridge AI V2 Validation ===" -ForegroundColor Cyan

# Check backend
Write-Host "`n[1] Checking Backend (port 3002)..." -ForegroundColor Yellow
try {
    $health = Invoke-RestMethod -Uri "http://localhost:3002/api/health" -Method Get
    Write-Host "    Backend: OK - Database: $($health.data.database.status)" -ForegroundColor Green
} catch {
    Write-Host "    Backend: FAILED" -ForegroundColor Red
}

# Check frontend
Write-Host "`n[2] Checking Frontend (port 5174)..." -ForegroundColor Yellow
try {
    $response = Invoke-WebRequest -Uri "http://localhost:5174" -UseBasicParsing
    if ($response.StatusCode -eq 200) {
        Write-Host "    Frontend: OK" -ForegroundColor Green
    }
} catch {
    Write-Host "    Frontend: FAILED" -ForegroundColor Red
}

# Check database
Write-Host "`n[3] Checking MySQL (port 3308)..." -ForegroundColor Yellow
try {
    $result = mysql -h localhost -P 3308 -u root -proot -e "SELECT 1" 2>$null
    Write-Host "    MySQL: OK" -ForegroundColor Green
} catch {
    Write-Host "    MySQL: Check manually" -ForegroundColor Yellow
}

Write-Host "`n=== Validation Complete ===" -ForegroundColor Cyan
```

### Bash Validation Script
```bash
#!/bin/bash
# validate-v2.sh - Quick validation of V2 services

echo "=== DataBridge AI V2 Validation ==="

# Check backend
echo -e "\n[1] Checking Backend (port 3002)..."
if curl -s http://localhost:3002/api/health | grep -q '"status":"ok"'; then
    echo "    Backend: OK"
else
    echo "    Backend: FAILED"
fi

# Check frontend
echo -e "\n[2] Checking Frontend (port 5174)..."
if curl -s -o /dev/null -w "%{http_code}" http://localhost:5174 | grep -q "200"; then
    echo "    Frontend: OK"
else
    echo "    Frontend: FAILED"
fi

# Check database
echo -e "\n[3] Checking MySQL (port 3308)..."
if mysql -h localhost -P 3308 -u root -proot -e "SELECT 1" &>/dev/null; then
    echo "    MySQL: OK"
else
    echo "    MySQL: Check manually"
fi

echo -e "\n=== Validation Complete ==="
```

---

## User Guide

### Getting Started

1. **Access the Application**
   - Frontend: http://localhost:5174
   - Login with your credentials

2. **Create a Project**
   - Navigate to "Projects" in the sidebar
   - Click "New Project"
   - Enter name and description

3. **Import Hierarchies**
   - Open your project
   - Click "Import CSV"
   - Upload hierarchy CSV and mapping CSV files

4. **Upload Reference Tables**
   - Go to "Hierarchy Viewer"
   - Click "Reference Tables" button
   - Drag and drop dimension CSV files (DIM_*.csv)

5. **View and Filter Hierarchies**
   - Select a project in Hierarchy Viewer
   - Expand tree nodes to see structure
   - Click pencil icon on mappings to filter by reference table values

6. **Map Columns to Reference Tables**
   - If a column is not automatically matched (pencil icon shows amber):
     - Click the pencil icon to open the mapping dialog
     - **Step 1**: Select a reference table from the list
     - **Step 2**: Select a column to map to
     - Check "Apply this mapping to all similar columns" to auto-map similar mappings
     - **Step 3**: Select filter values with checkboxes
     - Optionally select additional columns to display as children
     - Click "Save Selection" to apply

### Column Mapping States
| Icon Color | Description |
|------------|-------------|
| Gray | Column matched to reference table |
| Amber | Column not matched - click to configure mapping |
| Green checkmarks | Values selected for filtering |

### Keyboard Shortcuts
| Key | Action |
|-----|--------|
| `/` | Focus search |
| `Escape` | Close dialogs |
| `Enter` | Confirm/Save |

### Tips
- Use "Expand All" to see full hierarchy structure
- Reference table selections persist across sessions
- Use "Apply to all" when selecting values for consistent filtering
- Column mappings are stored in localStorage and persist across sessions
- Amber pencil icons indicate columns that need manual mapping
- Use "Apply this mapping to all" checkbox to quickly map similar columns

---

## MCP Tools Reference

### Hierarchy Tools
| Tool | Description |
|------|-------------|
| `create_hierarchy_project` | Create a new hierarchy project |
| `list_hierarchy_projects` | List all projects |
| `get_hierarchy_tree` | Get hierarchical tree structure |
| `create_hierarchy` | Create a hierarchy node |
| `update_hierarchy` | Update hierarchy properties |
| `delete_hierarchy` | Delete a hierarchy node |
| `add_source_mapping` | Add source mapping to hierarchy |
| `remove_source_mapping` | Remove source mapping |
| `import_hierarchy_csv` | Import from CSV files |
| `export_hierarchy_csv` | Export to CSV files |

### Connection Tools
| Tool | Description |
|------|-------------|
| `list_backend_connections` | List all database connections |
| `get_connection_databases` | Get databases for connection |
| `get_connection_tables` | Get tables in database |
| `compare_database_schemas` | Compare two schemas |

### Template Tools
| Tool | Description |
|------|-------------|
| `list_financial_templates` | List available templates |
| `get_template_details` | Get template structure |
| `create_project_from_template` | Create project from template |

---

## Troubleshooting

### Common Issues

#### Backend Not Starting
```powershell
# Check if port is in use
netstat -an | findstr "3002"

# Check logs
cd v2/backend
npm run start:dev
```

#### Frontend Build Errors
```powershell
# Clear cache and reinstall
cd v2/frontend
rm -rf node_modules
npm install
npm run dev
```

#### Database Connection Issues
```powershell
# Check MySQL is running
docker ps | findstr mysql

# Check database exists
mysql -h localhost -P 3308 -u root -proot -e "SHOW DATABASES"
```

#### Reference Tables Not Showing
1. Ensure CSV files are uploaded via Reference Tables dialog
2. Check browser console for errors
3. Verify table names match mapping source_table values

#### Pencil Icon Not Appearing
- Reference tables must be uploaded first
- Mapping's `source_table` must match a reference table name (case-insensitive)
- Check if the table was successfully created

### Log Locations
- Backend logs: Console output or `v2/backend/logs/`
- Frontend logs: Browser developer console (F12)
- Database logs: Docker logs for MySQL container

---

## Appendix

### Environment Variables
```env
# infrastructure/.env
DATABASE_URL="mysql://root:root@localhost:3308/databridge_ai_database"
JWT_SECRET="your-secret-key"
PORT=3002
```

### Database Schema (Key Tables)
```
reference_tables
├── id (UUID)
├── user_id
├── name
├── display_name
├── columns (JSON)
├── data (LongText - JSON stringified rows)
├── row_count
└── created_at / updated_at

hierarchy_viewer_selections
├── id (UUID)
├── user_id
├── project_id
├── hierarchy_id
├── table_name
├── column_name
├── selected_values (JSON array)
├── apply_to_all (boolean)
└── created_at / updated_at
```

### File Upload Limits
- CSV files: 50MB max
- Chunked upload supported for large files

---

*This documentation is updated with each feature change. Check the Change Log for latest updates.*
