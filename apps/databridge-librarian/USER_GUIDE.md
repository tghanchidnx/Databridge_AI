# Headless DataBridge AI - User Guide

**Version:** 3.0.0
**MCP Server:** Headless Databridge_AI - Python

---

## Quick Start

### Installation

```bash
# Navigate to librarian directory
cd C:\Users\telha\Databridge_AI\librarian

# Create virtual environment
python -m venv venv
.\venv\Scripts\activate   # Windows
source venv/bin/activate  # Linux/macOS

# Install dependencies
pip install -r requirements.txt

# Optional: Install database connectors & OCR
pip install -r requirements-optional.txt
```

### First Run

```bash
# Initialize the database
databridge init

# Check version
databridge --version

# Get help
databridge --help
```

---

## CLI Commands Reference

### Project Management

```bash
# Create a new project
databridge project create "My Hierarchy" --description "FY2024 P&L"

# List all projects
databridge project list

# Show project details
databridge project show <project-id>

# Delete a project
databridge project delete <project-id>
```

### Hierarchy Operations

```bash
# Create hierarchy node
databridge hierarchy create <project-id> "Revenue" \
    --level-1 "Income" \
    --level-2 "Product Sales"

# List hierarchies in project
databridge hierarchy list <project-id>

# Show as tree
databridge hierarchy tree <project-id>

# Update hierarchy
databridge hierarchy update <hierarchy-id> --name "New Name"

# Delete hierarchy
databridge hierarchy delete <hierarchy-id>
```

### Source Mappings

```bash
# Add a source mapping
databridge mapping add <hierarchy-id> \
    --database ANALYTICS \
    --schema PUBLIC \
    --table DIM_PRODUCT \
    --column PRODUCT_ID

# List mappings
databridge mapping list <hierarchy-id>

# Remove mapping by index
databridge mapping remove <hierarchy-id> --index 0

# View mapping summary
databridge mapping summary <project-id>
```

### CSV Import/Export

```bash
# Import hierarchy CSV
databridge csv import hierarchy <project-id> hierarchy.csv

# Import legacy format (older CSV structure)
databridge csv import hierarchy <project-id> old_hierarchy.csv --legacy

# Import mapping CSV
databridge csv import mapping <project-id> mapping.csv

# Export hierarchy to CSV
databridge csv export hierarchy <project-id> --output ./exports/

# Export mappings to CSV
databridge csv export mapping <project-id> --output ./exports/
```

### Data Reconciliation

```bash
# Load CSV file
databridge reconcile load csv sales_2024.csv

# Load JSON file
databridge reconcile load json data.json

# Profile data source
databridge reconcile profile <source-name>

# Compare two sources
databridge reconcile compare source_a source_b --keys id,date

# View orphans (records in one source only)
databridge reconcile orphans

# View conflicts (same key, different values)
databridge reconcile conflicts

# Fuzzy match column
databridge reconcile fuzzy <source-name> <column> --threshold 80

# Deduplicate column
databridge reconcile dedupe <source-name> <column>

# Transform column
databridge reconcile transform <source-name> <column> --op upper
# Operations: upper, lower, strip, trim_spaces, remove_special
```

### Templates

```bash
# List all templates
databridge template list

# Filter by domain
databridge template list --domain accounting

# Filter by industry
databridge template list --industry "Oil & Gas"

# Show template details
databridge template show standard_pl

# Create project from template
databridge template create-project standard_pl "Q1 Report"

# Save project as template
databridge template save <project-id> "my_custom_template"
```

### Skills

```bash
# List available skills
databridge skill list

# Show skill details
databridge skill show financial-analyst

# Get skill system prompt
databridge skill prompt financial-analyst
```

### Knowledge Base

```bash
# List client profiles
databridge kb client list

# Create client profile
databridge kb client create "Acme Corp" --industry manufacturing

# Show client knowledge
databridge kb client show <client-id>

# Add custom prompt for client
databridge kb prompt add <client-id> "Always use GAAP accounting standards"

# Update client knowledge
databridge kb update <client-id> --field preferences --value '{"currency":"USD"}'
```

### Database Connections

```bash
# Create Snowflake connection
databridge connection create prod-snowflake \
    --type snowflake \
    --account xy12345.us-east-1 \
    --user myuser \
    --warehouse COMPUTE_WH

# Create MySQL connection
databridge connection create local-mysql \
    --type mysql \
    --host localhost \
    --port 3306 \
    --database mydb

# List connections
databridge connection list

# Test connection
databridge connection test <connection-id>

# Browse database schema
databridge connection browse <connection-id>
databridge connection browse <connection-id> --database mydb
databridge connection browse <connection-id> --database mydb --schema public
databridge connection browse <connection-id> --database mydb --schema public --table users
```

### Deployment

```bash
# Generate INSERT script
databridge deploy generate <project-id> <connection-id> --type insert

# Generate VIEW script
databridge deploy generate <project-id> <connection-id> --type view

# Generate all scripts
databridge deploy generate <project-id> <connection-id> --type all

# Execute deployment
databridge deploy execute <project-id> <connection-id>

# View deployment history
databridge deploy history <project-id>
```

### Interactive Mode

```bash
# Start interactive shell
databridge shell

# In the shell:
databridge> help
databridge> project list
databridge> hierarchy tree <project-id>
databridge> exit
```

---

## Using with Claude (MCP)

### Configure Claude Desktop

Edit your Claude Desktop configuration file:

**Windows:** `%APPDATA%\Claude\claude_desktop_config.json`
**macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "headless-databridge": {
      "command": "python",
      "args": [
        "-m",
        "databridge.mcp.server"
      ],
      "cwd": "C:\\Users\\telha\\Databridge_AI\\librarian",
      "env": {
        "DATABRIDGE_DB_PATH": "C:\\Users\\telha\\Databridge_AI\\librarian\\data\\databridge.db"
      }
    }
  }
}
```

### MCP Tools Available (92 Total)

#### Hierarchy Tools (41)
- `create_hierarchy_project`, `list_hierarchy_projects`, `delete_hierarchy_project`
- `create_hierarchy`, `get_hierarchy`, `update_hierarchy`, `delete_hierarchy`
- `get_hierarchy_tree`, `get_hierarchy_children`, `get_hierarchy_path`
- `add_source_mapping`, `remove_source_mapping`, `get_mapping_summary`
- `import_hierarchy_csv`, `import_mapping_csv`, `export_hierarchy_csv`, `export_mapping_csv`
- `create_formula_group`, `add_formula_rule`, `list_formula_groups`
- And 21 more...

#### Reconciliation Tools (20)
- `load_csv`, `load_json`, `query_database`
- `profile_data`, `detect_schema_drift`
- `compare_hashes`, `get_orphan_details`, `get_conflict_details`
- `fuzzy_match_columns`, `fuzzy_deduplicate`
- `extract_text_from_pdf`, `ocr_image`, `parse_table_from_text`
- And 7 more...

#### Template Tools (16)
- `list_financial_templates`, `get_template_details`, `create_project_from_template`
- `list_available_skills`, `get_skill_details`, `get_skill_prompt`
- `list_client_profiles`, `get_client_knowledge`, `update_client_knowledge`
- And 7 more...

#### Connection Tools (8)
- `list_backend_connections`, `test_backend_connection`
- `get_connection_databases`, `get_connection_schemas`
- `get_connection_tables`, `get_connection_columns`
- And 2 more...

#### Schema/Data Tools (7)
- `compare_database_schemas`, `generate_merge_sql_script`
- `compare_table_data`, `get_data_comparison_summary`
- And 3 more...

### Example Claude Prompts

```
"Create a new hierarchy project called 'FY2024 Financial Report'"

"List all available P&L templates for oil and gas industry"

"Import this CSV file into my project" (attach file)

"Show me the hierarchy tree for project xyz"

"Compare the sales_2024.csv with budget_2024.csv on the order_id column"

"Find similar customer names in the customer column with 85% threshold"

"Generate Snowflake deployment scripts for my hierarchy"
```

---

## CSV File Formats

### Hierarchy CSV (PROJECT_HIERARCHY.CSV)

| Column | Description | Required |
|--------|-------------|----------|
| HIERARCHY_ID | Unique identifier | Yes |
| HIERARCHY_NAME | Display name | Yes |
| PARENT_ID | Parent hierarchy ID | No |
| DESCRIPTION | Description | No |
| LEVEL_1 through LEVEL_10 | Hierarchy level values | No |
| LEVEL_1_SORT through LEVEL_10_SORT | Sort order per level | No |
| INCLUDE_FLAG | Include in reports | No |
| EXCLUDE_FLAG | Exclude from reports | No |
| SORT_ORDER | Overall sort order | No |

### Mapping CSV (PROJECT_HIERARCHY_MAPPING.CSV)

| Column | Description | Required |
|--------|-------------|----------|
| HIERARCHY_ID | Links to hierarchy | Yes |
| MAPPING_INDEX | Order of mapping | Yes |
| SOURCE_DATABASE | Database name | Yes |
| SOURCE_SCHEMA | Schema name | Yes |
| SOURCE_TABLE | Table name | Yes |
| SOURCE_COLUMN | Column name | Yes |
| SOURCE_UID | Specific value filter | No |
| PRECEDENCE_GROUP | Grouping for precedence | No |
| INCLUDE_FLAG | Include flag | No |
| EXCLUDE_FLAG | Exclude flag | No |

---

## Configuration (.env)

```ini
# Database
DATABRIDGE_DB_PATH=data/databridge.db
DATABRIDGE_DATA_DIR=data

# Logging
DATABRIDGE_AUDIT_LOG=data/audit_trail.csv
DATABRIDGE_WORKFLOW_FILE=data/workflow.json

# Display limits
DATABRIDGE_MAX_ROWS_DISPLAY=10
DATABRIDGE_FUZZY_THRESHOLD=80

# Optional: V2 Backend Sync
NESTJS_BACKEND_URL=http://localhost:3002/api
NESTJS_API_KEY=v2-dev-key-1
NESTJS_SYNC_ENABLED=false

# Optional: Snowflake
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_DATABASE=
SNOWFLAKE_SCHEMA=

# Optional: Tesseract OCR
TESSERACT_PATH=C:/Program Files/Tesseract-OCR/tesseract.exe
```

---

## Troubleshooting

### Common Issues

**1. "Module not found" error**
```bash
# Ensure virtual environment is activated
.\venv\Scripts\activate  # Windows
pip install -r requirements.txt
```

**2. Database connection fails**
```bash
# Test connection
databridge connection test <connection-id>

# Check credentials in .env file
```

**3. OCR not working**
```bash
# Install Tesseract OCR
# Windows: Download from https://github.com/UB-Mannheim/tesseract/wiki
# Set TESSERACT_PATH in .env
```

**4. Snowflake authentication error**
```bash
# For OAuth: Use databridge connection create with --auth oauth
# For password: Ensure account format is correct (xy12345.us-east-1)
```

---

## Support

- **Issues:** https://github.com/your-org/databridge-cli/issues
- **Documentation:** See PLAN.md for full architecture details
- **MCP Guide:** See docs/MCP_GUIDE.md for Claude integration details

---

*Last Updated: 2025-01-28*
