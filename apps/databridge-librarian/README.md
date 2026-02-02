# DataBridge Librarian

The MCP (Model Context Protocol) server component of DataBridge AI, providing **92+ tools** for data reconciliation, hierarchy management, and automated deployments.

## Features

- **Data Reconciliation** - Compare CSV, JSON, SQL, PDF sources with hash-based and fuzzy matching
- **Hierarchy Builder** - Create multi-level hierarchical structures (up to 15 levels)
- **Source Mappings** - Link database columns to hierarchy nodes with precedence rules
- **Formula Engine** - Build calculated nodes with SUM, SUBTRACT, MULTIPLY, DIVIDE operations
- **Template System** - 20 pre-built templates for financial statements and operational hierarchies
- **AI Skills** - 7 expertise profiles for domain-specific analysis
- **Auto-Sync** - Automatic synchronization with NestJS backend
- **Git Automation** - Commit dbt projects and create deployment PRs

## Installation

```bash
# From repository root
pip install -e "apps/databridge-librarian[dev]"

# Or with specific extras
pip install -e "apps/databridge-librarian[ocr,snowflake]"
```

## MCP Configuration

Add to your MCP client settings (Claude Desktop, Claude Code, etc.):

```json
{
  "mcpServers": {
    "databridge-librarian": {
      "command": "python",
      "args": ["-m", "fastmcp", "run", "apps/databridge-librarian/src/mcp/server.py"],
      "cwd": "/path/to/databridge-ai",
      "env": {
        "DATABRIDGE_BACKEND_URL": "http://localhost:8001",
        "DATABRIDGE_AUTO_SYNC": "true"
      }
    }
  }
}
```

## MCP Tools Reference

### Data Loading & Profiling

| Tool | Description |
|------|-------------|
| `load_csv` | Load CSV file with schema preview |
| `load_json` | Load JSON file (array or object) |
| `query_database` | Execute SQL query with preview |
| `profile_data` | Analyze structure and data quality |
| `detect_schema_drift` | Compare schemas between two CSVs |

### Data Comparison

| Tool | Description |
|------|-------------|
| `compare_hashes` | Hash-based row comparison (orphans, conflicts) |
| `get_orphan_details` | Retrieve orphan record details |
| `get_conflict_details` | Show conflicting records side-by-side |
| `fuzzy_match_columns` | RapidFuzz column matching |
| `fuzzy_deduplicate` | Find potential duplicates |

### OCR & PDF Processing

| Tool | Description |
|------|-------------|
| `extract_text_from_pdf` | Extract text from PDF pages |
| `ocr_image` | Tesseract OCR on images |
| `parse_table_from_text` | Parse tabular data from text |

### Hierarchy Management

| Tool | Description |
|------|-------------|
| `create_hierarchy_project` | Create new hierarchy project |
| `list_hierarchy_projects` | List all projects with stats |
| `get_hierarchy_project` | Get project details |
| `delete_hierarchy_project` | Delete project and hierarchies |
| `create_hierarchy` | Create hierarchy node |
| `update_hierarchy` | Update hierarchy properties |
| `delete_hierarchy` | Remove hierarchy node |
| `get_hierarchy_tree` | Get complete tree structure |
| `list_hierarchies` | List all hierarchies in project |

### Source Mappings

| Tool | Description |
|------|-------------|
| `add_source_mapping` | Map database column to hierarchy |
| `remove_source_mapping` | Remove mapping by index |
| `get_inherited_mappings` | Get mappings including children |
| `get_mapping_summary` | Project-wide mapping overview |
| `get_mappings_by_precedence` | Filter by precedence group |

### Formulas

| Tool | Description |
|------|-------------|
| `create_formula_group` | Create calculated hierarchy |
| `add_formula_rule` | Add operation to formula |
| `list_formula_groups` | List all formula hierarchies |

### Import/Export

| Tool | Description |
|------|-------------|
| `export_hierarchy_csv` | Export structure to CSV |
| `export_mapping_csv` | Export mappings to CSV |
| `import_hierarchy_csv` | Import from CSV |
| `import_mapping_csv` | Import mappings from CSV |
| `export_project_json` | Full project backup |

### Deployment

| Tool | Description |
|------|-------------|
| `generate_hierarchy_scripts` | Generate INSERT/VIEW SQL |
| `generate_deployment_scripts` | Backend SQL generation |
| `push_hierarchy_to_snowflake` | Deploy to Snowflake |
| `get_deployment_history` | View deployment logs |

### Git Automation

| Tool | Description |
|------|-------------|
| `commit_dbt_project` | Generate and commit dbt project |
| `create_deployment_pr` | Create PR from deployment branch |
| `commit_deployment_scripts` | Commit SQL scripts to branch |

### Database Connections

| Tool | Description |
|------|-------------|
| `list_backend_connections` | List all connections |
| `test_backend_connection` | Test connectivity |
| `get_connection_databases` | List databases |
| `get_connection_schemas` | List schemas |
| `get_connection_tables` | List tables |
| `get_connection_columns` | Get column metadata |
| `get_column_distinct_values` | Sample distinct values |

### Schema & Data Comparison

| Tool | Description |
|------|-------------|
| `compare_database_schemas` | Cross-connection schema diff |
| `compare_table_data` | Row-level data comparison |
| `get_data_comparison_summary` | Summary statistics |
| `generate_merge_sql_script` | Generate MERGE SQL |

## Templates

### Accounting Domain (10 templates)

| Template ID | Name | Industry |
|-------------|------|----------|
| `standard_pl` | Standard P&L | General |
| `standard_bs` | Standard Balance Sheet | General |
| `oil_gas_los` | Oil & Gas LOS | Oil & Gas |
| `upstream_oil_gas_pl` | Upstream Oil & Gas P&L | Oil & Gas - E&P |
| `midstream_oil_gas_pl` | Midstream Oil & Gas P&L | Oil & Gas - Midstream |
| `oilfield_services_pl` | Oilfield Services P&L | Oil & Gas - Services |
| `manufacturing_pl` | Industrial Manufacturing P&L | Manufacturing |
| `industrial_services_pl` | Industrial Services P&L | Industrial Services |
| `saas_pl` | SaaS Company P&L | SaaS |
| `transportation_pl` | Transportation & Logistics P&L | Transportation |

### Operations Domain (8 templates)

| Template ID | Name | Industry |
|-------------|------|----------|
| `geographic_hierarchy` | Geographic Hierarchy | General |
| `department_hierarchy` | Department Hierarchy | General |
| `asset_hierarchy` | Asset Class Hierarchy | General |
| `legal_entity_hierarchy` | Legal Entity Hierarchy | General |
| `upstream_field_hierarchy` | Upstream Field Hierarchy | Oil & Gas - E&P |
| `midstream_asset_hierarchy` | Midstream Asset Hierarchy | Oil & Gas - Midstream |
| `manufacturing_plant_hierarchy` | Manufacturing Plant Hierarchy | Manufacturing |
| `fleet_hierarchy` | Fleet & Route Hierarchy | Transportation |

### Finance Domain (2 templates)

| Template ID | Name | Industry |
|-------------|------|----------|
| `cost_center_hierarchy` | Cost Center Hierarchy | General |
| `profit_center_hierarchy` | Profit Center Hierarchy | General |

## Skills (AI Expertise Profiles)

| Skill ID | Name | Industries |
|----------|------|------------|
| `financial-analyst` | Financial Analyst | General |
| `manufacturing-analyst` | Manufacturing Analyst | Manufacturing |
| `fpa-oil-gas-analyst` | FP&A Oil & Gas Analyst | Oil & Gas |
| `fpa-cost-analyst` | FP&A Cost Analyst | General, Manufacturing, SaaS |
| `saas-metrics-analyst` | SaaS Metrics Analyst | SaaS, Technology |
| `operations-analyst` | Operations Analyst | General, Manufacturing, Transportation |
| `transportation-analyst` | Transportation Analyst | Transportation, Logistics |

## CSV Import/Export Format

### Hierarchy CSV (`_HIERARCHY.CSV`)

| Column | Description |
|--------|-------------|
| HIERARCHY_ID | Unique identifier |
| HIERARCHY_NAME | Display name |
| PARENT_ID | Parent hierarchy ID |
| DESCRIPTION | Optional description |
| LEVEL_1 - LEVEL_10 | Hierarchy level values |
| LEVEL_1_SORT - LEVEL_10_SORT | Sort order for each level |
| INCLUDE_FLAG | Include in calculations |
| CALCULATION_FLAG | Is calculated node |
| ACTIVE_FLAG | Is active |
| IS_LEAF_NODE | Is leaf node |
| FORMULA_GROUP | Formula group name |
| SORT_ORDER | Overall sort order |

### Mapping CSV (`_HIERARCHY_MAPPING.CSV`)

| Column | Description |
|--------|-------------|
| HIERARCHY_ID | Links to hierarchy |
| MAPPING_INDEX | Order of mapping |
| SOURCE_DATABASE | Database name |
| SOURCE_SCHEMA | Schema name |
| SOURCE_TABLE | Table name |
| SOURCE_COLUMN | Column name |
| SOURCE_UID | Specific value filter |
| PRECEDENCE_GROUP | Precedence grouping |
| INCLUDE_FLAG | Include mapping |
| ACTIVE_FLAG | Is active |

## Development

### Running Tests

```bash
# All tests
pytest apps/databridge-librarian/tests -v

# Unit tests only
pytest apps/databridge-librarian/tests/unit -v

# With coverage
pytest apps/databridge-librarian/tests --cov=src --cov-report=html
```

### Testing MCP Server

```bash
# Interactive testing with FastMCP
fastmcp dev apps/databridge-librarian/src/mcp/server.py

# List tools
python -c "from src.mcp.server import mcp; print(f'{len(mcp._tool_manager._tools)} tools')"
```

### Project Structure

```
apps/databridge-librarian/
├── src/
│   ├── mcp/
│   │   ├── server.py           # FastMCP server entry
│   │   └── tools/              # MCP tool definitions
│   │       ├── data_reconciliation.py
│   │       ├── hierarchy.py
│   │       ├── templates.py
│   │       └── git_automation.py
│   ├── hierarchy/              # Hierarchy service layer
│   ├── source/                 # Source discovery
│   ├── sql_generator/          # SQL/dbt generation
│   └── core/                   # Core utilities
├── tests/
│   ├── unit/
│   └── integration/
└── data/                       # Local storage (workflow, audit)
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `DATABRIDGE_BACKEND_URL` | NestJS backend URL | `http://localhost:8001` |
| `DATABRIDGE_AUTO_SYNC` | Enable auto-sync | `true` |
| `DATABRIDGE_DATA_DIR` | Local data directory | `./data` |
| `SNOWFLAKE_ACCOUNT` | Snowflake account | - |
| `SNOWFLAKE_USER` | Snowflake username | - |
| `SNOWFLAKE_PASSWORD` | Snowflake password | - |

## License

MIT License - see [LICENSE](../../LICENSE) for details.
