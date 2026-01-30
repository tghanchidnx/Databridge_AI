# DataBridge AI - User Documentation

## Complete Guide to Data Reconciliation & Hierarchy Management

---

## Table of Contents

1. [Introduction](#introduction)
2. [Installation & Setup](#installation--setup)
3. [Architecture Overview](#architecture-overview)
4. [Data Reconciliation Module](#data-reconciliation-module)
5. [Hierarchy Builder Module](#hierarchy-builder-module)
6. [MCP Tools Reference](#mcp-tools-reference)
7. [Sample Data & Use Cases](#sample-data--use-cases)
8. [Troubleshooting](#troubleshooting)

---

## Introduction

DataBridge AI is an enterprise-grade Model Context Protocol (MCP) application that provides two powerful modules:

1. **Data Reconciliation Engine** - Compare, match, and reconcile data across multiple sources with fuzzy matching, hashing, and conflict detection.

2. **Hierarchy Builder** - Create, manage, and deploy financial hierarchies with support for 15 levels, source mappings, formula groups, and database deployment scripts.

### Key Features

- **AI-Powered**: Works seamlessly with Claude and other frontier AI models via MCP
- **Enterprise-Ready**: Built for financial data, compliance, and audit requirements
- **Flexible Import/Export**: Support for CSV, JSON, SQL, and database connections
- **Comprehensive Validation**: Data integrity checks and conflict detection
- **Deployment Scripts**: Generate Snowflake-ready SQL scripts

---

## Installation & Setup

### Prerequisites

- Python 3.10 or higher
- pip (Python package manager)
- Claude Desktop (for AI integration)

### Step 1: Install Dependencies

```bash
cd C:\Users\telha\databridge_ai
pip install -r requirements.txt
```

### Step 2: Configure Environment

Create a `.env` file in the project root:

```env
APP_NAME=DataBridge AI
DATA_DIR=data
LOG_LEVEL=INFO
ENABLE_FUZZY_MATCHING=true
FUZZY_THRESHOLD=85.0
HASH_ALGORITHM=sha256
```

### Step 3: Configure Claude Desktop

Add to your Claude Desktop configuration file:

**Windows**: `%APPDATA%\Claude\claude_desktop_config.json`
**macOS**: `~/Library/Application Support/Claude/claude_desktop_config.json`

```json
{
  "mcpServers": {
    "databridge-ai": {
      "command": "python",
      "args": ["-m", "src.server"],
      "cwd": "C:\\Users\\telha\\databridge_ai",
      "env": {
        "PYTHONPATH": "C:\\Users\\telha\\databridge_ai"
      }
    }
  }
}
```

### Step 4: Start the Server

```bash
cd C:\Users\telha\databridge_ai
python -m src.server
```

Or using FastMCP development mode:

```bash
fastmcp dev src/server.py
```

---

## Architecture Overview

```
DataBridge AI
├── src/
│   ├── server.py              # Main MCP server (all tools registered here)
│   ├── config.py              # Pydantic settings configuration
│   └── hierarchy/             # Hierarchy Builder module
│       ├── __init__.py        # Module exports
│       ├── types.py           # Pydantic data models
│       ├── service.py         # Business logic & persistence
│       └── mcp_tools.py       # MCP tool definitions
├── data/                      # Runtime data storage
│   ├── hierarchy_projects.json
│   └── hierarchies.json
├── samples/                   # Sample data files
│   ├── customers_source_a.csv
│   ├── customers_source_b.csv
│   ├── products_inventory.csv
│   ├── chart_of_accounts.csv
│   ├── hierarchy_financial_report.csv
│   └── hierarchy_mapping.csv
├── docs/                      # Documentation
│   ├── MANIFEST.md
│   └── USER_DOCUMENTATION.md
└── tests/                     # Test suite
```

---

## Data Reconciliation Module

### Overview

The reconciliation module helps you compare data from multiple sources, identify discrepancies, and resolve conflicts.

### Core Capabilities

#### 1. Data Loading

Load data from multiple formats:

```
# CSV Files
"Load customer data from samples/customers_source_a.csv"

# JSON Files
"Load configuration from data/config.json"

# SQL Databases
"Connect to the sales database and load the orders table"
```

#### 2. Hash Comparison

Generate cryptographic hashes to detect data changes:

```
"Compare hashes between source A and source B customer files"
```

**Output includes:**
- Total records in each source
- Matching records (same hash)
- Orphan records (exist in one source only)
- Conflict records (same key, different data)

#### 3. Fuzzy Matching

Match records even with minor differences:

```
"Find fuzzy matches for customer names between the two sources with 85% threshold"
```

**Matching algorithms:**
- Levenshtein distance
- Token sort ratio
- Partial ratio
- Weighted composite score

#### 4. Conflict Detection

Identify and categorize data conflicts:

```
"Identify all conflicts in the customer reconciliation"
```

**Conflict types:**
- Value mismatch (same record, different values)
- Missing in source A/B
- Duplicate keys
- Schema differences

### Reconciliation Workflow

1. **Load Sources** - Import data from both sources
2. **Generate Hashes** - Create record fingerprints
3. **Compare** - Identify matches, orphans, conflicts
4. **Review** - Analyze discrepancies
5. **Resolve** - Apply fixes or document exceptions
6. **Export** - Generate reconciliation report

---

## Hierarchy Builder Module

### Overview

Build complex financial hierarchies for P&L reports, balance sheets, and management reporting.

### Core Concepts

#### Hierarchy Structure

```
Income Statement (Level 1)
├── Revenue (Level 2)
│   ├── Product Sales (Level 3)
│   │   ├── Hardware Sales (Level 4) [LEAF]
│   │   └── Software Sales (Level 4) [LEAF]
│   └── Service Revenue (Level 3)
│       ├── Consulting (Level 4) [LEAF]
│       └── Support (Level 4) [LEAF]
├── Expenses (Level 2)
│   ├── COGS (Level 3)
│   └── Operating (Level 3)
└── Net Income (Level 2) [CALCULATED]
```

#### Source Mappings

Link database columns to hierarchy leaf nodes:

| Hierarchy Node | Source Table | Column | Filter Value |
|---------------|--------------|--------|--------------|
| Hardware Sales | DIM_ACCOUNT | ACCOUNT_CODE | 4100-100 |
| Software Sales | DIM_ACCOUNT | ACCOUNT_CODE | 4100-200 |

#### Formula Groups

Define calculations for aggregated nodes:

```
Net Income = Total Revenue - Total Expenses

Formula Group: Net Income
├── Rule 1: ADD Total Revenue
└── Rule 2: SUBTRACT Total Expenses
```

### Creating a Hierarchy Project

#### Step 1: Create Project

```
"Create a new hierarchy project called 'Q4 Financial Report'
for fiscal year 2024"
```

#### Step 2: Define Root Hierarchy

```
"Create the Income Statement root hierarchy with description
'Main P&L for Q4 reporting'"
```

#### Step 3: Add Child Nodes

```
"Add Revenue as a child of Income Statement at level 2"
"Add Product Sales as a child of Revenue at level 3"
"Add Hardware Sales as a leaf node under Product Sales"
```

#### Step 4: Add Source Mappings

```
"Map Hardware Sales to DIM_ACCOUNT.ACCOUNT_CODE where value is 4100-100"
```

#### Step 5: Create Formula Groups

```
"Create formula group 'Net Income' that adds Total Revenue
and subtracts Total Expenses"
```

#### Step 6: Validate & Export

```
"Validate the Q4 Financial Report project"
"Export the hierarchy to CSV format"
"Generate Snowflake deployment scripts"
```

### Import Existing Hierarchies

Import from CSV files in legacy format:

```
"Import hierarchy from samples/hierarchy_financial_report.csv"
"Import source mappings from samples/hierarchy_mapping.csv"
```

### CSV Format Reference

#### Hierarchy CSV

| Column | Description | Required |
|--------|-------------|----------|
| HIERARCHY_ID | Unique identifier | Yes |
| HIERARCHY_NAME | Display name | Yes |
| PARENT_ID | Parent node ID (empty for root) | No |
| DESCRIPTION | Node description | No |
| LEVEL_1 through LEVEL_15 | Level names | No |
| INCLUDE_FLAG | Include in reports | Yes |
| EXCLUDE_FLAG | Exclude from reports | Yes |
| TRANSFORM_FLAG | Apply transformation | Yes |
| CALCULATION_FLAG | Is calculated node | Yes |
| ACTIVE_FLAG | Is active | Yes |
| IS_LEAF_NODE | Has source mappings | Yes |
| FORMULA_GROUP | Formula group name | No |
| SORT_ORDER | Display order | No |

#### Mapping CSV

| Column | Description | Required |
|--------|-------------|----------|
| HIERARCHY_ID | Target hierarchy node | Yes |
| MAPPING_INDEX | Order within hierarchy | Yes |
| SOURCE_DATABASE | Database name | Yes |
| SOURCE_SCHEMA | Schema name | Yes |
| SOURCE_TABLE | Table name | Yes |
| SOURCE_COLUMN | Column name | Yes |
| SOURCE_UID | Filter value | Yes |
| PRECEDENCE_GROUP | Precedence for conflicts | No |
| INCLUDE_FLAG | Include mapping | Yes |
| ACTIVE_FLAG | Is active | Yes |

---

## MCP Tools Reference

### Data Reconciliation Tools

| Tool | Description |
|------|-------------|
| `load_csv` | Load CSV file into memory |
| `load_json` | Load JSON file into memory |
| `connect_database` | Establish database connection |
| `generate_hashes` | Create record hashes |
| `compare_hashes` | Compare hashes between sources |
| `fuzzy_match` | Find fuzzy matches between datasets |
| `detect_conflicts` | Identify data conflicts |
| `export_results` | Export reconciliation results |
| `parse_pdf` | Extract data from PDF files |
| `manage_workflow` | Create/update reconciliation workflows |

### Hierarchy Builder Tools

| Tool | Description |
|------|-------------|
| `create_hierarchy_project` | Create new project |
| `list_hierarchy_projects` | List all projects |
| `get_hierarchy_project` | Get project details |
| `update_hierarchy_project` | Update project |
| `delete_hierarchy_project` | Delete project |
| `create_hierarchy` | Create hierarchy node |
| `get_hierarchy` | Get node details |
| `get_hierarchy_tree` | Get full tree structure |
| `update_hierarchy` | Update node |
| `delete_hierarchy` | Delete node |
| `add_source_mapping` | Add source mapping |
| `remove_source_mapping` | Remove mapping |
| `get_source_mappings` | List mappings |
| `create_formula_group` | Create formula group |
| `get_formula_groups` | List formula groups |
| `delete_formula_group` | Delete formula group |
| `import_hierarchy_csv` | Import from CSV |
| `export_hierarchy_csv` | Export to CSV |
| `generate_hierarchy_scripts` | Generate SQL scripts |
| `validate_hierarchy_project` | Validate project |

---

## Sample Data & Use Cases

### Sample Files Included

#### Reconciliation Samples

1. **customers_source_a.csv** - Customer master from System A
2. **customers_source_b.csv** - Customer master from System B
3. **products_inventory.csv** - Product inventory data

#### Hierarchy Samples

4. **chart_of_accounts.csv** - Chart of accounts reference
5. **hierarchy_financial_report.csv** - Pre-built P&L hierarchy
6. **hierarchy_mapping.csv** - Source mappings for hierarchy

### Use Case 1: Customer Data Reconciliation

**Scenario**: Compare customer data between CRM and ERP systems

```
1. "Load customers_source_a.csv as CRM data"
2. "Load customers_source_b.csv as ERP data"
3. "Compare hashes on customer_id between CRM and ERP"
4. "Show me the orphan records"
5. "Find fuzzy matches for customer names with 80% threshold"
6. "Export reconciliation report"
```

**Expected Results**:
- 2 orphans in Source A (C004, C009, C010 missing in B)
- 2 orphans in Source B (C011, C012 missing in A)
- Revenue and date conflicts for matching records

### Use Case 2: Building Income Statement Hierarchy

**Scenario**: Create P&L hierarchy for financial reporting

```
1. "Create project 'FY2024 P&L' for fiscal year 2024"
2. "Import hierarchy from hierarchy_financial_report.csv"
3. "Import mappings from hierarchy_mapping.csv"
4. "Create formula group 'Net Income' = Revenue - Expenses"
5. "Validate the project"
6. "Generate Snowflake deployment scripts"
```

**Expected Output**:
- Complete hierarchy with 17 nodes
- 16 source mappings to account codes
- Formula group for Net Income calculation
- SQL scripts for deployment

### Use Case 3: Monthly Close Reconciliation

**Scenario**: Month-end inventory reconciliation

```
1. "Load current inventory from products_inventory.csv"
2. "Connect to warehouse database"
3. "Load warehouse inventory counts"
4. "Compare inventory quantities by product_id"
5. "Flag items with variance > 5%"
6. "Generate exception report"
```

---

## Troubleshooting

### Common Issues

#### Server Won't Start

**Error**: `ModuleNotFoundError: No module named 'fastmcp'`

**Solution**: Install dependencies
```bash
pip install fastmcp pydantic pandas rapidfuzz
```

#### Claude Desktop Not Connecting

**Error**: MCP server not appearing in Claude

**Solutions**:
1. Verify path in `claude_desktop_config.json`
2. Ensure Python is in PATH
3. Restart Claude Desktop after config changes
4. Check server logs for errors

#### Import Errors

**Error**: `Failed to import hierarchy CSV`

**Solutions**:
1. Verify CSV column headers match expected format
2. Check for encoding issues (use UTF-8)
3. Ensure required columns are present
4. Validate data types (booleans, integers)

#### Database Connection Failed

**Error**: `Connection refused` or `Authentication failed`

**Solutions**:
1. Verify connection string format
2. Check firewall rules
3. Confirm credentials
4. Test connection independently

### Log Files

Logs are stored in the `data/` directory:
- `databridge.log` - Application log
- `reconciliation_history.json` - Reconciliation audit trail
- `hierarchy_projects.json` - Project data

### Getting Help

1. Check the [MANIFEST.md](MANIFEST.md) for tool specifications
2. Review error messages in server console
3. Enable debug logging: `LOG_LEVEL=DEBUG` in `.env`

---

## Hierarchy Viewer

### Overview

The Hierarchy Viewer provides a read-only visual tree interface for viewing hierarchies with their source mappings displayed as child nodes. It includes a persistent activity log that tracks all viewing actions.

**Note:** For editing hierarchies, use the **Hierarchy Knowledge Base** page which provides full CRUD operations, drag-drop reordering, and AI-powered editing.

### Accessing the Hierarchy Viewer

1. Navigate to **Hierarchy Viewer** from the sidebar
2. Select a project from the dropdown
3. View the tree structure with mappings displayed inline

### Features

#### Tree View with Mapping Children

Each hierarchy node displays its source mappings as expandable child nodes with color-coded tags:

| Tag Color | Field | Description |
|-----------|-------|-------------|
| **Purple** | Database | `source_database` - The source database name |
| **Blue** | Schema | `source_schema` - The database schema |
| **Green** | Table | `source_table` - The source table name |
| **Amber** | Column | `source_column` - The column being mapped |
| **Rose** | ID Value | `source_uid` - The specific value filter |

#### Navigation Controls

| Control | Description |
|---------|-------------|
| **Expand All** | Expand all hierarchy nodes and mappings |
| **Collapse All** | Collapse entire tree to root nodes |
| **Search** | Filter hierarchies by name |
| **Refresh** | Reload data from backend |

#### Statistics Dashboard

The top stats cards show:
- **Hierarchies** - Total number of hierarchy nodes
- **Root Nodes** - Number of top-level nodes
- **With Mappings** - Nodes that have source mappings
- **Total Mappings** - Sum of all mapping entries

#### Activity Log

The Activity Log panel (right side) provides:
- **Persistent logging** - Logs survive page refresh (stored in localStorage)
- **Timestamped entries** - Each action shows when it occurred
- **Color-coded types** - Info, errors, and actions are color-coded
- **Clear function** - Remove all log entries when needed

### For Editing

To make changes to hierarchies, use the **Hierarchy Knowledge Base** page which offers:
- Drag-and-drop reordering (hold Shift to drop as child)
- Multi-select for bulk operations
- AI Chat assistant for natural language commands
- Full CRUD operations (create, rename, move, delete)
- Mapping and formula configuration

---

## Quick Reference Card

### Reconciliation Commands

| Task | Command |
|------|---------|
| Load data | "Load [file] as [name]" |
| Compare | "Compare [source A] with [source B]" |
| Find matches | "Find fuzzy matches with [X]% threshold" |
| Export | "Export results to [format]" |

### Hierarchy Commands

| Task | Command |
|------|---------|
| Create project | "Create project [name] for [year]" |
| Add node | "Add [name] under [parent]" |
| Map source | "Map [node] to [table.column] = [value]" |
| Create formula | "Create formula [name] = [expression]" |
| Export | "Export hierarchy to CSV" |
| Deploy | "Generate deployment scripts" |

---

*DataBridge AI v1.0 - Enterprise Data Reconciliation & Hierarchy Management*
