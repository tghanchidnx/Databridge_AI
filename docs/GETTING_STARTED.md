# Getting Started with DataBridge AI

A comprehensive tutorial to help you get up and running with DataBridge AI, the headless MCP-native data reconciliation and hierarchy management engine.

---

## Table of Contents

1. [Introduction](#1-introduction)
2. [Installation & Setup](#2-installation--setup)
3. [Quick Start: Building a P&L Hierarchy](#3-quick-start-building-a-pl-hierarchy)
4. [Wright Pipeline Builder](#4-wright-pipeline-builder)
5. [dbt Integration](#5-dbt-integration)
6. [Using the Dashboard UI](#6-using-the-dashboard-ui)
7. [Next Steps](#7-next-steps)

---

## 1. Introduction

### What is DataBridge AI?

DataBridge AI is a **headless, MCP-native data reconciliation engine** with **348 MCP tools** designed to bridge messy data sources (OCR, PDF, SQL) with structured comparison pipelines. It provides a complete toolkit for:

- **Data Reconciliation** - Compare, match, and validate data across sources
- **Hierarchy Management** - Build and manage hierarchical reporting structures
- **Data Mart Generation** - Automated 4-object pipeline creation
- **AI Integration** - Snowflake Cortex AI for intelligent data processing
- **dbt Workflows** - Generate dbt projects with AI-powered documentation

### Key Modules

| Module | Tools | Description |
|--------|-------|-------------|
| **Wright Builder** | 31 | Hierarchy-driven data mart generation (VW_1, DT_2, DT_3A, DT_3 pipeline) |
| **Hierarchy Builder** | 49 | Create, manage, and deploy hierarchical data structures |
| **dbt Integration** | 8 | Generate dbt projects, models, and CI/CD pipelines |
| **Data Catalog** | 19 | Centralized metadata registry with business glossary |
| **Cortex AI** | 26 | Snowflake Cortex integration for AI-powered analysis |
| **Data Observability** | 15 | Real-time metrics, alerting, anomaly detection |
| **GraphRAG Engine** | 10 | Anti-hallucination layer with graph + vector retrieval |
| **Data Quality** | 7 | Expectation suites and validation runners |
| **Lineage** | 11 | Column-level lineage tracking and impact analysis |

### Architecture Overview

```
                           +------------------+
                           |   Claude / LLM   |
                           +--------+---------+
                                    |
                           +--------v---------+
                           |   MCP Protocol   |
                           +--------+---------+
                                    |
+-----------------------------------v-----------------------------------+
|                         DataBridge AI Engine                          |
|  +-------------+  +-------------+  +-------------+  +-------------+  |
|  |   Wright    |  | Hierarchies |  |     dbt     |  |   Catalog   |  |
|  |   Builder   |  |   Builder   |  | Integration |  |   Manager   |  |
|  +-------------+  +-------------+  +-------------+  +-------------+  |
+-----------------------------------+-----------------------------------+
                                    |
                    +---------------+---------------+
                    |               |               |
              +-----v-----+   +-----v-----+   +-----v-----+
              | Snowflake |   |   MySQL   |   |  CSV/JSON |
              +-----------+   +-----------+   +-----------+
```

---

## 2. Installation & Setup

### Prerequisites

- **Python 3.10 or higher**
- **pip** (Python package manager)
- **Snowflake account** (optional, for database operations)
- **Node.js 18+** (optional, for web dashboard features)

### Installation

1. **Clone the repository:**

```bash
git clone https://github.com/your-org/databridge-ai.git
cd databridge-ai
```

2. **Create a virtual environment:**

```bash
python -m venv venv

# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate
```

3. **Install dependencies:**

```bash
pip install -r requirements.txt
```

4. **Configure environment variables:**

Create a `.env` file in the project root:

```env
# Optional: Snowflake connection
SNOWFLAKE_ACCOUNT=your_account.snowflakecomputing.com
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=ANALYTICS
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_WAREHOUSE=COMPUTE_WH

# Optional: Backend sync (NestJS)
NESTJS_BACKEND_URL=http://localhost:8001
NESTJS_API_KEY=dev-key-1
```

### Starting the Dashboard

Launch the DataBridge AI Dashboard to access all features through a web interface:

```bash
python run_ui.py
```

You should see:

```
╔════════════════════════════════════════════════════════════════╗
║              DataBridge AI Dashboard v0.40.0                   ║
║                     348 MCP Tools                              ║
╠════════════════════════════════════════════════════════════════╣
║  Dashboard:  http://127.0.0.1:5050                             ║
║                                                                ║
║  Features:                                                     ║
║  • Data Reconciliation    • Hierarchy Builder                  ║
║  • Wright Pipeline        • dbt Integration                    ║
║  • Data Catalog           • Cortex AI                          ║
║  • Data Observability     • GraphRAG Engine                    ║
╚════════════════════════════════════════════════════════════════╝
```

Open your browser to **http://localhost:5050** to access the dashboard.

### Verifying Installation

Test the installation by loading a sample file:

```python
# In Python or via MCP tool call
from databridge_ai import load_csv

result = load_csv("data/sample_gl_journal.csv")
print(result)
```

Expected output:

```json
{
  "status": "success",
  "row_count": 25,
  "columns": ["transaction_id", "posting_date", "period", "account_code",
              "account_name", "department", "cost_center", "debit", "credit",
              "balance", "description"],
  "preview": [...]
}
```

---

## 3. Quick Start: Building a P&L Hierarchy

This walkthrough demonstrates how to build a Profit & Loss (P&L) hierarchy from GL data using DataBridge AI's core tools.

### Step 1: Load Sample GL Data

First, load and examine your GL journal data:

```python
# Tool: load_csv
load_csv(
    file_path="data/sample_gl_journal.csv",
    preview_rows=5
)
```

**Expected Output:**

```json
{
  "status": "success",
  "file_path": "data/sample_gl_journal.csv",
  "row_count": 25,
  "columns": [
    "transaction_id", "posting_date", "period", "account_code",
    "account_name", "department", "cost_center", "debit", "credit",
    "balance", "description"
  ],
  "preview": [
    {
      "transaction_id": "TXN001",
      "posting_date": "2024-01-15",
      "account_code": "1000",
      "account_name": "Cash",
      "debit": 150000,
      "credit": 0
    }
  ]
}
```

### Step 2: Create a Hierarchy Project

Create a new project to contain your P&L hierarchies:

```python
# Tool: create_hierarchy_project
create_hierarchy_project(
    name="Financial Reporting 2024",
    description="P&L and Balance Sheet hierarchies for FY2024"
)
```

**Expected Output:**

```json
{
  "status": "success",
  "project": {
    "id": "abc-123-def-456",
    "name": "Financial Reporting 2024",
    "description": "P&L and Balance Sheet hierarchies for FY2024",
    "created_at": "2024-01-15T10:30:00Z",
    "hierarchy_count": 0
  },
  "sync": {
    "auto_sync": "success"
  }
}
```

Save the `project_id` for subsequent operations.

### Step 3: Add Hierarchies

Build the P&L structure by creating hierarchy nodes:

```python
# Create the root node
create_hierarchy(
    project_id="abc-123-def-456",
    hierarchy_name="Income Statement",
    description="Top-level P&L hierarchy"
)

# Create Revenue category (child of Income Statement)
create_hierarchy(
    project_id="abc-123-def-456",
    hierarchy_name="Revenue",
    parent_id="INCOME_STATEMENT_1",
    description="Total Revenue"
)

# Create sub-categories
create_hierarchy(
    project_id="abc-123-def-456",
    hierarchy_name="Product Revenue",
    parent_id="REVENUE_1",
    description="Revenue from product sales"
)

create_hierarchy(
    project_id="abc-123-def-456",
    hierarchy_name="Service Revenue",
    parent_id="REVENUE_1",
    description="Revenue from services"
)

# Create COGS category
create_hierarchy(
    project_id="abc-123-def-456",
    hierarchy_name="Cost of Revenue",
    parent_id="INCOME_STATEMENT_1",
    description="Cost of goods sold and direct costs"
)

# Create Gross Profit (calculated)
create_hierarchy(
    project_id="abc-123-def-456",
    hierarchy_name="Gross Profit",
    parent_id="INCOME_STATEMENT_1",
    description="Revenue minus COGS",
    flags='{"calculation_flag": true}'
)
```

**Expected Output (for each):**

```json
{
  "status": "success",
  "hierarchy": {
    "hierarchy_id": "REVENUE_1",
    "hierarchy_name": "Revenue",
    "parent_id": "INCOME_STATEMENT_1",
    "level_1": "Income Statement",
    "level_2": "Revenue"
  }
}
```

### Step 4: Add Source Mappings

Map GL account codes to hierarchy nodes:

```python
# Map account codes starting with '3' to Revenue
add_source_mapping(
    project_id="abc-123-def-456",
    hierarchy_id="PRODUCT_REVENUE_1",
    source_database="ANALYTICS",
    source_schema="GL",
    source_table="FACT_JOURNAL_ENTRIES",
    source_column="ACCOUNT_CODE",
    source_uid="30%",
    precedence_group="REVENUE"
)

# Map service revenue
add_source_mapping(
    project_id="abc-123-def-456",
    hierarchy_id="SERVICE_REVENUE_1",
    source_database="ANALYTICS",
    source_schema="GL",
    source_table="FACT_JOURNAL_ENTRIES",
    source_column="ACCOUNT_CODE",
    source_uid="31%",
    precedence_group="REVENUE"
)

# Map COGS accounts
add_source_mapping(
    project_id="abc-123-def-456",
    hierarchy_id="COST_OF_REVENUE_1",
    source_database="ANALYTICS",
    source_schema="GL",
    source_table="FACT_JOURNAL_ENTRIES",
    source_column="ACCOUNT_CODE",
    source_uid="4%",
    precedence_group="COGS"
)
```

**Expected Output:**

```json
{
  "status": "success",
  "mapping": {
    "hierarchy_id": "PRODUCT_REVENUE_1",
    "source_table": "FACT_JOURNAL_ENTRIES",
    "source_column": "ACCOUNT_CODE",
    "source_uid": "30%",
    "precedence_group": "REVENUE"
  }
}
```

### Step 5: View the Hierarchy Tree

Visualize the complete hierarchy:

```python
# Tool: get_hierarchy_tree
get_hierarchy_tree(project_id="abc-123-def-456")
```

**Expected Output:**

```json
{
  "project_id": "abc-123-def-456",
  "name": "Financial Reporting 2024",
  "tree": {
    "hierarchy_id": "INCOME_STATEMENT_1",
    "hierarchy_name": "Income Statement",
    "children": [
      {
        "hierarchy_id": "REVENUE_1",
        "hierarchy_name": "Revenue",
        "children": [
          {"hierarchy_id": "PRODUCT_REVENUE_1", "hierarchy_name": "Product Revenue"},
          {"hierarchy_id": "SERVICE_REVENUE_1", "hierarchy_name": "Service Revenue"}
        ]
      },
      {
        "hierarchy_id": "COST_OF_REVENUE_1",
        "hierarchy_name": "Cost of Revenue"
      },
      {
        "hierarchy_id": "GROSS_PROFIT_1",
        "hierarchy_name": "Gross Profit",
        "is_calculated": true
      }
    ]
  }
}
```

### Step 6: Export to CSV

Export the hierarchy for deployment or sharing:

```python
# Tool: export_hierarchy_csv
export_hierarchy_csv(
    project_id="abc-123-def-456",
    output_path="./exports/financial_reporting_2024_hierarchy.csv"
)
```

**Expected Output:**

```json
{
  "status": "success",
  "output_path": "./exports/financial_reporting_2024_hierarchy.csv",
  "row_count": 6,
  "columns": ["HIERARCHY_ID", "HIERARCHY_NAME", "PARENT_ID", "LEVEL_1",
              "LEVEL_2", "LEVEL_3", "SORT_ORDER", "INCLUDE_FLAG"]
}
```

The exported CSV:

```csv
HIERARCHY_ID,HIERARCHY_NAME,PARENT_ID,LEVEL_1,LEVEL_2,LEVEL_3,SORT_ORDER,INCLUDE_FLAG
INCOME_STATEMENT_1,Income Statement,,Income Statement,,,1,TRUE
REVENUE_1,Revenue,INCOME_STATEMENT_1,Income Statement,Revenue,,10,TRUE
PRODUCT_REVENUE_1,Product Revenue,REVENUE_1,Income Statement,Revenue,Product Revenue,11,TRUE
SERVICE_REVENUE_1,Service Revenue,REVENUE_1,Income Statement,Revenue,Service Revenue,12,TRUE
COST_OF_REVENUE_1,Cost of Revenue,INCOME_STATEMENT_1,Income Statement,Cost of Revenue,,20,TRUE
GROSS_PROFIT_1,Gross Profit,INCOME_STATEMENT_1,Income Statement,Gross Profit,,30,TRUE
```

---

## 4. Wright Pipeline Builder

The Wright Pipeline Builder generates a complete 4-object data mart pipeline from hierarchy definitions. This is DataBridge AI's flagship feature for automated data warehouse generation.

### Understanding the 4-Object Pipeline

```
+-------------------+     +-------------------+     +-------------------+     +-------------------+
|       VW_1        |     |       DT_2        |     |      DT_3A        |     |       DT_3        |
|   Translation     | --> |    Granularity    | --> |   Pre-Aggregation | --> |    Data Mart      |
|      View         |     |   Dynamic Table   |     |       Fact        |     |      (Final)      |
+-------------------+     +-------------------+     +-------------------+     +-------------------+
| CASE on ID_SOURCE |     | UNPIVOT measures  |     | UNION ALL branches|     | Formula precedence|
| Hierarchy lookup  |     | Apply exclusions  |     | Join fact table   |     | Surrogate keys    |
+-------------------+     +-------------------+     +-------------------+     +-------------------+
```

| Layer | Object Type | Purpose |
|-------|-------------|---------|
| **VW_1** | View | Translates ID_SOURCE values using CASE statements |
| **DT_2** | Dynamic Table | Unpivots measures and applies exclusions |
| **DT_3A** | Dynamic Table | Pre-aggregation with UNION ALL branches per join pattern |
| **DT_3** | Dynamic Table | Final data mart with formula calculations and surrogate keys |

### Step 1: Create a Mart Configuration

Define the pipeline parameters:

```python
# Tool: create_mart_config
create_mart_config(
    project_name="upstream_gross",
    report_type="GROSS",
    hierarchy_table="ANALYTICS.PUBLIC.TBL_0_GROSS_LOS_REPORT_HIERARCHY",
    mapping_table="ANALYTICS.PUBLIC.TBL_0_GROSS_LOS_REPORT_HIERARCHY_MAPPING",
    account_segment="GROSS",
    has_group_filter_precedence=True,
    target_database="ANALYTICS",
    target_schema="MARTS",
    description="Upstream gross revenue data mart"
)
```

**Expected Output:**

```json
{
  "success": true,
  "config_id": "cfg-upstream-gross-001",
  "project_name": "upstream_gross",
  "report_type": "GROSS",
  "account_segment": "GROSS",
  "message": "Created mart configuration 'upstream_gross'"
}
```

### Step 2: Add Join Patterns

Define how hierarchy metadata joins to the fact table:

```python
# Primary join pattern: Account dimension
add_mart_join_pattern(
    config_name="upstream_gross",
    name="account",
    join_keys="LOS_ACCOUNT_ID_FILTER",
    fact_keys="FK_ACCOUNT_KEY",
    description="Primary account dimension join"
)

# Secondary join pattern: Deduct + Product dimensions
add_mart_join_pattern(
    config_name="upstream_gross",
    name="deduct_product",
    join_keys="LOS_DEDUCT_CODE_FILTER,LOS_PRODUCT_CODE_FILTER",
    fact_keys="FK_DEDUCT_KEY,FK_PRODUCT_KEY",
    description="Deduction and product dimension join"
)

# Tertiary join pattern: Royalty
add_mart_join_pattern(
    config_name="upstream_gross",
    name="royalty",
    join_keys="LOS_ROYALTY_FILTER",
    fact_keys="FK_ROYALTY_KEY",
    filter="ROYALTY_FLAG = 'Y'",
    description="Royalty owner join with filter"
)
```

**Expected Output:**

```json
{
  "success": true,
  "pattern_id": "pat-account-001",
  "name": "account",
  "join_keys": ["LOS_ACCOUNT_ID_FILTER"],
  "fact_keys": ["FK_ACCOUNT_KEY"],
  "message": "Added join pattern 'account' to configuration"
}
```

### Step 3: Generate the Pipeline

Generate all 4 DDL objects:

```python
# Tool: generate_mart_pipeline
generate_mart_pipeline(
    config_name="upstream_gross",
    output_format="ddl",
    include_formulas=True
)
```

**Expected Output:**

```json
{
  "success": true,
  "config_name": "upstream_gross",
  "object_count": 4,
  "objects": [
    {
      "name": "VW_1_UPSTREAM_GROSS_TRANSLATED",
      "layer": "VW_1",
      "type": "VIEW",
      "ddl": "CREATE OR REPLACE VIEW ANALYTICS.MARTS.VW_1_UPSTREAM_GROSS_TRANSLATED AS\nSELECT\n  CASE\n    WHEN ID_SOURCE = 'ACCOUNT_CODE' THEN ...\n  END AS RESOLVED_VALUE,\n  ..."
    },
    {
      "name": "DT_2_UPSTREAM_GROSS_GRANULARITY",
      "layer": "DT_2",
      "type": "DYNAMIC_TABLE",
      "ddl": "CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.MARTS.DT_2_UPSTREAM_GROSS_GRANULARITY\n  TARGET_LAG = '1 hour'\n  WAREHOUSE = COMPUTE_WH\nAS\n  SELECT\n    UNPIVOT(...)\n  ..."
    },
    {
      "name": "DT_3A_UPSTREAM_GROSS_PREAGG",
      "layer": "DT_3A",
      "type": "DYNAMIC_TABLE",
      "ddl": "CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.MARTS.DT_3A_UPSTREAM_GROSS_PREAGG AS\n  -- Branch: account\n  SELECT ... FROM DT_2 JOIN FACT ON ...\n  UNION ALL\n  -- Branch: deduct_product\n  SELECT ... FROM DT_2 JOIN FACT ON ...\n  ..."
    },
    {
      "name": "DT_3_UPSTREAM_GROSS_MART",
      "layer": "DT_3",
      "type": "DYNAMIC_TABLE",
      "ddl": "CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.MARTS.DT_3_UPSTREAM_GROSS_MART AS\n  WITH P1 AS (...),\n  P2 AS (...),\n  P3 AS (...)\n  SELECT\n    MD5(...) AS SURROGATE_KEY,\n    ..."
    }
  ],
  "message": "Generated 4 pipeline objects"
}
```

### Step 4: Generate Individual Objects

Generate a single layer for inspection:

```python
# Tool: generate_mart_object
generate_mart_object(
    config_name="upstream_gross",
    layer="VW_1"
)
```

### Step 5: Validate the Pipeline

Validate the generated DDL:

```python
# Tool: validate_mart_pipeline
validate_mart_pipeline(
    config_name="upstream_gross"
)
```

**Expected Output:**

```json
{
  "success": true,
  "valid": true,
  "config_name": "upstream_gross",
  "layer_results": {
    "VW_1": {"valid": true, "warnings": []},
    "DT_2": {"valid": true, "warnings": []},
    "DT_3A": {"valid": true, "warnings": []},
    "DT_3": {"valid": true, "warnings": []}
  },
  "errors": [],
  "warnings": [],
  "message": "Pipeline validation complete"
}
```

---

## 5. dbt Integration

DataBridge AI provides tools to convert Wright pipelines to dbt models with AI-powered documentation and testing.

### Step 1: Convert Wright to dbt Model

Transform a Wright pipeline object into a dbt-compatible model:

```python
# Tool: wright_to_dbt_model
wright_to_dbt_model(
    config_name="upstream_gross",
    wright_object_type="DT_3",
    output_path="./dbt_project/models/marts/fct_upstream_gross.sql",
    include_documentation=True
)
```

**Expected Output:**

```json
{
  "success": true,
  "config_name": "upstream_gross",
  "wright_object_type": "DT_3",
  "output_path": "./dbt_project/models/marts/fct_upstream_gross.sql",
  "dbt_model_sql": "{{/*\n  Model: fct_upstream_gross\n  Generated by Wright Pipeline Builder\n  Config: upstream_gross\n*/}}\n\n{{ config(\n    materialized='incremental',\n    unique_key='surrogate_key',\n    tags=['upstream_gross', 'mart']\n) }}\n\nWITH P1 AS (\n  SELECT ...\n  FROM {{ ref('DT_3A_UPSTREAM_GROSS_PREAGG') }}\n),\n..."
}
```

### Step 2: Generate AI-Powered Schema Documentation

Use Cortex AI to generate comprehensive schema.yml:

```python
# Tool: cortex_generate_dbt_schema_yml
cortex_generate_dbt_schema_yml(
    config_name="upstream_gross",
    model_name="fct_upstream_gross",
    output_path="./dbt_project/models/marts/schema.yml"
)
```

**Expected Output:**

```yaml
version: 2

models:
  - name: fct_upstream_gross
    description: >
      Upstream gross revenue data mart containing all revenue,
      tax, and deduction line items aggregated by hierarchy.
      Generated from Wright pipeline configuration 'upstream_gross'.

    columns:
      - name: surrogate_key
        description: Unique identifier (MD5 hash of dimension keys)
        tests:
          - unique
          - not_null

      - name: fk_date_key
        description: Foreign key to date dimension
        tests:
          - not_null
          - relationships:
              to: ref('dim_date')
              field: date_key

      - name: gross_amount
        description: >
          Gross revenue amount before deductions and taxes.
          Positive values indicate revenue; negative values indicate
          adjustments or reversals.

      - name: hierarchy_name
        description: Name of the reporting hierarchy node
        tests:
          - not_null

      - name: precedence_group
        description: >
          Formula precedence group for calculation ordering.
          Values: REVENUE, TAXES, DEDUCTS, GROSS_PROFIT
```

### Step 3: Generate AI-Suggested Tests

Let Cortex AI suggest data quality tests:

```python
# Tool: cortex_suggest_dbt_tests
cortex_suggest_dbt_tests(
    config_name="upstream_gross",
    model_name="fct_upstream_gross"
)
```

**Expected Output:**

```json
{
  "success": true,
  "model_name": "fct_upstream_gross",
  "suggested_tests": [
    {
      "test_type": "singular",
      "name": "test_gross_profit_formula",
      "description": "Validates that Gross Profit = Revenue - Taxes - Deducts",
      "sql": "SELECT * FROM {{ ref('fct_upstream_gross') }} WHERE ABS(gross_profit - (revenue - taxes - deducts)) > 0.01"
    },
    {
      "test_type": "generic",
      "name": "accepted_values",
      "column": "precedence_group",
      "values": ["REVENUE", "TAXES", "DEDUCTS", "GROSS_PROFIT", "OPEX"]
    },
    {
      "test_type": "singular",
      "name": "test_no_orphan_hierarchies",
      "description": "Ensures all hierarchy IDs have source mappings",
      "sql": "..."
    }
  ]
}
```

### Step 4: Run dbt Commands

Execute dbt commands directly:

```python
# Tool: run_dbt_command
run_dbt_command(
    command="build",
    select="tag:upstream_gross",
    project_dir="./dbt_project"
)
```

**Expected Output:**

```json
{
  "success": true,
  "command": "dbt build --select tag:upstream_gross",
  "exit_code": 0,
  "stdout": "Running with dbt=1.7.0\n\n...\n\nCompleted successfully\n\nDone. PASS=4 WARN=0 ERROR=0 SKIP=0 TOTAL=4",
  "models_run": 4,
  "tests_passed": 5
}
```

### Complete dbt Workflow Example

```python
# 1. Generate dbt sources from hierarchy mappings
wright_generate_dbt_sources(
    config_name="upstream_gross",
    output_path="./dbt_project/models/staging/sources.yml"
)

# 2. Generate dbt tests for formula validation
wright_generate_dbt_tests(
    config_name="upstream_gross",
    output_path="./dbt_project/tests/"
)

# 3. Generate semantic metrics
wright_generate_dbt_metrics(
    config_name="upstream_gross",
    output_path="./dbt_project/models/marts/metrics.yml"
)

# 4. Generate CI/CD pipeline
wright_generate_dbt_ci(
    config_name="upstream_gross",
    platform="github_actions",
    output_path="./.github/workflows/dbt-ci.yml"
)

# 5. Run the full pipeline
run_dbt_command(command="build", select="tag:upstream_gross")
```

---

## 6. Using the Dashboard UI

The DataBridge AI Dashboard provides a visual interface for all MCP tools.

### Accessing the Dashboard

1. Start the dashboard server:

```bash
python run_ui.py
```

2. Open your browser to **http://localhost:5050**

### Dashboard Tabs

#### MCP CLI Tab

Execute any MCP tool directly:

1. Type a tool name in the search box (e.g., `load_csv`)
2. Fill in the parameters
3. Click **Execute**
4. View the JSON response

#### Wright Builder Tab

Visual interface for the 4-object pipeline:

1. **Configuration Panel**
   - Create new mart configurations
   - Set hierarchy and mapping tables
   - Configure account segments and flags

2. **Join Patterns Panel**
   - Add UNION ALL branch definitions
   - Specify join keys and fact keys
   - Add optional filters

3. **Pipeline Preview**
   - View generated DDL for each layer
   - Syntax-highlighted SQL
   - Copy to clipboard

4. **Validation**
   - Run pipeline validation
   - View errors and warnings
   - Compare to baseline DDL

#### dbt Workflow Tab

End-to-end dbt project management:

1. **Model Generation**
   - Convert Wright objects to dbt models
   - Generate schema.yml with AI documentation
   - Create data tests

2. **Execution**
   - Run dbt commands (build, test, docs)
   - View real-time logs
   - Track model execution status

3. **Artifacts**
   - Browse generated files
   - Download project as ZIP
   - View dbt docs

#### Data Catalog Tab

Metadata management and discovery:

1. **Asset Browser**
   - View all cataloged assets
   - Filter by type, classification, or tag
   - Search across names and descriptions

2. **Business Glossary**
   - Create and manage terms
   - Link terms to assets
   - Track term approval status

3. **Data Discovery**
   - Scan database connections
   - Auto-detect PII
   - Profile column statistics

### Dashboard API Endpoints

The dashboard also exposes REST endpoints:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/health` | GET | Health check and tool count |
| `/api/stats` | GET | Module statistics |
| `/api/tools/{tool_name}` | POST | Execute an MCP tool |

Example API call:

```bash
curl -X POST http://localhost:5050/api/tools/load_csv \
  -H "Content-Type: application/json" \
  -d '{"file_path": "data/sample_gl_journal.csv"}'
```

---

## 7. Next Steps

### Exploring All 348 Tools

Browse the complete tool reference:

```python
# View tool manifest
list_tools()  # Returns all available tools with descriptions
```

Or view the auto-generated documentation at `docs/MANIFEST.md`.

### Key Tool Categories to Explore

| Category | Example Tools | Use Case |
|----------|---------------|----------|
| **Data Loading** | `load_csv`, `load_json`, `query_database` | Import data from various sources |
| **Data Profiling** | `profile_data`, `detect_schema_drift` | Understand data quality and structure |
| **Fuzzy Matching** | `fuzzy_match_columns`, `fuzzy_deduplicate` | Handle messy data with approximate matching |
| **PDF/OCR** | `extract_text_from_pdf`, `ocr_image` | Process unstructured documents |
| **Templates** | `list_financial_templates`, `create_project_from_template` | Quick-start with industry templates |
| **AI Orchestration** | `submit_orchestrated_task`, `plan_workflow` | Multi-agent task coordination |

### Cortex AI Integration

If you have a Snowflake account with Cortex AI enabled:

```python
# Configure Cortex connection
configure_cortex_agent(
    connection_id="snowflake-prod",
    cortex_model="mistral-large"
)

# Use AI for text generation
cortex_complete(prompt="Explain the difference between GAAP and IFRS revenue recognition")

# Run reasoning loop for complex analysis
cortex_reason(
    goal="Analyze data quality issues in the GL_ENTRIES table",
    context='{"table": "ANALYTICS.GL.GL_ENTRIES"}'
)

# Natural language to SQL with Cortex Analyst
analyst_ask(
    question="What was total revenue by region last quarter?",
    semantic_model_file="@ANALYTICS.PUBLIC.MODELS/finance.yaml"
)
```

### Data Quality and Versioning

Implement data contracts and validation:

```python
# Create expectation suite
generate_expectation_suite(
    name="gl_accounts_suite",
    database="ANALYTICS",
    schema_name="GL",
    table_name="GL_ACCOUNTS"
)

# Add column expectations
add_column_expectation(
    suite_name="gl_accounts_suite",
    column="ACCOUNT_CODE",
    expectation_type="match_regex",
    regex="^[1-9][0-9]{3}$",
    severity="high"
)

# Run validation
run_validation(suite_name="gl_accounts_suite")
```

### Lineage and Impact Analysis

Track data dependencies:

```python
# Track column lineage
track_column_lineage(
    graph_name="finance_lineage",
    source_node="DIM_ACCOUNT",
    source_columns="ACCOUNT_CODE",
    target_node="VW_1_TRANSLATED",
    target_column="RESOLVED_VALUE",
    transformation_type="CASE"
)

# Analyze impact of changes
analyze_change_impact(
    graph_name="finance_lineage",
    node="DIM_ACCOUNT",
    change_type="REMOVE_COLUMN",
    column="ACCOUNT_CODE"
)
```

### Additional Resources

- **API Reference**: `docs/API_REFERENCE.md`
- **Architecture Guide**: `docs/ARCHITECTURE.md`
- **Lessons Learned**: `docs/LESSONS_LEARNED.md`
- **Deployment Guide**: `docs/DEPLOYMENT_GUIDE.md`

### Getting Help

- **GitHub Issues**: Report bugs or request features
- **Discord Community**: Join the DataBridge AI community
- **Documentation**: Full reference at `docs/`

---

**Happy Data Bridging!**

*DataBridge AI v0.40.0 - 348 MCP Tools*
