# DataBridge AI: Detailed Reference Guide

> **For Gemini Context Memory** - This file contains detailed documentation, examples, and architecture diagrams.
> Claude uses compact CLAUDE.md for quick reference and queries Gemini for detailed info.

---

## Module Reference (292 Tools)

### 1. File Discovery & Staging (3 tools)

```python
# Search for files
find_files(pattern="*.csv", search_name="hierarchy")

# Stage to data directory
stage_file("/Users/john/Downloads/my_hierarchy.csv")

# Check working directory
get_working_directory()
```

---

### 2. Data Reconciliation (38 tools)

**Data Loading:** `load_csv`, `load_json`, `query_database`
**Profiling:** `profile_data`, `detect_schema_drift`
**Comparison:** `compare_hashes`, `get_orphan_details`, `get_conflict_details`
**Fuzzy Matching:** `fuzzy_match_columns`, `fuzzy_deduplicate`
**OCR/PDF:** `extract_text_from_pdf`, `ocr_image`, `parse_table_from_text`
**Transforms:** `transform_column`, `merge_sources`
**Workflow:** `save_workflow_step`, `get_workflow`, `clear_workflow`, `get_audit_log`

---

### 3. Hierarchy Knowledge Base (44 tools)

#### Flexible Import Tiers

| Tier | Columns | Use Case |
|------|---------|----------|
| **Tier 1** | 2-3 | Quick grouping (source_value, group_name) |
| **Tier 2** | 5-7 | Basic parent-child (hierarchy_name, parent_name) |
| **Tier 3** | 10-12 | Full control with explicit IDs |
| **Tier 4** | 28+ | Enterprise full format |

**Tier 1 Example:**
```csv
source_value,group_name
4100,Revenue
4200,Revenue
5100,COGS
```

**Tier 2 Example:**
```csv
hierarchy_name,parent_name,source_value,sort_order
Revenue,,4%,1
Product Revenue,Revenue,41%,2
Service Revenue,Revenue,42%,3
```

**Import Workflow:**
```python
# 1. Detect format
detect_hierarchy_format(content)

# 2. Configure defaults
configure_project_defaults(project_id, database, schema, table, column)

# 3. Preview
preview_import(content, source_defaults)

# 4. Import
import_flexible_hierarchy(project_id, content, source_defaults)

# 5. Export simplified
export_hierarchy_simplified(project_id, target_tier)
```

#### CSV Columns Reference

**Hierarchy CSV:**
- HIERARCHY_ID, HIERARCHY_NAME, PARENT_ID, DESCRIPTION
- LEVEL_1 - LEVEL_10, LEVEL_1_SORT - LEVEL_10_SORT
- INCLUDE_FLAG, EXCLUDE_FLAG, FORMULA_GROUP, SORT_ORDER

**Mapping CSV:**
- HIERARCHY_ID, MAPPING_INDEX
- SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN
- SOURCE_UID, PRECEDENCE_GROUP, INCLUDE_FLAG, EXCLUDE_FLAG

**IMPORTANT:** Sort orders come from HIERARCHY CSV, not MAPPING CSV.

---

### 4. SQL Discovery (2 tools)

```python
# Convert SQL CASE to hierarchies
sql_to_hierarchy(
    sql="""SELECT CASE WHEN account_code LIKE '4%' THEN 'Revenue' END""",
    project_id="my-project",
    source_database="WAREHOUSE",
    source_schema="FINANCE",
    source_table="GL_TRANSACTIONS",
    source_column="ACCOUNT_CODE"
)
```

### 5. Smart SQL Analyzer (2 tools) - RECOMMENDED

Respects WHERE clause filters (NOT IN, <>, NOT LIKE).

```python
smart_analyze_sql(
    sql=sql,
    coa_path="C:/data/DIM_ACCOUNT.csv",
    output_dir="./result_export",
    export_name="los_analysis"
)
```

---

### 6. AI Orchestrator (16 tools)

**Architecture:**
```
┌─────────────────────────────────────────┐
│           AI Orchestrator               │
├─────────────────────────────────────────┤
│  Task Queue     │  Priority scheduling  │
│  Agent Registry │  Health monitoring    │
│  Event Bus      │  Redis Pub/Sub        │
│  AI-Link        │  Agent messaging      │
└─────────────────────────────────────────┘
```

```python
# Submit task
submit_orchestrated_task(
    task_type="hierarchy_import",
    payload='{"file": "data.csv"}',
    priority=5
)

# Register agent
register_agent(
    agent_id="my-agent",
    capabilities='["import", "validate"]'
)

# Send message
send_agent_message(
    from_agent="agent-1",
    to_agent="agent-2",
    message="Task complete"
)
```

---

### 7. PlannerAgent (11 tools)

**Available Agents:**
| Agent | Capabilities |
|-------|--------------|
| schema_scanner | scan_schema, extract_metadata |
| logic_extractor | parse_sql, extract_case |
| warehouse_architect | design_star_schema, dbt_models |
| deploy_validator | execute_ddl, validate_counts |
| hierarchy_builder | create_hierarchy, add_properties |
| data_reconciler | compare_sources, fuzzy_match |

```python
plan_workflow(
    request="Extract hierarchies from SQL and deploy to Snowflake",
    context='{"schema": "FINANCE", "database": "WAREHOUSE"}'
)
```

---

### 8. Cortex Agent (12 tools)

**Architecture:**
```
┌─────────────────────────────────────────────────────────────────┐
│                    CortexAgent                                  │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │  Reasoning Loop: OBSERVE → PLAN → EXECUTE → REFLECT       │ │
│  └───────────────────────────────────────────────────────────┘ │
│                           ↓                                     │
│  ┌───────────────────────────────────────────────────────────┐ │
│  │  CortexClient: COMPLETE, SUMMARIZE, SENTIMENT, TRANSLATE  │ │
│  └───────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

```python
# Configure
configure_cortex_agent(connection_id="snowflake-prod", cortex_model="mistral-large")

# Generate
cortex_complete(prompt="Explain data reconciliation")

# Reason
cortex_reason(goal="Analyze data quality", context='{"table": "PRODUCTS"}')
```

---

### 9. Cortex Analyst (13 tools)

**Semantic Model YAML:**
```yaml
name: sales_analytics
tables:
  - name: sales
    base_table:
      database: ANALYTICS
      schema: PUBLIC
      table: SALES_FACT
    dimensions:
      - name: region
        expr: REGION_NAME
    metrics:
      - name: revenue
        expr: SUM(AMOUNT)
```

```python
# Create model
create_semantic_model(name="sales_analytics", database="ANALYTICS")

# Add table
add_semantic_table(model_name="sales_analytics", table_name="sales", ...)

# Deploy
deploy_semantic_model(model_name="sales_analytics", stage_path="@MODELS/sales.yaml")

# Query
analyst_ask(question="What was revenue by region?", semantic_model_file="@MODELS/sales.yaml")
```

---

### 10. Wright Module (18 tools)

**4-Object Pipeline:**
| Object | Purpose |
|--------|---------|
| VW_1 | Translation View (CASE on ID_SOURCE) |
| DT_2 | Granularity Table (UNPIVOT, exclusions) |
| DT_3A | Pre-Aggregation Fact (UNION ALL branches) |
| DT_3 | Data Mart (formula precedence, surrogates) |

**5-Level Formula Precedence:**
| Level | Calculations |
|-------|--------------|
| P1 | Base totals (Revenue, Taxes, Deducts) |
| P2 | Combined totals |
| P3 | Gross Profit |
| P4 | Operating Income |
| P5 | Cash Flow |

```python
# Create config
create_mart_config(project_name="upstream_gross", report_type="GROSS", ...)

# Add join patterns
add_mart_join_pattern(config_name="upstream_gross", name="account", ...)

# Generate pipeline
generate_mart_pipeline(config_name="upstream_gross")

# Data quality
validate_hierarchy_data_quality(project_id="upstream_gross")
normalize_id_source_values(project_id="upstream_gross")
```

**Wright Pipeline Builder UI** (http://localhost:5050 → ✈️ Wright Builder):

| Tab | Configurable Dropdowns | Generated SQL |
|-----|----------------------|---------------|
| VW_1 | ID_SOURCE mappings, Additional columns | Translation View with CASE |
| DT_2 | Unpivot measures, Exclusion categories | Granularity with UNPIVOT |
| DT_3A | Join patterns (dynamic add/remove) | Pre-Agg with UNION ALL |
| DT_3 | Formula precedence, Dimension keys | Data Mart with cascade |

Features: Generate per-step, Copy to clipboard, Export SQL bundle, Save config to localStorage.

---

### 11. dbt Integration (8 tools)

```python
# Create project
create_dbt_project(
    name="finance_analytics",
    profile="snowflake_prod",
    hierarchy_project_id="revenue-pl"
)

# Generate models
generate_dbt_model(project_name="finance_analytics", model_name="gl_accounts", model_type="staging")

# Generate CI/CD
generate_cicd_pipeline(project_name="finance_analytics", platform="github_actions")

# Export
export_dbt_project(project_name="finance_analytics", as_zip=True)
```

---

### 12. Data Quality (7 tools)

**Expectation Types:**
| Type | Description |
|------|-------------|
| not_null | Required fields |
| unique | Primary keys |
| in_set | Status codes |
| match_regex | Pattern matching |
| between | Range validation |

```python
# Create suite
generate_expectation_suite(name="gl_suite", table_name="GL_ACCOUNTS")

# Add expectation
add_column_expectation(suite_name="gl_suite", column="ACCOUNT_CODE", expectation_type="match_regex", regex="^[4-9][0-9]{3}$")

# Run validation
run_validation(suite_name="gl_suite", data='[...]')
```

---

### 13. Data Catalog (15 tools)

**Classifications:**
| Type | Auto-Detection |
|------|----------------|
| PII | email, phone, ssn |
| PHI | dob, medical_* |
| PCI | credit_card, cvv |

```python
# Scan connection
catalog_scan_connection(connection_id="snowflake-prod", database="ANALYTICS", detect_pii=True)

# Create glossary term
catalog_create_term(name="Revenue", definition="Total income before deductions")

# Search
catalog_search(query="revenue accounts", limit=10)
```

---

### 14. Data Versioning (12 tools)

**Versioned Objects:**
HIERARCHY_PROJECT, HIERARCHY, CATALOG_ASSET, GLOSSARY_TERM, SEMANTIC_MODEL, DATA_CONTRACT, EXPECTATION_SUITE

```python
# Create version
version_create(object_type="hierarchy", object_id="revenue-pl", change_description="Added mappings", version_bump="minor")

# List history
version_list(object_type="hierarchy", object_id="revenue-pl")

# Diff
version_diff(object_type="hierarchy", object_id="revenue-pl", from_version="1.0.0", to_version="1.1.0")

# Rollback
version_rollback(object_type="hierarchy", object_id="revenue-pl", to_version="1.0.0")
```

---

### 15. Lineage & Impact Analysis (11 tools)

**Impact Severity:**
| Change | Target | Severity |
|--------|--------|----------|
| REMOVE_COLUMN | DATA_MART | CRITICAL |
| RENAME_COLUMN | DATA_MART | HIGH |
| MODIFY_FORMULA | DATA_MART | HIGH |

```python
# Add nodes
add_lineage_node(graph_name="finance", node_name="DIM_ACCOUNT", node_type="TABLE")

# Track lineage
track_column_lineage(graph_name="finance", source_node="DIM_ACCOUNT", target_node="VW_1", ...)

# Impact analysis
analyze_change_impact(graph_name="finance", node="DIM_ACCOUNT", change_type="REMOVE_COLUMN", column="CODE")

# Export diagram
export_lineage_diagram(graph_name="finance", format="mermaid")
```

---

### 16. Git/CI-CD Integration (12 tools)

```python
# Configure
configure_git(repo_path="./my-dbt", remote_url="https://github.com/org/repo.git", token="ghp_xxx")

# Branch
git_create_branch(branch_name="feature/add-hierarchy")

# Commit
git_commit(message="Add hierarchy models", files="models/*.sql")

# Push and PR
git_push(set_upstream=True)
github_create_pr(title="Add hierarchy", body="## Summary\n...", reviewers="john,jane")

# Generate workflow
generate_dbt_workflow(project_name="my-mart", output_path=".github/workflows/dbt-ci.yml")
```

---

## Templates (20)

### Accounting Domain (10)
| ID | Name | Industry |
|----|------|----------|
| standard_pl | Standard P&L | General |
| standard_bs | Balance Sheet | General |
| oil_gas_los | Oil & Gas LOS | Oil & Gas |
| upstream_oil_gas_pl | Upstream O&G P&L | E&P |
| midstream_oil_gas_pl | Midstream O&G P&L | Midstream |
| oilfield_services_pl | Oilfield Services P&L | Services |
| manufacturing_pl | Manufacturing P&L | Manufacturing |
| industrial_services_pl | Industrial Services P&L | Industrial |
| saas_pl | SaaS P&L | SaaS |
| transportation_pl | Transportation P&L | Transportation |

### Operations Domain (8)
| ID | Name | Industry |
|----|------|----------|
| geographic_hierarchy | Geographic | General |
| department_hierarchy | Department | General |
| asset_hierarchy | Asset Class | General |
| legal_entity_hierarchy | Legal Entity | General |
| upstream_field_hierarchy | Field Hierarchy | E&P |
| midstream_asset_hierarchy | Midstream Assets | Midstream |
| manufacturing_plant_hierarchy | Plant Hierarchy | Manufacturing |
| fleet_hierarchy | Fleet & Routes | Transportation |

### Finance Domain (2)
| ID | Name |
|----|------|
| cost_center_hierarchy | Cost Centers |
| profit_center_hierarchy | Profit Centers |

---

## Skills (7)

| ID | Domain | Industries |
|----|--------|------------|
| financial-analyst | Accounting | General |
| manufacturing-analyst | Accounting | Manufacturing |
| fpa-oil-gas-analyst | Finance | Oil & Gas |
| fpa-cost-analyst | Finance | General, Manufacturing |
| saas-metrics-analyst | Finance | SaaS |
| operations-analyst | Operations | General |
| transportation-analyst | Operations | Transportation |

---

## Tech Stack

### Service Ports
| Service | Port |
|---------|------|
| Frontend | 8000 |
| Backend | 8001 |
| MySQL | 3308 |
| Redis | 6381 |

### API Keys
- `v2-dev-key-1` - Primary
- `v2-dev-key-2` - Secondary

---

## Industry Keywords

| Industry | Keywords |
|----------|----------|
| oil_gas | well, field, basin, lease, royalty, LOE, DD&A, BOE |
| manufacturing | plant, BOM, WIP, variance, standard_cost |
| saas | ARR, MRR, churn, LTV, CAC, cohort |
| transportation | fleet, lane, terminal, operating_ratio |

---

## Folder Structure

```
Databridge_AI/
├── src/                    # Core logic and FastMCP server
├── data/                   # workflow.json, audit_trail.csv
├── docs/                   # MANIFEST.md
├── tests/                  # Pytest suite
├── templates/              # Hierarchy templates by domain
├── skills/                 # AI expertise definitions
└── knowledge_base/         # Client-specific configurations
```

---

*Last Updated: 2026-02-07 | Version: 0.34.0 | Tools: 292*
