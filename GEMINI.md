# DataBridge AI: Detailed Reference Guide

> **For Gemini Context Memory** - This file contains detailed documentation, examples, and architecture diagrams.
> Claude uses compact CLAUDE.md for quick reference and queries Gemini for detailed info.

---

## Commercialization & Licensing Structure

DataBridge AI uses a tiered product structure with open-source Community Edition and licensed Pro/Enterprise editions.

### Product Tiers

```
┌──────────────────────────────────────────────────────────────────────────────────────┐
│                              DataBridge AI Product Tiers                              │
├──────────────────────────────────────────────────────────────────────────────────────┤
│  ┌────────────────┐  ┌────────────────┐  ┌──────────────────┐  ┌────────────────┐   │
│  │ COMMUNITY (CE) │  │  PRO (Licensed)│  │  PRO EXAMPLES    │  │   ENTERPRISE   │   │
│  │   Free         │  │ GitHub Packages│  │  Licensed Add-on │  │    Custom      │   │
│  │   Public PyPI  │  │  License Key   │  │  GitHub Packages │  │  Dedicated     │   │
│  ├────────────────┤  ├────────────────┤  ├──────────────────┤  ├────────────────┤   │
│  │ • Data Recon.  │  │ Everything CE  │  │ 47 test files    │  │ Everything Pro │   │
│  │ • Fuzzy Match  │  │ + Cortex AI    │  │ 19 use-case      │  │ + Custom agents│   │
│  │ • PDF/OCR      │  │ + Wright       │  │   tutorials      │  │ + White-label  │   │
│  │ • Data Profile │  │ + GraphRAG     │  │ • Beginner (4)   │  │ + SLA support  │   │
│  │ • dbt Basic    │  │ + Observability│  │ • Financial (7)  │  │ + On-premise   │   │
│  │ • Data Quality │  │ + Full Catalog │  │ • Faux Objects(8)│  │ + Training     │   │
│  │ • UI Dashboard │  │ + Lineage      │  │ • CE tests       │  │                │   │
│  │                │  │ + Orchestrator │  │ • Pro tests      │  │                │   │
│  │ ~106 tools     │  │ ~284 tools     │  │ Requires Pro key │  │ 348+ tools     │   │
│  └────────────────┘  └────────────────┘  └──────────────────┘  └────────────────┘   │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

### License Key System

**Format:** `DB-{TIER}-{CUSTOMER_ID}-{EXPIRY}-{SIGNATURE}`

**Examples:**
- `DB-CE-FREE0001-20990101-000000000000` (CE - perpetual)
- `DB-PRO-ACME0001-20270209-a1b2c3d4e5f6` (Pro - 1 year)
- `DB-ENTERPRISE-BIGCORP-20280101-xyz123` (Enterprise - custom)

**Validation:** Offline hash-based (SHA256), no server required.

### Module Classification by Tier

| Tier | Modules | Tools |
|------|---------|-------|
| **CE** | File Discovery, Data Loading, Profiling, Hashing, Fuzzy Matching, PDF/OCR, Workflow, Transform, Documentation, Templates (basic), Diff Utilities, dbt Basic, Data Quality | ~106 |
| **PRO** | Hierarchy Builder, Connections, Schema Matcher, Data Matcher, Orchestrator, Cortex AI, Wright Pipeline, Lineage, Git/CI-CD, Data Catalog, Versioning, GraphRAG, Observability | ~171 |
| **PRO EXAMPLES** | Beginner tutorials (4), Financial tutorials (7), Faux Objects tutorials (8), CE test suite, Pro test suite, Shared fixtures | 47 tests + 19 use cases |
| **ENT** | Custom Agents, White-label, SLA Support | Custom |

### Directory Structure

```
Databridge_AI/                    # PRIVATE - Main development repo
├── src/
│   ├── plugins/                  # License management
│   │   ├── __init__.py          # LicenseManager class
│   │   └── registry.py          # Plugin discovery
│   └── server.py                # Tier-aware tool registration
├── databridge-ce/               # PUBLIC - Community Edition
│   ├── pyproject.toml          # PyPI: databridge-ai
│   └── LICENSE                  # MIT
├── databridge-pro/              # PRIVATE - Pro Edition
│   ├── pyproject.toml          # GitHub Packages: databridge-ai-pro
│   └── LICENSE                  # Proprietary
├── databridge-ai-examples/      # PRIVATE - Pro Examples package
│   ├── src/
│   │   ├── __init__.py         # Examples registration
│   │   ├── use_cases/          # 19 tutorial use cases
│   │   └── tests/              # CE & Pro test suites
│   │       ├── ce/             # CE module tests
│   │       ├── pro/            # Pro module tests
│   │       └── conftest.py     # Shared fixtures
│   ├── pyproject.toml          # GitHub Packages: databridge-ai-examples
│   └── README.md
└── scripts/
    └── generate_license.py      # License key generator
```

### Key Commands

```bash
# Generate license key
python scripts/generate_license.py PRO CUSTOMER01 365
# Output: DB-PRO-CUSTOMER01-20270209-a1b2c3d4e5f6

# Validate license key
python scripts/generate_license.py --validate DB-PRO-CUSTOMER01-20270209-a1b2c3d4e5f6

# Test license system
python scripts/test_license_system.py

# Set license key (environment)
export DATABRIDGE_LICENSE_KEY="DB-PRO-..."
```

### Distribution (LIVE)

| Package | Registry | Install Command |
|---------|----------|-----------------|
| `databridge-ai` | [PyPI](https://pypi.org/project/databridge-ai/) | `pip install databridge-ai` |
| `databridge-ai-pro` | [GitHub](https://github.com/tghanchidnx/databridge-ai-pro) | See below |
| `databridge-ai-examples` | GitHub Packages | `pip install databridge-ai-examples` (+ license key) |

**Pro Installation:**
```bash
# Set credentials
export GH_TOKEN="<github_personal_access_token>"
export DATABRIDGE_LICENSE_KEY="DB-PRO-CUSTOMER-EXPIRY-SIGNATURE"

# Install CE first, then Pro
pip install databridge-ai
pip install "databridge-ai-pro @ git+https://${GH_TOKEN}@github.com/tghanchidnx/databridge-ai-pro.git@v0.40.0"
```

**Pro Examples Installation:**
```bash
# CE tests + beginner use cases
pip install databridge-ai-examples

# Include Pro tests + advanced use cases
pip install databridge-ai-examples[pro]
```

### GitHub Repositories

| Repository | Visibility | URL |
|------------|------------|-----|
| Databridge_AI | Private | github.com/tghanchidnx/Databridge_AI |
| databridge-ai | Public | github.com/tghanchidnx/databridge-ai |
| databridge-ai-pro | Private | github.com/tghanchidnx/databridge-ai-pro |

### LicenseManager API

```python
from src.plugins import get_license_manager

mgr = get_license_manager()
print(mgr.tier)              # 'CE', 'PRO', or 'ENTERPRISE'
print(mgr.is_pro())          # True if Pro or higher
print(mgr.is_pro_examples()) # True if Pro Examples available (requires Pro)
print(mgr.is_enterprise())   # True if Enterprise
print(mgr.get_status())      # Full status dict

# MCP Tool
get_license_status()         # Returns license info as JSON
```

### Full Documentation
See `docs/COMMERCIALIZATION.md` for complete details.

---

## Module Reference (348 Tools)

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

### 3. Hierarchy Knowledge Base (49 tools)

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

### 9. Cortex Analyst (14 tools)

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

### 10. Wright Module (31 tools)

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
| Service | Port | Notes |
|---------|------|-------|
| UI Dashboard (primary) | 5050 | `python run_ui.py` — Flask-based, main UI |
| Frontend (Docker) | 8000 | |
| Backend (Docker) | 8001 | |
| MySQL | 3308 | |
| Redis | 6381 | |

### API Keys
- `dev-key-1` - Primary
- `dev-key-2` - Secondary

---

## Industry Keywords

| Industry | Keywords |
|----------|----------|
| oil_gas | well, field, basin, lease, royalty, LOE, DD&A, BOE |
| manufacturing | plant, BOM, WIP, variance, standard_cost |
| saas | ARR, MRR, churn, LTV, CAC, cohort |
| transportation | fleet, lane, terminal, operating_ratio |

---

## Pro Examples Sub-Tier

The **Pro Examples** sub-tier packages tests and use-case tutorials as a separate pip package (`databridge-ai-examples`), gated behind a Pro license.

### Package: `databridge-ai-examples`

**License Gate:** Requires `DATABRIDGE_LICENSE_KEY` with Pro or higher tier.

**Module Set (`PRO_EXAMPLES_MODULES`):**
| Module ID | Contents |
|-----------|----------|
| `examples_beginner` | Use cases 01-04: Pizza shop sales, friend matching, school hierarchies, sports comparison |
| `examples_financial` | Use cases 05-11: SEC EDGAR analysis, Apple/Microsoft financials, balance sheets, full pipelines |
| `examples_faux_objects` | Use cases 12-19: Domain persona tutorials (financial, oil & gas, manufacturing, SaaS, transportation, SQL translator) |
| `tests_ce` | Test suite for CE modules: data loading, hashing, fuzzy matching, dbt, data quality, diff utilities |
| `tests_pro` | Test suite for Pro modules: hierarchy, cortex, catalog, versioning, wright, lineage, observability |
| `test_fixtures` | Shared conftest.py, sample data helpers |

### Use-Case Tutorials (19 total)

**Beginner (01-04):**
| # | Case | Description |
|---|------|-------------|
| 01 | Pizza Shop Sales Check | Sales reconciliation between POS and delivery |
| 02 | Find My Friends | Data matching across class rosters |
| 03 | School Report Card Hierarchy | Building a grading hierarchy |
| 04 | Sports League Comparison | Comparing stats from two sources |

**Financial (05-11):**
| # | Case | Description |
|---|------|-------------|
| 05 | Apple Money Checkup | SEC EDGAR financial analysis |
| 06 | Apple Money Tree | Hierarchy analysis of financials |
| 07 | Apple vs Microsoft | Comparative financial analysis |
| 08 | Apple Time Machine | Multi-year income statement analysis |
| 09 | Balance Sheet Detective | Balance sheet reconciliation |
| 10 | Full Financial Pipeline | End-to-end financial data pipeline |
| 11 | Wall Street Analyst | Advanced multi-statement analysis |

**Faux Objects (12-19):**
| # | Case | Description |
|---|------|-------------|
| 12 | Financial Analyst | Financial domain persona |
| 13 | Oil & Gas Analyst | Oil & gas domain persona |
| 14 | Operations Analyst | Operations domain persona |
| 15 | Cost Analyst | Cost analysis domain persona |
| 16 | Manufacturing Analyst | Manufacturing domain persona |
| 17 | SaaS Analyst | SaaS metrics domain persona |
| 18 | Transportation Analyst | Transportation domain persona |
| 19 | SQL Translator | SQL translation across dialects |

### Test Suites

**CE Tests (~12 files):** `test_data_loading`, `test_hashing`, `test_fuzzy_matching`, `test_workflow`, `test_dbt_integration`, `test_data_quality`, `test_diff`

**Pro Tests (~15 files):** `test_hierarchy_kb`, `test_flexible_import`, `test_faux_objects`, `test_faux_objects_personas`, `test_faux_objects_translator`, `test_cortex_agent`, `test_cortex_analyst`, `test_console_ws`, `test_lineage`, `test_git_integration`, `test_versioning`, `test_wright_enhancements`, `test_mart_factory`, `test_data_catalog`, `test_observability`

### Registration API

```python
from databridge_ai_examples import register_examples, USE_CASE_CATEGORIES

# Auto-registered via entry point: databridge.plugins -> examples
result = register_examples(mcp)
# Returns: {'status': 'registered', 'version': '0.40.0', 'use_case_count': 19, 'categories': ['beginner', 'financial', 'faux_objects']}

# License check
from src.plugins import get_license_manager
mgr = get_license_manager()
mgr.is_pro_examples()        # True if Pro license active
mgr.can_use_module('tests_ce')  # True if Pro license active
```

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
├── knowledge_base/         # Client-specific configurations
├── databridge-ai-examples/ # Pro Examples package (tests + tutorials)
└── use_cases_by_claude/    # Source use-case tutorials
```

---

*Last Updated: 2026-02-09 | Version: 0.40.0 | Tools: 348 | Packages: 3 (CE, Pro, Pro Examples)*


## Test Section
_Updated: 2026-02-08 23:35:42_

Test context storage

---


## Data Observability Module
_Updated: 2026-02-08 23:36:55_

Phase 32: Data Observability Module (15 tools)

Real-time metrics collection, alerting, anomaly detection, and health monitoring.

## Components
- MetricsStore: Time-series storage (JSONL append-only)
- AlertManager: Threshold-based alerting with severity levels (info/warning/critical)
- AnomalyDetector: Z-score based statistical anomaly detection
- HealthScorer: Composite health scoring for assets (quality/freshness/completeness/reliability)

## MCP Tools (15)

### Metrics (4)
- obs_record_metric: Record a metric data point (gauge/counter/histogram)
- obs_query_metrics: Query metrics by name, time range, and tags
- obs_get_metric_stats: Get aggregated statistics (min/max/avg/p50/p95/p99)
- obs_list_metrics: List all available metric names

### Alerting (5)
- obs_create_alert_rule: Create threshold-based alert rule
- obs_list_alert_rules: List all alert rules
- obs_list_active_alerts: List active (unresolved) alerts
- obs_acknowledge_alert: Acknowledge an alert
- obs_get_alert_history: Get historical alerts

### Anomaly Detection (3)
- obs_detect_anomaly: Check if value is anomalous vs historical baseline
- obs_get_anomaly_report: Get recent anomalies for a metric
- obs_configure_anomaly: Set Z-score threshold and sensitivity

### Health Scoring (3)
- obs_get_asset_health: Get composite health score (0-100) for an asset
- obs_get_system_health: Get overall system health dashboard
- obs_get_health_trends: Get health score trends over time

## Storage Format
data/observability/
  metrics.jsonl      # Append-only time-series
  alert_rules.json   # Alert rule definitions
  alerts.json        # Active and historical alerts
  anomalies.jsonl    # Detected anomalies log
  health_scores.json # Cached health scores

## Example Usage

# Record a metric
obs_record_metric(name='hierarchy.validation.success_rate', value=98.5, type='gauge', tags='{"project_id": "revenue-pl"}')

# Create alert rule
obs_create_alert_rule(name='Low success rate', metric_name='hierarchy.validation.success_rate', threshold=95.0, comparison='<', severity='warning')

# Detect anomaly
obs_detect_anomaly(metric_name='hierarchy.validation.success_rate', value=72.0)

# Get asset health
obs_get_asset_health(asset_id='revenue-pl', asset_type='hierarchy_project')

---


## README Documentation
_Updated: 2026-02-08 23:45:57_

README.md Updated - v0.40.0 with 348 Tools

Key modules documented:
- Hierarchy Builder (49 tools) - Multi-level hierarchy projects
- Data Reconciliation (38 tools) - CSV/SQL/PDF/JSON comparison
- Wright Module (31 tools) - Data mart generation with 4-object pipeline
- Cortex AI (26 tools) - Natural language to SQL via Snowflake
- Data Catalog (19 tools) - Metadata registry with business glossary
- Data Observability (15 tools) - Metrics, alerting, anomaly detection, health scoring
- Data Versioning (12 tools) - Semantic versioning with snapshots/rollback
- GraphRAG Engine (10 tools) - Anti-hallucination with graph+vector RAG
- Lineage Tracking (11 tools) - Column-level lineage and impact analysis
- Git/CI-CD (12 tools) - Automated workflows and GitHub integration
- dbt Integration (8 tools) - Generate dbt projects from hierarchies
- Data Quality (7 tools) - Expectation suites and data contracts

Architecture: MCP Protocol -> DataBridge Server -> Snowflake/CSV/SQL/PDF

---


## Documentation Updates v0.40.0
_Updated: 2026-02-08 23:49:54_

Updated documentation to v0.40.0 with 348 tools. Changes include: GETTING_STARTED.md (updated module table with Data Observability and GraphRAG, version bumps throughout), run_dashboard.py (API health/stats endpoints updated with observability module). All documentation now consistent across README.md, MANIFEST.md, GETTING_STARTED.md, and dashboard.

---


## UI Architecture
_Updated: 2026-02-08 23:59:41_

## UI Architecture (v0.40.0)

### Single Dashboard on Port 5050

**Entry Point:** `python run_ui.py`

**Location:** databridge-ce/ui/

**Stack:**
- Flask web server (server.py)
- Modern dark theme UI (index.html - 106KB)
- Professional typography: JetBrains Mono, DM Sans, Playfair Display

**Key Pages:**
- Dashboard - Stats, recent activity, quick start
- Agent Console - Real-time agent messaging  
- Tool Workbench - MCP tool execution
- Hierarchy Projects - Project management
- Workflow Editor - Visual workflow building
- Wright Builder - 4-object pipeline generation
- Administration - Settings and config
- Help/Docs - Documentation viewer

**API Endpoints:**
- GET /api/dashboard/stats - Tool count, project count, version
- GET /api/tools - List available MCP tools
- GET /api/projects - List hierarchy projects
- POST /api/wright/generate - Generate Wright SQL

**Styling:**
- Dark theme: #0a0e17 background, #3b82f6 accent
- Card-based layout with subtle borders
- Animated status indicators
- Responsive grid layouts

---
