# DataBridge AI Pro

*Enterprise-grade data reconciliation, AI agents, and advanced analytics — ~277 tools.*

---

## Overview

**DataBridge AI Pro** extends the Community Edition (~106 tools) with **17 additional modules** and ~171 tools for enterprise data management. Pro includes everything in Community Edition plus:

### Data Infrastructure
| Module | Tools | Description |
|--------|-------|-------------|
| **Hierarchy Builder** | 44 | Multi-level hierarchy projects (up to 15 levels) for financial reporting and organizational structures |
| **Wright Pipeline** | 29 | 4-object data mart factory (VW_1 → DT_2 → DT_3A → DT_3) for hierarchy-driven mart generation |
| **Cortex AI** | 26 | Snowflake Cortex integration — natural language to SQL, AI reasoning loops, semantic models |
| **Data Catalog** | 19 | Centralized metadata registry with business glossary and automatic lineage detection |
| **Faux Objects** | 18 | Domain persona-based hierarchy generation and semantic modeling |
| **Connections** | 16 | Multi-database connectivity management for Snowflake, PostgreSQL, MySQL, and more |

### AI & Automation
| Module | Tools | Description |
|--------|-------|-------------|
| **AI Orchestrator** | 16 | Multi-agent task coordination, event publishing, and workflow management |
| **PlannerAgent** | 11 | AI-powered workflow planning, agent suggestions, and execution optimization |
| **GraphRAG Engine** | 10 | Anti-hallucination layer with graph + vector retrieval-augmented generation |
| **Unified AI Agent** | 10 | Cross-system operations with Book/Librarian/Researcher pattern |
| **Smart Recommendations** | 5 | Context-aware feature suggestions and guided workflows |

### Governance & Operations
| Module | Tools | Description |
|--------|-------|-------------|
| **Data Observability** | 15 | Real-time metrics, alerting, anomaly detection, and health scoring |
| **Data Versioning** | 12 | Semantic versioning, snapshots, rollback, and diff for all data objects |
| **Git/CI-CD** | 12 | Automated git workflows, GitHub PR creation, and CI/CD pipeline generation |
| **Lineage Tracking** | 11 | Column-level lineage from SQL/dbt with impact analysis |
| **Console Dashboard** | 5 | Real-time broadcast messaging and system monitoring |
| **Schema Matcher** | 5 | Cross-database schema comparison and fuzzy column mapping |
| **Data Matcher** | 4 | Row-level data comparison across database connections |

## Requirements

- DataBridge AI Community Edition >= 0.39.0
- Valid Pro or Enterprise license key
- Python 3.10+

## Installation

### Step 1: Set Your License Key

```bash
# Set environment variable
export DATABRIDGE_LICENSE_KEY="DB-PRO-YOURCOMPANY-20260101-yoursignature"

# Or add to .env file
echo 'DATABRIDGE_LICENSE_KEY=DB-PRO-YOURCOMPANY-20260101-yoursignature' >> .env
```

### Step 2: Install from GitHub Packages

```bash
# Install Pro package
pip install databridge-ai-pro --extra-index-url https://ghp_TOKEN@raw.githubusercontent.com/tghanchidnx/Databridge_AI/main/
```

### Step 3: Verify Installation

```python
from databridge_ai_pro import get_pro_status

status = get_pro_status()
print(f"License valid: {status['license_valid']}")
print(f"Features: {status['features']}")
```

## Pro Examples Add-on

The **Pro Examples** package (`databridge-ai-examples`) provides comprehensive tests and tutorials:

| Category | Contents | Count |
|----------|----------|-------|
| Beginner Use Cases | Pizza, friends, school, sports tutorials | 4 cases |
| Financial Use Cases | SEC EDGAR, Apple, Microsoft analysis | 7 cases |
| Faux Objects Use Cases | Domain persona tutorials | 8 cases |
| CE Test Suite | Data loading, hashing, fuzzy, dbt, quality, diff | ~12 files |
| Pro Test Suite | Hierarchy, cortex, catalog, versioning, wright | ~15 files |
| Shared Fixtures | conftest.py, sample data | 2 files |

```bash
# Install CE tests + beginner tutorials
pip install databridge-ai-examples

# Install with Pro tests + advanced tutorials (requires Pro key)
pip install databridge-ai-examples[pro]
```

## Feature Highlights

### Cortex AI Agent

AI-powered data analysis using Snowflake Cortex:

```python
# Via MCP tools
cortex_complete(prompt="Analyze sales trends", model="mistral-large")
cortex_reason(question="Why did revenue drop in Q3?", max_steps=5)

# Cortex Analyst — natural language to SQL
analyst_ask(question="What was total revenue by region?",
            semantic_model_file="@ANALYTICS.PUBLIC.MODELS/sales.yaml")
```

### Hierarchy Builder

Multi-level hierarchy management for financial reporting:

```python
# Create and manage hierarchies
create_hierarchy_project(name="Revenue P&L", description="Revenue hierarchy")
create_hierarchy(project_id="...", name="Product Revenue", parent_id="...")
add_source_mapping(hierarchy_id="...", source_column="ACCOUNT_CODE", source_uid="41%")

# Export and deploy
export_hierarchy_csv(project_id="...")
generate_hierarchy_scripts(project_id="...")
```

### Wright Pipeline

Generate complete data mart structures:

```python
# Create a data mart configuration
create_mart_config(
    project_name="upstream_gross",
    report_type="GROSS",
    hierarchy_table="TBL_0_GROSS_LOS_REPORT_HIERARCHY"
)

# Generate the full 4-object pipeline
generate_mart_pipeline(config_name="upstream_gross")
```

### GraphRAG Engine

Validate AI outputs against your data:

```python
# Search with context
results = rag_search(query="revenue by region", top_k=5)

# Validate AI-generated content
validation = rag_validate_output(content="Revenue increased 20%", sources=results)
```

### Data Observability

Monitor data quality in real-time:

```python
# Record metrics
obs_record_metric(name="hierarchy.validation.success_rate", value=98.5,
                  type="gauge", tags='{"project_id": "revenue-pl"}')

# Create alert rules
obs_create_alert_rule(name="row_count_drop",
                      metric_name="row_count", threshold=900000,
                      comparison="<", severity="critical")

# Get asset health
obs_get_asset_health(asset_id="revenue-pl", asset_type="hierarchy_project")
```

### Data Catalog

Comprehensive metadata management:

```python
# Scan a connection for metadata
catalog_scan_connection(connection_id="snowflake_prod")

# Search the catalog
results = catalog_search(query="customer dimension")

# Get automatic lineage from SQL
lineage = catalog_auto_lineage_from_sql(sql="SELECT * FROM dim_customer")
```

### Lineage Tracking

Column-level lineage and impact analysis:

```python
# Track lineage from SQL
catalog_auto_lineage_from_sql(sql="INSERT INTO fact_sales SELECT ...")

# Analyze change impact
catalog_impact_from_asset(asset_id="dim_customer")
```

## License Tiers

| Feature | Community | Pro | Pro Examples | Enterprise |
|---------|:---------:|:---:|:------------:|:----------:|
| Data Reconciliation (~106 tools) | ✅ | ✅ | | ✅ |
| Hierarchy Builder (44 tools) | | ✅ | | ✅ |
| Wright Pipeline (29 tools) | | ✅ | | ✅ |
| Cortex AI Agent (26 tools) | | ✅ | | ✅ |
| Data Catalog (19 tools) | | ✅ | | ✅ |
| Faux Objects (18 tools) | | ✅ | | ✅ |
| Connections (16 tools) | | ✅ | | ✅ |
| AI Orchestrator (16 tools) | | ✅ | | ✅ |
| Data Observability (15 tools) | | ✅ | | ✅ |
| Data Versioning (12 tools) | | ✅ | | ✅ |
| Git/CI-CD (12 tools) | | ✅ | | ✅ |
| Lineage Tracking (11 tools) | | ✅ | | ✅ |
| PlannerAgent (11 tools) | | ✅ | | ✅ |
| GraphRAG Engine (10 tools) | | ✅ | | ✅ |
| Unified AI Agent (10 tools) | | ✅ | | ✅ |
| Console Dashboard (5 tools) | | ✅ | | ✅ |
| Schema Matcher (5 tools) | | ✅ | | ✅ |
| Data Matcher (4 tools) | | ✅ | | ✅ |
| 47 Tests + 19 Tutorials | | | ✅ | |
| Custom Agents | | | | ✅ |
| White-label | | | | ✅ |
| SLA Support | | | | ✅ |
| On-premise Deploy | | | | ✅ |

**License Key Format:** `DB-{TIER}-{CUSTOMER_ID}-{EXPIRY}-{SIGNATURE}`

## Package Distribution

| Package | Location | Install |
|---------|----------|---------|
| `databridge-ai` | PyPI (public) | `pip install databridge-ai` |
| `databridge-ai-pro` | GitHub Packages (private) | `pip install databridge-ai-pro` (+ license key) |
| `databridge-ai-examples` | GitHub Packages (private) | `pip install databridge-ai-examples` (+ license key) |

## Support

- **Pro License**: Email support (support@databridge.ai)
- **Enterprise License**: Priority support with SLA

## Contact

- Sales: sales@databridge.ai
- Support: support@databridge.ai

## License

Proprietary - see [LICENSE](LICENSE) for details.
