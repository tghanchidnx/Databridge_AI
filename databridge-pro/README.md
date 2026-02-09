# DataBridge AI Pro

*Enterprise-grade data reconciliation, AI agents, and advanced analytics.*

---

## Overview

**DataBridge AI Pro** extends the Community Edition with advanced features for enterprise data management:

- **Cortex AI Agent** - Snowflake Cortex integration for AI-powered data analysis
- **Wright Pipeline** - 4-object data mart factory (source, dim, fact, mart)
- **GraphRAG Engine** - Anti-hallucination layer with vector search
- **Data Observability** - Metrics, alerting, and health monitoring
- **Full Data Catalog** - Metadata registry with automatic lineage
- **Column Lineage** - Track data flow from source to destination
- **AI Orchestrator** - Multi-agent task coordination

## Requirements

- DataBridge AI Community Edition >= 0.39.0
- Valid Pro or Enterprise license key
- Python 3.10+

## Installation

### Step 1: Configure Access to Private Package Index

```bash
# Using pip config
pip config set global.extra-index-url https://pypi.databridge.ai/simple/

# Or set environment variable
export PIP_EXTRA_INDEX_URL=https://pypi.databridge.ai/simple/
```

### Step 2: Install the Package

```bash
pip install databridge-ai-pro
```

### Step 3: Set Your License Key

```bash
# Set environment variable
export DATABRIDGE_LICENSE_KEY="DB-PRO-YOURCOMPANY-20260101-yoursignature"

# Or add to .env file
echo 'DATABRIDGE_LICENSE_KEY=DB-PRO-YOURCOMPANY-20260101-yoursignature' >> .env
```

### Step 4: Verify Installation

```python
from databridge_ai_pro import get_pro_status

status = get_pro_status()
print(f"License valid: {status['license_valid']}")
print(f"Features: {status['features']}")
```

## Features

### Cortex AI Agent

AI-powered data analysis using Snowflake Cortex:

```python
# Via MCP tools
cortex_complete(prompt="Analyze sales trends", model="mistral-large")
cortex_reason(question="Why did revenue drop in Q3?", max_steps=5)
```

### Wright Pipeline

Generate complete data mart structures:

```python
# Create a data mart configuration
create_mart_config(
    name="sales_mart",
    source_table="raw.orders",
    grain=["order_id"]
)

# Generate dimension, fact, and mart tables
generate_mart_pipeline("sales_mart")
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
obs_record_metric(asset="orders_table", metric="row_count", value=1000000)

# Create alert rules
obs_create_alert_rule(
    name="row_count_drop",
    condition="row_count < 900000",
    severity="critical"
)

# Get asset health
health = obs_get_asset_health(asset="orders_table")
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

## License Tiers

| Feature | Pro | Enterprise |
|---------|:---:|:----------:|
| Cortex AI Agent | ✅ | ✅ |
| Wright Pipeline | ✅ | ✅ |
| GraphRAG Engine | ✅ | ✅ |
| Data Observability | ✅ | ✅ |
| Data Catalog | ✅ | ✅ |
| Column Lineage | ✅ | ✅ |
| AI Orchestrator | ✅ | ✅ |
| Custom Agents | ❌ | ✅ |
| White-label | ❌ | ✅ |
| SLA Support | ❌ | ✅ |
| On-premise Deploy | ❌ | ✅ |

## Support

- **Pro License**: Email support (support@databridge.ai)
- **Enterprise License**: Priority support with SLA

## Contact

- Sales: sales@databridge.ai
- Support: support@databridge.ai
- Website: https://databridge.ai/pro

## License

Proprietary - see [LICENSE](LICENSE) for details.
