# DataBridge AI API Reference

This document provides a complete reference for all MCP tools available in DataBridge AI V3 and V4.

---

## Table of Contents

1. [Overview](#overview)
2. [V4 Analytics Engine (37 Tools)](#v4-analytics-engine-37-tools)
   - [Query Tools (10)](#query-tools-10)
   - [Insights Tools (8)](#insights-tools-8)
   - [Knowledge Base Tools (7)](#knowledge-base-tools-7)
   - [FP&A Tools (12)](#fpa-tools-12)
3. [V3 Hierarchy Builder (92 Tools)](#v3-hierarchy-builder-92-tools)
   - [Project Tools (5)](#project-tools-5)
   - [Hierarchy Tools (15)](#hierarchy-tools-15)
   - [Reconciliation Tools (20)](#reconciliation-tools-20)
   - [Template Tools (16)](#template-tools-16)
4. [Error Handling](#error-handling)

---

## Overview

DataBridge AI provides MCP (Model Context Protocol) tools that allow LLMs like Claude to interact with data warehouses, perform analytics, and manage financial hierarchies.

### Response Format

All tools return a consistent response format:

```json
{
  "success": true,
  "message": "Operation completed successfully",
  "data": { ... },
  "errors": []
}
```

### Authentication

Tools use API key authentication. Pass the key via:
- Environment variable: `DATABRIDGE_API_KEY`
- Or in tool configuration

---

## V4 Analytics Engine (37 Tools)

### Query Tools (10)

#### `execute_sql`
Execute a SQL query against the connected data warehouse.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| query | string | Yes | SQL query to execute |
| limit | integer | No | Maximum rows to return (default: 1000) |
| timeout | integer | No | Query timeout in seconds (default: 30) |

**Example:**
```python
execute_sql(
    query="SELECT * FROM fact_gl_journal WHERE period = '2024-01'",
    limit=100
)
```

---

#### `build_query`
Build a SQL query using a fluent interface.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| table | string | Yes | Main table to query |
| columns | list[string] | No | Columns to select (default: all) |
| filters | dict | No | WHERE conditions |
| group_by | list[string] | No | GROUP BY columns |
| order_by | list[string] | No | ORDER BY columns |
| limit | integer | No | Result limit |

**Example:**
```python
build_query(
    table="fact_sales",
    columns=["region", "SUM(amount) as total"],
    filters={"year": 2024},
    group_by=["region"]
)
```

---

#### `nl_to_sql`
Convert natural language to SQL query.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| question | string | Yes | Natural language question |
| context | dict | No | Additional context (tables, columns) |
| dialect | string | No | SQL dialect (snowflake, postgresql, tsql) |

**Example:**
```python
nl_to_sql(
    question="What is the total revenue by region for Q1 2024?",
    dialect="snowflake"
)
```

---

#### `execute_nl_query`
Execute a natural language query (converts to SQL and executes).

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| question | string | Yes | Natural language question |
| limit | integer | No | Maximum rows to return |

---

#### `get_query_plan`
Get the execution plan for a SQL query.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| query | string | Yes | SQL query to analyze |

---

#### `explain_query`
Explain what a SQL query does in plain English.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| query | string | Yes | SQL query to explain |

---

#### `suggest_questions`
Suggest questions that can be asked about the data.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| table | string | No | Specific table to focus on |
| category | string | No | Category (finance, sales, operations) |

---

#### `get_query_history`
Get recent query history.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| limit | integer | No | Number of queries to return |
| user | string | No | Filter by user |

---

#### `save_query`
Save a query for reuse.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| name | string | Yes | Query name |
| query | string | Yes | SQL query |
| description | string | No | Query description |
| tags | list[string] | No | Tags for organization |

---

#### `export_query_results`
Export query results to a file.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| query | string | Yes | SQL query to execute |
| format | string | No | Output format (csv, json, xlsx) |
| filename | string | No | Output filename |

---

### Insights Tools (8)

#### `detect_anomalies`
Detect statistical anomalies in data.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| data | list | Yes | Data values to analyze |
| method | string | No | Detection method (zscore, iqr, isolation_forest) |
| threshold | float | No | Anomaly threshold (default: 3.0 for z-score) |
| time_column | string | No | Time column for time series |

**Returns:**
```json
{
  "anomalies": [
    {"index": 5, "value": 1500000, "score": 4.2, "severity": "high"}
  ],
  "summary": {"total_points": 100, "anomaly_count": 3}
}
```

---

#### `analyze_trends`
Analyze trends in time series data.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| data | list | Yes | Time series data |
| time_column | string | No | Time column name |
| value_column | string | No | Value column name |

**Returns:**
```json
{
  "direction": "increasing",
  "strength": 0.85,
  "percent_change": 12.5,
  "is_significant": true,
  "forecast": [110, 115, 120]
}
```

---

#### `compare_periods`
Compare two time periods.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| current_period | dict | Yes | Current period data |
| prior_period | dict | Yes | Prior period data |
| metrics | list[string] | No | Metrics to compare |

---

#### `generate_summary`
Generate a summary of data insights.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| data | dict | Yes | Data to summarize |
| focus_areas | list[string] | No | Areas to focus on |

---

#### `identify_patterns`
Identify patterns in data.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| data | list | Yes | Data to analyze |
| pattern_types | list[string] | No | Types to look for (seasonal, cyclical, trend) |

---

#### `forecast_simple`
Generate a simple forecast.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| data | list | Yes | Historical data |
| periods | integer | Yes | Number of periods to forecast |
| method | string | No | Method (linear, moving_average, exponential) |

---

#### `get_kpi_dashboard`
Get KPI dashboard data.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| entity | string | No | Entity to filter by |
| period | string | No | Period to show |

---

#### `generate_insight_report`
Generate a comprehensive insight report.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| data | dict | Yes | Data to analyze |
| report_type | string | No | Type (executive, detailed, technical) |

---

### Knowledge Base Tools (7)

#### `search_glossary`
Search the business glossary.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| query | string | Yes | Search query |
| domain | string | No | Domain filter (finance, accounting) |
| limit | integer | No | Maximum results |

---

#### `get_term_definition`
Get the definition of a business term.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| term | string | Yes | Term to look up |

---

#### `add_glossary_term`
Add a new term to the glossary.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| term | string | Yes | Term name |
| definition | string | Yes | Term definition |
| domain | string | No | Domain category |
| synonyms | list[string] | No | Alternative names |

---

#### `search_metrics`
Search for metric definitions.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| query | string | Yes | Search query |
| category | string | No | Metric category |

---

#### `get_metric_definition`
Get detailed metric definition.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| metric_name | string | Yes | Metric name |

---

#### `get_hierarchy_context`
Get hierarchy context for analysis.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| hierarchy_id | string | Yes | Hierarchy ID from V3 |
| include_mappings | boolean | No | Include source mappings |

---

#### `get_industry_patterns`
Get industry-specific patterns and benchmarks.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| industry | string | Yes | Industry code |
| pattern_type | string | No | Type (kpis, seasonality, benchmarks) |

---

### FP&A Tools (12)

#### `sync_period_data`
Initialize and sync data for a close period.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| period | string | Yes | Period identifier (e.g., "2024-01") |
| fiscal_year | string | Yes | Fiscal year (e.g., "FY2024") |
| entity | string | No | Entity or "ALL" |
| source_systems | list[string] | No | Systems to sync from |

---

#### `validate_close_readiness`
Validate readiness for period close.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| period | string | Yes | Period identifier |
| validation_checks | list[string] | No | Specific checks to run |

**Returns:**
```json
{
  "ready": true,
  "validations": [
    {"check": "data_completeness", "passed": true},
    {"check": "balance_reconciliation", "passed": true}
  ],
  "blocking_issues": []
}
```

---

#### `reconcile_subledger_to_gl`
Reconcile a subledger to the general ledger.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| period | string | Yes | Period identifier |
| subledger | string | Yes | Subledger name (AR, AP, FA) |
| gl_account | string | Yes | GL account number |
| tolerance | float | No | Acceptable difference (default: 0.01) |

---

#### `lock_period`
Lock a period to prevent further changes.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| period | string | Yes | Period to lock |
| approver | string | Yes | Approver name/ID |
| comments | string | No | Lock comments |

---

#### `analyze_budget_variance`
Analyze budget vs actual variance.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| period | string | Yes | Period to analyze |
| hierarchy_id | string | No | Hierarchy for grouping |
| threshold | float | No | Materiality threshold |
| include_commentary | boolean | No | Generate commentary |

**Returns:**
```json
{
  "total_variance": -50000,
  "variance_percent": -5.2,
  "is_favorable": false,
  "drivers": [
    {"name": "Revenue", "variance": -75000, "contribution": 0.6},
    {"name": "COGS", "variance": 25000, "contribution": 0.4}
  ],
  "commentary": "Revenue shortfall driven by..."
}
```

---

#### `analyze_prior_year_variance`
Analyze current vs prior year variance.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| period | string | Yes | Current period |
| prior_period | string | No | Prior period (auto-calculated if omitted) |
| metrics | list[string] | No | Metrics to compare |

---

#### `identify_variance_drivers`
Identify drivers of variance.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| period | string | Yes | Period to analyze |
| comparison_type | string | Yes | Type (budget, prior_year) |
| top_n | integer | No | Number of drivers to return |

---

#### `generate_variance_commentary`
Generate executive commentary for variances.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| variance_data | dict | Yes | Variance analysis results |
| style | string | No | Commentary style (executive, detailed) |
| tone | string | No | Tone (neutral, optimistic, cautious) |

---

#### `get_current_forecast`
Get the current forecast.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| fiscal_year | string | Yes | Fiscal year |
| scenario | string | No | Scenario name (default: "base") |
| granularity | string | No | Period granularity (monthly, quarterly) |

---

#### `update_rolling_forecast`
Update the rolling forecast with actuals.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| fiscal_year | string | Yes | Fiscal year |
| actual_period | string | Yes | Last actual period |
| method | string | No | Forecast method (straight_line, trend, seasonal) |

---

#### `model_scenario`
Model a what-if scenario.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| scenario_name | string | Yes | Scenario name |
| base_scenario | string | No | Base scenario to start from |
| assumptions | list[dict] | Yes | List of assumption changes |

**Example:**
```python
model_scenario(
    scenario_name="Revenue +10%",
    assumptions=[
        {"metric": "revenue", "adjustment_type": "percent", "value": 10}
    ]
)
```

---

#### `compare_scenarios`
Compare multiple forecast scenarios.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| scenarios | list[string] | Yes | Scenario names to compare |
| metrics | list[string] | No | Metrics to compare |
| format | string | No | Output format (table, chart_data) |

---

## V3 Hierarchy Builder (92 Tools)

### Project Tools (5)

| Tool | Description |
|------|-------------|
| `create_hierarchy_project` | Create a new hierarchy project |
| `list_hierarchy_projects` | List all projects |
| `get_hierarchy_project` | Get project details |
| `update_hierarchy_project` | Update project settings |
| `delete_hierarchy_project` | Delete a project |

### Hierarchy Tools (15)

| Tool | Description |
|------|-------------|
| `create_hierarchy` | Create a new hierarchy node |
| `get_hierarchy` | Get hierarchy details |
| `update_hierarchy` | Update hierarchy properties |
| `delete_hierarchy` | Delete a hierarchy node |
| `get_hierarchy_tree` | Get full hierarchy tree |
| `move_hierarchy_node` | Move a node in the tree |
| `get_hierarchy_children` | Get child nodes |
| `add_source_mapping` | Add source system mapping |
| `remove_source_mapping` | Remove a mapping |
| `get_mappings_by_precedence` | Get mappings sorted by precedence |
| `create_formula_group` | Create a formula group |
| `add_formula_rule` | Add a formula rule |
| `list_formula_groups` | List all formula groups |
| `export_hierarchy_csv` | Export hierarchy to CSV |
| `import_hierarchy_csv` | Import hierarchy from CSV |

### Reconciliation Tools (20)

| Tool | Description |
|------|-------------|
| `load_csv` | Load data from CSV file |
| `load_json` | Load data from JSON file |
| `query_database` | Query a database |
| `profile_data` | Generate data profile |
| `detect_schema_drift` | Detect schema changes |
| `compare_hashes` | Compare data using hashes |
| `get_orphan_details` | Get orphan record details |
| `get_conflict_details` | Get conflict details |
| `fuzzy_match_columns` | Fuzzy match columns |
| `fuzzy_deduplicate` | Remove fuzzy duplicates |
| `transform_column` | Apply column transformation |
| `merge_sources` | Merge multiple sources |
| `save_workflow_step` | Save workflow step |
| `get_workflow` | Get workflow details |
| `clear_workflow` | Clear workflow |
| `get_audit_log` | Get audit trail |
| `extract_text_from_pdf` | Extract text from PDF |
| `ocr_image` | OCR an image |
| `parse_table_from_text` | Parse table from text |
| `update_manifest` | Update the manifest |

### Template Tools (16)

| Tool | Description |
|------|-------------|
| `list_financial_templates` | List available templates |
| `get_template_details` | Get template information |
| `create_project_from_template` | Create project from template |
| `save_project_as_template` | Save as new template |
| `get_template_recommendations` | Get recommended templates |
| `list_available_skills` | List AI skills |
| `get_skill_details` | Get skill information |
| `get_skill_prompt` | Get skill system prompt |
| `list_client_profiles` | List client profiles |
| `get_client_knowledge` | Get client knowledge base |
| `update_client_knowledge` | Update client KB |
| `create_client_profile` | Create new client profile |
| `add_client_custom_prompt` | Add custom prompt |
| `list_application_documentation` | List docs |
| `get_application_documentation` | Get specific doc |
| `get_user_guide_section` | Get user guide section |

---

## Error Handling

All tools return errors in a consistent format:

```json
{
  "success": false,
  "message": "Error description",
  "errors": [
    "Detailed error message 1",
    "Detailed error message 2"
  ],
  "error_code": "ERR_001"
}
```

### Common Error Codes

| Code | Description |
|------|-------------|
| `AUTH_001` | Authentication failed |
| `AUTH_002` | Insufficient permissions |
| `CONN_001` | Database connection failed |
| `CONN_002` | Connection timeout |
| `DATA_001` | Invalid data format |
| `DATA_002` | Data not found |
| `QUERY_001` | Invalid SQL syntax |
| `QUERY_002` | Query timeout |
| `WF_001` | Workflow error |
| `WF_002` | Period locked |

---

*Document Version: 1.0.0 | Last Updated: 2026-01-30*
