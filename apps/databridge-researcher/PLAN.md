# Headless DataBridge Analytics - Python CLI Application

## Version 4.0.0 - Fact Data Analysis Engine

**MCP Server Name:** Headless DataBridge Analytics - Python
**Companion To:** v3 Headless DataBridge AI (Hierarchy Management)

---

## Executive Summary

### Purpose
A **pure Python command-line analytics engine** that:
1. Connects to enterprise data warehouses (Snowflake, Databricks, SQL Server)
2. Reads metadata, schemas, and fact/dimension structures
3. Leverages v3 hierarchies for reporting dimensions
4. Performs analysis with **compute pushdown** to the data warehouse
5. Uses **natural language knowledgebase** for contextual understanding
6. Provides AI-powered insights through MCP integration

### Key Differentiator: Compute Pushdown
Unlike traditional tools that pull data locally, v4 **pushes computation to the data warehouse**:
- Generates optimized SQL/SparkSQL for the target platform
- Reads only metadata, schemas, and aggregated results
- Minimizes data transfer and memory usage
- Leverages data warehouse computing power

### Relationship to v3
```
┌─────────────────────────────────────────────────────────────────┐
│                    User / Claude AI                             │
├─────────────────────────────────────────────────────────────────┤
│  ┌──────────────────────┐        ┌──────────────────────┐      │
│  │  V3: Hierarchy       │◄──────►│  V4: Analytics       │      │
│  │  Management          │        │  Engine              │      │
│  │                      │        │                      │      │
│  │  • Build hierarchies │        │  • Analyze facts     │      │
│  │  • Map sources       │        │  • Query dimensions  │      │
│  │  • Define formulas   │        │  • Generate insights │      │
│  │  • Deploy structures │        │  • Pushdown compute  │      │
│  └──────────────────────┘        └──────────────────────┘      │
│           │                               │                     │
│           └───────────────┬───────────────┘                     │
│                           ▼                                     │
│              ┌──────────────────────┐                          │
│              │   Data Warehouse     │                          │
│              │  (Snowflake/         │                          │
│              │   Databricks/SQL)    │                          │
│              └──────────────────────┘                          │
└─────────────────────────────────────────────────────────────────┘
```

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [Core Capabilities](#2-core-capabilities)
3. [Data Warehouse Connectors](#3-data-warehouse-connectors)
4. [Natural Language Knowledgebase](#4-natural-language-knowledgebase)
5. [Skills Library](#5-skills-library)
6. [Docker Sample Dataset](#6-docker-sample-dataset)
7. [Implementation Plan](#7-implementation-plan)
8. [MCP Tools](#8-mcp-tools)
9. [Python Libraries](#9-python-libraries)
10. [CLI Commands](#10-cli-commands)

---

## 1. Architecture Overview

### 1.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         User Interface Layer                            │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────────────┐    │
│  │   CLI App      │  │  Interactive   │  │   MCP Server           │    │
│  │  (Typer/Click) │  │    REPL        │  │ "DataBridge Analytics" │    │
│  └────────────────┘  └────────────────┘  └────────────────────────┘    │
├─────────────────────────────────────────────────────────────────────────┤
│                         Analytics Layer                                 │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────────────┐    │
│  │  Query Engine  │  │  NL-to-SQL     │  │  Insight Generator     │    │
│  │  (Pushdown)    │  │  Translator    │  │  (Patterns/Anomalies)  │    │
│  └────────────────┘  └────────────────┘  └────────────────────────┘    │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────────────┐    │
│  │  Schema        │  │  Metadata      │  │  Knowledgebase         │    │
│  │  Analyzer      │  │  Catalog       │  │  (Context + Semantics) │    │
│  └────────────────┘  └────────────────┘  └────────────────────────┘    │
├─────────────────────────────────────────────────────────────────────────┤
│                         Connector Layer                                 │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────────────┐    │
│  │   Snowflake    │  │   Databricks   │  │   SQL Server           │    │
│  │   Connector    │  │   Connector    │  │   Connector            │    │
│  └────────────────┘  └────────────────┘  └────────────────────────┘    │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────────────┐    │
│  │   PostgreSQL   │  │   MySQL        │  │   V3 Hierarchy         │    │
│  │   Connector    │  │   Connector    │  │   Integration          │    │
│  └────────────────┘  └────────────────┘  └────────────────────────┘    │
├─────────────────────────────────────────────────────────────────────────┤
│                         Storage Layer                                   │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────────────┐    │
│  │   Metadata     │  │   Vector       │  │   Query                │    │
│  │   Cache        │  │   Store        │  │   History              │    │
│  │   (SQLite)     │  │   (ChromaDB)   │  │   (SQLite)             │    │
│  └────────────────┘  └────────────────┘  └────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────┘
```

### 1.2 Directory Structure

```
v4/
├── src/
│   ├── __init__.py
│   ├── main.py                      # Entry point
│   ├── cli/                         # CLI commands
│   │   ├── __init__.py
│   │   ├── app.py                   # Typer app
│   │   ├── connect_commands.py      # Connection management
│   │   ├── catalog_commands.py      # Metadata catalog
│   │   ├── analyze_commands.py      # Analysis commands
│   │   ├── query_commands.py        # Query execution
│   │   └── interactive.py           # REPL mode
│   │
│   ├── core/                        # Core services
│   │   ├── __init__.py
│   │   ├── config.py                # Pydantic settings
│   │   ├── database.py              # Local SQLite models
│   │   └── audit.py                 # Query audit logging
│   │
│   ├── connectors/                  # Data warehouse connectors
│   │   ├── __init__.py
│   │   ├── base.py                  # Abstract connector interface
│   │   ├── snowflake.py             # Snowflake connector
│   │   ├── databricks.py            # Databricks/Spark connector
│   │   ├── sqlserver.py             # SQL Server connector
│   │   ├── postgresql.py            # PostgreSQL connector
│   │   ├── mysql.py                 # MySQL connector
│   │   └── factory.py               # Connector factory
│   │
│   ├── catalog/                     # Metadata catalog
│   │   ├── __init__.py
│   │   ├── extractor.py             # Schema extraction
│   │   ├── models.py                # Catalog models (tables, columns, relationships)
│   │   ├── lineage.py               # Data lineage tracking
│   │   ├── statistics.py            # Column statistics
│   │   └── cache.py                 # Metadata caching
│   │
│   ├── analyzer/                    # Analysis engine
│   │   ├── __init__.py
│   │   ├── schema_analyzer.py       # Star/snowflake schema detection
│   │   ├── fact_analyzer.py         # Fact table analysis
│   │   ├── dimension_analyzer.py    # Dimension analysis
│   │   ├── measure_detector.py      # Measure/metric detection
│   │   ├── relationship_finder.py   # FK/join path discovery
│   │   └── quality_scorer.py        # Data quality scoring
│   │
│   ├── query/                       # Query engine
│   │   ├── __init__.py
│   │   ├── builder.py               # SQL query builder
│   │   ├── optimizer.py             # Query optimization
│   │   ├── executor.py              # Pushdown execution
│   │   ├── result_handler.py        # Result processing
│   │   └── dialects/                # SQL dialect adapters
│   │       ├── snowflake_dialect.py
│   │       ├── databricks_dialect.py
│   │       └── sqlserver_dialect.py
│   │
│   ├── nlp/                         # Natural language processing
│   │   ├── __init__.py
│   │   ├── nl_to_sql.py             # Natural language to SQL
│   │   ├── intent_classifier.py     # Query intent detection
│   │   ├── entity_extractor.py      # Table/column extraction
│   │   └── context_manager.py       # Conversation context
│   │
│   ├── knowledgebase/               # Semantic knowledgebase
│   │   ├── __init__.py
│   │   ├── schema_kb.py             # Schema knowledge
│   │   ├── business_glossary.py     # Business term definitions
│   │   ├── metric_definitions.py    # Metric/KPI definitions
│   │   ├── relationship_kb.py       # Join path knowledge
│   │   └── context_loader.py        # V3 hierarchy integration
│   │
│   ├── insights/                    # Insight generation
│   │   ├── __init__.py
│   │   ├── pattern_detector.py      # Trend/pattern detection
│   │   ├── anomaly_detector.py      # Anomaly identification
│   │   ├── comparison_engine.py     # Period/segment comparison
│   │   ├── summary_generator.py     # Executive summaries
│   │   └── recommendation_engine.py # Action recommendations
│   │
│   ├── vectors/                     # Vector embeddings
│   │   ├── __init__.py
│   │   ├── embedder.py              # Schema/column embeddings
│   │   ├── store.py                 # ChromaDB wrapper
│   │   └── semantic_search.py       # Semantic column search
│   │
│   ├── mcp/                         # MCP Server
│   │   ├── __init__.py
│   │   ├── server.py                # FastMCP server
│   │   ├── catalog_tools.py         # Catalog MCP tools
│   │   ├── analyze_tools.py         # Analysis MCP tools
│   │   ├── query_tools.py           # Query MCP tools
│   │   ├── insight_tools.py         # Insight MCP tools
│   │   └── kb_tools.py              # Knowledgebase MCP tools
│   │
│   └── integration/                 # V3 integration
│       ├── __init__.py
│       ├── hierarchy_client.py      # V3 hierarchy access
│       └── dimension_mapper.py      # Map hierarchies to dimensions
│
├── docker/                          # Docker sample environment
│   ├── docker-compose.yml           # Full stack orchestration
│   ├── Dockerfile.analytics         # V4 application container
│   ├── init-scripts/                # Database initialization
│   │   ├── 01-create-schema.sql
│   │   ├── 02-create-dimensions.sql
│   │   ├── 03-create-facts.sql
│   │   ├── 04-load-dimensions.sql
│   │   ├── 05-load-facts.sql
│   │   └── 06-create-views.sql
│   └── sample-data/                 # CSV seed data
│       ├── dim_date.csv
│       ├── dim_customer.csv
│       ├── dim_product.csv
│       ├── dim_geography.csv
│       ├── dim_account.csv
│       ├── fact_sales.csv
│       ├── fact_gl_transactions.csv
│       └── fact_production.csv
│
├── sample_data/                     # Sample data generators
│   ├── generators/
│   │   ├── date_generator.py
│   │   ├── customer_generator.py
│   │   ├── product_generator.py
│   │   ├── sales_generator.py
│   │   └── gl_generator.py
│   └── scenarios/
│       ├── oil_gas_scenario.py
│       ├── manufacturing_scenario.py
│       └── retail_scenario.py
│
├── skills/                          # AI Skills
│   ├── index.json
│   ├── README.md
│   ├── data-analyst-prompt.txt
│   ├── bi-developer-prompt.txt
│   ├── data-scientist-prompt.txt
│   ├── business-user-prompt.txt
│   ├── data-steward-prompt.txt
│   ├── sql-expert-prompt.txt
│   └── executive-consumer-prompt.txt
│
├── knowledgebase/                   # Business glossary & definitions
│   ├── glossary.json                # Business term definitions
│   ├── metrics.json                 # KPI/metric definitions
│   ├── relationships.json           # Known table relationships
│   └── industry/                    # Industry-specific knowledge
│       ├── oil_gas.json
│       ├── manufacturing.json
│       ├── retail.json
│       └── healthcare.json
│
├── docs/                            # Documentation
│   ├── USER_GUIDE.md
│   ├── QUERY_GUIDE.md
│   ├── CONNECTOR_GUIDE.md
│   └── SAMPLE_DATA.md
│
├── tests/                           # Test suite
│   ├── conftest.py
│   ├── test_connectors.py
│   ├── test_catalog.py
│   ├── test_analyzer.py
│   ├── test_query.py
│   └── test_mcp.py
│
├── pyproject.toml                   # Poetry/pip config
├── requirements.txt                 # Dependencies
├── requirements-optional.txt        # Optional deps
├── .env.example                     # Environment template
├── PLAN.md                          # This file
└── README.md                        # Project README
```

---

## 2. Core Capabilities

### 2.1 Metadata Catalog

**Extract and cache schema information without pulling data:**

```python
# Catalog extraction (metadata only - no data transfer)
catalog = CatalogExtractor(connection)

# Get all tables in a schema
tables = catalog.get_tables("ANALYTICS", "REPORTING")
# Returns: [TableMetadata(name, columns, row_count_estimate, size_bytes, ...)]

# Get column details with statistics
columns = catalog.get_columns("ANALYTICS", "REPORTING", "FACT_SALES")
# Returns: [ColumnMetadata(name, type, nullable, min, max, distinct_count, ...)]

# Detect relationships (FK inference)
relationships = catalog.detect_relationships("ANALYTICS", "REPORTING")
# Returns: [Relationship(from_table, from_column, to_table, to_column, confidence)]
```

### 2.2 Schema Analysis

**Automatically detect star/snowflake schema patterns:**

```python
analyzer = SchemaAnalyzer(catalog)

# Detect fact tables (high cardinality, numeric measures, date FKs)
fact_tables = analyzer.detect_fact_tables()
# Returns: [FactTableAnalysis(table_name, measures, dimensions, grain)]

# Detect dimension tables (low cardinality, descriptive attributes)
dimension_tables = analyzer.detect_dimension_tables()
# Returns: [DimensionTableAnalysis(table_name, attributes, hierarchy_levels)]

# Map to V3 hierarchies
hierarchy_mapping = analyzer.map_to_v3_hierarchies(v3_client)
# Returns: {dim_table: hierarchy_id, ...}
```

### 2.3 Query Pushdown Engine

**Generate optimized SQL for the target platform:**

```python
# Build query with pushdown
query = QueryBuilder(connection) \
    .select("d.region", "SUM(f.amount) as total_sales") \
    .from_table("fact_sales", alias="f") \
    .join("dim_geography", "d", "f.geo_key = d.geo_key") \
    .where("f.date_key >= 20240101") \
    .group_by("d.region") \
    .order_by("total_sales DESC") \
    .limit(10)

# Execute on data warehouse (only results returned)
results = query.execute()
# Actual SQL executed on Snowflake/Databricks/SQL Server
# Only aggregated results (10 rows) transferred
```

### 2.4 Natural Language to SQL

**Translate business questions to optimized queries:**

```python
nl_engine = NLToSQL(catalog, knowledgebase)

# Natural language query
question = "What were total sales by region last quarter?"

# Generate SQL with context
sql, explanation = nl_engine.translate(question)
# SQL: SELECT d.region, SUM(f.amount) FROM fact_sales f JOIN dim_geography d...
# Explanation: "I'm summing the 'amount' measure from fact_sales,
#              grouped by region from dim_geography, filtered to Q4 2024"

# Execute with pushdown
results = nl_engine.execute(question)
```

### 2.5 Insight Generation

**Automatically detect patterns and anomalies:**

```python
insights = InsightEngine(connection, catalog)

# Detect anomalies in a metric
anomalies = insights.detect_anomalies(
    table="fact_sales",
    measure="amount",
    time_column="date_key",
    dimensions=["region", "product_category"]
)
# Returns: [Anomaly(date, dimension_values, expected, actual, severity)]

# Compare periods
comparison = insights.compare_periods(
    table="fact_sales",
    measure="amount",
    current_period="2024-Q4",
    prior_period="2023-Q4",
    dimensions=["region"]
)
# Returns: [Comparison(dimension, current, prior, change_pct, significance)]

# Generate executive summary
summary = insights.generate_summary(
    table="fact_sales",
    time_period="2024-Q4"
)
# Returns: "Q4 2024 sales totaled $12.3M, up 8% YoY. Northeast region
#          drove 45% of growth. Product category 'Electronics' showed
#          15% decline, flagged for review."
```

---

## 3. Data Warehouse Connectors

### 3.1 Connector Interface

```python
from abc import ABC, abstractmethod
from typing import List, Dict, Any

class DataWarehouseConnector(ABC):
    """Abstract base class for all data warehouse connectors."""

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to data warehouse."""
        pass

    @abstractmethod
    def get_databases(self) -> List[str]:
        """List available databases."""
        pass

    @abstractmethod
    def get_schemas(self, database: str) -> List[str]:
        """List schemas in a database."""
        pass

    @abstractmethod
    def get_tables(self, database: str, schema: str) -> List[TableMetadata]:
        """Get table metadata (no data transfer)."""
        pass

    @abstractmethod
    def get_columns(self, database: str, schema: str, table: str) -> List[ColumnMetadata]:
        """Get column metadata with statistics."""
        pass

    @abstractmethod
    def execute_query(self, sql: str, limit: int = 1000) -> QueryResult:
        """Execute SQL on the data warehouse (pushdown)."""
        pass

    @abstractmethod
    def get_query_plan(self, sql: str) -> QueryPlan:
        """Get execution plan without running query."""
        pass

    @abstractmethod
    def get_dialect(self) -> SQLDialect:
        """Return SQL dialect for query generation."""
        pass
```

### 3.2 Snowflake Connector

```python
class SnowflakeConnector(DataWarehouseConnector):
    """Snowflake-specific connector with compute pushdown."""

    def __init__(self, config: SnowflakeConfig):
        self.account = config.account
        self.user = config.user
        self.warehouse = config.warehouse
        self.role = config.role
        # Authentication: password, key pair, or OAuth

    def get_table_statistics(self, table: str) -> TableStatistics:
        """Get Snowflake-specific table stats from INFORMATION_SCHEMA."""
        sql = f"""
        SELECT
            ROW_COUNT,
            BYTES,
            CLUSTERING_KEY,
            LAST_ALTERED
        FROM {self.database}.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_NAME = '{table}'
        """
        return self.execute_query(sql)

    def get_column_statistics(self, table: str, column: str) -> ColumnStatistics:
        """Get column-level statistics with APPROXIMATE_COUNT_DISTINCT."""
        sql = f"""
        SELECT
            APPROX_COUNT_DISTINCT({column}) as distinct_count,
            MIN({column}) as min_value,
            MAX({column}) as max_value,
            AVG(CASE WHEN {column} IS NULL THEN 1 ELSE 0 END) as null_ratio
        FROM {table}
        """
        return self.execute_query(sql)
```

### 3.3 Databricks Connector

```python
class DatabricksConnector(DataWarehouseConnector):
    """Databricks/Spark connector with SQL warehouse support."""

    def __init__(self, config: DatabricksConfig):
        self.host = config.host  # adb-xxx.azuredatabricks.net
        self.http_path = config.http_path  # /sql/1.0/warehouses/xxx
        self.token = config.access_token
        self.catalog = config.catalog  # Unity Catalog

    def get_table_metadata(self, table: str) -> TableMetadata:
        """Use DESCRIBE EXTENDED for metadata."""
        sql = f"DESCRIBE EXTENDED {self.catalog}.{self.schema}.{table}"
        return self.execute_query(sql)

    def get_table_history(self, table: str) -> List[TableVersion]:
        """Delta Lake history for versioned tables."""
        sql = f"DESCRIBE HISTORY {table}"
        return self.execute_query(sql)
```

### 3.4 SQL Server Connector

```python
class SQLServerConnector(DataWarehouseConnector):
    """SQL Server connector with query optimization hints."""

    def __init__(self, config: SQLServerConfig):
        self.server = config.server
        self.database = config.database
        self.auth_mode = config.auth_mode  # windows, sql, azure_ad

    def get_index_info(self, table: str) -> List[IndexMetadata]:
        """Get index information for query optimization."""
        sql = f"""
        SELECT i.name, i.type_desc,
               STRING_AGG(c.name, ',') as columns
        FROM sys.indexes i
        JOIN sys.index_columns ic ON i.index_id = ic.index_id
        JOIN sys.columns c ON ic.column_id = c.column_id
        WHERE i.object_id = OBJECT_ID('{table}')
        GROUP BY i.name, i.type_desc
        """
        return self.execute_query(sql)
```

### 3.5 PostgreSQL Connector (Docker Sample)

```python
class PostgreSQLConnector(DataWarehouseConnector):
    """PostgreSQL connector for local Docker sample database."""

    def __init__(self, config: PostgreSQLConfig):
        self.host = config.host
        self.port = config.port
        self.database = config.database
        self.user = config.user

    def get_table_statistics(self, table: str) -> TableStatistics:
        """Use pg_stat_user_tables for statistics."""
        sql = f"""
        SELECT
            n_live_tup as row_estimate,
            pg_total_relation_size('{table}') as size_bytes,
            last_analyze
        FROM pg_stat_user_tables
        WHERE relname = '{table}'
        """
        return self.execute_query(sql)
```

---

## 4. Natural Language Knowledgebase

### 4.1 Business Glossary

```json
{
  "terms": [
    {
      "term": "Revenue",
      "definition": "Total income generated from sales of goods or services",
      "synonyms": ["sales", "income", "top line"],
      "tables": ["fact_sales", "fact_gl_transactions"],
      "columns": ["amount", "revenue_amount", "sales_amount"],
      "aggregation": "SUM",
      "industry": "general"
    },
    {
      "term": "COGS",
      "definition": "Cost of Goods Sold - direct costs attributable to production",
      "synonyms": ["cost of sales", "direct costs"],
      "tables": ["fact_gl_transactions"],
      "columns": ["debit_amount"],
      "filters": {"account_type": "COGS"},
      "aggregation": "SUM",
      "industry": "general"
    },
    {
      "term": "LOE",
      "definition": "Lease Operating Expense - costs to operate producing wells",
      "synonyms": ["lease operating", "operating expense per BOE"],
      "tables": ["fact_production_costs"],
      "columns": ["operating_cost"],
      "aggregation": "SUM",
      "calculation": "SUM(operating_cost) / SUM(boe_produced)",
      "industry": "oil_gas"
    }
  ]
}
```

### 4.2 Metric Definitions

```json
{
  "metrics": [
    {
      "id": "gross_margin",
      "name": "Gross Margin",
      "definition": "Profitability ratio measuring revenue minus COGS",
      "formula": "(Revenue - COGS) / Revenue * 100",
      "sql_template": "(SUM(revenue) - SUM(cogs)) / NULLIF(SUM(revenue), 0) * 100",
      "unit": "percentage",
      "direction": "higher_is_better",
      "benchmarks": {
        "poor": "<20%",
        "average": "20-40%",
        "good": ">40%"
      }
    },
    {
      "id": "yoy_growth",
      "name": "Year-over-Year Growth",
      "definition": "Percentage change compared to same period prior year",
      "sql_template": "(SUM(CASE WHEN year = CURRENT_YEAR THEN {measure} END) - SUM(CASE WHEN year = CURRENT_YEAR - 1 THEN {measure} END)) / NULLIF(SUM(CASE WHEN year = CURRENT_YEAR - 1 THEN {measure} END), 0) * 100",
      "unit": "percentage",
      "requires": ["time_dimension"]
    }
  ]
}
```

### 4.3 Schema Knowledge

```json
{
  "schemas": [
    {
      "database": "ANALYTICS",
      "schema": "REPORTING",
      "description": "Production reporting schema with star schema design",
      "fact_tables": [
        {
          "name": "FACT_SALES",
          "grain": "One row per sales transaction",
          "measures": ["amount", "quantity", "discount_amount"],
          "dimensions": ["date_key", "customer_key", "product_key", "geo_key"]
        }
      ],
      "dimension_tables": [
        {
          "name": "DIM_DATE",
          "type": "date",
          "key_column": "date_key",
          "hierarchy": ["year", "quarter", "month", "week", "day"]
        },
        {
          "name": "DIM_CUSTOMER",
          "type": "slowly_changing",
          "scd_type": 2,
          "key_column": "customer_key",
          "natural_key": "customer_id",
          "attributes": ["customer_name", "segment", "industry", "region"]
        }
      ]
    }
  ]
}
```

### 4.4 V3 Hierarchy Integration

```python
class HierarchyContextLoader:
    """Load V3 hierarchies as dimensional context."""

    def __init__(self, v3_client: V3HierarchyClient):
        self.v3_client = v3_client

    def load_hierarchy_as_dimension(self, hierarchy_id: str) -> DimensionContext:
        """
        Load a V3 hierarchy as dimension context for NL understanding.

        Example: P&L hierarchy becomes context for "show me revenue by department"
        - Revenue node has source mappings to GL accounts
        - Department levels define the drill path
        """
        hierarchy = self.v3_client.get_hierarchy_tree(hierarchy_id)

        return DimensionContext(
            name=hierarchy['hierarchy_name'],
            levels=[h['level_1'], h['level_2'], ...],  # Hierarchy levels
            members=self._extract_members(hierarchy),   # All nodes
            mappings=self._extract_mappings(hierarchy), # Source system links
            formulas=self._extract_formulas(hierarchy)  # Calculated members
        )

    def enrich_knowledgebase(self, project_id: str):
        """Add all V3 hierarchies to the knowledgebase for NL understanding."""
        hierarchies = self.v3_client.list_hierarchies(project_id)
        for h in hierarchies:
            context = self.load_hierarchy_as_dimension(h['hierarchy_id'])
            self.knowledgebase.add_dimension(context)
```

---

## 5. Skills Library

### 5.1 Skills Overview

| Skill ID | Name | Focus | Best For |
|----------|------|-------|----------|
| `data-analyst` | Data Analyst | SQL queries, data exploration | Ad-hoc analysis, report building |
| `bi-developer` | BI Developer | Schema design, optimization | Dashboard creation, data modeling |
| `data-scientist` | Data Scientist | Statistical analysis, patterns | Advanced analytics, ML prep |
| `business-user` | Business User | Plain English, no SQL | Self-service analytics |
| `data-steward` | Data Steward | Data quality, governance | Catalog management, lineage |
| `sql-expert` | SQL Expert | Complex queries, performance | Query optimization, debugging |
| `executive-consumer` | Executive Consumer | High-level insights | Strategic summaries, KPIs |

### 5.2 Skill Comparison

| Capability | Data Analyst | Business User | Executive |
|------------|--------------|---------------|-----------|
| Shows SQL | Yes | No | No |
| Technical depth | High | Low | Medium |
| Explains methodology | Yes | Simple | Summary |
| Visualization focus | Tables/Charts | Plain text | KPIs |
| Drill-down offers | Yes | Guided | Limited |

### 5.3 Skill Prompt Structure

Each skill includes:
1. **Role Definition** - Who they are, their expertise
2. **Communication Style** - How they explain things
3. **Query Approach** - How they build/explain queries
4. **Output Format** - How they present results
5. **Tool Preferences** - Which MCP tools they use
6. **Example Interactions** - Sample Q&A patterns

---

## 6. Docker Sample Dataset

### 6.1 Overview

Self-contained Docker environment with realistic financial/operational data:

```yaml
# docker/docker-compose.yml
version: '3.8'

services:
  # PostgreSQL as sample data warehouse
  postgres:
    image: postgres:16
    container_name: databridge-analytics-db
    environment:
      POSTGRES_DB: analytics
      POSTGRES_USER: analytics_user
      POSTGRES_PASSWORD: analytics_pass
    ports:
      - "5434:5432"
    volumes:
      - ./init-scripts:/docker-entrypoint-initdb.d
      - postgres_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U analytics_user -d analytics"]
      interval: 10s
      timeout: 5s
      retries: 5

  # pgAdmin for visual database management
  pgadmin:
    image: dpage/pgadmin4
    container_name: databridge-pgadmin
    environment:
      PGADMIN_DEFAULT_EMAIL: admin@databridge.local
      PGADMIN_DEFAULT_PASSWORD: admin
    ports:
      - "5050:80"
    depends_on:
      - postgres

  # Vector database for semantic search
  chromadb:
    image: chromadb/chroma:latest
    container_name: databridge-chromadb
    ports:
      - "8001:8000"
    volumes:
      - chroma_data:/chroma/chroma

  # V4 Analytics Application (optional - can run locally)
  analytics:
    build:
      context: .
      dockerfile: Dockerfile.analytics
    container_name: databridge-analytics-v4
    environment:
      - DATABASE_URL=postgresql://analytics_user:analytics_pass@postgres:5432/analytics
      - CHROMA_HOST=chromadb
      - CHROMA_PORT=8000
    depends_on:
      postgres:
        condition: service_healthy
    ports:
      - "8002:8000"
    volumes:
      - ../:/app

volumes:
  postgres_data:
  chroma_data:
```

### 6.2 Sample Schema Design

**Star Schema for Multi-Industry Analysis:**

```sql
-- ============================================
-- DIMENSION TABLES
-- ============================================

-- Date Dimension (Conformed)
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,                    -- YYYYMMDD format
    full_date DATE NOT NULL,
    day_of_week INT,
    day_name VARCHAR(10),
    day_of_month INT,
    day_of_year INT,
    week_of_year INT,
    month_number INT,
    month_name VARCHAR(10),
    quarter INT,
    quarter_name VARCHAR(6),                     -- Q1 2024
    year INT,
    fiscal_year INT,
    fiscal_quarter INT,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);

-- Customer Dimension
CREATE TABLE dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,            -- Natural key
    customer_name VARCHAR(255),
    segment VARCHAR(50),                         -- Enterprise, SMB, Consumer
    industry VARCHAR(100),
    region VARCHAR(100),
    country VARCHAR(100),
    state VARCHAR(100),
    city VARCHAR(100),
    credit_rating VARCHAR(10),
    effective_from DATE,
    effective_to DATE,
    is_current BOOLEAN DEFAULT TRUE
);

-- Product Dimension
CREATE TABLE dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL,
    product_name VARCHAR(255),
    category VARCHAR(100),
    subcategory VARCHAR(100),
    brand VARCHAR(100),
    unit_cost DECIMAL(15,2),
    unit_price DECIMAL(15,2),
    is_active BOOLEAN DEFAULT TRUE
);

-- Geography Dimension
CREATE TABLE dim_geography (
    geo_key SERIAL PRIMARY KEY,
    geo_id VARCHAR(50) NOT NULL,
    region VARCHAR(100),
    country VARCHAR(100),
    state VARCHAR(100),
    city VARCHAR(100),
    postal_code VARCHAR(20),
    latitude DECIMAL(10,6),
    longitude DECIMAL(10,6)
);

-- Account Dimension (Chart of Accounts)
CREATE TABLE dim_account (
    account_key SERIAL PRIMARY KEY,
    account_id VARCHAR(50) NOT NULL,
    account_name VARCHAR(255),
    account_type VARCHAR(50),                    -- Asset, Liability, Equity, Revenue, Expense
    account_subtype VARCHAR(100),
    is_balance_sheet BOOLEAN,
    is_debit_normal BOOLEAN,
    parent_account_id VARCHAR(50),
    level_1 VARCHAR(100),                        -- Maps to V3 hierarchy
    level_2 VARCHAR(100),
    level_3 VARCHAR(100),
    level_4 VARCHAR(100)
);

-- Cost Center Dimension
CREATE TABLE dim_cost_center (
    cost_center_key SERIAL PRIMARY KEY,
    cost_center_id VARCHAR(50) NOT NULL,
    cost_center_name VARCHAR(255),
    department VARCHAR(100),
    division VARCHAR(100),
    business_unit VARCHAR(100),
    manager VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE
);

-- ============================================
-- FACT TABLES
-- ============================================

-- Sales Fact (Transactional Grain)
CREATE TABLE fact_sales (
    sales_key SERIAL PRIMARY KEY,
    date_key INT REFERENCES dim_date(date_key),
    customer_key INT REFERENCES dim_customer(customer_key),
    product_key INT REFERENCES dim_product(product_key),
    geo_key INT REFERENCES dim_geography(geo_key),

    -- Degenerate dimensions
    order_id VARCHAR(50),
    order_line_number INT,

    -- Measures
    quantity INT,
    unit_price DECIMAL(15,2),
    discount_percent DECIMAL(5,2),
    discount_amount DECIMAL(15,2),
    gross_amount DECIMAL(15,2),
    net_amount DECIMAL(15,2),
    cost_amount DECIMAL(15,2),
    margin_amount DECIMAL(15,2)
);

-- GL Transactions Fact (Monthly Grain)
CREATE TABLE fact_gl_transactions (
    gl_key SERIAL PRIMARY KEY,
    date_key INT REFERENCES dim_date(date_key),
    account_key INT REFERENCES dim_account(account_key),
    cost_center_key INT REFERENCES dim_cost_center(cost_center_key),

    -- Degenerate dimensions
    journal_id VARCHAR(50),
    journal_line INT,
    source_system VARCHAR(50),

    -- Measures
    debit_amount DECIMAL(15,2),
    credit_amount DECIMAL(15,2),
    net_amount DECIMAL(15,2),                    -- debit - credit

    -- Audit
    posted_date DATE,
    posted_by VARCHAR(100)
);

-- Budget Fact (Monthly Grain)
CREATE TABLE fact_budget (
    budget_key SERIAL PRIMARY KEY,
    date_key INT REFERENCES dim_date(date_key),
    account_key INT REFERENCES dim_account(account_key),
    cost_center_key INT REFERENCES dim_cost_center(cost_center_key),

    -- Version tracking
    budget_version VARCHAR(50),                  -- Original, Revised, Forecast

    -- Measures
    budget_amount DECIMAL(15,2),
    forecast_amount DECIMAL(15,2)
);

-- Production Fact (Daily Grain - Oil & Gas / Manufacturing)
CREATE TABLE fact_production (
    production_key SERIAL PRIMARY KEY,
    date_key INT REFERENCES dim_date(date_key),
    asset_key INT,                               -- Well, Production Line, etc.
    product_key INT REFERENCES dim_product(product_key),

    -- Measures
    produced_quantity DECIMAL(15,4),
    target_quantity DECIMAL(15,4),
    downtime_hours DECIMAL(10,2),
    operating_hours DECIMAL(10,2),
    efficiency_percent DECIMAL(5,2),
    unit_cost DECIMAL(15,4)
);

-- ============================================
-- INDEXES FOR PERFORMANCE
-- ============================================

CREATE INDEX idx_fact_sales_date ON fact_sales(date_key);
CREATE INDEX idx_fact_sales_customer ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product ON fact_sales(product_key);
CREATE INDEX idx_fact_gl_date ON fact_gl_transactions(date_key);
CREATE INDEX idx_fact_gl_account ON fact_gl_transactions(account_key);
CREATE INDEX idx_fact_budget_date ON fact_budget(date_key);
```

### 6.3 Sample Data Generation

```python
# sample_data/generators/sales_generator.py

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

def generate_sales_data(
    start_date: datetime = datetime(2022, 1, 1),
    end_date: datetime = datetime(2024, 12, 31),
    num_customers: int = 500,
    num_products: int = 200,
    avg_daily_transactions: int = 100
) -> pd.DataFrame:
    """
    Generate realistic sales transaction data.

    Includes:
    - Seasonality (Q4 higher, summer lower)
    - Weekend patterns
    - Customer concentration (80/20 rule)
    - Product mix variation
    - Regional distribution
    """

    date_range = pd.date_range(start_date, end_date, freq='D')
    transactions = []

    for date in date_range:
        # Seasonality factor
        month = date.month
        if month in [11, 12]:
            seasonality = 1.4  # Holiday boost
        elif month in [6, 7, 8]:
            seasonality = 0.85  # Summer slowdown
        else:
            seasonality = 1.0

        # Weekend factor
        if date.weekday() >= 5:
            weekend_factor = 0.6
        else:
            weekend_factor = 1.0

        # Daily transaction count
        daily_count = int(avg_daily_transactions * seasonality * weekend_factor * np.random.uniform(0.8, 1.2))

        for _ in range(daily_count):
            # Pareto distribution for customers (80/20 rule)
            customer_key = int(np.random.pareto(1.5) * 10) % num_customers + 1
            product_key = np.random.randint(1, num_products + 1)
            geo_key = np.random.choice([1, 2, 3, 4, 5], p=[0.3, 0.25, 0.2, 0.15, 0.1])

            quantity = np.random.randint(1, 20)
            unit_price = np.random.uniform(10, 500)
            discount_percent = np.random.choice([0, 5, 10, 15, 20], p=[0.5, 0.2, 0.15, 0.1, 0.05])

            gross_amount = quantity * unit_price
            discount_amount = gross_amount * discount_percent / 100
            net_amount = gross_amount - discount_amount
            cost_amount = net_amount * np.random.uniform(0.4, 0.7)
            margin_amount = net_amount - cost_amount

            transactions.append({
                'date_key': int(date.strftime('%Y%m%d')),
                'customer_key': customer_key,
                'product_key': product_key,
                'geo_key': geo_key,
                'order_id': f"ORD-{date.strftime('%Y%m%d')}-{len(transactions):05d}",
                'order_line_number': 1,
                'quantity': quantity,
                'unit_price': round(unit_price, 2),
                'discount_percent': discount_percent,
                'discount_amount': round(discount_amount, 2),
                'gross_amount': round(gross_amount, 2),
                'net_amount': round(net_amount, 2),
                'cost_amount': round(cost_amount, 2),
                'margin_amount': round(margin_amount, 2)
            })

    return pd.DataFrame(transactions)
```

### 6.4 Quick Start Commands

```bash
# Start the sample environment
cd v4/docker
docker-compose up -d

# Wait for PostgreSQL to be ready
docker-compose logs -f postgres

# Verify data loaded
docker exec -it databridge-analytics-db psql -U analytics_user -d analytics -c "
SELECT
    'dim_date' as table_name, COUNT(*) as row_count FROM dim_date
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL
SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL
SELECT 'fact_sales', COUNT(*) FROM fact_sales
UNION ALL
SELECT 'fact_gl_transactions', COUNT(*) FROM fact_gl_transactions;
"

# Connect v4 to sample database
export DATABASE_URL="postgresql://analytics_user:analytics_pass@localhost:5434/analytics"
databridge-analytics catalog sync

# Run a test query
databridge-analytics query "SELECT SUM(net_amount) FROM fact_sales WHERE date_key >= 20240101"
```

### 6.5 Sample Data Statistics

| Table | Row Count | Time Range | Notes |
|-------|-----------|------------|-------|
| `dim_date` | 1,461 | 2022-2025 | 4 years of dates |
| `dim_customer` | 500 | - | 5 segments, 10 industries |
| `dim_product` | 200 | - | 10 categories, 50 subcategories |
| `dim_geography` | 50 | - | 5 regions, 10 countries |
| `dim_account` | 150 | - | Full P&L + Balance Sheet |
| `dim_cost_center` | 30 | - | 5 divisions, 6 departments each |
| `fact_sales` | ~110,000 | 2022-2024 | ~100 transactions/day |
| `fact_gl_transactions` | ~50,000 | 2022-2024 | Monthly journals |
| `fact_budget` | ~5,400 | 2022-2024 | Monthly by account/CC |

---

## 7. Implementation Plan

### Phase 1: Foundation (Weeks 1-2)
- Project setup and configuration
- PostgreSQL connector (for Docker sample)
- Basic CLI framework
- Metadata catalog extraction

### Phase 2: Connectors (Weeks 3-4)
- Snowflake connector with statistics
- SQL Server connector
- Databricks connector
- Connection management CLI

### Phase 3: Schema Analysis (Weeks 5-6)
- Star/snowflake schema detection
- Fact table identification
- Dimension analysis
- Relationship discovery

### Phase 4: Query Engine (Weeks 7-8)
- SQL query builder
- Dialect adapters
- Pushdown execution
- Result handling

### Phase 5: Knowledgebase (Weeks 9-10)
- Business glossary
- Metric definitions
- V3 hierarchy integration
- Vector embeddings

### Phase 6: NL-to-SQL (Weeks 11-12)
- Intent classification
- Entity extraction
- SQL generation
- Context management

### Phase 7: Insights (Weeks 13-14)
- Anomaly detection
- Period comparison
- Summary generation
- Recommendation engine

### Phase 8: Skills & MCP (Weeks 15-16)
- 7 skill prompts
- MCP tool registration
- Claude integration
- Testing and documentation

---

## 8. MCP Tools

### 8.1 Catalog Tools (15 tools)

```python
# Connection management
list_connections()                    # List configured data warehouses
add_connection(config)               # Add new connection
test_connection(connection_id)       # Test connectivity
remove_connection(connection_id)     # Remove connection

# Metadata extraction
sync_catalog(connection_id)          # Extract full catalog
get_databases(connection_id)         # List databases
get_schemas(connection_id, database) # List schemas
get_tables(connection_id, database, schema)  # List tables
get_columns(connection_id, table)    # Get column metadata
get_table_statistics(connection_id, table)   # Row counts, size
get_column_statistics(connection_id, table, column)  # Min, max, distinct

# Relationship discovery
detect_relationships(connection_id, schema)  # Find FKs/joins
get_join_paths(connection_id, from_table, to_table)  # Possible joins
validate_relationship(connection_id, relationship)   # Test a join
```

### 8.2 Analysis Tools (12 tools)

```python
# Schema analysis
detect_fact_tables(connection_id, schema)        # Find fact tables
detect_dimension_tables(connection_id, schema)   # Find dimensions
analyze_star_schema(connection_id, schema)       # Full schema analysis
detect_measures(connection_id, table)            # Find numeric measures
detect_hierarchies(connection_id, table)         # Find level columns

# Data quality
profile_table(connection_id, table)              # Comprehensive profile
score_data_quality(connection_id, table)         # Quality score
find_nulls(connection_id, table)                 # Null analysis
find_duplicates(connection_id, table, columns)   # Duplicate detection
validate_referential_integrity(connection_id, schema)  # FK validation

# V3 integration
map_to_v3_hierarchy(connection_id, table, v3_project)  # Link to V3
sync_dimension_from_v3(connection_id, v3_hierarchy)     # Import hierarchy
```

### 8.3 Query Tools (10 tools)

```python
# Query execution
execute_query(connection_id, sql)                # Run SQL (pushdown)
explain_query(connection_id, sql)                # Get execution plan
estimate_query_cost(connection_id, sql)          # Estimate resources

# Query building
build_aggregation_query(connection_id, fact_table, measures, dimensions)
build_comparison_query(connection_id, table, periods)
build_trend_query(connection_id, table, measure, time_column)

# Natural language
nl_to_sql(connection_id, question)               # Translate NL to SQL
execute_nl_query(connection_id, question)        # Translate and run
explain_nl_translation(question, sql)            # Explain the translation
suggest_questions(connection_id, table)          # Suggest questions
```

### 8.4 Insight Tools (8 tools)

```python
# Pattern detection
detect_anomalies(connection_id, table, measure, dimensions)
detect_trends(connection_id, table, measure, time_column)
detect_seasonality(connection_id, table, measure, time_column)

# Comparison
compare_periods(connection_id, table, measure, current, prior)
compare_segments(connection_id, table, measure, segment_column)

# Summarization
generate_summary(connection_id, table, time_period)
generate_kpi_report(connection_id, metrics, time_period)
generate_variance_analysis(connection_id, fact_table, budget_table)
```

### 8.5 Knowledgebase Tools (7 tools)

```python
# Glossary management
add_business_term(term, definition, tables, columns)
get_business_term(term)
search_glossary(query)

# Metric management
add_metric_definition(metric_id, name, formula, sql_template)
get_metric_definition(metric_id)
calculate_metric(connection_id, metric_id, dimensions)

# Context
get_table_context(connection_id, table)          # Full semantic context
```

---

## 9. Python Libraries

### 9.1 Core Requirements

```
# requirements.txt

# CLI Framework
typer>=0.12.0
rich>=13.7.0
prompt-toolkit>=3.0.0

# Data Processing
pandas>=2.2.0
numpy>=1.26.0
pyarrow>=15.0.0              # Efficient data transfer

# Database Connectivity
sqlalchemy>=2.0.0
alembic>=1.13.0

# Configuration
pydantic>=2.6.0
pydantic-settings>=2.2.0
python-dotenv>=1.0.0

# MCP Server
fastmcp>=0.4.0

# Vector Embeddings
chromadb>=0.4.22
sentence-transformers>=2.5.0

# Security
cryptography>=42.0.0

# HTTP Client
httpx>=0.27.0

# Utilities
python-slugify>=8.0.0
arrow>=1.3.0
tabulate>=0.9.0
tqdm>=4.66.0
```

### 9.2 Connector Requirements

```
# requirements-connectors.txt

# Snowflake
snowflake-connector-python>=3.6.0
snowflake-sqlalchemy>=1.5.0

# Databricks
databricks-sql-connector>=3.0.0

# SQL Server
pyodbc>=5.1.0
pymssql>=2.2.0

# PostgreSQL (Docker sample)
psycopg2-binary>=2.9.9
asyncpg>=0.29.0

# MySQL
pymysql>=1.1.0
```

### 9.3 Optional Requirements

```
# requirements-optional.txt

# Natural Language Processing
spacy>=3.7.0
transformers>=4.38.0

# Statistical Analysis
scipy>=1.12.0
statsmodels>=0.14.0

# Development
pytest>=8.0.0
pytest-cov>=4.1.0
pytest-asyncio>=0.23.0
black>=24.1.0
ruff>=0.2.0
mypy>=1.8.0
```

---

## 10. CLI Commands

```bash
# ================================================
# CONNECTION MANAGEMENT
# ================================================

# Add a connection
databridge-analytics connect add snowflake \
    --name prod-snowflake \
    --account xy12345.us-east-1 \
    --warehouse COMPUTE_WH \
    --database ANALYTICS

databridge-analytics connect add databricks \
    --name prod-databricks \
    --host adb-xxx.azuredatabricks.net \
    --http-path /sql/1.0/warehouses/xxx \
    --catalog unity_catalog

databridge-analytics connect add sqlserver \
    --name prod-sqlserver \
    --server sqlserver.company.com \
    --database DW

databridge-analytics connect add postgresql \
    --name local-sample \
    --host localhost \
    --port 5434 \
    --database analytics

# List connections
databridge-analytics connect list

# Test connection
databridge-analytics connect test prod-snowflake

# ================================================
# CATALOG MANAGEMENT
# ================================================

# Sync catalog from connection
databridge-analytics catalog sync prod-snowflake

# List databases
databridge-analytics catalog databases prod-snowflake

# List schemas
databridge-analytics catalog schemas prod-snowflake --database ANALYTICS

# List tables
databridge-analytics catalog tables prod-snowflake --database ANALYTICS --schema REPORTING

# Show table details
databridge-analytics catalog describe prod-snowflake ANALYTICS.REPORTING.FACT_SALES

# Detect relationships
databridge-analytics catalog relationships prod-snowflake --schema REPORTING

# ================================================
# SCHEMA ANALYSIS
# ================================================

# Analyze star schema
databridge-analytics analyze schema prod-snowflake --schema REPORTING

# Detect fact tables
databridge-analytics analyze facts prod-snowflake --schema REPORTING

# Detect dimensions
databridge-analytics analyze dimensions prod-snowflake --schema REPORTING

# Profile a table
databridge-analytics analyze profile prod-snowflake ANALYTICS.REPORTING.FACT_SALES

# Data quality score
databridge-analytics analyze quality prod-snowflake ANALYTICS.REPORTING.FACT_SALES

# ================================================
# QUERY EXECUTION
# ================================================

# Execute SQL query
databridge-analytics query run prod-snowflake "SELECT SUM(net_amount) FROM fact_sales"

# Natural language query
databridge-analytics query ask prod-snowflake "What were total sales by region last quarter?"

# Explain query plan
databridge-analytics query explain prod-snowflake "SELECT ..."

# Build aggregation query
databridge-analytics query build-agg prod-snowflake \
    --fact fact_sales \
    --measures "SUM(net_amount),COUNT(*)" \
    --dimensions "region,product_category" \
    --filters "date_key >= 20240101"

# ================================================
# INSIGHTS
# ================================================

# Detect anomalies
databridge-analytics insights anomalies prod-snowflake \
    --table fact_sales \
    --measure net_amount \
    --time-column date_key \
    --dimensions region,product_category

# Compare periods
databridge-analytics insights compare prod-snowflake \
    --table fact_sales \
    --measure net_amount \
    --current "2024-Q4" \
    --prior "2023-Q4" \
    --dimensions region

# Generate summary
databridge-analytics insights summary prod-snowflake \
    --table fact_sales \
    --period "2024-Q4"

# ================================================
# KNOWLEDGEBASE
# ================================================

# Add business term
databridge-analytics kb term add "Revenue" \
    --definition "Total income from sales" \
    --tables fact_sales \
    --columns net_amount

# Search glossary
databridge-analytics kb term search "sales"

# Add metric
databridge-analytics kb metric add gross_margin \
    --name "Gross Margin" \
    --formula "(Revenue - COGS) / Revenue * 100"

# Calculate metric
databridge-analytics kb metric calc prod-snowflake gross_margin \
    --dimensions region,product_category

# ================================================
# V3 INTEGRATION
# ================================================

# Link table to V3 hierarchy
databridge-analytics v3 link prod-snowflake FACT_GL_TRANSACTIONS \
    --v3-project my-hierarchy-project \
    --v3-hierarchy pl-revenue

# Sync dimension from V3
databridge-analytics v3 sync-dimension prod-snowflake \
    --v3-hierarchy account-hierarchy \
    --target-table dim_account

# ================================================
# INTERACTIVE & MCP
# ================================================

# Start interactive shell
databridge-analytics shell

# Start MCP server
databridge-analytics mcp serve
```

---

## Summary

### V4 Capabilities

| Capability | Description |
|------------|-------------|
| **Metadata Catalog** | Extract and cache schema, column stats, relationships |
| **Schema Analysis** | Detect star/snowflake patterns, fact/dimension tables |
| **Query Pushdown** | Execute SQL on data warehouse, minimize data transfer |
| **NL-to-SQL** | Translate natural language to optimized queries |
| **Insight Generation** | Anomalies, trends, comparisons, summaries |
| **Knowledgebase** | Business glossary, metrics, V3 integration |
| **Multi-Warehouse** | Snowflake, Databricks, SQL Server, PostgreSQL |

### Tool Count

| Category | Tools |
|----------|-------|
| Catalog | 15 |
| Analysis | 12 |
| Query | 10 |
| Insights | 8 |
| Knowledgebase | 7 |
| **Total** | **52** |

### Skills

| Skill | Focus |
|-------|-------|
| Data Analyst | SQL queries, data exploration |
| BI Developer | Schema design, optimization |
| Data Scientist | Statistical analysis, patterns |
| Business User | Plain English, no SQL |
| Data Steward | Data quality, governance |
| SQL Expert | Complex queries, performance |
| Executive Consumer | High-level insights, KPIs |

---

**Ready for approval to proceed with implementation.**
