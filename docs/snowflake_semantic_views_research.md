# Snowflake Semantic Views: Research & DataBridge AI Integration Strategy

## Table of Contents

1. [What Are Semantic Views?](#1-what-are-semantic-views)
2. [Architecture & Core Concepts](#2-architecture--core-concepts)
3. [Building Semantic Views](#3-building-semantic-views)
4. [Profiling Semantic Views](#4-profiling-semantic-views)
5. [Maintaining Semantic Views](#5-maintaining-semantic-views)
6. [Querying Semantic Views](#6-querying-semantic-views)
7. [Dynamic Querying](#7-dynamic-querying)
8. [BI Tool Compatibility: The Abstraction Layer Problem](#8-bi-tool-compatibility-the-abstraction-layer-problem)
9. [Snowpark & Python Stored Procedures: The BI Bridge](#9-snowpark--python-stored-procedures-the-bi-bridge)
10. [Multi-Agent Orchestration for Semantic Views](#10-multi-agent-orchestration-for-semantic-views)
11. [DataBridge AI Integration Opportunities](#11-databridge-ai-integration-opportunities)
12. [Competitive Landscape](#12-competitive-landscape)
13. [Sources](#13-sources)
14. [Faux Objects Implementation](#14-faux-objects-implementation)

---

## 1. What Are Semantic Views?

Snowflake Semantic Views are **schema-level database objects** that store business concepts directly in the database. They create a business-focused abstraction layer that bridges the gap between how data is physically stored (e.g., column `amt_ttl_pre_dsc`) and how business users describe it (e.g., "Gross Revenue").

Unlike traditional views that simply wrap SQL queries, semantic views explicitly define:
- **Business entities** (customers, orders, products) as logical tables
- **Business metrics** (Total Revenue, Profit Margin %) as aggregated calculations
- **Dimensions** (region, product category, fiscal year) for slicing and filtering
- **Relationships** between entities via foreign keys and joins

### Timeline

| Date | Milestone |
|------|-----------|
| April 2025 | Preview release |
| August 2025 | General Availability |
| October 2025 | Snowsight UI for creating/managing semantic views (GA) |
| December 2025 | Optimize with verified queries (Preview) |
| February 3, 2026 | **Semantic View Autopilot (GA)** - AI-powered automated creation and maintenance |

### Why They Matter

- **For AI**: Cortex Analyst reads semantic view definitions and generates SQL against physical tables directly, combining LLM reasoning with rule-based definitions for better accuracy
- **For BI**: Business users get consistent metrics and dimensions across all tools - no more "which Revenue number is correct?"
- **For Data Teams**: Centralized location for business logic reduces duplication across queries, dashboards, and reports
- **For Governance**: Single source of truth for metric definitions, shareable via Snowflake Marketplace and data listings

---

## 2. Architecture & Core Concepts

### The Four Building Blocks

```
SEMANTIC VIEW
├── LOGICAL TABLES     (business entities mapped to physical tables)
│   └── PRIMARY KEY / UNIQUE constraints
├── RELATIONSHIPS      (foreign key joins between logical tables)
│   └── ASOF joins supported for time-series data
├── FACTS              (row-level numerical attributes - the raw building blocks)
│   └── Can be PRIVATE (hidden from queries, used only in calculations)
├── DIMENSIONS         (categorical attributes for grouping/filtering)
│   └── Always PUBLIC (cannot be made private)
└── METRICS            (aggregated KPIs: SUM, AVG, COUNT over facts)
    └── Can be PRIVATE, can use window functions
```

### How Facts, Dimensions, and Metrics Relate

| Concept | What It Answers | Granularity | Example |
|---------|----------------|-------------|---------|
| **Fact** | "How much/many at the row level?" | Individual row | `l_extendedprice * (1 - l_discount)` |
| **Dimension** | "Who/What/Where/When?" | Categorical | `customer_market_segment`, `order_date` |
| **Metric** | "What is the KPI?" | Aggregated across rows | `SUM(revenue_amount)`, `AVG(order_value)` |

### Key Constraints

- Semantic views are **metadata objects** - they don't store data, only definitions
- Maximum ~32,000 tokens (~128,000 characters) for the underlying YAML configuration
- Recommended: **no more than 50-100 columns** across all tables for optimal Cortex Analyst performance
- **Cannot be replicated** - requires separate synchronization for multi-region deployments
- **Cannot be incrementally altered** - modifications require `CREATE OR REPLACE`

---

## 3. Building Semantic Views

### Method 1: SQL DDL (Recommended for Automation)

```sql
CREATE OR REPLACE SEMANTIC VIEW finance.reporting.revenue_analysis
  COMMENT = 'Revenue analysis across customers and regions'
  AI_SQL_GENERATION = 'Revenue is recognized at point of sale. Fiscal year ends September 30.'
  AI_QUESTION_CATEGORIZATION = 'This model covers revenue, costs, and profitability metrics.'

  TABLES (
    customer AS FINANCE.RAW.DIM_CUSTOMER
      PRIMARY KEY (customer_id)
      WITH SYNONYMS = ('client', 'account')
      COMMENT = 'Customer master data',

    orders AS FINANCE.RAW.FACT_ORDERS
      PRIMARY KEY (order_id)
      COMMENT = 'Sales order transactions',

    product AS FINANCE.RAW.DIM_PRODUCT
      PRIMARY KEY (product_id)
      WITH SYNONYMS = ('item', 'sku')
  )

  RELATIONSHIPS (
    orders (customer_id) REFERENCES customer,
    orders (product_id) REFERENCES product
  )

  FACTS (
    orders.order_amount AS o_total_price
      WITH SYNONYMS = ('sale amount', 'order value')
      COMMENT = 'Total price of the order before discounts',

    orders.discount_amount AS o_total_price * o_discount_pct
      COMMENT = 'Dollar amount of discount applied',

    PRIVATE orders.raw_cost AS o_unit_cost * o_quantity
      COMMENT = 'Internal cost - not exposed to queries'
  )

  DIMENSIONS (
    customer.customer_name AS c_name
      WITH SYNONYMS = ('client name', 'account name'),

    customer.region AS c_region
      WITH SYNONYMS = ('territory', 'area'),

    customer.segment AS c_market_segment
      WITH SYNONYMS = ('market', 'industry'),

    orders.order_date AS o_orderdate
      COMMENT = 'Date the order was placed',

    product.category AS p_category
      WITH SYNONYMS = ('product type', 'product line')
  )

  METRICS (
    orders.total_revenue AS SUM(orders.order_amount)
      WITH SYNONYMS = ('revenue', 'sales', 'top line')
      COMMENT = 'Gross revenue before discounts',

    orders.net_revenue AS SUM(orders.order_amount) - SUM(orders.discount_amount)
      COMMENT = 'Revenue after discounts',

    orders.average_order_value AS AVG(orders.order_amount)
      WITH SYNONYMS = ('AOV')
      COMMENT = 'Average dollar value per order',

    orders.order_count AS COUNT(orders.order_amount)
      COMMENT = 'Total number of orders',

    orders.gross_margin AS
      (SUM(orders.order_amount) - SUM(orders.raw_cost)) / NULLIF(SUM(orders.order_amount), 0) * 100
      COMMENT = 'Gross margin percentage'
  );
```

### Method 2: From YAML (Migration from Cortex Analyst Semantic Models)

```sql
CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
  $$
  name: revenue_analysis
  tables:
    - name: customer
      base_table:
        database: FINANCE
        schema: RAW
        table: DIM_CUSTOMER
      primary_key: [customer_id]
      dimensions:
        - name: customer_name
          expr: c_name
          synonyms: [client name, account name]
  ...
  $$
);
```

### Method 3: Snowsight UI

The Snowsight Object Explorer provides a guided wizard:
1. Navigate to **Data > Databases > Schema > Semantic Views**
2. Click **+ Create** and follow the wizard to select tables, define relationships, and add metrics/dimensions
3. Best for initial setup and exploration; not ideal for version-controlled deployments

### Method 4: dbt Package

```yaml
# dbt model file: models/semantic/revenue_analysis.sql
{{ config(materialized='semantic_view') }}
-- Uses the dbt_semantic_view package
```

Install: `dbt_semantic_view` package, then manage semantic views as dbt models with full CI/CD, version control, and peer review.

---

## 4. Profiling Semantic Views

### Introspection Commands

**List all semantic views:**
```sql
SHOW SEMANTIC VIEWS IN DATABASE finance;
SHOW SEMANTIC VIEWS IN SCHEMA finance.reporting;
```

**Describe structure (tables, relationships, dimensions, facts, metrics):**
```sql
DESCRIBE SEMANTIC VIEW revenue_analysis;
```

Returns a result set with five columns:

| Column | Purpose |
|--------|---------|
| `object_kind` | TABLE, RELATIONSHIP, DIMENSION, FACT, METRIC, DERIVED_METRIC, CUSTOM_INSTRUCTIONS |
| `object_name` | Name of the specific element |
| `parent_entity` | Parent logical table (NULL for tables and view-level properties) |
| `property` | Attribute being described (EXPRESSION, DATA_TYPE, etc.) |
| `property_value` | The actual value |

**List dimensions, facts, metrics separately:**
```sql
SHOW SEMANTIC DIMENSIONS IN revenue_analysis;
SHOW SEMANTIC FACTS IN revenue_analysis;
SHOW SEMANTIC METRICS IN revenue_analysis;
```

**Find compatible dimensions for a specific metric:**
```sql
SHOW SEMANTIC DIMENSIONS IN revenue_analysis FOR METRIC total_revenue;
```

The output includes a `required` column indicating which dimensions **must** be included (important for window function metrics).

**Export DDL:**
```sql
SELECT GET_DDL('SEMANTIC_VIEW', 'revenue_analysis', TRUE);
```

**Export as YAML:**
```sql
SELECT SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW('finance.reporting.revenue_analysis');
```

### Profiling Strategy for DataBridge AI

A comprehensive profile of a semantic view would capture:

```
Semantic View Profile
├── Metadata: name, database, schema, comment, created_date
├── Tables: count, base table references, primary keys
├── Relationships: count, join types, ASOF relationships
├── Dimensions: count, data types, synonyms, per-table distribution
├── Facts: count (public vs private), expressions, data types
├── Metrics: count (public vs private), aggregation types, window functions
├── Metric-Dimension Compatibility Matrix
├── Size Assessment: total columns, estimated token count
└── Validation: orphaned tables, unused facts, missing relationships
```

---

## 5. Maintaining Semantic Views

### The Immutability Challenge

Semantic views **cannot be incrementally altered** (except comments). All structural changes require `CREATE OR REPLACE`:

```sql
-- Adding a new metric requires replacing the entire view
CREATE OR REPLACE SEMANTIC VIEW revenue_analysis
  COPY GRANTS  -- Preserve existing access privileges
  TABLES (...)
  RELATIONSHIPS (...)
  FACTS (...)
  DIMENSIONS (...)
  METRICS (
    -- existing metrics...
    orders.new_metric AS SUM(orders.some_fact)  -- new addition
  );
```

### Maintenance Operations

| Operation | Command | Notes |
|-----------|---------|-------|
| Add/remove dimension | `CREATE OR REPLACE` | Must rebuild entire view |
| Add/remove metric | `CREATE OR REPLACE` | Must rebuild entire view |
| Change table mapping | `CREATE OR REPLACE` | Must rebuild entire view |
| Update comment | `ALTER SEMANTIC VIEW ... SET COMMENT` | Only alterable property |
| Drop view | `DROP SEMANTIC VIEW name` | Permanent |
| Clone across environments | Schema-level clone | Replication not supported |

### Version Control Workflow

```
1. Export YAML: SYSTEM$READ_YAML_FROM_SEMANTIC_VIEW()
2. Commit to Git repository
3. Peer review changes
4. Apply via SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML() or CREATE OR REPLACE
5. Validate with DESCRIBE and SHOW commands
```

### Schema Drift Detection

When underlying physical tables change (columns renamed, dropped, types changed), semantic views that reference those columns will break. Proactive monitoring is needed:

```sql
-- Check if base tables still exist and columns are valid
DESCRIBE SEMANTIC VIEW revenue_analysis;
-- Errors here indicate drift
```

### Semantic View Autopilot (GA Feb 2026)

Snowflake's AI-powered service that **automates** maintenance:
- Learns from real user activity and query patterns
- Automatically builds, optimizes, and maintains semantic views
- Imports business definitions from existing BI tools (Looker, Sigma, ThoughtSpot)
- Reduces semantic model creation from **days to minutes**
- Part of the Open Semantic Interchange (OSI) initiative for cross-platform interoperability

---

## 6. Querying Semantic Views

### Method 1: SEMANTIC_VIEW() Clause (GA)

```sql
-- Basic: one dimension, one metric
SELECT * FROM SEMANTIC_VIEW(
  revenue_analysis
  DIMENSIONS customer.region
  METRICS orders.total_revenue
)
ORDER BY region;

-- Multiple dimensions and metrics
SELECT * FROM SEMANTIC_VIEW(
  revenue_analysis
  DIMENSIONS customer.region, customer.segment
  METRICS orders.total_revenue, orders.order_count, orders.average_order_value
)
ORDER BY region, segment;

-- With aliases
SELECT * FROM SEMANTIC_VIEW(
  revenue_analysis
  DIMENSIONS customer.region AS territory
  METRICS orders.total_revenue AS revenue
)
ORDER BY territory;

-- Wildcard: all dimensions and metrics from a logical table
SELECT * FROM SEMANTIC_VIEW(
  revenue_analysis
  DIMENSIONS customer.*
  METRICS orders.*
);

-- WHERE filter (applied before metric computation)
SELECT * FROM SEMANTIC_VIEW(
  revenue_analysis
  DIMENSIONS customer.region
  METRICS orders.total_revenue
  WHERE customer.segment = 'AUTOMOBILE'
)
ORDER BY region;

-- Facts only (no metrics in same query)
SELECT * FROM SEMANTIC_VIEW(
  revenue_analysis
  DIMENSIONS customer.customer_name
  FACTS orders.order_amount
)
ORDER BY customer_name;
```

### Method 2: Direct Reference (Preview)

```sql
-- Metrics must be wrapped in AGG(), MIN(), MAX(), or ANY_VALUE()
SELECT
  region,
  segment,
  AGG(total_revenue) AS revenue,
  AGG(order_count) AS orders,
  AGG(average_order_value) AS aov
FROM revenue_analysis
WHERE region = 'AMERICA'
GROUP BY region, segment
HAVING AGG(total_revenue) > 1000000
ORDER BY revenue DESC
LIMIT 10;
```

### Query Rules

| Rule | Details |
|------|---------|
| Must include at least one | DIMENSION, METRIC, or FACT |
| Cannot mix FACTS and METRICS | In the same SEMANTIC_VIEW() clause |
| Dimensions must relate to metrics | Via defined RELATIONSHIPS |
| Dimension granularity | Must be equal or lower than metric granularity |
| WHERE filters | Applied **before** metrics are computed |
| HAVING filters | Only metrics allowed, wrapped in AGG() |
| Not supported | JOIN, PIVOT, UNPIVOT, window functions, subqueries, QUALIFY, LATERAL |

### Privileges

Users need only `SELECT` on the semantic view - **not** on the underlying tables. However, Cortex Analyst requires `SELECT` on both the semantic view and underlying tables.

---

## 7. Dynamic Querying

### The Challenge

Semantic views are inherently dynamic in that the **metric computation** happens at query time based on which dimensions are selected. However, there is no mechanism for:
- Parameterized semantic view definitions
- Runtime metric creation
- Ad-hoc dimension injection

### Dynamic Querying via Stored Procedures

```sql
CREATE OR REPLACE PROCEDURE query_semantic_dynamic(
  sv_name VARCHAR,
  dimensions ARRAY,
  metrics ARRAY,
  filter_clause VARCHAR DEFAULT NULL
)
RETURNS TABLE()
LANGUAGE SQL
AS
$$
DECLARE
  dim_list VARCHAR;
  met_list VARCHAR;
  query VARCHAR;
BEGIN
  dim_list := ARRAY_TO_STRING(dimensions, ', ');
  met_list := ARRAY_TO_STRING(metrics, ', ');

  query := 'SELECT * FROM SEMANTIC_VIEW(' || sv_name ||
           ' DIMENSIONS ' || dim_list ||
           ' METRICS ' || met_list;

  IF (filter_clause IS NOT NULL) THEN
    query := query || ' WHERE ' || filter_clause;
  END IF;

  query := query || ')';

  RETURN TABLE(RESULT_SCAN(LAST_QUERY_ID()));
END;
$$;

-- Usage
CALL query_semantic_dynamic(
  'revenue_analysis',
  ARRAY_CONSTRUCT('customer.region', 'customer.segment'),
  ARRAY_CONSTRUCT('orders.total_revenue', 'orders.order_count'),
  'customer.region = ''AMERICA'''
);
```

### Dynamic Querying via Cortex Analyst

The most powerful dynamic querying is through natural language via Cortex Analyst:

```
User: "What was our revenue by region last quarter?"
Cortex Analyst: Reads semantic view definition -> Generates SQL -> Returns results
```

This is the primary mechanism Snowflake envisions for dynamic, ad-hoc semantic queries.

---

## 8. BI Tool Compatibility: The Abstraction Layer Problem

### The Core Problem

Most BI tools (Power BI, Tableau, Excel, SSRS) generate **standard SQL**:
```sql
SELECT column1, column2 FROM schema.table WHERE ...
```

But semantic views require **proprietary syntax**:
```sql
SELECT * FROM SEMANTIC_VIEW(name DIMENSIONS ... METRICS ...)
-- or
SELECT AGG(metric) FROM semantic_view GROUP BY dimension
```

**BI tools that cannot issue these queries will not see semantic views as queryable objects.**

### Current BI Tool Integration Status

| BI Tool | Native Support | Status |
|---------|---------------|--------|
| Sigma | Yes | GA - direct semantic view querying |
| Looker | Yes | Via Semantic View Autopilot integration |
| ThoughtSpot | Yes | Via Semantic View Autopilot integration |
| dbt | Yes | Via `dbt_semantic_view` package |
| Cortex Analyst | Yes | Primary consumption channel |
| **Power BI** | **No** | No native semantic view support |
| **Tableau** | **No** | No native semantic view support |
| **Excel/ODBC** | **No** | No native semantic view support |
| **SSRS** | **No** | No native semantic view support |

### Solution 1: Regular View Wrapper (Static)

Create regular views that wrap fixed SEMANTIC_VIEW() queries:

```sql
-- Revenue by Region (fixed dimensions/metrics)
CREATE OR REPLACE VIEW reporting.vw_revenue_by_region AS
SELECT * FROM SEMANTIC_VIEW(
  revenue_analysis
  DIMENSIONS customer.region, customer.segment
  METRICS orders.total_revenue, orders.net_revenue, orders.order_count
);

-- Revenue by Product (different slice)
CREATE OR REPLACE VIEW reporting.vw_revenue_by_product AS
SELECT * FROM SEMANTIC_VIEW(
  revenue_analysis
  DIMENSIONS product.category, orders.order_date
  METRICS orders.total_revenue, orders.average_order_value
);
```

**Pros**: Any BI tool can query these like regular views
**Cons**: Static - must pre-define every dimension/metric combination

### Solution 2: Dynamic Table Materialization (Automated Refresh)

```sql
-- Auto-refreshing materialized wrapper
CREATE OR REPLACE DYNAMIC TABLE reporting.dt_revenue_summary
  TARGET_LAG = '1 hour'
  WAREHOUSE = analytics_wh
AS
SELECT * FROM SEMANTIC_VIEW(
  revenue_analysis
  DIMENSIONS customer.region, customer.segment, orders.order_date
  METRICS orders.total_revenue, orders.net_revenue, orders.order_count, orders.gross_margin
);
```

**Pros**: Pre-computed for fast BI queries, auto-refreshes on a schedule
**Cons**: Storage cost, still static dimension/metric combinations

### Solution 3: Stored Procedure + Table Materialization

```sql
CREATE OR REPLACE PROCEDURE materialize_semantic_view(
  semantic_view_name VARCHAR,
  target_table VARCHAR,
  dimensions ARRAY,
  metrics ARRAY
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  dim_str VARCHAR;
  met_str VARCHAR;
  sql_stmt VARCHAR;
BEGIN
  dim_str := ARRAY_TO_STRING(dimensions, ', ');
  met_str := ARRAY_TO_STRING(metrics, ', ');

  sql_stmt := 'CREATE OR REPLACE TABLE ' || target_table || ' AS ' ||
              'SELECT * FROM SEMANTIC_VIEW(' || semantic_view_name ||
              ' DIMENSIONS ' || dim_str ||
              ' METRICS ' || met_str || ')';

  EXECUTE IMMEDIATE sql_stmt;
  RETURN 'Materialized ' || semantic_view_name || ' into ' || target_table;
END;
$$;

-- Schedule with a Snowflake Task
CREATE OR REPLACE TASK refresh_revenue_tables
  WAREHOUSE = analytics_wh
  SCHEDULE = 'USING CRON 0 */4 * * * America/Chicago'  -- Every 4 hours
AS
CALL materialize_semantic_view(
  'revenue_analysis',
  'reporting.tbl_revenue_by_region',
  ARRAY_CONSTRUCT('customer.region', 'customer.segment'),
  ARRAY_CONSTRUCT('orders.total_revenue', 'orders.order_count')
);
```

**BI tools see `reporting.tbl_revenue_by_region` as a regular table.**

### Solution 4: Stored Procedure as a "Virtual Table" Interface

For BI tools that can call stored procedures (Power BI via DirectQuery, Tableau via Initial SQL):

```sql
CREATE OR REPLACE PROCEDURE get_revenue_report(
  group_by VARCHAR DEFAULT 'region',        -- region, segment, product, date
  date_from DATE DEFAULT NULL,
  date_to DATE DEFAULT NULL
)
RETURNS TABLE(
  dimension_value VARCHAR,
  total_revenue FLOAT,
  net_revenue FLOAT,
  order_count INT,
  avg_order_value FLOAT
)
LANGUAGE SQL
AS
$$
DECLARE
  dim_col VARCHAR;
  where_clause VARCHAR := '';
  query VARCHAR;
BEGIN
  CASE group_by
    WHEN 'region' THEN dim_col := 'customer.region';
    WHEN 'segment' THEN dim_col := 'customer.segment';
    WHEN 'product' THEN dim_col := 'product.category';
    WHEN 'date' THEN dim_col := 'orders.order_date';
    ELSE dim_col := 'customer.region';
  END CASE;

  IF (date_from IS NOT NULL AND date_to IS NOT NULL) THEN
    where_clause := ' WHERE orders.order_date BETWEEN ''' ||
                    date_from || ''' AND ''' || date_to || '''';
  END IF;

  query := 'SELECT * FROM SEMANTIC_VIEW(revenue_analysis' ||
           ' DIMENSIONS ' || dim_col ||
           ' METRICS orders.total_revenue, orders.net_revenue, ' ||
           'orders.order_count, orders.average_order_value' ||
           where_clause || ')';

  RETURN TABLE(RESULT_SCAN(LAST_QUERY_ID()));
END;
$$;
```

### Solution 5: Third-Party Semantic Layer (Honeydew, AtScale)

- **Honeydew**: Wraps Snowflake semantic views and makes them immediately available to any BI tool for dynamic live queries
- **AtScale**: Available as a Snowflake Native App; gives Power BI and Tableau a governed, consistent, high-performance way to access Snowflake data

### Recommended Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    SNOWFLAKE SEMANTIC VIEW                         │
│         (Single source of truth for business logic)                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────────────────┐   │
│  │ Cortex       │  │ Native BI    │  │ Legacy BI Tools        │   │
│  │ Analyst      │  │ (Sigma,      │  │ (Power BI, Tableau,    │   │
│  │ (NL queries) │  │  Looker,     │  │  Excel, SSRS)          │   │
│  │              │  │  ThoughtSpot)│  │                        │   │
│  │ Direct       │  │ Direct       │  │ Via Wrapper Layer:     │   │
│  │ SEMANTIC_    │  │ SEMANTIC_    │  │ ┌────────────────────┐ │   │
│  │ VIEW()       │  │ VIEW()       │  │ │ Regular Views      │ │   │
│  │ queries      │  │ queries      │  │ │ Dynamic Tables     │ │   │
│  │              │  │              │  │ │ Stored Procedures  │ │   │
│  │              │  │              │  │ │ Materialized Tables│ │   │
│  └──────────────┘  └──────────────┘  │ └────────────────────┘ │   │
│                                       └────────────────────────┘   │
│                                                                     │
│  ┌──────────────────────────────────────────────────────────────┐  │
│  │              DataBridge AI Orchestration Layer               │  │
│  │  - Build semantic views from hierarchy definitions          │  │
│  │  - Generate wrapper views/tables/procedures automatically   │  │
│  │  - Monitor schema drift and validate definitions            │  │
│  │  - Sync between semantic views and hierarchy projects       │  │
│  └──────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 9. Snowpark & Python Stored Procedures: The BI Bridge

This is the most practical solution for making semantic views consumable by BI tools (Power BI, Tableau, Excel, SSRS) that cannot issue `SEMANTIC_VIEW()` queries natively.

### The Strategy

```
┌───────────────────────────────────────────────────────────────────┐
│                    SEMANTIC VIEW (Source of Truth)                │
│  Defines: TABLES, RELATIONSHIPS, DIMENSIONS, FACTS, METRICS     │
└──────────────────────────┬────────────────────────────────────────┘
                           │
                           ▼
┌───────────────────────────────────────────────────────────────────┐
│              SNOWPARK PYTHON STORED PROCEDURES                   │
│                                                                   │
│  1. Read semantic view metadata (DESCRIBE/SHOW commands)         │
│  2. Build SEMANTIC_VIEW() queries dynamically                    │
│  3. Execute queries and return results as DataFrames             │
│  4. Optionally materialize into tables for scheduled refresh     │
│                                                                   │
│  Key: RETURNS TABLE(col1 TYPE, col2 TYPE, ...) with fixed schema │
│  Enables: SELECT * FROM TABLE(procedure_name(args))              │
└──────────────────────────┬────────────────────────────────────────┘
                           │
                           ▼
┌───────────────────────────────────────────────────────────────────┐
│                    BI TOOLS                                       │
│                                                                   │
│  Power BI:  Value.NativeQuery("SELECT * FROM TABLE(proc(...))")  │
│  Tableau:   Custom SQL → SELECT * FROM TABLE(proc(...))          │
│  Excel:     ODBC → SELECT * FROM TABLE(proc(...))                │
│  Any JDBC:  Standard SELECT statement                            │
│                                                                   │
│  BI tools see this as a REGULAR TABLE RESULT - no special syntax │
└───────────────────────────────────────────────────────────────────┘
```

### 9.1 Python Stored Procedure: Fixed-Schema Semantic Query

This is the simplest pattern. Pre-define the output columns so BI tools get a predictable schema.

```sql
CREATE OR REPLACE PROCEDURE semantic_revenue_by_region(
  filter_segment VARCHAR DEFAULT NULL
)
RETURNS TABLE(
  region VARCHAR,
  segment VARCHAR,
  total_revenue FLOAT,
  net_revenue FLOAT,
  order_count INT,
  avg_order_value FLOAT
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
def run(session, filter_segment=None):
    # Build the SEMANTIC_VIEW query
    dims = "customer.region, customer.segment"
    metrics = ("orders.total_revenue, orders.net_revenue, "
               "orders.order_count, orders.average_order_value")

    where = ""
    if filter_segment:
        where = f" WHERE customer.segment = '{filter_segment}'"

    query = f"""
        SELECT * FROM SEMANTIC_VIEW(
            revenue_analysis
            DIMENSIONS {dims}
            METRICS {metrics}
            {where}
        )
    """
    return session.sql(query)
$$;
```

**BI tool consumption:**
```sql
-- Power BI / Tableau / Any JDBC tool:
SELECT * FROM TABLE(semantic_revenue_by_region('AUTOMOBILE'));

-- No filter:
SELECT * FROM TABLE(semantic_revenue_by_region());
```

### 9.2 Python Stored Procedure: Parameterized Dimension Selection

Allow the BI tool to choose which dimension to group by:

```sql
CREATE OR REPLACE PROCEDURE semantic_revenue_pivot(
  group_by_dimension VARCHAR DEFAULT 'region',
  date_from DATE DEFAULT NULL,
  date_to DATE DEFAULT NULL
)
RETURNS TABLE(
  dimension_value VARCHAR,
  total_revenue FLOAT,
  net_revenue FLOAT,
  order_count INT,
  gross_margin FLOAT
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
def run(session, group_by_dimension='region', date_from=None, date_to=None):
    # Map friendly names to semantic view dimension paths
    dim_map = {
        'region':   'customer.region',
        'segment':  'customer.segment',
        'product':  'product.category',
        'date':     'orders.order_date',
    }

    dim = dim_map.get(group_by_dimension, 'customer.region')

    metrics = ("orders.total_revenue, orders.net_revenue, "
               "orders.order_count, orders.gross_margin")

    where = ""
    if date_from and date_to:
        where = (f" WHERE orders.order_date BETWEEN "
                 f"'{date_from}' AND '{date_to}'")

    query = f"""
        SELECT * FROM SEMANTIC_VIEW(
            revenue_analysis
            DIMENSIONS {dim}
            METRICS {metrics}
            {where}
        )
    """
    return session.sql(query)
$$;
```

**BI tool consumption:**
```sql
-- Revenue by product category for Q4 2025
SELECT * FROM TABLE(semantic_revenue_pivot(
  'product', '2025-10-01', '2025-12-31'
));

-- Revenue by region, no date filter
SELECT * FROM TABLE(semantic_revenue_pivot('region'));
```

### 9.3 Python Stored Procedure: Multi-Metric Report Builder

A more advanced pattern that generates a complete report from multiple semantic views:

```sql
CREATE OR REPLACE PROCEDURE semantic_executive_dashboard(
  fiscal_year INT DEFAULT 2025
)
RETURNS TABLE(
  metric_name VARCHAR,
  metric_category VARCHAR,
  current_value FLOAT,
  dimension_group VARCHAR
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
def run(session, fiscal_year=2025):
    import pandas as pd

    results = []

    # Query 1: Revenue metrics by region
    rev_df = session.sql(f"""
        SELECT * FROM SEMANTIC_VIEW(
            revenue_analysis
            DIMENSIONS customer.region
            METRICS orders.total_revenue, orders.net_revenue
            WHERE orders.order_date >= '{fiscal_year}-01-01'
              AND orders.order_date < '{fiscal_year + 1}-01-01'
        )
    """).to_pandas()

    for _, row in rev_df.iterrows():
        results.append({
            'metric_name': 'Total Revenue',
            'metric_category': 'Revenue',
            'current_value': float(row['TOTAL_REVENUE']),
            'dimension_group': row['REGION']
        })
        results.append({
            'metric_name': 'Net Revenue',
            'metric_category': 'Revenue',
            'current_value': float(row['NET_REVENUE']),
            'dimension_group': row['REGION']
        })

    # Query 2: Cost metrics (from a different semantic view)
    cost_df = session.sql(f"""
        SELECT * FROM SEMANTIC_VIEW(
            cost_analysis
            DIMENSIONS department.department_name
            METRICS expenses.total_cost, expenses.budget_variance
            WHERE expenses.fiscal_year = {fiscal_year}
        )
    """).to_pandas()

    for _, row in cost_df.iterrows():
        results.append({
            'metric_name': 'Total Cost',
            'metric_category': 'Cost',
            'current_value': float(row['TOTAL_COST']),
            'dimension_group': row['DEPARTMENT_NAME']
        })

    # Return as Snowpark DataFrame
    return session.create_dataframe(pd.DataFrame(results))
$$;
```

### 9.4 Materializer Pattern: Stored Procedure + Snowflake Task

For BI tools that cannot call stored procedures directly, or where performance requires pre-computed results:

```sql
-- Step 1: Python procedure that materializes semantic query results into a table
CREATE OR REPLACE PROCEDURE materialize_semantic_report(
  semantic_view_name VARCHAR,
  target_table VARCHAR,
  dimensions VARCHAR,
  metrics VARCHAR,
  where_clause VARCHAR DEFAULT NULL
)
RETURNS TABLE(status VARCHAR, row_count INT, refreshed_at TIMESTAMP_NTZ)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
from datetime import datetime

def run(session, semantic_view_name, target_table, dimensions, metrics,
        where_clause=None):
    # Build SEMANTIC_VIEW query
    query = f"""
        SELECT * FROM SEMANTIC_VIEW(
            {semantic_view_name}
            DIMENSIONS {dimensions}
            METRICS {metrics}
            {f'WHERE {where_clause}' if where_clause else ''}
        )
    """

    # Execute and materialize
    df = session.sql(query)
    row_count = df.count()

    # Write to target table (CREATE OR REPLACE)
    df.write.mode("overwrite").save_as_table(target_table)

    # Return status
    import pandas as pd
    return session.create_dataframe(pd.DataFrame([{
        'status': 'SUCCESS',
        'row_count': row_count,
        'refreshed_at': datetime.utcnow()
    }]))
$$;

-- Step 2: Create Snowflake Tasks for scheduled refresh

-- Revenue by region (refresh every 4 hours)
CREATE OR REPLACE TASK refresh_revenue_by_region
  WAREHOUSE = analytics_wh
  SCHEDULE = 'USING CRON 0 */4 * * * America/Chicago'
AS
CALL materialize_semantic_report(
  'revenue_analysis',
  'REPORTING.BI_REVENUE_BY_REGION',
  'customer.region, customer.segment',
  'orders.total_revenue, orders.net_revenue, orders.order_count',
  NULL
);

-- Revenue by product (refresh daily at 6 AM)
CREATE OR REPLACE TASK refresh_revenue_by_product
  WAREHOUSE = analytics_wh
  SCHEDULE = 'USING CRON 0 6 * * * America/Chicago'
AS
CALL materialize_semantic_report(
  'revenue_analysis',
  'REPORTING.BI_REVENUE_BY_PRODUCT',
  'product.category, orders.order_date',
  'orders.total_revenue, orders.average_order_value',
  NULL
);

-- Resume the tasks (they start in suspended state)
ALTER TASK refresh_revenue_by_region RESUME;
ALTER TASK refresh_revenue_by_product RESUME;
```

**Result**: `REPORTING.BI_REVENUE_BY_REGION` and `REPORTING.BI_REVENUE_BY_PRODUCT` are regular tables that **any BI tool** can query with zero special syntax.

### 9.5 UDTF Pattern: Semantic View as a Table Function

User-Defined Table Functions (UDTFs) provide another approach. Unlike stored procedures, UDTFs can be used directly in JOINs and subqueries:

```sql
CREATE OR REPLACE FUNCTION semantic_query_fn(
  dimension_name VARCHAR,
  metric_names VARCHAR,
  filter_expr VARCHAR
)
RETURNS TABLE(
  dimension_value VARCHAR,
  metric_1 FLOAT,
  metric_2 FLOAT
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'SemanticQueryHandler'
AS
$$
class SemanticQueryHandler:
    def process(self, dimension_name, metric_names, filter_expr):
        # Note: UDTFs have limited access to session in some contexts
        # This pattern works best with Snowpark Container Services
        # for full SQL execution capability
        pass
$$;
```

> **Note**: UDTFs have limitations around SQL execution within handlers. For full dynamic SQL capability, stored procedures are the better choice. UDTFs work best when the data transformation logic is self-contained (no need to run additional queries).

### 9.6 BI Tool Connection Patterns

#### Power BI

```
Method 1: Value.NativeQuery() in Advanced Editor (Recommended)
─────────────────────────────────────────────────────────────
let
    Source = Snowflake.Databases("account.snowflakecomputing.com", "ANALYTICS_WH"),
    DB = Source{[Name="FINANCE"]}[Data],
    Schema = DB{[Name="REPORTING"]}[Data],
    Result = Value.NativeQuery(
        Schema,
        "SELECT * FROM TABLE(semantic_revenue_by_region(#(lf)'AUTOMOBILE'#(lf)))",
        null,
        [EnableFolding=true]
    )
in
    Result

Method 2: Materialized Table (Simplest)
───────────────────────────────────────
Connect directly to REPORTING.BI_REVENUE_BY_REGION
(refreshed by Snowflake Task every 4 hours)
Power BI sees it as a regular table - no special config needed.

Method 3: DirectQuery on Materialized Table
──────────────────────────────────────────
Same as Method 2 but with DirectQuery mode enabled.
Near-real-time if Task refresh frequency is high enough.
```

#### Tableau

```
Method 1: Custom SQL
────────────────────
New Data Source → Snowflake → Custom SQL:
  SELECT * FROM TABLE(semantic_revenue_by_region('AUTOMOBILE'))

Method 2: Initial SQL (for session setup)
─────────────────────────────────────────
Connection → Initial SQL:
  CALL materialize_semantic_report(
    'revenue_analysis',
    'REPORTING.TEMP_TABLEAU_REPORT',
    'customer.region, customer.segment',
    'orders.total_revenue, orders.net_revenue',
    NULL
  );
Then connect to REPORTING.TEMP_TABLEAU_REPORT as a regular table.

Method 3: Materialized Table
────────────────────────────
Connect directly to pre-materialized table.
No stored procedure calls needed.
```

#### Excel / ODBC

```
Method 1: ODBC Query
────────────────────
Data → Get External Data → ODBC:
  SELECT * FROM TABLE(semantic_revenue_pivot('region'))

Method 2: Materialized Table
────────────────────────────
Direct table connection via ODBC to pre-materialized table.
```

### 9.7 Architecture Decision Matrix

| Pattern | Dynamic? | BI Compatibility | Performance | Complexity | Best For |
|---------|----------|-----------------|-------------|------------|----------|
| **Fixed-schema stored proc** | Parameterized | High (SELECT FROM TABLE) | Real-time | Medium | Power BI DirectQuery, Tableau Custom SQL |
| **Materialized table + Task** | No (scheduled) | Universal | Fast (pre-computed) | Low | Excel, SSRS, any ODBC tool |
| **Dynamic Table wrapper** | No (auto-refresh) | Universal | Fast (auto-maintained) | Low | Dashboards needing auto-refresh |
| **Regular View wrapper** | No (static) | Universal | Real-time | Low | Simple, fixed reports |
| **Python UDTF** | Limited | Medium | Real-time | High | Advanced joins, subqueries |
| **Cortex Analyst** | Full NL | Requires API | Real-time | Low | AI-powered ad-hoc queries |

### 9.8 Complete End-to-End Example: P&L Semantic View with BI Wrappers

```sql
-- =====================================================
-- STEP 1: Create the Semantic View (source of truth)
-- =====================================================
CREATE OR REPLACE SEMANTIC VIEW finance.semantic.pl_analysis
  COMMENT = 'Profit & Loss analysis for all business units'
  AI_SQL_GENERATION = 'Revenue is recognized at point of sale. COGS includes materials and labor.'

  TABLES (
    gl_entries AS FINANCE.GL.FACT_JOURNAL_ENTRIES
      PRIMARY KEY (journal_entry_id),
    accounts AS FINANCE.GL.DIM_ACCOUNT
      PRIMARY KEY (account_code),
    cost_centers AS FINANCE.GL.DIM_COST_CENTER
      PRIMARY KEY (cost_center_id),
    periods AS FINANCE.GL.DIM_PERIOD
      PRIMARY KEY (period_id)
  )

  RELATIONSHIPS (
    gl_entries (account_code) REFERENCES accounts,
    gl_entries (cost_center_id) REFERENCES cost_centers,
    gl_entries (period_id) REFERENCES periods
  )

  FACTS (
    gl_entries.debit_amount AS debit_amount,
    gl_entries.credit_amount AS credit_amount,
    gl_entries.net_amount AS debit_amount - credit_amount
  )

  DIMENSIONS (
    accounts.account_name AS account_name
      WITH SYNONYMS = ('GL account', 'ledger account'),
    accounts.account_category AS account_category
      WITH SYNONYMS = ('account type'),
    accounts.hierarchy_level_1 AS pl_section
      WITH SYNONYMS = ('P&L section', 'income statement section'),
    accounts.hierarchy_level_2 AS pl_subsection,
    cost_centers.cost_center_name AS cost_center,
    cost_centers.business_unit AS business_unit,
    periods.fiscal_year AS fiscal_year,
    periods.fiscal_quarter AS fiscal_quarter,
    periods.fiscal_month AS fiscal_month
  )

  METRICS (
    gl_entries.total_revenue AS SUM(
      CASE WHEN accounts.account_category = 'Revenue'
           THEN gl_entries.net_amount ELSE 0 END
    ) COMMENT = 'Total revenue across all accounts',

    gl_entries.total_cogs AS SUM(
      CASE WHEN accounts.account_category = 'COGS'
           THEN gl_entries.net_amount ELSE 0 END
    ) COMMENT = 'Cost of goods sold',

    gl_entries.gross_profit AS
      SUM(CASE WHEN accounts.account_category = 'Revenue'
               THEN gl_entries.net_amount ELSE 0 END) -
      SUM(CASE WHEN accounts.account_category = 'COGS'
               THEN gl_entries.net_amount ELSE 0 END)
    COMMENT = 'Revenue minus COGS',

    gl_entries.total_opex AS SUM(
      CASE WHEN accounts.account_category = 'Operating Expense'
           THEN gl_entries.net_amount ELSE 0 END
    ) COMMENT = 'Total operating expenses',

    gl_entries.net_income AS SUM(gl_entries.net_amount)
      COMMENT = 'Bottom line net income'
  );

-- =====================================================
-- STEP 2: Create Python stored procedures for BI tools
-- =====================================================

-- Procedure A: P&L by business unit
CREATE OR REPLACE PROCEDURE get_pl_by_business_unit(
  fiscal_year INT DEFAULT 2025,
  business_unit_filter VARCHAR DEFAULT NULL
)
RETURNS TABLE(
  business_unit VARCHAR,
  total_revenue FLOAT,
  total_cogs FLOAT,
  gross_profit FLOAT,
  total_opex FLOAT,
  net_income FLOAT
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
def run(session, fiscal_year=2025, business_unit_filter=None):
    where = f"WHERE periods.fiscal_year = {fiscal_year}"
    if business_unit_filter:
        where += f" AND cost_centers.business_unit = '{business_unit_filter}'"

    return session.sql(f"""
        SELECT * FROM SEMANTIC_VIEW(
            finance.semantic.pl_analysis
            DIMENSIONS cost_centers.business_unit
            METRICS gl_entries.total_revenue,
                    gl_entries.total_cogs,
                    gl_entries.gross_profit,
                    gl_entries.total_opex,
                    gl_entries.net_income
            {where}
        )
    """)
$$;

-- Procedure B: P&L trend by quarter
CREATE OR REPLACE PROCEDURE get_pl_quarterly_trend(
  fiscal_year INT DEFAULT 2025
)
RETURNS TABLE(
  fiscal_quarter VARCHAR,
  total_revenue FLOAT,
  gross_profit FLOAT,
  net_income FLOAT
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
def run(session, fiscal_year=2025):
    return session.sql(f"""
        SELECT * FROM SEMANTIC_VIEW(
            finance.semantic.pl_analysis
            DIMENSIONS periods.fiscal_quarter
            METRICS gl_entries.total_revenue,
                    gl_entries.gross_profit,
                    gl_entries.net_income
            WHERE periods.fiscal_year = {fiscal_year}
        )
    """)
$$;

-- =====================================================
-- STEP 3: Materialize for universal BI access
-- =====================================================

-- Materialized P&L summary (refreshed every 2 hours)
CREATE OR REPLACE DYNAMIC TABLE reporting.dt_pl_summary
  TARGET_LAG = '2 hours'
  WAREHOUSE = analytics_wh
AS
SELECT * FROM SEMANTIC_VIEW(
  finance.semantic.pl_analysis
  DIMENSIONS cost_centers.business_unit,
             periods.fiscal_year,
             periods.fiscal_quarter,
             accounts.pl_section
  METRICS gl_entries.total_revenue,
          gl_entries.total_cogs,
          gl_entries.gross_profit,
          gl_entries.total_opex,
          gl_entries.net_income
);

-- =====================================================
-- STEP 4: BI tools connect to their preferred interface
-- =====================================================

-- Power BI DirectQuery: SELECT * FROM TABLE(get_pl_by_business_unit(2025))
-- Tableau Custom SQL:   SELECT * FROM TABLE(get_pl_quarterly_trend(2025))
-- Excel ODBC:           SELECT * FROM reporting.dt_pl_summary
-- SSRS:                 SELECT * FROM reporting.dt_pl_summary
-- Sigma/Looker:         Direct SEMANTIC_VIEW() queries (native support)
```

### 9.9 Key Constraints for SELECT FROM TABLE(procedure())

For a stored procedure to be callable via `SELECT * FROM TABLE(proc())`:

| Requirement | Details |
|-------------|---------|
| **Fixed output schema** | Must use `RETURNS TABLE(col1 TYPE, col2 TYPE, ...)` with explicit column names and types |
| **No DDL/DML side effects** | Procedure can only execute SELECT, SHOW, DESCRIBE, or CALL |
| **No dynamic schema** | `RETURNS TABLE()` without columns is **not** eligible |
| **No bind variables** | Cannot use `?` placeholders in arguments |
| **No correlated args** | Cannot reference outer query scope |
| **No use in views/UDFs** | Cannot embed `TABLE(proc())` inside a CREATE VIEW |

**Workaround for DDL procedures** (like the materializer): Use `CALL` directly from a Snowflake Task, not from BI tools. BI tools connect to the materialized output table instead.

---

## 10. Multi-Agent Orchestration for Semantic Views

### Proposed Agent Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                  SEMANTIC VIEW ORCHESTRATOR                      │
│              (DataBridge AI PlannerAgent)                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐ │
│  │  Schema Scanner  │  │ Semantic Builder │  │ Drift Detector  │ │
│  │  Agent           │  │ Agent            │  │ Agent           │ │
│  │                   │  │                   │  │                 │ │
│  │ - Scan database   │  │ - Generate DDL    │  │ - Compare DDL   │ │
│  │   schemas         │  │ - Map hierarchies │  │   vs physical   │ │
│  │ - Profile tables  │  │   to semantic     │  │   tables        │ │
│  │ - Detect keys     │  │   concepts        │  │ - Alert on      │ │
│  │ - Identify joins  │  │ - Apply templates │  │   column drops  │ │
│  │ - Catalog columns │  │ - Create metrics  │  │ - Suggest fixes │ │
│  └────────┬──────────┘  └────────┬──────────┘  └────────┬───────┘ │
│           │                      │                       │         │
│  ┌────────┴──────────┐  ┌────────┴──────────┐  ┌────────┴───────┐ │
│  │ Wrapper Generator │  │ Validation Agent  │  │ BI Sync Agent  │ │
│  │ Agent             │  │                   │  │                 │ │
│  │                   │  │ - Verify metrics  │  │ - Generate      │ │
│  │ - Create regular  │  │   match expected  │  │   wrapper views │ │
│  │   views           │  │   values          │  │ - Schedule      │ │
│  │ - Create dynamic  │  │ - Cross-check     │  │   materialization│
│  │   tables          │  │   across semantic │  │ - Monitor BI    │ │
│  │ - Create stored   │  │   views           │  │   tool access   │ │
│  │   procedures      │  │ - Profile data    │  │ - Refresh       │ │
│  │ - Schedule tasks  │  │   quality         │  │   on demand     │ │
│  └───────────────────┘  └───────────────────┘  └─────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

### Agent Workflows

#### Workflow 1: Build Semantic View from Hierarchy

```
User Request: "Create a semantic view for our P&L hierarchy"

1. Schema Scanner Agent
   ├── Scan FINANCE.GL.FACT_JOURNAL_ENTRIES
   ├── Scan FINANCE.GL.DIM_ACCOUNT
   ├── Profile columns, detect keys, identify joins
   └── Output: schema_profile.json

2. Semantic Builder Agent
   ├── Read DataBridge hierarchy project (P&L hierarchy)
   ├── Map hierarchy nodes to DIMENSIONS
   ├── Map source mappings to FACTS
   ├── Map formula groups to METRICS
   ├── Apply financial template (standard_pl)
   └── Output: CREATE SEMANTIC VIEW DDL

3. Validation Agent
   ├── Execute DESCRIBE SEMANTIC VIEW
   ├── Run sample queries for each metric
   ├── Compare metric results to known values
   └── Output: validation_report.json

4. Wrapper Generator Agent
   ├── Analyze BI tool requirements (Power BI? Tableau?)
   ├── Generate regular views for common slices
   ├── Generate stored procedures for dynamic access
   ├── Create Snowflake Tasks for scheduled refresh
   └── Output: wrapper_objects.sql
```

#### Workflow 2: Monitor and Maintain

```
Scheduled (daily/hourly):

1. Drift Detector Agent
   ├── DESCRIBE SEMANTIC VIEW → extract column references
   ├── DESCRIBE TABLE → extract actual columns
   ├── Compare: missing columns? type changes? new columns?
   ├── If drift detected:
   │   ├── Alert via orchestrator event
   │   ├── Suggest DDL fix
   │   └── Optionally auto-fix with CREATE OR REPLACE
   └── Output: drift_report.json

2. Validation Agent
   ├── Run pre-defined metric validation queries
   ├── Compare results to previous run (detect anomalies)
   ├── Check for NULL metric values, zero counts, etc.
   └── Output: validation_delta_report.json

3. BI Sync Agent
   ├── Check wrapper views/tables are still valid
   ├── Refresh materialized tables if stale
   ├── Update stored procedure signatures if metrics changed
   └── Output: sync_status.json
```

#### Workflow 3: Dynamic Report Generation

```
User Request: "Show me revenue by product for Q4, break it down by region"

1. PlannerAgent
   ├── Parse natural language request
   ├── Identify semantic view: revenue_analysis
   ├── Identify dimensions: product.category, customer.region
   ├── Identify metrics: orders.total_revenue
   ├── Identify filter: orders.order_date in Q4 range
   └── Output: query_plan.json

2. Query Execution
   ├── Build SEMANTIC_VIEW() query
   ├── Execute against Snowflake
   ├── Format results
   └── Return to user

3. (Optional) Materialization
   ├── If user says "save this as a report"
   ├── Create wrapper view or dynamic table
   ├── Register in DataBridge workflow
   └── Schedule refresh
```

---

## 11. DataBridge AI Integration Opportunities

### Existing Capabilities That Map Directly

| DataBridge AI Capability | Semantic View Application |
|--------------------------|---------------------------|
| **Hierarchy Builder** (44 tools) | Generate DIMENSIONS and TABLES from hierarchy projects |
| **Formula Groups** | Map directly to METRICS (SUM, AVG, COUNT expressions) |
| **Source Mappings** | Map to FACTS (row-level expressions from source columns) |
| **Templates** (20 templates) | Pre-built semantic view patterns for P&L, Balance Sheet, etc. |
| **Skills** (7 skills) | Guide metric/dimension naming, industry-specific synonyms |
| **Schema Matching** (`compare_database_schemas`) | Detect drift between semantic view and physical tables |
| **Data Comparison** (`compare_table_data`) | Validate metric values against known benchmarks |
| **Diff Utilities** (6 tools) | Character-level comparison of DDL versions |
| **SQL Discovery** (`sql_to_hierarchy`) | Extract semantic concepts from existing SQL CASE statements |
| **Smart Recommendations** | Suggest dimensions, metrics, and templates based on data profile |
| **PlannerAgent** (11 tools) | Orchestrate multi-step semantic view workflows |
| **AI Orchestrator** (16 tools) | Multi-agent coordination for build/validate/deploy |
| **Deployment Scripts** (`generate_hierarchy_scripts`) | Generate CREATE SEMANTIC VIEW DDL |
| **Push to Snowflake** (`push_hierarchy_to_snowflake`) | Deploy semantic views to Snowflake |
| **Workflow Engine** | Track build/validate/deploy pipeline steps |
| **Audit Trail** | Log all semantic view changes for governance |
| **dbt Integration** | Align with `dbt_semantic_view` package |
| **Export CSV/JSON** | Export semantic view definitions for documentation |

### New Tools to Build

#### Tool Category 1: Semantic View Builder

```python
# New MCP tools for semantic view lifecycle
@mcp.tool()
def generate_semantic_view_ddl(project_id: str, options: dict) -> str:
    """
    Generate CREATE SEMANTIC VIEW DDL from a DataBridge hierarchy project.

    Maps:
    - Hierarchy levels -> DIMENSIONS
    - Source mappings -> FACTS
    - Formula groups -> METRICS
    - Source table/column references -> TABLES and RELATIONSHIPS
    """

@mcp.tool()
def create_semantic_view_from_template(template_id: str, connection_id: str, mappings: dict) -> str:
    """
    Create a semantic view using a DataBridge financial template.
    Auto-maps template structure to physical tables via connection.
    """

@mcp.tool()
def profile_semantic_view(view_name: str, connection_id: str) -> dict:
    """
    Profile an existing semantic view: tables, relationships,
    dimensions, facts, metrics, compatibility matrix.
    """
```

#### Tool Category 2: Wrapper Generator

```python
@mcp.tool()
def generate_bi_wrappers(
    semantic_view_name: str,
    bi_tool: str,  # 'power_bi', 'tableau', 'excel', 'generic'
    dimension_combinations: list,
    refresh_schedule: str = None
) -> dict:
    """
    Generate wrapper objects (views, dynamic tables, stored procedures)
    that make a semantic view consumable by BI tools that don't
    natively support SEMANTIC_VIEW() syntax.

    Returns: DDL for all wrapper objects + Snowflake Task for refresh
    """

@mcp.tool()
def materialize_semantic_query(
    semantic_view_name: str,
    dimensions: list,
    metrics: list,
    target_table: str,
    connection_id: str
) -> dict:
    """
    Execute a SEMANTIC_VIEW() query and materialize results into
    a regular table that any BI tool can query.
    """
```

#### Tool Category 3: Drift Detection & Validation

```python
@mcp.tool()
def detect_semantic_drift(
    semantic_view_name: str,
    connection_id: str
) -> dict:
    """
    Compare semantic view definition against current physical table schemas.
    Detect: dropped columns, type changes, new columns not in view,
    broken relationships, stale metrics.
    """

@mcp.tool()
def validate_semantic_metrics(
    semantic_view_name: str,
    connection_id: str,
    benchmark_queries: list = None
) -> dict:
    """
    Run validation queries against semantic view metrics.
    Compare results to benchmarks or previous runs.
    Flag anomalies (NULL metrics, zero counts, drastic changes).
    """
```

#### Tool Category 4: Sync & Lifecycle

```python
@mcp.tool()
def sync_hierarchy_to_semantic_view(
    project_id: str,
    semantic_view_name: str,
    connection_id: str,
    mode: str = 'preview'  # 'preview' or 'apply'
) -> dict:
    """
    Synchronize a DataBridge hierarchy project to a Snowflake semantic view.
    Detects changes in hierarchy and generates CREATE OR REPLACE DDL.
    """

@mcp.tool()
def export_semantic_view_yaml(
    semantic_view_name: str,
    connection_id: str
) -> str:
    """
    Export semantic view definition as YAML for version control.
    """

@mcp.tool()
def import_semantic_view_to_hierarchy(
    semantic_view_name: str,
    connection_id: str,
    project_name: str
) -> dict:
    """
    Reverse-engineer: create a DataBridge hierarchy project from
    an existing Snowflake semantic view.
    """
```

### Hierarchy-to-Semantic-View Mapping

```
DataBridge Hierarchy Project          Snowflake Semantic View
─────────────────────────────         ─────────────────────────
Project                        →      SEMANTIC VIEW
├── Hierarchies                →      DIMENSIONS (categorical grouping)
│   ├── Level 1: Revenue       →        customer.revenue_category
│   ├── Level 2: Product Rev   →        orders.product_revenue_type
│   └── Level 3: Sub-category  →        lineitem.sub_category
├── Source Mappings             →      FACTS + TABLES + RELATIONSHIPS
│   ├── Database: FINANCE      →        TABLES (base table references)
│   ├── Schema: GL             →        TABLES (schema references)
│   ├── Table: JOURNAL         →        TABLES (table references)
│   ├── Column: ACCOUNT_CODE   →        FACTS (column expressions)
│   └── Join keys              →        RELATIONSHIPS (foreign keys)
├── Formula Groups              →      METRICS
│   ├── SUM(revenue)           →        SUM(orders.revenue_amount)
│   ├── SUBTRACT(net)          →        SUM(revenue) - SUM(discounts)
│   └── DIVIDE(margin_pct)     →        (revenue - cost) / revenue * 100
├── Properties                  →      SYNONYMS + COMMENTS
│   ├── display_name           →        WITH SYNONYMS = (...)
│   └── description            →        COMMENT = '...'
└── Templates                   →      AI_SQL_GENERATION + AI_QUESTION_CATEGORIZATION
    └── Industry-specific       →        Instructions for Cortex Analyst
        naming conventions
```

---

## 12. Competitive Landscape

### Semantic Layer Comparison (2025-2026)

| Feature | Snowflake Semantic Views | dbt MetricFlow | Databricks Metric Views |
|---------|-------------------------|----------------|------------------------|
| **Status** | GA (Aug 2025) | GA | GA (late 2025) |
| **Architecture** | Native database object | Vendor-neutral middleware | Lakehouse-native |
| **Multi-warehouse** | Snowflake only | Any dbt-supported warehouse | Databricks only |
| **AI Integration** | Cortex Analyst (native) | Via partner tools | Unity AI (native) |
| **BI Tools** | Sigma, Looker, ThoughtSpot | Universal via JDBC/ODBC | Power BI, Tableau |
| **Autopilot/AI Build** | Yes (GA Feb 2026) | No | No |
| **Version Control** | YAML export + dbt package | Native dbt integration | Unity Catalog |
| **DataBridge AI fit** | Strong (existing Snowflake tools) | Moderate (dbt integration exists) | Low (no current tooling) |

### Open Semantic Interchange (OSI)

Industry initiative backed by Snowflake, dbt Labs, Salesforce, and ThoughtSpot to create a vendor-neutral standard for semantic layer definitions. Meaningful interoperability expected in 2026-2027.

---

## 13. Sources

### Official Snowflake Documentation
- [Overview of Semantic Views](https://docs.snowflake.com/en/user-guide/views-semantic/overview)
- [Querying Semantic Views](https://docs.snowflake.com/en/user-guide/views-semantic/querying)
- [CREATE SEMANTIC VIEW](https://docs.snowflake.com/en/sql-reference/sql/create-semantic-view)
- [ALTER SEMANTIC VIEW](https://docs.snowflake.com/en/sql-reference/sql/alter-semantic-view)
- [DESCRIBE SEMANTIC VIEW](https://docs.snowflake.com/en/sql-reference/sql/desc-semantic-view)
- [SEMANTIC_VIEW Clause](https://docs.snowflake.com/en/sql-reference/constructs/semantic_view)
- [SHOW SEMANTIC DIMENSIONS](https://docs.snowflake.com/en/sql-reference/sql/show-semantic-dimensions)
- [SHOW SEMANTIC METRICS](https://docs.snowflake.com/en/sql-reference/sql/show-semantic-metrics)
- [SQL Commands for Semantic Views](https://docs.snowflake.com/en/user-guide/views-semantic/sql)
- [Example: Creating a Semantic View](https://docs.snowflake.com/en/user-guide/views-semantic/example)
- [Best Practices for Semantic Views](https://docs.snowflake.com/en/user-guide/views-semantic/best-practices-dev)
- [SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML](https://docs.snowflake.com/en/sql-reference/stored-procedures/system_create_semantic_view_from_yaml)
- [Semantic Views Release Notes (April 2025)](https://docs.snowflake.com/en/release-notes/2025/other/2025-04-17-semantic-views)

### Snowpark & Stored Procedures
- [Returning Tabular Data from Python Stored Procedures](https://docs.snowflake.com/en/developer-guide/stored-procedure/python/procedure-python-tabular-data)
- [Selecting FROM a Stored Procedure](https://docs.snowflake.com/en/developer-guide/stored-procedure/stored-procedures-selecting-from)
- [SELECT FROM Stored Procedures (Snowflake Blog)](https://medium.com/snowflake/snowflake-supports-select-from-stored-procedures-728eea7fc22e)
- [Writing Stored Procedures with SQL and Python](https://docs.snowflake.com/en/sql-reference/stored-procedures-python)
- [Stored Procedures Overview](https://docs.snowflake.com/en/developer-guide/stored-procedure/stored-procedures-overview)
- [Power BI DirectQuery Best Practices with Snowflake](https://medium.com/snowflake/best-practices-for-using-power-bi-in-directquery-mode-with-snowflake-bfd1312ca7ab)
- [Power Query Snowflake Connector](https://learn.microsoft.com/en-us/power-query/connectors/snowflake)
- [Value.NativeQuery() for Power BI](https://blog.crossjoin.co.uk/2016/12/11/passing-parameters-to-sql-queries-with-value-nativequery-in-power-query-and-power-bi/)
- [Snowpark Container Services Overview](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/overview)
- [Creating Python UDFs in Snowpark](https://docs.snowflake.com/en/developer-guide/snowpark/python/creating-udfs)

### Announcements & Analysis
- [Snowflake Delivers Semantic View Autopilot (Feb 2026)](https://www.snowflake.com/en/news/press-releases/snowflake-delivers-semantic-view-autopilot-as-the-foundation-for-trusted-scalable-enterprise-ready-AI/)
- [Snowflake Native Semantic Views: AI-Powered BI](https://www.snowflake.com/en/engineering-blog/native-semantic-views-ai-bi/)
- [Manage Semantic Views with dbt Package](https://www.snowflake.com/en/engineering-blog/dbt-semantic-view-package/)
- [Semantic Layer 2025: MetricFlow vs Snowflake vs Databricks](https://www.typedef.ai/resources/semantic-layer-metricflow-vs-snowflake-vs-databricks)
- [Snowflake Semantic Views: Real-World Insights (phData)](https://www.phdata.io/blog/snowflake-semantic-views-real-world-insights-best-practices-and-phdatas-approach/)
- [Getting Started with Snowflake Semantic View (Medium)](https://medium.com/snowflake/getting-started-with-snowflake-semantic-view-7eced29abe6f)
- [Implementing a Centralized Semantic Layer (AtScale)](https://www.atscale.com/blog/centralized-semantic-layer-power-bi-tableau-snowflake/)
- [Snowflake at BUILD London 2026](https://siliconangle.com/2026/02/03/snowflake-bets-platform-native-ai-enterprises-rethink-custom-development/)

---

## 14. Faux Objects Implementation

**Faux Objects** is the DataBridge AI feature that generates standard Snowflake objects wrapping Semantic Views for BI tool consumption. The name reflects that these objects are "faux" — they look like regular views, procedures, or tables to BI tools, but they actually proxy through to the Semantic View layer underneath.

### 14.1 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    DataBridge AI MCP Server                  │
│                  src/faux_objects/ (13 tools)                │
├──────────────┬──────────────┬───────────────┬───────────────┤
│   types.py   │  service.py  │ mcp_tools.py  │  __init__.py  │
│  Data Models │ SQL Gen Core │ MCP Tool Reg  │   Package     │
└──────┬───────┴──────┬───────┴───────┬───────┴───────────────┘
       │              │               │
       ▼              ▼               ▼
┌─────────────────────────────────────────────────────────────┐
│                Generated Snowflake Objects                   │
├──────────────┬──────────────┬───────────────┬───────────────┤
│  Standard    │   Stored     │   Dynamic     │  Snowflake    │
│  VIEW        │  PROCEDURE   │   TABLE       │   TASK        │
│  (static)    │ (parameterized) │ (auto-refresh)│ (scheduled) │
└──────────────┴──────────────┴───────────────┴───────────────┘
       │              │               │               │
       └──────────────┴───────────────┴───────────────┘
                              │
                    BI Tools see these as
                    regular database objects
```

### 14.2 Faux Object Types

| Type | Snowflake Object | BI Compatibility | Use Case |
|------|-----------------|------------------|----------|
| **View** | `CREATE VIEW` | Universal | Static reports, simple dashboards |
| **Stored Procedure** | `CREATE PROCEDURE` with `RETURNS TABLE` | Power BI, Tableau, Custom SQL | Parameterized queries, interactive filters |
| **Dynamic Table** | `CREATE DYNAMIC TABLE` | Universal | Auto-refreshing dashboards |
| **Task** | `CREATE TASK` + materializer proc | Universal | Scheduled batch reports, Excel/ODBC |

### 14.3 MCP Tools (13 total)

**Project Management:**
- `create_faux_project` — Create a new Faux Objects project
- `list_faux_projects` — List all projects
- `get_faux_project` — Get full project details
- `delete_faux_project` — Delete a project

**Semantic View Definition:**
- `define_faux_semantic_view` — Define the semantic view to wrap
- `add_faux_semantic_table` — Add a table reference
- `add_faux_semantic_column` — Add a dimension/metric/fact column
- `add_faux_semantic_relationship` — Add a table relationship

**Faux Object Configuration:**
- `add_faux_object` — Configure a faux object (view/proc/dt/task)
- `remove_faux_object` — Remove a faux object

**SQL Generation:**
- `generate_faux_scripts` — Generate SQL for all objects
- `generate_faux_deployment_bundle` — Complete deployment script
- `generate_semantic_view_ddl` — Generate CREATE SEMANTIC VIEW DDL
- `export_faux_scripts` — Export to individual .sql files

### 14.4 Example Workflow

```
Step 1: Create project
  → create_faux_project("Sales Analysis Wrappers")

Step 2: Define semantic view
  → define_faux_semantic_view(id, "sales_analysis", "ANALYTICS", "SEMANTIC")
  → add_faux_semantic_table(id, "orders", "ANALYTICS.DW.FACT_ORDERS", "order_id")
  → add_faux_semantic_column(id, "region", "dimension", "VARCHAR", "customers")
  → add_faux_semantic_column(id, "total_revenue", "metric", "FLOAT", "orders",
      expression="SUM(order_amount)")

Step 3: Add faux objects
  → add_faux_object(id, "V_SALES", "view", "REPORTING", "PUBLIC")
  → add_faux_object(id, "GET_SALES", "stored_procedure", "REPORTING", "PUBLIC",
      parameters=[{"name":"REGION","data_type":"VARCHAR","default_value":"NULL"}])
  → add_faux_object(id, "DT_SALES", "dynamic_table", "REPORTING", "PUBLIC",
      warehouse="ANALYTICS_WH", target_lag="1 hour")

Step 4: Generate deployment SQL
  → generate_faux_deployment_bundle(id)
  → export_faux_scripts(id, "./deployment/")
```

### 14.5 Files

| File | Purpose |
|------|---------|
| `src/faux_objects/__init__.py` | Package initialization |
| `src/faux_objects/types.py` | Pydantic models (SemanticViewDefinition, FauxObjectConfig, etc.) |
| `src/faux_objects/service.py` | SQL generation engine for all four faux object types |
| `src/faux_objects/mcp_tools.py` | MCP tool registration (13 tools) |
| `tests/test_faux_objects.py` | 42 test cases covering all types and edge cases |
