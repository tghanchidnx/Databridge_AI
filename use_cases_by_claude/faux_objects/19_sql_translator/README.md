# Use Case 19: SQL Translator - Import Existing SQL into Faux Objects

## The Story

You're a **Data Engineer** at a company that has been building SQL views for years. Your Snowflake environment has dozens of `CREATE VIEW` statements, complex `SELECT` queries, and reporting dashboards built on top of them.

Now, your team wants to adopt **Snowflake Semantic Views** for AI-powered analytics with Cortex Analyst. But rewriting all those existing views from scratch? That's weeks of work and a recipe for introducing bugs.

Enter the **SQL Translator**. This tool reverse-engineers your existing SQL into structured `SemanticViewDefinition` objects, automatically classifying columns as dimensions, metrics, and facts. You can then convert between formats, create Faux Objects projects, and deploy to Snowflake.

---

## What You Will Learn

- How to **detect** what format your SQL is in (SELECT, CREATE VIEW, or CREATE SEMANTIC VIEW)
- How to **translate** any SQL format into a SemanticViewDefinition
- How column **classification** works (GROUP BY → dimension, aggregation → metric)
- How to **convert** between SQL formats (roundtrip conversion)
- How to **create a project** directly from existing SQL

---

## Tools Used

| Tool | What It Does |
|------|-------------|
| `detect_sql_format` | Identifies whether SQL is SELECT, CREATE VIEW, or CREATE SEMANTIC VIEW |
| `translate_sql_to_semantic_view` | Parses SQL into a structured semantic view definition with tables, dimensions, metrics, facts |
| `translate_sql_to_faux_project` | One-step: parses SQL and creates a complete FauxProject with optional faux objects |
| `convert_sql_format` | Converts SQL between formats (SELECT → DDL, DDL → VIEW, etc.) |

---

## Understanding Column Classification

The translator automatically classifies columns based on SQL structure:

| Context | Classification | Reason |
|---------|---------------|--------|
| Column in `GROUP BY` | **DIMENSION** | Grouping column = categorical attribute |
| `SUM(...)`, `COUNT(...)`, `AVG(...)`, etc. | **METRIC** | Aggregation = calculated measure |
| Raw column not in `GROUP BY` | **FACT** | Unaggregated measure |
| `CASE WHEN ... END` inside aggregation | **METRIC** | Still an aggregation |
| No `GROUP BY` at all | All columns → **FACT** | No grouping = all raw values |

---

## Step-by-Step Instructions

### Scenario A: Import a SELECT Query with GROUP BY/SUM

You have an existing reporting query that calculates regional sales:

```sql
SELECT
    region,
    product_category,
    SUM(order_amount) as total_sales,
    COUNT(*) as order_count,
    AVG(order_amount) as avg_order_value
FROM WAREHOUSE.SALES.FACT_ORDERS o
JOIN WAREHOUSE.SALES.DIM_PRODUCTS p ON o.product_id = p.product_id
GROUP BY region, product_category
```

**Step 1: Detect the format**

```
Detect the SQL format of my regional sales query
```

You'll see:
```json
{
  "format": "select_query",
  "description": "SELECT query (may contain aggregations)"
}
```

**Step 2: Translate to semantic view**

```
Translate this SELECT query to a semantic view:
- Name: regional_sales_analysis
- Database: ANALYTICS
- Schema: SEMANTIC
```

You'll see the automatically classified columns:
- **Dimensions**: `region`, `product_category` (they're in GROUP BY)
- **Metrics**: `total_sales`, `order_count`, `avg_order_value` (they're aggregations)
- **Tables**: `o` (FACT_ORDERS), `p` (DIM_PRODUCTS)
- **Relationships**: `o.product_id` → `p`

**Step 3: Create a project from the SQL**

```
Create a faux objects project from this SQL called "Regional Sales Wrappers"
and add a view faux object targeting REPORTING.PUBLIC
```

Now you have a complete project with the semantic view populated and a wrapper view ready to generate!

---

### Scenario B: Import an Existing CREATE VIEW

Your DBA wrote this view years ago:

```sql
CREATE OR REPLACE VIEW REPORTING.FINANCE.V_MONTHLY_REVENUE
    COMMENT = 'Monthly revenue by business unit'
AS
SELECT
    bu.business_unit_name,
    p.fiscal_year,
    p.fiscal_month,
    SUM(gl.credit_amount) - SUM(gl.debit_amount) as net_revenue
FROM FINANCE.GL.FACT_JOURNAL_ENTRIES gl
JOIN FINANCE.GL.DIM_BUSINESS_UNIT bu ON gl.bu_id = bu.bu_id
JOIN FINANCE.GL.DIM_PERIOD p ON gl.period_id = p.period_id
WHERE gl.account_code LIKE '4%'  -- Revenue accounts
GROUP BY bu.business_unit_name, p.fiscal_year, p.fiscal_month
```

**Step 1: Translate the CREATE VIEW**

```
Translate this CREATE VIEW statement into a semantic view definition
```

The translator will:
1. Extract the view name (`V_MONTHLY_REVENUE`)
2. Extract the comment ("Monthly revenue by business unit")
3. Parse the inner SELECT query
4. Classify columns:
   - **Dimensions**: `business_unit_name`, `fiscal_year`, `fiscal_month`
   - **Metrics**: `net_revenue` (with expression: `SUM(credit_amount) - SUM(debit_amount)`)
5. Create a FauxObjectConfig for the view

**Step 2: Convert to SEMANTIC VIEW DDL**

```
Convert this CREATE VIEW to CREATE SEMANTIC VIEW DDL format
with database FINANCE and schema SEMANTIC
```

You'll get a proper SEMANTIC VIEW DDL with:
- `TABLES` block listing all three source tables
- `RELATIONSHIPS` block showing how they join
- `DIMENSIONS` block with your grouping columns
- `METRICS` block with the calculated revenue expression

---

### Scenario C: Import a CREATE SEMANTIC VIEW DDL

Someone already created a SEMANTIC VIEW DDL file:

```sql
CREATE OR REPLACE SEMANTIC VIEW FINANCE.SEMANTIC.gl_reconciliation
    COMMENT = 'General Ledger reconciliation for trial balance'
    AI_SQL_GENERATION = 'Debits are positive, credits are negative.'

TABLES (
    gl_entries AS FINANCE.GL.FACT_JOURNAL_ENTRIES
        PRIMARY KEY (journal_entry_id),
    accounts AS FINANCE.GL.DIM_ACCOUNT
        PRIMARY KEY (account_code),
    periods AS FINANCE.GL.DIM_PERIOD
        PRIMARY KEY (period_id)
)

RELATIONSHIPS (
    gl_entries (account_code) REFERENCES accounts,
    gl_entries (period_id) REFERENCES periods
)

FACTS (
    gl_entries.debit_amount,
    gl_entries.credit_amount
)

DIMENSIONS (
    accounts.account_name AS account_name
        WITH SYNONYMS = ('GL account', 'ledger account'),
    accounts.account_category,
    periods.fiscal_year,
    periods.fiscal_quarter
)

METRICS (
    gl_entries.total_debits AS SUM(debit_amount),
    gl_entries.total_credits AS SUM(credit_amount),
    gl_entries.net_balance AS SUM(debit_amount) - SUM(credit_amount)
        COMMENT = 'Net balance (debits minus credits)'
)
```

**Step 1: Create a project from this DDL**

```
Create a faux objects project called "GL Reconciliation Wrappers"
from this CREATE SEMANTIC VIEW DDL
and add a view faux object
```

The translator will:
1. Parse all TABLES with their primary keys
2. Parse RELATIONSHIPS
3. Parse FACTS, DIMENSIONS, METRICS with synonyms and comments
4. Create the project with everything populated
5. Add a wrapper view

---

### Scenario D: Roundtrip Conversion (Format Interoperability)

Sometimes you need to see your semantic view as a plain SELECT query (for debugging) or convert between formats.

**Step 1: Start with a SELECT**

```sql
SELECT
    customer_segment,
    SUM(lifetime_value) as total_ltv,
    COUNT(DISTINCT customer_id) as customer_count
FROM analytics.customers
GROUP BY customer_segment
```

**Step 2: Convert to SEMANTIC VIEW DDL**

```
Convert this SELECT to semantic_view_ddl format
with name "customer_ltv_analysis", database "ANALYTICS", schema "SEMANTIC"
```

**Step 3: Then convert to CREATE VIEW**

```
Now convert that DDL back to a create_view format
targeting REPORTING.DASHBOARDS
```

**Step 4: And back to a SELECT query**

```
Convert it back to a select_query format
```

You now have three representations of the same logic:
1. Original SELECT
2. CREATE SEMANTIC VIEW DDL
3. CREATE VIEW wrapping SEMANTIC_VIEW()

All three are interoperable!

---

## What Did We Learn?

| Concept | What It Means |
|---------|--------------|
| **Format Detection** | The translator identifies SELECT, CREATE VIEW, or CREATE SEMANTIC VIEW |
| **Column Classification** | GROUP BY → DIMENSION, aggregation → METRIC, raw column → FACT |
| **Expression Preservation** | Metric expressions like `SUM(a) - SUM(b)` are captured |
| **Metadata Extraction** | Comments, AI hints, synonyms, primary keys are all parsed |
| **Bidirectional Conversion** | Convert between any two formats as needed |

---

## Bonus Challenge

Try importing a complex multi-join query with CASE WHEN inside aggregations:

```sql
SELECT
    r.region_name,
    p.product_line,
    SUM(CASE WHEN s.order_status = 'Completed' THEN s.order_total ELSE 0 END) as completed_revenue,
    SUM(CASE WHEN s.order_status = 'Cancelled' THEN s.order_total ELSE 0 END) as cancelled_revenue,
    COUNT(DISTINCT s.customer_id) as unique_customers,
    AVG(NULLIF(s.order_total, 0)) as avg_order_value
FROM sales.fact_orders s
JOIN sales.dim_region r ON s.region_id = r.region_id
JOIN sales.dim_product p ON s.product_id = p.product_id
WHERE s.order_date >= '2024-01-01'
GROUP BY r.region_name, p.product_line
```

Questions to explore:
- How does the translator handle CASE WHEN inside SUM?
- What happens to the WHERE clause?
- Are the NULLIF expressions preserved in metric expressions?

---

## What's Next?

Return to the [Faux Objects landing page](../README.md) to explore other use cases, or try:
- [Use Case 12: Financial Analyst](../12_financial_analyst/README.md) - Build a GL reconciliation semantic view from scratch
- [Use Case 13: Oil & Gas Analyst](../13_oil_gas_analyst/README.md) - Create drilling economics views with complex calculations
