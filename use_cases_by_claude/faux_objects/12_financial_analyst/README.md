# Use Case 12: Financial Analyst - GL Reconciliation Semantic View

## The Story

You are a **Financial Analyst** responsible for the monthly close process. Every month, you need to produce a **trial balance**, verify that debits equal credits, and identify any accounts with unexplained variances. Your data lives in Snowflake across four tables: journal entries, accounts, periods, and cost centers.

Your team recently built a **Semantic View** that defines all the business logic -- what "total debits" means, how "net balance" is calculated, and which tables join together. But your BI team in Power BI can't query Semantic Views directly. They need **regular views and stored procedures**.

That's where **Faux Objects** come in. You'll create wrapper objects that look like regular Snowflake objects but are powered by the semantic view underneath.

---

## What You Will Learn

- How to **define a semantic view** with tables, dimensions, facts, and metrics
- How to create a **standard view** wrapping a semantic view
- How to create a **stored procedure** with multiple parameters for parameterized queries
- How to create a **filtered view** for a specific fiscal year
- How to generate **deployment SQL** ready for Snowflake

---

## Tools Used

| Tool | What It Does |
|------|-------------|
| `create_faux_project` | Creates a new project container for the GL reconciliation |
| `define_faux_semantic_view` | Defines the semantic view metadata (name, database, schema) |
| `add_faux_semantic_table` | Adds table references (FACT_JOURNAL_ENTRIES, DIM_ACCOUNT, etc.) |
| `add_faux_semantic_column` | Adds dimensions, facts, and metrics with SQL expressions |
| `add_faux_semantic_relationship` | Defines how tables join together |
| `add_faux_object` | Creates a faux object (view or stored procedure) |
| `generate_faux_scripts` | Generates the SQL for all faux objects |
| `generate_semantic_view_ddl` | Generates the CREATE SEMANTIC VIEW DDL |

---

## Step-by-Step Instructions

### Step 1: Create the project

```
Create a faux objects project called "GL Reconciliation Wrappers" with description
"Trial balance and GL reconciliation views for Finance team"
```

**What happens:** DataBridge creates a new empty project container. You'll get a project ID that you'll use in all subsequent steps.

### Step 2: Define the semantic view

```
Define the semantic view for the GL Reconciliation project with:
- Name: gl_reconciliation
- Database: FINANCE
- Schema: SEMANTIC
- Comment: "General Ledger reconciliation for trial balance reporting"
- AI SQL Generation: "Debits are positive, credits are negative. Net balance = debits - credits."
```

**What happens:** The semantic view metadata is set. This tells Snowflake (and AI tools like Cortex) how to interpret the data.

### Step 3: Add the source tables

```
Add these tables to the GL Reconciliation semantic view:
1. journal_entries -> FINANCE.GL.FACT_JOURNAL_ENTRIES (primary key: journal_entry_id)
2. accounts -> FINANCE.GL.DIM_ACCOUNT (primary key: account_code)
3. periods -> FINANCE.GL.DIM_PERIOD (primary key: period_id)
4. cost_centers -> FINANCE.GL.DIM_COST_CENTER (primary key: cost_center_id)
```

**What happens:** Four table references are added. The aliases (journal_entries, accounts, etc.) are how you'll reference columns later.

### Step 4: Add relationships

```
Add these relationships to the GL Reconciliation semantic view:
1. journal_entries.account_code -> accounts
2. journal_entries.period_id -> periods
3. journal_entries.cost_center_id -> cost_centers
```

**What happens:** DataBridge now knows how the tables join together, which is essential for generating correct SQL.

### Step 5: Add dimensions

```
Add these dimension columns to the GL Reconciliation semantic view:
1. account_name (VARCHAR) from accounts - synonyms: "GL account", "ledger account"
2. account_category (VARCHAR) from accounts
3. fiscal_year (INT) from periods
4. fiscal_quarter (VARCHAR) from periods
5. cost_center (VARCHAR) from cost_centers
```

**What happens:** Five dimensions are defined. Dimensions are the "group by" columns -- how you slice and dice the data. The synonyms help AI tools understand alternative names.

### Step 6: Add facts

```
Add these fact columns to the GL Reconciliation semantic view:
1. debit_amount (FLOAT) from journal_entries
2. credit_amount (FLOAT) from journal_entries
```

**What happens:** Facts are raw measure values from the fact table. They're the numbers that get aggregated in metrics.

### Step 7: Add metrics

```
Add these metric columns to the GL Reconciliation semantic view:
1. total_debits (FLOAT) = SUM(debit_amount) - "Sum of all debit entries"
2. total_credits (FLOAT) = SUM(credit_amount) - "Sum of all credit entries"
3. net_balance (FLOAT) = SUM(debit_amount) - SUM(credit_amount) - "Net balance (debits minus credits)"
4. variance_amount (FLOAT) = ABS(SUM(debit_amount) - SUM(credit_amount)) - "Absolute variance for reconciliation"
```

**What happens:** Metrics are calculated aggregations. The SQL expressions define the business logic. Anyone querying the semantic view gets consistent calculations.

### Step 8: Create the Trial Balance view

```
Add a faux object to the GL Reconciliation project:
- Name: V_TRIAL_BALANCE
- Type: view
- Target Database: REPORTING
- Target Schema: FINANCE
- Comment: "Trial balance summary view"
```

**What happens:** A standard view wrapper is configured. When generated, it will create a `CREATE VIEW` statement that wraps a `SEMANTIC_VIEW()` call.

### Step 9: Create the Reconcile Period stored procedure

```
Add a faux object to the GL Reconciliation project:
- Name: RECONCILE_PERIOD
- Type: stored_procedure
- Target Database: REPORTING
- Target Schema: FINANCE
- Parameters:
  - FISCAL_YEAR (INT, default: 2025)
  - FISCAL_QUARTER (VARCHAR, default: NULL)
  - COST_CENTER (VARCHAR, default: NULL)
- Selected dimensions: account_name, account_category, fiscal_year, fiscal_quarter, cost_center
- Selected metrics: total_debits, total_credits, net_balance
```

**What happens:** A Snowpark Python stored procedure is configured with three parameters. BI tools can call `SELECT * FROM TABLE(RECONCILE_PERIOD(2025, 'Q1', 'CORP-HQ'))`.

### Step 10: Create a filtered view for 2025

```
Add a faux object to the GL Reconciliation project:
- Name: V_TB_2025
- Type: view
- Target Database: REPORTING
- Target Schema: FINANCE
- Where clause: "periods.fiscal_year = 2025"
- Comment: "Trial balance for fiscal year 2025"
```

**What happens:** A view with a static WHERE clause is configured. This is perfect for dashboards that always show the current year.

### Step 11: Generate all SQL scripts

```
Generate all faux object scripts for the GL Reconciliation project
```

**What happens:** DataBridge generates SQL for the semantic view DDL, the trial balance view, the reconcile procedure, and the filtered 2025 view.

---

## What Did We Find?

After generating the scripts, you should see:

### Trial Balance View SQL
```sql
CREATE OR REPLACE VIEW REPORTING.FINANCE.V_TRIAL_BALANCE
    COMMENT = 'Trial balance summary view'
AS
SELECT * FROM SEMANTIC_VIEW(
    FINANCE.SEMANTIC.gl_reconciliation
    DIMENSIONS accounts.account_name, accounts.account_category, periods.fiscal_year, ...
    FACTS journal_entries.debit_amount, journal_entries.credit_amount
    METRICS journal_entries.total_debits, journal_entries.total_credits, ...
);
```

### Reconcile Period Procedure SQL
```sql
CREATE OR REPLACE PROCEDURE REPORTING.FINANCE.RECONCILE_PERIOD(
    FISCAL_YEAR INT DEFAULT 2025,
    FISCAL_QUARTER VARCHAR DEFAULT NULL,
    COST_CENTER VARCHAR DEFAULT NULL
)
RETURNS TABLE(
    ACCOUNT_NAME VARCHAR,
    ...
    NET_BALANCE FLOAT
)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
def run(session, fiscal_year=2025, fiscal_quarter=NULL, cost_center=NULL):
    ...
$$;
```

---

## Understanding Faux Objects

| Concept | What It Means |
|---------|--------------|
| **Semantic View** | The "source of truth" defining tables, joins, dimensions, and metrics |
| **Faux Object** | A wrapper that makes the semantic view accessible to BI tools |
| **Dimension** | A categorical attribute for grouping (e.g., account_name, fiscal_year) |
| **Fact** | A raw measure value from a table (e.g., debit_amount) |
| **Metric** | A calculated aggregation with a SQL expression (e.g., SUM(debit_amount)) |
| **Synonym** | An alternative name for AI tools (e.g., "GL account" for account_name) |

---

## Bonus Challenge

Try creating a **Dynamic Table** that auto-refreshes the trial balance every 2 hours:

```
Add a faux object to the GL Reconciliation project:
- Name: DT_TRIAL_BALANCE
- Type: dynamic_table
- Target Database: REPORTING
- Target Schema: FINANCE
- Warehouse: ANALYTICS_WH
- Target Lag: 2 hours
- Comment: "Auto-refreshing trial balance"
```

Then generate the scripts to see the `CREATE DYNAMIC TABLE` SQL with the `TARGET_LAG` setting.

---

## What's Next?

Now try [Use Case 13: Oil & Gas Analyst](../13_oil_gas_analyst/README.md) to build
a drilling economics semantic view with NULLIF division and dynamic tables!
