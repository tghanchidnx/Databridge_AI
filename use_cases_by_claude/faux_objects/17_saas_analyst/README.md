# Use Case 17: SaaS Analyst - Subscription Metrics Semantic View

## The Story

You are a **SaaS Metrics Analyst** at a subscription software company preparing for the quarterly board meeting. The board wants to see **MRR, ARR, net retention, churn rate, LTV, and CAC** -- the core metrics that tell the story of your subscription business.

Your data includes subscription records with monthly revenue and customer counts across different segments (Enterprise, Mid-Market, SMB) and product tiers (Starter, Professional, Enterprise). The investor relations team needs a **cohort analysis procedure** they can call for specific acquisition cohorts, while the executive team wants a **near-real-time MRR dashboard** that refreshes every 30 minutes.

This is the most complex semantic view in the series, with nested NULLIF expressions, LAG window functions, and derived metrics (ARR = MRR x 12).

---

## What You Will Learn

- How to define **complex metric expressions** with nested NULLIF and CASE WHEN
- How to create a **30-minute refresh dynamic table** for near-real-time dashboards
- How to create a **cohort analysis procedure** with acquisition cohort parameters
- How **complex SQL expressions persist** through the save/load cycle
- How **AI SQL generation context** appears in the semantic view DDL

---

## Tools Used

| Tool | What It Does |
|------|-------------|
| `create_faux_project` | Creates a project for subscription metrics |
| `define_faux_semantic_view` | Defines the subscription metrics semantic view with AI context |
| `add_faux_semantic_table` | Adds FACT_SUBSCRIPTIONS, DIM_CUSTOMER, DIM_PRODUCT, DIM_PERIOD |
| `add_faux_semantic_column` | Adds complex metrics like net_retention with nested NULLIF |
| `add_faux_object` | Creates procedure and dynamic table wrappers |
| `generate_semantic_view_ddl` | Generates DDL with AI_SQL_GENERATION clause |

---

## Step-by-Step Instructions

### Step 1: Create the project

```
Create a faux objects project called "Subscription Metrics Wrappers" with description
"SaaS subscription metrics for investor reporting and ops dashboards"
```

### Step 2: Define the semantic view with AI context

```
Define the semantic view for the Subscription Metrics project with:
- Name: subscription_metrics
- Database: SAAS
- Schema: SEMANTIC
- Comment: "SaaS subscription metrics for ARR/MRR tracking"
- AI SQL Generation: "MRR is monthly recurring revenue. ARR = MRR * 12. Net retention includes expansion."
```

**What happens:** The AI SQL generation context helps Snowflake Cortex understand SaaS-specific terminology. When someone asks Cortex "What's our ARR?", it knows to multiply MRR by 12.

### Step 3: Add tables and relationships

```
Add tables:
1. subscriptions -> SAAS.DW.FACT_SUBSCRIPTIONS (PK: subscription_id)
2. customers -> SAAS.DW.DIM_CUSTOMER (PK: customer_id)
3. products -> SAAS.DW.DIM_PRODUCT (PK: product_id)
4. periods -> SAAS.DW.DIM_PERIOD (PK: period_id)

Add relationships:
1. subscriptions.customer_id -> customers
2. subscriptions.product_id -> products
3. subscriptions.period_id -> periods
```

### Step 4: Add dimensions and facts

```
Add dimensions:
1. customer_segment (VARCHAR) from customers - synonyms: "segment", "customer type"
2. product_tier (VARCHAR) from products - synonyms: "tier", "plan"
3. acquisition_cohort (VARCHAR) from customers
4. fiscal_month (VARCHAR) from periods

Add facts:
1. monthly_revenue (FLOAT) from subscriptions
2. customer_count (INT) from subscriptions
```

### Step 5: Add complex SaaS metrics

This is where the complexity ramps up:

```
Add metrics:
1. mrr (FLOAT) = SUM(monthly_revenue) - "Monthly recurring revenue"
2. arr (FLOAT) = SUM(monthly_revenue) * 12 - "Annual recurring revenue (MRR x 12)"
3. net_retention (FLOAT) = CASE WHEN SUM(CASE WHEN periods.fiscal_month = LAG(periods.fiscal_month) THEN monthly_revenue ELSE 0 END) > 0 THEN SUM(monthly_revenue) / NULLIF(SUM(CASE WHEN periods.fiscal_month = LAG(periods.fiscal_month) THEN monthly_revenue ELSE 0 END), 0) ELSE NULL END - "Net dollar retention rate"
4. churn_rate (FLOAT) = 1 - (SUM(customer_count) / NULLIF(LAG(SUM(customer_count)), 0)) - "Customer churn rate"
5. ltv (FLOAT) = SUM(monthly_revenue) / NULLIF(1 - (SUM(customer_count) / NULLIF(LAG(SUM(customer_count)), 0)), 0) - "Customer lifetime value"
6. cac (FLOAT) = SUM(monthly_revenue) * 0.3 - "Estimated customer acquisition cost (30% of revenue)"
```

**What happens:** Six metrics are defined with increasing complexity:
- **MRR** and **ARR** are straightforward aggregations
- **net_retention** uses nested CASE WHEN and NULLIF to safely compute retention
- **churn_rate** uses LAG and NULLIF to compare current vs previous period
- **ltv** builds on churn_rate with another level of NULLIF nesting
- **cac** is a simple estimate (30% of revenue)

### Step 6: Create the Cohort Analysis procedure

```
Add a faux object to the Subscription Metrics project:
- Name: ANALYZE_COHORT
- Type: stored_procedure
- Target Database: REPORTING
- Target Schema: SAAS
- Parameters:
  - COHORT (VARCHAR, default: NULL)
  - TIER (VARCHAR, default: NULL)
- Selected dimensions: customer_segment, product_tier, acquisition_cohort, fiscal_month
- Selected metrics: mrr, arr, net_retention, churn_rate
```

**What happens:** The investor relations team can call `SELECT * FROM TABLE(ANALYZE_COHORT('2024-Q1', 'Enterprise'))` to see retention and churn for a specific cohort and tier.

### Step 7: Create the MRR Dashboard dynamic table

```
Add a faux object to the Subscription Metrics project:
- Name: DT_MRR_DASHBOARD
- Type: dynamic_table
- Target Database: REPORTING
- Target Schema: SAAS
- Warehouse: ANALYTICS_WH
- Target Lag: 30 minutes
- Selected dimensions: customer_segment, product_tier, fiscal_month
- Selected metrics: mrr, arr, churn_rate
- Comment: "Near real-time MRR dashboard"
```

**What happens:** A dynamic table with 30-minute refresh is configured. The executive team sees MRR data that's never more than 30 minutes old.

### Step 8: Generate the DDL and scripts

```
Generate the semantic view DDL for the Subscription Metrics project
```

Then:

```
Generate all faux object scripts for the Subscription Metrics project
```

---

## What Did We Find?

### AI SQL Generation in DDL

The generated DDL includes the AI context:

```sql
CREATE OR REPLACE SEMANTIC VIEW SAAS.SEMANTIC.subscription_metrics
    COMMENT = 'SaaS subscription metrics for ARR/MRR tracking'
    AI_SQL_GENERATION = 'MRR is monthly recurring revenue. ARR = MRR * 12. Net retention includes expansion.'
```

### Complex Expression Persistence

The net_retention metric with nested NULLIF survives the save/load cycle:

```sql
METRICS (
    subscriptions.net_retention AS CASE WHEN SUM(CASE WHEN periods.fiscal_month = LAG(...)
        THEN monthly_revenue ELSE 0 END) > 0
        THEN SUM(monthly_revenue) / NULLIF(SUM(CASE WHEN ...), 0) ELSE NULL END
        COMMENT = 'Net dollar retention rate'
)
```

### 30-Minute Dynamic Table

```sql
CREATE OR REPLACE DYNAMIC TABLE REPORTING.SAAS.DT_MRR_DASHBOARD
    TARGET_LAG = '30 minutes'
    WAREHOUSE = ANALYTICS_WH
AS
SELECT * FROM SEMANTIC_VIEW(
    SAAS.SEMANTIC.subscription_metrics
    DIMENSIONS customers.customer_segment, products.product_tier, periods.fiscal_month
    METRICS subscriptions.mrr, subscriptions.arr, subscriptions.churn_rate
);
```

---

## Understanding SaaS Metrics

| Metric | Formula | What It Tells You |
|--------|---------|------------------|
| **MRR** | SUM(monthly_revenue) | Monthly recurring revenue |
| **ARR** | MRR x 12 | Annualized revenue |
| **Net Retention** | Current MRR / Prior MRR | Revenue retention including expansion |
| **Churn Rate** | 1 - (current customers / prior customers) | Customer loss rate |
| **LTV** | MRR / Churn Rate | Lifetime value of a customer |
| **CAC** | Revenue x 30% | Cost to acquire a customer |

> **Rule of thumb:** Net retention > 100% means expansion revenue exceeds churn (very healthy).

---

## Bonus Challenge

Try creating a view for just the **Enterprise** segment:

```
Add a faux object to the Subscription Metrics project:
- Name: V_ENTERPRISE_METRICS
- Type: view
- Target Database: REPORTING
- Target Schema: SAAS
- Where clause: "customers.customer_segment = 'Enterprise'"
- Selected dimensions: product_tier, fiscal_month
- Selected metrics: mrr, arr, net_retention, ltv
```

---

## What's Next?

Now try [Use Case 18: Transportation Analyst](../18_transportation_analyst/README.md) to build
fleet operations with 5-table joins, DATE parameters, and export scripts!
