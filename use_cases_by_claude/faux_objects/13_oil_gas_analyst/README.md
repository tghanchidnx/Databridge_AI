# Use Case 13: Oil & Gas Analyst - Drilling Economics Semantic View

## The Story

You are an **FP&A Oil & Gas Analyst** responsible for well-level economics at an E&P (Exploration & Production) company. Every month, you need to evaluate which basins and wells are profitable, calculate lease operating expenses per BOE, and estimate net present value for the drilling portfolio.

Your data is spread across well economics, well master, basin reference, and period tables in Snowflake. The engineering team built a Semantic View with all the business logic, but the operations team needs a **stored procedure** they can call with a specific well or basin, and the executive team needs an **auto-refreshing dashboard** at the basin level.

---

## What You Will Learn

- How to build a semantic view for **oil & gas production data**
- How to create a **stored procedure** with well/basin parameters
- How to create a **dynamic table** with auto-refresh for executive dashboards
- How to use **NULLIF** to avoid division by zero in per-unit metrics
- How to generate a complete **deployment bundle**

---

## Tools Used

| Tool | What It Does |
|------|-------------|
| `create_faux_project` | Creates a project for drilling economics wrappers |
| `define_faux_semantic_view` | Defines the drilling economics semantic view |
| `add_faux_semantic_table` | Adds FACT_WELL_ECONOMICS, DIM_WELL, DIM_BASIN, DIM_PERIOD |
| `add_faux_semantic_column` | Adds dimensions (well, basin), facts (production, revenue), and metrics (LOE/BOE, NPV) |
| `add_faux_semantic_relationship` | Defines joins between well economics and dimension tables |
| `add_faux_object` | Creates stored procedure and dynamic table wrappers |
| `generate_faux_deployment_bundle` | Generates complete deployment SQL |

---

## Step-by-Step Instructions

### Step 1: Create the project

```
Create a faux objects project called "Drilling Economics Wrappers" with description
"Well-level economics for upstream oil & gas operations"
```

**What happens:** A new project container is created for all the drilling economics faux objects.

### Step 2: Define the semantic view

```
Define the semantic view for the Drilling Economics project with:
- Name: drilling_economics
- Database: PRODUCTION
- Schema: SEMANTIC
- Comment: "Drilling economics for upstream E&P operations"
- AI SQL Generation: "BOE = barrels of oil equivalent. LOE = lease operating expense. NPV uses 10% discount rate."
```

**What happens:** The semantic view is defined with AI context that helps Snowflake Cortex understand oil & gas terminology.

### Step 3: Add tables and relationships

```
Add these tables to the Drilling Economics semantic view:
1. well_economics -> PRODUCTION.DW.FACT_WELL_ECONOMICS (PK: well_economics_id)
2. wells -> PRODUCTION.DW.DIM_WELL (PK: well_id)
3. basins -> PRODUCTION.DW.DIM_BASIN (PK: basin_id)
4. periods -> PRODUCTION.DW.DIM_PERIOD (PK: period_id)

And these relationships:
1. well_economics.well_id -> wells
2. well_economics.basin_id -> basins
3. well_economics.period_id -> periods
```

**What happens:** The four-table star schema is established with proper join paths.

### Step 4: Add dimensions and facts

```
Add these dimensions:
1. well_name (VARCHAR) from wells - synonyms: "well", "wellbore"
2. basin_name (VARCHAR) from basins - synonyms: "basin", "play"
3. formation (VARCHAR) from wells
4. operator (VARCHAR) from wells
5. fiscal_year (INT) from periods

Add these facts:
1. production_boe (FLOAT) from well_economics
2. oil_revenue (FLOAT) from well_economics
3. gas_revenue (FLOAT) from well_economics
4. loe_cost (FLOAT) from well_economics
```

**What happens:** The dimensional model is defined with well and basin attributes plus raw financial facts.

### Step 5: Add metrics with complex expressions

```
Add these metrics:
1. total_revenue (FLOAT) = SUM(oil_revenue + gas_revenue) - "Total oil + gas revenue"
2. loe_per_boe (FLOAT) = SUM(loe_cost) / NULLIF(SUM(production_boe), 0) - "Lease operating expense per BOE"
3. npv (FLOAT) = SUM(oil_revenue + gas_revenue - loe_cost) / POWER(1.10, periods.fiscal_year - 2024) - "Net present value at 10% discount rate"
4. irr (FLOAT) = SUM(oil_revenue + gas_revenue - loe_cost) / NULLIF(SUM(loe_cost), 0) - "Internal rate of return proxy"
5. eur (FLOAT) = SUM(production_boe) * 1.15 - "Estimated ultimate recovery (BOE)"
```

**What happens:** Five metrics are defined, including `loe_per_boe` which uses `NULLIF` to avoid division by zero when a well has no production.

### Step 6: Create the Well Economics stored procedure

```
Add a faux object to the Drilling Economics project:
- Name: GET_WELL_ECONOMICS
- Type: stored_procedure
- Target Database: REPORTING
- Target Schema: PRODUCTION
- Parameters:
  - WELL_ID (VARCHAR, default: NULL)
  - BASIN (VARCHAR, default: NULL)
- Selected dimensions: well_name, basin_name, formation, fiscal_year
- Selected facts: production_boe, oil_revenue, gas_revenue, loe_cost
- Selected metrics: total_revenue, loe_per_boe, npv
```

**What happens:** A Snowpark Python procedure is configured that the operations team can call with `SELECT * FROM TABLE(GET_WELL_ECONOMICS(well_id=>'W-1234'))`.

### Step 7: Create the Basin Dashboard dynamic table

```
Add a faux object to the Drilling Economics project:
- Name: DT_BASIN_DASHBOARD
- Type: dynamic_table
- Target Database: REPORTING
- Target Schema: PRODUCTION
- Warehouse: ANALYTICS_WH
- Target Lag: 4 hours
- Selected dimensions: basin_name, fiscal_year
- Selected metrics: total_revenue, loe_per_boe, npv, eur
- Comment: "Auto-refreshing basin-level economics dashboard"
```

**What happens:** A dynamic table is configured that Snowflake will automatically refresh every 4 hours. The executive dashboard connects to this like a regular table.

### Step 8: Generate the deployment bundle

```
Generate the deployment bundle for the Drilling Economics project
```

**What happens:** A complete deployment script is generated with the semantic view DDL first, followed by the stored procedure and dynamic table in the correct dependency order.

---

## What Did We Find?

### Key SQL Patterns

**NULLIF for safe division:**
```sql
-- LOE per BOE avoids division by zero
SUM(loe_cost) / NULLIF(SUM(production_boe), 0)
```

**Dynamic table with auto-refresh:**
```sql
CREATE OR REPLACE DYNAMIC TABLE REPORTING.PRODUCTION.DT_BASIN_DASHBOARD
    TARGET_LAG = '4 hours'
    WAREHOUSE = ANALYTICS_WH
AS
SELECT * FROM SEMANTIC_VIEW(
    PRODUCTION.SEMANTIC.drilling_economics
    DIMENSIONS basins.basin_name, periods.fiscal_year
    METRICS well_economics.total_revenue, well_economics.loe_per_boe, ...
);
```

### Deployment Bundle Contents
1. Semantic View DDL (CREATE SEMANTIC VIEW)
2. GET_WELL_ECONOMICS stored procedure
3. DT_BASIN_DASHBOARD dynamic table

---

## Understanding Dynamic Tables

A **Dynamic Table** is a Snowflake object that:

- Looks like a regular table to BI tools
- Automatically refreshes based on the `TARGET_LAG` setting
- No scheduled tasks needed -- Snowflake handles the refresh
- Perfect for dashboards that need near-real-time data

| Setting | What It Means |
|---------|--------------|
| `TARGET_LAG = '4 hours'` | Data is never more than 4 hours stale |
| `WAREHOUSE = ANALYTICS_WH` | Uses this warehouse for refresh computations |
| The underlying query | A `SEMANTIC_VIEW()` call with your dimensions and metrics |

---

## Bonus Challenge

Try creating a view filtered to just the **Permian Basin**:

```
Add a faux object to the Drilling Economics project:
- Name: V_PERMIAN_ECONOMICS
- Type: view
- Target Database: REPORTING
- Target Schema: PRODUCTION
- Where clause: "basins.basin_name = 'Permian'"
- Selected dimensions: well_name, formation, fiscal_year
- Selected metrics: total_revenue, loe_per_boe
```

---

## What's Next?

Now try [Use Case 14: Operations Analyst](../14_operations_analyst/README.md) to build
a geographic operations semantic view with scheduled tasks!
