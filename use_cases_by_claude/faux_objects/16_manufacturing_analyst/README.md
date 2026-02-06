# Use Case 16: Manufacturing Analyst - Plant Operations Semantic View

## The Story

You are a **Manufacturing Analyst** responsible for tracking production output across multiple plants. Every month, you compare **actual costs** against **standard costs** to identify volume and efficiency variances. If Plant A in Houston produced fewer units than expected, that's a **volume variance**. If they used more labor hours per unit than the standard, that's an **efficiency variance**.

Your data includes production volumes (integer counts), labor hours, and material costs. The plant managers want an **auto-refreshing dashboard** with 1-hour refresh, while the CFO wants a **variance-only view** for the monthly operations review.

---

## What You Will Learn

- How to handle **INT data types** for production counts (not everything is FLOAT)
- How to create a **dynamic table** with 1-hour refresh for plant dashboards
- How to create a **variance-focused view** with selected metrics
- How to generate SQL where **INT types are preserved** in RETURNS TABLE
- How to use **batch generation** with multiple faux objects

---

## Tools Used

| Tool | What It Does |
|------|-------------|
| `create_faux_project` | Creates a project for plant operations |
| `define_faux_semantic_view` | Defines the plant operations semantic view |
| `add_faux_semantic_table` | Adds FACT_PRODUCTION, DIM_PLANT, DIM_PRODUCT, DIM_PERIOD |
| `add_faux_semantic_column` | Adds INT facts (units_produced), FLOAT facts, and variance metrics |
| `add_faux_object` | Creates dynamic table and view wrappers |
| `generate_faux_scripts` | Generates SQL with correct data types |

---

## Step-by-Step Instructions

### Step 1: Create the project

```
Create a faux objects project called "Plant Operations Wrappers" with description
"Manufacturing plant performance and variance analysis"
```

### Step 2: Define the semantic view

```
Define the semantic view for the Plant Operations project with:
- Name: plant_operations
- Database: MANUFACTURING
- Schema: SEMANTIC
- Comment: "Plant operations for production and variance tracking"
```

### Step 3: Add tables and relationships

```
Add tables:
1. production -> MANUFACTURING.DW.FACT_PRODUCTION (PK: production_id)
2. plants -> MANUFACTURING.DW.DIM_PLANT (PK: plant_id)
3. products -> MANUFACTURING.DW.DIM_PRODUCT (PK: product_id)
4. periods -> MANUFACTURING.DW.DIM_PERIOD (PK: period_id)

Add relationships:
1. production.plant_id -> plants
2. production.product_id -> products
3. production.period_id -> periods
```

### Step 4: Add dimensions and facts

```
Add dimensions:
1. plant_name (VARCHAR) from plants
2. product_line (VARCHAR) from products
3. region (VARCHAR) from plants
4. fiscal_month (VARCHAR) from periods

Add facts:
1. units_produced (INT) from production    <-- Note: INT, not FLOAT!
2. labor_hours (FLOAT) from production
3. material_cost (FLOAT) from production
```

**What happens:** The `units_produced` fact is defined as INT because production counts are whole numbers. This type will be preserved in generated RETURNS TABLE clauses.

### Step 5: Add variance metrics

```
Add metrics:
1. total_output (INT) = SUM(units_produced) - "Total units produced"
2. standard_cost (FLOAT) = SUM(units_produced) * 12.50 - "Standard cost at $12.50 per unit"
3. actual_cost (FLOAT) = SUM(labor_hours * 25.00 + material_cost) - "Actual cost (labor at $25/hr + materials)"
4. volume_variance (FLOAT) = (SUM(units_produced) - 1000) * 12.50 - "Volume variance vs 1000 unit standard"
5. efficiency_variance (FLOAT) = SUM(labor_hours * 25.00 + material_cost) - SUM(units_produced) * 12.50 - "Efficiency variance (actual - standard)"
```

**What happens:** Five metrics capture the full variance analysis:
- **Standard cost** = units x $12.50/unit
- **Actual cost** = (labor hours x $25/hr) + material cost
- **Volume variance** = (actual units - standard units) x standard rate
- **Efficiency variance** = actual cost - standard cost

### Step 6: Create the Plant Dashboard dynamic table

```
Add a faux object to the Plant Operations project:
- Name: DT_PLANT_DASHBOARD
- Type: dynamic_table
- Target Database: REPORTING
- Target Schema: MFG
- Warehouse: MFG_WH
- Target Lag: 1 hour
- Comment: "Auto-refreshing plant performance dashboard"
```

**What happens:** A dynamic table with 1-hour refresh is configured. Plant managers see data that's never more than 1 hour old.

### Step 7: Create the Variance Summary view

```
Add a faux object to the Plant Operations project:
- Name: V_VARIANCE_SUMMARY
- Type: view
- Target Database: REPORTING
- Target Schema: MFG
- Selected dimensions: plant_name, product_line, fiscal_month
- Selected metrics: standard_cost, actual_cost, volume_variance, efficiency_variance
- Comment: "Manufacturing variance analysis view"
```

**What happens:** A focused view showing only variance-related metrics. The CFO uses this for the monthly operations review.

### Step 8: Generate all scripts

```
Generate all faux object scripts for the Plant Operations project
```

---

## What Did We Find?

### INT Type Preservation

When generating a stored procedure, INT facts and metrics keep their types:

```sql
RETURNS TABLE(
    PLANT_NAME VARCHAR,
    UNITS_PRODUCED INT,     -- INT preserved, not FLOAT!
    TOTAL_OUTPUT INT,       -- INT metric preserved too
    STANDARD_COST FLOAT,
    ACTUAL_COST FLOAT,
    VOLUME_VARIANCE FLOAT,
    EFFICIENCY_VARIANCE FLOAT
)
```

### Batch Generation Output

The `generate_all_scripts` call returns 3 scripts:
1. **Semantic View DDL** - The CREATE SEMANTIC VIEW statement
2. **Dynamic Table** - DT_PLANT_DASHBOARD with 1-hour refresh
3. **View** - V_VARIANCE_SUMMARY with selected metrics only

---

## Understanding Manufacturing Variances

| Variance | Formula | What It Tells You |
|----------|---------|------------------|
| **Volume** | (Actual Units - Standard Units) x Standard Rate | Did we produce enough? |
| **Efficiency** | Actual Cost - Standard Cost | Did we produce efficiently? |
| **Standard Cost** | Units x $12.50 | What it *should* have cost |
| **Actual Cost** | Labor + Materials | What it *actually* cost |

A positive efficiency variance means we spent more than expected (unfavorable).

---

## Bonus Challenge

Try filtering to a specific plant:

```
Add a faux object to the Plant Operations project:
- Name: V_PLANT_A_OPS
- Type: view
- Target Database: REPORTING
- Target Schema: MFG
- Where clause: "plants.plant_name = 'Plant A - Houston'"
```

---

## What's Next?

Now try [Use Case 17: SaaS Analyst](../17_saas_analyst/README.md) to build
subscription metrics with complex NULLIF/CASE expressions and near-real-time dashboards!
