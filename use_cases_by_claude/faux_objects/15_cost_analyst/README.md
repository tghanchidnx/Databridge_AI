# Use Case 15: Cost Analyst - Cost Allocation Semantic View

## The Story

You are an **FP&A Cost Analyst** managing the company's cost center budget process. Every quarter, you compare **budgeted amounts** to **actual expenses** and calculate variances. Different departments use different **allocation methods** (direct, step-down, reciprocal), and leadership wants to see which cost centers are over or under budget.

Your current process involves complex SQL with CASE WHEN statements to separate budget and actual amounts from the same GL transaction table. A Semantic View can encapsulate this logic, but the FP&A team needs a **stored procedure** they can call with a specific cost center and period, plus a **summary view** showing allocation method performance.

---

## What You Will Learn

- How to define **CASE WHEN metrics** for budget vs actual separation
- How to create a **stored procedure** with cost center and period parameters
- How to combine **parameters and static WHERE clauses** in a single procedure
- How **complex CASE WHEN expressions persist** through save/load cycles
- How to create an **allocation-focused view** with selected columns

---

## Tools Used

| Tool | What It Does |
|------|-------------|
| `create_faux_project` | Creates a project for cost allocation wrappers |
| `define_faux_semantic_view` | Defines the cost allocation semantic view |
| `add_faux_semantic_table` | Adds GL transactions, cost centers, periods, allocations |
| `add_faux_semantic_column` | Adds CASE WHEN metrics for budget/actual/variance |
| `add_faux_object` | Creates procedure and view wrappers |
| `generate_faux_scripts` | Generates SQL with CASE WHEN expressions preserved |

---

## Step-by-Step Instructions

### Step 1: Create the project

```
Create a faux objects project called "Cost Allocation Wrappers" with description
"Budget vs actual analysis and cost allocation reporting"
```

### Step 2: Define the semantic view

```
Define the semantic view for the Cost Allocation project with:
- Name: cost_allocation
- Database: FINANCE
- Schema: SEMANTIC
- Comment: "Cost allocation and budget variance analysis"
- AI SQL Generation: "Variance = Budget - Actual. Positive variance means under budget."
```

**What happens:** The AI SQL generation context tells Snowflake Cortex how to interpret variance -- a positive variance is favorable (under budget).

### Step 3: Add tables, relationships, dimensions

```
Add tables:
1. gl_transactions -> FINANCE.GL.FACT_GL_TRANSACTIONS (PK: transaction_id)
2. cost_centers -> FINANCE.GL.DIM_COST_CENTER (PK: cost_center_id)
3. periods -> FINANCE.GL.DIM_PERIOD (PK: period_id)
4. allocations -> FINANCE.GL.DIM_ALLOCATION (PK: allocation_id)

Add relationships:
1. gl_transactions.cost_center_id -> cost_centers
2. gl_transactions.period_id -> periods
3. gl_transactions.allocation_id -> allocations

Add dimensions:
1. cost_center (VARCHAR) from cost_centers
2. department (VARCHAR) from cost_centers
3. allocation_method (VARCHAR) from allocations - synonyms: "alloc method", "distribution method"
4. fiscal_period (VARCHAR) from periods

Add fact:
1. transaction_amount (FLOAT) from gl_transactions
```

### Step 4: Add CASE WHEN metrics

This is where the real business logic lives:

```
Add metrics:
1. budget_amount (FLOAT) = SUM(CASE WHEN transaction_type = 'BUDGET' THEN transaction_amount ELSE 0 END) - "Total budgeted amount"
2. actual_amount (FLOAT) = SUM(CASE WHEN transaction_type = 'ACTUAL' THEN transaction_amount ELSE 0 END) - "Total actual amount"
3. variance (FLOAT) = SUM(CASE WHEN transaction_type = 'BUDGET' THEN transaction_amount ELSE 0 END) - SUM(CASE WHEN transaction_type = 'ACTUAL' THEN transaction_amount ELSE 0 END) - "Budget minus actual variance"
4. allocation_total (FLOAT) = SUM(transaction_amount) - "Total allocated amount"
```

**What happens:** The CASE WHEN expressions separate budget and actual from the same transaction table. The variance metric computes the difference. All of these expressions are stored exactly as written and will appear in the generated SQL.

### Step 5: Create the Budget vs Actual procedure

```
Add a faux object to the Cost Allocation project:
- Name: GET_BUDGET_VS_ACTUAL
- Type: stored_procedure
- Target Database: REPORTING
- Target Schema: FINANCE
- Parameters:
  - COST_CENTER (VARCHAR, default: NULL)
  - FISCAL_PERIOD (VARCHAR, default: NULL)
- Selected dimensions: cost_center, department, fiscal_period
- Selected metrics: budget_amount, actual_amount, variance
```

**What happens:** The FP&A team can now call `SELECT * FROM TABLE(GET_BUDGET_VS_ACTUAL('CORP-HQ', '2025-Q1'))` to get the budget vs actual for a specific cost center and period.

### Step 6: Create the Allocation Summary view

```
Add a faux object to the Cost Allocation project:
- Name: V_ALLOCATION_SUMMARY
- Type: view
- Target Database: REPORTING
- Target Schema: FINANCE
- Selected dimensions: allocation_method, cost_center, fiscal_period
- Selected metrics: allocation_total, actual_amount
- Comment: "Allocation method summary view"
```

**What happens:** A focused view is created showing allocation methods with their totals. This is useful for validating that allocations balance correctly.

### Step 7: Create a procedure with static WHERE

```
Add a faux object to the Cost Allocation project:
- Name: GET_OPEX_ONLY
- Type: stored_procedure
- Target Database: REPORTING
- Target Schema: FINANCE
- Parameters:
  - DEPARTMENT (VARCHAR, default: NULL)
- Where clause: "cost_centers.cost_center LIKE 'OPEX%'"
- Selected dimensions: cost_center, department
- Selected metrics: budget_amount, actual_amount, variance
```

**What happens:** This procedure combines a **static WHERE** filter (only OPEX cost centers) with a **dynamic parameter** (department). The generated Python code will always include the OPEX filter and optionally add the department filter.

### Step 8: Generate scripts

```
Generate all faux object scripts for the Cost Allocation project
```

---

## What Did We Find?

### CASE WHEN in Generated SQL

The CASE WHEN expressions are preserved exactly as defined:

```sql
METRICS gl_transactions.budget_amount AS
    SUM(CASE WHEN transaction_type = 'BUDGET' THEN transaction_amount ELSE 0 END)
    COMMENT = 'Total budgeted amount',
gl_transactions.variance AS
    SUM(CASE WHEN transaction_type = 'BUDGET' THEN ...) -
    SUM(CASE WHEN transaction_type = 'ACTUAL' THEN ...)
    COMMENT = 'Budget minus actual variance'
```

### Static WHERE + Dynamic Parameters

The GET_OPEX_ONLY procedure has both:
```python
def run(session, department=NULL):
    conditions = ["cost_centers.cost_center LIKE 'OPEX%'"]  # Always applied
    if department is not None:
        conditions.append(f"DEPARTMENT = '{department}'")    # Optional
    where = 'WHERE ' + ' AND '.join(conditions)
```

---

## Understanding Budget vs Actual Patterns

| Pattern | What It Means |
|---------|--------------|
| **CASE WHEN ... BUDGET** | Filters transactions to only budget entries |
| **CASE WHEN ... ACTUAL** | Filters transactions to only actual entries |
| **Variance = Budget - Actual** | Positive = under budget (favorable), Negative = over budget |
| **allocation_total** | Sum of ALL transactions regardless of type |

---

## Bonus Challenge

Try creating a **Dynamic Table** that shows real-time budget utilization:

```
Add a faux object to the Cost Allocation project:
- Name: DT_BUDGET_TRACKER
- Type: dynamic_table
- Target Database: REPORTING
- Target Schema: FINANCE
- Warehouse: FINANCE_WH
- Target Lag: 1 hour
- Comment: "Real-time budget utilization tracker"
```

---

## What's Next?

Now try [Use Case 16: Manufacturing Analyst](../16_manufacturing_analyst/README.md) to build
a plant operations semantic view with variance analysis and INT data types!
