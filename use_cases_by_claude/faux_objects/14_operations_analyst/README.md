# Use Case 14: Operations Analyst - Geographic Operations Semantic View

## The Story

You are an **Operations Analyst** supporting the COO's office. Your company operates across multiple regions, countries, and cities. You need to track **headcount**, **square footage**, and **space utilization** across the enterprise.

Every Monday morning, the leadership team expects a fresh **operations report** on their desks. Currently, someone manually runs a query and emails an Excel file. You want to automate this with a **Snowflake Task** that materializes the data every Monday at 8am, plus a **view** for ad-hoc regional analysis.

---

## What You Will Learn

- How to build a semantic view for **geographic operations data**
- How to create a **standard view** for regional summary reporting
- How to create a **Snowflake Task** with a CRON schedule for weekly materialization
- How to create **filtered views** by region
- How tasks generate both a **materializer procedure** and a **task** with auto-resume

---

## Tools Used

| Tool | What It Does |
|------|-------------|
| `create_faux_project` | Creates a project for geographic operations |
| `define_faux_semantic_view` | Defines the geo operations semantic view |
| `add_faux_semantic_table` | Adds FACT_OPERATIONS, DIM_LOCATION, DIM_DEPARTMENT, DIM_ASSET |
| `add_faux_semantic_column` | Adds dimensions (region, country), facts (headcount, sqft), and metrics |
| `add_faux_object` | Creates view and task wrappers |
| `generate_faux_scripts` | Generates SQL for all objects |

---

## Step-by-Step Instructions

### Step 1: Create the project

```
Create a faux objects project called "Geographic Operations Wrappers" with description
"Regional operations metrics for executive dashboards"
```

**What happens:** A project container is created for all geographic operations faux objects.

### Step 2: Define the semantic view

```
Define the semantic view for the Geographic Operations project with:
- Name: geo_operations
- Database: ENTERPRISE
- Schema: SEMANTIC
- Comment: "Geographic operations for regional performance tracking"
```

**What happens:** The semantic view metadata is established. No AI SQL generation context is needed for this straightforward operations model.

### Step 3: Add tables and relationships

```
Add these tables:
1. operations -> ENTERPRISE.DW.FACT_OPERATIONS (PK: operation_id)
2. locations -> ENTERPRISE.DW.DIM_LOCATION (PK: location_id)
3. departments -> ENTERPRISE.DW.DIM_DEPARTMENT (PK: department_id)
4. assets -> ENTERPRISE.DW.DIM_ASSET (PK: asset_id)

And relationships:
1. operations.location_id -> locations
2. operations.department_id -> departments
3. operations.asset_id -> assets
```

**What happens:** The four-table operational model is configured with proper join paths.

### Step 4: Add dimensions, facts, and metrics

```
Add dimensions:
1. region (VARCHAR) from locations - synonym: "geographic region"
2. country (VARCHAR) from locations
3. city (VARCHAR) from locations
4. department (VARCHAR) from departments
5. asset_class (VARCHAR) from assets

Add facts:
1. headcount (INT) from operations
2. square_footage (FLOAT) from operations

Add metrics:
1. total_headcount (INT) = SUM(headcount) - "Total employee headcount"
2. total_sqft (FLOAT) = SUM(square_footage) - "Total occupied square footage"
3. utilization_rate (FLOAT) = SUM(headcount) / NULLIF(SUM(square_footage) / 150, 0) - "Space utilization rate (150 sqft per person standard)"
```

**What happens:** The dimensional model includes an INT fact type (headcount) and a utilization metric that divides headcount by available capacity.

### Step 5: Create the Regional Summary view

```
Add a faux object to the Geographic Operations project:
- Name: V_REGIONAL_SUMMARY
- Type: view
- Target Database: REPORTING
- Target Schema: OPS
- Comment: "Regional operations summary for executive dashboard"
```

**What happens:** A view is configured that includes all dimensions and metrics. Power BI and Tableau can connect to this directly.

### Step 6: Create the Weekly Operations Report task

```
Add a faux object to the Geographic Operations project:
- Name: WEEKLY_OPS_REPORT
- Type: task
- Target Database: REPORTING
- Target Schema: OPS
- Warehouse: ANALYTICS_WH
- Schedule: "USING CRON 0 8 * * 1 America/Chicago"
- Materialized Table: REPORTING.OPS.WEEKLY_OPS_REPORT_MAT
- Comment: "Weekly operations report materialized every Monday at 8am"
```

**What happens:** DataBridge configures a task that will:
1. Create a **materializer stored procedure** that queries the semantic view and writes to a table
2. Create a **Snowflake Task** on a CRON schedule (Monday at 8am Central)
3. Generate an `ALTER TASK ... RESUME` to activate the task

### Step 7: Generate the SQL

```
Generate all faux object scripts for the Geographic Operations project
```

**What happens:** SQL is generated for the semantic view DDL, the regional summary view, and the weekly task (which includes the materializer procedure, the task itself, and the resume command).

---

## What Did We Find?

### Weekly Task SQL Structure

The task generates **three SQL statements**:

```sql
-- Step 1: Materializer Procedure
CREATE OR REPLACE PROCEDURE REPORTING.OPS.WEEKLY_OPS_REPORT_MATERIALIZER()
RETURNS TABLE(status VARCHAR, row_count INT, refreshed_at TIMESTAMP_NTZ)
LANGUAGE PYTHON
...
AS
$$
def run(session):
    query = """
        SELECT * FROM SEMANTIC_VIEW(
            ENTERPRISE.SEMANTIC.geo_operations
            DIMENSIONS locations.region, locations.country, ...
            METRICS operations.total_headcount, ...
        )
    """
    df = session.sql(query)
    row_count = df.count()
    df.write.mode('overwrite').save_as_table('REPORTING.OPS.WEEKLY_OPS_REPORT_MAT')
    ...
$$;

-- Step 2: Scheduled Task
CREATE OR REPLACE TASK REPORTING.OPS.WEEKLY_OPS_REPORT_REFRESH
    WAREHOUSE = ANALYTICS_WH
    SCHEDULE = 'USING CRON 0 8 * * 1 America/Chicago'
AS
CALL REPORTING.OPS.WEEKLY_OPS_REPORT_MATERIALIZER();

-- Step 3: Activate
ALTER TASK REPORTING.OPS.WEEKLY_OPS_REPORT_REFRESH RESUME;
```

### CRON Schedule Explained

`0 8 * * 1 America/Chicago` means:
- **0** - minute 0
- **8** - hour 8 (8:00 AM)
- __*__ - any day of the month
- __*__ - any month
- **1** - Monday (day of week)
- **America/Chicago** - Central time zone

---

## Understanding Tasks vs Dynamic Tables

| Feature | Task | Dynamic Table |
|---------|------|---------------|
| **Refresh** | On a CRON schedule you control | Automatic based on target_lag |
| **Output** | A regular materialized table | A special dynamic table object |
| **Control** | Full control over timing | Snowflake decides when to refresh |
| **Best for** | Batch reports, specific timing needs | Dashboards needing near-real-time data |
| **Generated SQL** | 3 statements (proc + task + resume) | 1 statement |

---

## Bonus Challenge

Try creating a view filtered to just **North America**:

```
Add a faux object to the Geographic Operations project:
- Name: V_NORTH_AMERICA
- Type: view
- Target Database: REPORTING
- Target Schema: OPS
- Where clause: "locations.region = 'North America'"
- Selected dimensions: country, city, department
- Selected metrics: total_headcount, utilization_rate
```

---

## What's Next?

Now try [Use Case 15: Cost Analyst](../15_cost_analyst/README.md) to build
a cost allocation semantic view with budget vs actual analysis!
