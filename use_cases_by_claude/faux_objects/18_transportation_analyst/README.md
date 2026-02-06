# Use Case 18: Transportation Analyst - Fleet Operations Semantic View

## The Story

You are a **Transportation Analyst** at a trucking company managing a fleet of hundreds of trucks across dozens of terminals. Every day, you need to track **revenue per mile**, **cost per mile**, **operating ratio**, and **utilization rate** to identify which lanes are profitable and which drivers are most efficient.

Your data is spread across **five tables**: trips (the fact table), drivers, equipment, lanes, and terminals. This is the most complex join pattern in the series. The dispatch team needs a **daily fleet report** materialized at 5am, the network planning team wants a **lane profitability view**, and management wants a **driver stats procedure** with DATE parameters for flexible date range queries.

---

## What You Will Learn

- How to build a semantic view with **5 table joins** (the most complex join pattern)
- How to create a **Snowflake Task** with a daily 5am CRON schedule
- How to create a **lane profitability view** for network planning
- How to create a **stored procedure with DATE parameters**
- How to **export individual SQL files** for deployment pipelines

---

## Tools Used

| Tool | What It Does |
|------|-------------|
| `create_faux_project` | Creates a project for fleet operations |
| `define_faux_semantic_view` | Defines the fleet operations semantic view |
| `add_faux_semantic_table` | Adds 5 tables: FACT_TRIPS, DIM_DRIVER, DIM_EQUIPMENT, DIM_LANE, DIM_TERMINAL |
| `add_faux_semantic_column` | Adds complex metrics like operating_ratio and cost_per_mile |
| `add_faux_semantic_relationship` | Defines 4 relationships from the trips fact table |
| `add_faux_object` | Creates task, view, and procedure wrappers |
| `export_faux_scripts` | Exports individual .sql files for CI/CD |

---

## Step-by-Step Instructions

### Step 1: Create the project

```
Create a faux objects project called "Fleet Operations Wrappers" with description
"Fleet operations and lane profitability for logistics"
```

### Step 2: Define the semantic view

```
Define the semantic view for the Fleet Operations project with:
- Name: fleet_operations
- Database: LOGISTICS
- Schema: SEMANTIC
- Comment: "Fleet operations for trucking and logistics companies"
- AI SQL Generation: "Operating ratio = total cost / revenue. Below 100% means profitable."
```

**What happens:** The AI context explains the key industry metric -- operating ratio. In trucking, a ratio below 100% means the company is profitable on that lane/driver.

### Step 3: Add 5 tables

```
Add tables:
1. trips -> LOGISTICS.DW.FACT_TRIPS (PK: trip_id)
2. drivers -> LOGISTICS.DW.DIM_DRIVER (PK: driver_id)
3. equipment -> LOGISTICS.DW.DIM_EQUIPMENT (PK: equipment_id)
4. lanes -> LOGISTICS.DW.DIM_LANE (PK: lane_id)
5. terminals -> LOGISTICS.DW.DIM_TERMINAL (PK: terminal_id)
```

**What happens:** Five tables are added -- the most complex data model in the tutorial series.

### Step 4: Add relationships

```
Add relationships:
1. trips.driver_id -> drivers
2. trips.equipment_id -> equipment
3. trips.lane_id -> lanes
4. trips.origin_terminal_id -> terminals
```

**What happens:** Four foreign key relationships connect the trips fact table to all dimension tables.

### Step 5: Add dimensions and facts

```
Add dimensions:
1. driver_name (VARCHAR) from drivers - synonyms: "driver", "operator"
2. equipment_type (VARCHAR) from equipment - synonyms: "truck type", "asset type"
3. origin_terminal (VARCHAR) from terminals
4. destination_terminal (VARCHAR) from lanes
5. lane_id (VARCHAR) from lanes

Add facts:
1. loaded_miles (FLOAT) from trips
2. empty_miles (FLOAT) from trips
3. fuel_gallons (FLOAT) from trips
4. freight_revenue (FLOAT) from trips
```

### Step 6: Add complex transportation metrics

```
Add metrics:
1. revenue_per_mile (FLOAT) = SUM(freight_revenue) / NULLIF(SUM(loaded_miles + empty_miles), 0) - "Revenue per total mile"
2. cost_per_mile (FLOAT) = (SUM(fuel_gallons) * 3.50 + SUM(loaded_miles + empty_miles) * 0.15) / NULLIF(SUM(loaded_miles + empty_miles), 0) - "Cost per mile (fuel at $3.50/gal + $0.15/mile maintenance)"
3. operating_ratio (FLOAT) = (SUM(fuel_gallons) * 3.50 + SUM(loaded_miles + empty_miles) * 0.15) / NULLIF(SUM(freight_revenue), 0) * 100 - "Operating ratio percentage (below 100 = profitable)"
4. utilization_rate (FLOAT) = SUM(loaded_miles) / NULLIF(SUM(loaded_miles + empty_miles), 0) * 100 - "Loaded miles as percentage of total miles"
```

**What happens:** Four metrics capture fleet efficiency:
- **Revenue per mile** -- How much we earn per mile driven
- **Cost per mile** -- How much it costs per mile (fuel + maintenance)
- **Operating ratio** -- Total cost / total revenue (below 100% is profitable)
- **Utilization rate** -- What percentage of miles are loaded (vs empty/deadhead)

### Step 7: Create the Daily Fleet Report task

```
Add a faux object to the Fleet Operations project:
- Name: DAILY_FLEET_REPORT
- Type: task
- Target Database: REPORTING
- Target Schema: LOGISTICS
- Warehouse: LOGISTICS_WH
- Schedule: "USING CRON 0 5 * * * America/Chicago"
- Materialized Table: REPORTING.LOGISTICS.DAILY_FLEET_REPORT_MAT
- Comment: "Daily fleet operations report materialized at 5am"
```

**What happens:** A task is configured that runs at 5am daily. The dispatch team queries `REPORTING.LOGISTICS.DAILY_FLEET_REPORT_MAT` each morning for their daily operations briefing.

### Step 8: Create the Lane Profitability view

```
Add a faux object to the Fleet Operations project:
- Name: V_LANE_PROFITABILITY
- Type: view
- Target Database: REPORTING
- Target Schema: LOGISTICS
- Selected dimensions: lane_id, origin_terminal, destination_terminal
- Selected metrics: revenue_per_mile, cost_per_mile, operating_ratio
- Selected facts: loaded_miles, freight_revenue
- Comment: "Lane-level profitability analysis"
```

**What happens:** A focused view for the network planning team showing profitability by lane. They use this to decide which lanes to add or drop.

### Step 9: Create the Driver Stats procedure with DATE params

```
Add a faux object to the Fleet Operations project:
- Name: GET_DRIVER_STATS
- Type: stored_procedure
- Target Database: REPORTING
- Target Schema: LOGISTICS
- Parameters:
  - DRIVER_ID (VARCHAR, default: NULL)
  - START_DATE (DATE, default: NULL)
  - END_DATE (DATE, default: NULL)
- Selected dimensions: driver_name, equipment_type
- Selected facts: loaded_miles, empty_miles, fuel_gallons, freight_revenue
- Selected metrics: revenue_per_mile, utilization_rate
```

**What happens:** A procedure with DATE-type parameters is configured. Management can call `SELECT * FROM TABLE(GET_DRIVER_STATS('D-1234', '2025-01-01'::DATE, '2025-03-31'::DATE))` for quarterly driver reviews.

### Step 10: Export individual SQL files

```
Export the faux object scripts for the Fleet Operations project to a local directory
```

**What happens:** Individual .sql files are created for each object plus a complete deployment bundle:
- `logistics_semantic_fleet_operations_view.sql`
- `reporting_logistics_daily_fleet_report_task.sql`
- `reporting_logistics_v_lane_profitability_view.sql`
- `reporting_logistics_get_driver_stats_stored_procedure.sql`
- `deployment_bundle.sql`

These files can be checked into Git and deployed through your CI/CD pipeline.

---

## What Did We Find?

### 5-Table DDL

The semantic view DDL includes all 5 tables and 4 relationships:

```sql
CREATE OR REPLACE SEMANTIC VIEW LOGISTICS.SEMANTIC.fleet_operations
    COMMENT = 'Fleet operations for trucking and logistics companies'
    AI_SQL_GENERATION = 'Operating ratio = total cost / revenue. Below 100% means profitable.'

    TABLES (
        trips AS LOGISTICS.DW.FACT_TRIPS PRIMARY KEY (trip_id),
        drivers AS LOGISTICS.DW.DIM_DRIVER PRIMARY KEY (driver_id),
        equipment AS LOGISTICS.DW.DIM_EQUIPMENT PRIMARY KEY (equipment_id),
        lanes AS LOGISTICS.DW.DIM_LANE PRIMARY KEY (lane_id),
        terminals AS LOGISTICS.DW.DIM_TERMINAL PRIMARY KEY (terminal_id)
    )

    RELATIONSHIPS (
        trips (driver_id) REFERENCES drivers,
        trips (equipment_id) REFERENCES equipment,
        trips (lane_id) REFERENCES lanes,
        trips (origin_terminal_id) REFERENCES terminals
    )
    ...
```

### DATE Parameters in Procedure

```sql
CREATE OR REPLACE PROCEDURE REPORTING.LOGISTICS.GET_DRIVER_STATS(
    DRIVER_ID VARCHAR DEFAULT NULL,
    START_DATE DATE DEFAULT NULL,
    END_DATE DATE DEFAULT NULL
)
RETURNS TABLE(
    DRIVER_NAME VARCHAR,
    EQUIPMENT_TYPE VARCHAR,
    LOADED_MILES FLOAT,
    EMPTY_MILES FLOAT,
    FUEL_GALLONS FLOAT,
    FREIGHT_REVENUE FLOAT,
    REVENUE_PER_MILE FLOAT,
    UTILIZATION_RATE FLOAT
)
...
```

---

## Understanding Fleet Metrics

| Metric | Good Value | What It Means |
|--------|-----------|---------------|
| **Revenue/Mile** | > $2.50 | Higher is better -- more revenue per mile driven |
| **Cost/Mile** | < $1.80 | Lower is better -- cheaper to operate |
| **Operating Ratio** | < 90% | Lower is better -- more profit margin |
| **Utilization Rate** | > 85% | Higher is better -- fewer empty/deadhead miles |

> **Industry benchmark:** An operating ratio below 95% is considered efficient. Top carriers achieve 85-90%.

---

## Bonus Challenge

Try exporting the scripts and reviewing the deployment bundle:

```
Generate the deployment bundle for the Fleet Operations project
```

Look at the deployment order -- the semantic view DDL comes first, then the task (with materializer + scheduler + resume), then the view, then the procedure. This order ensures dependencies are satisfied.

---

## What's Next?

Congratulations! You've completed all 7 persona tutorials. Here's what you've built:

| # | Persona | Semantic View | Faux Objects |
|---|---------|---------------|--------------|
| 12 | Financial Analyst | GL Reconciliation | 3 views + 1 procedure |
| 13 | Oil & Gas Analyst | Drilling Economics | 1 procedure + 1 dynamic table |
| 14 | Operations Analyst | Geographic Operations | 1 view + 1 task |
| 15 | Cost Analyst | Cost Allocation | 1 view + 2 procedures |
| 16 | Manufacturing Analyst | Plant Operations | 1 dynamic table + 1 view |
| 17 | SaaS Analyst | Subscription Metrics | 1 procedure + 1 dynamic table |
| 18 | Transportation Analyst | Fleet Operations | 1 task + 1 view + 1 procedure |

Go back to the [Faux Objects landing page](../README.md) for the full coverage matrix and testing report.
