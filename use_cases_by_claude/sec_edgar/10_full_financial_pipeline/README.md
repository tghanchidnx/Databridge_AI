# Use Case 10: Full Financial Pipeline

## The Story

You just got promoted to **Data Engineer** at a financial services company! Your
first big project: build a **complete data pipeline** for Apple's financial data.

A data pipeline is like an assembly line in a factory:
1. **Load** the raw data
2. **Check** its quality
3. **Organize** it into a hierarchy
4. **Export** the results
5. **Record** every step so you can prove what you did

Every step must be tracked in an **audit trail** - a log that records who did what
and when. This is important in finance because auditors need to verify that
everything was done correctly.

---

## What You Will Learn

- How to build an **end-to-end data pipeline** from raw CSV to organized hierarchy
- How to use **workflow recording** to track every step
- How to **configure project defaults** for source database settings
- How to **export** hierarchies to CSV
- How to use the **audit trail** to see a history of everything that happened
- How real data engineers think about **data pipelines**

---

## Tools Used

| Tool | What It Does |
|------|-------------|
| `load_csv` | Load the raw financial data |
| `profile_data` | Check data quality |
| `save_workflow_step` | Record each pipeline step |
| `create_hierarchy_project` | Create the hierarchy project |
| `configure_project_defaults` | Set source database/schema/table defaults |
| `import_flexible_hierarchy` | Build the hierarchy from CSV |
| `get_hierarchy_tree` | View the result |
| `export_hierarchy_csv` | Export the hierarchy to a CSV file |
| `get_workflow` | See all recorded workflow steps |
| `get_audit_log` | See the complete audit trail |

**Components used:** Books (full pipeline), Librarian (project mgmt, export), Workflow engine

---

## Step-by-Step Instructions

### Step 1: Start the Workflow - Load Data

First, load the full chart of accounts:

```
Load the CSV file at data/apple_full_chart_of_accounts.csv and call it "apple_coa"
```

Now record this step in the workflow:

```
Save a workflow step: step name "load_data", description "Loaded Apple full chart of accounts from SEC EDGAR data. Contains both Income Statement and Balance Sheet accounts.", status "completed"
```

### Step 2: Quality Check

Profile the data to check for issues:

```
Profile the data quality of the apple_coa data
```

Record the quality check:

```
Save a workflow step: step name "quality_check", description "Profiled Apple chart of accounts. Found 20 accounts across Income Statement and Balance Sheet. No missing values - SEC data is clean. Amount range from negative $19.2B to $391B.", status "completed"
```

### Step 3: Create the Hierarchy Project

```
Create a hierarchy project called "Apple Full COA Pipeline" with description "Complete Apple chart of accounts hierarchy built via automated pipeline from SEC EDGAR data"
```

Record the step (use the project ID you received):

```
Save a workflow step: step name "create_project", description "Created hierarchy project 'Apple Full COA Pipeline' to hold the organized chart of accounts.", status "completed"
```

### Step 4: Configure Project Defaults

Set up the source database information (where this data would live in a real system):

```
Configure project defaults for project YOUR_PROJECT_ID with source database "APPLE_WAREHOUSE", source schema "FINANCE", source table "GL_ACCOUNTS", source column "ACCOUNT_CODE"
```

Record it:

```
Save a workflow step: step name "configure_defaults", description "Configured source defaults: database=APPLE_WAREHOUSE, schema=FINANCE, table=GL_ACCOUNTS, column=ACCOUNT_CODE", status "completed"
```

### Step 5: Build the Hierarchy

Import the data into the hierarchy:

```
Import the flexible hierarchy from data/apple_balance_sheet_tier1.csv into project YOUR_PROJECT_ID
```

Record it:

```
Save a workflow step: step name "import_hierarchy", description "Imported Apple balance sheet accounts using Tier 1 format. Created hierarchy groups for Assets, Liabilities, and Equity.", status "completed"
```

### Step 6: View the Result

```
Show me the hierarchy tree for project YOUR_PROJECT_ID
```

Record it:

```
Save a workflow step: step name "verify_tree", description "Verified hierarchy tree structure. All accounts properly organized under parent groups.", status "completed"
```

### Step 7: Export the Hierarchy

```
Export the hierarchy CSV for project YOUR_PROJECT_ID
```

Record it:

```
Save a workflow step: step name "export_csv", description "Exported hierarchy to CSV file for deployment or review.", status "completed"
```

### Step 8: Review the Complete Workflow

Now let's see every step we recorded:

```
Show me the complete workflow
```

**What happens:** You'll see a timeline of all 7 steps with their names,
descriptions, and completion status. This is your pipeline documentation!

### Step 9: Check the Audit Trail

```
Show me the audit log
```

**What happens:** The audit log shows every action DataBridge took, including
timestamps. This is the "paper trail" that auditors love.

---

## What Did We Find?

### Our Pipeline Steps

| Step | Name | What We Did |
|------|------|-------------|
| 1 | load_data | Loaded 20 accounts from SEC EDGAR |
| 2 | quality_check | Verified data quality (no missing values) |
| 3 | create_project | Created a hierarchy project |
| 4 | configure_defaults | Set source database information |
| 5 | import_hierarchy | Built the tree from Tier 1 CSV |
| 6 | verify_tree | Confirmed the structure looks correct |
| 7 | export_csv | Exported for deployment |

### Key Insights

1. **Reproducibility:** Because we recorded every step, anyone can follow this
   workflow and get the same result. That's the power of a pipeline!

2. **Audit Trail:** The audit log proves exactly what happened and when. In real
   finance jobs, this is required by law (SOX compliance, internal audits).

3. **Pipeline Pattern:** Load → Quality Check → Transform → Export is the
   standard pattern for almost ALL data pipelines, not just financial ones.

4. **Project Defaults:** Configuring the source database means the hierarchy
   knows WHERE the data came from. When you deploy to Snowflake or another
   database, it will know which tables and columns to reference.

---

## Understanding Data Pipelines

A data pipeline has these stages:

```
┌──────────┐    ┌──────────┐    ┌───────────┐    ┌──────────┐    ┌──────────┐
│  INGEST  │───→│ VALIDATE │───→│ TRANSFORM │───→│  EXPORT  │───→│  DEPLOY  │
│ Load raw │    │ Check    │    │ Build     │    │ Generate │    │ Push to  │
│ data     │    │ quality  │    │ hierarchy │    │ CSV/SQL  │    │ database │
└──────────┘    └──────────┘    └───────────┘    └──────────┘    └──────────┘
     │               │               │               │               │
     └───────────────┴───────────────┴───────────────┴───────────────┘
                              AUDIT TRAIL
                    (every step is recorded)
```

We did steps 1-4 in this tutorial. Step 5 (Deploy) would push the hierarchy
to a real database like Snowflake - we'll leave that for production use!

---

## Bonus Challenge

Try adding one more step to the pipeline - profile the exported CSV to make
sure it looks right:

```
Load the exported hierarchy CSV and profile it to verify the export quality
```

Then record it:

```
Save a workflow step: step name "verify_export", description "Loaded and profiled the exported CSV to verify export quality.", status "completed"
```

Now check the workflow again to see all 8 steps:

```
Show me the complete workflow
```

---

## What's Next?

You built a real data pipeline!
Now try [Use Case 11: Wall Street Analyst](../11_wall_street_analyst/README.md) -
the capstone challenge where you use EVERYTHING you've learned to build a
comprehensive financial analysis!
