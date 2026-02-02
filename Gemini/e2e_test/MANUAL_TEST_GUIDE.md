# Manual End-to-End Test Guide for DataBridge AI

This guide walks you through a complete end-to-end workflow to test the functionality of the DataBridge AI platform, from creating a hierarchy in Librarian to analyzing data with it in Researcher.

**Prerequisites:**
*   You have installed the necessary dependencies for both the `v3` and `v4` applications.
*   You are in the root directory of the `Databridge_AI` project.
*   The sample data has been generated in the `Gemini/e2e_test/data` directory.

---

## Part 1: Librarian - Hierarchy Setup and Management

In this part, we will use the `v3` application to create a project, import our P&L hierarchy, and map it to our general ledger accounts.

### Step 1: Create a New Project

First, we need a project to house our hierarchy.

```bash
# Assuming the unified CLI from the suggestions is not yet implemented,
# we will use the v3 CLI directly.
# (If a unified CLI 'databridge' exists, use that instead)

python v3/src/main.py project create "E2E Financials" --description "End-to-end test project"
```
**Expected Output:** You should see a success message with the details of the newly created project, including its ID. **Copy the project ID** for the next steps.

### Step 2: Import the Hierarchy

Now, import the sample P&L hierarchy from the CSV file.

```bash
# Replace <project-id> with the ID you copied in the previous step.
python v3/src/main.py csv import hierarchy <project-id> Gemini/e2e_test/data/e2e_hierarchy.csv
```
**Expected Output:** A success message indicating that the hierarchy has been imported.

### Step 3: Verify the Hierarchy

Let's view the hierarchy as a tree to ensure it was imported correctly.

```bash
python v3/src/main.py hierarchy tree <project-id>
```
**Expected Output:** You should see a tree structure representing the P&L:
```
- Net Income
  - Total Revenue
    - Product Revenue
    - Service Revenue
  - Total Expenses
    - Cost of Goods Sold
    - Operating Expenses
      - Sales & Marketing
      - General & Administrative
```

### Step 4: Import the Source Mappings

Next, we need to import the mappings that link our GL accounts to the hierarchy nodes.

```bash
python v3/src/main.py csv import mapping <project-id> Gemini/e2e_test/data/e2e_mapping.csv
```
**Expected Output:** A success message indicating that the mappings have been imported.

### Step 5: Verify the Mappings

You can view the mappings for a specific hierarchy node to verify the import. Let's check the mappings for "Product Revenue" (ID `11`).

```bash
python v3/src/main.py mapping list 11
```
**Expected Output:** A table showing the two GL accounts (`4000` and `4010`) mapped to this node.

---

## Part 2: Researcher - Data Analysis

With the hierarchy set up in Librarian, we can now use the Researcher analytics engine to query our transactional data. Researcher's "compute pushdown" will simulate connecting to a database and running queries. For this test, we will point it to our CSV file.

*Note: The Researcher application is designed to connect to data warehouses. For this test, we assume a connector that can query CSV files directly (like one using DuckDB or Polars) is available or that the data is loaded into a local test database (e.g., the sample PostgreSQL DB from the `v4/docker` environment).*

### Step 1: Set up a "Database" Connection

First, configure a connection in Researcher that points to our sample data. In a real-world test, you would load `e2e_transactions.csv` into a database and connect to it. For this manual guide, we will assume a connection named `e2e_db` has been configured to read from our CSV files.

*(This step is illustrative, as a CSV connector might not be a default feature. If using the provided Docker environment, you would use `psql` or `pgAdmin` to load the CSVs into the `databridge-analytics-db` and then connect Researcher to that).*

### Step 2: Run an Analytical Query

Now, let's ask a business question using the Researcher natural language query interface. This query will use the hierarchy from Librarian to aggregate the data from `e2e_transactions.csv`.

**Question:** "What was our total Product Revenue in January 2024?"

```bash
# This command assumes a connection named 'e2e_db' has been configured
# to point to our sample CSV data.
python v4/src/main.py query ask e2e_db "What was our total Product Revenue in January 2024?"
```

### How This Works End-to-End:

1.  The Researcher `query ask` command parses the question.
2.  It identifies "Product Revenue" as a business term.
3.  It queries the Librarian knowledge base (or a synced version of it) to understand what "Product Revenue" means. It finds that "Product Revenue" corresponds to hierarchy ID `11`.
4.  It then finds the mappings for hierarchy ID `11`, which are `account_id` `4000` and `4010`.
5.  It identifies "January 2024" as a time filter.
6.  Researcher's query engine constructs a SQL query that it "pushes down" to the connected data source (`e2e_db`). The query will look something like this:
    ```sql
    SELECT SUM(amount)
    FROM transactions
    WHERE account_id IN ('4000', '4010')
      AND date >= '2024-01-01'
      AND date < '2024-02-01';
    ```
7.  The query is executed against the `e2e_transactions.csv` data.
8.  The result is returned to the user.

**Expected Output:**
Based on our sample data, the query should sum the amounts for transactions with account IDs `4000` and `4010` in January.
*   Transaction 1 (account 4000): 15000
*   Transaction 2 (account 4010): 8500
*   **Total: 23500**

The CLI should display a result like:
> Total Product Revenue for January 2024 was **23,500**.

---

This completes the end-to-end test, demonstrating that a hierarchy created and managed in Librarian can be seamlessly used to perform meaningful, context-aware analysis in Researcher.
