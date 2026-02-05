# Use Case 5: Integrated Variance Analysis

This use case demonstrates how the `Book`, `Librarian`, and `Researcher` components can be used together to perform a comprehensive variance analysis.

## Workflow Overview

1.  **`Librarian`:** A standard financial reporting hierarchy (Income Statement) is defined and stored in the `Librarian`. This acts as the master structure for the analysis.
2.  **`Book` (Data Ingestion):**
    *   Actual financial results are sourced from an SEC filing and loaded into a `Book` object.
    *   A corresponding budget is loaded from a CSV file into a second `Book` object.
3.  **`Researcher` (Analysis):** A script, acting as the `Researcher`, takes the master hierarchy and the two `Book` objects as input. It then performs a variance analysis by comparing the actual and budget amounts for each account in the hierarchy.

## Files

*   `download_sec_data.py`: A script to download financial data from the SEC. For this use case, it creates a dummy `actuals.csv` file.
*   `actuals.csv`: A CSV file containing the actual financial results (dummy data).
*   `budget.csv`: A CSV file containing the budget data.
*   `setup_librarian.py`: A script that simulates the creation of the income statement hierarchy in the `Librarian`.
*   `run_analysis.py`: The main script that performs the variance analysis.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Prepare the Data

First, run the `download_sec_data.py` script to create the `actuals.csv` file. This simulates the process of getting real-world data from the SEC.

```bash
python download_sec_data.py
```

### 3. Run the Analysis

Now, run the `run_analysis.py` script to perform the variance analysis.

```bash
python run_analysis.py
```

### 4. What's Happening?

1.  **Load Master Hierarchy:** The `run_analysis.py` script starts by calling `create_income_statement_hierarchy()` from the `setup_librarian` module. This simulates the `Researcher` fetching the master hierarchy from the `Librarian`.
2.  **Load Data into Books:** The script then loads the `actuals.csv` and `budget.csv` files into two separate `Book` objects. This demonstrates the `Book` library's role as a flexible, in-memory data container.
3.  **Perform Variance Analysis:** The script (acting as the `Researcher`) iterates through the master hierarchy. For each account, it finds the corresponding nodes in the `actuals` and `budget` books, calculates the variance, and creates a new `Node` in a `variance_report` book.
4.  **Print Report:** Finally, the script prints a formatted variance analysis report, showing the actuals, budget, and variance for each account.

### Expected Output

```
INFO:__main__:Starting variance analysis use case...
INFO:__main__:Loading hierarchy from Librarian...
INFO:__main__:Loading actuals and budget data...
INFO:__main__:Performing variance analysis...
INFO:__main__:Variance Analysis Report:
Account                   Actual          Budget        Variance
----------------------------------------------------------------------
Revenue                   $383,285,000,000 $400,000,000,000 $-16,715,000,000
Cost of Revenue           $214,137,000,000 $220,000,000,000 $-5,863,000,000
Gross Profit              $169,148,000,000 $180,000,000,000 $-10,852,000,000
Operating Expenses        $54,847,000,000  $60,000,000,000  $-5,153,000,000
Operating Income          $114,301,000,000 $120,000,000,000 $-5,699,000,000
```

This integrated use case demonstrates the power of the Databridge AI ecosystem. The `Librarian` provides the stable, centralized master data; the `Book` library provides the flexibility to ingest and structure ad-hoc data; and the `Researcher` performs the complex analytics that bring it all together.
