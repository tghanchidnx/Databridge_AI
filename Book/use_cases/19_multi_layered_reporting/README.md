# Use Case 19: Multi-layered Financial Reporting

This use case demonstrates how to use the `Book` library's formula engine to create a complex, multi-layered financial report with dependencies between calculations. This is a common requirement for financial statements like an income statement where you have multiple levels of subtotals.

## Features Highlighted

*   **Advanced Formula Usage:** Shows how to create multiple formula-based nodes.
*   **Formula Dependencies:** Demonstrates how the result of one formula can be used as an input for another.
*   **Hierarchical Reporting:** Creates a structured, multi-level report from flat data.

## Components Involved

*   **`Book` Library:** Used to structure the report and perform the multi-layered calculations.

## Files

*   `detailed_trial_balance.csv`: A sample CSV file containing a more detailed trial balance with accounts needed for a multi-layered income statement.
*   `generate_multi_layered_report.py`: The Python script that builds the report and executes the formulas.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Run the Report Generation Script

Navigate to the `Book/use_cases/19_multi_layered_reporting` directory and run the `generate_multi_layered_report.py` script:

```bash
python generate_multi_layered_report.py
```

### 3. What's Happening?

1.  **Load Data:** The script loads the trial balance data from `detailed_trial_balance.csv`.
2.  **Define Hierarchy:** It manually defines a multi-layered hierarchy for the income statement, including nodes for `Total Revenue`, `Total Cost of Goods Sold`, `Gross Profit`, `Total Operating Expenses`, `Operating Income`, and `Net Income`.
3.  **Add Formulas with Dependencies:** The script attaches `Formula` objects to the calculated nodes (`Gross Profit`, `Operating Income`, `Net Income`). Crucially, the formula for `Operating Income` depends on the result of the `Gross Profit` calculation, and `Net Income` depends on `Operating Income`.
4.  **Execute Formulas in Order:** The `execute_formulas` function is called for each calculated node in the correct order to ensure that the dependencies are met. The result of each calculation is passed as an input to the next.
5.  **Assemble and Print Report:** The final report `Book` is assembled, and the script prints a formatted, multi-layered income statement.

### Expected Output

You should see output similar to this in your terminal:

```
INFO:__main__:Starting multi-layered financial reporting use case...
INFO:__main__:Loading detailed trial balance data...
INFO:__main__:Defining the multi-layered hierarchy...
INFO:__main__:Adding formulas with dependencies...
INFO:__main__:Executing formulas in order...

--- Multi-layered Income Statement ---
Total Revenue:
  Product Revenue:           $1,000,000.00
  Service Revenue:           $250,000.00
Total Cost of Goods Sold:
  Product COGS:              $400,000.00
  Service COGS:              $100,000.00
Gross Profit                   $750,000.00
Total Operating Expenses:
  Sales & Marketing:         $150,000.00
  Research & Development:    $100,000.00
  General & Administrative:  $50,000.00
Operating Income               $450,000.00
Total Non-operating Income/Expense:
  Interest Income:           $10,000.00
  Interest Expense:          $-5,000.00
Net Income                     $455,000.00

INFO:__main__:
Multi-layered reporting use case completed.
```

This use case demonstrates how the `Book` library's formula engine can handle complex, multi-step calculations, making it a powerful tool for financial modeling and reporting.
