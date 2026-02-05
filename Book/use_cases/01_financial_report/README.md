# Use Case 1: Building a Financial Report

This use case demonstrates how to use the `Book` library to build a simple income statement from a CSV file. It highlights the following features:

*   Data ingestion from CSV.
*   Hierarchical data creation.
*   Formula-based calculations.

## Files

*   `trial_balance.csv`: The input data, a simple trial balance with account information.
*   `build_report.py`: The Python script that implements the use case.

## Step-by-Step Instructions

### 1. Set up the Environment

First, make sure you have the `Book` library and its dependencies installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Run the Script

Navigate to the `Book/use_cases/01_financial_report` directory and run the `build_report.py` script:

```bash
python build_report.py
```

### 3. What's Happening?

When you run the script, it performs the following steps:

1.  **Ingests Data:** The script reads the `trial_balance.csv` file and loads the data into a list of dictionaries.
2.  **Builds Hierarchy:** It then uses the `from_list` function to create a hierarchical structure of `Node` objects based on the parent-child relationships defined in the CSV.
3.  **Creates Book:** A `Book` object named "Income Statement" is created to hold the hierarchy.
4.  **Adds Formulas:** The script identifies the "Gross Margin" and "Operating Income" nodes and attaches `Formula` objects to them. These formulas define how to calculate the `amount` for these nodes.
5.  **Executes Formulas:** The `execute_formulas` function is called for the nodes with formulas. This function evaluates the expressions in the formulas and stores the results in the `properties` of the respective nodes.
6.  **Prints Report:** Finally, the script prints a simple, indented view of the income statement, showing the calculated amounts for "Gross Margin" and "Operating Income".

### Expected Output

You should see the following output in your terminal:

```
INFO:__main__:Starting financial report use case...
INFO:__main__:Ingesting data from trial_balance.csv...
INFO:__main__:Building the hierarchy from the ingested data...
INFO:__main__:Adding formulas for Gross Margin and Operating Income...
INFO:__main__:Executing formulas...
INFO:__main__:Financial Report:
Revenue: 
  Product Revenue: 500000
  Service Revenue: 100000
COGS: 
  Product COGS: 200000
  Service COGS: 50000
Gross Margin: 350000
Operating Expenses: 
  Sales & Marketing: 100000
  General & Administrative: 75000
Operating Income: 175000
```

This use case demonstrates how the `Book` library can be used to quickly and easily transform flat, tabular data into a meaningful, hierarchical report with calculated values.
