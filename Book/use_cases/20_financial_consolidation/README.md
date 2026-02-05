# Use Case 20: Full-Cycle Financial Consolidation

This capstone use case provides a comprehensive, end-to-end demonstration of how the `Librarian`, `Book`, and `Researcher` components work together to perform a financial consolidation for a company with multiple subsidiaries.

## Features Highlighted

*   **`Librarian` (Simulated):** Provides the master consolidation hierarchy, including accounts for intercompany eliminations.
*   **`Book` Library:** Used to ingest and structure the trial balance data from multiple subsidiaries.
*   **`Researcher` (Simulated):** Performs the complex consolidation logic, including aggregating data, handling eliminations, and calculating consolidated figures.
*   **Formula Engine:** Used to calculate subtotals (Gross Profit, Operating Income) in the final consolidated report.

## Components Involved

*   **`Librarian` (Simulated):** Manages the master consolidation hierarchy.
*   **`Book` Library:** Used to represent the financial data for each subsidiary and for the final consolidated report.
*   **`Researcher` (Simulated):** The `run_consolidation.py` script acts as the "Researcher," orchestrating the entire consolidation process.

## Files

*   `subsidiary_a_trial_balance.csv`: A CSV file containing the trial balance for Subsidiary A.
*   `subsidiary_b_trial_balance.csv`: A CSV file containing the trial balance for Subsidiary B.
*   `setup_librarian.py`: A script that *simulates* the creation of the master consolidation hierarchy in the `Librarian`.
*   `run_consolidation.py`: The Python script that performs the financial consolidation.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Run the Consolidation Script

Navigate to the `Book/use_cases/20_financial_consolidation` directory and run the `run_consolidation.py` script:

```bash
python run_consolidation.py
```

### 3. What's Happening?

1.  **Load Master Hierarchy (Librarian):** The script starts by loading the master consolidation hierarchy from the simulated `Librarian`.
2.  **Load Subsidiary Data (Book):** The trial balances for Subsidiary A and Subsidiary B are loaded into dictionaries.
3.  **Perform Consolidation (Researcher):** The script then performs the consolidation logic:
    *   It aggregates the corresponding accounts from both subsidiaries.
    *   It performs the intercompany eliminations by adjusting the `Consolidated Revenue` and `Consolidated COGS`.
4.  **Create Consolidated Report (Book):** A new `Book` object is created to hold the final consolidated report.
5.  **Calculate Formulas:** The script uses the formula engine to calculate the `Consolidated Gross Profit` and `Consolidated Operating Income`.
6.  **Print Report:** Finally, the script prints the fully consolidated financial report.

### Expected Output

You should see output similar to this in your terminal:

```
INFO:__main__:Starting financial consolidation use case...
INFO:__main__:Loading consolidation hierarchy from Librarian...
INFO:__main__:Loading subsidiary trial balances...
INFO:__main__:Performing financial consolidation...
INFO:__main__:Creating consolidated report Book...
INFO:__main__:Executing consolidation formulas...

--- Consolidated Financial Report ---
Account                             Amount
--------------------------------------------------
Consolidated Revenue                $750,000.00
Consolidated COGS                   $270,000.00
Consolidated Gross Profit           $480,000.00
Consolidated Operating Expenses     $180,000.00
Consolidated Operating Income       $300,000.00
Intercompany Eliminations          

INFO:__main__:
Financial consolidation use case completed.
```

This comprehensive use case demonstrates the seamless integration of the `Librarian`, `Book`, and `Researcher` components to tackle a complex, real-world financial task. It shows how the `Librarian` provides the master data structure, the `Book` provides the flexibility to handle raw data, and the `Researcher` provides the analytical power to transform that data into valuable insights.
