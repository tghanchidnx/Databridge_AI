# Use Case 3: Data Reconciliation and Deduplication

This use case demonstrates how to use the `Book` library to reconcile and deduplicate customer data from two different CSV files that contain inconsistencies. It highlights the following features:

*   Ingesting data from multiple sources.
*   Using a third-party library (`rapidfuzz`) for fuzzy string matching.
*   Creating a `Book` to represent the reconciled data.
*   Storing structured data, including match status and scores, as properties on nodes.

## Files

*   `source_a.csv`: The first customer data source, considered the "source of truth."
*   `source_b.csv`: The second customer data source, with inconsistencies in names and different records.
*   `reconcile.py`: The Python script that performs the reconciliation.

## Step-by-Step Instructions

### 1. Run the Script

Navigate to the `Book/use_cases/03_data_reconciliation` directory and run the `reconcile.py` script:

```bash
python reconcile.py
```

### 2. What's Happening?

1.  **Load Data:** The script loads the customer data from `source_a.csv` and `source_b.csv`.
2.  **Fuzzy Matching:** It uses the `rapidfuzz` library to find potential matches between the two data sources based on the customer names. It calculates a similarity score and considers anything above an 85% match as a potential duplicate.
3.  **Create Reconciliation Book:** A `Book` is created to store the results of the reconciliation.
4.  **Populate Book:** The script iterates through the matches and orphans and creates a `Node` for each:
    *   **Matched Nodes:** For each match, a single node is created. The properties of this node contain the original data from both sources, the match status, and the similarity score.
    *   **Orphan Nodes:** For records that are present in one source but not the other, a node is created with a status of "orphan\_a" or "orphan\_b".
5.  **Print Report:** The script then prints a summary of the reconciliation, listing the matched records and the orphans from each source.

### Expected Output

You should see the following output in your terminal:

```
INFO:__main__:Starting data reconciliation use case...
INFO:__main__:Loading data from source_a.csv and source_b.csv...
INFO:__main__:Performing fuzzy matching on customer names...
INFO:__main__:Creating nodes for reconciled data...
INFO:__main__:Reconciliation Report:
- Matched: 'John Smith' (A) and 'Jon Smith' (B) with score 93.33
- Matched: 'Jane Doe' (A) and 'Jane D.' (B) with score 90.91
- Matched: 'Peter Jones' (A) and 'Peter Jones' (B) with score 100.00
- Orphan in Source A: 'Mary Brown'
- Orphan in Source B: 'Susan White'
```

This use case shows how the `Book` library can be used as a flexible tool for data quality and reconciliation tasks, even when dealing with messy, real-world data.
