# Use Case 18: Change Data Capture (CDC) and Reconciliation

This use case demonstrates how to use the `Book` and `LinkedBook` features to process a Change Data Capture (CDC) log and reconcile it with a master inventory dataset. This is a common scenario in data warehousing and master data management.

## Features Highlighted

*   **`Librarian` (Simulated):** Represents the master data source.
*   **`LinkedBook`:** Used to apply changes from a CDC feed without modifying the original master data.
*   **Data Reconciliation:** Shows a pattern for handling `INSERT`, `UPDATE`, and `DELETE` operations from a change log.
*   **Change Materialization:** Demonstrates how to create a new, reconciled version of the data by applying the changes.

## Components Involved

*   **`Librarian` (Simulated):** Manages the master inventory.
*   **`Book` and `LinkedBook`:** Used to represent the initial state, process changes, and create the final reconciled state.

## Files

*   `initial_inventory.csv`: A CSV file representing the initial state of the product inventory.
*   `inventory_cdc_log.csv`: A CSV file containing a log of changes (updates, inserts, and deletes) to the inventory.
*   `run_cdc_reconciliation.py`: The Python script that performs the CDC reconciliation.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Run the Reconciliation Script

Navigate to the `Book/use_cases/18_cdc_reconciliation` directory and run the `run_cdc_reconciliation.py` script:

```bash
python run_cdc_reconciliation.py
```

### 3. What's Happening?

1.  **Load Initial State:** The script loads the `initial_inventory.csv` into a `Book` object, representing the master inventory managed by the `Librarian`.
2.  **Create LinkedBook:** A `LinkedBook` is created from the master inventory `Book`. This will be used to apply the changes from the CDC log.
3.  **Process CDC Log:** The script reads the `inventory_cdc_log.csv` file and processes each change:
    *   **UPDATE:** For `UPDATE` operations, the script adds a delta to the `LinkedBook` with the new value for the `stock_level`.
    *   **INSERT:** For `INSERT` operations, a new `Node` is created and added to the book (in this simplified example, it's added to the master book directly).
    *   **DELETE:** For `DELETE` operations, a delta is added to the `LinkedBook` to change the `status` of the node to "deleted."
4.  **Materialize New Version:** The `to_book` method is called on the `LinkedBook` to create a new `Book` object, `reconciled_inventory`, which contains the result of applying all the changes.
5.  **Print Report:** Finally, the script prints a reconciliation report that shows the original and new stock levels, as well as the final status of each product.

### Expected Output

You should see output similar to this in your terminal:

```
INFO:__main__:Starting CDC reconciliation use case...
INFO:__main__:Loading initial inventory...
INFO:__main__:Creating a LinkedBook for change management...
INFO:__main__:Processing CDC log...
INFO:__main__:  - Applied UPDATE for P001: stock_level -> 140
INFO:__main__:  - Applied UPDATE for P003: stock_level -> 180
INFO:__main__:  - Applied INSERT for P004: {'product_name': 'Gaming Mouse', 'stock_level': 50}
INFO:__main__:  - Applied DELETE for P002

Materializing reconciled inventory...

--- Reconciliation Report ---
Product Name         Original Stock  New Stock       Status         
-----------------------------------------------------------------
Laptop Pro           150             140             active         
Mechanical Keyboard  80              80              deleted        
Wireless Mouse       200             180             active         
Gaming Mouse         N/A             50              active         

INFO:__main__:
CDC reconciliation use case completed.
```

This use case demonstrates how the `Book` and `LinkedBook` can be used to build powerful and flexible data reconciliation pipelines, enabling you to manage and audit changes to your data in a structured and programmatic way.
