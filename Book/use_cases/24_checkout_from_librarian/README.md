# Use Case 24: Checking Out a Hierarchy from the Librarian

This use case demonstrates the reverse of the "promotion" workflow: "checking out" a master hierarchy from the `Librarian`'s central repository into a local, in-memory `Book` object for analysis, modification, or reporting.

## Features Highlighted

*   **`Librarian` (Simulated):** Acts as the central repository for the master data hierarchy.
*   **`Book` Library:** Used to create a local, in-memory copy of the hierarchy.
*   **Data Portability:** Shows how data can be moved from a persistent, structured format (the `Librarian`'s database) into a flexible, in-memory object (`Book`).

## Components Involved

*   **`Librarian` (Simulated):** The source of the master hierarchy, represented by a `TinyDB` database file (`librarian_db.json`).
*   **`Book` Library:** The destination for the checked-out hierarchy.

## Files

*   `checkout_from_librarian.py`: The Python script that simulates the checkout workflow.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies, including `TinyDB`, installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Run the Checkout Script

Navigate to the `Book/use_cases/24_checkout_from_librarian` directory and run the `checkout_from_librarian.py` script:

```bash
python checkout_from_librarian.py
```

### 3. What's Happening?

1.  **Setup Simulated Librarian:** The script first calls `setup_simulated_librarian()` to create a temporary `librarian_db.json` file. This file is pre-populated with a sample "Master Chart of Accounts" project, simulating a hierarchy that is already stored in the `Librarian`.
2.  **Checkout Hierarchy:** The `checkout_librarian_hierarchy` function is called. This function:
    *   Queries the `Librarian`'s `TinyDB` database for the specified project.
    *   Fetches all the hierarchy nodes associated with that project.
    *   Reconstructs the hierarchy in memory as a `Book` object, including the parent-child relationships.
3.  **Verification:** The script prints the name and structure of the checked-out `Book` to verify that the process was successful.
4.  **Cleanup:** The temporary `librarian_db.json` file is removed.

### Expected Output

You should see output similar to this in your terminal:

```
INFO:__main__:Starting 'Checkout from Librarian' use case...
INFO:__main__:Simulated Librarian database created at 'librarian_db.json' with project 'proj-master-coa-001'.
INFO:__main__:Checking out hierarchy for project 'proj-master-coa-001'...

--- Verifying Checkout ---
INFO:__main__:Checked out book name: Master Chart of Accounts
INFO:__main__:Hierarchy structure:
- Assets
  - Current Assets
    - Cash
- Liabilities
- Equity

INFO:__main__:
Cleaned up librarian_db.json.

INFO:__main__:
'Checkout from Librarian' use case completed.
```

This use case demonstrates how users can easily pull centrally managed hierarchies from the `Librarian` into the flexible `Book` library for local tasks, enabling a powerful and agile data management workflow.
