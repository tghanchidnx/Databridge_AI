# Use Case 23: Promoting a Book to the Librarian

This use case demonstrates a key integration workflow: "promoting" a `Book` object that has been prototyped and refined into a centrally managed `Librarian` project. This represents the transition from ad-hoc analysis to a governed, master data hierarchy.

## Features Highlighted

*   **`Librarian` (Simulated):** The central repository for master data, represented by a `TinyDB` database.
*   **`Book` Library:** Used to create the initial prototype of the hierarchy.
*   **Data Governance Workflow:** Shows a pattern for formalizing a prototyped data structure into a master data project.

## Components Involved

*   **`Librarian` (Simulated):** The target for the promotion, represented by a `TinyDB` database file (`librarian_db.json`).
*   **`Book` Library:** The source of the hierarchy to be promoted.

## Files

*   `promote_to_librarian.py`: The Python script that simulates the promotion workflow.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies, including `TinyDB`, installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Run the Promotion Script

Navigate to the `Book/use_cases/23_promote_to_librarian` directory and run the `promote_to_librarian.py` script:

```bash
python promote_to_librarian.py
```

### 3. What's Happening?

1.  **Create Prototype Book:** The script starts by creating a sample `Book` object named "Q3 Sales Plan." This represents a hierarchy that an analyst might create for their own analysis or as a proposal for a new master data structure.
2.  **Promote to Librarian:** The `promote_book_to_librarian` function is called. This function:
    *   Creates a new project in the simulated `Librarian` database (`librarian_db.json`).
    *   Recursively traverses the nodes of the input `Book`.
    *   For each node, it creates a corresponding record in the `Librarian`'s `hierarchies` table, linking it to the newly created project.
3.  **Verification:** After the promotion is complete, the script queries the `librarian_db.json` file to verify that the project and its associated hierarchy nodes have been successfully created.
4.  **Cleanup:** The script removes the temporary `librarian_db.json` file.

### Expected Output

You should see output similar to this in your terminal:

```
INFO:__main__:Starting 'Promote Book to Librarian' use case...
INFO:__main__:Creating a sample 'Sales Region' Book...
INFO:__main__:Successfully promoted Book 'Q3 Sales Plan' to Librarian project '[some-uuid]'.

--- Verifying promotion in Librarian DB ---
INFO:__main__:Found project: Q3 Sales Plan (ID: [some-uuid])
INFO:__main__:Found 6 nodes in the hierarchy.

INFO:__main__:
Cleaned up librarian_db.json.

INFO:__main__:
'Promote Book to Librarian' use case completed.
```

This use case demonstrates a critical governance workflow, providing a clear path for moving from flexible, ad-hoc data structures in the `Book` library to a centrally managed and governed master data hierarchy in the `Librarian`.
