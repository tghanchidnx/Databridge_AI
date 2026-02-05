# Use Case 13: Version Control and Auditing of Hierarchies

This use case demonstrates how the `LinkedBook` feature of the `Book` library can be used to manage changes to a master hierarchy in a controlled and auditable way. This workflow is essential for data governance and for maintaining a history of changes to critical master data.

## Features Highlighted

*   **`Librarian` (Simulated):** Acts as the central repository for the master data hierarchy.
*   **`LinkedBook`:** Used to create a lightweight, versioned copy of the master hierarchy where changes are stored as deltas.
*   **Auditing:** The list of deltas in the `LinkedBook` serves as a clear and concise audit trail of all proposed changes.
*   **Change Materialization:** Demonstrates how to apply the deltas to create a new, independent version of the `Book`.

## Components Involved

*   **`Librarian` (Simulated):** Manages the master Chart of Accounts (CoA).
*   **`Book` and `LinkedBook`:** Used to propose, track, and apply changes to the CoA.

## Files

*   `setup_librarian.py`: A script that *simulates* the creation and storage of the master CoA hierarchy within the `Librarian`.
*   `run_versioning_workflow.py`: The Python script that demonstrates the versioning and auditing workflow.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Run the Versioning Workflow Script

Navigate to the `Book/use_cases/13_version_control_and_auditing` directory and run the `run_versioning_workflow.py` script:

```bash
python run_versioning_workflow.py
```

### 3. What's Happening?

1.  **Load Master Hierarchy:** The script begins by loading the master CoA from the simulated `Librarian`.
2.  **Create LinkedBook:** A `LinkedBook` is created from the master CoA. This creates a "live" link to the base book without duplicating all the data.
3.  **Propose Changes:** The script then adds changes to the `LinkedBook` by calling the `add_change` method. These changes are stored as `Delta` objects and do not affect the original master CoA.
4.  **Access Changed Data:** When properties are accessed from the `LinkedBook`, it first checks the deltas for any changes and then falls back to the base book. This ensures that the view from the `LinkedBook` always reflects the proposed changes.
5.  **Audit Trail:** The script prints the list of `Delta` objects, which serves as a clear audit trail of what was changed, on which node, and what the new value is.
6.  **Materialize New Version:** Finally, the script calls the `to_book` method on the `LinkedBook` to apply the deltas and create a new, independent `Book` object, "Master Chart of Accounts v2." This new version can then be saved, deployed, or used for further analysis.

### Expected Output

You should see output similar to this in your terminal:

```
INFO:__main__:Starting version control and auditing use case...
INFO:__main__:Loading master CoA from Librarian...
INFO:__main__:Creating a LinkedBook to propose changes...
INFO:__main__:Proposing changes (adding deltas)...
INFO:__main__:  - Proposed change: Add 'gl_code' property to 'Cash' node.
INFO:__main__:  - Proposed change: Modify 'is_liquid' property on 'Cash' node.

Accessing properties from the LinkedBook (reflects changes)...
INFO:__main__:  - 'Cash' gl_code: 10100
INFO:__main__:  - 'Cash' is_liquid: False

--- Audit Trail (Deltas) ---
INFO:__main__:  - Node ID: [some-uuid], Key: gl_code, New Value: 10100
INFO:__main__:  - Node ID: [some-uuid], Key: is_liquid, New Value: False

Materializing the LinkedBook into a new version of the CoA...
INFO:__main__:New CoA version name: Master Chart of Accounts v2
INFO:__main__:  - 'Cash' gl_code in v2: 10100
INFO:__main__:  - 'Cash' is_liquid in v2: False

INFO:__main__:Version control and auditing use case completed.
```

This use case demonstrates a powerful pattern for data governance, allowing for controlled, auditable changes to master data hierarchies without compromising the integrity of the master data itself.
