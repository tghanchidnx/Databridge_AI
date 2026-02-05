# Use Case 6: Asset Hierarchy Management (Librarian & Book)

This use case demonstrates how the `Librarian` (simulated) can be used to manage a master asset hierarchy, while the `Book` library provides flexibility for local, ad-hoc manipulation and inspection of that hierarchy. This scenario highlights a common pattern where a central system (Librarian) maintains the authoritative data, and a flexible tool (Book) allows for quick, localized modifications and analysis without directly altering the master.

## Components Involved

*   **`Librarian` (Simulated):** Provides the master, persistent asset hierarchy. In this example, its role is simulated by the `setup_librarian.py` script, which defines the master hierarchy structure.
*   **`Book` Library:** Used to ingest a subset of the asset data, perform in-memory modifications, and export the result.

## Files

*   `asset_inventory.csv`: A sample CSV file representing a simplified asset inventory. This data will be ingested into a local `Book`.
*   `setup_librarian.py`: A script that *simulates* the creation and storage of a master asset hierarchy within the `Librarian`. In a real scenario, the `Librarian`'s CLI or MCP tools would be used for this.
*   `run_management.py`: The Python script that demonstrates the local management of the asset hierarchy using the `Book` library.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Run the Management Script

Navigate to the `Book/use_cases/06_asset_hierarchy` directory and run the `run_management.py` script:

```bash
python run_management.py
```

### 3. What's Happening?

1.  **Master Hierarchy (Librarian Simulation):** The `run_management.py` script first calls `create_asset_hierarchy()` from `setup_librarian.py`. This function returns a `Book` object representing the master asset hierarchy. In a real-world scenario, this master hierarchy would be retrieved from the `Librarian`'s persistent storage (e.g., its SQLite database), possibly via its CLI or MCP tools.
2.  **Local Asset Book (Book):** The script then ingests the `asset_inventory.csv` file into a separate, local `Book` object named "Local Asset View." This simulates an analyst pulling relevant data for local work.
3.  **Property Modification:** The script demonstrates how to modify a property of a specific node (`Desk 1`) within the local `Book`, changing its `status` from "Active" to "Under Maintenance."
4.  **Property Addition:** It also shows how to add a new property (`last_checked`) to another node (`Server Rack 1`).
5.  **Export to GML:** The modified local `Book` is then exported to a GML file (`local_asset_view.gml`). This could represent saving local changes, sharing with a colleague, or preparing for eventual synchronization back to the `Librarian` (a more advanced future feature).
6.  **Verification:** The script reloads the GML file to demonstrate that the changes were successfully persisted.
7.  **Cleanup:** The temporary GML file is removed.

### Expected Output

You should see output similar to this in your terminal:

```
INFO:__main__:Starting asset hierarchy management use case...
INFO:__main__:Getting master asset hierarchy from Librarian (simulated)...
INFO:__main__:Master Hierarchy Name: Master Asset Hierarchy
INFO:__main__:Ingesting asset inventory data from asset_inventory.csv into a local Book...
INFO:__main__:Modifying a property of 'Desk 1' in the local Book...
INFO:__main__:Updated 'Desk 1' status from 'Active' to 'Under Maintenance'
INFO:__main__:Adding a new property 'last_checked' to 'Server Rack 1'...
INFO:__main__:Added 'last_checked' to 'Server Rack 1': 2026-02-04
INFO:__main__:Exporting local asset Book to local_asset_view.gml...
INFO:__main__:Local Book successfully exported to local_asset_view.gml
INFO:__main__:Loading local_asset_view.gml back to verify...
INFO:__main__:Loaded 'Desk 1' status: Under Maintenance
INFO:__main__:Cleaned up local_asset_view.gml
INFO:__main__:Asset hierarchy management use case completed.
```

This use case illustrates a powerful pattern for flexible data management: using the `Book` library for temporary, local data exploration and modification while relying on a centralized system like `Librarian` for long-term master data governance.
