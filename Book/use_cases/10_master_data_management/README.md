# Use Case 10: Master Data Management (Librarian)

This use case demonstrates how the `Librarian` acts as a central hub for managing various master data hierarchies within an organization. It highlights the `Librarian`'s role in:

*   Maintaining multiple master data projects (e.g., Customer, Product).
*   Providing a consistent interface (CLI/MCP) for accessing and exploring these hierarchies.
*   Allowing users to "drill down" into specific parts of a master data hierarchy.

## Components Involved

*   **`Librarian` (Simulated):** The core component for persistent storage and management of master data hierarchies. Its functions are simulated by Python scripts that create and expose `Book` objects.

## Files

*   `customer_master.csv`: A sample CSV file representing a partial customer master data.
*   `setup_librarian.py`: A script that *simulates* the creation and storage of the Customer Master hierarchy within the `Librarian`.
*   `run_mdm_operations.py`: The Python script that simulates various interactions with the `Librarian`'s CLI/MCP for MDM operations.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Run the MDM Operations Script

Navigate to the `Book/use_cases/10_master_data_management` directory and run the `run_mdm_operations.py` script:

```bash
python run_mdm_operations.py
```

### 3. What's Happening?

1.  **Simulated Project Listing:** The script begins by simulating a call to the `Librarian`'s `project list` command. This would typically show all available master data projects (e.g., "Customer Master Data", "Product Master Data").
2.  **Load Master Hierarchy:** It then simulates loading the "Customer Master Data" hierarchy, which is represented by a `Book` object created by `create_customer_hierarchy()` from `setup_librarian.py`.
3.  **Simulated Drill-down:** The script demonstrates a "drill-down" operation. It simulates a user requesting details for the "North" region within the Customer Master hierarchy. In a real `Librarian` scenario, this would involve querying the `Librarian`'s database and returning a subset of the hierarchy.
4.  **Display Details:** The script prints the properties of the identified "North" region node and lists the customers associated with it, showcasing how detailed information can be retrieved from the master data.

### Expected Output

You should see output similar to this in your terminal:

```
INFO:__main__:Simulating Librarian interactions for Master Data Management use case...

--- Librarian: List Projects ---
INFO:__main__:Command: databridge project list
INFO:__main__:- Project ID: proj-cust-001, Name: Customer Master Data, Hierarchies: 1
INFO:__main__:- Project ID: proj-prod-002, Name: Product Master Data, Hierarchies: 1

--- Librarian: Get Customer Master Hierarchy ---
INFO:__main__:Command: databridge hierarchy show proj-cust-001
INFO:__main__:Loaded Hierarchy: Customer Master

--- Librarian: Drill down into 'North' region ---
INFO:__main__:Command: databridge hierarchy tree proj-cust-001 --path 'North'
INFO:__main__:Details for 'North' (ID: C001):
INFO:__main__:  customer_id: C001
INFO:__main__:  customer_name: Alpha Corp
INFO:__main__:  region: North
INFO:__main__:  segment: Enterprise
INFO:__main__:  Children in North Region:
INFO:__main__:    - Alpha Corp (ID: C001)
INFO:__main__:    - Gamma Inc (ID: C003)

INFO:__main__:Master Data Management use case completed.
```

This use case illustrates the fundamental role of the `Librarian` in providing a structured, centralized, and accessible repository for critical master data, which is essential for consistent reporting and analysis across an organization.
