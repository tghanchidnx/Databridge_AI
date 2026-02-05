# Use Case 9: Supply Chain Optimization (Librarian & Researcher)

This use case demonstrates how the `Librarian` can store a master supply chain hierarchy, and how a script (simulating the `Researcher`) can analyze this hierarchy to identify potential bottlenecks and calculate accumulated costs. It highlights:

*   **`Librarian`:** As a central repository for complex, multi-level hierarchies (e.g., supply chain structures).
*   **`Book` Library:** Used to represent the supply chain data in-memory for analysis.
*   **`Researcher` (Simulated):** Performing analytical tasks like bottleneck detection and cost accumulation on the hierarchical data.

## Components Involved

*   **`Librarian` (Simulated):** Manages the master supply chain hierarchy.
*   **`Book` Library:** Provides the data structure for in-memory analysis.
*   **`Researcher` (Simulated):** The `run_analysis.py` script acts as a basic "Researcher" performing the analytical tasks.

## Files

*   `supply_chain_data.csv`: A sample CSV file containing a simplified supply chain structure with capacity and cost information.
*   `setup_librarian.py`: A script that *simulates* the creation and storage of the master supply chain hierarchy within the `Librarian`.
*   `run_analysis.py`: The Python script that performs the supply chain analysis.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Run the Analysis Script

Navigate to the `Book/use_cases/09_supply_chain_optimization` directory and run the `run_analysis.py` script:

```bash
python run_analysis.py
```

### 3. What's Happening?

1.  **Load Master Hierarchy (Librarian Simulation):** The script first calls `create_supply_chain_hierarchy()` from `setup_librarian.py`. This simulates the `Researcher` fetching the master supply chain hierarchy from the `Librarian`. The hierarchy includes properties like `type`, `capacity`, and `cost_per_unit`.
2.  **Perform Analysis (Researcher Simulation):** The `analyze_supply_chain` function (simulating a `Researcher` task) recursively traverses the `Book`'s nodes:
    *   **Bottleneck Detection:** For `Supplier` and `Factory` nodes, it checks if their `capacity` falls below a predefined threshold (15,000 units in this example). If so, it flags the node as a bottleneck and adds a reason to its properties.
    *   **Cost Accumulation:** It calculates the `accumulated_cost` for each node, effectively propagating costs down the supply chain.
3.  **Print Report:** Finally, the script prints a "Supply Chain Analysis Report," highlighting potential bottlenecks and showing the accumulated cost for each stage of the supply chain.

### Expected Output

You should see output similar to this in your terminal:

```
INFO:__main__:Starting supply chain optimization use case...
INFO:__main__:Loading master supply chain hierarchy from Librarian (simulated)...
INFO:__main__:Performing bottleneck and cost analysis (simulating Researcher)...

--- Supply Chain Analysis Report ---
Global Supply Chain (Type: None): Accumulated Cost: $0.00
  Raw Materials (Type: Stage): Accumulated Cost: $0.00
    Supplier A (Type: Supplier): BOTTLENECK! Accumulated Cost: $0.50
      Reason: Low capacity: 10000
    Supplier B (Type: Supplier): Accumulated Cost: $0.45
  Manufacturing (Type: Stage): Accumulated Cost: $0.00
    Factory X (Type: Factory): Accumulated Cost: $1.20
    Factory Y (Type: Factory): Accumulated Cost: $1.15
  Distribution (Type: Stage): Accumulated Cost: $0.00
    Warehouse P (Type: Warehouse): Accumulated Cost: $0.10
    Warehouse Q (Type: Warehouse): Accumulated Cost: $0.12
  Retail (Type: Stage): Accumulated Cost: $0.00
    Store 1 (Type: Store): Accumulated Cost: $0.05
    Store 2 (Type: Store): Accumulated Cost: $0.05

INFO:__main__:Supply chain optimization use case completed.
```

This use case demonstrates how the combined power of `Librarian` for master data and `Book` for flexible in-memory analysis can be leveraged to gain insights into complex operational structures like a supply chain.
