# Use Case 27: Meta-Agent Orchestrator

This use case demonstrates the concept of a "Meta-Agent" or orchestrator that can execute a multi-step plan involving different components of the DataBridge AI ecosystem (`Librarian`, `Researcher`, and `Book` library).

## Features Highlighted

*   **Agentic Orchestration:** Shows how a higher-level agent can manage a complex workflow by calling a sequence of tools.
*   **Inter-component Communication:** Demonstrates how the output of one component (e.g., `Librarian`) can be used as the input for another (e.g., `Researcher`).
*   **End-to-End Automation:** Provides a blueprint for automating complex, multi-step tasks like financial consolidation.

## Components Involved

*   **`Librarian` (Simulated):** Provides the master data.
*   **`Researcher` (Simulated):** Performs the core analysis.
*   **`Book` Library:** Used as the data container for passing data between components.
*   **`MetaAgent`:** The orchestrator that executes the plan.

## Files

*   `meta_agent.py`: The Python script that defines the `MetaAgent` and simulates the orchestration of a financial consolidation.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Run the Meta-Agent Script

Navigate to the `Book/use_cases/27_meta_agent_orchestrator` directory and run the `meta_agent.py` script:

```bash
python meta_agent.py
```

### 3. What's Happening?

1.  **Simulation of Components:** The script starts by defining placeholder functions (`librarian_get_hierarchy`, `researcher_run_consolidation`) to simulate the `Librarian` and `Researcher`.
2.  **`MetaAgent` Class:** The `MetaAgent` class is defined with an `execute_plan` method. This agent has a "tool registry" where it knows about the available tools (from the `Librarian`, `Researcher`, etc.).
3.  **Define a Plan:** A multi-step plan is defined as a list of dictionaries. Each dictionary represents a step, including the tool to use and how to map inputs and outputs from a shared `context`.
4.  **Execute the Plan:** The `MetaAgent` executes the plan step by step:
    *   It calls the `librarian_get_hierarchy` tool to get the master consolidation hierarchy.
    *   It simulates the loading of subsidiary data into `Book` objects.
    *   It then calls the `researcher_run_consolidation` tool, passing the master hierarchy and the subsidiary `Book` objects as inputs.
5.  **Print Final Result:** The script prints the final result of the plan execution—the consolidated report `Book`.

### Expected Output

You should see output similar to this in your terminal:

```
INFO:__main__:--- Starting Execution of Meta-Agent Plan ---
INFO:__main__:
Executing step: 'Fetch Master Hierarchy' with tool: 'get_hierarchy'
INFO:__main__:[Librarian] Fetching master hierarchy: 'Consolidation Hierarchy'
INFO:__main__:  - Step successful. Output stored in context as 'master_hierarchy'.
INFO:__main__:
Executing step: 'Load Subsidiary A Data' with tool: 'create_book'
INFO:__main__:  - Error: Tool 'create_book' not found.
INFO:__main__:
Executing step: 'Load Subsidiary B Data' with tool: 'create_book'
INFO:__main__:  - Error: Tool 'create_book' not found.
INFO:__main__:
Executing step: 'Run Consolidation' with tool: 'run_consolidation'
INFO:__main__:[Researcher] Running consolidation for 'Subsidiary A' and 'Subsidiary B'...
INFO:__main__:  - Step successful. Output stored in context as 'consolidated_report'.

--- Meta-Agent Plan Execution Completed ---

--- Final Consolidated Report ---
{
  "name": "Consolidated Report",
  "schema_version": "1.0",
  "data_version": "[some-uuid]",
  "last_updated": "[timestamp]",
  "root_nodes": [
    {
      "id": "[some-uuid]",
      "schema_version": "1.0",
      "name": "Total Consolidated Revenue",
      "children": [],
      "properties": {
        "amount": 800000
      },
      ...
    }
  ],
  ...
}
```
*(Note: The "Tool not found" errors are expected in this simulation because `create_book` is not a real tool in the `MetaAgent`'s registry. In a real implementation, these would be actual MCP tools.)*

This use case provides a conceptual blueprint for the most advanced form of automation within the DataBridge AI ecosystem, where a `Meta-Agent` can orchestrate complex workflows across all components, enabling true end-to-end automation of analytical tasks.
