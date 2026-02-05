# Use Case 22: Persistent Graph Database with TinyDB

This use case demonstrates how to save a `Book`'s graph representation to a persistent, file-based graph database using `TinyDB` and then load it back. This showcases the library's ability to persist hierarchical data in a graph format.

## Features Highlighted

*   **Persistent Graph Storage:** Using `TinyDB` as a simple, file-based graph database.
*   **`save_graph_to_tinydb`:** A function to serialize a `networkx` graph into a `TinyDB` database.
*   **`load_graph_from_tinydb`:** A function to deserialize a graph from a `TinyDB` database.
*   **End-to-End Workflow:** Demonstrates the full cycle of creating a `Book`, converting it to a graph, persisting it, loading it back, and converting it back to a `Book`.

## Components Involved

*   **`Book` Library:** Provides the `Book` data structure and the graph database functions.
*   **`networkx`:** Used for the in-memory graph representation.
*   **`TinyDB`:** Used as the file-based persistent graph database.

## Files

*   `run_persistent_graph.py`: The Python script that implements the use case.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies, including `TinyDB`, installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Run the Script

Navigate to the `Book/use_cases/22_persistent_graph_db` directory and run the `run_persistent_graph.py` script:

```bash
python run_persistent_graph.py
```

### 3. What's Happening?

1.  **Create a Book:** The script creates a sample `Book` with a simple hierarchy.
2.  **Convert to Graph:** The `Book` is converted to a `networkx` graph.
3.  **Save to TinyDB:** The `save_graph_to_tinydb` function is called to save the graph to a file named `project_graph.json`. This function stores the nodes and edges of the graph in two separate tables within the `TinyDB` database.
4.  **Load from TinyDB:** The `load_graph_from_tinydb` function is called to load the graph from the `project_graph.json` file.
5.  **Convert back to Book:** The loaded graph is converted back into a `Book` object.
6.  **Verification:** The script prints the names of the nodes in the reloaded book to verify that the process was successful.
7.  **Cleanup:** The script removes the `project_graph.json` file.

### Expected Output

You should see output similar to this in your terminal:

```
INFO:__main__:Starting persistent graph database use case...
INFO:__main__:Creating a sample Book...
INFO:__main__:Converting Book to a networkx graph...
INFO:__main__:Saving graph to TinyDB database: project_graph.json...
INFO:__main__:Loading graph from project_graph.json...
INFO:__main__:Converting loaded graph back to a Book...

--- Verification ---
INFO:__main__:Reloaded book name: Reloaded Project
INFO:__main__:Nodes in reloaded book:
- Task 1
  - Subtask 1.1
  - Subtask 1.2
- Task 2

INFO:__main__:
Cleaned up project_graph.json.

INFO:__main__:
Persistent graph database use case completed.
```

This use case demonstrates how the `Book` library can be used to persist hierarchical data in a graph format, which can be useful for sharing data, for caching, or as a step towards a more robust graph database solution.
