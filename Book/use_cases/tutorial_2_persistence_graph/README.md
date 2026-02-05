# Tutorial 2: Persistent Storage & Graph Operations

## What You Will Learn

- Convert a Book to a NetworkX directed graph
- Inspect graph structure (nodes, edges, degrees)
- Save a graph to TinyDB for persistent storage
- Load a graph back from TinyDB and reconstruct the Book
- Save/load Books using the management module (JSON and GML formats)
- Use SyncManager to maintain replicas

## Prerequisites

```bash
cd C:\Users\telha\Databridge_AI
pip install tinydb networkx
```

## Step-by-Step Walkthrough

### Run the Tutorial

```bash
cd C:\Users\telha\Databridge_AI
set PYTHONPATH=./Book
python Book/use_cases/tutorial_2_persistence_graph/run_tutorial.py
```

### What the Script Does

#### Step 1: Create a Sample Book

We create a project management hierarchy:

```python
book = Book(name="Project Alpha", root_nodes=[
    Node(name="Planning", children=[
        Node(name="Requirements", properties={"status": "complete"}),
        Node(name="Design", properties={"status": "in_progress"}),
    ]),
    Node(name="Development", children=[
        Node(name="Backend", properties={"status": "in_progress"}),
        Node(name="Frontend", properties={"status": "not_started"}),
    ]),
])
```

#### Step 2: Convert to NetworkX Graph

```python
graph = book_to_graph(book)
print(f"Nodes: {graph.number_of_nodes()}")
print(f"Edges: {graph.number_of_edges()}")
```

**Result:** 6 nodes, 4 edges (Planning→Requirements, Planning→Design, etc.)

#### Step 3: Save to TinyDB

```python
save_graph_to_tinydb(graph, "project_alpha.json")
```

**Result:** A JSON file with `nodes` and `edges` tables is created on disk.

#### Step 4: Load from TinyDB

```python
loaded_graph = load_graph_from_tinydb("project_alpha.json")
restored_book = graph_to_book(loaded_graph, "Project Alpha Restored")
```

**Result:** The full hierarchy is reconstructed from the database file.

#### Step 5: Verify Round-Trip Integrity

```python
assert restored_book.root_nodes[0].name == "Planning"
assert restored_book.root_nodes[0].children[0].name == "Requirements"
```

#### Step 6: Management Module (JSON/GML)

```python
# Save as JSON
copy_book(book, "project_alpha_backup.json", "json")

# Load from JSON
loaded = load_book("project_alpha_backup.json", "json", "Loaded Book")

# SyncManager for replicas
sync = SyncManager(book)
sync.add_replica("replica_1.json", "json")
sync.add_replica("replica_2.gml", "gml")
sync.update_replicas(book)  # Updates all replicas
```

### Expected Output

```
=== Tutorial 2: Persistent Storage & Graph Operations ===

--- Step 1: Create Book ---
Book: Project Alpha (6 nodes)

--- Step 2: Convert to Graph ---
Graph nodes: 6
Graph edges: 4
Root nodes (in-degree 0): ['Planning', 'Development']

--- Step 3: Save to TinyDB ---
Saved to project_alpha.json

--- Step 4: Load from TinyDB ---
Loaded graph: 6 nodes, 4 edges
Restored book: Project Alpha Restored

--- Step 5: Verify Round-Trip ---
Planning → Requirements: status = complete
Planning → Design: status = in_progress
Round-trip integrity verified!

--- Step 6: Management Module ---
Saved as JSON: project_alpha_backup.json
Loaded from JSON: Loaded Book (6 nodes)
SyncManager: 2 replicas updated

All steps completed successfully!
```

## Key Concepts

| Concept | What It Does |
|---------|-------------|
| **NetworkX DiGraph** | Directed graph representation of Book hierarchy |
| **TinyDB** | Lightweight JSON document database for persistence |
| **Round-trip** | Book → Graph → TinyDB → Graph → Book with no data loss |
| **GML** | Graph Modelling Language file format (standard graph format) |
| **SyncManager** | Maintains multiple replicas of a Book in different formats |

## Next Tutorial

Continue to [Tutorial 3: Librarian Promote & Checkout](../tutorial_3_librarian_workflow/README.md)
