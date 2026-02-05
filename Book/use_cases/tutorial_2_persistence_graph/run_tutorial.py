"""
Tutorial 2: Persistent Storage & Graph Operations
===================================================
Demonstrates: NetworkX graphs, TinyDB persistence, JSON/GML export, SyncManager.

Usage:
    cd C:\\Users\\telha\\Databridge_AI
    set PYTHONPATH=./Book
    python Book/use_cases/tutorial_2_persistence_graph/run_tutorial.py
"""

import os
from book import (
    Book, Node,
    book_to_graph, graph_to_book,
    save_graph_to_tinydb, load_graph_from_tinydb,
    save_graph, load_graph,
    copy_book, load_book, SyncManager,
)


def count_nodes(nodes):
    total = len(nodes)
    for n in nodes:
        total += count_nodes(n.children)
    return total


def main():
    print("=== Tutorial 2: Persistent Storage & Graph Operations ===\n")

    # --- Step 1: Create a sample Book ---
    print("--- Step 1: Create Book ---")
    book = Book(
        name="Project Alpha",
        root_nodes=[
            Node(name="Planning", properties={"phase": 1}, children=[
                Node(name="Requirements", properties={"status": "complete", "owner": "Alice"}),
                Node(name="Design", properties={"status": "in_progress", "owner": "Bob"}),
            ]),
            Node(name="Development", properties={"phase": 2}, children=[
                Node(name="Backend", properties={"status": "in_progress", "owner": "Charlie"}),
                Node(name="Frontend", properties={"status": "not_started", "owner": "Diana"}),
            ]),
        ]
    )
    total = count_nodes(book.root_nodes)
    print(f"Book: {book.name} ({total} nodes)")
    print()

    # --- Step 2: Convert to NetworkX graph ---
    print("--- Step 2: Convert to Graph ---")
    graph = book_to_graph(book)
    print(f"Graph nodes: {graph.number_of_nodes()}")
    print(f"Graph edges: {graph.number_of_edges()}")

    # Find root nodes (no incoming edges)
    roots = [n for n in graph.nodes() if graph.in_degree(n) == 0]
    root_names = [graph.nodes[n].get("name", n) for n in roots]
    print(f"Root nodes (in-degree 0): {root_names}")
    print()

    # --- Step 3: Save to TinyDB ---
    print("--- Step 3: Save to TinyDB ---")
    db_path = "project_alpha.json"
    # Remove existing DB to avoid stale data
    if os.path.exists(db_path):
        os.remove(db_path)
    save_graph_to_tinydb(graph, db_path)
    print(f"Saved to {db_path}")
    file_size = os.path.getsize(db_path)
    print(f"File size: {file_size:,} bytes")
    print()

    # --- Step 4: Load from TinyDB ---
    print("--- Step 4: Load from TinyDB ---")
    loaded_graph = load_graph_from_tinydb(db_path)
    print(f"Loaded graph: {loaded_graph.number_of_nodes()} nodes, {loaded_graph.number_of_edges()} edges")

    restored_book = graph_to_book(loaded_graph, "Project Alpha Restored")
    print(f"Restored book: {restored_book.name}")
    print()

    # --- Step 5: Verify round-trip integrity ---
    print("--- Step 5: Verify Round-Trip ---")
    planning = restored_book.root_nodes[0]
    requirements = planning.children[0]
    design = planning.children[1]

    print(f"Planning -> Requirements: status = {requirements.properties.get('status')}")
    print(f"Planning -> Design: status = {design.properties.get('status')}")

    assert planning.name == "Planning", f"Expected 'Planning', got '{planning.name}'"
    assert requirements.properties.get("status") == "complete"
    assert design.properties.get("status") == "in_progress"
    print("Round-trip integrity verified!")
    print()

    # --- Step 6: Management module ---
    print("--- Step 6: Management Module ---")
    json_path = "project_alpha_backup.json"
    copy_book(book, json_path, "json")
    print(f"Saved as JSON: {json_path}")

    loaded = load_book(json_path, "json", "Loaded Book")
    loaded_count = count_nodes(loaded.root_nodes)
    print(f"Loaded from JSON: {loaded.name} ({loaded_count} nodes)")

    # SyncManager
    replica_json = "replica_1.json"
    replica_gml = "replica_2.gml"
    sync = SyncManager()
    sync.add_replica(replica_json, "json")
    sync.add_replica(replica_gml, "gml")
    sync.update_replicas(book)
    print(f"SyncManager: {len(sync.replicas)} replicas updated")
    print()

    # Clean up
    for f in [db_path, json_path, replica_json, replica_gml]:
        if os.path.exists(f):
            os.remove(f)

    print("All steps completed successfully!")


if __name__ == "__main__":
    main()
