"""
Tutorial 3: Librarian Promote & Checkout Workflow
===================================================
Demonstrates: Promote Book to Librarian, checkout, modify, re-promote.

Usage:
    cd C:\\Users\\telha\\Databridge_AI
    set PYTHONPATH=./Book
    python Book/use_cases/tutorial_3_librarian_workflow/run_tutorial.py
"""

import os
import uuid
from tinydb import TinyDB, Query
from book import Book, Node, get_logger

logger = get_logger(__name__)


def promote_book_to_librarian(book, db_path):
    """Promote a Book to a simulated Librarian project."""
    db = TinyDB(db_path)
    projects = db.table("projects")
    hierarchies = db.table("hierarchies")

    project_id = str(uuid.uuid4())
    projects.insert({
        "id": project_id,
        "name": book.name,
        "description": book.metadata.get("description", ""),
    })

    def add_nodes(nodes, parent_id=None):
        for node in nodes:
            node_id = str(uuid.uuid4())
            hierarchies.insert({
                "id": node_id,
                "project_id": project_id,
                "name": node.name,
                "parent_id": parent_id,
                "properties": node.properties,
            })
            if node.children:
                add_nodes(node.children, node_id)

    add_nodes(book.root_nodes)
    db.close()
    return project_id


def checkout_librarian_hierarchy(project_id, db_path):
    """Checkout a Librarian project into a Book."""
    db = TinyDB(db_path)
    projects = db.table("projects")
    hierarchies = db.table("hierarchies")

    project = projects.get(Query().id == project_id)
    if not project:
        db.close()
        raise ValueError(f"Project '{project_id}' not found")

    node_data = hierarchies.search(Query().project_id == project_id)

    nodes = {item["id"]: Node(name=item["name"], properties=item.get("properties", {}))
             for item in node_data}

    root_nodes = []
    for item in node_data:
        node = nodes[item["id"]]
        if item["parent_id"]:
            parent = nodes[item["parent_id"]]
            parent.children.append(node)
        else:
            root_nodes.append(node)

    db.close()
    return Book(name=project["name"], root_nodes=root_nodes)


def count_nodes(nodes):
    total = len(nodes)
    for n in nodes:
        total += count_nodes(n.children)
    return total


def print_tree(nodes, indent=""):
    for node in nodes:
        target = node.properties.get("target", "")
        extra = f" (target: {target:,})" if target else ""
        print(f"{indent}{node.name}{extra}")
        print_tree(node.children, indent + "  ")


def main():
    print("=== Tutorial 3: Librarian Promote & Checkout Workflow ===\n")
    db_path = "tutorial_librarian.json"

    # --- Step 1: Create a financial hierarchy ---
    print("--- Step 1: Create Financial Hierarchy ---")
    book = Book(
        name="FY2025 P&L Hierarchy",
        metadata={"description": "Profit & Loss hierarchy for fiscal year 2025"},
        root_nodes=[
            Node(name="Revenue", properties={"target": 10000000}, children=[
                Node(name="Product Revenue", properties={"target": 7000000}),
                Node(name="Service Revenue", properties={"target": 3000000}),
            ]),
            Node(name="Cost of Goods Sold", properties={"target": 4000000}, children=[
                Node(name="Materials", properties={"target": 2500000}),
                Node(name="Labor", properties={"target": 1500000}),
            ]),
            Node(name="Operating Expenses", properties={"target": 3000000}, children=[
                Node(name="R&D", properties={"target": 1500000}),
                Node(name="SG&A", properties={"target": 1000000}),
                Node(name="Depreciation", properties={"target": 500000}),
            ]),
        ]
    )

    node_count = count_nodes(book.root_nodes)
    print(f"Book: {book.name} ({node_count} nodes)")
    print_tree(book.root_nodes)
    print()

    # --- Step 2: Promote to Librarian ---
    print("--- Step 2: Promote to Librarian ---")
    project_id = promote_book_to_librarian(book, db_path)
    print(f"Promoted to project: {project_id}")

    # Count nodes in DB
    db = TinyDB(db_path)
    h_table = db.table("hierarchies")
    db_nodes = h_table.search(Query().project_id == project_id)
    print(f"Nodes in Librarian DB: {len(db_nodes)}")
    db.close()
    print()

    # --- Step 3: Verify ---
    print("--- Step 3: Verify Promotion ---")
    db = TinyDB(db_path)
    p_table = db.table("projects")
    project = p_table.get(Query().id == project_id)
    print(f"Project found: {project['name']}")
    print(f"All {len(db_nodes)} nodes present in database.")
    db.close()
    print()

    # --- Step 4: Checkout from Librarian ---
    print("--- Step 4: Checkout from Librarian ---")
    checked_out = checkout_librarian_hierarchy(project_id, db_path)
    co_count = count_nodes(checked_out.root_nodes)
    print(f"Checked out: {checked_out.name} ({co_count} nodes)")
    print_tree(checked_out.root_nodes)
    print()

    # --- Step 5: Modify the checked-out Book ---
    print("--- Step 5: Modify Checked-Out Book ---")
    opex = checked_out.root_nodes[2]  # Operating Expenses
    opex.children.append(Node(name="Marketing", properties={"target": 800000}))
    new_count = count_nodes(checked_out.root_nodes)
    print(f"Added 'Marketing' under Operating Expenses")
    print(f"Node count: {new_count}")
    assert new_count == node_count + 1
    print()

    # --- Step 6: Re-promote ---
    print("--- Step 6: Re-Promote Modified Book ---")
    new_project_id = promote_book_to_librarian(checked_out, db_path)
    print(f"Re-promoted to new project: {new_project_id}")

    db = TinyDB(db_path)
    h_table = db.table("hierarchies")
    new_db_nodes = h_table.search(Query().project_id == new_project_id)
    print(f"Nodes in updated project: {len(new_db_nodes)}")
    assert len(new_db_nodes) == new_count
    db.close()
    print()

    # Clean up
    if os.path.exists(db_path):
        os.remove(db_path)

    print("Full round-trip completed successfully!")


if __name__ == "__main__":
    main()
