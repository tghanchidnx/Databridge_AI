from book import Book, Node, get_logger
from tinydb import TinyDB, Query
import uuid
import os

logger = get_logger(__name__)

def setup_simulated_librarian(db_path: str) -> str:
    """
    Sets up a simulated Librarian database with a sample project.
    """
    db = TinyDB(db_path)
    db.truncate()
    projects_table = db.table('projects')
    hierarchies_table = db.table('hierarchies')
    
    project_id = "proj-master-coa-001"
    projects_table.insert({
        "id": project_id,
        "name": "Master Chart of Accounts",
    })

    # Add nodes to the hierarchies table
    nodes_to_add = [
        {'id': 'n1', 'project_id': project_id, 'name': 'Assets', 'parent_id': None},
        {'id': 'n2', 'project_id': project_id, 'name': 'Current Assets', 'parent_id': 'n1'},
        {'id': 'n3', 'project_id': project_id, 'name': 'Cash', 'parent_id': 'n2'},
        {'id': 'n4', 'project_id': project_id, 'name': 'Liabilities', 'parent_id': None},
        {'id': 'n5', 'project_id': project_id, 'name': 'Equity', 'parent_id': None},
    ]
    hierarchies_table.insert_multiple(nodes_to_add)
    db.close()
    return project_id

def checkout_librarian_hierarchy(project_id: str, librarian_db_path: str) -> Book:
    """
    Simulates checking out a hierarchy from the Librarian into a Book object.
    """
    db = TinyDB(librarian_db_path)
    projects_table = db.table('projects')
    hierarchies_table = db.table('hierarchies')
    
    Project = Query()
    project = projects_table.get(Project.id == project_id)
    if not project:
        raise ValueError(f"Project with ID '{project_id}' not found.")

    Hierarchy = Query()
    project_nodes_data = hierarchies_table.search(Hierarchy.project_id == project_id)
    
    nodes = {item['id']: Node(name=item['name'], properties=item) for item in project_nodes_data}
    
    root_nodes = []
    for item in project_nodes_data:
        node = nodes[item['id']]
        if item['parent_id']:
            parent = nodes[item['parent_id']]
            parent.children.append(node)
        else:
            root_nodes.append(node)

    db.close()
    return Book(name=project['name'], root_nodes=root_nodes)

def main():
    """
    This script demonstrates the workflow for checking out a hierarchy from the Librarian.
    """
    logger.info("Starting 'Checkout from Librarian' use case...")

    # 1. Set up the simulated Librarian database
    librarian_db_path = "librarian_db.json"
    project_id = setup_simulated_librarian(librarian_db_path)
    logger.info(f"Simulated Librarian database created at '{librarian_db_path}' with project '{project_id}'.")

    # 2. Checkout the hierarchy from the Librarian
    logger.info(f"Checking out hierarchy for project '{project_id}'...")
    checked_out_book = checkout_librarian_hierarchy(project_id, librarian_db_path)

    # 3. Verify the checkout
    logger.info("\n--- Verifying Checkout ---")
    logger.info(f"Checked out book name: {checked_out_book.name}")
    logger.info("Hierarchy structure:")
    def print_hierarchy(nodes, indent=""):
        for node in nodes:
            print(f"{indent}- {node.name}")
            print_hierarchy(node.children, indent + "  ")
    print_hierarchy(checked_out_book.root_nodes)

    # Clean up
    os.remove(librarian_db_path)
    logger.info(f"\nCleaned up {librarian_db_path}.")

    logger.info("'Checkout from Librarian' use case completed.")

if __name__ == "__main__":
    main()
