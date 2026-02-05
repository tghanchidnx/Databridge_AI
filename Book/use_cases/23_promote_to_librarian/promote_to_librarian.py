from book import Book, Node, get_logger
from tinydb import TinyDB, Query
import uuid

logger = get_logger(__name__)

def promote_book_to_librarian(book: Book, librarian_db_path: str):
    """
    Simulates promoting a Book to a Librarian project by saving it to a TinyDB database.
    """
    db = TinyDB(librarian_db_path)
    projects_table = db.table('projects')
    hierarchies_table = db.table('hierarchies')

    # Create a new project in the Librarian
    project_id = str(uuid.uuid4())
    projects_table.insert({
        "id": project_id,
        "name": book.name,
        "description": book.metadata.get("description", ""),
    })

    # Recursively add nodes to the hierarchies table
    def add_nodes_to_db(nodes, parent_id=None):
        for node in nodes:
            node_id = str(uuid.uuid4())
            hierarchies_table.insert({
                "id": node_id,
                "project_id": project_id,
                "name": node.name,
                "parent_id": parent_id,
                "properties": node.properties,
            })
            if node.children:
                add_nodes_to_db(node.children, node_id)

    add_nodes_to_db(book.root_nodes)
    db.close()
    logger.info(f"Successfully promoted Book '{book.name}' to Librarian project '{project_id}'.")
    return project_id

def main():
    """
    This script demonstrates the workflow for promoting a prototyped Book
    into a centrally managed Librarian project.
    """
    logger.info("Starting 'Promote Book to Librarian' use case...")

    # 1. Create a sample Book to be promoted
    logger.info("Creating a sample 'Sales Region' Book...")
    prototyped_book = Book(
        name="Q3 Sales Plan",
        metadata={"description": "A prototyped sales hierarchy for Q3."},
        root_nodes=[
            Node(name="North America", properties={"target": 1000000}, children=[
                Node(name="USA", properties={"target": 700000}),
                Node(name="Canada", properties={"target": 300000}),
            ]),
            Node(name="Europe", properties={"target": 800000}, children=[
                Node(name="Germany", properties={"target": 400000}),
                Node(name="France", properties={"target": 400000}),
            ]),
        ]
    )

    # 2. Promote the Book to the Librarian (simulated)
    librarian_db_path = "librarian_db.json"
    promote_book_to_librarian(prototyped_book, librarian_db_path)

    # 3. Verify the promotion by querying the Librarian's database
    logger.info("\n--- Verifying promotion in Librarian DB ---")
    db = TinyDB(librarian_db_path)
    projects_table = db.table('projects')
    hierarchies_table = db.table('hierarchies')
    
    Project = Query()
    project = projects_table.get(Project.name == "Q3 Sales Plan")
    
    if project:
        logger.info(f"Found project: {project['name']} (ID: {project['id']})")
        
        Hierarchy = Query()
        project_nodes = hierarchies_table.search(Hierarchy.project_id == project['id'])
        logger.info(f"Found {len(project_nodes)} nodes in the hierarchy.")
    else:
        logger.info("Project not found in Librarian DB.")
        
    # Clean up
    db.close()
    os.remove(librarian_db_path)
    logger.info(f"\nCleaned up {librarian_db_path}.")

    logger.info("\n'Promote Book to Librarian' use case completed.")

if __name__ == "__main__":
    import os
    main()
