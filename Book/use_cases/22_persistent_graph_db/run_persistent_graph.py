from book import (
    Book,
    Node,
    get_logger,
    book_to_graph,
    graph_to_book,
    save_graph_to_tinydb,
    load_graph_from_tinydb,
)
import os

logger = get_logger(__name__)

def main():
    """
    This script demonstrates saving a Book's graph representation to a persistent
    TinyDB database and loading it back.
    """
    logger.info("Starting persistent graph database use case...")

    # 1. Create a sample Book
    logger.info("Creating a sample Book...")
    sample_book = Book(name="Sample Project", root_nodes=[
        Node(name="Task 1", children=[
            Node(name="Subtask 1.1"),
            Node(name="Subtask 1.2"),
        ]),
        Node(name="Task 2"),
    ])

    # 2. Convert the Book to a networkx graph
    logger.info("Converting Book to a networkx graph...")
    graph = book_to_graph(sample_book)

    # 3. Save the graph to a TinyDB database
    db_path = "project_graph.json"
    logger.info(f"Saving graph to TinyDB database: {db_path}...")
    save_graph_to_tinydb(graph, db_path)

    # 4. Load the graph back from the TinyDB database
    logger.info(f"Loading graph from {db_path}...")
    loaded_graph = load_graph_from_tinydb(db_path)

    # 5. Convert the loaded graph back to a Book
    logger.info("Converting loaded graph back to a Book...")
    reloaded_book = graph_to_book(loaded_graph, "Reloaded Project")

    # 6. Verify the reloaded book
    logger.info("\n--- Verification ---")
    logger.info(f"Reloaded book name: {reloaded_book.name}")
    logger.info("Nodes in reloaded book:")
    def print_hierarchy(nodes, indent=""):
        for node in nodes:
            print(f"{indent}- {node.name}")
            print_hierarchy(node.children, indent + "  ")
    print_hierarchy(reloaded_book.root_nodes)

    # Clean up the database file
    os.remove(db_path)
    logger.info(f"\nCleaned up {db_path}.")

    logger.info("\nPersistent graph database use case completed.")

if __name__ == "__main__":
    main()
