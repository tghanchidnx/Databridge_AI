from book import Book, Node, from_list, get_logger
import csv
import code

logger = get_logger(__name__)

def load_csv(file_path: str) -> list:
    """Loads data from a CSV file."""
    with open(file_path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def main():
    """
    This script loads server data into a Book and starts an interactive session.
    """
    logger.info("Starting interactive data exploration use case...")

    # Load server data into a Book
    logger.info("Loading server inventory data...")
    server_data = load_csv("server_inventory.csv")
    server_nodes = [Node(name=row["server_name"], properties=row) for row in server_data]
    server_book = Book(name="Server Inventory", root_nodes=server_nodes)

    # Start an interactive session
    logger.info("\n--- Starting Interactive Session ---")
    logger.info("The 'server_book' object is available for exploration.")
    logger.info("Type 'exit()' or press Ctrl-Z to exit.")
    
    # Create a banner with some helpful tips
    banner = (
        "Welcome to the interactive Book exploration session!\n"
        "Here are some things you can try:\n"
        "  - `print(server_book.name)`\n"
        "  - `for node in server_book.root_nodes: print(node.name, node.properties)`\n"
        "  - `online_servers = [n for n in server_book.root_nodes if n.properties.get('status') == 'online']`\n"
        "  - `print(len(online_servers))`\n"
        "  - `from book import add_property; add_property(server_book.root_nodes[0], 'owner', 'Team A')`\n"
    )
    
    local_vars = globals().copy()
    local_vars.update(locals())
    
    code.interact(banner=banner, local=local_vars)

    logger.info("\nInteractive session ended.")

if __name__ == "__main__":
    main()
