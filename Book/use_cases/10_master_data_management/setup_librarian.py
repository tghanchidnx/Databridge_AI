from book import Book, Node, from_list, get_logger
import csv

logger = get_logger(__name__)

def load_csv(file_path: str) -> list:
    """Loads data from a CSV file."""
    with open(file_path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def create_customer_hierarchy():
    """
    Simulates creating a Customer hierarchy.
    """
    customer_data = load_csv("customer_master.csv")
    root_nodes = from_list(customer_data, parent_col=None, child_col="customer_id", name_col="customer_name")
    
    customer_book = Book(name="Customer Master", root_nodes=root_nodes)
    return customer_book

def main():
    logger.info("Simulating Master Data Management with Librarian...")

    # Simulate creating a Customer Master project in Librarian
    logger.info("Creating 'Customer Master' hierarchy (simulated Librarian project)...")
    customer_master_hierarchy = create_customer_hierarchy()
    
    logger.info("Customer Master Hierarchy created:")
    def print_hierarchy(nodes, indent=""):
        for node in nodes:
            print(f"{indent}{node.name} (ID: {node.properties.get('customer_id')})")
            print_hierarchy(node.children, indent + "  ")
    print_hierarchy(customer_master_hierarchy.root_nodes)

    logger.info("\nMaster Data Management simulation completed.")

if __name__ == "__main__":
    main()

