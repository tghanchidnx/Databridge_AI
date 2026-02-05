from book import Book, Node, from_list, get_logger, add_property
from setup_librarian import create_income_statement_hierarchy
import csv

logger = get_logger(__name__)

def load_csv_to_book(file_path: str, book_name: str) -> Book:
    """Loads a simple two-column CSV into a Book object."""
    nodes = []
    with open(file_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            node = Node(name=row["account"])
            add_property(node, "amount", float(row["amount"]))
            nodes.append(node)
    return Book(name=book_name, root_nodes=nodes)

def find_node_by_name(nodes: list, name: str) -> Node | None:
    """Finds a node in a list of nodes by its name."""
    for node in nodes:
        if node.name == name:
            return node
        if node.children:
            found = find_node_by_name(node.children, name)
            if found:
                return found
    return None

def main():
    """
    This script performs a variance analysis by comparing actuals from an SEC
    filing to a budget, using a hierarchy from the Librarian.
    """
    logger.info("Starting variance analysis use case...")

    # 1. Get the standard income statement hierarchy from the "Librarian"
    logger.info("Loading hierarchy from Librarian...")
    master_hierarchy = create_income_statement_hierarchy()

    # 2. Load the actuals and budget data into Book objects
    logger.info("Loading actuals and budget data...")
    actuals_book = load_csv_to_book("actuals.csv", "Actuals")
    budget_book = load_csv_to_book("budget.csv", "Budget")

    # 3. Create the variance analysis report Book
    variance_report = Book(name="Variance Analysis")

    # 4. Perform the variance analysis
    logger.info("Performing variance analysis...")
    
    for node in master_hierarchy.root_nodes:
        actual_node = find_node_by_name(actuals_book.root_nodes, node.name)
        budget_node = find_node_by_name(budget_book.root_nodes, node.name)

        actual_amount = actual_node.properties.get("amount", 0) if actual_node else 0
        budget_amount = budget_node.properties.get("amount", 0) if budget_node else 0
        
        variance = actual_amount - budget_amount

        variance_node = Node(name=node.name)
        add_property(variance_node, "actual", actual_amount)
        add_property(variance_node, "budget", budget_amount)
        add_property(variance_node, "variance", variance)
        variance_report.root_nodes.append(variance_node)

    # 5. Print the report
    logger.info("Variance Analysis Report:")
    print(f"{'Account':<25} {'Actual':>15} {'Budget':>15} {'Variance':>15}")
    print("-" * 70)
    for node in variance_report.root_nodes:
        actual = node.properties.get("actual", 0)
        budget = node.properties.get("budget", 0)
        variance = node.properties.get("variance", 0)
        print(f"{node.name:<25} ${actual:,.0f} ${budget:,.0f} ${variance:,.0f}")

if __name__ == "__main__":
    main()
