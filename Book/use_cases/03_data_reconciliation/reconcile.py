from book import Book, Node, get_logger, add_property
from rapidfuzz import process, fuzz
import csv

logger = get_logger(__name__)

def load_csv(file_path: str) -> list:
    """Loads data from a CSV file."""
    with open(file_path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def main():
    """
    This script demonstrates a data reconciliation and deduplication workflow
    between two CSV files with data inconsistencies.
    """
    logger.info("Starting data reconciliation use case...")

    # 1. Load data from two sources
    logger.info("Loading data from source_a.csv and source_b.csv...")
    source_a = load_csv("source_a.csv")
    source_b = load_csv("source_b.csv")

    # 2. Create a Book to store the reconciled data
    reconciliation_book = Book(name="Customer Reconciliation")

    # 3. Perform fuzzy matching to identify potential matches
    logger.info("Performing fuzzy matching on customer names...")
    
    source_a_names = [customer["name"] for customer in source_a]
    source_b_names = [customer["name"] for customer in source_b]
    
    matches = []
    for name_b in source_b_names:
        # Find the best match in source A for each name in source B
        best_match = process.extractOne(name_b, source_a_names, scorer=fuzz.WRatio)
        if best_match and best_match[1] > 85: # Using a threshold of 85
            matches.append((name_b, best_match[0]))

    # 4. Create nodes for the reconciled data
    logger.info("Creating nodes for reconciled data...")
    
    matched_b_names = [match[0] for match in matches]
    matched_a_names = [match[1] for match in matches]

    # Add matched customers
    for name_b, name_a in matches:
        customer_b = next(c for c in source_b if c["name"] == name_b)
        customer_a = next(c for c in source_a if c["name"] == name_a)

        node = Node(name=name_a) # Use name from source A as the canonical name
        add_property(node, "source_a_data", customer_a)
        add_property(node, "source_b_data", customer_b)
        add_property(node, "status", "matched")
        add_property(node, "match_score", fuzz.WRatio(name_b, name_a))
        reconciliation_book.root_nodes.append(node)

    # Add orphans from source A
    for customer_a in source_a:
        if customer_a["name"] not in matched_a_names:
            node = Node(name=customer_a["name"])
            add_property(node, "source_a_data", customer_a)
            add_property(node, "status", "orphan_a")
            reconciliation_book.root_nodes.append(node)

    # Add orphans from source B
    for customer_b in source_b:
        if customer_b["name"] not in matched_b_names:
            node = Node(name=customer_b["name"])
            add_property(node, "source_b_data", customer_b)
            add_property(node, "status", "orphan_b")
            reconciliation_book.root_nodes.append(node)

    # 5. Print the reconciliation report
    logger.info("Reconciliation Report:")
    for node in reconciliation_book.root_nodes:
        status = node.properties.get('status', 'unknown')
        if status == "matched":
            score = node.properties.get('match_score', 0)
            name_a = node.properties.get('source_a_data', {}).get('name')
            name_b = node.properties.get('source_b_data', {}).get('name')
            print(f"- Matched: '{name_a}' (A) and '{name_b}' (B) with score {score:.2f}")
        else:
            name = node.name
            source = "A" if status == "orphan_a" else "B"
            print(f"- Orphan in Source {source}: '{name}'")

if __name__ == "__main__":
    main()
