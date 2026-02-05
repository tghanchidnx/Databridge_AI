from book import Book, Node, from_list, get_logger, add_property
import csv

logger = get_logger(__name__)

def load_csv(file_path: str) -> list:
    """Loads data from a CSV file."""
    with open(file_path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def main():
    """
    This script demonstrates data quality monitoring using the Book library.
    """
    logger.info("Starting data quality monitoring use case...")

    # 1. Load product data into a Book
    logger.info("Loading product data from product_data.csv into a Book...")
    product_data_raw = load_csv("product_data.csv")
    product_nodes = from_list(product_data_raw, parent_col=None, child_col="product_id", name_col="product_name")
    product_book = Book(name="Product Data", root_nodes=product_nodes)

    # 2. Iterate through nodes and perform data quality checks
    logger.info("Performing data quality checks...")
    quality_issues_found = False

    for node in product_book.root_nodes:
        # Check for missing values
        if not node.properties.get("price"):
            node.flags["missing_price"] = True
            add_property(node, "quality_comment", "Missing price.")
            quality_issues_found = True
        if not node.properties.get("stock_level"):
            node.flags["missing_stock"] = True
            add_property(node, "quality_comment", "Missing stock level.")
            quality_issues_found = True
        
        # Check for invalid data types and out-of-range values
        try:
            price = float(node.properties.get("price", 0))
            if price < 0:
                node.flags["negative_price"] = True
                add_property(node, "quality_comment", "Negative price detected.")
                quality_issues_found = True
        except ValueError:
            node.flags["invalid_price_type"] = True
            add_property(node, "quality_comment", "Invalid price data type.")
            quality_issues_found = True

        try:
            stock_level = int(node.properties.get("stock_level", 0))
            if stock_level < 0:
                node.flags["negative_stock"] = True
                add_property(node, "quality_comment", "Negative stock level detected.")
                quality_issues_found = True
        except ValueError:
            node.flags["invalid_stock_type"] = True
            add_property(node, "quality_comment", "Invalid stock level data type.")
            quality_issues_found = True
        
        if not node.properties.get("category"):
            node.flags["missing_category"] = True
            add_property(node, "quality_comment", "Missing category.")
            quality_issues_found = True

    # 3. Print data quality report
    logger.info("--- Data Quality Report ---")
    if not quality_issues_found:
        logger.info("No major data quality issues found.")
    else:
        for node in product_book.root_nodes:
            if any(node.flags.values()): # If any flag is True
                print(f"Product: {node.name} (ID: {node.properties.get('product_id')})")
                for flag, value in node.flags.items():
                    if value:
                        print(f"  - Issue: {flag.replace('_', ' ').title()}")
                print(f"  - Comment: {node.properties.get('quality_comment', 'N/A')}")
                print("-" * 30)

    logger.info("Data quality monitoring use case completed.")

if __name__ == "__main__":
    main()
