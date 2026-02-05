from book import (
    Book,
    Node,
    from_list,
    get_logger,
    add_property,
    get_property,
    execute_formulas,
    Formula,
)
import csv

logger = get_logger(__name__)

def load_csv(file_path: str) -> list:
    """Loads data from a CSV file."""
    with open(file_path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def set_usd_sales(nodes: list, book: Book):
    """
    Recursively calculates and sets the sales_usd property for each node.
    """
    for node in nodes:
        rate = get_property(node, book, "usd_conversion_rate")
        sales_local_str = node.properties.get("sales_local", "0")
        if sales_local_str:
            sales_local = float(sales_local_str)
            add_property(node, "sales_usd", sales_local * rate)
        
        if node.children:
            set_usd_sales(node.children, book)

def aggregate_sales(nodes: list):
    """
    Recursively aggregates sales_usd from children to parents.
    """
    for node in nodes:
        if node.children:
            aggregate_sales(node.children)
            total_child_sales = sum(child.properties.get("sales_usd", 0) for child in node.children)
            # If the node has its own sales, add it to the aggregated children sales
            node_sales = node.properties.get("sales_usd", 0)
            add_property(node, "sales_usd", node_sales + total_child_sales)

def main():
    """
    This script demonstrates hierarchical aggregation with global and local property overrides.
    """
    logger.info("Starting hierarchical aggregation use case...")

    # 1. Load data
    logger.info("Loading global sales data...")
    data = load_csv("global_sales.csv")

    # 2. Create a Book with global properties
    logger.info("Creating Book with global USD conversion rates...")
    global_conversion_rates = {
        "usd_conversion_rate": 1.0,
        "CAD": 0.73,
        "EUR": 1.08,
        "GBP": 1.27,
    }
    sales_book = Book(name="Global Sales Report", global_properties=global_conversion_rates)

    # 3. Build hierarchy
    logger.info("Building sales hierarchy...")
    sales_book.root_nodes = from_list(data, parent_col="parent_entity", child_col="entity_id", name_col="entity_name")

    # 4. Set local property overrides for currency conversion
    logger.info("Setting local currency conversion overrides...")
    all_nodes = sales_book.root_nodes[0].children
    for node in all_nodes: # North America, Europe
        for child in node.children: # USA, Canada, Germany, France, UK
            currency = child.properties.get("local_currency")
            if currency and currency != "USD":
                rate = get_property(child, sales_book, currency)
                add_property(child, "usd_conversion_rate", rate)
                for grandchild in child.children:
                    add_property(grandchild, "usd_conversion_rate", rate)


    # 5. Calculate USD sales for all nodes
    logger.info("Calculating USD sales for all nodes...")
    set_usd_sales(sales_book.root_nodes, sales_book)
    
    # 6. Aggregate sales up the hierarchy
    logger.info("Aggregating sales up the hierarchy...")
    aggregate_sales(sales_book.root_nodes)

    # 7. Print the final report
    logger.info("Aggregated Global Sales Report (in USD):")
    def print_hierarchy(nodes, indent=""):
        for node in nodes:
            sales_usd = node.properties.get("sales_usd", 0)
            print(f"{indent}{node.name}: ${sales_usd:,.2f}")
            print_hierarchy(node.children, indent + "  ")

    print_hierarchy(sales_book.root_nodes)

if __name__ == "__main__":
    main()
