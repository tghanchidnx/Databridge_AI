from book import Book, Node, get_logger, add_property
import csv

logger = get_logger(__name__)

def load_csv_to_dict(file_path: str, key_column: str) -> dict:
    """Loads data from a CSV file into a dictionary keyed by a specific column."""
    data_dict = {}
    with open(file_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            data_dict[row[key_column]] = row
    return data_dict

def main():
    """
    This script demonstrates integrating data from multiple sources to create a
    360-degree customer view.
    """
    logger.info("Starting multi-source data integration use case...")

    # 1. Load data from CRM and billing systems
    logger.info("Loading data from CRM and billing systems...")
    crm_data = load_csv_to_dict("crm_data.csv", "customer_id")
    billing_data = load_csv_to_dict("billing_data.csv", "customer_id")

    # 2. Create a unified customer Book
    logger.info("Creating a unified customer Book...")
    customer_360_book = Book(name="Customer 360 View")
    
    all_customer_ids = set(crm_data.keys()) | set(billing_data.keys())

    for customer_id in all_customer_ids:
        # Get data from both sources
        crm_info = crm_data.get(customer_id, {})
        billing_info = billing_data.get(customer_id, {})

        # Create a unified node
        customer_name = crm_info.get("customer_name", f"Customer {customer_id}")
        node = Node(name=customer_name)
        
        # Add properties from both sources
        add_property(node, "crm_data", crm_info)
        add_property(node, "billing_data", billing_info)
        
        customer_360_book.root_nodes.append(node)

    # 3. Print the integrated customer view
    logger.info("\n--- Customer 360-Degree View ---")
    for node in customer_360_book.root_nodes:
        print(f"Customer: {node.name}")
        crm = node.properties.get("crm_data", {})
        billing = node.properties.get("billing_data", {})
        
        print(f"  - CRM Info:")
        print(f"    - Region: {crm.get('region', 'N/A')}")
        print(f"    - Segment: {crm.get('segment', 'N/A')}")
        print(f"    - Account Manager: {crm.get('account_manager', 'N/A')}")
        
        print(f"  - Billing Info:")
        print(f"    - Status: {billing.get('billing_status', 'N/A')}")
        print(f"    - Total Spend: ${float(billing.get('total_spend', 0)):,.2f}")
        print(f"    - Last Invoice: {billing.get('last_invoice_date', 'N/A')}")
        
        print("-" * 40)

    logger.info("\nMulti-source data integration use case completed.")

if __name__ == "__main__":
    main()
