from book import (
    Book,
    Node,
    LinkedBook,
    get_logger,
    add_property,
    from_list,
)
import csv
import json

logger = get_logger(__name__)

def load_csv(file_path: str) -> list:
    """Loads data from a CSV file."""
    with open(file_path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

def find_node_by_id(nodes: list, node_id: str) -> Node | None:
    """Finds a node in a list of nodes by its ID."""
    for node in nodes:
        if node.properties.get("product_id") == node_id:
            return node
    return None

def main():
    """
    This script demonstrates reconciling a Change Data Capture (CDC) log with a master inventory.
    """
    logger.info("Starting CDC reconciliation use case...")

    # 1. Load initial inventory (simulating Librarian's master data)
    logger.info("Loading initial inventory...")
    initial_inventory_data = load_csv("initial_inventory.csv")
    initial_nodes = from_list(initial_inventory_data, parent_col=None, child_col="product_id", name_col="product_name")
    master_inventory = Book(name="Master Inventory", root_nodes=initial_nodes)

    # 2. Create a LinkedBook to manage changes
    logger.info("Creating a LinkedBook for change management...")
    linked_inventory = LinkedBook(base_book=master_inventory)

    # 3. Process the CDC log and apply changes as deltas
    logger.info("Processing CDC log...")
    cdc_log = load_csv("inventory_cdc_log.csv")
    
    for change in cdc_log:
        product_id = change["product_id"]
        change_type = change["change_type"]
        new_value = change["new_value"]

        if change_type == "UPDATE":
            node = find_node_by_id(master_inventory.root_nodes, product_id)
            if node:
                linked_inventory.add_change(node.id, "stock_level", new_value)
                logger.info(f"  - Applied UPDATE for {product_id}: stock_level -> {new_value}")
        
        elif change_type == "INSERT":
            # For INSERT, we add a new node to a temporary list and will add it later
            # A more robust solution would handle this directly in the LinkedBook
            new_node_data = json.loads(new_value)
            new_node = Node(name=new_node_data["product_name"], properties={"product_id": product_id, **new_node_data})
            # This is a simplified approach for demonstration
            master_inventory.root_nodes.append(new_node)
            logger.info(f"  - Applied INSERT for {product_id}: {new_node_data}")

        elif change_type == "DELETE":
            node = find_node_by_id(master_inventory.root_nodes, product_id)
            if node:
                linked_inventory.add_change(node.id, "status", "deleted")
                logger.info(f"  - Applied DELETE for {product_id}")

    # 4. Materialize the reconciled inventory
    logger.info("\nMaterializing reconciled inventory...")
    reconciled_inventory = linked_inventory.to_book("Reconciled Inventory")

    # 5. Print the reconciliation report
    logger.info("\n--- Reconciliation Report ---")
    print(f"{ 'Product Name':<20} {'Original Stock':<15} {'New Stock':<15} {'Status':<15}")
    print("-" * 65)

    all_products = {node.properties.get("product_id"): node.name for node in master_inventory.root_nodes}
    
    for node in reconciled_inventory.root_nodes:
        product_id = node.properties.get("product_id")
        original_node = find_node_by_id(master_inventory.root_nodes, product_id)
        original_stock = original_node.properties.get("stock_level", "N/A") if original_node else "N/A"
        
        new_stock = node.properties.get("stock_level", original_stock)
        status = node.properties.get("status", "active")
        
        if status == "deleted":
            new_stock = "DELETED"

        print(f"{node.name:<20} {original_stock:<15} {new_stock:<15} {status:<15}")
    
    logger.info("\nCDC reconciliation use case completed.")

if __name__ == "__main__":
    main()
