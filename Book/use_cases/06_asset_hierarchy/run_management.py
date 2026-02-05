from book import (
    Book,
    Node,
    from_list,
    get_logger,
    add_property,
    update_property,
    copy_book,
    load_book,
)
from setup_librarian import create_asset_hierarchy
import csv
import os

logger = get_logger(__name__)

def load_csv(file_path: str) -> list:
    """Loads data from a CSV file."""
    with open(file_path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))

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
    This script demonstrates managing an asset hierarchy using Librarian (simulated) and Book.
    """
    logger.info("Starting asset hierarchy management use case...")

    # 1. Simulate getting the master asset hierarchy from the Librarian
    logger.info("Getting master asset hierarchy from Librarian (simulated)...")
    master_hierarchy_book = create_asset_hierarchy()
    logger.info(f"Master Hierarchy Name: {master_hierarchy_book.name}")

    # 2. Ingest a subset of asset data for local manipulation (e.g., from CSV)
    logger.info("Ingesting asset inventory data from asset_inventory.csv into a local Book...")
    asset_data = load_csv("asset_inventory.csv")
    local_asset_nodes = from_list(asset_data, parent_col="parent_asset_id", child_col="asset_id", name_col="asset_name")
    local_asset_book = Book(name="Local Asset View", root_nodes=local_asset_nodes)

    # 3. Modify a property of a specific asset in the local Book
    logger.info("Modifying a property of 'Desk 1' in the local Book...")
    desk1_node = find_node_by_name(local_asset_book.root_nodes, "Desk 1")
    if desk1_node:
        current_status = desk1_node.properties.get("status")
        update_property(desk1_node, "status", "Under Maintenance")
        logger.info(f"Updated 'Desk 1' status from '{current_status}' to '{desk1_node.properties['status']}'")
    else:
        logger.warning("'Desk 1' not found in local asset book.")

    # 4. Add a new property to 'Server Rack 1'
    logger.info("Adding a new property 'last_checked' to 'Server Rack 1'...")
    server_rack_node = find_node_by_name(local_asset_book.root_nodes, "Server Rack 1")
    if server_rack_node:
        add_property(server_rack_node, "last_checked", "2026-02-04")
        logger.info(f"Added 'last_checked' to 'Server Rack 1': {server_rack_node.properties['last_checked']}")
    else:
        logger.warning("'Server Rack 1' not found in local asset book.")

    # 5. Export the local Book to a temporary GML file
    output_gml_path = "local_asset_view.gml"
    logger.info(f"Exporting local asset Book to {output_gml_path}...")
    copy_book(local_asset_book, output_gml_path, "gml")
    logger.info(f"Local Book successfully exported to {output_gml_path}")

    # 6. (Optional) Load the GML back to verify
    logger.info(f"Loading {output_gml_path} back to verify...")
    loaded_book = load_book(output_gml_path, "gml", "Reloaded Asset View")
    desk1_loaded_node = find_node_by_name(loaded_book.root_nodes, "Desk 1")
    if desk1_loaded_node:
        logger.info(f"Loaded 'Desk 1' status: {desk1_loaded_node.properties['status']}")
    
    # Clean up the temporary file
    os.remove(output_gml_path)
    logger.info(f"Cleaned up {output_gml_path}")

    logger.info("Asset hierarchy management use case completed.")

if __name__ == "__main__":
    main()
