from book import (
    Book,
    Node,
    LinkedBook,
    get_logger,
)
from setup_librarian import create_master_coa

logger = get_logger(__name__)

def main():
    """
    This script demonstrates a version control and auditing workflow for hierarchies.
    """
    logger.info("Starting version control and auditing use case...")

    # 1. Load the master CoA from the "Librarian"
    logger.info("Loading master CoA from Librarian...")
    master_coa = create_master_coa()

    # 2. Create a LinkedBook to propose changes
    logger.info("Creating a LinkedBook to propose changes...")
    linked_coa = LinkedBook(base_book=master_coa)

    # 3. Propose changes by adding deltas
    logger.info("Proposing changes (adding deltas)...")
    
    # Find the 'Cash' node to modify it. In a real application, you'd have a more robust
    # way to find nodes, but for this example, we'll traverse the structure.
    assets_node = next((n for n in master_coa.root_nodes if n.name == "Assets"), None)
    current_assets_node = next((n for n in assets_node.children if n.name == "Current Assets"), None)
    cash_node = next((n for n in current_assets_node.children if n.name == "Cash"), None)

    if cash_node:
        # Propose adding a new property to the 'Cash' node
        linked_coa.add_change(cash_node.id, "gl_code", "10100")
        logger.info(f"  - Proposed change: Add 'gl_code' property to 'Cash' node.")

        # Propose changing an existing property (we'll add one first for the example)
        cash_node.properties["is_liquid"] = True
        linked_coa.add_change(cash_node.id, "is_liquid", False)
        logger.info(f"  - Proposed change: Modify 'is_liquid' property on 'Cash' node.")

    # 4. Access properties from the LinkedBook
    logger.info("\nAccessing properties from the LinkedBook (reflects changes)...")
    if cash_node:
        gl_code = linked_coa.get_property(cash_node.id, "gl_code")
        is_liquid = linked_coa.get_property(cash_node.id, "is_liquid")
        logger.info(f"  - 'Cash' gl_code: {gl_code}")
        logger.info(f"  - 'Cash' is_liquid: {is_liquid}")

    # 5. Simulate auditing by logging the deltas
    logger.info("\n--- Audit Trail (Deltas) ---")
    for delta in linked_coa.deltas:
        logger.info(f"  - Node ID: {delta.node_id}, Key: {delta.key}, New Value: {delta.new_value}")

    # 6. Materialize the LinkedBook into a new version
    logger.info("\nMaterializing the LinkedBook into a new version of the CoA...")
    new_coa_version = linked_coa.to_book("Master Chart of Accounts v2")
    
    # Verify the changes in the new version
    cash_node_v2 = find_node_by_name(new_coa_version.root_nodes, "Cash")
    if cash_node_v2:
        logger.info(f"New CoA version name: {new_coa_version.name}")
        logger.info(f"  - 'Cash' gl_code in v2: {cash_node_v2.properties['gl_code']}")
        logger.info(f"  - 'Cash' is_liquid in v2: {cash_node_v2.properties['is_liquid']}")

    logger.info("\nVersion control and auditing use case completed.")

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

if __name__ == "__main__":
    main()
