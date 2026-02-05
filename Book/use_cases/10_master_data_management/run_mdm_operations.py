from book import get_logger, Book, Node
from setup_librarian import create_customer_hierarchy
import json

logger = get_logger(__name__)

def main():
    """
    This script simulates interactions with the Librarian for Master Data Management.
    """
    logger.info("Simulating Librarian interactions for Master Data Management use case...")

    # In a real scenario, the Librarian would manage multiple projects
    # We will simulate having a 'Customer Master' project and a 'Product Master' project.
    
    # Simulate loading projects from Librarian
    # (Librarian's 'list_hierarchy_projects' tool output)
    simulated_librarian_projects = [
        {"id": "proj-cust-001", "name": "Customer Master Data", "hierarchies_count": 1},
        {"id": "proj-prod-002", "name": "Product Master Data", "hierarchies_count": 1}
    ]

    logger.info("\n--- Librarian: List Projects ---")
    logger.info("Command: databridge project list")
    for project in simulated_librarian_projects:
        logger.info(f"- Project ID: {project['id']}, Name: {project['name']}, Hierarchies: {project['hierarchies_count']}")

    # Simulate loading a specific hierarchy from Librarian
    logger.info("\n--- Librarian: Get Customer Master Hierarchy ---")
    logger.info("Command: databridge hierarchy show proj-cust-001")
    customer_master_book = create_customer_hierarchy()
    logger.info(f"Loaded Hierarchy: {customer_master_book.name}")

    # Simulate a drill-down operation
    logger.info("\n--- Librarian: Drill down into 'North' region ---")
    logger.info("Command: databridge hierarchy tree proj-cust-001 --path 'North'")
    
    # For demonstration, we'll manually find the 'North' region in our simulated Book
    north_region_node = next((node for node in customer_master_book.root_nodes if node.properties.get('region') == 'North'), None)

    if north_region_node:
        logger.info(f"Details for '{north_region_node.name}' (ID: {north_region_node.properties.get('customer_id')}):")
        for key, value in north_region_node.properties.items():
            logger.info(f"  {key}: {value}")
        # In a real Librarian, it would traverse children based on hierarchy
        logger.info("  Children in North Region:")
        for node in customer_master_book.root_nodes:
            if node.properties.get('region') == north_region_node.properties.get('region'):
                logger.info(f"    - {node.name} (ID: {node.properties.get('customer_id')})")
    else:
        logger.info("North region node not found in simulated hierarchy.")

    logger.info("Master Data Management use case completed.")

if __name__ == "__main__":
    main()
