from book import (
    Book,
    Node,
    get_logger,
    dbt_integration,
)
import os
import json

logger = get_logger(__name__)

def create_dummy_manifest():
    """
    Creates a dummy manifest.json file for demonstration purposes.
    """
    manifest_data = {
        "metadata": {
            "project_name": "My dbt Project"
        },
        "nodes": {
            "source.my_dbt_project.my_source.my_table": {
                "name": "my_table",
                "resource_type": "source",
                "package_name": "my_dbt_project",
                "path": "models/my_source.yml",
                "original_file_path": "models/my_source.yml",
                "unique_id": "source.my_dbt_project.my_source.my_table",
                "depends_on": {"nodes": []}
            },
            "model.my_dbt_project.my_model": {
                "name": "my_model",
                "resource_type": "model",
                "package_name": "my_dbt_project",
                "path": "models/my_model.sql",
                "original_file_path": "models/my_model.sql",
                "unique_id": "model.my_dbt_project.my_model",
                "depends_on": {"nodes": ["source.my_dbt_project.my_source.my_table"]}
            }
        }
    }
    with open("manifest.json", "w") as f:
        json.dump(manifest_data, f, indent=2)

def main():
    """
    This script demonstrates the bi-directional integration between the Book
    library and a dbt project.
    """
    logger.info("Starting dbt integration use case...")

    # --- Part 1: Book -> dbt ---
    logger.info("\n--- Part 1: Generating a dbt project from a Book ---")
    
    # 1. Create a sample Book
    product_hierarchy = Book(name="Product Hierarchy", root_nodes=[
        Node(name="All Products", id="1", children=[
            Node(name="Electronics", id="2"),
        ]),
    ])

    # 2. Generate a dbt project from the Book
    dbt_integration.generate_dbt_project_from_book(product_hierarchy)

    # --- Part 2: dbt -> Book ---
    logger.info("\n--- Part 2: Creating a Book from a dbt manifest ---")
    
    # 3. Create a dummy manifest.json file (simulating 'dbt compile')
    logger.info("Creating a dummy manifest.json file...")
    create_dummy_manifest()

    # 4. Create a Book from the manifest
    dbt_book = dbt_integration.create_book_from_dbt_manifest("manifest.json")

    # 5. Print the structure of the dbt Book
    logger.info("\n--- dbt Project as a Book ---")
    def print_hierarchy(nodes, indent=""):
        for node in nodes:
            print(f"{indent}- {node.name} ({node.properties.get('resource_type')})")
            print_hierarchy(node.children, indent + "  ")
    
    print_hierarchy(dbt_book.root_nodes)

    # Clean up
    os.remove("manifest.json")
    # You would also remove the generated dbt project directory in a real scenario
    logger.info("\ndbt integration use case completed.")

if __name__ == "__main__":
    main()
