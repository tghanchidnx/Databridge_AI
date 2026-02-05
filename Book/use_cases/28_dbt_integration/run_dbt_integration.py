from book import (
    Book,
    Node,
    get_logger,
    dbt_integration,
)
import os
import json
import shutil

logger = get_logger(__name__)

def create_dummy_manifest(project_name: str):
    """
    Creates a dummy manifest.json file for demonstration purposes.
    """
    manifest_data = {
        "metadata": {
            "project_name": project_name,
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
            "dbt_version": "1.9.0",
            "generated_at": "2026-02-05T00:00:00.000000Z",
            "invocation_id": "00000000-0000-0000-0000-000000000000",
            "adapter_type": "snowflake"
        },
        "nodes": {
            f"source.{project_name}.my_source.my_table": {
                "name": "my_table",
                "resource_type": "source",
                "package_name": project_name,
                "path": "models/my_source.yml",
                "original_file_path": "models/my_source.yml",
                "unique_id": f"source.{project_name}.my_source.my_table",
                "depends_on": {"nodes": []}
            },
            f"model.{project_name}.{project_name.lower().replace(' ', '_')}": {
                "name": project_name.lower().replace(' ', '_'),
                "resource_type": "model",
                "package_name": project_name,
                "path": f"models/{project_name.lower().replace(' ', '_')}.sql",
                "original_file_path": f"models/{project_name.lower().replace(' ', '_')}.sql",
                "unique_id": f"model.{project_name}.{project_name.lower().replace(' ', '_')}",
                "depends_on": {"nodes": [f"source.{project_name}.my_source.my_table"]}
            }
        }
    }
    manifest_dir = os.path.join(project_name, "target")
    os.makedirs(manifest_dir, exist_ok=True)
    manifest_path = os.path.join(manifest_dir, "manifest.json")
    with open(manifest_path, "w") as f:
        json.dump(manifest_data, f, indent=2)
    return manifest_path

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
    manifest_path = create_dummy_manifest(product_hierarchy.name)

    # 4. Create a Book from the manifest
    dbt_book = dbt_integration.create_book_from_dbt_manifest(manifest_path)

    # 5. Print the structure of the dbt Book
    logger.info("\n--- dbt Project as a Book ---")
    def print_hierarchy(nodes, indent=""):
        for node in nodes:
            print(f"{indent}- {node.name} ({node.properties.get('resource_type')})")
            print_hierarchy(node.children, indent + "  ")
    
    print_hierarchy(dbt_book.root_nodes)

    # Clean up
    shutil.rmtree(product_hierarchy.name)
    logger.info(f"\nCleaned up dbt project directory: {product_hierarchy.name}")

    logger.info("\ndbt integration use case completed.")

if __name__ == "__main__":
    main()