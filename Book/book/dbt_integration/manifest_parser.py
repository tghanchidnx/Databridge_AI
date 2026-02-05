from book import Book, Node, get_logger
from dbt_artifacts_parser.parser import parse_manifest
import json
from typing import Dict

logger = get_logger(__name__)

def create_book_from_dbt_manifest(manifest_path: str) -> Book:
    """
    Creates a Book object from a dbt manifest.json file.
    """
    logger.info(f"Creating Book from dbt manifest: {manifest_path}...")

    with open(manifest_path, "r") as fp:
        manifest_dict = json.load(fp)
    
    manifest = parse_manifest(manifest_dict)

    book = Book(name=f"dbt Project: {manifest.metadata.project_name}")

    nodes: Dict[str, Node] = {}

    # First pass: create all nodes
    for node_id, node_info in manifest.nodes.items():
        nodes[node_id] = Node(
            name=node_info.name,
            properties={
                "resource_type": node_info.resource_type.value,
                "package_name": node_info.package_name,
                "path": node_info.path,
                "original_file_path": node_info.original_file_path,
                "unique_id": node_info.unique_id,
            }
        )

    # Second pass: build the hierarchy
    root_nodes = []
    for node_id, node_info in manifest.nodes.items():
        node = nodes[node_id]
        
        # Check for parents
        if node_info.depends_on:
            for parent_id in node_info.depends_on.nodes:
                if parent_id in nodes:
                    parent_node = nodes[parent_id]
                    parent_node.children.append(node)
        else:
            # If no parents, it's a root node
            # This is a simplification; dbt graphs can have multiple roots (sources)
            # For visualization, we'll add all nodes without parents as roots.
            # A better approach might be to create a single root "dbt project" node.
            
            # For now, we will add only sources as root nodes
            if node.properties.get("resource_type") == "source":
                 root_nodes.append(node)


    book.root_nodes = root_nodes
    
    # A simple check if no sources are defined as roots
    if not book.root_nodes:
        logger.warning("No sources found as root nodes. Adding all nodes without parents as roots.")
        book.root_nodes = [n for n in nodes.values() if not any(n in child.children for child in nodes.values())]


    logger.info(f"Successfully created Book from dbt manifest. Found {len(nodes)} nodes.")
    return book
