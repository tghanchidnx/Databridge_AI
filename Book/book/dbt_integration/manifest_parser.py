from book import Book, Node, get_logger
import json
from typing import Dict

logger = get_logger(__name__)


def _parse_manifest_dict(manifest_dict: dict) -> dict:
    """Parse manifest dict directly, with optional strict parsing via dbt_artifacts_parser."""
    try:
        from dbt_artifacts_parser.parser import parse_manifest
        manifest = parse_manifest(manifest_dict)
        # Convert parsed manifest nodes to simple dicts
        nodes = {}
        for node_id, node_info in manifest.nodes.items():
            nodes[node_id] = {
                "name": node_info.name,
                "resource_type": node_info.resource_type.value,
                "package_name": node_info.package_name,
                "path": node_info.path,
                "original_file_path": node_info.original_file_path,
                "unique_id": node_info.unique_id,
                "depends_on": {"nodes": list(node_info.depends_on.nodes)} if node_info.depends_on else {"nodes": []},
            }
        return {
            "project_name": manifest.metadata.project_name,
            "nodes": nodes,
        }
    except Exception:
        # Fallback: parse manifest dict directly (for simplified/demo manifests)
        logger.info("Using direct manifest parsing (strict parser unavailable or manifest is simplified).")
        project_name = manifest_dict.get("metadata", {}).get("project_name", "unknown")
        raw_nodes = manifest_dict.get("nodes", {})
        nodes = {}
        for node_id, node_info in raw_nodes.items():
            deps = node_info.get("depends_on", {})
            nodes[node_id] = {
                "name": node_info.get("name", node_id),
                "resource_type": node_info.get("resource_type", "model"),
                "package_name": node_info.get("package_name", project_name),
                "path": node_info.get("path", ""),
                "original_file_path": node_info.get("original_file_path", ""),
                "unique_id": node_info.get("unique_id", node_id),
                "depends_on": {"nodes": deps.get("nodes", []) if isinstance(deps, dict) else []},
            }
        return {"project_name": project_name, "nodes": nodes}


def create_book_from_dbt_manifest(manifest_path: str) -> Book:
    """
    Creates a Book object from a dbt manifest.json file.
    """
    logger.info(f"Creating Book from dbt manifest: {manifest_path}...")

    with open(manifest_path, "r") as fp:
        manifest_dict = json.load(fp)

    parsed = _parse_manifest_dict(manifest_dict)

    book = Book(name=f"dbt Project: {parsed['project_name']}")

    nodes: Dict[str, Node] = {}

    # First pass: create all nodes
    for node_id, node_info in parsed["nodes"].items():
        nodes[node_id] = Node(
            name=node_info["name"],
            properties={
                "resource_type": node_info["resource_type"],
                "package_name": node_info["package_name"],
                "path": node_info["path"],
                "original_file_path": node_info["original_file_path"],
                "unique_id": node_info["unique_id"],
            }
        )

    # Second pass: build the hierarchy
    for node_id, node_info in parsed["nodes"].items():
        node = nodes[node_id]
        dep_nodes = node_info.get("depends_on", {}).get("nodes", [])
        for parent_id in dep_nodes:
            if parent_id in nodes:
                parent_node = nodes[parent_id]
                if node not in parent_node.children:
                    parent_node.children.append(node)

    # Find root nodes (nodes that are not children of any other node)
    all_children = set()
    for node in nodes.values():
        for child in node.children:
            all_children.add(id(child))

    root_nodes = [n for n in nodes.values() if id(n) not in all_children]

    book.root_nodes = root_nodes

    logger.info(f"Successfully created Book from dbt manifest. Found {len(nodes)} nodes.")
    return book