from typing import List, Dict, Any, Optional
from .models import Node

def from_list(
    data: List[Dict[str, Any]], parent_col: str, child_col: str, name_col: Optional[str] = None
) -> List[Node]:
    """
    Builds a hierarchy of Node objects from a list of dictionaries.

    This function assumes a parent-child relationship is defined by two
    columns in the input data.

    Args:
        data: A list of dictionaries, where each dictionary represents a record.
        parent_col: The name of the column containing the parent's identifier.
        child_col: The name of the column containing the child's identifier.
        name_col: The name of the column to use for the node's name. 
                  If not provided, child_col is used.

    Returns:
        A list of root Node objects.
    """
    nodes: Dict[str, Node] = {}
    root_nodes: List[Node] = []

    # Create all nodes
    for item in data:
        child_id = item.get(child_col)
        if not child_id:
            continue

        node_name = item.get(name_col) if name_col else child_id

        if child_id not in nodes:
            nodes[child_id] = Node(name=node_name, properties=item)
        else:
            nodes[child_id].properties.update(item)

    # Build the hierarchy
    for item in data:
        child_id = item.get(child_col)
        parent_id = item.get(parent_col)

        if not child_id:
            continue

        node = nodes[child_id]

        if parent_id and parent_id in nodes:
            parent_node = nodes[parent_id]
            parent_node.children.append(node)
        elif not parent_id:
            root_nodes.append(node)

    return root_nodes


def sort_nodes(nodes: List[Node], sort_by: str, reverse: bool = False):
    """
    Recursively sorts the children of each node in a list of nodes.

    Args:
        nodes: A list of nodes to sort.
        sort_by: The key in the node's properties to sort by.
        reverse: Whether to sort in descending order.
    """
    for node in nodes:
        if node.children:
            node.children.sort(
                key=lambda n: n.properties.get(sort_by), reverse=reverse
            )
            sort_nodes(node.children, sort_by, reverse)
    nodes.sort(key=lambda n: n.properties.get(sort_by), reverse=reverse)
