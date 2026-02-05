from typing import Any, List, Optional
from .models import Node, Book

def get_property(node: Node, book: Book, key: str) -> Any:
    """
    Gets a property value, checking the node's properties first,
    then the book's global properties.

    Args:
        node: The node to get the property from.
        book: The book containing the node and global properties.
        key: The key of the property to get.

    Returns:
        The value of the property.

    Raises:
        KeyError: If the property is not found on the node or in the book's
                  global properties.
    """
    if key in node.properties:
        return node.properties[key]
    if key in book.global_properties:
        return book.global_properties[key]
    raise KeyError(f"Property '{key}' not found on node '{node.name}' or in the book's global properties.")

def add_property(node: Node, key: str, value: Any):
    """
    Adds a property to a node.

    Args:
        node: The node to add the property to.
        key: The key of the property.
        value: The value of the property.
    """
    node.properties[key] = value

def update_property(node: Node, key: str, value: Any):
    """
    Updates a property on a node.

    Args:
        node: The node to update the property on.
        key: The key of the property.
        value: The new value of the property.
    """
    if key in node.properties:
        node.properties[key] = value
    else:
        raise KeyError(f"Property '{key}' not found on node '{node.name}'.")

def remove_property(node: Node, key: str):
    """
    Removes a property from a node.

    Args:
        node: The node to remove the property from.
        key: The key of the property to remove.
    """
    if key in node.properties:
        del node.properties[key]
    else:
        raise KeyError(f"Property '{key}' not found on node '{node.name}'.")

def propagate_to_children(node: Node, key: str, value: Any):
    """
    Recursively propagates a property to all children of a node.

    Args:
        node: The starting node.
        key: The key of the property.
        value: The value of the property.
    """
    add_property(node, key, value)
    for child in node.children:
        add_property(child, key, value)
        propagate_to_children(child, key, value)

def _find_parent(target_node: Node, current_node: Node) -> Optional[Node]:
    """Helper function to find the parent of a node."""
    for child in current_node.children:
        if child.id == target_node.id:
            return current_node
        parent = _find_parent(target_node, child)
        if parent:
            return parent
    return None

def propagate_to_parents(node: Node, key: str, value: Any, root_nodes: List[Node]):
    """
    Propagates a property to all parents of a node.

    Args:
        node: The starting node.
        key: The key of the property.
        value: The value of the property.
        root_nodes: The list of all root nodes in the book.
    """
    add_property(node, key, value)
    parent = None
    for root_node in root_nodes:
        parent = _find_parent(node, root_node)
        if parent:
            break

    if parent:
        add_property(parent, key, value)
        propagate_to_parents(parent, key, value, root_nodes)

