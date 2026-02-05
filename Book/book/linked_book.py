from .models import Book, Node
from typing import List, Any, Dict, Optional
from pydantic import BaseModel

class Delta(BaseModel):
    """
    Represents a change to a node's property.
    """
    node_id: str
    key: str
    new_value: Any

class LinkedBook:
    """
    Represents a linked book that stores changes as deltas from a base book.
    """
    def __init__(self, base_book: Book):
        self.base_book = base_book
        self.deltas: List[Delta] = []
        self._node_cache: Dict[str, Node] = self._build_node_cache(base_book)

    def _build_node_cache(self, book: Book) -> Dict[str, Node]:
        """Builds a cache of nodes for quick access."""
        cache = {}
        all_nodes = self._get_all_nodes(book.root_nodes)
        for node in all_nodes:
            cache[node.id] = node
        return cache

    def _get_all_nodes(self, nodes: List[Node]) -> List[Node]:
        """Recursively gets all nodes in a list of nodes."""
        all_nodes = []
        for node in nodes:
            all_nodes.append(node)
            all_nodes.extend(self._get_all_nodes(node.children))
        return all_nodes

    def add_change(self, node_id: str, key: str, new_value: Any):
        """
        Adds a change to the linked book.
        """
        # For simplicity, we just append the delta. A more robust implementation
        # would check for existing deltas for the same node and key.
        delta = Delta(node_id=node_id, key=key, new_value=new_value)
        self.deltas.append(delta)

    def get_property(self, node_id: str, key: str) -> Any:
        """
        Gets a property value, checking the deltas first, then the base book.
        """
        # Check deltas in reverse order to get the latest change
        for delta in reversed(self.deltas):
            if delta.node_id == node_id and delta.key == key:
                return delta.new_value

        # If not in deltas, get from the base book
        node = self._node_cache.get(node_id)
        if not node:
            raise KeyError(f"Node with id '{node_id}' not found in the base book.")

        return node.properties.get(key)

    def to_book(self, new_book_name: str) -> Book:
        """
        Applies the deltas to the base book and returns a new, independent Book object.
        """
        # Deep copy the base book to avoid modifying it
        new_book = self.base_book.model_copy(deep=True)
        new_book.name = new_book_name
        
        # Re-build the node cache for the new book
        new_node_cache = self._build_node_cache(new_book)

        for delta in self.deltas:
            if delta.node_id in new_node_cache:
                node = new_node_cache[delta.node_id]
                node.properties[delta.key] = delta.new_value
        
        return new_book
