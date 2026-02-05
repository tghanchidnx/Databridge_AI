from typing import List
import networkx as nx
from .models import Book, Node
from tinydb import TinyDB, Query

def _add_nodes_to_graph(graph: nx.DiGraph, nodes: List[Node]):
    """Recursively adds nodes and their children to the graph."""
    for node in nodes:
        node_data = {k: v for k, v in node.model_dump().items() if v is not None}
        if 'children' in node_data:
            del node_data['children']
        graph.add_node(node.id, **node_data)
        _add_nodes_to_graph(graph, node.children)
        for child in node.children:
            graph.add_edge(node.id, child.id)

def book_to_graph(book: Book) -> nx.DiGraph:
    """
    Converts a Book object into a networkx.DiGraph.

    Args:
        book: The Book object to convert.

    Returns:
        A networkx.DiGraph representing the Book.
    """
    graph = nx.DiGraph(name=book.name, **book.metadata)
    _add_nodes_to_graph(graph, book.root_nodes)
    return graph

def _build_book_hierarchy(graph: nx.DiGraph, node_id: str) -> Node:
    """Recursively builds a Node hierarchy from the graph."""
    node_data = graph.nodes[node_id]
    children = [
        _build_book_hierarchy(graph, child_id)
        for child_id in graph.successors(node_id)
    ]
    node_data['children'] = children
    return Node(**node_data)

def graph_to_book(graph: nx.DiGraph, book_name: str) -> Book:
    """
    Converts a networkx.DiGraph back into a Book object.

    Args:
        graph: The networkx.DiGraph to convert.
        book_name: The name for the new Book object.

    Returns:
        A Book object.
    """
    root_nodes = [
        node_id for node_id, in_degree in graph.in_degree() if in_degree == 0
    ]
    book_root_nodes = [
        _build_book_hierarchy(graph, root_id) for root_id in root_nodes
    ]
    metadata = {k: v for k, v in graph.graph.items() if k != 'name'}
    return Book(name=book_name, root_nodes=book_root_nodes, metadata=metadata)


def save_graph(graph: nx.DiGraph, file_path: str):
    """
    Saves the graph to a GML file.

    Args:
        graph: The graph to save.
        file_path: The path to the GML file.
    """
    nx.write_gml(graph, file_path)


def load_graph(file_path: str) -> nx.DiGraph:
    """
    Loads a graph from a GML file.

    Args:
        file_path: The path to the GML file.

    Returns:
        The loaded graph.
    """
    return nx.read_gml(file_path)

def save_graph_to_tinydb(graph: nx.DiGraph, db_path: str):
    """
    Saves a networkx graph to a TinyDB database.
    """
    db = TinyDB(db_path)
    db.truncate()  # Clear the database before saving
    
    nodes_table = db.table('nodes')
    edges_table = db.table('edges')
    
    for node_id, attributes in graph.nodes(data=True):
        nodes_table.insert({'id': node_id, **attributes})
        
    for u, v in graph.edges():
        edges_table.insert({'source': u, 'target': v})

def load_graph_from_tinydb(db_path: str) -> nx.DiGraph:
    """
    Loads a graph from a TinyDB database.
    """
    db = TinyDB(db_path)
    nodes_table = db.table('nodes')
    edges_table = db.table('edges')
    
    graph = nx.DiGraph()
    
    for item in nodes_table.all():
        node_id = item['id']
        attributes = {k: v for k, v in item.items() if k != 'id'}
        graph.add_node(node_id, **attributes)
        
    for item in edges_table.all():
        graph.add_edge(item['source'], item['target'])
        
    return graph