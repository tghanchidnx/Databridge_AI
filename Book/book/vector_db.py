import chromadb
from chromadb.utils import embedding_functions
from typing import List, Dict, Any
from .models import Book, Node

def get_node_text_representation(node: Node, book: Book) -> str:
    """
    Creates a text representation of a node by concatenating its name and properties.
    """
    prop_texts = [f"{key}: {value}" for key, value in node.properties.items()]
    return f"Node: {node.name}\nProperties:\n" + "\n".join(prop_texts)

def create_collection(collection_name: str, embedding_model_name: str = 'all-MiniLM-L6-v2'):
    """
    Creates a new collection in ChromaDB.
    """
    client = chromadb.Client()
    sentence_transformer_ef = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name=embedding_model_name
    )
    collection = client.get_or_create_collection(
        name=collection_name, embedding_function=sentence_transformer_ef
    )
    return collection

def _get_all_nodes(nodes: List[Node]) -> List[Node]:
    """Recursively gets all nodes in a list of nodes."""
    all_nodes = []
    for node in nodes:
        all_nodes.append(node)
        all_nodes.extend(_get_all_nodes(node.children))
    return all_nodes

def add_nodes_to_collection(collection_name: str, book: Book):
    """
    Adds all nodes from a book to the specified ChromaDB collection.
    """
    collection = create_collection(collection_name)
    all_nodes = _get_all_nodes(book.root_nodes)
    
    documents = [get_node_text_representation(node, book) for node in all_nodes]
    metadatas = [node.properties for node in all_nodes]
    ids = [node.id for node in all_nodes]

    collection.add(
        documents=documents,
        metadatas=metadatas,
        ids=ids
    )

def query_collection(
    collection_name: str, query: str, n_results: int = 5
) -> List[Dict]:
    """
    Queries the collection for similar nodes.
    """
    collection = create_collection(collection_name)
    results = collection.query(query_texts=[query], n_results=n_results)
    return results