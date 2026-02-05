from .models import Book
from .graph_db import book_to_graph, save_graph, load_graph, graph_to_book
from typing import Literal, Dict, List

StorageFormat = Literal["gml", "json"]

class SyncManager:
    """
    Manages synchronization of a book across multiple replicas.
    """
    def __init__(self):
        self.replicas: List[Dict[str, str]] = []

    def add_replica(self, path: str, format: StorageFormat):
        """
        Adds a replica to be tracked by the SyncManager.
        """
        self.replicas.append({"path": path, "format": format})

    def update_replicas(self, book: Book):
        """
        Updates all tracked replicas with the current state of the book.
        """
        for replica in self.replicas:
            copy_book(book, replica["path"], replica["format"])


def copy_book(book: Book, destination_path: str, destination_format: StorageFormat):
    """
    Copies a Book object to a specified destination in a given format.

    Args:
        book: The Book object to copy.
        destination_path: The path to save the copied book.
        destination_format: The format to save the book in ('gml' or 'json').
    """
    if destination_format == "gml":
        graph = book_to_graph(book)
        save_graph(graph, destination_path)
    elif destination_format == "json":
        with open(destination_path, 'w') as f:
            f.write(book.model_dump_json(indent=2))
    else:
        raise ValueError(f"Unsupported destination format: {destination_format}")

def load_book(source_path: str, source_format: StorageFormat, book_name: str = "Loaded Book") -> Book:
    """
    Loads a Book object from a specified source.

    Args:
        source_path: The path to the book file.
        source_format: The format of the book file.
        book_name: The name to assign to the loaded book.

    Returns:
        A Book object.
    """
    if source_format == "gml":
        graph = load_graph(source_path)
        return graph_to_book(graph, book_name)
    elif source_format == "json":
        with open(source_path, 'r') as f:
            return Book.model_validate_json(f.read())
    else:
        raise ValueError(f"Unsupported source format: {source_format}")
