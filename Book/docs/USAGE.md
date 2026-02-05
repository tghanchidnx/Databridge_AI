# Usage Guide

This guide provides a comprehensive overview of how to use the `book` library to create, manipulate, and manage hierarchical data structures.

## Installation

To install the `book` library, you can use `poetry`:

```bash
poetry install
```

## Creating a Book

### From a List of Dictionaries

You can create a `Book` from a list of dictionaries, which is useful when your data is in a tabular format (e.g., from a CSV).

```python
from book import Book, from_list

data = [
    {"id": "1", "name": "A", "parent_id": None},
    {"id": "2", "name": "B", "parent_id": "1"},
]

root_nodes = from_list(data, parent_col="parent_id", child_col="id", name_col="name")
my_book = Book(name="My First Book", root_nodes=root_nodes)
```

### From a File

You can load a `Book` from a GML or JSON file.

```python
from book import load_book

# Load from GML
gml_book = load_book("my_book.gml", source_format="gml", book_name="Loaded GML Book")

# Load from JSON
json_book = load_book("my_book.json", source_format="json")
```

## Manipulating Nodes

### Properties

You can add, update, and remove properties on any node.

```python
from book import add_property, update_property, remove_property, get_property

node = my_book.root_nodes[0]

# Add a new property
add_property(node, "new_key", "new_value")

# Update an existing property
update_property(node, "new_key", "updated_value")

# Get a property (checks node, then book's global properties)
value = get_property(node, my_book, "new_key")

# Remove a property
remove_property(node, "new_key")
```

### Propagating Properties

You can propagate properties to all children or parents of a node.

```python
from book import propagate_to_children, propagate_to_parents

# Propagate to children
propagate_to_children(node, "shared_prop", True)

# Propagate to parents
child_node = node.children[0]
propagate_to_parents(child_node, "ancestor_prop", "val", my_book.root_nodes)
```

## Actions

### Python Functions

You can attach and run Python functions on nodes.

```python
from book import add_python_function, run_python_function

func_code = "result = node.properties.get('value', 1) * 2"
add_python_function(node, func_code)

result = run_python_function(node)
```

### LLM Prompts

You can attach and run LLM prompts on nodes.

```python
from book import add_llm_prompt, run_llm_prompt

class MockLLMClient:
    def generate(self, prompt: str):
        return f"LLM responded to: {prompt}"

mock_client = MockLLMClient()
prompt_text = "Analyze this node data: " + node.name
add_llm_prompt(node, prompt_text)

response = run_llm_prompt(node, mock_client)
```

## Storage

### Saving and Loading

You can save a `Book` to a GML or JSON file.

```python
from book import copy_book

# Save to GML
copy_book(my_book, "my_book.gml", destination_format="gml")

# Save to JSON
copy_book(my_book, "my_book.json", destination_format="json")
```

### NetworkX Integration

You can convert a `Book` to a `networkx.DiGraph` for graph analysis.

```python
from book import book_to_graph

graph = book_to_graph(my_book)
```

### Vector Search with ChromaDB

You can create a vector index of your book's nodes for semantic search.

```python
from book import create_collection, add_nodes_to_collection, query_collection

collection_name = "my_book_collection"
create_collection(collection_name)
add_nodes_to_collection(collection_name, my_book)

results = query_collection(collection_name, "Find nodes related to 'B'")
```

## Object Management

### Synchronization

Use the `SyncManager` to keep replicas of a book up-to-date.

```python
from book import SyncManager

sync_manager = SyncManager()
sync_manager.add_replica("my_book_replica.gml", "gml")
sync_manager.add_replica("my_book_replica.json", "json")

# ... make changes to my_book ...

sync_manager.update_replicas(my_book)
```

### Linked Books (Versioning)

Use `LinkedBook` to create lightweight versions of a book that only store changes (deltas).

```python
from book import LinkedBook

linked_book = LinkedBook(base_book=my_book)

# Add changes
node_id = my_book.root_nodes[0].id
linked_book.add_change(node_id, "new_key", "delta_value")

# Get a property (checks deltas first)
value = linked_book.get_property(node_id, "new_key") # Returns "delta_value"

# Materialize the linked book into a new, independent book
new_book = linked_book.to_book("My New Book")
```

## Logging

The library uses Python's built-in `logging` module. You can get a configured logger for your module like this:

```python
from book import get_logger

logger = get_logger(__name__)
logger.info("This is an informational message.")
```
