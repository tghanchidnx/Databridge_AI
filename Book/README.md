# Book

A library to create and manage hierarchical data structures.

This project is designed to facilitate the creation, manipulation, and storage of parent-child hierarchies from various data sources. The core object, the "Book," represents a hierarchical structure that can be enriched with properties, functions, and other metadata.

## Features

- **Hierarchical Data Modeling:** Create JSON-based parent-child relationships from sources like CSV, JSON, databases, and more.
- **Node-Level Customization:** Add properties, Python functions, LLM prompts, and boolean flags to any node.
- **Flexible Storage:** Persist "Book" objects in graph databases (like Neo4j) or vector databases for advanced querying and AI applications.
- **Object Management:**
    - **Copying:** Create duplicates of "Book" objects.
    - **Synchronization:** Apply changes from one "Book" to all its copies.
    - **Linked Versions:** Create lightweight, linked copies that only store modifications.
- **Extensible:** Built with a modular design to easily add new data sources, storage backends, and functionalities.

## Getting Started

*Installation and usage instructions will be added here.*

## Project Structure

- `book/`: The core Python library.
- `tests/`: Unit and integration tests.
- `docs/`: Documentation.
- `examples/`: Example usage and sample data.
