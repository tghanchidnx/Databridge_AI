# Book

A library to create and manage hierarchical data structures, with powerful integrations for data transformation and quality.

This project is designed to facilitate the creation, manipulation, and storage of parent-child hierarchies from various data sources. The core object, the "Book," represents a hierarchical structure that can be enriched with properties, functions, and other metadata.

## Features

- **Hierarchical Data Modeling:** Create JSON-based parent-child relationships from sources like CSV, JSON, databases, and more.
- **Node-Level Customization:** Add properties, Python functions, LLM prompts, and boolean flags to any node.
- **Flexible Storage:** Persist "Book" objects in graph databases (like Neo4j) or vector databases for advanced querying and AI applications.
- **Object Management:**
    - **Copying:** Create duplicates of "Book" objects.
    - **Synchronization:** Apply changes from one "Book" to all its copies.
    - **Linked Versions:** Create lightweight, linked copies that only store modifications.
- **dbt Integration:**
    - **`Book` -> `dbt`:** Automatically generate a dbt project from a `Book` hierarchy.
    - **`dbt` -> `Book`:** Create a `Book` from a dbt `manifest.json` file to visualize and analyze the project's dependency graph.
- **Great Expectations Integration:**
    - **Automatic Expectation Generation:** Generate a Great Expectations "Expectation Suite" from a `Book` object.
    - **Data Validation:** Run the validation and attach the results to the `Book`.
    - **AI-powered Suggestions:** Use the `AIAgent` to analyze the validation results and provide human-readable suggestions.
- **Extensible:** Built with a modular design to easily add new data sources, storage backends, and functionalities.

## Getting Started

*Installation and usage instructions will be added here.*

## Project Structure

- `book/`: The core Python library.
- `tests/`: Unit and integration tests.
- `docs/`: Documentation.
- `use_cases/`: Example usage and sample data.
