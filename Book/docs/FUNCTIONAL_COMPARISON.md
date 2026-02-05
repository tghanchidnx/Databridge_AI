# Functional Comparison: Book vs. Librarian vs. Researcher

This document provides a functional comparison of the three core components of the Databridge AI ecosystem: the `Book` library, the `Librarian`, and the `Researcher`. It also identifies potential gaps and areas for future development.

## Core Philosophy

*   **`Book` Library:** A lightweight, in-memory Python library for creating, manipulating, and versioning hierarchical data structures. It is designed for maximum flexibility and is ideal for data scripting, ad-hoc analysis, and prototyping. It can be thought of as a "Swiss Army knife" for hierarchical data.
*   **`Librarian`:** A robust, persistent hierarchy management system. It acts as a central repository for master data hierarchies and their mappings to source systems. It is designed to be the "single source of truth" for hierarchical data within an organization.
*   **`Researcher`:** An analytics engine that leverages the hierarchies from the `Librarian` to perform complex analysis on large datasets in data warehouses. It is designed for "compute pushdown," meaning it pushes the analytical workload to the data warehouse for performance and scalability.

## Feature Comparison

| Feature | `Book` Library | `Librarian` | `Researcher` |
| :--- | :--- | :--- | :--- |
| **Primary Focus** | In-memory, flexible hierarchical data manipulation | Persistent, centralized hierarchy management | Large-scale data warehouse analytics |
| **Data Storage** | In-memory objects, file-based (JSON, GML) | Centralized database (SQLite by default) | Not applicable (queries data warehouses) |
| **Hierarchy Creation** | From code, CSV, JSON | From UI, CLI, MCP, templates, CSV | N/A (consumes hierarchies from Librarian) |
| **Formula Engine** | Yes (in-memory, Python `eval`) | Yes (persistent, basic arithmetic) | Yes (generates SQL for data warehouse) |
| **AI Agent** | Yes (suggests enhancements for a single `Book`) | Yes (skills for domain-specific analysis) | Yes (NL-to-SQL, insights, forecasting) |
| **Data Ingestion** | CSV, JSON | CSV, JSON, SQL, PDF, OCR | SQL (via warehouse connectors) |
| **Data Volume** | Small to medium (in-memory) | Large (database-backed) | Very large (data warehouse scale) |
| **Versioning** | `LinkedBook` (deltas) | Not explicitly mentioned (likely versioned by project) | N/A |
| **Deployment** | N/A | Yes (generates SQL, dbt, Snowflake deployment) | N/A (runs queries, doesn't deploy) |
| **User Interface** | Python library (for developers) | CLI, REPL, MCP Server | CLI, REPL, MCP Server |

## What's Missing? (Gap Analysis)

Based on the comparison, here are some potential gaps and areas for future integration and development:

*   **Integration between `Book` and `Librarian`:** There is currently no direct way to "promote" a `Book` object into a `Librarian` project, or to "check out" a `Librarian` hierarchy into a `Book` object for local manipulation. This would be a powerful workflow, allowing analysts to prototype hierarchies in a `Book` and then formalize them in the `Librarian`.
*   **Bi-directional `Book`-`Researcher` Integration:** The `Researcher` can consume hierarchies from the `Librarian`, but it would be beneficial if it could also use an in-memory `Book` object as a dimensional context for a query. This would allow for ad-hoc analysis without first having to persist the hierarchy in the `Librarian`.
*   **Advanced Formula Engine in `Librarian`:** The `Librarian`'s formula engine is limited to basic arithmetic. It could be enhanced with more advanced functions (e.g., statistical functions, conditional logic) or by integrating the `Book`'s Python-based formula engine.
*   **Unified AI Agent:** The `Book`, `Librarian`, and `Researcher` each have their own AI capabilities. A unified AI agent that can operate across all three components would be a powerful addition. For example, the agent could analyze a `Book`, suggest that it be promoted to a `Librarian` project, and then use the `Researcher` to run analytics on it.
*   **Visual User Interface for `Book`:** While the `Book` library is designed to be code-first, a simple web-based UI for visualizing and editing `Book` objects could be a useful addition for less technical users.
*   **Real-time Collaboration:** The current architecture does not explicitly support real-time collaboration on `Book` or `Librarian` projects. This could be a future area of development, potentially using a websocket-based architecture.

This comparison provides a solid foundation for understanding how the different components of the Databridge AI ecosystem fit together and how they can be used in combination to solve a wide range of business problems.
