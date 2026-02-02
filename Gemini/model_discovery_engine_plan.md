# Implementation Plan: The Model Discovery Engine

This document outlines the implementation plan for a new core utility in DataBridge AI: the **Model Discovery Engine**. Its purpose is to analyze existing SQL queries and their results to automatically reverse-engineer and generate the foundational hierarchies and documentation needed for the automated data warehouse creation process.

This plan assumes the unified monorepo architecture, with the new engine being built as a new library (`libs/databridge-discovery`) that is utilized by the main application.

---

## Phase 1: Input Processing and Syntactic Analysis

**Goal:** To create a robust foundation that can ingest and parse the raw inputs (SQL and CSV) into a structured, machine-readable format.

### Features & Implementation
1.  **Input Ingestion Module (`libs/databridge-discovery/ingest.py`):**
    *   Create a service that accepts a SQL file and a CSV result file.
    *   It will validate the inputs (e.g., ensure the SQL is parseable, the CSV is well-formed).
2.  **SQL Abstract Syntax Tree (AST) Parser:**
    *   **Action:** Integrate a powerful Python SQL parsing library like `sqlglot`. This library is crucial as it can parse complex, dialect-specific SQL into a standardized AST.
    *   **Details:** The parser will be responsible for breaking down the entire SQL query into its component parts: `SELECT` clauses, `FROM` clauses, `JOIN` conditions, `WHERE` clauses, `GROUP BY` clauses, and especially the nested logic within `CASE` statements.
3.  **Result Set Analyzer:**
    *   **Action:** Ingest the provided CSV file and analyze its structure.
    *   **Details:** The analyzer will determine the data type of each column in the result set. Critically, it will map each column back to its corresponding expression in the `SELECT` clause of the SQL query (e.g., Column 5, "total_val", maps to the SQL expression `ROUND(entries.amount_gl, 2)`).

### MCP Tool Updates
*   **New Tool:** `discovery_create_session(sql_file_path: str, csv_file_path: str) -> str`: This tool will initiate the workflow, run the ingestion and parsing, and return a unique session ID for subsequent operations.

### Documentation & User Guide
*   **Internal:** Document the AST structure produced by `sqlglot` and how it's stored.
*   **User Guide:** Add a section "Importing a Legacy Query" that explains the required inputs.

### Testing
*   **Unit Tests:** Test the SQL parser with various complex queries, including nested subqueries and different join types. Test the result set analyzer with different CSV formats.
*   **UAT:** "Provide the `FP&A Queries.sql` file and a sample CSV. Does the `discovery_create_session` tool execute without errors?"

---

## Phase 2: Graph-Based Semantic Modeling

**Goal:** To translate the syntactic AST from Phase 1 into a rich, semantic graph. This graph represents the "understanding" of the query's logic and data flow.

### Features & Implementation
1.  **Graph Database Integration:**
    *   **Action:** Integrate a graph database library. For ease of deployment, a library like `networkx` for in-memory graph processing is a good start. For larger-scale analysis, the architecture should allow plugging in a persistent graph DB like Neo4j.
2.  **AST-to-Graph Transformation Module (`libs/databridge-discovery/graph_builder.py`):**
    *   **Action:** Develop the core logic to traverse the SQL AST and populate the graph.
    *   **Graph Schema:**
        *   **Nodes:** Represent SQL concepts: `Table`, `Column`, `Function` (e.g., `SUM`), `Operator` (e.g., `LIKE`), `Literal` (e.g., 'Capex'), `Query`.
        *   **Edges:** Represent relationships: `JOINS_TO`, `SELECTS_FROM`, `HAS_COLUMN`, `FILTERS_ON`, `GROUPS_BY`, `CALLS_FUNCTION`.
3.  **Vector Embedding for Semantics:**
    *   **Action:** While building the graph, use the existing vector embedding model (from Researcher) to create embeddings for table and column names.
    *   **Details:** Store these vectors as properties on the `Table` and `Column` nodes in the graph. This will be crucial in the next phase for identifying semantically similar concepts (e.g., recognizing that `cust_id` and `customer_number` are likely the same entity).

### MCP Tool Updates
*   **New Tool:** `discovery_get_graph(session_id: str) -> str`: Returns a JSON representation of the semantic graph for visualization or debugging.

### Documentation & User Guide
*   **Internal:** Detailed documentation of the graph schema (node labels, properties, and edge types).
*   **User Guide:** Add a conceptual chapter, "How DataBridge Understands Your Queries," with a visual example of a simple SQL query and its corresponding graph representation.

### Testing
*   **Unit Tests:** Test the graph builder with a simple `SELECT ... FROM ... WHERE` query and assert that the correct nodes and edges are created.
*   **UAT:** "After running `discovery_create_session`, use the `discovery_get_graph` tool. Can you see nodes representing the `fact_financial_details` and `dim_account` tables, with a `JOINS_TO` edge connecting them?"

---

## Phase 3: Automated Hierarchy Extraction

**Goal:** To analyze the semantic graph and automatically identify and extract the business logic suitable for creating DataBridge hierarchies.

### Features & Implementation
1.  **Pattern Recognition Engine (`libs/databridge-discovery/extractors.py`):**
    *   **Action:** Build "Extractor" algorithms that traverse the graph to find common reporting patterns.
    *   **Primary Extractor (`CaseToHierarchy`):** This will be the first and most important extractor. It will be specifically designed to find the pattern of a large `CASE` statement that maps the values of one column to a set of string literals. It will parse this logic into a parent-child list suitable for a hierarchy.
    *   **Future Extractors:** The framework can be expanded to include extractors for other patterns, such as parent-child relationships within a single table (e.g., an `employee` table with a `manager_id` column).
2.  **Semantic Consolidation:**
    *   **Action:** After extracting hierarchies from multiple queries, use the vector embeddings on the graph nodes to suggest which hierarchies are semantically similar.
    *   **Example:** If one query has a `CASE` statement that creates a 'Sales Region' and another query has one that creates a 'Geo Region', the engine can calculate the similarity and ask the user, "Should these be part of the same canonical `Region` hierarchy?"

### MCP Tool Updates
*   **New Tool:** `discovery_get_proposed_hierarchies(session_id: str) -> str`: Analyzes the graph and returns a list of potential hierarchies that can be created.
*   **New Tool:** `discovery_preview_hierarchy(proposed_hierarchy_id: str) -> str`: Shows a tree view of what a specific proposed hierarchy will look like.

### Documentation & User Guide
*   **Internal:** Developer docs on how to create a new "Extractor" algorithm for future patterns.
*   **User Guide:** Add a "Hierarchy Discovery" section explaining how the engine finds hierarchies and how to interpret the proposals.

### Testing
*   **Unit Tests:** Create a test graph containing a `CASE` statement pattern and assert that the `CaseToHierarchy` extractor correctly outputs a structured list of parent-child nodes.
*   **UAT:** "Run `discovery_get_proposed_hierarchies` on your session. Does the tool propose a hierarchy named 'gl' based on the main `CASE` statement in the query?"

---

## Phase 4: Generation and Integration

**Goal:** To take the approved, extracted hierarchy data and formally create the DataBridge project, hierarchies, and user documentation.

### Features & Implementation
1.  **Hierarchy Project Generator (`libs/databridge-discovery/generator.py`):**
    *   **Action:** Create a service that uses the Librarian application's internal API/services to perform the final generation.
    *   **Workflow:**
        1.  Create a new DataBridge Project.
        2.  For each approved hierarchy, create a new `GROUPING` hierarchy.
        3.  Populate the hierarchy with the nodes and relationships extracted in Phase 3.
        4.  Create the necessary `XREF` hierarchy to link the new `GROUPING` hierarchy to the source tables from the SQL query.
2.  **Automated Documentation Generator:**
    *   **Action:** Create a new MCP tool that uses an LLM to write documentation.
    *   **Process:** The tool will be fed a context string containing:
        *   The original SQL query snippet that the hierarchy was derived from.
        *   The final structure of the created hierarchy.
        *   The name of the tables and columns it maps to.
    *   **Output:** It will generate a user-friendly markdown document explaining what the hierarchy is, where it came from, and how to manage it using DataBridge commands.

### MCP Tool Updates
*   **New Tool:** `discovery_commit_hierarchy(proposed_hierarchy_id: str, project_name: str) -> str`: Takes an approved hierarchy and formally creates the DataBridge project and objects. Returns the new project ID.
*   **New Tool:** `discovery_generate_docs(project_id: str) -> str`: Generates the management documentation for the newly created project.

### Documentation & User Guide
*   **User Guide:** The output of the documentation generator *is* the user guide for the new asset. Add a section explaining how to access and use this auto-generated documentation.

### Testing
*   **Integration Tests:** A full end-to-end test for the discovery engine. It will start with the SQL/CSV, run all the new MCP tools in sequence, and finally use the Librarian API to verify that a project and hierarchy with the correct structure were actually created in the database.
*   **UAT:**
    1.  "Run `discovery_commit_hierarchy` on the 'gl' hierarchy you approved in the previous phase."
    2.  "Does it return a new project ID?"
    3.  "Now, using the standard Librarian commands (`databridge hierarchy list <new_project_id>`), can you see the newly created hierarchy?"
    4.  "Run `discovery_generate_docs` on the new project. Is the generated documentation clear and accurate?"