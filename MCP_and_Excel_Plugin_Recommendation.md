# Recommendation: Databridge AI MCP Server & Excel Add-in

**Document Purpose:** This document provides a strategic recommendation for creating a Databridge AI MCP Server and an accompanying Excel Add-in. The goal is to expose the powerful headless capabilities of the Databridge AI engine to business users within the familiar environment of Microsoft Excel.

---

## Part 1: The Databridge AI MCP Server

### Finding and Activating the Server
Based on the codebase analysis, the Databridge AI application is **already a fully-featured MCP server** built with the `fastmcp` library. The primary task is not to create a new server, but to ensure it is properly installed, registered, and accessible within the Gemini environment.

To make it available, you would typically:
1.  Ensure all dependencies from `requirements.txt` are installed in the environment.
2.  Run the server using `python server.py` or a production-grade application server.
3.  Use `mcp_add` with the server's correct address and name to register it with Gemini.

Once activated, all the functions decorated with `@mcp.tool()` in `src/server.py` will be available for me to use.

### Exposing Functionality via API
The `FastMCP` server automatically exposes its tools via a RESTful API. To facilitate the Excel Add-in's development, it is recommended to leverage the automatically generated OpenAPI (Swagger) documentation available at the `/docs` endpoint of the running server. This will provide a clear, interactive API specification for the frontend developers.

---

## Part 2: Excel Add-in Architecture

We recommend building a modern **Office Add-in** using web technologies. This ensures cross-platform compatibility (Windows, Mac, Web) and a rich user experience.

*   **Frontend Technology:**
    *   **Framework:** **React** (using the `office-js` library) is the recommended framework for building a dynamic and responsive user interface.
    *   **UI Library:** **Microsoft's Fluent UI** should be used for all components. This will ensure the add-in has the native look and feel of the Microsoft Office suite, making it intuitive for users.

*   **Backend Communication:**
    *   The Excel Add-in will be a client-side application that communicates with the **Databridge AI MCP Server** via standard HTTPS RESTful API calls.
    *   The add-in will call the endpoints corresponding to the MCP tools (e.g., `POST /tools/compare_hashes`).

*   **Authentication:**
    *   The MCP server must handle authentication. On login, it should issue a secure, short-lived token (e.g., JWT).
    *   The Excel Add-in will store this token securely for the duration of the session (e.g., using `Office.context.roamingSettings` or session storage) and include it in the `Authorization` header of all subsequent API requests. **No credentials or secrets should ever be stored in the client-side code.**

---

## Part 3: Core Add-in Features (For All Personas)

The Add-in should feature a primary task pane with the following core functionalities:

1.  **Connection Manager:**
    *   A UI for creating and managing connection strings for data warehouses (Snowflake, Postgres, etc.).
    *   This will interact with the `connections` module tools on the MCP server.
    *   Connections should be stored securely on the server, not in Excel.

2.  **Data Loader:**
    *   An interface to pull data directly into an Excel sheet.
    *   **From Database:** Select a saved connection, input a SQL query, and execute it using the `query_database` tool.
    *   **From File:** Upload a CSV or JSON file to the server, which then processes it with `load_csv`/`load_json` and returns the data to be inserted into the sheet.

3.  **Workflow Runner:**
    *   A UI to execute pre-defined data reconciliation workflows saved on the server (using `get_workflow` and `run_workflow` tools).

---

## Part 4: Persona-Driven Functionalities

This section details specific features tailored to the unique workflows of each target user.

### A. For the FP&A Analyst

**Goal:** To streamline financial modeling, variance analysis, and month-end reconciliation directly in Excel.

*   **Feature 1: Interactive Hierarchy Mapping**
    *   **UI:** A button in the ribbon called "Map Hierarchy".
    *   **Workflow:**
        1.  The analyst selects a range of data (e.g., a trial balance with account codes and descriptions).
        2.  They select a target hierarchy (e.g., "Standard P&L") from a dropdown populated from the `hierarchy` module.
        3.  The add-in sends this data to a new MCP tool, `suggest_mappings`.
        4.  This tool uses `fuzzy_match_columns` to compare the sheet's account descriptions to the hierarchy's node descriptions, suggesting the correct mapping for each account.
        5.  The suggestions are presented in a task pane, where the analyst can approve or correct them before finalizing.

*   **Feature 2: One-Click Sheet Reconciliation**
    *   **UI:** A button called "Reconcile Sheets".
    *   **Workflow:**
        1.  The analyst selects two sheets to compare (e.g., "January Actuals" and "January Budget").
        2.  A simple UI asks for the key columns (e.g., `Cost Center`, `Account Code`).
        3.  The add-in uses the `compare_hashes` tool to find differences.
        4.  A summary report is generated in a new sheet, and the add-in uses Excel's formatting to highlight orphan rows and conflicting cells in the original sheets.

### B. For the Business Analyst Consultant

**Goal:** To rapidly profile data, validate requirements, and create clean datasets for dashboards and reports.

*   **Feature 1: Data Profiler & Quality Check**
    *   **UI:** A task pane with a "Profile Selected Data" button.
    *   **Workflow:**
        1.  The analyst highlights a range of data in Excel.
        2.  The add-in passes this data to the `profile_data` MCP tool.
        3.  The results (null percentages, cardinality, potential keys, duplicate rows) are displayed in a clean, graphical format in the task pane, giving an instant overview of data quality.

*   **Feature 2: Visual Query Builder**
    *   **UI:** A visual interface for building SQL queries.
    *   **Workflow:**
        1.  The analyst selects a database connection.
        2.  They can see tables and columns, and visually select columns, add filters, and create joins.
        3.  The add-in constructs the SQL query string and sends it to the `query_database` tool.
        4.  The results are loaded into a new, clean sheet, ready for use in a PivotTable or other visualization.

### C. For the Database Developer

**Goal:** To bridge the gap between messy Excel data and structured databases, making data import/export safe and efficient.

*   **Feature 1: "Stage for Import" Tool**
    *   **UI:** A button called "Stage for Import".
    *   **Workflow:**
        1.  The developer selects a messy data range.
        2.  A UI allows them to apply a sequence of transformations (e.g., `trim_spaces`, `remove_special`, `upper` on a specific column) using the `transform_column` tool.
        3.  Once done, the tool saves the cleaned data as a new CSV on the server and returns its path, ready for a database bulk import operation.

*   **Feature 2: Ad-hoc Query Tool**
    *   **UI:** A simple task pane with a connection dropdown and a large text box.
    *   **Workflow:**
        1.  The developer selects a connection, writes a raw SQL query, and hits "Run".
        2.  The query is sent directly to the `query_database` tool.
        3.  The full, raw result set is streamed back and populated into a new Excel sheet for quick analysis or validation, bypassing the need to open a separate SQL client.

---

## Part 5: Design & Architectural Best Practices

*   **Asynchronous Operations:** All calls to the MCP server must be asynchronous to prevent Excel from freezing. The UI should use loading indicators (spinners, progress bars) to provide clear feedback to the user.
*   **Modularity:** The Add-in's JavaScript code should be modular, with clear separation between UI components, API service calls, and Office.js interaction logic.
*   **Error Handling:** Implement robust error handling. API errors should be caught and displayed to the user in a friendly, understandable format.
*   **Performance:** For large datasets, avoid pulling the entire dataset into Excel at once. Leverage the server-side capabilities of Databridge AI to perform aggregations and filtering, and only pull the necessary summary data or a preview into the client.
*   **Single Responsibility:** The Excel Add-in should be a "thin" client responsible for UI and user interaction. The "heavy lifting" of data processing, comparison, and business logic should always reside on the `Databridge AI` MCP server.
