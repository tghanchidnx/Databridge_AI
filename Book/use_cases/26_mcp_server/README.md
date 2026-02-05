# Use Case 26: MCP Server for the Book Library

This use case demonstrates how to expose the functionalities of the `Book` library as a set of tools on an MCP (Model Context Protocol) server. This makes the library's features accessible to AI assistants like Claude, enabling them to programmatically create and manipulate `Book` objects.

## Features Highlighted

*   **MCP Server:** Creating an MCP server using the `fastmcp` library.
*   **MCP Tools:** Wrapping `Book` library functions as MCP tools.
*   **AI Assistant Integration:** Shows how an AI assistant could interact with the `Book` library through the MCP server.

## Components Involved

*   **`Book` Library:** Provides the core functionalities to be exposed as tools.
*   **`fastmcp`:** The library used to create the MCP server.

## Files

*   `mcp_server.py`: The Python script that defines and simulates the MCP server and its tools.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies, including `fastmcp`, installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Run the MCP Server Simulation Script

Navigate to the `Book/use_cases/26_mcp_server` directory and run the `mcp_server.py` script:

```bash
python mcp_server.py
```

### 3. What's Happening?

1.  **MCP Server Initialization:** The script initializes a `FastMCP` server.
2.  **Tool Definition:** It defines several functions (`create_book_from_csv`, `add_formula_to_book`, `get_book_as_json`) and decorates them with `@mcp.tool()`. This registers them as tools that can be called by an MCP client.
3.  **Client Simulation:** The `main()` function simulates a client (like Claude) interacting with the MCP server:
    *   It first creates a sample CSV file for the use case.
    *   It then calls the `create_book_from_csv` tool to create a `Book` from the CSV.
    *   Next, it calls the `add_formula_to_book` tool to add a formula to a node in the newly created `Book`.
    *   Finally, it calls `get_book_as_json` to retrieve the state of the `Book` after the modifications.
4.  **Real-world Scenario:** In a real-world scenario, you would run the server with `mcp.run()` and connect to it from an MCP client like Claude Desktop. The client would then be able to see and execute the tools you've defined.

### Expected Output

You should see output similar to this in your terminal:

```
INFO:__main__:--- Simulating MCP Client Interaction ---

Client call: create_book_from_csv
INFO:book.mcp_server:Creating book 'My MCP Book' from mcp_test_data.csv...
Successfully created book. Handle: book-0

Client call: add_formula_to_book
INFO:book.mcp_server:Adding formula 'total' to node 'A' in book 'book-0'...
Successfully added formula.

Client call: get_book_as_json
{
  "name": "My MCP Book",
  "schema_version": "1.0",
  "data_version": "[some-uuid]",
  "last_updated": "[timestamp]",
  "root_nodes": [
    {
      "id": "[some-uuid]",
      "schema_version": "1.0",
      "name": "A",
      "children": [
        {
          "id": "[some-uuid]",
          "schema_version": "1.0",
          "name": "B",
          "children": [],
          "properties": { ... },
          "python_function": null,
          "llm_prompt": null,
          "flags": {},
          "formulas": []
        },
        {
          "id": "[some-uuid]",
          "schema_version": "1.0",
          "name": "C",
          ...
        }
      ],
      "properties": { ... },
      "python_function": null,
      "llm_prompt": null,
      "flags": {},
      "formulas": [
        {
          "name": "total",
          "expression": "B + C",
          "operands": [
            "B",
            "C"
          ]
        }
      ]
    }
  ],
  "metadata": {},
  "global_properties": {}
}
```

This use case demonstrates how the `Book` library can be extended into a powerful, AI-accessible tool by exposing its functionalities through the Model Context Protocol.
