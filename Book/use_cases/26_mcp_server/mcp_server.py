from book import (
    Book,
    Node,
    from_list,
    execute_formulas,
    Formula,
    get_logger,
    add_property,
)
import csv
import json

logger = get_logger(__name__)

# --- In-memory store for Books ---
book_store = {}


def _create_book_from_csv(file_path: str, book_name: str) -> str:
    """
    Creates a Book from a CSV file and returns a handle to it.
    """
    logger.info(f"Creating book '{book_name}' from {file_path}...")
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = list(csv.DictReader(f))

        root_nodes = from_list(data, parent_col="parent_id", child_col="id", name_col="name")
        book = Book(name=book_name, root_nodes=root_nodes)

        book_handle = f"book-{len(book_store)}"
        book_store[book_handle] = book

        return f"Successfully created book. Handle: {book_handle}"
    except Exception as e:
        return f"Error: {e}"


def _add_formula_to_book(book_handle: str, node_name: str, formula_name: str, expression: str, operands: list) -> str:
    """
    Adds a formula to a node in a Book.
    """
    logger.info(f"Adding formula '{formula_name}' to node '{node_name}' in book '{book_handle}'...")
    if book_handle not in book_store:
        return "Error: Book not found."

    book = book_store[book_handle]

    def find_node(nodes, name):
        for node in nodes:
            if node.name == name:
                return node
            if node.children:
                found = find_node(node.children, name)
                if found:
                    return found
        return None

    node_to_update = find_node(book.root_nodes, node_name)

    if not node_to_update:
        return f"Error: Node '{node_name}' not found."

    formula = Formula(name=formula_name, expression=expression, operands=operands)
    node_to_update.formulas.append(formula)

    # Add operands to properties for execution
    for op in operands:
        add_property(node_to_update, op, 0)  # Initialize with 0

    return "Successfully added formula."


def _get_book_as_json(book_handle: str) -> str:
    """
    Returns a Book as a JSON string.
    """
    if book_handle not in book_store:
        return "Error: Book not found."

    return book_store[book_handle].model_dump_json(indent=2)


def register_book_mcp_tools(mcp):
    """Register Book MCP tools with a FastMCP server instance."""
    mcp.tool()(_create_book_from_csv)
    mcp.tool()(_add_formula_to_book)
    mcp.tool()(_get_book_as_json)


def main():
    """
    This function simulates a client interacting with the MCP server.
    """
    logger.info("--- Simulating MCP Client Interaction ---")

    # 1. Create a sample CSV for the client to use
    csv_path = "mcp_test_data.csv"
    with open(csv_path, "w") as f:
        f.write("id,name,parent_id\n")
        f.write("1,A,\n")
        f.write("2,B,1\n")
        f.write("3,C,1\n")

    # 2. Call the 'create_book_from_csv' tool
    logger.info("\nClient call: create_book_from_csv")
    result = _create_book_from_csv(csv_path, "My MCP Book")
    print(result)
    book_handle = result.split(": ")[1]

    # 3. Call the 'add_formula_to_book' tool
    logger.info("\nClient call: add_formula_to_book")
    result = _add_formula_to_book(book_handle, "A", "total", "B + C", ["B", "C"])
    print(result)

    # 4. Call the 'get_book_as_json' tool
    logger.info("\nClient call: get_book_as_json")
    result = _get_book_as_json(book_handle)
    print(result)

    # Clean up
    import os
    os.remove(csv_path)

    logger.info("\n--- To run as a real MCP server ---")
    logger.info("from fastmcp import FastMCP")
    logger.info("mcp = FastMCP('BookLib-MCP')")
    logger.info("register_book_mcp_tools(mcp)")
    logger.info("mcp.run()")


if __name__ == "__main__":
    main()
