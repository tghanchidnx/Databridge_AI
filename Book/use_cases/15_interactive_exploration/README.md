# Use Case 15: Interactive Data Exploration with REPL (Book)

This use case demonstrates how the `Book` library can be used for interactive data exploration in a Read-Eval-Print Loop (REPL) session. This is a common workflow for data analysts and developers who need to perform ad-hoc analysis and manipulation of data.

## Features Highlighted

*   Loading data into a `Book` for in-memory analysis.
*   Starting an interactive Python shell with the `Book` object available for exploration.
*   Performing on-the-fly data filtering, manipulation, and inspection.

## Components Involved

*   **`Book` Library:** Used to structure the data and provide the tools for interactive analysis.

## Files

*   `server_inventory.csv`: A sample CSV file containing a list of servers with their properties.
*   `interactive_session.py`: The Python script that loads the data and starts the interactive REPL session.

## Step-by-Step Instructions

### 1. Set up the Environment

Make sure you have the `Book` library and its dependencies installed. From the `Book` directory, run:

```bash
poetry install
```

### 2. Start the Interactive Session

Navigate to the `Book/use_cases/15_interactive_exploration` directory and run the `interactive_session.py` script:

```bash
python interactive_session.py
```

### 3. What's Happening?

1.  **Load Data:** The script loads the `server_inventory.csv` file and creates a `Book` object named "Server Inventory," where each server is a `Node`.
2.  **Start Interactive Session:** It then starts an interactive Python shell. The `server_book` object is available in the local scope of this shell, ready for you to interact with.
3.  **Interactive Exploration:** You can now perform ad-hoc analysis directly in your terminal. The `README` provides a few examples of commands you can run.

### Interactive Session Examples

Once the script starts the interactive session, you will see a welcome banner. Here are some commands you can type into the interactive shell to explore the data:

*   **Inspect the Book's name:**
    ```python
    print(server_book.name)
    ```

*   **List all servers and their properties:**
    ```python
    for node in server_book.root_nodes:
        print(node.name, node.properties)
    ```

*   **Filter for all online servers:**
    ```python
    online_servers = [n for n in server_book.root_nodes if n.properties.get('status') == 'online']
    print(f"Number of online servers: {len(online_servers)}")
    ```

*   **Add a new property to a server:**
    ```python
    from book import add_property
    add_property(server_book.root_nodes[0], 'owner', 'Team A')
    print(server_book.root_nodes[0].properties['owner'])
    ```

*   **Calculate the total memory of all online servers:**
    ```python
    total_memory = sum(int(n.properties.get('memory_gb', 0)) for n in online_servers)
    print(f"Total memory of online servers: {total_memory} GB")
    ```

To exit the interactive session, type `exit()` or press `Ctrl-Z` (on Windows) or `Ctrl-D` (on Linux/macOS).

This use case demonstrates the power and flexibility of the `Book` library for hands-on, interactive data analysis, allowing you to quickly explore and understand your data without the need for a complex setup or a predefined analysis script.
