# Creating a Plugin

The Databridge AI framework is built to be extensible. You can add your own tools and capabilities by creating simple plugins. This guide will walk you through the process using the `hello_world` plugin as a template.

## 1. Plugin Directory Structure

A plugin is simply a directory inside the `databridge-ce/plugins/` folder. The server automatically discovers any subdirectories in this folder. The directory must contain an `mcp_tools.py` file.

```
/databridge-ce/
└── /plugins/
    └── /your_plugin_name/
        └── mcp_tools.py
```

## 2. Creating a Tool

A tool is a standard Python function decorated with `@tool`.

Let's look at `plugins/hello_world/mcp_tools.py`:

```python
# mcp_tools.py for the hello_world plugin

from fastmcp import FastMCP, tool

@tool("A simple tool that returns a greeting.")
def hello_world(name: str = "World") -> str:
    """
    This is an example tool for the Databridge AI Community Edition.
    It takes a name and returns a greeting.
    
    :param name: The name to include in the greeting.
    """
    return f"Hello, {name}! Welcome to the Databridge community."

```

- **`@tool(...)`:** The decorator that registers the function as an MCP tool. The string inside is a short, user-friendly description that appears in the UI.
- **Type Hinting:** The function uses standard Python type hints (`name: str`, `-> str`). The MCP server uses these hints to understand the tool's inputs and outputs, and the UI uses them to generate the parameter form.
- **Docstrings:** The docstring is used to provide a more detailed description of the tool and its parameters.

## 3. Registering the Tool

For the plugin loader to find your tool, you must create a `register_tools` function within your `mcp_tools.py` file. This function receives the main `mcp_instance` as an argument.

```python
def register_tools(mcp_instance: FastMCP):
    """
    Registers the hello_world tool with the MCP instance.
    This function is discovered and called by the plugin loader.
    """
    mcp_instance.register_tool(hello_world)
```

That's it! When you restart the Databridge Workbench server, it will automatically discover your new plugin and the `hello_world` tool will appear in the UI.
