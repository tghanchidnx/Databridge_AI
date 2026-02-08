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

def register_tools(mcp_instance: FastMCP):
    """
    Registers the hello_world tool with the MCP instance.
    This function is discovered and called by the plugin loader.
    """
    mcp_instance.register_tool(hello_world)
