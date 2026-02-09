"""Example plugin for DataBridge AI Community Edition.

This demonstrates how to create custom plugins that extend DataBridge AI.
"""


def register_tools(mcp_instance):
    """Register the hello_world tool with the MCP instance.

    This function is discovered and called by the plugin loader.

    Args:
        mcp_instance: FastMCP instance to register tools with
    """

    @mcp_instance.tool()
    def hello_world(name: str = "World") -> str:
        """A simple greeting tool to demonstrate plugin capabilities.

        Args:
            name: The name to include in the greeting.

        Returns:
            A friendly greeting message.
        """
        return f"Hello, {name}! Welcome to the DataBridge AI community."

    @mcp_instance.tool()
    def plugin_info() -> str:
        """Get information about loaded plugins.

        Returns:
            JSON with plugin information.
        """
        import json
        return json.dumps({
            "plugin_name": "hello_world",
            "version": "1.0.0",
            "description": "Example plugin demonstrating DataBridge AI extensibility",
            "tools": ["hello_world", "plugin_info"],
        }, indent=2)

    print("[Plugin] hello_world registered 2 tools")
