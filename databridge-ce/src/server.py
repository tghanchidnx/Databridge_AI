import os
import sys
import importlib.util
from pathlib import Path
from fastmcp import FastMCP

# --- Dynamic Plugin Loading System ---

def load_plugins(mcp_instance: FastMCP, plugins_dir: Path):
    """Dynamically discovers and loads tools from a plugin directory."""
    if not plugins_dir.is_dir():
        return

    for plugin_name in os.listdir(plugins_dir):
        plugin_path = plugins_dir / plugin_name
        if plugin_path.is_dir():
            mcp_tools_file = plugin_path / "mcp_tools.py"
            if mcp_tools_file.is_file():
                try:
                    # Dynamically import the mcp_tools.py module
                    spec = importlib.util.spec_from_file_location(f"plugins.{plugin_name}.mcp_tools", mcp_tools_file)
                    module = importlib.util.module_from_spec(spec)
                    sys.modules[spec.name] = module
                    spec.loader.exec_module(module)

                    # Call the registration function if it exists
                    if hasattr(module, "register_tools"):
                        print(f"Loading plugin: {plugin_name}")
                        module.register_tools(mcp_instance)
                except Exception as e:
                    print(f"Error loading plugin '{plugin_name}': {e}")


# --- Databridge AI Backend Initialization ---

# Assume the script is run from the `databridge-ce` directory
project_root = Path(__file__).parent.parent.parent 
sys.path.insert(0, str(project_root))

try:
    from databridge-ce.src.config import settings
    
    # 1. Create the core MCP instance
    databridge_mcp = FastMCP("Databridge Community Edition", "An open-source framework for data integration and analysis.")

    # 2. Define plugin directories
    # The open-source plugins that ship with the project
    public_plugins_dir = project_root / "databridge-ce" / "plugins"
    # The user's private, proprietary plugins
    private_plugins_dir = project_root / "private_plugins"

    # 3. Load all plugins
    load_plugins(databridge_mcp, public_plugins_dir)
    load_plugins(databridge_mcp, private_plugins_dir)

    print("--- Databridge CE Server: All tools registered successfully. ---")

except ImportError as e:
    print(f"FATAL ERROR: Could not initialize Databridge server components: {e}")
    sys.exit(1)

# Note: The Flask app setup that was previously here has been moved to `run_ui.py`
# to keep this server file focused on the MCP logic.