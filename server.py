"""DataBridge AI - Entry Point.

Run the MCP server using: python server.py
Or use FastMCP dev mode: fastmcp dev src/server.py
"""
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.server import mcp

if __name__ == "__main__":
    mcp.run()
