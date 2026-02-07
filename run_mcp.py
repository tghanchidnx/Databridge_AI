#!/usr/bin/env python
"""MCP Server wrapper for Claude Desktop."""
import sys
import os

# Ensure the project root is in the path
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Change to project directory
os.chdir(project_root)

# Now import and run the server
from src.server import mcp

if __name__ == "__main__":
    mcp.run()
