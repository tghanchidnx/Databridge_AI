#!/usr/bin/env python
"""
Launcher script for DataBridge MCP server.
This ensures proper working directory and Python path setup.
"""
import os
import sys

# Set working directory to project root
project_root = os.path.dirname(os.path.abspath(__file__))
os.chdir(project_root)

# Add src to Python path
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "src"))

# Now import and run the server
from src.server import mcp

if __name__ == "__main__":
    mcp.run()
