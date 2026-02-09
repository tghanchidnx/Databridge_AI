"""
DataBridge AI - Dashboard Launcher
Run the beautiful Flask-based UI on port 5050.
"""
import sys
import os
from pathlib import Path

# Setup paths
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / 'databridge-ce'))
sys.path.insert(0, str(project_root / 'src'))

# Import and run the Flask app
from ui.server import app

if __name__ == '__main__':
    print("""
+================================================================+
|              DataBridge AI Dashboard v0.40.0                   |
|                     348 MCP Tools                              |
+================================================================+
|  Dashboard:  http://127.0.0.1:5050                             |
|                                                                |
|  Features:                                                     |
|  - Data Reconciliation    - Hierarchy Builder                  |
|  - Wright Pipeline        - dbt Integration                    |
|  - Data Catalog           - Cortex AI                          |
|  - Data Observability     - GraphRAG Engine                    |
+================================================================+
    """)
    app.run(debug=True, port=5050, host='127.0.0.1')
