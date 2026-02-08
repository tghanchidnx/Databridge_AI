"""
DataBridge AI Dashboard - Combined Server
Serves the dashboard UI and provides MCP API endpoints.
"""
import http.server
import socketserver
import os
import sys
import json
import threading
from pathlib import Path

# Get the dashboard directory
DASHBOARD_DIR = Path(__file__).parent
PROJECT_ROOT = DASHBOARD_DIR.parent.parent

# Add paths for imports
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / 'src'))

# Dashboard port
DASHBOARD_PORT = 5051

class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    """Handler that serves static files and provides API endpoints."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(DASHBOARD_DIR), **kwargs)

    def do_GET(self):
        # API endpoints
        if self.path == '/api/health':
            self._send_json({'status': 'ok', 'version': '0.36.0', 'tools': 315})
        elif self.path == '/api/stats':
            self._send_json({
                'tool_count': 315,
                'version': '0.36.0',
                'modules': {
                    'wright': 29,
                    'hierarchies': 44,
                    'catalog': 15,
                    'cortex': 25,
                    'dbt': 8,
                    'quality': 7,
                    'versioning': 12
                }
            })
        else:
            # Serve static files
            super().do_GET()

    def do_OPTIONS(self):
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()

    def _send_cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def _send_json(self, data, status=200):
        self.send_response(status)
        self._send_cors_headers()
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode('utf-8'))

    def log_message(self, format, *args):
        print(f"[Dashboard] {args[0]}")


def run_dashboard():
    """Run the dashboard server."""
    with socketserver.TCPServer(("", DASHBOARD_PORT), DashboardHandler) as httpd:
        print(f"""
╔═══════════════════════════════════════════════════════════╗
║           DataBridge AI Dashboard v0.36.0                 ║
║                    315 MCP Tools                          ║
╠═══════════════════════════════════════════════════════════╣
║  Dashboard:  http://localhost:{DASHBOARD_PORT}                       ║
║  MCP API:    http://localhost:8085 (run mcp_api.py)       ║
╠═══════════════════════════════════════════════════════════╣
║  Tabs:                                                    ║
║  • Multi AI      - LLM orchestration                      ║
║  • MCP CLI       - Tool execution                         ║
║  • Wright Builder - 4-object pipeline                     ║
║  • dbt Workflow  - AI-powered dbt                         ║
║  • Data Catalog  - Metadata registry                      ║
╚═══════════════════════════════════════════════════════════╝
        """)
        httpd.serve_forever()


if __name__ == '__main__':
    run_dashboard()
