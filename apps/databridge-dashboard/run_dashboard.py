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

# Try to import MCP server for tool access
mcp_server = None
try:
    from src.server import mcp as _mcp
    mcp_server = _mcp
    print("[Dashboard] MCP server imported successfully")
except Exception as e:
    print(f"[Dashboard] MCP server not available: {e}")
    print("[Dashboard] MCP tool endpoints will return limited info")


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    """Handler that serves static files and provides API + MCP endpoints."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(DASHBOARD_DIR), **kwargs)

    def do_GET(self):
        # Backend API endpoints
        if self.path == '/api/health':
            self._send_json({
                'statusCode': 200,
                'data': {
                    'status': 'ok',
                    'version': '0.39.0',
                    'tools': 341
                }
            })
        elif self.path == '/api/stats':
            self._send_json({
                'tool_count': 341,
                'version': '0.39.0',
                'modules': {
                    'wright': 29,
                    'hierarchies': 44,
                    'catalog': 19,
                    'cortex': 27,
                    'dbt': 8,
                    'quality': 7,
                    'versioning': 12,
                    'graphrag': 10,
                    'observability': 15
                }
            })
        # MCP API endpoints
        elif self.path == '/mcp/health':
            self._send_json({'status': 'ok', 'message': 'MCP API is running'})
        elif self.path == '/mcp/tools':
            self._list_mcp_tools()
        else:
            # Serve static files
            super().do_GET()

    def do_POST(self):
        if self.path == '/mcp/call':
            self._call_mcp_tool()
        else:
            self._send_json({'error': 'Not found'}, 404)

    def do_OPTIONS(self):
        self.send_response(200)
        self._send_cors_headers()
        self.end_headers()

    def _list_mcp_tools(self):
        """List all available MCP tools."""
        if not mcp_server:
            self._send_json({'tools': [], 'count': 0, 'note': 'MCP server not loaded'})
            return
        try:
            tools = []
            for name, tool in mcp_server._tool_manager._tools.items():
                tools.append({
                    'name': name,
                    'description': tool.description or '',
                    'parameters': tool.parameters if hasattr(tool, 'parameters') else {}
                })
            self._send_json({'tools': tools, 'count': len(tools)})
        except Exception as e:
            self._send_json({'error': str(e)}, 500)

    def _call_mcp_tool(self):
        """Call an MCP tool."""
        if not mcp_server:
            self._send_json({'error': 'MCP server not loaded'}, 503)
            return
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length).decode('utf-8')
            data = json.loads(body) if body else {}

            tool_name = data.get('tool')
            params = data.get('params', {})

            if not tool_name:
                self._send_json({'error': 'Missing "tool" parameter'}, 400)
                return

            if tool_name not in mcp_server._tool_manager._tools:
                self._send_json({'error': f'Tool "{tool_name}" not found'}, 404)
                return

            tool = mcp_server._tool_manager._tools[tool_name]

            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                func = tool.fn
                if asyncio.iscoroutinefunction(func):
                    result = loop.run_until_complete(func(**params))
                else:
                    result = func(**params)

                if hasattr(result, 'model_dump'):
                    result = result.model_dump()
                elif hasattr(result, '__dict__'):
                    result = result.__dict__

                self._send_json({'success': True, 'result': result})
            finally:
                loop.close()

        except json.JSONDecodeError:
            self._send_json({'error': 'Invalid JSON'}, 400)
        except Exception as e:
            import traceback
            self._send_json({'error': str(e), 'traceback': traceback.format_exc()}, 500)

    def _send_cors_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def _send_json(self, data, status=200):
        self.send_response(status)
        self._send_cors_headers()
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode('utf-8'))

    def log_message(self, format, *args):
        print(f"[Dashboard] {args[0]}")


def run_dashboard():
    """Run the dashboard server."""
    mcp_status = "integrated" if mcp_server else "not available (tools won't load)"
    with socketserver.TCPServer(("", DASHBOARD_PORT), DashboardHandler) as httpd:
        print(f"""
============================================================
  DataBridge AI Dashboard v0.39.0 - 341 MCP Tools
============================================================
  Dashboard:  http://localhost:{DASHBOARD_PORT}
  API:        http://localhost:{DASHBOARD_PORT}/api/health
  MCP:        http://localhost:{DASHBOARD_PORT}/mcp/health
  MCP Status: {mcp_status}
------------------------------------------------------------
  Tabs:
  - Multi AI       - LLM orchestration
  - MCP CLI        - Tool execution
  - Wright Builder - 4-object pipeline
  - dbt Workflow   - AI-powered dbt
  - Data Catalog   - Metadata registry
============================================================
        """)
        httpd.serve_forever()


if __name__ == '__main__':
    run_dashboard()
