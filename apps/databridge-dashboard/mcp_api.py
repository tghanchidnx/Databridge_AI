"""
MCP HTTP API Wrapper
Provides a simple REST API for calling MCP tools from the dashboard.
"""
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import sys
import os

# Add parent directories to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

# Import the MCP server
from src.server import mcp

# CORS headers
CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
    'Content-Type': 'application/json'
}

class MCPAPIHandler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        for key, value in CORS_HEADERS.items():
            self.send_header(key, value)
        self.end_headers()

    def do_GET(self):
        if self.path == '/health':
            self._send_json({'status': 'ok', 'message': 'MCP API is running'})
        elif self.path == '/tools':
            self._list_tools()
        else:
            self._send_json({'error': 'Not found'}, 404)

    def do_POST(self):
        if self.path == '/call':
            self._call_tool()
        else:
            self._send_json({'error': 'Not found'}, 404)

    def _list_tools(self):
        """List all available MCP tools"""
        try:
            tools = []
            for name, tool in mcp._tool_manager._tools.items():
                tools.append({
                    'name': name,
                    'description': tool.description or '',
                    'parameters': tool.parameters if hasattr(tool, 'parameters') else {}
                })
            self._send_json({'tools': tools, 'count': len(tools)})
        except Exception as e:
            self._send_json({'error': str(e)}, 500)

    def _call_tool(self):
        """Call an MCP tool"""
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length).decode('utf-8')
            data = json.loads(body) if body else {}

            tool_name = data.get('tool')
            params = data.get('params', {})

            if not tool_name:
                self._send_json({'error': 'Missing "tool" parameter'}, 400)
                return

            # Get the tool
            if tool_name not in mcp._tool_manager._tools:
                self._send_json({'error': f'Tool "{tool_name}" not found'}, 404)
                return

            tool = mcp._tool_manager._tools[tool_name]

            # Call the tool
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            try:
                # Get the actual function
                func = tool.fn
                if asyncio.iscoroutinefunction(func):
                    result = loop.run_until_complete(func(**params))
                else:
                    result = func(**params)

                # Handle different result types
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

    def _send_json(self, data, status=200):
        self.send_response(status)
        for key, value in CORS_HEADERS.items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(json.dumps(data, default=str).encode('utf-8'))

    def log_message(self, format, *args):
        print(f"[MCP API] {args[0]}")

def run_server(port=8085):
    server = HTTPServer(('0.0.0.0', port), MCPAPIHandler)
    print(f"MCP API Server running on http://localhost:{port}")
    print(f"  GET  /health - Health check")
    print(f"  GET  /tools  - List all tools")
    print(f"  POST /call   - Call a tool (body: {{tool: 'name', params: {{}}}})")
    server.serve_forever()

if __name__ == '__main__':
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 8085
    run_server(port)
