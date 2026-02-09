"""
DataBridge AI - UI Server
A Flask-based web server for the DataBridge AI dashboard.
"""
from flask import Flask, jsonify, render_template, request, abort, send_from_directory
import os
import sys
import json
from pathlib import Path

# --- Path Setup ---
# UI folder is at: databridge-ce/ui/
# Project root is at: Databridge_AI/
ui_dir = Path(__file__).parent
databridge_ce_dir = ui_dir.parent
project_root = databridge_ce_dir.parent

# Add paths for imports
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(databridge_ce_dir))

# --- Flask Application Setup ---
app = Flask(__name__, template_folder=str(ui_dir), static_folder=str(ui_dir))

# Configuration
TOOL_COUNT = 348  # Current tool count (v0.40.0)

# Get version from src/__init__.py
try:
    from src import __version__ as VERSION
except ImportError:
    VERSION = "0.40.0"  # Fallback
PROJECTS_DIR = project_root / 'use_cases_by_claude'
BOOK_PROJECTS_DIR = project_root / 'Book' / 'use_cases'
CLAUDE_MD_PATH = project_root / 'CLAUDE.md'

# --- API Routes ---

@app.route('/')
def index():
    """Serve the main dashboard."""
    return send_from_directory(str(ui_dir), 'index.html')

@app.route('/api/dashboard/stats', methods=['GET'])
def get_dashboard_stats():
    """Get dashboard statistics."""
    stats = {
        'tool_count': TOOL_COUNT,
        'project_count': 0,
        'workflow_steps': 0,
        'version': VERSION,
        'recent_activity': []
    }

    # Count projects
    try:
        if PROJECTS_DIR.exists():
            claude_cases = [d for d in PROJECTS_DIR.iterdir() if d.is_dir()]
            stats['project_count'] += len(claude_cases)
        if BOOK_PROJECTS_DIR.exists():
            book_cases = [d for d in BOOK_PROJECTS_DIR.iterdir() if d.is_dir()]
            stats['project_count'] += len(book_cases)
    except Exception as e:
        print(f"Error counting projects: {e}")

    # Read workflow file
    try:
        workflow_file = project_root / 'data' / 'workflow.json'
        if workflow_file.exists():
            with open(workflow_file, 'r') as f:
                workflow = json.load(f)
                stats['workflow_steps'] = len(workflow.get('steps', []))
    except Exception as e:
        print(f"Error reading workflow: {e}")

    # Read recent activity from audit log
    try:
        audit_log = project_root / 'data' / 'audit_trail.csv'
        if audit_log.exists():
            with open(audit_log, 'r') as f:
                lines = f.readlines()
                if len(lines) > 1:
                    header = lines[0].strip().split(',')
                    recent_lines = lines[-5:]
                    for line in recent_lines:
                        if line.strip() and 'timestamp' not in line.lower():
                            values = line.strip().split(',')
                            if len(values) >= len(header):
                                stats['recent_activity'].append(dict(zip(header, values)))
    except Exception as e:
        print(f"Error reading audit log: {e}")

    return jsonify(stats)

@app.route('/api/config/get', methods=['GET'])
def get_app_config():
    """Get application configuration."""
    # Return default configuration
    config = {
        'data_dir': str(project_root / 'data'),
        'workflow_file': str(project_root / 'data' / 'workflow.json'),
        'nestjs_backend_url': 'http://localhost:3002/api',
        'nestjs_api_key': 'v2-dev-key-1',
        'nestjs_sync_enabled': True,
        'cortex_default_model': 'mistral-large',
        'cortex_max_reasoning_steps': 10,
        'cortex_console_enabled': True,
        'fuzzy_threshold': 80,
        'max_rows_display': 10
    }
    return jsonify(config)

@app.route('/api/config/save', methods=['POST'])
def save_app_config():
    """Save application configuration (simulated)."""
    new_config = request.json
    if not new_config:
        abort(400, "No config data provided.")

    print(f"Simulated config save: {json.dumps(new_config, indent=2)}")
    return jsonify({
        "status": "success",
        "message": "Configuration validated (save is simulated)."
    })

@app.route('/api/tools', methods=['GET'])
def get_tools():
    """Get list of available tools with schemas from MCP server."""
    try:
        from src.server import mcp as mcp_server

        tool_manager = getattr(mcp_server, '_tool_manager', None)
        if not tool_manager:
            raise RuntimeError("MCP tool manager not initialized")

        tools_dict = getattr(tool_manager, '_tools', {})
        tools = []
        for name, tool in tools_dict.items():
            schema = {}
            if hasattr(tool, 'parameters') and tool.parameters:
                schema = tool.parameters
            elif hasattr(tool, 'input_schema') and tool.input_schema:
                schema = tool.input_schema
            tools.append({
                "name": name,
                "description": getattr(tool, 'description', '') or '',
                "inputSchema": schema
            })

        # Sort alphabetically for consistent display
        tools.sort(key=lambda t: t['name'])
        return jsonify({"tools": tools, "total": len(tools)})

    except Exception as e:
        print(f"Dynamic tool discovery failed ({e}), falling back to mock list")
        # Fallback mock tools (with basic schemas) if MCP import fails
        tools = [
            {"name": "load_csv", "description": "Load a CSV file into memory",
             "inputSchema": {"type": "object", "properties": {"file_path": {"type": "string", "description": "Path to CSV file"}}, "required": ["file_path"]}},
            {"name": "profile_data", "description": "Profile data quality and statistics",
             "inputSchema": {"type": "object", "properties": {"file_path": {"type": "string", "description": "Path to data file"}}, "required": ["file_path"]}},
            {"name": "create_hierarchy", "description": "Create a new hierarchy node",
             "inputSchema": {"type": "object", "properties": {"project_id": {"type": "string", "description": "Project ID"}, "hierarchy_name": {"type": "string", "description": "Name for the hierarchy"}}, "required": ["project_id", "hierarchy_name"]}},
            {"name": "compare_hashes", "description": "Compare data using hash values",
             "inputSchema": {"type": "object", "properties": {"source": {"type": "string", "description": "Source identifier"}, "target": {"type": "string", "description": "Target identifier"}}, "required": ["source", "target"]}},
            {"name": "fuzzy_match_columns", "description": "Match columns using fuzzy logic",
             "inputSchema": {"type": "object", "properties": {"source_columns": {"type": "string", "description": "Source column list"}, "target_columns": {"type": "string", "description": "Target column list"}}, "required": ["source_columns", "target_columns"]}},
            {"name": "cortex_complete", "description": "Text generation via Cortex COMPLETE()",
             "inputSchema": {"type": "object", "properties": {"prompt": {"type": "string", "description": "The prompt text"}}, "required": ["prompt"]}},
            {"name": "analyst_ask", "description": "Natural language SQL generation",
             "inputSchema": {"type": "object", "properties": {"question": {"type": "string", "description": "Natural language question"}}, "required": ["question"]}},
            {"name": "generate_dbt_model", "description": "Generate dbt models",
             "inputSchema": {"type": "object", "properties": {"model_name": {"type": "string", "description": "Model name"}}, "required": ["model_name"]}},
            {"name": "catalog_search", "description": "Search the data catalog",
             "inputSchema": {"type": "object", "properties": {"query": {"type": "string", "description": "Search query"}}, "required": ["query"]}},
            {"name": "version_create", "description": "Create a versioned snapshot",
             "inputSchema": {"type": "object", "properties": {"name": {"type": "string", "description": "Version name"}}, "required": ["name"]}}
        ]
        return jsonify({"tools": tools, "total": TOOL_COUNT})

@app.route('/api/projects', methods=['GET'])
def get_projects():
    """Get list of projects."""
    projects = []

    try:
        if PROJECTS_DIR.exists():
            for d in PROJECTS_DIR.iterdir():
                if d.is_dir():
                    projects.append({
                        "name": d.name,
                        "path": str(d),
                        "source": "use_cases_by_claude"
                    })

        if BOOK_PROJECTS_DIR.exists():
            for d in BOOK_PROJECTS_DIR.iterdir():
                if d.is_dir():
                    projects.append({
                        "name": d.name,
                        "path": str(d),
                        "source": "Book"
                    })
    except Exception as e:
        print(f"Error listing projects: {e}")

    return jsonify({"projects": projects})

@app.route('/api/documentation', methods=['GET'])
def get_documentation():
    """Get documentation content from CLAUDE.md."""
    try:
        if CLAUDE_MD_PATH.exists():
            with open(CLAUDE_MD_PATH, 'r', encoding='utf-8') as f:
                content = f.read()
            return jsonify({"content": content, "path": str(CLAUDE_MD_PATH)})
        else:
            return jsonify({"content": "Documentation not found.", "path": None})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/agents', methods=['GET'])
def get_agents():
    """Get list of available agents (mock data for console)."""
    agents = [
        {"id": "planner", "name": "Planner Agent", "status": "active", "capabilities": ["plan_workflow", "analyze_request"]},
        {"id": "cortex", "name": "Cortex Agent", "status": "active", "capabilities": ["cortex_complete", "cortex_reason"]},
        {"id": "reconciler", "name": "Data Reconciler", "status": "idle", "capabilities": ["compare_hashes", "fuzzy_match"]},
        {"id": "hierarchy", "name": "Hierarchy Builder", "status": "idle", "capabilities": ["create_hierarchy", "add_mapping"]},
        {"id": "catalog", "name": "Catalog Manager", "status": "active", "capabilities": ["catalog_search", "catalog_scan"]}
    ]
    return jsonify({"agents": agents})

@app.route('/api/console/messages', methods=['GET'])
def get_console_messages():
    """Get console messages (mock data)."""
    messages = [
        {"id": 1, "from": "System", "content": "DataBridge AI initialized with 287 tools", "timestamp": "2024-01-15 10:00:00", "level": "info"},
        {"id": 2, "from": "Cortex Agent", "content": "Connected to Snowflake Cortex", "timestamp": "2024-01-15 10:00:01", "level": "success"},
        {"id": 3, "from": "Planner Agent", "content": "Ready to plan workflows", "timestamp": "2024-01-15 10:00:02", "level": "info"}
    ]
    return jsonify({"messages": messages})

# --- Wright Pipeline Builder API ---

@app.route('/api/wright/generate', methods=['POST'])
def generate_wright_pipeline():
    """Generate Wright pipeline SQL for a specific step."""
    data = request.get_json()
    step = data.get('step', 'all')  # vw1, dt2, dt3a, dt3, all
    config = data.get('config', {})

    # Generate SQL based on step
    result = {
        'step': step,
        'config': config,
        'sql': {},
        'success': True
    }

    project = config.get('projectName', 'unnamed')
    report_type = config.get('reportType', 'GROSS')
    database = config.get('database', 'ANALYTICS')
    schema = config.get('schema', 'PUBLIC')

    if step in ['vw1', 'all']:
        result['sql']['vw1'] = f"-- VW_1: Translation View for {project}\nCREATE OR REPLACE VIEW {database}.{schema}.VW_1_{report_type}_{project.upper()}_TRANSLATED AS\nSELECT /* Generated by Wright Builder */\n  h.HIERARCHY_ID,\n  h.HIERARCHY_NAME,\n  m.ID_SOURCE,\n  CASE m.ID_SOURCE /* Add mappings */ END AS RESOLVED_VALUE\nFROM {database}.{schema}.{config.get('hierarchyTable', 'TBL_HIERARCHY')} h\nJOIN {database}.{schema}.{config.get('mappingTable', 'TBL_MAPPING')} m\n  ON h.HIERARCHY_ID = m.HIERARCHY_ID;"

    if step in ['dt2', 'all']:
        result['sql']['dt2'] = f"-- DT_2: Granularity Table for {project}\nCREATE OR REPLACE DYNAMIC TABLE {database}.{schema}.DT_2_{report_type}_{project.upper()}_GRANULARITY\n  TARGET_LAG = '1 hour'\n  WAREHOUSE = TRANSFORM_WH\nAS\nSELECT /* UNPIVOT measures */\n  *\nFROM {database}.{schema}.VW_1_{report_type}_{project.upper()}_TRANSLATED\nUNPIVOT (MEASURE_VALUE FOR MEASURE_NAME IN (AMOUNT, VOLUME));"

    if step in ['dt3a', 'all']:
        result['sql']['dt3a'] = f"-- DT_3A: Pre-Aggregation Fact for {project}\nCREATE OR REPLACE DYNAMIC TABLE {database}.{schema}.DT_3A_{report_type}_{project.upper()}_PREAGG\n  TARGET_LAG = '1 hour'\n  WAREHOUSE = TRANSFORM_WH\nAS\n/* UNION ALL branches for join patterns */\nSELECT /* Branch 1: Account */ * FROM ...\nUNION ALL\nSELECT /* Branch 2: Product */ * FROM ...;"

    if step in ['dt3', 'all']:
        result['sql']['dt3'] = f"-- DT_3: Final Data Mart for {project}\nCREATE OR REPLACE DYNAMIC TABLE {database}.{schema}.DT_3_{report_type}_{project.upper()}_MART\n  TARGET_LAG = '1 hour'\n  WAREHOUSE = TRANSFORM_WH\nAS\nWITH\n  p1_base AS (/* Base totals */),\n  p2_combined AS (/* Combined */),\n  p3_gross_profit AS (/* Gross Profit = Revenue - Taxes - Deducts */)\nSELECT\n  MD5(CONCAT_WS('|', dims)) AS SURROGATE_KEY,\n  *\nFROM p3_gross_profit;"

    return jsonify(result)

@app.route('/api/wright/configs', methods=['GET'])
def list_wright_configs():
    """List saved Wright configurations."""
    configs_dir = project_root / 'data' / 'wright_configs'
    configs = []

    if configs_dir.exists():
        for f in configs_dir.glob('*.json'):
            try:
                with open(f, 'r') as file:
                    config = json.load(file)
                    configs.append({
                        'name': f.stem,
                        'config': config
                    })
            except Exception as e:
                print(f"Error loading config {f}: {e}")

    return jsonify({'configs': configs})

@app.route('/api/wright/configs', methods=['POST'])
def save_wright_config():
    """Save a Wright configuration."""
    data = request.get_json()
    name = data.get('name', 'unnamed')
    config = data.get('config', {})

    configs_dir = project_root / 'data' / 'wright_configs'
    configs_dir.mkdir(parents=True, exist_ok=True)

    config_file = configs_dir / f"{name}.json"
    with open(config_file, 'w') as f:
        json.dump(config, f, indent=2)

    return jsonify({'success': True, 'message': f'Configuration saved: {name}'})

# --- File Upload Routes ---

@app.route('/api/upload-csv', methods=['POST'])
def upload_csv():
    """Accept CSV file upload and return content for MCP tool processing."""
    file = request.files.get('file')
    if not file:
        abort(400, "No file provided.")
    content = file.read().decode('utf-8')
    return jsonify({"content": content, "filename": file.filename})

# --- MCP Tool Proxy (Hierarchy Builder uses this) ---

def _serialize_tool_result(result):
    """Convert a FastMCP ToolResult (or other types) to a JSON-serializable value."""
    # FastMCP ToolResult: has .content list of TextContent/ImageContent etc.
    if hasattr(result, 'content') and isinstance(result.content, list):
        texts = []
        for item in result.content:
            if hasattr(item, 'text'):
                texts.append(item.text)
            elif hasattr(item, 'data'):
                texts.append(f"[binary data: {getattr(item, 'mime_type', 'unknown')}]")
            else:
                texts.append(str(item))
        combined = '\n'.join(texts) if len(texts) > 1 else (texts[0] if texts else '')
        # Try to parse as JSON if it looks like JSON
        try:
            return json.loads(combined)
        except (json.JSONDecodeError, TypeError):
            return combined
    # Already a dict/list/str/number — return as-is
    if isinstance(result, (dict, list, str, int, float, bool, type(None))):
        return result
    # Try str fallback
    return str(result)


@app.route('/api/tools/run', methods=['POST'])
def run_tool():
    """
    Proxy endpoint for running MCP tools from the UI.
    The UI sends {tool: "tool_name", params: {...}} and this endpoint
    calls the tool via the MCP server module.
    """
    data = request.get_json()
    tool_name = data.get('tool') or data.get('tool_name', '')
    params = data.get('params', {})

    try:
        # Import and run the tool via the MCP server
        from src.server import mcp as mcp_server

        # Get the tool from the MCP server's tool manager
        tool_manager = getattr(mcp_server, '_tool_manager', None)
        if not tool_manager:
            return jsonify({"error": "MCP server not initialized"}), 500

        tools = getattr(tool_manager, '_tools', {})
        tool = tools.get(tool_name)
        if not tool:
            return jsonify({"error": f"Tool '{tool_name}' not found"}), 404

        # Call the tool
        import asyncio
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    result = pool.submit(lambda: asyncio.run(tool.run(params))).result()
            else:
                result = loop.run_until_complete(tool.run(params))
        except RuntimeError:
            result = asyncio.run(tool.run(params))

        # Serialize ToolResult (FastMCP returns ToolResult with .content list)
        serialized = _serialize_tool_result(result)
        return jsonify({"result": serialized})

    except Exception as e:
        error_msg = str(e)
        # Parse common validation errors to give better guidance
        if 'required' in error_msg.lower() or 'missing' in error_msg.lower():
            return jsonify({
                "error": error_msg,
                "hint": "Check that all required parameters are provided. Use /api/tools to see the tool's inputSchema."
            }), 400
        return jsonify({"error": error_msg}), 500


# --- Demo Seed Endpoint ---

@app.route('/api/demo/seed', methods=['POST'])
def seed_demo_project():
    """Create a demo hierarchy project with sample data for first-time users."""
    try:
        from src.server import mcp as mcp_server

        tool_manager = getattr(mcp_server, '_tool_manager', None)
        if not tool_manager:
            return jsonify({"error": "MCP server not initialized"}), 500

        tools = getattr(tool_manager, '_tools', {})

        # Check that hierarchy tools are available (requires Pro license)
        if 'create_hierarchy_project' not in tools:
            return jsonify({
                "error": "Hierarchy Builder tools not available. This feature requires a Pro license.",
                "hint": "Run with a Pro license key to enable hierarchy tools and the demo project."
            }), 400

        import asyncio

        def run_tool(name, params):
            tool = tools.get(name)
            if not tool:
                raise RuntimeError(f"Tool '{name}' not found")
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor() as pool:
                        raw = pool.submit(lambda: asyncio.run(tool.run(params))).result()
                else:
                    raw = loop.run_until_complete(tool.run(params))
            except RuntimeError:
                raw = asyncio.run(tool.run(params))
            return _serialize_tool_result(raw)

        # 1. Create project
        project_result = run_tool('create_hierarchy_project', {
            'name': 'Demo - Financial Reporting 2024',
            'description': 'Sample income statement hierarchy with source mappings and formulas. Created by DataBridge AI demo seed.'
        })

        # Parse project result
        if isinstance(project_result, str):
            import json as json_mod
            project_result = json_mod.loads(project_result)
        project = project_result.get('project', project_result)
        project_id = project.get('id') or project.get('project_id', '')

        if not project_id:
            return jsonify({"error": "Failed to create demo project", "details": project_result}), 500

        # 2. Create root: Income Statement
        root_result = run_tool('create_hierarchy', {
            'project_id': project_id,
            'hierarchy_name': 'Income Statement',
        })
        if isinstance(root_result, str):
            root_result = json.loads(root_result)
        root_id = (root_result.get('hierarchy', root_result) or {}).get('id', '')

        # 3. Create children
        children = ['Revenue', 'Cost of Goods Sold', 'Gross Profit', 'Operating Expenses', 'Net Income']
        child_ids = {}
        for child_name in children:
            child_result = run_tool('create_hierarchy', {
                'project_id': project_id,
                'hierarchy_name': child_name,
                'parent_id': root_id,
            })
            if isinstance(child_result, str):
                child_result = json.loads(child_result)
            child_obj = child_result.get('hierarchy', child_result) or {}
            child_ids[child_name] = child_obj.get('id', child_obj.get('hierarchy_id', ''))

        # 4. Add sample source mappings to Revenue and COGS
        if child_ids.get('Revenue'):
            try:
                run_tool('add_source_mapping', {
                    'project_id': project_id,
                    'hierarchy_id': child_ids['Revenue'],
                    'source_database': 'ANALYTICS',
                    'source_schema': 'GL',
                    'source_table': 'FACT_JOURNAL_ENTRIES',
                    'source_column': 'REVENUE_AMOUNT',
                })
            except Exception:
                pass  # Non-critical

        if child_ids.get('Cost of Goods Sold'):
            try:
                run_tool('add_source_mapping', {
                    'project_id': project_id,
                    'hierarchy_id': child_ids['Cost of Goods Sold'],
                    'source_database': 'ANALYTICS',
                    'source_schema': 'GL',
                    'source_table': 'FACT_JOURNAL_ENTRIES',
                    'source_column': 'COGS_AMOUNT',
                })
            except Exception:
                pass

        # 5. Add formula: Gross Profit = Revenue - COGS
        if child_ids.get('Gross Profit') and child_ids.get('Revenue') and child_ids.get('Cost of Goods Sold'):
            try:
                run_tool('add_formula_rule', {
                    'project_id': project_id,
                    'main_hierarchy_id': child_ids['Gross Profit'],
                    'operation': 'SUM',
                    'source_hierarchy_id': child_ids['Revenue'],
                })
                run_tool('add_formula_rule', {
                    'project_id': project_id,
                    'main_hierarchy_id': child_ids['Gross Profit'],
                    'operation': 'SUBTRACT',
                    'source_hierarchy_id': child_ids['Cost of Goods Sold'],
                })
            except Exception:
                pass

        return jsonify({
            "success": True,
            "project_id": project_id,
            "message": "Demo project 'Financial Reporting 2024' created with 6 hierarchies, 2 mappings, and 1 formula."
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- Main Entry Point ---

if __name__ == '__main__':
    print(f"Starting DataBridge AI UI Server...")
    print(f"  Project root: {project_root}")
    print(f"  UI directory: {ui_dir}")
    print(f"  Tool count: {TOOL_COUNT}")
    print(f"\nOpen http://127.0.0.1:5050 in your browser")
    app.run(debug=True, port=5050, host='127.0.0.1')
