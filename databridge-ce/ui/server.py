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
TOOL_COUNT = 315  # Current tool count (v0.36.0)

# Get version from src/__init__.py
try:
    from src import __version__ as VERSION
except ImportError:
    VERSION = "0.35.0"  # Fallback
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
    """Get list of available tools (mock data)."""
    # Return a sample of tools for the UI
    tools = [
        {"name": "load_csv", "category": "Data Loading", "description": "Load a CSV file into memory"},
        {"name": "profile_data", "category": "Profiling", "description": "Profile data quality and statistics"},
        {"name": "create_hierarchy", "category": "Hierarchy", "description": "Create a new hierarchy node"},
        {"name": "compare_hashes", "category": "Comparison", "description": "Compare data using hash values"},
        {"name": "fuzzy_match_columns", "category": "Fuzzy Matching", "description": "Match columns using fuzzy logic"},
        {"name": "cortex_complete", "category": "Cortex AI", "description": "Text generation via Cortex COMPLETE()"},
        {"name": "analyst_ask", "category": "Cortex Analyst", "description": "Natural language SQL generation"},
        {"name": "generate_dbt_model", "category": "dbt Integration", "description": "Generate dbt models"},
        {"name": "catalog_search", "category": "Data Catalog", "description": "Search the data catalog"},
        {"name": "version_create", "category": "Versioning", "description": "Create a versioned snapshot"}
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

# --- Main Entry Point ---

if __name__ == '__main__':
    print(f"Starting DataBridge AI UI Server...")
    print(f"  Project root: {project_root}")
    print(f"  UI directory: {ui_dir}")
    print(f"  Tool count: {TOOL_COUNT}")
    print(f"\nOpen http://127.0.0.1:5050 in your browser")
    app.run(debug=True, port=5050, host='127.0.0.1')
