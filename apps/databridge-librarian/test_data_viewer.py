"""
Test script for the Data Viewer and Workflow Visualization PoC.
"""
import os
from pathlib import Path
import sys

# Add the 'src' directory to the Python path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from fastmcp import FastMCP
from mcp.tools import reconciliation
from databridge_core.audit.logger import (
    WorkflowLogger, set_workflow_logger,
    AuditLogger, set_audit_logger
)

# --- Setup ---
# 1. Initialize Loggers
log_dir = Path("./logs")
log_dir.mkdir(exist_ok=True)
os.environ["DATABRIDGE_LOG_DIR"] = str(log_dir.resolve())

# Set up the workflow logger
workflow_log_path = log_dir / "workflow_trace.csv"
workflow_logger = WorkflowLogger(log_path=workflow_log_path)
set_workflow_logger(workflow_logger)

# Set up a dummy audit logger (as it's expected by some components)
audit_log_path = log_dir / "audit.csv"
audit_logger = AuditLogger(log_path=audit_log_path, source="test_script")
set_audit_logger(audit_logger)


# 2. Initialize MCP and register tools
mcp = FastMCP()
reconciliation.register_reconciliation_tools(mcp)

# --- Test Execution ---
def run_test():
    """Execute the test case."""
    print("--- Starting Data Viewer PoC Test ---")

    # 1. Define sample data path
    sample_csv_path = "../../samples/chart_of_accounts.csv"
    print(f"\n[Step 1] Using sample data: {sample_csv_path}")

    # 2. Call the instrumented `load_csv` tool
    print("\n[Step 2] Calling the `load_csv` tool...")
    load_result = mcp.run_tool(
        "load_csv",
        {"file_path": sample_csv_path}
    )

    if not load_result.get("success"):
        print("\n--- TEST FAILED ---")
        print("`load_csv` tool failed to execute.")
        print("Errors:", load_result.get("errors"))
        return

    session_id = load_result.get("session_id")
    step_id = load_result.get("step_id")
    
    print("\n`load_csv` tool executed successfully!")
    print(f"  - Session ID: {session_id}")
    print(f"  - Step ID: {step_id}")
    print(f"  - Output Snapshot Key: {load_result.get('output_snapshot')}")

    # 3. Call `get_data_snapshot_url` to get the access URL
    print("\n[Step 3] Calling `get_data_snapshot_url` to get the snapshot URL...")
    url_result = mcp.run_tool(
        "get_data_snapshot_url",
        {"session_id": session_id, "step_id": step_id}
    )

    if not url_result.get("success"):
        print("\n--- TEST FAILED ---")
        print("`get_data_snapshot_url` tool failed.")
        print("Error:", url_result.get("error"))
        return

    print("\n`get_data_snapshot_url` tool executed successfully!")
    print(f"  - Access URL: {url_result.get('access_url')}")
    print(f"  - Note: {url_result.get('format_note')}")

    # 4. Call `export_workflow_diagram`
    output_html_path = "workflow_diagram.html"
    print(f"\n[Step 4] Calling `export_workflow_diagram` to generate the visual workflow...")
    diagram_result = mcp.run_tool(
        "export_workflow_diagram",
        {"session_id": session_id, "output_path": output_html_path}
    )

    if not diagram_result.get("success"):
        print("\n--- TEST FAILED ---")
        print("`export_workflow_diagram` tool failed.")
        print("Error:", diagram_result.get("error"))
        return

    print("\n`export_workflow_diagram` tool executed successfully!")
    print(f"  - Diagram saved to: {diagram_result.get('output_path')}")

    print("\n--- TEST COMPLETED SUCCESSFULLY ---")
    print("\nNext Steps:")
    print("1. Open the `workflow_diagram.html` file in your browser to see the visual workflow.")
    print("2. Although the API endpoint is not yet active, you can see the URL that BI tools will use.")
    print("3. Check the `logs/workflow_trace.csv` file to see the raw structured log of the execution.")


if __name__ == "__main__":
    run_test()
