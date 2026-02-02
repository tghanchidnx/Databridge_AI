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

# --- Logger Initialization ---
from pathlib import Path
from databridge_core.audit.logger import (
    WorkflowLogger, set_workflow_logger,
    AuditLogger, set_audit_logger
)

log_dir = Path("./logs")
log_dir.mkdir(exist_ok=True)
os.environ["DATABRIDGE_LOG_DIR"] = str(log_dir.resolve())

workflow_log_path = log_dir / "workflow_trace.csv"
workflow_logger = WorkflowLogger(log_path=workflow_log_path)
set_workflow_logger(workflow_logger)

audit_log_path = log_dir / "audit.csv"
audit_logger = AuditLogger(log_path=audit_log_path, source="mcp_server")
set_audit_logger(audit_logger)
# ---------------------------

# Now import and run the server
from src.server import mcp

if __name__ == "__main__":
    mcp.run()
