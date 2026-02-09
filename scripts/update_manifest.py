"""Script to update the MANIFEST.md documentation."""

import sys
sys.path.insert(0, '.')

from src.server import mcp
from datetime import datetime
from pathlib import Path

tools = mcp._tool_manager._tools

manifest = f"""# DataBridge AI - Tool Manifest

> Auto-generated documentation for all MCP tools.
> Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

---

## Overview

DataBridge AI provides **{len(tools)} tools** across these categories:

| Category | Tools |
|----------|-------|
| File Discovery | find_files, stage_file, get_working_directory |
| Data Loading | load_csv, load_json, query_database |
| Profiling | profile_data, detect_schema_drift |
| Comparison | compare_hashes, get_orphan_details, get_conflict_details |
| Fuzzy Matching | fuzzy_match_columns, fuzzy_deduplicate |
| PDF/OCR | extract_text_from_pdf, ocr_image, parse_table_from_text |
| Workflow | save_workflow_step, get_workflow, clear_workflow, get_audit_log |
| Transform | transform_column, merge_sources |
| Hierarchy Builder | 49 tools for hierarchy management |
| Connections | Backend connection management |
| Schema Matcher | Database schema comparison |
| Data Matcher | Table data comparison |
| Templates & Skills | 16 tools for templates, skills, knowledge base |
| AI Orchestrator | 16 tools for task management, agent messaging |
| Planner Agent | 11 tools for AI workflow planning |
| Recommendations | 5 tools for smart CSV import suggestions |
| Diff Utilities | 6 tools for character-level comparison |
| Unified Agent | 10 tools for cross-system operations |
| Faux Objects | 18 tools for semantic view wrappers |
| Cortex Agent | 12 tools for Cortex AI LLM functions |
| Cortex Analyst | 10 tools for natural language to SQL |
| Documentation | update_manifest |

---

## Tool Reference

"""

for name in sorted(tools.keys()):
    tool = tools[name]
    doc = tool.fn.__doc__ or "No description available."
    doc_clean = doc.strip()
    manifest += f"### `{name}`\n\n{doc_clean}\n\n---\n\n"

manifest_path = Path("docs/MANIFEST.md")
manifest_path.parent.mkdir(parents=True, exist_ok=True)

with open(manifest_path, "w", encoding="utf-8") as f:
    f.write(manifest)

print(f"Updated MANIFEST.md with {len(tools)} tools")
