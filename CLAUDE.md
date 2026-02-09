# DataBridge AI: Quick Reference

> **Compact Config** - Detailed docs in GEMINI.md. Query Gemini via `ask_claude_gemini.py` for examples.

## Project Summary
- **Version:** 0.39.0 | **Tools:** 341 | **License:** MIT
- **Type:** Headless MCP-native data reconciliation engine

## Tool Categories (341 total)

| Module | Tools | Key Tools |
|--------|-------|-----------|
| File Discovery | 3 | `find_files`, `stage_file` |
| Data Reconciliation | 38 | `load_csv`, `profile_data`, `fuzzy_match_columns` |
| Hierarchy Builder | 44 | `create_hierarchy`, `import_flexible_hierarchy`, `export_hierarchy_csv` |
| Templates/Skills/KB | 16 | `list_financial_templates`, `get_skill_prompt` |
| Git Automation | 4 | `commit_dbt_project`, `create_deployment_pr` |
| SQL Discovery | 2 | `sql_to_hierarchy`, `smart_analyze_sql` |
| Mapping Enrichment | 5 | `configure_mapping_enrichment`, `enrich_mapping_file` |
| AI Orchestrator | 16 | `submit_orchestrated_task`, `register_agent` |
| PlannerAgent | 11 | `plan_workflow`, `suggest_agents` |
| Smart Recommendations | 5 | `get_smart_recommendations`, `smart_import_csv` |
| Diff Utilities | 6 | `diff_text`, `diff_dicts`, `explain_diff` |
| Unified AI Agent | 10 | `checkout_librarian_to_book`, `sync_book_and_librarian` |
| Cortex Agent | 12 | `cortex_complete`, `cortex_reason` |
| Cortex Analyst | 14 | `analyst_ask`, `create_semantic_model`, `cortex_bootstrap_semantic_model` |
| Console Dashboard | 5 | `start_console_server`, `broadcast_console_message` |
| dbt Integration | 8 | `create_dbt_project`, `generate_dbt_model` |
| Data Quality | 7 | `generate_expectation_suite`, `run_validation` |
| Wright Module | 29 | `create_mart_config`, `generate_mart_pipeline`, `wright_to_dbt_model`, `cortex_suggest_dbt_tests` |
| Lineage & Impact | 11 | `track_column_lineage`, `analyze_change_impact` |
| Git/CI-CD | 12 | `git_commit`, `github_create_pr` |
| Data Catalog | 19 | `catalog_scan_connection`, `catalog_search`, `catalog_auto_lineage_from_sql` |
| Data Versioning | 12 | `version_create`, `version_diff`, `version_rollback` |
| GraphRAG Engine | 10 | `rag_search`, `rag_validate_output`, `rag_get_context`, `rag_entity_extract` |
| Data Observability | 15 | `obs_record_metric`, `obs_create_alert_rule`, `obs_get_asset_health` |

## Development Rules

1. **Tool-First:** Every capability = @mcp.tool with docstrings
2. **Atomic Commits:** One tool at a time, verify via `fastmcp dev`
3. **Context Limit:** Never return >10 rows raw data; use `df.describe()`
4. **Living Docs:** Run `update_manifest` after tool changes
5. **Review:** Check `docs/LESSONS_LEARNED.md` before changes

## Quick Commands

```bash
# Start services
python start_services.py

# Start UI
python run_ui.py  # Port 5050

# Query Gemini for context
python ask_claude_gemini.py "your question"
python ask_claude_gemini.py --sync  # Sync context
python ask_claude_gemini.py --status  # Check session
```

## Import Tiers (Hierarchy)

| Tier | Columns | Use Case |
|------|---------|----------|
| 1 | 2-3 | Quick: source_value, group_name |
| 2 | 5-7 | Basic: hierarchy_name, parent_name |
| 3 | 10-12 | Full control with explicit IDs |
| 4 | 28+ | Enterprise full format |

## Service Ports

| Service | Port | Notes |
|---------|------|-------|
| UI Dashboard (primary) | 5050 | `python run_ui.py` — Flask-based, main UI |
| Dashboard API Server | 5051 | `python apps/databridge-dashboard/run_dashboard.py` — lightweight, serves API + MCP endpoints |
| Frontend (Docker) | 8000 | |
| Backend (Docker) | 8001 | |
| MySQL | 3308 | |
| Redis | 6381 | |

## Safety

- Keys from `.env` via Pydantic Settings
- No PII in audit logs
- Knowledge Base local-only (not synced)

## Gemini Integration

Use `ask_claude_gemini.py` to leverage Gemini's 1M+ token context:
- Detailed tool examples → GEMINI.md
- Architecture diagrams → GEMINI.md
- Templates/Skills details → GEMINI.md

---
*Query Gemini: `python ask_claude_gemini.py "How do I use the Wright module?"`*
