# DataBridge AI - Tool Manifest

> Auto-generated documentation for all MCP tools.
> Last updated: 2026-01-28

---

## Overview

DataBridge AI provides **92 MCP tools** across three major modules:

### Module Summary

| Module | Tools | Description |
|--------|-------|-------------|
| Data Reconciliation | 38 | Bridges messy sources (OCR/PDF/SQL) with structured comparison |
| Hierarchy Knowledge Base | 38 | Creates and manages hierarchical data structures |
| Templates, Skills & KB | 16 | Pre-built templates, AI expertise, client knowledge |

### Data Reconciliation Tools (38)

| Category | Tools |
|----------|-------|
| Data Loading | `load_csv`, `load_json`, `query_database` |
| Profiling | `profile_data`, `detect_schema_drift` |
| Comparison | `compare_hashes`, `get_orphan_details`, `get_conflict_details` |
| Fuzzy Matching | `fuzzy_match_columns`, `fuzzy_deduplicate` |
| PDF/OCR | `extract_text_from_pdf`, `ocr_image`, `parse_table_from_text` |
| Workflow | `save_workflow_step`, `get_workflow`, `clear_workflow`, `get_audit_log` |
| Transform | `transform_column`, `merge_sources` |
| Documentation | `update_manifest` |

### Hierarchy Knowledge Base Tools (38)

| Category | Tools |
|----------|-------|
| Projects | `create_hierarchy_project`, `list_hierarchy_projects`, `delete_hierarchy_project` |
| Hierarchies | `create_hierarchy`, `update_hierarchy`, `delete_hierarchy`, `get_hierarchy_tree` |
| Mappings | `add_source_mapping`, `remove_source_mapping`, `get_inherited_mappings`, `get_mapping_summary` |
| Import/Export | `export_hierarchy_csv`, `export_mapping_csv`, `import_hierarchy_csv`, `import_mapping_csv` |
| Formulas | `create_formula_group`, `add_formula_rule`, `list_formula_groups` |
| Deployment | `generate_hierarchy_scripts`, `push_hierarchy_to_snowflake`, `get_deployment_history` |
| Backend Sync | `sync_to_backend`, `sync_from_backend`, `configure_auto_sync` |
| Connections | `list_backend_connections`, `get_connection_databases`, `get_connection_tables` |
| Schema Matching | `compare_database_schemas`, `generate_merge_sql_script` |
| Data Matching | `compare_table_data`, `get_data_comparison_summary` |

### Templates, Skills & Knowledge Base Tools (16)

| Category | Tools |
|----------|-------|
| Templates | `list_financial_templates`, `get_template_details`, `create_project_from_template` |
| Skills | `list_available_skills`, `get_skill_details`, `get_skill_prompt` |
| Knowledge Base | `list_client_profiles`, `get_client_knowledge`, `update_client_knowledge` |
| Documentation | `list_application_documentation`, `get_application_documentation` |

---

## Tool Reference

### Data Loading

#### `load_csv`
Load a CSV file and return a preview with schema information.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `file_path` | string | Yes | Path to the CSV file |
| `preview_rows` | int | No | Number of rows to preview (max 10, default 5) |

**Returns:** JSON with schema info, dtypes, null counts, and sample data.

---

#### `load_json`
Load a JSON file (array or object) and return a preview.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `file_path` | string | Yes | Path to the JSON file |
| `preview_rows` | int | No | Number of rows to preview (max 10, default 5) |

**Returns:** JSON with schema info and sample data.

---

#### `query_database`
Execute a SQL query and return results.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `connection_string` | string | Yes | SQLAlchemy connection string |
| `query` | string | Yes | SQL SELECT query to execute |
| `preview_rows` | int | No | Maximum rows to return (max 10) |

**Returns:** JSON with query results and metadata.

**Note:** Only SELECT queries are allowed for safety.

---

### Data Profiling

#### `profile_data`
Analyze data structure and quality. Identifies table type and anomalies.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source_path` | string | Yes | Path to CSV file to profile |

**Returns:** JSON with:
- Row/column counts
- Structure type (Fact vs Dimension)
- Potential key columns
- High/low cardinality columns
- Data quality metrics (nulls, duplicates)
- Statistical summary

---

#### `detect_schema_drift`
Compare schemas between two CSV files to detect drift.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source_a_path` | string | Yes | Path to first CSV (baseline) |
| `source_b_path` | string | Yes | Path to second CSV (target) |

**Returns:** JSON with added columns, removed columns, and type changes.

---

### Comparison Engine

#### `compare_hashes`
Compare two CSV sources by hashing rows to identify orphans and conflicts.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source_a_path` | string | Yes | Path to the first CSV file (source of truth) |
| `source_b_path` | string | Yes | Path to the second CSV file (target) |
| `key_columns` | string | Yes | Comma-separated key column names |
| `compare_columns` | string | No | Columns to compare (defaults to all non-key) |

**Returns:** JSON statistical summary:
```json
{
  "statistics": {
    "orphans_only_in_source_a": 50,
    "orphans_only_in_source_b": 10,
    "total_orphans": 60,
    "conflicts": 25,
    "exact_matches": 915,
    "match_rate_percent": 97.33
  }
}
```

---

#### `get_orphan_details`
Retrieve details of orphan records (records in one source but not the other).

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source_a_path` | string | Yes | Path to the first CSV file |
| `source_b_path` | string | Yes | Path to the second CSV file |
| `key_columns` | string | Yes | Comma-separated key column names |
| `orphan_source` | string | No | Which orphans: 'a', 'b', or 'both' (default) |
| `limit` | int | No | Maximum orphans to return (max 10) |

**Returns:** JSON with orphan record samples.

---

#### `get_conflict_details`
Retrieve details of conflicting records (same key, different values).

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source_a_path` | string | Yes | Path to the first CSV file |
| `source_b_path` | string | Yes | Path to the second CSV file |
| `key_columns` | string | Yes | Comma-separated key column names |
| `compare_columns` | string | No | Columns to compare |
| `limit` | int | No | Maximum conflicts to return (max 10) |

**Returns:** JSON with conflict details showing both versions side-by-side.

---

### Fuzzy Matching

#### `fuzzy_match_columns`
Find fuzzy matches between two columns using RapidFuzz.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source_a_path` | string | Yes | Path to the first CSV file |
| `source_b_path` | string | Yes | Path to the second CSV file |
| `column_a` | string | Yes | Column name in source A |
| `column_b` | string | Yes | Column name in source B |
| `threshold` | int | No | Minimum similarity (0-100, default 80) |
| `limit` | int | No | Maximum matches to return (max 10) |

**Returns:** JSON with fuzzy match results including similarity scores.

---

#### `fuzzy_deduplicate`
Find potential duplicate values within a single column.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source_path` | string | Yes | Path to the CSV file |
| `column` | string | Yes | Column to check for duplicates |
| `threshold` | int | No | Minimum similarity (0-100, default 90) |
| `limit` | int | No | Maximum groups to return (max 10) |

**Returns:** JSON with potential duplicate groups.

---

### PDF/OCR

#### `extract_text_from_pdf`
Extract text content from a PDF file.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `file_path` | string | Yes | Path to the PDF file |
| `pages` | string | No | Pages to extract: 'all', '1,2,3', or '1-5' |

**Returns:** JSON with extracted text per page.

---

#### `ocr_image`
Extract text from an image using OCR (Tesseract).

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `file_path` | string | Yes | Path to the image file |
| `language` | string | No | Tesseract language code (default 'eng') |

**Returns:** JSON with extracted text.

**Requires:** Tesseract OCR installed and configured in `.env`.

---

#### `parse_table_from_text`
Attempt to parse tabular data from extracted text.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `text` | string | Yes | Raw text containing tabular data |
| `delimiter` | string | No | Column delimiter: 'auto', 'tab', 'space', 'pipe' |

**Returns:** JSON with parsed table data.

---

### Workflow Management

#### `save_workflow_step`
Save a reconciliation step to the workflow recipe.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `step_name` | string | Yes | Descriptive name for this step |
| `step_type` | string | Yes | Type of operation |
| `parameters` | string | Yes | JSON string of parameters |

**Returns:** Confirmation with step ID and total steps.

---

#### `get_workflow`
Retrieve the current workflow recipe.

**Returns:** JSON with all workflow steps.

---

#### `clear_workflow`
Clear all steps from the current workflow.

**Returns:** Confirmation message.

---

#### `get_audit_log`
Retrieve recent entries from the audit trail.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `limit` | int | No | Maximum entries to return (max 10) |

**Returns:** JSON with recent audit entries.

---

### Data Transformation

#### `transform_column`
Apply a transformation to a column.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source_path` | string | Yes | Path to the CSV file |
| `column` | string | Yes | Column name to transform |
| `operation` | string | Yes | Operation: 'upper', 'lower', 'strip', 'trim_spaces', 'remove_special' |
| `output_path` | string | No | Path to save transformed file |

**Returns:** JSON with transformation preview.

---

#### `merge_sources`
Merge two CSV sources on key columns.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source_a_path` | string | Yes | Path to the first CSV file |
| `source_b_path` | string | Yes | Path to the second CSV file |
| `key_columns` | string | Yes | Comma-separated key column names |
| `merge_type` | string | No | Merge type: 'inner', 'left', 'right', 'outer' |
| `output_path` | string | No | Path to save merged file |

**Returns:** JSON with merge statistics and preview.

---

### Documentation

#### `update_manifest`
Regenerate the MANIFEST.md documentation from tool docstrings.

**Returns:** Confirmation message with tool count.

---

## Internal Helpers (Not Exposed as Tools)

| Helper | Purpose |
|--------|---------|
| `log_action` | Records audit entries to `data/audit_trail.csv` |
| `compute_row_hash` | Generates SHA-256 hashes for row comparison |
| `truncate_dataframe` | Enforces context sensitivity limits |
