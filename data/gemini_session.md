# Gemini Session Context

Last Updated: 2026-02-09T03:17:55.075507

Full GEMINI.md context synced to Gemini for detailed reference.

---

# Gemini Session Context

Last Updated: 2026-02-09T02:55:18.624999

Full GEMINI.md context synced to Gemini for detailed reference.

---

# Gemini Session Context

Last Updated: 2026-02-09T02:52:50.843961

Full GEMINI.md context synced to Gemini for detailed reference.

---

# Gemini Session Context

Last Updated: 2026-02-09T02:23:01.126756

Full GEMINI.md context synced to Gemini for detailed reference.

---

# Gemini Session Context

Last Updated: 2026-02-09T00:52:42.585183

Full GEMINI.md context synced to Gemini for detailed reference.

---

# Gemini Session Context

Last Updated: 2026-02-09T00:32:39.602013

Full GEMINI.md context synced to Gemini for detailed reference.

---

# Gemini Session Context

Last Updated: 2026-02-09T00:26:38.211609

Full GEMINI.md context synced to Gemini for detailed reference.

---

# Gemini Session Context

Last Updated: 2026-02-09T00:01:04.870468

Full GEMINI.md context synced to Gemini for detailed reference.

---

# Gemini Session Context

Last Updated: 2026-02-08T23:51:03.138838

Full GEMINI.md context synced to Gemini for detailed reference.

---

# Gemini Session Context

Last Updated: 2026-02-08T23:50:33.913841

Full GEMINI.md context synced to Gemini for detailed reference.

---

# Gemini Session Context

Last Updated: 2026-02-08T23:49:43.374255

Full GEMINI.md context synced to Gemini for detailed reference.

---

# Gemini Session Context

Last Updated: 2026-02-08T23:37:47.986565

Full GEMINI.md context synced to Gemini for detailed reference.

---

# Gemini Session Context

Last Updated: 2026-02-08T02:29:03.669440

Full GEMINI.md context synced to Gemini for detailed reference.

---

# DataBridge AI: Detailed Reference Guide

> **For Gemini Context Memory** - This file contains detailed documentation, examples, and architecture diagrams.
> Claude uses compact CLAUDE.md for quick reference and queries Gemini for detailed info.

---

## Module Reference (292 Tools)

### 1. File Discovery & Staging (3 tools)

```python
# Search for files
find_files(pattern="*.csv", search_name="hierarchy")

# Stage to data directory
stage_file("/Users/john/Downloads/my_hierarchy.csv")

# Check working directory
get_working_directory()
```

---

### 2. Data Reconciliation (38 tools)

**Data Loading:** `load_csv`, `load_json`, `query_database`
**Profiling:** `profile_data`, `detect_schema_drift`
**Comparison:** `compare_hashes`, `get_orphan_details`, `get_conflict_details`
**Fuzzy Matching:** `fuzzy_match_columns`, `fuzzy_deduplicate`
**OCR/PDF:** `extract_text_from_pdf`, `ocr_image`, `parse_table_from_text`
**Transforms:** `transform_column`, `merge_sources`
**Workflow:** `save_workflow_step`, `get_workflow`, `clear_workflow`, `get_audit_log`

---

### 3. Hierarchy Knowledge Base (44 tools)

#### Flexible Import Tiers

| Tier | Columns | Use Case |
|------|---------|----------|
| **Tier 1** | 2-3 | Quick grouping (source_value, group_name) |
| **Tier 2** | 5-7 | Basic parent-child (hierarchy_name, parent_name) |
| **Tier 3** | 10-12 | Full control with explicit IDs |
| **Tier 4** | 28+ | Enterprise full format |

**Tier 1 Example:**
```csv
source_value,group_name
4100,Revenue
4200,Revenue
5100,COGS
```

**Tier 2 Example:**
```csv
hierarchy_name,parent_name,source_value,sort_order
Revenue,,4%,1
Product Revenue,Revenue,41%,2
Service Revenue,Revenue,42%,3
```

**Import Workflow:**
```python
# 1. Detect format
detect_hierarchy_format(content)

# 2. Configure defaults
configure_project_defaults(project_id, database, schema, table, column)

# 3. Preview
preview_import(content, source_defaults)

# 4. Import
import_flexible_hierarchy(project_id, content, source_defaults)

# 5. Export simplified
export_hierarchy_simplified(project_id, target_tier)
```

#### CSV Columns Reference

**Hierarchy CSV:**
- HIERARCHY_ID, HIERARCHY_NAME, PARENT_ID, DESCRIPTION
- LEVEL_1 - LEVEL_10, LEVEL_1_SORT - LEVEL_10_SORT
- INCLUDE_FLAG, EXCLUDE_FLAG, FORMULA_GROUP, SORT_ORDER

**Mapping CSV:**
- HIERARCHY_ID, MAPPING_INDEX
- SOURCE_DATABASE, SOURCE_SCHEMA, SOURCE_TABLE, SOURCE_COLUMN
- SOURCE_UID, PRECEDENCE_GROUP, INCLUDE_FLAG, EXCLUDE_FLAG

**IMPORTANT:** Sort orders come from HIERARCHY CSV, not MAPPING CSV.

---

### 4. SQL Discovery (2 tools)

```python


## Latest Updates (2026-02-08)

### Wright Pipeline Builder UI Added
- Navigation: ✈️ Wright Builder in sidebar
- 4 tabs: VW_1, DT_2, DT_3A, DT_3 with configurable dropdowns
- Features: Generate SQL, Copy, Export bundle, Save config
- Commits: 4ba0b54 (UI 648 lines), 142d387 (SQL examples 1490 lines)

### Current Tool Count: 292 tools across 20 modules

