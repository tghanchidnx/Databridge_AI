# Hierarchy Knowledge Base MCP Tools

> Complete documentation for all MCP tools that integrate with the Hierarchy Knowledge Base application.
> Last updated: 2026-01-28

---

## Overview

The Hierarchy Knowledge Base MCP Tools extend the DataBridge AI MCP server with **38 tools** for managing database connections, comparing schemas and data, and enhanced hierarchy operations via the NestJS backend.

**Total across all modules: 92 MCP tools**

### New Features (January 2026)

- **AI-Powered Hierarchy Viewer**: Make changes via natural language
- **Visual Animations**: See changes animate in real-time
- **Direct Tree Editing**: Double-click rename, drag-drop reorder
- **Undo/Redo Support**: Full history tracking for all changes

### Key Feature: Auto-Sync

**Auto-Sync is enabled by default.** When you make changes via MCP tools (create, update, delete projects/hierarchies), they are automatically synchronized to the NestJS backend. This keeps the MCP server and Web UI in sync without manual intervention.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                       Claude Desktop                             │
│                      (MCP Client)                                │
└─────────────────────────┬───────────────────────────────────────┘
                          │ MCP Protocol (stdio)
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    DataBridge AI MCP Server                      │
│                     (src/server.py)                              │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌───────────┐  │
│  │ Connections │ │   Schema    │ │    Data     │ │ Hierarchy │  │
│  │   Module    │ │   Matcher   │ │   Matcher   │ │  Enhanced │  │
│  └──────┬──────┘ └──────┬──────┘ └──────┬──────┘ └─────┬─────┘  │
└─────────┼───────────────┼───────────────┼──────────────┼────────┘
          │               │               │              │
          └───────────────┴───────────────┴──────────────┘
                          │ HTTP (REST API) [AUTO-SYNC ✓]
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    NestJS Backend                                │
│                   (localhost:3001/api)                           │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌───────────┐  │
│  │ /connections│ │/schema-     │ │ /data-      │ │/smart-    │  │
│  │             │ │  matcher    │ │   matcher   │ │ hierarchy │  │
│  └─────────────┘ └─────────────┘ └─────────────┘ └───────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

### Auto-Sync Flow

```
┌──────────────────┐         ┌──────────────────┐         ┌──────────────────┐
│   Claude/MCP     │   AUTO  │   NestJS Backend │  DIRECT │   React Web UI   │
│   (Local JSON)   │ ──────► │   (MySQL DB)     │ ◄─────► │   (Browser)      │
└──────────────────┘  SYNC   └──────────────────┘         └──────────────────┘
        │                            ▲
        │ ON-DEMAND PULL             │
        └────────────────────────────┘
```

- **MCP → Backend**: Automatic (on every write operation)
- **Backend → MCP**: On-demand (call `sync_from_backend`)
- **Backend ↔ UI**: Real-time (direct API communication)

---

## Tool Categories

| Category | Tool Count | Description |
|----------|------------|-------------|
| **Connections** | 8 | Database connection management and metadata |
| **Schema Matcher** | 4 | Cross-database schema comparison |
| **Data Matcher** | 3 | Row-level data comparison |
| **Hierarchy Enhanced** | 11 | Dashboard, deployment, search, filters, auto-sync |
| **Existing Tools** | ~46 | Data loading, profiling, comparison, etc. |

---

## Auto-Sync Feature

### How It Works

When auto-sync is enabled (default), all write operations on hierarchy data automatically propagate to the NestJS backend:

| Operation | MCP Tool | Auto-Sync Action |
|-----------|----------|------------------|
| Create project | `create_hierarchy_project` | Creates in backend |
| Delete project | `delete_hierarchy_project` | Deletes from backend |
| Create hierarchy | `create_hierarchy` | Creates in backend |
| Update hierarchy | `update_hierarchy` | Updates in backend |
| Delete hierarchy | `delete_hierarchy` | Deletes from backend |
| Add mapping | `add_source_mapping` | Syncs mapping to backend |

### Response Format

When auto-sync is enabled, responses include sync status:

```json
{
  "status": "success",
  "project": { ... },
  "sync": {
    "auto_sync": "enabled",
    "synced": true
  }
}
```

If sync fails but local operation succeeds:

```json
{
  "status": "success",
  "project": { ... },
  "sync": {
    "auto_sync": "enabled",
    "synced": false,
    "error": "Backend not reachable"
  }
}
```

### `configure_auto_sync`

Enable or disable automatic synchronization.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| enabled | bool | No | True to enable, False to disable (default: True) |

**Returns:** JSON with new sync configuration status.

**Example:**
```python
# Disable auto-sync
configure_auto_sync(enabled=False)

# Re-enable auto-sync
configure_auto_sync(enabled=True)
```

---

## 1. Connections Module

### `list_backend_connections`
List all database connections from the NestJS backend.

**Returns:** JSON array of connections with id, name, type, host, port, database, status.

---

### `get_backend_connection`
Get detailed information about a specific database connection.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| connection_id | string | Yes | Connection UUID |

---

### `test_backend_connection`
Test a database connection's health and connectivity.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| connection_id | string | Yes | Connection UUID to test |

**Returns:** Test results including success status, latency, and any error messages.

---

### `get_connection_databases`
List all databases available in a connection.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| connection_id | string | Yes | Connection UUID |

---

### `get_connection_schemas`
List all schemas in a database.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| connection_id | string | Yes | Connection UUID |
| database | string | Yes | Database name |

---

### `get_connection_tables`
List all tables in a schema.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| connection_id | string | Yes | Connection UUID |
| database | string | Yes | Database name |
| schema | string | Yes | Schema name |

---

### `get_connection_columns`
Get column details for a table.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| connection_id | string | Yes | Connection UUID |
| database | string | Yes | Database name |
| schema | string | Yes | Schema name |
| table | string | Yes | Table name |

**Returns:** JSON array of columns with name, data type, nullable, and other metadata.

---

### `get_column_distinct_values`
Get distinct values from a specific column.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| connection_id | string | Yes | Connection UUID |
| database | string | Yes | Database name |
| schema | string | Yes | Schema name |
| table | string | Yes | Table name |
| column | string | Yes | Column name |
| limit | int | No | Max values (default: 100) |

---

## 2. Schema Matcher Module

### `compare_database_schemas`
Compare schemas between two tables from different database connections.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| source_connection_id | string | Yes | Source connection UUID |
| source_database | string | Yes | Source database name |
| source_schema | string | Yes | Source schema name |
| source_table | string | Yes | Source table name |
| target_connection_id | string | Yes | Target connection UUID |
| target_database | string | Yes | Target database name |
| target_schema | string | Yes | Target schema name |
| target_table | string | Yes | Target table name |

**Identifies:**
- Columns present in source but not in target
- Columns present in target but not in source
- Columns with different data types
- Columns with different nullability

---

### `get_schema_comparison_result`
Get the result of a previously executed schema comparison job.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| job_id | string | Yes | Comparison job UUID |

---

### `list_schema_comparisons`
List all schema comparison jobs.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| limit | int | No | Maximum jobs (default: 50) |

---

### `generate_merge_sql_script`
Generate a MERGE SQL script for synchronizing data between two tables.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| source_connection_id | string | Yes | Source connection UUID |
| source_database | string | Yes | Source database name |
| source_schema | string | Yes | Source schema name |
| source_table | string | Yes | Source table name |
| target_connection_id | string | Yes | Target connection UUID |
| target_database | string | Yes | Target database name |
| target_schema | string | Yes | Target schema name |
| target_table | string | Yes | Target table name |
| key_columns | string | Yes | Comma-separated key columns |
| script_type | string | No | MERGE, INSERT, UPDATE, DELETE |

---

## 3. Data Matcher Module

### `compare_table_data`
Compare data between two tables at the row level.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| source_connection_id | string | Yes | Source connection UUID |
| source_database | string | Yes | Source database name |
| source_schema | string | Yes | Source schema name |
| source_table | string | Yes | Source table name |
| target_connection_id | string | Yes | Target connection UUID |
| target_database | string | Yes | Target database name |
| target_schema | string | Yes | Target schema name |
| target_table | string | Yes | Target table name |
| key_columns | string | Yes | Comma-separated key columns |
| compare_columns | string | No | Columns to compare (default: all) |

**Identifies:**
- Rows in source but not in target (orphans)
- Rows in target but not in source (orphans)
- Rows with same key but different values (conflicts)
- Rows that match exactly

---

### `get_data_comparison_summary`
Get a statistical summary of data comparison between two tables.

**Parameters:** Same as `compare_table_data` (without compare_columns)

**Returns:** Summary statistics: total rows, matches, orphans, conflicts.

---

### `get_backend_table_statistics`
Get profiling statistics for a table from the backend.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| connection_id | string | Yes | Connection UUID |
| database | string | Yes | Database name |
| schema | string | Yes | Schema name |
| table | string | Yes | Table name |

**Returns:** Row count, column statistics (min, max, distinct count, null count), data distribution.

---

## 4. Enhanced Hierarchy Tools

### `get_dashboard_stats`
Get dashboard statistics from the NestJS backend.

**Returns:** Statistics including project count, hierarchy count, deployment stats, and activity summaries.

---

### `get_recent_activities`
Get recent activities from the backend dashboard.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| limit | int | No | Maximum activities (default: 10) |

---

### `search_hierarchies_backend`
Search hierarchies within a project via the backend.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| project_id | string | Yes | Project UUID |
| query | string | Yes | Search query string |

---

### `generate_deployment_scripts`
Generate SQL deployment scripts for a hierarchy project via the backend.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| project_id | string | Yes | Project UUID |
| table_name | string | No | Target table name (default: HIERARCHY_MASTER) |
| view_name | string | No | Target view name (default: V_HIERARCHY_MASTER) |
| include_insert | bool | No | Include INSERT script (default: true) |
| include_view | bool | No | Include VIEW script (default: true) |

---

### `push_hierarchy_to_snowflake`
Deploy a hierarchy project to Snowflake.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| project_id | string | Yes | Project UUID to deploy |
| connection_id | string | Yes | Snowflake connection UUID |
| target_database | string | Yes | Target database name |
| target_schema | string | Yes | Target schema name |
| target_table | string | Yes | Target table name |

---

### `get_deployment_history`
Get deployment history for a project.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| project_id | string | Yes | Project UUID |
| limit | int | No | Maximum entries (default: 50) |

---

### `export_hierarchy_csv_backend`
Export hierarchy to CSV via the NestJS backend.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| project_id | string | Yes | Project UUID |

---

### `import_hierarchy_csv_backend`
Import hierarchy from CSV via the NestJS backend.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| project_id | string | Yes | Target project UUID |
| csv_content | string | Yes | CSV content as string |

---

### `create_filter_group_backend`
Create a filter group via the NestJS backend.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| project_id | string | Yes | Project UUID |
| group_name | string | Yes | Name for the filter group |
| filters | string | Yes | JSON string of filter definitions |

---

### `list_filter_groups_backend`
List all filter groups for a project via the backend.

**Parameters:**
| Name | Type | Required | Description |
|------|------|----------|-------------|
| project_id | string | Yes | Project UUID |

---

## Authentication & Configuration

All tools that communicate with the NestJS backend use API Key authentication:

```
Header: X-API-Key: <your-api-key>
```

Configure in `.env`:
```env
NESTJS_BACKEND_URL=http://localhost:3001/api
NESTJS_API_KEY=dev-key-1
NESTJS_SYNC_ENABLED=true
```

### Auto-Sync Configuration

Auto-sync is **enabled by default** when `NESTJS_SYNC_ENABLED=true`. To control auto-sync at runtime:

```python
# Check sync status
sync_backend_health()  # Returns auto_sync_enabled, sync_mode, etc.

# Disable auto-sync (manual mode)
configure_auto_sync(enabled=False)

# Re-enable auto-sync
configure_auto_sync(enabled=True)
```

---

## Example Workflows

### 1. Schema Comparison Workflow

```
1. list_backend_connections
   → Get list of available database connections

2. get_connection_databases(connection_id="conn-123")
   → List databases in the connection

3. get_connection_tables(connection_id="conn-123", database="WAREHOUSE", schema="PUBLIC")
   → List tables to compare

4. compare_database_schemas(
     source_connection_id="conn-123",
     source_database="WAREHOUSE",
     source_schema="PUBLIC",
     source_table="CUSTOMERS",
     target_connection_id="conn-456",
     target_database="STAGING",
     target_schema="PUBLIC",
     target_table="CUSTOMERS"
   )
   → Get detailed schema differences

5. generate_merge_sql_script(...)
   → Generate SQL to synchronize schemas
```

### 2. Hierarchy Deployment Workflow

```
1. get_dashboard_stats()
   → Check overall system status

2. list_backend_projects()
   → List available hierarchy projects

3. validate_hierarchy_project(project_id="proj-123")
   → Validate project before deployment

4. generate_deployment_scripts(
     project_id="proj-123",
     table_name="MY_HIERARCHY",
     include_insert=true,
     include_view=true
   )
   → Generate deployment SQL

5. push_hierarchy_to_snowflake(
     project_id="proj-123",
     connection_id="snowflake-conn",
     target_database="ANALYTICS",
     target_schema="REPORTING",
     target_table="DIM_HIERARCHY"
   )
   → Deploy to Snowflake

6. get_deployment_history(project_id="proj-123")
   → Verify deployment status
```

---

## Testing

Run the test suite to verify all tools are working:

```bash
# Run all Hierarchy KB tests
pytest tests/test_hierarchy_kb.py -v

# Run specific module tests
pytest tests/test_hierarchy_kb.py::TestConnectionsModule -v
pytest tests/test_hierarchy_kb.py::TestSchemaMatcherModule -v
pytest tests/test_hierarchy_kb.py::TestDataMatcherModule -v

# Run with verbose output
pytest tests/test_hierarchy_kb.py -v --tb=long
```

---

## Requirements

- **Python 3.10+**
- **NestJS Backend** running at localhost:3001
- **Dependencies:** fastmcp, requests, pydantic, pydantic-settings

No additional Python packages required beyond existing DataBridge AI dependencies.

---

## Troubleshooting

### Backend Not Reachable
```
Error: {"error": True, "message": "Backend not reachable"}
```
**Solution:** Ensure NestJS backend is running at the configured URL.

### Backend Sync Not Enabled
```
Error: {"error": "Backend sync not enabled"}
```
**Solution:** Set `NESTJS_SYNC_ENABLED=true` in `.env`.

### Connection Test Failed
```
Error: Connection test failed
```
**Solution:** Verify database credentials in the NestJS backend configuration.
