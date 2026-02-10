# DataShield — Confidential Data Scrambling Module

## Context

DataBridge AI clients need to use the platform (Researcher, Wright Builder, Hierarchy projects) without exposing proprietary data to AI. DataShield is an **independent, optional module** that scrambles data before DataBridge reads it — preserving patterns, metadata, cardinality, distributions, and referential integrity so data warehouse design works normally, but confidential values are unreadable without the client's key.

**Key principle**: Unlike encryption, scrambled data remains *visible and usable* for analytics design. Same key + same input = same output (deterministic), so joins, foreign keys, and aggregations all work correctly.

---

## Module Structure

```
src/datashield/
├── __init__.py              # Public API exports
├── types.py                 # Pydantic models (configs, enums, rules)
├── constants.py             # Default synthetic data pools, regex patterns
├── engine.py                # Scrambling engine (6 strategies)
├── key_manager.py           # Local encrypted keystore (Fernet + PBKDF2)
├── classifier.py            # Auto-classify columns (reuses PII patterns from data_catalog)
├── service.py               # Core CRUD: shield configs, projects
├── snowflake_generator.py   # Generate UDFs + Views DDL for Snowflake
├── interceptor.py           # DataFrame interceptor for CSV/local data
└── mcp_tools.py             # MCP tool registration (~12 tools)
```

---

## Core Types

### Enums
- `ScrambleStrategy`: format_preserving_hash, numeric_scaling, synthetic_substitution, date_shift, pattern_preserving, passthrough
- `ColumnClassification`: measure, fact_dimension, descriptive, geographic, temporal, identifier, code, sensitive_pii, safe
- `ShieldScope`: table, column, schema

### Models
- `ColumnRule`: Per-column scrambling configuration
- `TableShieldConfig`: Per-table shield with column rules, key columns, skip columns
- `ShieldProject`: Top-level project with key alias and table configs

---

## Scrambling Engine (6 strategies)

| Strategy | Input | Output | Algorithm |
|----------|-------|--------|-----------|
| format_preserving_hash | `"INV-2024-00847"` | `"INV-7391-04218"` | HMAC-SHA256 → map digits/letters preserving format |
| numeric_scaling | `$1,234,567.89` | `$987,654.32` | Multiply by key-derived factor (0.5–2.0) |
| synthetic_substitution | `"Acme Corp"` | `"Vertex Industries"` | HMAC → index into synthetic pool |
| date_shift | `2024-03-15` | `2024-06-22` | Shift by key-derived offset (±30-365 days) |
| pattern_preserving | `"555-123-4567"` | `"555-847-2913"` | Regex-aware: preserve structure, scramble content |
| passthrough | `"Active"` | `"Active"` | No change |

**Determinism**: `HMAC-SHA256(project_key + column_name, value)` ensures same value → same output per column.

---

## Key Management

- Local keystore at `data/datashield_keystore.enc` (Fernet-encrypted JSON)
- Master key derived from user passphrase via PBKDF2 (100k iterations)
- Per-project isolation with random 256-bit keys
- Never transmitted — client-only reversal

---

## MCP Tools (12)

1. `create_shield_project` — Create project with name and passphrase
2. `list_shield_projects` — List all projects
3. `get_shield_project` — Get project details
4. `delete_shield_project` — Remove project
5. `auto_classify_table` — Auto-detect column classifications
6. `add_table_shield` — Add/update table shield config
7. `remove_table_shield` — Remove table from project
8. `preview_scrambled_data` — Before/after preview (5 rows)
9. `generate_shield_ddl` — Generate Snowflake UDFs + Views DDL
10. `deploy_shield_to_snowflake` — Execute DDL on Snowflake
11. `shield_local_file` — Scramble CSV/JSON file
12. `get_shield_status` — Project status and key health

---

## License Tier

**PRO tier** — add `'datashield'` to `PRO_MODULES`.

---

## Implementation Order

1. types.py → 2. constants.py → 3. engine.py → 4. key_manager.py → 5. classifier.py → 6. service.py → 7. snowflake_generator.py → 8. mcp_tools.py → 9. __init__.py → 10. server.py → 11. plugins/__init__.py
