# Faux Objects: Semantic View Wrappers for BI Tools

## What Are Faux Objects?

**Faux Objects** are standard Snowflake objects (views, stored procedures, dynamic tables, tasks) that **wrap Semantic Views** so BI tools like Power BI, Tableau, and Excel can consume them without understanding Snowflake's `SEMANTIC_VIEW()` syntax.

They're called "faux" because they **look like regular database objects** to BI tools, but underneath they're powered by Semantic Views.

---

## Why Do We Need Them?

Snowflake Semantic Views are powerful, but most BI tools can't call `SELECT * FROM SEMANTIC_VIEW(...)` directly. Faux Objects bridge this gap:

| Faux Object Type | Snowflake Object | Use Case | BI Tool Support |
|------------------|------------------|----------|-----------------|
| **View** | `CREATE VIEW` | Static reports, dashboards | Universal |
| **Stored Procedure** | Python `RETURNS TABLE` | Parameterized queries | Power BI, Tableau |
| **Dynamic Table** | `CREATE DYNAMIC TABLE` | Auto-refreshing dashboards | Universal |
| **Task** | Task + materializer | Scheduled batch jobs | Universal |

---

## The 7 Persona Tutorials

Each tutorial follows a real-world persona from DataBridge's skill library. You'll build a complete semantic view, configure faux objects, and generate deployment SQL.

| # | Persona | Semantic View | Difficulty | Key Concepts |
|---|---------|---------------|------------|--------------|
| 12 | [Financial Analyst](12_financial_analyst/README.md) | GL Reconciliation | Easy-Medium | Views, procedures with 3 params, filtered views |
| 13 | [Oil & Gas Analyst](13_oil_gas_analyst/README.md) | Drilling Economics | Medium | Procedures with facts, dynamic tables, NULLIF |
| 14 | [Operations Analyst](14_operations_analyst/README.md) | Geographic Operations | Medium | Views, tasks with CRON schedules |
| 15 | [Cost Analyst](15_cost_analyst/README.md) | Cost Allocation | Medium-Hard | CASE WHEN metrics, static WHERE, persistence |
| 16 | [Manufacturing Analyst](16_manufacturing_analyst/README.md) | Plant Operations | Medium | Dynamic tables, INT types, batch generation |
| 17 | [SaaS Analyst](17_saas_analyst/README.md) | Subscription Metrics | Hard | Complex NULLIF/CASE, 30-min refresh, AI context |
| 18 | [Transportation Analyst](18_transportation_analyst/README.md) | Fleet Operations | Hard | 5-table joins, DATE params, export scripts |

---

## Coverage Matrix

| Persona | VIEW | PROC | DYNAMIC TABLE | TASK | DDL | Batch | Export |
|---------|------|------|---------------|------|-----|-------|--------|
| Financial Analyst | 3 | 1 | - | - | 1 | - | - |
| Oil & Gas Analyst | 1 | 1 | 1 | - | - | 1 | - |
| Operations Analyst | 1 | - | - | 2 | - | - | - |
| Cost Analyst | 1 | 2 | - | - | - | - | - |
| Manufacturing Analyst | 1 | 1 | 1 | - | - | 1 | - |
| SaaS Analyst | 1 | 1 | 1 | - | 1 | - | - |
| Transportation Analyst | 1 | 1 | - | 1 | 1 | - | 1 |

---

## Tools Used Across All Tutorials

| Tool | What It Does |
|------|-------------|
| `create_faux_project` | Create a new project container |
| `define_faux_semantic_view` | Define the source semantic view |
| `add_faux_semantic_table` | Add a table reference |
| `add_faux_semantic_column` | Add a dimension, metric, or fact column |
| `add_faux_semantic_relationship` | Define table joins |
| `add_faux_object` | Configure a faux object wrapper |
| `generate_faux_scripts` | Generate SQL for all faux objects |
| `generate_faux_deployment_bundle` | Single deployment SQL script |
| `export_faux_scripts` | Export individual .sql files |
| `generate_semantic_view_ddl` | Generate the CREATE SEMANTIC VIEW DDL |

---

## Quick Start

If you want to jump right in, try **Use Case 12** (Financial Analyst) first. It's the most accessible and covers the core concepts.

If you're already comfortable with views and procedures, jump to **Use Case 17** (SaaS Analyst) or **Use Case 18** (Transportation Analyst) for the most complex scenarios.

---

## Testing

All tutorials have corresponding pytest tests in `tests/test_faux_objects_personas.py`. See the [Testing Report](testing_report.md) for full results and coverage analysis.

```bash
# Run just the persona tests
python -m pytest tests/test_faux_objects_personas.py -v

# Run full regression (all tests)
python -m pytest tests/ -q
```
