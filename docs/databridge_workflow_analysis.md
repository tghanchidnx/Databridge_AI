# DataBridge AI — Hierarchy-Driven Data Mart Factory

**Technical Design & Analysis Document**

Architecture Design | DDL Analysis | Template Specification | GROSS vs NET LOS Hierarchy Comparison

*DataNexum Consulting — February 2026*

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Architecture Overview](#2-architecture-overview)
3. [DDL Script Analysis – Upstream GROSS LOS](#3-ddl-script-analysis--upstream-gross-los)
4. [Source Data Analysis](#4-source-data-analysis)
5. [Join Pattern Analysis](#5-join-pattern-analysis)
6. [Dimension Table & ID_SOURCE Mapping](#6-dimension-table--id_source-mapping)
7. [Formula Precedence Chains](#7-formula-precedence-chains)
8. [Schema Differences](#8-schema-differences)
9. [Data Quality Findings](#9-data-quality-findings)
10. [GROUP_FILTER_PRECEDENCE (GROSS-Only Feature)](#10-group_filter_precedence-gross-only-feature)
11. [Template Configuration Specification](#11-template-configuration-specification)
12. [dbt YAML Configuration](#12-dbt-yaml-configuration)
13. [Implementation Roadmap](#13-implementation-roadmap)
14. [Next Steps](#14-next-steps)

---

## 1. Executive Summary

This document consolidates the complete technical analysis of DataBridge AI's hierarchy-driven data mart automation framework. It covers the architectural design, actual DDL script analysis, source data validation from both GROSS and NET Lease Operating Statement (LOS) hierarchies, and the finalized template specification that enables a single set of dbt macros to generate data marts for any hierarchy configuration.

The analysis confirms that a unified template approach is viable. Seven configuration variables fully parameterize the differences between the GROSS and NET LOS pipelines, and the same engine can extend to future hierarchy projects without code changes.

### Key Findings

- **4-Object Pipeline Pattern:** VW_1 (Translation View) → DT_2 (Granularity Table) → DT_3A (Pre-Aggregation Fact) → DT_3 (Data Mart). The DT_3A → DT_3 two-stage pattern is essential for formula precedence correctness.

- **Join Patterns Differ Structurally:** GROSS uses 3 UNION ALL branches (Account | Deduct+Product | Product+Royalty). NET uses 2 branches (Account | Account+Product). These are not subsets of each other — the template must specify exact join column combinations per branch.

- **7 Configuration Variables:** JOIN_PATTERNS[], DYNAMIC_COLUMN_MAP{}, ACCOUNT_SEGMENT, MEASURE_PREFIX, HAS_SIGN_CHANGE, HAS_EXCLUSIONS, HAS_GROUP_FILTER_PRECEDENCE. Everything else is structurally identical.

- **Data Quality Issues Found:** NET has 3 ID_SOURCE typos causing silent data loss risk. GROSS has duplicate Cash Flow rows, FILTER_GROUP_2 mismatches, and one orphan node. All are documented with remediation.

- **Template Viability: Confirmed.** Same dbt macros handle both hierarchies driven entirely by YAML config. Formula precedence engine reads dynamically from DT_2 metadata, requiring no code changes between hierarchies.

---

## 2. Architecture Overview

The Data Mart Factory transforms enterprise reporting hierarchies into automated, BI-ready data marts. The core principle: hierarchies are not just display structures — they encode the complete business logic for how source data should be filtered, joined, aggregated, and presented.

### 2.1 Five-Layer Pipeline

| Layer | Object | Purpose | Key Logic |
|-------|--------|---------|-----------|
| 1 | VW_1 – Translation View | Parse hierarchy mapping metadata into join-ready columns | CASE on ID_SOURCE to route to correct dimension column |
| 2 | DT_2 – Granularity Table | Convert translated mappings to lowest grain for fact joins | UNPIVOT + dynamic column mapping + exclusion filtering |
| 3A | DT_3A – Pre-Aggregation Fact | Join hierarchy to fact table, compute base measures | UNION ALL branches per join pattern, SUM measures by FK |
| 3B | DT_3 – Data Mart | Apply formula precedence, inject calculations, add surrogates | 5-level cascading formulas, DENSE_RANK keys, backfill levels |
| 4 | BI Consumption | Power BI / Tableau connects to DT_3 via surrogate keys | Direct query, no additional transforms needed |

### 2.2 AI Agent Integration Strategy

Six AI agents are proposed to automate the pipeline lifecycle:

| Agent | Role | Technology |
|-------|------|------------|
| Hierarchy Discovery Agent | Scans new hierarchy tables, identifies structure patterns | Cortex COMPLETE() |
| Template Configuration Agent | Generates YAML config from discovered hierarchy | Cortex COMPLETE() + dbt vars |
| DDL Generation Agent | Produces pipeline DDL from config | dbt macros + Jinja2 |
| Validation Agent | Tests generated DDL against source data | dbt tests + SQL assertions |
| Monitoring Agent | Tracks data quality, alerts on drift | Snowflake tasks + alerts |
| Documentation Agent | Auto-generates pipeline documentation | Cortex COMPLETE() |

---

## 3. DDL Script Analysis – Upstream GROSS LOS

Four production DDL scripts were analyzed to reverse-engineer the pipeline pattern and extract configuration variables. These scripts implement the Upstream GROSS Lease Operating Statement data mart.

### 3.1 Script Inventory

| Script | Object Type | Row Estimate | Key Operations |
|--------|------------|--------------|----------------|
| 01_VW_1_UPSTREAM_GROSS_REPORT_HIERARCHY_TRANSLATED.sql | View | ~400 rows | CASE on ID_SOURCE, joins to DIM_ACCOUNT, DIM_DEDUCT, DIM_PRODUCT |
| 02_DT_2_UPSTREAM_GROSS_LOS_REPORT_HIERARCHY.SQL | Dynamic Table | ~400 rows | UNPIVOT filter columns, dynamic column mapping, exclusion NOT IN |
| 03_DT_3A_UPSTREAM_GROSS_LOS.sql | Dynamic Table | ~millions | 3 UNION ALL branches, fact table joins, SUM aggregation by hierarchy key |
| 04_DT_3_UPSTREAM_GROSS_LOS.sql | Dynamic Table | ~millions | 5-level formula cascade, DENSE_RANK surrogate keys, hierarchy backfill |

### 3.2 Layer-by-Layer Breakdown

#### VW_1: Translation View

This view reads from the hierarchy mapping table and translates abstract ID_SOURCE + ID values into physical dimension column references. The core logic is a CASE statement:

```sql
CASE WHEN ID_SOURCE = 'BILLING_CATEGORY_CODE' THEN ACCT.ACCOUNT_BILLING_CATEGORY_CODE
     WHEN ID_SOURCE = 'BILLING_CATEGORY_TYPE_CODE' THEN ACCT.ACCOUNT_BILLING_CATEGORY_TYPE_CODE
     WHEN ID_SOURCE = 'ACCOUNT_CODE' THEN ACCT.ACCOUNT_CODE
END
```

This is the **DYNAMIC_COLUMN_MAP** configuration variable.

#### DT_2: Granularity Dynamic Table

Converts translated mapping rows into the lowest-grain join keys needed for the fact table. Key operations include UNPIVOT of FILTER_GROUP columns, dynamic column mapping resolution, and exclusion filtering via NOT IN subqueries. The output is one row per hierarchy-key-to-dimension-value mapping at the finest grain.

#### DT_3A: Pre-Aggregation Fact

Joins hierarchy metadata from DT_2 to the actual fact table (FACT_FINANCIAL_ACTUALS). Uses UNION ALL with separate branches for each join pattern. Each branch filters on the appropriate dimension keys and aggregates measures (AMOUNT, VOLUME, MCFE) to the hierarchy key level. This is where the ACCOUNT_SEGMENT filter ('GROSS' or 'NET') is applied.

#### DT_3: Data Mart (Final Output)

The most complex object. Performs five key operations:

1. Injects calculated rows using formula metadata from DT_2, cascading through 5 precedence levels
2. Generates DENSE_RANK surrogate keys for BI tool consumption
3. Backfills empty hierarchy levels for clean drill-down
4. Joins extension hierarchy for Operation Team Financial Group
5. Produces the final star-schema-ready output for Power BI

### 3.3 Critical Architectural Insight: DT_3A → DT_3 Split

The two-stage fact pattern (DT_3A for aggregation, DT_3 for formulas) is not arbitrary. Formula precedence requires that base aggregations complete before higher-level calculations can reference them. For example, Gross Profit (Precedence 3) subtracts Total Taxes (Precedence 1) and Total Deducts (Precedence 1) — both must exist as aggregated rows before the subtraction can execute. Merging DT_3A and DT_3 into a single object would break this dependency chain.

---

## 4. Source Data Analysis

Four CSV files from DataBridge were analyzed to validate the DDL-derived template against actual hierarchy metadata:

| File | Rows | Columns | Description |
|------|------|---------|-------------|
| tbl_0_gross_los_report_hierarchy_.csv | 148 | 42 | GROSS hierarchy nodes with flags, levels, formulas |
| tbl_0_gross_los_report_hierarchy_mapping.csv | 399 | 48 | GROSS hierarchy-to-dimension mappings |
| tbl_0_net_los_report_hierarchy.csv | 136 | 41 | NET hierarchy nodes with flags, levels, formulas |
| tbl_0_net_los_report_hierarchy_MAPPING.csv | 222 | 31 | NET hierarchy-to-dimension mappings |

### 4.1 Hierarchy Structure Comparison

| Metric | GROSS LOS | NET LOS |
|--------|-----------|---------|
| Total Nodes | 148 | 136 |
| Active Nodes | 123 | 131 |
| Calculation Nodes | 25 | 25 |
| Volume Nodes | 4 | 4 |
| Hierarchy Group | Lease Operating Statement | Lease Operating Statement |
| Hierarchy Levels | 9 + 9 extension | 9 + 9 extension |
| Sort Order Columns | 18 | 18 |
| Flag Columns | ~12 | ~12 |
| Filter Groups | 4 (FILTER_GROUP_1–4) | 4 (FILTER_GROUP_1–4) |
| Formula Columns | 5 (GROUP, PRECEDENCE, PARAM_REF, LOGIC, PARAM2) | 5 (identical) |

### 4.2 Level 2 Categories

Both hierarchies share the same fundamental structure under Level 1 = "Upstream". Key differences are highlighted:

| Level 2 Category | GROSS | NET | Notes |
|------------------|-------|-----|-------|
| Volumes | ✓ | ✓ | Both: Oil, Gas, NGL, Total MCFE |
| Revenue | ✓ | ✓ | Both: Oil, Gas, NGL sub-categories |
| Taxes | – | ✓ | NET uses Taxes_New as L2 name |
| Taxes (as L2) | ✓ | – | GROSS uses Taxes as L2 name |
| Deducts | ✓ | ✓ | GROSS: 30 deduct nodes. NET: fewer deduct nodes |
| Royalties | ✓ | – | GROSS-only category (3 nodes + Total Royalties) |
| Operating Expense | ✓ | ✓ | Both: LOE Recurring, Non-Recurring, COPAS, SWD |
| Capital Spend | ✓ | ✓ | Both: Tangible, Intangible sub-categories |
| Cumulative Cash Flow | ✓ | ✓ | Both: 5-level precedence cascade |

---

## 5. Join Pattern Analysis

This is the most architecturally significant finding. **The GROSS and NET hierarchies use structurally different join patterns that are not subsets of each other.** The template cannot assume "fewer branches = drop last ones" — it must specify exact join column combinations per branch.

### 5.1 GROSS LOS: 3 UNION ALL Branches

| Branch | Join Keys | Fact Keys | Filter | Nodes | Categories |
|--------|-----------|-----------|--------|-------|------------|
| 1 – Account | LOS_ACCOUNT_ID_FILTER | FK_ACCOUNT_KEY | – | 88 | Revenue, Volumes, OpEx, CapEx |
| 2 – Deduct+Product | LOS_DEDUCT_CODE_FILTER, LOS_PRODUCT_CODE_FILTER | FK_DEDUCT_KEY, FK_PRODUCT_KEY | – | 30 | Taxes, Deducts |
| 3 – Product+Royalty | LOS_PRODUCT_CODE_FILTER | FK_PRODUCT_KEY | ROYALTY_FILTER='Y' | 3 | Royalties |

### 5.2 NET LOS: 2 UNION ALL Branches

| Branch | Join Keys | Fact Keys | Filter | Nodes | Categories |
|--------|-----------|-----------|--------|-------|------------|
| 1 – Account | LOS_ACCOUNT_ID_FILTER | FK_ACCOUNT_KEY | – | 101 | All except tax-product nodes |
| 2 – Account+Product | LOS_ACCOUNT_ID_FILTER, LOS_PRODUCT_CODE_FILTER | FK_ACCOUNT_KEY, FK_PRODUCT_KEY | – | 9 | Tax nodes needing product filter |

### 5.3 Structural Implications

NET Branch 2 uses ACCOUNT+PRODUCT join — which does not exist in GROSS. GROSS Branch 2 uses DEDUCT+PRODUCT join — which does not exist in NET. GROSS Branch 3 uses PRODUCT-only with a WHERE filter — which does not exist in NET. This means the JOIN_PATTERNS[] configuration must be a full array-of-objects, not a simple toggle or count.

---

## 6. Dimension Table & ID_SOURCE Mapping

### 6.1 GROSS Mapping Distribution

399 total mappings across 121 hierarchy keys (average 3.3 mappings per node, max 18). 195 of these are DIM_DEDUCT mappings — nearly half of all GROSS mappings.

| ID_TABLE | ID_SOURCE | Count |
|----------|-----------|-------|
| DIM_ACCOUNT | BILLING_CATEGORY_CODE | 128 |
| DIM_ACCOUNT | BILLING_CATEGORY_TYPE_CODE | 16 |
| DIM_ACCOUNT | ACCOUNT_CODE | 13 |
| DIM_DEDUCT | DEDUCT_CODE | 195 |
| DIM_PRODUCT | PRODUCT_CODE | 44 |
| DT_1_FACT_FINANCIAL_ACTUALS | ROYALTY_FILTER | 3 |

### 6.2 NET Mapping Distribution

222 total mappings across 110 hierarchy keys (average 2.0 mappings per node, max 11). Zero deduct mappings. 3 ID_SOURCE values contain typos.

| ID_TABLE | ID_SOURCE | Count | Notes |
|----------|-----------|-------|-------|
| DIM_ACCOUNT | BILLING_CATEGORY_CODE | 124 | |
| DIM_ACCOUNT | ACCOUNT_CODE | 53 | |
| DIM_ACCOUNT | MINOR_CODE | 20 | Not in GROSS |
| DIM_ACCOUNT | BILLING_CATEGORY_TYPE_CODE | 13 | |
| DIM_ACCOUNT | BILLING_CATEGRY_CODE | 2 | ⚠️ TYPO – missing 'O' |
| DIM_ACCOUNT | BILLING_CATEGORY_TYPE | 1 | ⚠️ TYPO – missing '_CODE' |
| DIM_PRODUCT | PRODUCT_CODE | 9 | |

---

## 7. Formula Precedence Chains

Both hierarchies use an identical 5-level formula precedence cascade. The formula engine in DT_3 reads FORMULA_PARAM_REF dynamically from DT_2 metadata, so no code changes are needed between hierarchies — only the referenced formula groups differ.

| Precedence | Object | GROSS Calculations | NET Calculations |
|------------|--------|--------------------|------------------|
| 1 | DT_3A | Total MCFE, Total Revenue, Total Taxes, Total Deducts, Total OpEx, Total CapEx, Total Royalties | Total MCFE, Total Revenue, Total Taxes, Total Deducts, Total OpEx, Total CapEx, Total LOE Non-Recurring |
| 2 | DT_3 | Total Taxes and Deducts = Taxes + Deducts | Total Taxes and Deducts = Taxes + Deducts |
| 3 | DT_3 | Gross Profit = Revenue - Taxes - Deducts - Royalties | Gross Profit = Revenue - Taxes - Deducts |
| 4 | DT_3 | Operating Income = Revenue - Taxes - Deducts - OpEx - Royalties | Operating Income = Revenue - Taxes - Deducts - OpEx - Non-Recurring |
| 5 | DT_3 | Cash Flow = Revenue - Taxes - Deducts - OpEx - CapEx - Royalties | Cash Flow = Revenue - Taxes - Deducts - OpEx - Non-Recurring - CapEx |

Key observation: GROSS subtracts Royalties at Precedences 3–5. NET subtracts Non-Recurring LOE instead. The formula engine handles this automatically because it reads FORMULA_PARAM_REF (e.g., "Total Royalties" vs "Total LOE Non-Recurring") from the hierarchy metadata — no template code change required.

---

## 8. Schema Differences

### 8.1 Hierarchy Table Schema

| Feature | GROSS (42 cols) | NET (41 cols) | Impact |
|---------|----------------|---------------|--------|
| GROUP_FILTER_PRECEDENCE | Present (values 1–3) | Absent | Drives multi-round filtering in GROSS; NET uses single-round |
| SIGN_CHANGE_FLAG | All false (0 nodes) | 7 nodes true (Volumes + Revenue) | NET multiplies by -1 for sign-changed nodes |
| LEVEL structure | 9 levels + 9 extensions | 9 levels + 9 extensions | Identical |
| Sort columns | 18 (LEVEL_n_SORT) | 18 (LEVEL_n_SORT) | Identical |
| Filter groups | 4 (FILTER_GROUP_1–4) | 4 (FILTER_GROUP_1–4) | Identical |
| Formula columns | 5 (GROUP, PREC, PARAM, LOGIC, PARAM2) | 5 (identical) | Identical |

### 8.2 Mapping Table Schema

| Feature | GROSS (48 cols) | NET (31 cols) | Impact |
|---------|----------------|---------------|--------|
| LEVEL_1..LEVEL_9 (denormalized) | Present (18 extra columns) | Absent | GROSS pre-joins hierarchy levels; NET requires join at query time |
| SIGN_CHANGE_FLAG | Absent | Present | NET includes sign info in mapping; GROSS doesn't need it |
| Core mapping columns | FK_REPORT_KEY, ID, ID_NAME, ID_SOURCE, ID_TABLE, ID_SCHEMA, ID_DATABASE, EXCLUSION_FLAG | Identical | Same structure |

---

## 9. Data Quality Findings

Five data quality issues were discovered during source data analysis. Each requires remediation before or during pipeline execution.

### 9.1 NET ID_SOURCE Typos (3 mapping rows)

**Severity: HIGH.** Two distinct typos in the NET mapping table's ID_SOURCE column would cause silent data loss if not corrected.

| Typo Value | Correct Value | Rows Affected | Issue |
|------------|---------------|---------------|-------|
| BILLING_CATEGRY_CODE | BILLING_CATEGORY_CODE | 2 | Missing 'O' in CATEGORY |
| BILLING_CATEGORY_TYPE | BILLING_CATEGORY_TYPE_CODE | 1 | Missing '_CODE' suffix |

**Impact:** The CASE statement in DT_2 would not match these values, resulting in NULL join columns and rows silently dropping from the pipeline.

**Recommendation:** Either correct the source data or add alias normalization in the DYNAMIC_COLUMN_MAP configuration.

### 9.2 GROSS Duplicate Cash Flow Rows

**Severity: MEDIUM.** GROSS hierarchy keys 217 and 218 both subtract Total Royalties from Cash Flow at Precedence 5. This would cause double-counting of royalty deduction unless the DDL deduplicates. Appears to be a data entry error.

### 9.3 GROSS FILTER_GROUP_2 Mismatches

**Severity: MEDIUM.** 30 FILTER_GROUP_2 values in the GROSS mapping table don't appear in the hierarchy table, and 21 hierarchy FILTER_GROUP_2 values don't appear in mappings. This occurs because: (a) 25 calculation nodes have no mappings, and (b) concatenation logic drifts between tables.

Example: hierarchy has `Capital SpendUpstream CAPEXIntangibleFacility Cost` but mapping has `Capital SpendUpstream CAPEXFacility Cost` (missing "Intangible").

### 9.4 GROSS Orphan Active Node

**Severity: LOW.** Key 148 ("Operating Expense > Recurring > Operated COPAS Overhead") is active, non-calculation, but has zero mapping rows. Would produce zero fact rows in DT_3. Either an intentional placeholder or missing mapping data.

### 9.5 GROSS Zero Exclusions

**Severity: LOW.** GROSS mapping has 0 exclusion rows (all EXCLUSION_FLAG = false). NET has 3 exclusion mappings (FK=1, FK=5, FK=123 excluding specific ACCOUNT_CODE ranges). The template's NOT IN subquery logic in DT_2 must handle the zero-exclusion case gracefully (skip the subquery entirely).

---

## 10. GROUP_FILTER_PRECEDENCE (GROSS-Only Feature)

The GROSS hierarchy includes a GROUP_FILTER_PRECEDENCE column (values 1–3) that drives multi-round filtering logic. NET does not use this pattern — all NET joins are single-round.

### 10.1 Distribution by FILTER_GROUP_1

| FILTER_GROUP_1 | Precedence Values | Interpretation |
|----------------|-------------------|----------------|
| Revenue | 1 only | Single-round join |
| Volumes | 1 only | Single-round join |
| Royalties | 1 only | Single-round join |
| Capital Spend | 1, 2 | Two-round: primary join + secondary filter |
| Operating Expense | 1, 2 | Two-round: primary join + secondary filter |
| Taxes | 1, 2 | Two-round: primary join + secondary filter |
| Deducts | 1, 2, 3 | Three-round: primary + secondary + tertiary filter |

### 10.2 Distribution by ID_TABLE

| ID_TABLE | Precedence 1 | Precedence 2 | Total |
|----------|-------------|-------------|-------|
| DIM_ACCOUNT | 24 rows | 133 rows | 157 |
| DIM_DEDUCT | 105 rows | 90 rows | 195 |
| DIM_PRODUCT | 24 rows | 20 rows | 44 |
| DT_1_FACT_FINANCIAL_ACTUALS | 3 rows | – | 3 |

Interpretation: Precedence 1 = primary dimension join. Precedence 2 = secondary filter dimension applied after the primary join resolves. This enables hierarchies where a node's membership is defined by combinations of dimension values applied in sequence.

---

## 11. Template Configuration Specification

Seven configuration variables fully parameterize the differences between GROSS and NET LOS pipelines. Everything else — hierarchy level structure, sort order generation, calculation row injection, formula precedence engine, surrogate key generation, backfill logic, dimension FK pass-through, and extension hierarchy joins — is structurally identical.

### 11.1 Configuration Variables

| # | Variable | Type | GROSS Value | NET Value | DDL Impact |
|---|----------|------|-------------|-----------|------------|
| 1 | JOIN_PATTERNS[] | Array\<Object\> | 3 branches | 2 branches | DT_3A/DT_3 UNION ALL structure |
| 2 | DYNAMIC_COLUMN_MAP{} | Object | 3 entries | 5+ entries (inc. typo aliases) | DT_2 CASE statement |
| 3 | ACCOUNT_SEGMENT | String | 'GROSS' | 'NET' | DT_2 WHERE, DT_3A filter |
| 4 | MEASURE_PREFIX | String | 'GROSS' | 'NET' | Column names: GROSS_AMOUNT vs NET_AMOUNT |
| 5 | HAS_SIGN_CHANGE | Boolean | false | true (7 nodes) | DT_3A: multiply by -1 when flag true |
| 6 | HAS_EXCLUSIONS | Boolean | false | true (3 exclusions) | DT_2: NOT IN subquery generation |
| 7 | HAS_GROUP_FILTER_PRECEDENCE | Boolean | true (values 1–3) | false | VW_1 join logic, DT_2 filter rounds |

### 11.2 JOIN_PATTERNS[] Detail

Each element in the JOIN_PATTERNS array specifies one UNION ALL branch:

| Property | Description | Example (GROSS Branch 2) |
|----------|-------------|--------------------------|
| name | Human-readable identifier | deduct_product |
| join_keys[] | Columns from DT_2 used in ON clause | [LOS_DEDUCT_CODE_FILTER, LOS_PRODUCT_CODE_FILTER] |
| fact_keys[] | Corresponding FK columns in fact table | [FK_DEDUCT_KEY, FK_PRODUCT_KEY] |
| filter | Optional WHERE condition on fact table | null (or "ROYALTY_FILTER = 'Y'" for Branch 3) |

### 11.3 DYNAMIC_COLUMN_MAP{} Detail

Maps ID_SOURCE values from the hierarchy mapping table to physical dimension column references.

**GROSS Configuration:**

| ID_SOURCE Key | Physical Column Reference |
|---------------|--------------------------|
| BILLING_CATEGORY_CODE | ACCT.ACCOUNT_BILLING_CATEGORY_CODE |
| BILLING_CATEGORY_TYPE_CODE | ACCT.ACCOUNT_BILLING_CATEGORY_TYPE_CODE |
| ACCOUNT_CODE | ACCT.ACCOUNT_CODE |

**NET Configuration:**

| ID_SOURCE Key | Physical Column Reference | Notes |
|---------------|--------------------------|-------|
| BILLING_CATEGORY_CODE | ACCT.ACCOUNT_BILLING_CATEGORY_CODE | |
| BILLING_CATEGORY_TYPE_CODE | ACCT.ACCOUNT_BILLING_CATEGORY_TYPE_CODE | |
| ACCOUNT_CODE | ACCT.ACCOUNT_CODE | |
| MINOR_CODE | ACCT.ACCOUNT_MINOR_CODE | NET-only |
| BILLING_CATEGRY_CODE | ACCT.ACCOUNT_BILLING_CATEGORY_CODE | Typo alias |
| BILLING_CATEGORY_TYPE | ACCT.ACCOUNT_BILLING_CATEGORY_TYPE_CODE | Typo alias |

---

## 12. dbt YAML Configuration

The proposed dbt YAML configuration schema for each hierarchy project. These variables are passed to dbt macros via the `vars:` key in `dbt_project.yml` or as `--vars` on the command line.

### 12.1 GROSS LOS Configuration

```yaml
project_name: UPSTREAM_GROSS
report_type: GROSS
hierarchy_table: TBL_0_GROSS_LOS_REPORT_HIERARCHY_
mapping_table: TBL_0_GROSS_LOS_REPORT_HIERARCHY_MAPPING
account_segment: GROSS
measure_prefix: GROSS
has_sign_change: false
has_exclusions: false
has_group_filter_precedence: true

dynamic_column_map:
  BILLING_CATEGORY_CODE: ACCT.ACCOUNT_BILLING_CATEGORY_CODE
  BILLING_CATEGORY_TYPE_CODE: ACCT.ACCOUNT_BILLING_CATEGORY_TYPE_CODE
  ACCOUNT_CODE: ACCT.ACCOUNT_CODE

join_patterns:
  - name: account
    join_keys: [LOS_ACCOUNT_ID_FILTER]
    fact_keys: [FK_ACCOUNT_KEY]
    filter: null
  - name: deduct_product
    join_keys: [LOS_DEDUCT_CODE_FILTER, LOS_PRODUCT_CODE_FILTER]
    fact_keys: [FK_DEDUCT_KEY, FK_PRODUCT_KEY]
    filter: null
  - name: royalty
    join_keys: [LOS_PRODUCT_CODE_FILTER]
    fact_keys: [FK_PRODUCT_KEY]
    filter: "ROYALTY_FILTER = 'Y'"
```

### 12.2 NET LOS Configuration

```yaml
project_name: UPSTREAM_NET
report_type: NET
hierarchy_table: TBL_0_NET_LOS_REPORT_HIERARCHY
mapping_table: TBL_0_NET_LOS_REPORT_HIERARCHY_MAPPING
account_segment: NET
measure_prefix: NET
has_sign_change: true
has_exclusions: true
has_group_filter_precedence: false

dynamic_column_map:
  BILLING_CATEGORY_CODE: ACCT.ACCOUNT_BILLING_CATEGORY_CODE
  BILLING_CATEGORY_TYPE_CODE: ACCT.ACCOUNT_BILLING_CATEGORY_TYPE_CODE
  ACCOUNT_CODE: ACCT.ACCOUNT_CODE
  MINOR_CODE: ACCT.ACCOUNT_MINOR_CODE
  BILLING_CATEGRY_CODE: ACCT.ACCOUNT_BILLING_CATEGORY_CODE
  BILLING_CATEGORY_TYPE: ACCT.ACCOUNT_BILLING_CATEGORY_TYPE_CODE

join_patterns:
  - name: account
    join_keys: [LOS_ACCOUNT_ID_FILTER]
    fact_keys: [FK_ACCOUNT_KEY]
    filter: null
  - name: account_product
    join_keys: [LOS_ACCOUNT_ID_FILTER, LOS_PRODUCT_CODE_FILTER]
    fact_keys: [FK_ACCOUNT_KEY, FK_PRODUCT_KEY]
    filter: null
```

---

## 13. Implementation Roadmap

| Phase | Deliverables | Dependencies | Est. Effort |
|-------|-------------|--------------|-------------|
| Phase 1: Foundation | dbt project scaffold, YAML config schema, base macros for VW_1 and DT_2 | None | 2–3 weeks |
| Phase 2: Core Pipeline | DT_3A and DT_3 macros, formula precedence engine, UNION ALL branch generator | Phase 1 | 3–4 weeks |
| Phase 3: Validation | Diff-test generated DDL vs hand-written scripts, data reconciliation, edge case testing | Phase 2 | 1–2 weeks |
| Phase 4: NET Deployment | Apply template to NET LOS, validate output, fix data quality issues | Phase 3 | 1–2 weeks |
| Phase 5: AI Agents | Cortex COMPLETE() hierarchy discovery, auto-config generation, monitoring dashboard | Phase 4 | 4–6 weeks |

---

## 14. Next Steps

### 14.1 Immediate Actions

1. Resolve FILTER_GROUP_2 mismatch issue in GROSS mapping data — determine if concatenation logic should be standardized or if mismatches are acceptable (30 affected rows).
2. Correct NET ID_SOURCE typos (3 rows) — either fix source data or add alias normalization to DYNAMIC_COLUMN_MAP.
3. Investigate GROSS duplicate Cash Flow rows (keys 217/218) — confirm one should be removed.
4. Begin dbt macro development starting with VW_1 (Translation View) and DT_2 (Granularity Table).

### 14.2 Validation Milestones

1. Generate GROSS VW_1 DDL from template and diff against `01_VW_1_UPSTREAM_GROSS_REPORT_HIERARCHY_TRANSLATED.sql`.
2. Generate full GROSS pipeline (all 4 objects) and verify row counts match production.
3. Generate NET pipeline from config-only changes (no macro edits) and validate output.
4. Identify a third hierarchy project to confirm template portability beyond LOS.

### 14.3 Open Questions

1. Should the template handle the GROUP_FILTER_PRECEDENCE multi-round pattern as a configurable loop, or hardcode GROSS-specific logic?
2. How should the extension hierarchy join ("Operation Team Financial Group") be parameterized for projects that don't have it?
3. What is the preferred deployment model: one dbt project per hierarchy, or a multi-project monorepo?
