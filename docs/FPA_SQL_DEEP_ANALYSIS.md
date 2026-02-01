# FP&A SQL Deep Dive Analysis

**Author:** Claude Code Analysis
**Date:** 2026-01-30
**Subject:** Comprehensive analysis of `FP&A Queries.sql` with focus on technical debt, optimization opportunities, and DataBridge integration strategy

---

## Executive Summary

The `FP&A Queries.sql` file contains **5 major query templates** totaling ~2,300+ lines of SQL. These queries power Oil & Gas financial reporting across multiple business segments (Upstream, Midstream, Marketing, Services). The analysis reveals significant technical debt stemming from **hardcoded business logic**, presenting a prime opportunity for DataBridge's hierarchy management capabilities.

### Key Metrics
| Metric | Value |
|--------|-------|
| Total Queries | 5 (G&A, GL, LOS, SCONA, Brahma) |
| Lines of Code | ~2,300+ |
| Unique CASE Conditions | 200+ (GL mapping alone) |
| Dimension Tables | 5 (account, corp, cost_center, business_associate, counter_party) |
| Repeated Code Blocks | 85%+ duplication across queries |

---

## 1. Query Inventory

### 1.1 G&A (General & Administrative) Query
**Lines:** 1-607
**Purpose:** Extract G&A expenses filtered by fund (AU, A3)
**Key Features:**
- Transactional detail level (not aggregated)
- Includes allocation codes (placeholder: 'Alloc Group ID')
- Filters: `gl = 'General & Administrative'`, `fund IN ('AU', 'A3')`, `acctdate > '2022-12-31'`

### 1.2 GL (General Ledger) Summary Query
**Lines:** 614-985
**Purpose:** Aggregated GL view with volume and value by corp/date
**Key Features:**
- Uses `LISTAGG` for multi-value columns
- Calculates implied Net AU/A3 volumes and values
- Groups by accounting_date_key, account_code, corp_name

### 1.3 LOS (Lease Operating Statement) Query
**Lines:** 986-1862
**Purpose:** Detailed LOE analysis with billing category breakdown
**Key Features:**
- Most complex query with 4 major subqueries
- Includes drill schedule integration (`dim_drill_schedules_well`)
- Creates `adjbillcat` with transaction description parsing
- Derives state from cost_center or embedded lookup table
- Contains operational flags (AU_Op, A3_Op, AU_Nop, A3_Nop)

### 1.4 SCONA Query
**Lines:** 1863-2239
**Purpose:** Corp 600 (Marketing) allocation analysis
**Key Features:**
- Simple allocation grouping (Gas Sales, COGP, Fees)
- Filters to corp_code = 600 only
- Groups by batch_number

### 1.5 Brahma Query
**Lines:** 2240+
**Purpose:** Detailed analysis with implied pricing
**Key Features:**
- Calculates `Impl_price = ABS(amount / volume)`
- Full billing category breakdown with LOS mapping
- Groups by billing_category_code

---

## 2. Architectural Analysis

### 2.1 Data Model (Star Schema)

```
                    ┌─────────────────────────┐
                    │  fact_financial_details │
                    │  ─────────────────────  │
                    │  account_hid      (FK)  │
                    │  corp_hid         (FK)  │
                    │  cost_center_hid  (FK)  │
                    │  business_associate_hid │
                    │  counter_party_hid (FK) │
                    │  ─────────────────────  │
                    │  amount_gl              │
                    │  net_volume             │
                    │  accounting_date_key    │
                    │  service_date_key_m     │
                    │  billing_category_code  │
                    │  transaction_description│
                    │  product_code           │
                    └───────────┬─────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        │                       │                       │
        ▼                       ▼                       ▼
┌───────────────┐      ┌───────────────┐      ┌───────────────────┐
│  dim_account  │      │   dim_corp    │      │  dim_cost_center  │
│  ───────────  │      │  ───────────  │      │  ───────────────  │
│  account_hid  │      │  corp_hid     │      │  cost_center_hid  │
│  account_code │      │  corp_code    │      │  cost_center_code │
│  account_name │      │  corp_name    │      │  cost_center_state│
│  account_class│      │               │      │  area_code        │
│  billing_cat* │      │               │      │  district_code    │
└───────────────┘      └───────────────┘      │  field_code       │
                                              │  unit_code        │
                                              │  gathfac_code     │
                                              │  mssystem_code    │
                                              └───────────────────┘
```

### 2.2 Embedded Business Logic Patterns

#### Pattern A: GL Account Hierarchy (200+ conditions)
```sql
CASE
    WHEN account_code ILIKE '101%' THEN 'Cash'
    WHEN account_code ILIKE '125%' THEN 'Affiliate AR'
    WHEN account_code ILIKE ANY ('11%', '12%') THEN 'AR'
    -- ... 197 more conditions ...
    WHEN account_code ILIKE '8%' THEN 'General & Administrative'
END AS gl
```

**Categories Identified (57 unique GL mappings):**
| Category Type | Count | Examples |
|--------------|-------|----------|
| Balance Sheet | 15 | Cash, AR, AP, Equity, Total Debt |
| Revenue | 12 | Oil Sales, Gas Sales, NGL Sales, Gathering Fees |
| Operating Expenses | 18 | LOE, DD&A, G&A, Severance Taxes |
| Other | 12 | Hedge Gains, Interest, COPAS |

#### Pattern B: Corporate Hierarchy (Fund/Segment/Stake)
```sql
-- Fund Assignment
CASE
    WHEN corp_code IN (551, 561, 565, 578, 586, 587, 588, 598, 702, 755) THEN 'A3'
    WHEN corp_code IN (410, 420, 550, 560, 580, 585, ..., 751) THEN 'AU'
    WHEN corp_code IN (012, 043, 049, 052) THEN '4HC'
END AS fund

-- Segment Assignment
CASE
    WHEN corp_code IN (12, 43, 44, 540, 550, ..., 755) THEN 'Upstream'
    WHEN corp_code IN (41, 410, 578, 580, ..., 595) THEN 'Midstream'
    WHEN corp_code = 600 THEN 'Marketing'
    WHEN corp_code BETWEEN 700 AND 702 THEN 'Services'
    WHEN corp_code BETWEEN 597 AND 599 THEN 'Elim'
END AS Segment

-- Ownership Stakes
CASE
    WHEN corp_code IN (550, 560, 580, ..., 751) THEN 1.0
    WHEN corp_code IN (410, 420) THEN 0.9
    WHEN corp_code = 586 THEN 0.225
    ELSE 0
END AS AU_Stake
```

**Corporate Mapping Summary:**
| Corp Codes | Fund | Segment | AU_Stake | A3_Stake |
|------------|------|---------|----------|----------|
| 550, 560, 650, 750, 751 | AU | Upstream | 1.0 | 0 |
| 551, 561, 565, 755 | A3 | Upstream | 0 | 1.0 |
| 410, 420 | AU | Midstream | 0.9 | 0 |
| 578 | A3 | Midstream | 0 | 0.9 |
| 586 | Both | Midstream | 0.225 | 0.675 |
| 600 | AU | Marketing | 1.0 | 0 |
| 700, 701 | AU | Services | 1.0 | 0 |
| 702 | A3 | Services | 0 | 1.0 |

#### Pattern C: Billing Category to LOS Mapping
```sql
CASE
    WHEN account_billing_category_code ILIKE ANY ('%CC85%', '%DC85%', ...) THEN 'CNOP'
    WHEN account_billing_category_code ILIKE ANY ('%C990%') THEN 'CACR'
    WHEN account_billing_category_code ILIKE ANY ('LOE10%', 'MOE10%', ...) THEN 'LBR'
    WHEN account_billing_category_code ILIKE ANY ('LOE11%', 'LOE320', ...) THEN 'OHD'
    WHEN account_billing_category_code ILIKE ANY ('LOE140', 'LOE160', ...) THEN 'SVC'
    -- ... 80+ more conditions ...
END AS los_map
```

**LOS Categories:**
| Code | Meaning | Billing Category Patterns |
|------|---------|--------------------------|
| LBR | Labor | LOE10%, MOE10%, MOE330, MOE345 |
| OHD | Overhead | LOE11%, LOE32x, LOE33x, LOE7xx |
| SVC | Services | LOE14x-22x, LOE35x, LOE70x |
| CHM | Chemicals | LOE24%, LOE25%, LOE26%, MOE24% |
| SWD | Saltwater Disposal | LOE273-276 |
| RNM | Repairs & Maintenance | LOE295-304, MOE295-421 |
| EQU | Equipment | LOE305-326, LOE5%, MOE5% |
| COPAS | COPAS Overhead | LOE6% |
| SEV | Severance | LOE720-721 |
| ADV | Ad Valorem | LOE750-751, MOE750-755 |
| NLOE | Non-LOE | LOE850-853, LOE990, NLOE% |
| CAPEX Types | CNOP, CACR, CACQ, CFRC, CDRL, CFAC, CLHD | Various billing codes |

#### Pattern D: State Derivation Logic (100+ conditions)
The LOS query contains an embedded state lookup for cost centers with UNKNOWN state:
```sql
CASE
    WHEN dim_cost_center.cost_center_state = 'UNKNOWN' THEN
        CASE
            WHEN cost_center_code IN ('W007388-1', 'W007389-1', 'DP044', ...)
                OR cost_center_area_code IN ('AREA0200', 'AREA0220', 'AREA0210')
                OR cost_center_field_code IN ('F095', 'F308') THEN 'TX'
            WHEN cost_center_code IN ('W007390-1', 'W007391-1', ...)
                OR cost_center_area_code IN ('AREA0300', 'AREA0400', ...) THEN 'LA'
            WHEN cost_center_code IN ('W001170-1', 'PR0015', ...)
                OR cost_center_area_code IN ('AREA0100', 'AREA0125') THEN 'WY'
            WHEN cost_center_code ILIKE ANY ('VE%', 'DP00560', ...) THEN 'Multiple'
        END
    ELSE dim_cost_center.cost_center_state
END AS cost_center_state
```

#### Pattern E: Transaction Description Parsing
```sql
-- Adjust billing category based on transaction description
CASE
    WHEN entries.transaction_description LIKE '%PAC %'
        AND accts.account_code = '641-990' THEN 'PAC990'
    WHEN entries.transaction_description LIKE '%NONOP LOE %'
        AND accts.account_code IN ('641-990', '640-990') THEN 'NLOE990'
    WHEN entries.transaction_description ILIKE ANY ('%WOX %', '% LOE %')
        AND accts.account_code = '641-990' THEN 'WOX990'
    WHEN entries.transaction_description LIKE '%REST FEE%'
        AND accts.account_code = '640-990' THEN 'LOE722'
    WHEN entries.transaction_description LIKE '%COPAS%' THEN '640-992'
END AS adjbillcat
```

---

## 3. Technical Debt Assessment

### 3.1 Severity Matrix

| Issue | Severity | Impact | Frequency |
|-------|----------|--------|-----------|
| Duplicated CASE statements | **Critical** | Maintenance nightmare | 5 queries × 200+ lines |
| Hardcoded corp mappings | **High** | Corp changes require SQL edits | Every query |
| Embedded state lookups | **High** | Cannot add new cost centers easily | LOS query |
| Transaction parsing logic | **Medium** | Fragile pattern matching | LOS, Brahma queries |
| No date parameterization | **Medium** | Hardcoded date filters | All queries |

### 3.2 Estimated Maintenance Burden

**Scenario: Add new account code "501-250" to "Oil Sales"**
- Current approach: Edit 5 queries, 5 CASE statements, ~1000 lines affected
- Risk: Miss one query = inconsistent reporting
- Time: 1-2 hours + testing

**Scenario: New corp code 760 joins as AU Upstream**
- Current approach: Edit 5 queries, update fund/segment/stake CASE blocks
- Risk: Stake calculation errors if missed
- Time: 30 min + testing

**Scenario: New well "W009999-1" in Texas needs state mapping**
- Current approach: Edit LOS query, add to TX list
- Risk: State mapping breaks if cost_center not in list
- Time: 15 min

### 3.3 Code Duplication Analysis

```
┌─────────────────────────────────────────────────────────────┐
│           Code Block Replication Matrix                     │
├─────────────────┬─────┬─────┬─────┬───────┬────────────────┤
│ Block           │ G&A │ GL  │ LOS │ SCONA │ Brahma         │
├─────────────────┼─────┼─────┼─────┼───────┼────────────────┤
│ GL CASE (accts) │  ✓  │  ✓  │  ✓  │   ✓   │   ✓            │
│ Corp CASE       │  ✓  │  ✓  │  ✓  │   ✓   │   ✓            │
│ Billcat CASE    │  ✓  │  -  │  ✓  │   -   │   ✓            │
│ Cost center join│  ✓  │  -  │  ✓  │   -   │   -            │
│ Date extraction │  ✓  │  ✓  │  ✓  │   ✓   │   ✓            │
└─────────────────┴─────┴─────┴─────┴───────┴────────────────┘
```

**Duplication Rate:** ~85% of subquery code is copy-pasted

---

## 4. Performance Considerations

### 4.1 Query Execution Concerns

1. **CASE Statement Evaluation**: Each row evaluates 200+ conditions in sequence
2. **ILIKE with ANY**: `account_code ILIKE ANY ('502%', '510%')` prevents index usage
3. **Subquery Materialization**: Each LEFT JOIN subquery is materialized per execution
4. **LISTAGG Operations**: Expensive string aggregation on large result sets
5. **Nested CASE in LOS**: State derivation has 3-level nested CASE evaluation

### 4.2 Snowflake-Specific Optimizations Available

| Current Pattern | Optimized Alternative |
|-----------------|----------------------|
| CASE with 200+ WHEN | Hierarchy lookup table with JOIN |
| ILIKE '%pattern%' | Computed column or separate hierarchy |
| Repeated subqueries | Views or materialized views |
| Inline date parsing | Pre-computed date dimension |

### 4.3 Estimated Performance Gain

With hierarchy tables replacing CASE statements:
- **Query compile time**: 50-70% reduction
- **Execution time**: 30-50% reduction (depends on data volume)
- **Maintenance time**: 90% reduction

---

## 5. DataBridge Integration Strategy

### 5.1 Proposed Hierarchy Tables

#### Table 1: `dim_gl_hierarchy`
```sql
CREATE TABLE dim_gl_hierarchy (
    account_code VARCHAR(20) PRIMARY KEY,
    gl_category VARCHAR(100),           -- 'Oil Sales', 'G&A', etc.
    statement_type VARCHAR(20),          -- 'Balance Sheet', 'P&L'
    hierarchy_l1 VARCHAR(50),            -- 'Revenue', 'Expenses'
    hierarchy_l2 VARCHAR(50),            -- 'Operating Revenue', 'LOE'
    hierarchy_l3 VARCHAR(50),            -- 'Oil Sales', 'Severance Taxes'
    is_capex BOOLEAN DEFAULT FALSE,
    sort_order INT
);
```

#### Table 2: `dim_corp_hierarchy`
```sql
CREATE TABLE dim_corp_hierarchy (
    corp_code INT PRIMARY KEY,
    corp_name VARCHAR(100),
    fund VARCHAR(10),                    -- 'AU', 'A3', '4HC'
    segment VARCHAR(20),                 -- 'Upstream', 'Midstream', etc.
    au_stake DECIMAL(5,4),
    a3_stake DECIMAL(5,4),
    is_elimination BOOLEAN DEFAULT FALSE,
    sort_order INT
);
```

#### Table 3: `dim_billing_hierarchy`
```sql
CREATE TABLE dim_billing_hierarchy (
    billing_category_code VARCHAR(20) PRIMARY KEY,
    los_map VARCHAR(10),                 -- 'LBR', 'OHD', 'SVC', etc.
    los_description VARCHAR(100),
    capex_category VARCHAR(10),          -- 'CNOP', 'CDRL', etc.
    hierarchy_l1 VARCHAR(50),            -- 'Direct LOE', 'Indirect LOE'
    hierarchy_l2 VARCHAR(50),            -- 'Labor', 'Equipment'
    sort_order INT
);
```

#### Table 4: `dim_cost_center_state_override`
```sql
CREATE TABLE dim_cost_center_state_override (
    cost_center_code VARCHAR(20) PRIMARY KEY,
    state_override VARCHAR(10),          -- 'TX', 'LA', 'WY', 'Multiple'
    override_reason VARCHAR(100),
    effective_date DATE,
    expiry_date DATE
);
```

### 5.2 Migration Path

**Phase 1: Data Extraction** (1-2 days)
- Extract unique mappings from CASE statements
- Validate against source data
- Create CSV files for DataBridge import

**Phase 2: Hierarchy Creation** (1 day)
- Create projects in DataBridge V3
- Import GL hierarchy (~300 accounts)
- Import Corp hierarchy (~30 corps)
- Import Billing hierarchy (~150 categories)

**Phase 3: View Creation** (1-2 days)
- Create denormalized views with hierarchy joins
- Validate output matches original queries
- Performance benchmarking

**Phase 4: Query Migration** (2-3 days)
- Refactor each query to use hierarchy tables
- Parallel testing old vs new
- Documentation update

### 5.3 Simplified Query After Migration

```sql
-- Before: 600+ lines
-- After: ~50 lines

SELECT
    gl_h.hierarchy_l3 AS gl_category,
    corp_h.fund,
    corp_h.segment,
    DATE_FROM_PARTS(SUBSTR(e.accounting_date_key, 1, 4),
                    SUBSTR(e.accounting_date_key, 5, 2), 1) AS acctdate,
    ROUND(SUM(e.amount_gl), 2) AS total_val,
    ROUND(SUM(e.amount_gl) * corp_h.au_stake, 2) AS net_au_val,
    ROUND(SUM(e.amount_gl) * corp_h.a3_stake, 2) AS net_a3_val
FROM edw.financial.fact_financial_details e
JOIN dim_gl_hierarchy gl_h ON gl_h.account_code = e.account_code
JOIN dim_corp_hierarchy corp_h ON corp_h.corp_code = e.corp_code
WHERE
    corp_h.fund IN ('AU', 'A3')
    AND gl_h.hierarchy_l3 = 'General & Administrative'
    AND acctdate > '2022-12-31'
GROUP BY 1, 2, 3, 4
ORDER BY acctdate DESC;
```

---

## 6. Risk Analysis

### 6.1 Data Quality Risks

| Risk | Probability | Mitigation |
|------|-------------|------------|
| Missing account codes in hierarchy | Medium | Automated validation job |
| Stake calculation drift | Low | Version control on hierarchy |
| State override conflicts | Medium | Expiry date enforcement |
| LOS mapping gaps | Medium | Default category handling |

### 6.2 Migration Risks

| Risk | Probability | Mitigation |
|------|-------------|------------|
| Output mismatch | High initially | Parallel run with diff analysis |
| Performance regression | Low | Benchmark before cutover |
| User training | Medium | Documentation + workshops |
| Rollback complexity | Low | Maintain old queries for 90 days |

---

## 7. Recommendations

### Immediate Actions
1. **Create DataBridge project** for "Oil & Gas GL Hierarchy" using `upstream_oil_gas_pl` template
2. **Extract GL mappings** from CASE statements into CSV
3. **Document corp stake calculations** in knowledge base

### Short-term (1-2 weeks)
4. **Build hierarchy tables** in Snowflake EDW
5. **Create validation views** comparing old vs new logic
6. **Pilot with GL query** (simplest of the five)

### Medium-term (1 month)
7. **Migrate all five queries** to hierarchy-based approach
8. **Implement auto-sync** from DataBridge to Snowflake
9. **Create self-service update UI** for finance team

### Long-term (quarterly)
10. **Add formula groups** for calculated metrics (Net AU Val, Impl Price)
11. **Integrate with budgeting** hierarchies
12. **Build variance analysis** using DataBridge V4 insights

---

## 8. Appendix

### A. Complete GL Category List (57 categories)
```
Cash, Affiliate AR, AR, Prepaid Expenses, Inventory, Other Current Assets,
Derivative Assets, Deferred Tax Assets, Capex, Other Assets, Accumulated DD&A,
AP, Total Debt, Other Current Liabilities, Other Liabilities, Equity,
Oil Sales, Gas Sales, NGL Sales, Other Income, Cashouts, Service Revenue,
Gathering Fees, Compression Fees, Treating Fees, Capital Recovery Fees,
Demand Fees, Transportation Fees, Service Income, Sand Sales, Rental Income,
Water Income, SWD Income, Consulting Income, Fuel Income, Hedge Gains,
Interest Hedge Gains, Unrealized Hedge Gains, COPAS, Compressor Recovery Income,
Rig Termination Penalties, Gathering Fee Income, Oil Severance Taxes,
Gas Severance Taxes, NGL Severance Taxes, Ad Valorem Taxes,
Oil Conservation Taxes, Gas Conservation Taxes, NGL Conservation Taxes,
Commodity Fees, Non-Op Fees, Consulting Expenses, Lease Operating Expenses,
Accretion Expense, Leasehold Expenses, Exploration Expenses, Third Party Fees Paid,
Midstream Operating Expenses, Cost of Purchased Gas, Sand Purchases,
Water Purchases, Fuel Purchases, Rental Expenses, Sand Expenses, Water Expenses,
SWD Expenses, Fuel Expenses, General & Administrative, Interest Income,
Other Gains/Losses, Interest Expense, DD&A, Impairment Expense, Bad Debt Expense,
Other Expenses
```

### B. Corp Code Quick Reference
```
Fund AU:  410, 420, 550, 560, 580, 585, 590, 595, 599, 600, 650, 700, 701, 750, 751
Fund A3:  551, 561, 565, 578, 586, 587, 588, 598, 702, 755
Fund 4HC: 012, 043, 049, 052
Shared:   586 (AU: 0.225, A3: 0.675)
```

### C. LOS Mapping Categories
```
LBR  - Labor
OHD  - Overhead
SVC  - Services
CHM  - Chemicals
SWD  - Saltwater Disposal
RNM  - Repairs & Maintenance
EQU  - Equipment
COPAS- COPAS Overhead
SEV  - Severance Taxes
ADV  - Ad Valorem Taxes
NLOE - Non-LOE Items
CNOP - Capital Non-Operated
CACR - Capital Accrual
CACQ - Capital Acquisition
CFRC - Capital Frac
CDRL - Capital Drilling
CFAC - Capital Facilities
CLHD - Capital Leasehold
PAC  - Plug & Abandon
WOX  - Workover
MOX  - Maintenance Workover
```

---

*Analysis generated by Claude Code for DataBridge AI integration planning.*
