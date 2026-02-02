# Analysis and Recommendations for FP&A SQL Queries

This document provides a detailed analysis of the `FP&A Queries.sql` file, highlights commonalities and differences, and proposes a new hierarchy-based table design to simplify the queries and improve maintainability.

---

## 1. Analysis of Query Structure

The SQL file contains several large, complex queries designed for financial reporting and analysis. A deep analysis reveals a consistent underlying pattern:

*   **Central Fact Table:** All queries are built around a single, central fact table: `edw.financial.fact_financial_details`. This table contains the lowest-level transactional data, including amounts, dates, and foreign keys.

*   **Star Schema:** The queries use a classic star schema approach, joining the `fact_financial_details` table to several dimension tables:
    *   `dim_account`
    *   `dim_corp`
    *   `dim_cost_center`
    *   `dim_business_associate` / `dim_counter_party`

*   **Embedded Business Logic:** The most critical observation is that the vast majority of business logic is **hardcoded directly into the queries** using massive `CASE` statements within Common Table Expressions (CTEs) or subqueries.

    For example, the subquery on `dim_account` contains a `CASE` statement with over **200 `WHEN` conditions** to map low-level `account_code` values to high-level financial reporting lines like 'Capex', 'Oil Sales', 'Lease Operating Expenses', etc. This is repeated in almost every query.

    This pattern indicates that the business's Chart of Accounts hierarchy, corporate structure, and billing categorizations are not stored structurally in the database; they exist only as logic within these specific queries.

---

## 2. Commonalities and Differences

### Commonalities
The queries share a remarkably consistent foundation, which is a strong indicator that a new design can be highly effective.

| Common Element | Description |
| :--- | :--- |
| **Fact Table** | All queries use `edw.financial.fact_financial_details` as their primary source of data. |
| **Dimensions** | All queries join to `dim_account`, `dim_corp`, and `dim_cost_center`. |
| **Redundant Subqueries** | The CTEs or subqueries that build the `accts`, `corps`, and `billcats` tables are **copied verbatim** across multiple queries in the file. This is a major source of redundancy and risk. |
| **Reporting Logic** | The method for creating financial groupings (the giant `CASE` statements) is identical everywhere it appears. |

### Differences
The differences between the queries lie in how they slice and dice the final, processed data.

| Difference | Description |
| :--- | :--- |
| **Filtering (`WHERE` clause)** | Each query filters for a different business context. For example: `accts.gl = 'General & Administrative'`, `segment = 'Upstream'`, or `Corps.corp_code = 600`. |
| **Grouping (`GROUP BY`)** | The final aggregation level changes. One query groups by `corp_name`, another by `acctdate`, `svcdate`, and `cost_center_state`. |
| **Selection (`SELECT` list)**| Each query selects a different combination of columns and calculates slightly different final metrics (e.g., `Net_AU_Val`, `Impl_price`). |

---

## 3. Hierarchy Table Design Recommendation

The core recommendation is to **externalize the business logic from the `CASE` statements into dedicated hierarchy and mapping tables**. This moves the logic from the *query* layer to the *data* layer, which is more flexible, maintainable, and scalable.

I propose creating the following new tables, which can be managed by the **DataBridge Librarian Hierarchy Builder**:

### 3.1. `dim_gl_hierarchy`
This table will replace the giant `CASE` statement that maps `account_code` to financial reporting lines.

| Column | Example | Purpose |
| :--- | :--- | :--- |
| `account_code` (PK) | '501-100' | The unique General Ledger account code. |
| `hierarchy_l1` | 'Revenue' | The highest level of the P&L. |
| `hierarchy_l2` | 'Sales Revenue' | The second level. |
| `hierarchy_l3` | 'Oil Sales' | The most granular reporting line from the `CASE` statement. |
| `is_balance_sheet` | FALSE | Flag for BS vs. P&L. |
| `is_capex` | FALSE | Specific flag for 'Capex' logic. |

### 3.2. `dim_corp_hierarchy`
This table will replace the `CASE` statement that maps `corp_code` to Funds and Segments.

| Column | Example | Purpose |
| :--- | :--- | :--- |
| `corp_code` (PK) | 550 | The unique corporate entity code. |
| `fund` | 'AU' | The Fund associated with the corp code. |
| `segment` | 'Upstream' | The business Segment. |
| `au_stake` | 1.0 | The calculated AU Stake value. |
| `a3_stake` | 0.0 | The calculated A3 Stake value. |

### 3.3. `dim_billing_hierarchy`
This table will replace the complex `CASE` statement used to derive the `los_map`.

| Column | Example | Purpose |
| :--- | :--- | :--- |
| `billing_category_code` (PK) | 'LOE110' | The billing category code. |
| `los_map` | 'OHD' | The resulting Lease Operating Statement mapping. |

---

## 4. How the New Design Simplifies Queries

By joining to these new hierarchy tables, the queries become dramatically simpler, more readable, and more efficient. The database can use indexed joins on the new tables instead of repeatedly processing huge `CASE` statements.

### Before: The Old Query Structure (Simplified Snippet)

```sql
SELECT
    -- ... other columns
    CASE
        WHEN accts.account_code ILIKE '501%' THEN 'Oil Sales'
        WHEN accts.account_code ILIKE '502%' THEN 'Gas Sales'
        -- ... 200 more lines ...
        ELSE 'Other'
    END as gl_reporting_line,
    SUM(entries.amount_gl) as total_amount
FROM
    edw.financial.fact_financial_details AS entries
LEFT JOIN 
    edw.financial.dim_account AS accts ON accts.account_hid = entries.account_hid
-- ... other joins
GROUP BY
    -- complex grouping
```

### After: The New, Simplified Query

With the new design, the subqueries with `CASE` statements are **completely eliminated**.

```sql
SELECT
    -- ... other columns
    -- The hierarchy is now just a column we can select and group by
    gl_h.hierarchy_l3 as gl_reporting_line, 
    corp_h.fund,
    corp_h.segment,
    SUM(entries.amount_gl) as total_amount
FROM
    edw.financial.fact_financial_details AS entries
-- Simply join to the new, clean hierarchy tables
LEFT JOIN 
    dim_gl_hierarchy AS gl_h ON gl_h.account_code = entries.account_code -- Or via a key
LEFT JOIN 
    dim_corp_hierarchy AS corp_h ON corp_h.corp_code = entries.corp_code -- Or via a key
-- ... other joins
WHERE
    -- Filtering becomes much more readable
    corp_h.fund IN ('AU', 'A3') 
    AND gl_h.hierarchy_l3 = 'General & Administrative'
GROUP BY
    gl_h.hierarchy_l3,
    corp_h.fund,
    corp_h.segment
```

### Key Benefits of the New Design

1.  **Maintainability:** If a new account `501-200` needs to be added to 'Oil Sales', you simply **insert one row** into the `dim_gl_hierarchy` table. You do **not** have to find and edit every single query where the logic is hardcoded.
2.  **Consistency:** All queries are guaranteed to use the exact same business logic because the logic lives in the data, not in the query.
3.  **Performance:** The database query optimizer can create much better execution plans using joins on indexed keys instead of scanning massive `CASE` statements.
4.  **Empowerment:** Business users and financial analysts can manage their own hierarchies using a tool like **DataBridge Librarian** without needing a SQL developer to update complex queries. This is the core value proposition of the DataBridge platform.