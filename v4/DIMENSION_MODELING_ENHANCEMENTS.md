# Dimension Modeling Enhancements for V3 & V4

## Executive Summary

This document outlines advanced dimension modeling techniques to enhance the DataBridge platform's ability to handle complex FP&A scenarios, historical tracking, and multi-system reconciliation.

---

## 1. Slowly Changing Dimensions (SCD)

### Problem
Cost centers, accounts, and organizational structures change over time. Without historical tracking, analysis becomes inconsistent when comparing periods before and after restructuring.

### Enhancement: SCD Type 2 Support

**V3 Changes (Hierarchy Builder):**
```sql
-- Add versioning columns to hierarchy tables
ALTER TABLE hierarchies ADD COLUMN effective_from DATE NOT NULL DEFAULT CURRENT_DATE;
ALTER TABLE hierarchies ADD COLUMN effective_to DATE DEFAULT '9999-12-31';
ALTER TABLE hierarchies ADD COLUMN is_current BOOLEAN DEFAULT TRUE;
ALTER TABLE hierarchies ADD COLUMN version_number INTEGER DEFAULT 1;
```

**V4 Changes (Analytics Engine):**
```sql
-- Dimension tables with SCD Type 2
CREATE TABLE dimensions.dim_cost_center_scd2 (
    cost_center_sk SERIAL PRIMARY KEY,           -- Surrogate key
    cost_center_id VARCHAR(20) NOT NULL,         -- Natural key
    cost_center_name VARCHAR(100),
    department VARCHAR(50),
    division VARCHAR(50),
    region VARCHAR(50),

    -- SCD Type 2 tracking
    effective_from DATE NOT NULL,
    effective_to DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    version_number INTEGER DEFAULT 1,

    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(50)
);

-- Index for point-in-time lookups
CREATE INDEX idx_cc_pit ON dimensions.dim_cost_center_scd2
    (cost_center_id, effective_from, effective_to);
```

**MCP Tools to Add:**
| Tool | Purpose |
|------|---------|
| `create_dimension_version` | Create new version when attributes change |
| `get_dimension_as_of` | Retrieve dimension state at specific date |
| `compare_dimension_versions` | Show changes between versions |
| `get_dimension_history` | Full audit trail for a dimension member |

### Use Case: Reorg Tracking
```
Q: "Show me Q1 2024 expenses by cost center using the org structure as of March 2024"
A: Uses point-in-time lookup to join facts with dimensions effective during Q1
```

---

## 2. Conformed Dimensions

### Problem
Different source systems use different identifiers and hierarchies for the same business concepts. GL uses account numbers, budgeting uses planning accounts, operations uses cost codes.

### Enhancement: Cross-System Dimension Conformance

**V3 Changes:**
```python
# New table: dimension_conformance
class DimensionConformance(Base):
    __tablename__ = 'dimension_conformance'

    id = Column(Integer, primary_key=True)
    dimension_type = Column(String(50))  # account, cost_center, product

    # Canonical (master) identifier
    canonical_id = Column(String(50))
    canonical_name = Column(String(200))

    # Source system mappings
    source_system = Column(String(50))  # GL, EPM, OPS, FIELD
    source_id = Column(String(50))
    source_name = Column(String(200))

    # Confidence and validation
    match_type = Column(String(20))  # exact, fuzzy, manual
    match_confidence = Column(Float)
    validated_by = Column(String(50))
    validated_at = Column(DateTime)
```

**V4 Schema Addition:**
```sql
-- Conformed dimension mapping table
CREATE TABLE metadata.conformed_dimension_map (
    map_id SERIAL PRIMARY KEY,
    dimension_type VARCHAR(50) NOT NULL,

    -- Canonical reference
    canonical_key VARCHAR(50) NOT NULL,
    canonical_name VARCHAR(200),

    -- Source system reference
    source_system VARCHAR(50) NOT NULL,
    source_connection_id VARCHAR(50),
    source_key VARCHAR(50) NOT NULL,
    source_name VARCHAR(200),

    -- Mapping metadata
    mapping_rule VARCHAR(20),  -- exact, prefix, suffix, regex, lookup
    mapping_expression TEXT,
    confidence_score DECIMAL(3,2),

    -- Audit
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_validated TIMESTAMP,

    UNIQUE(dimension_type, source_system, source_key)
);
```

**MCP Tools to Add:**
| Tool | Purpose |
|------|---------|
| `suggest_dimension_mappings` | AI-powered fuzzy matching suggestions |
| `create_conformed_dimension` | Define canonical dimension with mappings |
| `validate_dimension_conformance` | Check all sources resolve to canonical |
| `get_cross_system_view` | Unified view across multiple sources |

---

## 3. Role-Playing Dimensions

### Problem
The same dimension is used multiple ways in a fact table. Date appears as transaction_date, posting_date, effective_date. Entity appears as from_entity and to_entity in intercompany.

### Enhancement: Dimension Role Support

**V4 Schema:**
```sql
-- Role-playing dimension views
CREATE VIEW dimensions.dim_date_transaction AS
SELECT * FROM dimensions.dim_date;

CREATE VIEW dimensions.dim_date_posting AS
SELECT * FROM dimensions.dim_date;

CREATE VIEW dimensions.dim_date_effective AS
SELECT * FROM dimensions.dim_date;

-- Intercompany with role-playing entities
CREATE TABLE facts.fact_intercompany (
    intercompany_id SERIAL PRIMARY KEY,

    -- Role-playing entity dimension
    from_entity_key INTEGER REFERENCES dimensions.dim_entity(entity_key),
    to_entity_key INTEGER REFERENCES dimensions.dim_entity(entity_key),

    -- Role-playing date dimension
    transaction_date_key INTEGER REFERENCES dimensions.dim_date(date_key),
    settlement_date_key INTEGER REFERENCES dimensions.dim_date(date_key),

    -- Measures
    amount DECIMAL(18,2),
    currency_code CHAR(3),
    amount_usd DECIMAL(18,2),

    -- Status
    status VARCHAR(20),
    elimination_status VARCHAR(20)
);
```

**Metadata Registration:**
```sql
-- Register dimension roles
CREATE TABLE metadata.dimension_roles (
    role_id SERIAL PRIMARY KEY,
    dimension_name VARCHAR(50),
    role_name VARCHAR(50),
    role_alias VARCHAR(100),
    description TEXT,

    UNIQUE(dimension_name, role_name)
);

INSERT INTO metadata.dimension_roles VALUES
(1, 'dim_date', 'transaction_date', 'Transaction Date', 'When transaction occurred'),
(2, 'dim_date', 'posting_date', 'GL Posting Date', 'When posted to general ledger'),
(3, 'dim_date', 'effective_date', 'Effective Date', 'Business effective date'),
(4, 'dim_entity', 'from_entity', 'Sending Entity', 'Intercompany sender'),
(5, 'dim_entity', 'to_entity', 'Receiving Entity', 'Intercompany receiver');
```

---

## 4. Hierarchical Dimension Attributes (Ragged Hierarchies)

### Problem
Not all dimension members have the same depth. One product line might have 5 levels, another only 2. One cost center might roll up through 4 levels, another through 6.

### Enhancement: Flexible Hierarchy Depth

**V3 Enhancement:**
```python
# Support for ragged hierarchies
class HierarchyNode(Base):
    __tablename__ = 'hierarchy_nodes'

    id = Column(Integer, primary_key=True)
    hierarchy_id = Column(Integer, ForeignKey('hierarchies.id'))

    # Flexible depth using materialized path
    node_path = Column(String(500))  # /1/5/23/156/
    node_depth = Column(Integer)

    # Parent-child (adjacency list)
    parent_node_id = Column(Integer, ForeignKey('hierarchy_nodes.id'))

    # Attributes at this level
    node_code = Column(String(50))
    node_name = Column(String(200))

    # Level-specific attributes (JSON for flexibility)
    level_attributes = Column(JSON)

    # Rollup behavior
    rollup_method = Column(String(20))  # sum, avg, last, weighted
    is_leaf = Column(Boolean, default=False)
```

**V4 Dimension Pattern:**
```sql
-- Closure table for efficient hierarchy queries
CREATE TABLE dimensions.dim_account_closure (
    ancestor_key INTEGER NOT NULL,
    descendant_key INTEGER NOT NULL,
    depth INTEGER NOT NULL,

    PRIMARY KEY (ancestor_key, descendant_key),
    FOREIGN KEY (ancestor_key) REFERENCES dimensions.dim_account(account_key),
    FOREIGN KEY (descendant_key) REFERENCES dimensions.dim_account(account_key)
);

-- Efficient queries:
-- All descendants of account 100:
SELECT d.* FROM dimensions.dim_account d
JOIN dimensions.dim_account_closure c ON d.account_key = c.descendant_key
WHERE c.ancestor_key = 100;

-- All ancestors of account 456:
SELECT d.* FROM dimensions.dim_account d
JOIN dimensions.dim_account_closure c ON d.account_key = c.ancestor_key
WHERE c.descendant_key = 456
ORDER BY c.depth DESC;
```

---

## 5. Bridge Tables (Many-to-Many Relationships)

### Problem
A single GL transaction might allocate to multiple cost centers. A journal entry might affect multiple accounts. A customer might belong to multiple segments.

### Enhancement: Allocation Bridge Tables

**V4 Schema:**
```sql
-- Bridge table for multi-cost-center allocation
CREATE TABLE dimensions.bridge_gl_cost_center (
    bridge_id SERIAL PRIMARY KEY,
    gl_line_key INTEGER NOT NULL,
    cost_center_key INTEGER NOT NULL,
    allocation_percent DECIMAL(5,4) NOT NULL,
    allocation_method VARCHAR(50),

    FOREIGN KEY (cost_center_key) REFERENCES dimensions.dim_cost_center(cost_center_key),
    CHECK (allocation_percent > 0 AND allocation_percent <= 1)
);

-- Ensure allocations sum to 100%
CREATE OR REPLACE FUNCTION check_allocation_total()
RETURNS TRIGGER AS $$
BEGIN
    IF (SELECT SUM(allocation_percent)
        FROM dimensions.bridge_gl_cost_center
        WHERE gl_line_key = NEW.gl_line_key) > 1.0001 THEN
        RAISE EXCEPTION 'Allocation percentages exceed 100%%';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Customer segment bridge (customer can be in multiple segments)
CREATE TABLE dimensions.bridge_customer_segment (
    customer_key INTEGER NOT NULL,
    segment_key INTEGER NOT NULL,
    segment_weight DECIMAL(5,4) DEFAULT 1.0,
    primary_segment BOOLEAN DEFAULT FALSE,

    PRIMARY KEY (customer_key, segment_key)
);
```

**MCP Tools to Add:**
| Tool | Purpose |
|------|---------|
| `create_allocation_bridge` | Define many-to-many allocation rules |
| `validate_allocation_completeness` | Ensure allocations sum to 100% |
| `analyze_allocation_impact` | Show how allocations affect reporting |

---

## 6. Mini-Dimensions (Rapidly Changing Attributes)

### Problem
Some dimension attributes change frequently (customer credit status, employee salary band). Full SCD Type 2 creates too many rows.

### Enhancement: Mini-Dimension Pattern

**V4 Schema:**
```sql
-- Main customer dimension (slowly changing)
CREATE TABLE dimensions.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    customer_name VARCHAR(200),
    industry VARCHAR(50),
    region VARCHAR(50),
    -- Stable attributes only
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Mini-dimension for rapidly changing attributes
CREATE TABLE dimensions.dim_customer_status (
    status_key SERIAL PRIMARY KEY,
    credit_rating CHAR(1),      -- A, B, C, D, F
    payment_terms VARCHAR(20),   -- NET30, NET60, COD
    risk_category VARCHAR(20),   -- LOW, MEDIUM, HIGH
    active_flag BOOLEAN,

    -- Unique combination of attributes
    UNIQUE(credit_rating, payment_terms, risk_category, active_flag)
);

-- Fact table references both
CREATE TABLE facts.fact_ar_aging (
    aging_id SERIAL PRIMARY KEY,
    customer_key INTEGER REFERENCES dimensions.dim_customer(customer_key),
    customer_status_key INTEGER REFERENCES dimensions.dim_customer_status(status_key),
    date_key INTEGER REFERENCES dimensions.dim_date(date_key),

    invoice_amount DECIMAL(18,2),
    current_bucket DECIMAL(18,2),
    bucket_30 DECIMAL(18,2),
    bucket_60 DECIMAL(18,2),
    bucket_90 DECIMAL(18,2),
    bucket_over_90 DECIMAL(18,2)
);
```

---

## 7. Degenerate Dimensions

### Problem
Some dimensional attributes exist only in the fact table (invoice number, PO number, journal entry ID). Creating full dimension tables is overkill.

### Enhancement: Degenerate Dimension Metadata

**V4 Metadata:**
```sql
-- Register degenerate dimensions for AI understanding
CREATE TABLE metadata.degenerate_dimensions (
    degenerate_id SERIAL PRIMARY KEY,
    fact_table VARCHAR(100) NOT NULL,
    column_name VARCHAR(100) NOT NULL,
    business_name VARCHAR(200),
    description TEXT,
    format_pattern VARCHAR(100),
    example_values TEXT[],

    -- AI hints
    is_transaction_id BOOLEAN DEFAULT FALSE,
    is_document_number BOOLEAN DEFAULT FALSE,
    source_system VARCHAR(50)
);

INSERT INTO metadata.degenerate_dimensions VALUES
(1, 'fact_gl_journal', 'journal_id', 'Journal Entry ID',
   'Unique identifier for GL journal entry', 'JE-YYYY-NNNNNN',
   ARRAY['JE-2024-000123', 'JE-2024-000124'], TRUE, FALSE, 'GL'),
(2, 'fact_gl_journal', 'document_number', 'Source Document',
   'Reference to source document (invoice, PO, etc)', NULL,
   ARRAY['INV-2024-001', 'PO-2024-500'], FALSE, TRUE, 'Various');
```

---

## 8. Junk Dimensions

### Problem
Many low-cardinality flags and indicators clutter fact tables (is_recurring, is_adjustment, is_intercompany, posting_status).

### Enhancement: Consolidated Flag Dimensions

**V4 Schema:**
```sql
-- Junk dimension for GL posting flags
CREATE TABLE dimensions.dim_gl_flags (
    flag_key SERIAL PRIMARY KEY,

    -- Boolean flags
    is_recurring BOOLEAN DEFAULT FALSE,
    is_adjustment BOOLEAN DEFAULT FALSE,
    is_intercompany BOOLEAN DEFAULT FALSE,
    is_eliminating BOOLEAN DEFAULT FALSE,
    is_manual BOOLEAN DEFAULT FALSE,
    is_reversing BOOLEAN DEFAULT FALSE,

    -- Status codes
    posting_status CHAR(1),  -- P=Posted, U=Unposted, R=Reversed
    approval_status CHAR(1), -- A=Approved, P=Pending, R=Rejected

    -- Human-readable description
    flag_description VARCHAR(200) GENERATED ALWAYS AS (
        CONCAT_WS(', ',
            CASE WHEN is_recurring THEN 'Recurring' END,
            CASE WHEN is_adjustment THEN 'Adjustment' END,
            CASE WHEN is_intercompany THEN 'Intercompany' END,
            CASE WHEN is_manual THEN 'Manual' END,
            CASE WHEN is_reversing THEN 'Reversing' END
        )
    ) STORED,

    UNIQUE(is_recurring, is_adjustment, is_intercompany, is_eliminating,
           is_manual, is_reversing, posting_status, approval_status)
);

-- Pre-populate common combinations
INSERT INTO dimensions.dim_gl_flags
    (is_recurring, is_adjustment, is_intercompany, is_eliminating,
     is_manual, is_reversing, posting_status, approval_status)
SELECT
    r.is_recurring, a.is_adjustment, i.is_intercompany, e.is_eliminating,
    m.is_manual, v.is_reversing, p.posting_status, ap.approval_status
FROM
    (VALUES (FALSE), (TRUE)) AS r(is_recurring),
    (VALUES (FALSE), (TRUE)) AS a(is_adjustment),
    (VALUES (FALSE), (TRUE)) AS i(is_intercompany),
    (VALUES (FALSE), (TRUE)) AS e(is_eliminating),
    (VALUES (FALSE), (TRUE)) AS m(is_manual),
    (VALUES (FALSE), (TRUE)) AS v(is_reversing),
    (VALUES ('P'), ('U'), ('R')) AS p(posting_status),
    (VALUES ('A'), ('P'), ('R')) AS ap(approval_status);
```

---

## 9. Dimension Versioning for Scenarios

### Problem
Planning and forecasting need multiple "what-if" versions of dimension hierarchies (reorg scenarios, new product launches, acquisition modeling).

### Enhancement: Scenario-Based Dimension Versions

**V3 Enhancement:**
```python
class HierarchyScenario(Base):
    __tablename__ = 'hierarchy_scenarios'

    id = Column(Integer, primary_key=True)
    project_id = Column(Integer, ForeignKey('projects.id'))

    scenario_code = Column(String(50))  # BASE, REORG_Q3, ACQUISITION_ABC
    scenario_name = Column(String(200))
    scenario_type = Column(String(50))  # actual, budget, forecast, what_if

    # Version control
    base_scenario_id = Column(Integer, ForeignKey('hierarchy_scenarios.id'))
    version_number = Column(Integer, default=1)
    is_locked = Column(Boolean, default=False)

    # Metadata
    effective_from = Column(Date)
    effective_to = Column(Date)
    created_by = Column(String(50))
    created_at = Column(DateTime, default=datetime.utcnow)

    # Scenario-specific hierarchy overrides stored as JSON delta
    hierarchy_delta = Column(JSON)
```

**V4 Support:**
```sql
-- Scenario dimension for fact tables
CREATE TABLE dimensions.dim_scenario (
    scenario_key SERIAL PRIMARY KEY,
    scenario_code VARCHAR(50) NOT NULL,
    scenario_name VARCHAR(200),
    scenario_type VARCHAR(50),

    -- Versioning
    base_scenario_key INTEGER REFERENCES dimensions.dim_scenario(scenario_key),
    version_number INTEGER DEFAULT 1,

    -- Time boundaries
    effective_from DATE,
    effective_to DATE,

    -- Status
    is_active BOOLEAN DEFAULT TRUE,
    is_locked BOOLEAN DEFAULT FALSE,

    UNIQUE(scenario_code, version_number)
);

-- Facts can now be scenario-aware
ALTER TABLE facts.fact_budget
ADD COLUMN scenario_key INTEGER REFERENCES dimensions.dim_scenario(scenario_key);

ALTER TABLE facts.fact_forecast
ADD COLUMN scenario_key INTEGER REFERENCES dimensions.dim_scenario(scenario_key);
```

---

## 10. Semantic Layer Integration

### Problem
Business users don't think in terms of tables and columns. They think in business metrics like "Revenue", "Gross Margin %", "Operating Expenses".

### Enhancement: Business Semantic Layer

**V4 Metadata:**
```sql
-- Business metrics catalog
CREATE TABLE metadata.business_metrics (
    metric_id SERIAL PRIMARY KEY,
    metric_code VARCHAR(50) NOT NULL UNIQUE,
    metric_name VARCHAR(200) NOT NULL,
    metric_category VARCHAR(50),  -- revenue, expense, ratio, volume

    -- Calculation
    calculation_type VARCHAR(20),  -- simple, derived, calculated
    base_measure VARCHAR(100),     -- column name for simple
    calculation_formula TEXT,      -- SQL expression for calculated

    -- Dimensions
    applicable_dimensions TEXT[],  -- which dimensions make sense
    default_aggregation VARCHAR(20), -- sum, avg, last, weighted_avg

    -- Display
    format_string VARCHAR(50),     -- #,##0.00 or 0.0%
    unit_of_measure VARCHAR(20),   -- USD, units, %, hours

    -- AI hints
    synonyms TEXT[],               -- alternative names
    description TEXT,
    business_context TEXT
);

INSERT INTO metadata.business_metrics VALUES
(1, 'REVENUE', 'Total Revenue', 'revenue', 'simple', 'amount', NULL,
   ARRAY['account', 'entity', 'cost_center', 'product', 'customer', 'date'],
   'sum', '#,##0', 'USD',
   ARRAY['sales', 'income', 'top line', 'gross revenue'],
   'Total revenue from all sources',
   'Filter to revenue accounts (4xxxx)'),

(2, 'GROSS_MARGIN_PCT', 'Gross Margin %', 'ratio', 'calculated', NULL,
   '(SUM(CASE WHEN account_type = ''REVENUE'' THEN amount ELSE 0 END) - ' ||
   'SUM(CASE WHEN account_type = ''COGS'' THEN amount ELSE 0 END)) / ' ||
   'NULLIF(SUM(CASE WHEN account_type = ''REVENUE'' THEN amount ELSE 0 END), 0) * 100',
   ARRAY['entity', 'cost_center', 'product', 'date'],
   'calculated', '0.0%', '%',
   ARRAY['gross profit margin', 'GP%', 'margin'],
   'Revenue minus COGS as percentage of revenue',
   'Higher is better. Industry benchmarks vary.');
```

**MCP Tools to Add:**
| Tool | Purpose |
|------|---------|
| `list_business_metrics` | Show available metrics with descriptions |
| `calculate_metric` | Compute metric value with filters |
| `explain_metric` | AI explanation of metric meaning |
| `suggest_metrics` | Recommend relevant metrics for analysis |

---

## Implementation Priority

### Phase 1: Foundation (Weeks 1-2)
1. SCD Type 2 support in V3 and V4
2. Conformed dimension framework
3. Basic semantic layer

### Phase 2: Advanced Patterns (Weeks 3-4)
4. Role-playing dimensions
5. Bridge tables for allocations
6. Junk dimensions

### Phase 3: Flexibility (Weeks 5-6)
7. Ragged hierarchy support (closure tables)
8. Mini-dimensions
9. Scenario versioning

### Phase 4: Polish (Week 7)
10. Degenerate dimension metadata
11. Full semantic layer with synonyms
12. AI training on dimension patterns

---

## MCP Tool Summary

### New V3 Tools (8)
| Tool | Category |
|------|----------|
| `create_dimension_version` | SCD |
| `get_dimension_as_of` | SCD |
| `compare_dimension_versions` | SCD |
| `suggest_dimension_mappings` | Conformance |
| `create_conformed_dimension` | Conformance |
| `validate_dimension_conformance` | Conformance |
| `create_hierarchy_scenario` | Scenarios |
| `compare_scenarios` | Scenarios |

### New V4 Tools (10)
| Tool | Category |
|------|----------|
| `get_dimension_history` | SCD |
| `get_cross_system_view` | Conformance |
| `create_allocation_bridge` | Bridge |
| `validate_allocation_completeness` | Bridge |
| `analyze_allocation_impact` | Bridge |
| `list_business_metrics` | Semantic |
| `calculate_metric` | Semantic |
| `explain_metric` | Semantic |
| `suggest_metrics` | Semantic |
| `query_with_roles` | Role-Playing |

---

## Benefits Summary

| Enhancement | V3 Benefit | V4 Benefit |
|-------------|------------|------------|
| SCD Type 2 | Historical hierarchy tracking | Point-in-time analysis |
| Conformed Dimensions | Cross-system mapping | Unified analytics |
| Role-Playing | N/A | Flexible date/entity analysis |
| Ragged Hierarchies | Variable depth support | Efficient rollups |
| Bridge Tables | Allocation rules | Multi-dimensional splits |
| Mini-Dimensions | N/A | Efficient status tracking |
| Junk Dimensions | N/A | Clean fact tables |
| Scenarios | What-if modeling | Scenario comparison |
| Semantic Layer | Business terminology | Natural language queries |

---

*These enhancements align with Kimball dimensional modeling best practices while supporting the unique needs of FP&A multi-perspective analysis.*
