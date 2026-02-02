-- ============================================
-- DataBridge Analytics - Schema Creation
-- ============================================
-- This script creates the star schema for the
-- sample financial data warehouse.
-- ============================================

-- Create schemas
CREATE SCHEMA IF NOT EXISTS dimensions;
CREATE SCHEMA IF NOT EXISTS facts;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS analytics;

-- ============================================
-- DIMENSION TABLES
-- ============================================

-- Date Dimension (Conformed)
CREATE TABLE dimensions.dim_date (
    date_key INT PRIMARY KEY,                    -- YYYYMMDD format
    full_date DATE NOT NULL UNIQUE,
    day_of_week INT NOT NULL,
    day_name VARCHAR(10) NOT NULL,
    day_of_month INT NOT NULL,
    day_of_year INT NOT NULL,
    week_of_year INT NOT NULL,
    month_number INT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter INT NOT NULL,
    quarter_name VARCHAR(6) NOT NULL,            -- Q1 2024
    year INT NOT NULL,
    fiscal_year INT NOT NULL,
    fiscal_quarter INT NOT NULL,
    fiscal_month INT NOT NULL,
    is_weekend BOOLEAN NOT NULL DEFAULT FALSE,
    is_holiday BOOLEAN NOT NULL DEFAULT FALSE,
    is_month_end BOOLEAN NOT NULL DEFAULT FALSE,
    is_quarter_end BOOLEAN NOT NULL DEFAULT FALSE,
    is_year_end BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_dim_date_year_month ON dimensions.dim_date(year, month_number);
CREATE INDEX idx_dim_date_fiscal ON dimensions.dim_date(fiscal_year, fiscal_quarter);

-- Period Dimension (Monthly Grain)
CREATE TABLE dimensions.dim_period (
    period_key INT PRIMARY KEY,                  -- YYYYMM format
    period_name VARCHAR(20) NOT NULL,            -- January 2024
    month_number INT NOT NULL,
    month_name VARCHAR(10) NOT NULL,
    quarter INT NOT NULL,
    quarter_name VARCHAR(6) NOT NULL,
    year INT NOT NULL,
    fiscal_year INT NOT NULL,
    fiscal_quarter INT NOT NULL,
    first_day_of_period DATE NOT NULL,
    last_day_of_period DATE NOT NULL,
    days_in_period INT NOT NULL,
    is_current_period BOOLEAN DEFAULT FALSE,
    is_closed BOOLEAN DEFAULT FALSE
);

-- Account Dimension (Chart of Accounts)
CREATE TABLE dimensions.dim_account (
    account_key SERIAL PRIMARY KEY,
    account_id VARCHAR(20) NOT NULL UNIQUE,
    account_name VARCHAR(255) NOT NULL,
    account_type VARCHAR(50) NOT NULL,           -- Asset, Liability, Equity, Revenue, Expense
    account_subtype VARCHAR(100),
    is_balance_sheet BOOLEAN NOT NULL,
    is_debit_normal BOOLEAN NOT NULL,
    parent_account_id VARCHAR(20),

    -- Hierarchy levels (for Librarian integration)
    level_1 VARCHAR(100),                        -- e.g., "Total Revenue"
    level_2 VARCHAR(100),                        -- e.g., "Product Revenue"
    level_3 VARCHAR(100),                        -- e.g., "Hardware"
    level_4 VARCHAR(100),                        -- e.g., "Servers"
    level_5 VARCHAR(100),

    sort_order INT,
    is_active BOOLEAN DEFAULT TRUE,
    effective_from DATE,
    effective_to DATE,

    -- librarian mapping reference
    librarian_hierarchy_id VARCHAR(255),
    librarian_hierarchy_node VARCHAR(255)
);

CREATE INDEX idx_dim_account_type ON dimensions.dim_account(account_type);
CREATE INDEX idx_dim_account_parent ON dimensions.dim_account(parent_account_id);
CREATE INDEX idx_dim_account_librarian ON dimensions.dim_account(librarian_hierarchy_id);

-- Cost Center Dimension
CREATE TABLE dimensions.dim_cost_center (
    cost_center_key SERIAL PRIMARY KEY,
    cost_center_id VARCHAR(20) NOT NULL UNIQUE,
    cost_center_name VARCHAR(255) NOT NULL,
    department VARCHAR(100),
    division VARCHAR(100),
    business_unit VARCHAR(100),
    region VARCHAR(100),
    manager_name VARCHAR(255),
    is_active BOOLEAN DEFAULT TRUE,

    -- librarian mapping reference
    librarian_hierarchy_id VARCHAR(255),
    librarian_hierarchy_node VARCHAR(255)
);

CREATE INDEX idx_dim_cc_bu ON dimensions.dim_cost_center(business_unit);

-- Legal Entity Dimension
CREATE TABLE dimensions.dim_entity (
    entity_key SERIAL PRIMARY KEY,
    entity_id VARCHAR(20) NOT NULL UNIQUE,
    entity_name VARCHAR(255) NOT NULL,
    entity_type VARCHAR(50),                     -- Operating, Holding, Eliminated
    country VARCHAR(100),
    currency_code VARCHAR(3),
    parent_entity_id VARCHAR(20),
    consolidation_method VARCHAR(50),            -- Full, Equity, Proportional
    is_eliminating_entity BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE
);

-- Product Dimension
CREATE TABLE dimensions.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id VARCHAR(50) NOT NULL UNIQUE,
    product_name VARCHAR(255) NOT NULL,
    product_category VARCHAR(100),
    product_subcategory VARCHAR(100),
    product_line VARCHAR(100),
    brand VARCHAR(100),
    unit_of_measure VARCHAR(20),
    standard_cost DECIMAL(15,4),
    standard_price DECIMAL(15,4),
    is_active BOOLEAN DEFAULT TRUE
);

CREATE INDEX idx_dim_product_cat ON dimensions.dim_product(product_category);

-- Customer Dimension
CREATE TABLE dimensions.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    customer_name VARCHAR(255) NOT NULL,
    customer_type VARCHAR(50),                   -- Enterprise, SMB, Consumer
    industry VARCHAR(100),
    segment VARCHAR(100),
    region VARCHAR(100),
    country VARCHAR(100),
    state VARCHAR(100),
    city VARCHAR(100),
    credit_rating VARCHAR(10),
    is_active BOOLEAN DEFAULT TRUE,

    -- SCD Type 2 tracking
    effective_from DATE NOT NULL,
    effective_to DATE,
    is_current BOOLEAN DEFAULT TRUE
);

CREATE INDEX idx_dim_customer_segment ON dimensions.dim_customer(segment);
CREATE INDEX idx_dim_customer_current ON dimensions.dim_customer(is_current);

-- Geography Dimension
CREATE TABLE dimensions.dim_geography (
    geo_key SERIAL PRIMARY KEY,
    geo_id VARCHAR(50) NOT NULL UNIQUE,
    region VARCHAR(100),
    country VARCHAR(100),
    state VARCHAR(100),
    city VARCHAR(100),
    postal_code VARCHAR(20)
);

-- Asset Dimension (for operational data)
CREATE TABLE dimensions.dim_asset (
    asset_key SERIAL PRIMARY KEY,
    asset_id VARCHAR(50) NOT NULL UNIQUE,
    asset_name VARCHAR(255) NOT NULL,
    asset_type VARCHAR(100),                     -- Well, Production Line, Vehicle, etc.
    asset_category VARCHAR(100),
    location VARCHAR(255),
    parent_asset_id VARCHAR(50),
    acquisition_date DATE,
    status VARCHAR(50),                          -- Active, Idle, Disposed

    -- Industry-specific fields
    -- Oil & Gas
    well_api VARCHAR(20),
    field_name VARCHAR(100),
    basin_name VARCHAR(100),
    working_interest DECIMAL(7,4),
    net_revenue_interest DECIMAL(7,4),

    -- Manufacturing
    production_line VARCHAR(100),
    work_center VARCHAR(100),
    capacity_per_hour DECIMAL(15,4)
);

-- Budget Version Dimension
CREATE TABLE dimensions.dim_budget_version (
    version_key SERIAL PRIMARY KEY,
    version_id VARCHAR(50) NOT NULL UNIQUE,
    version_name VARCHAR(100) NOT NULL,
    version_type VARCHAR(50),                    -- Original, Revised, Forecast
    effective_date DATE,
    approved_by VARCHAR(255),
    approved_date DATE,
    is_current BOOLEAN DEFAULT FALSE,
    notes TEXT
);

-- ============================================
-- FACT TABLES
-- ============================================

-- GL Journal Entries (Transaction Grain)
CREATE TABLE facts.fact_gl_journal (
    journal_key SERIAL PRIMARY KEY,
    date_key INT NOT NULL REFERENCES dimensions.dim_date(date_key),
    period_key INT NOT NULL REFERENCES dimensions.dim_period(period_key),
    account_key INT NOT NULL REFERENCES dimensions.dim_account(account_key),
    cost_center_key INT REFERENCES dimensions.dim_cost_center(cost_center_key),
    entity_key INT NOT NULL REFERENCES dimensions.dim_entity(entity_key),

    -- Transaction details
    journal_id VARCHAR(50) NOT NULL,
    journal_line INT NOT NULL,
    journal_type VARCHAR(20),                    -- Standard, Adjusting, Closing, Reversing
    source_system VARCHAR(50),
    document_number VARCHAR(100),
    description VARCHAR(500),

    -- Measures
    debit_amount DECIMAL(15,2) DEFAULT 0,
    credit_amount DECIMAL(15,2) DEFAULT 0,
    net_amount DECIMAL(15,2) GENERATED ALWAYS AS (debit_amount - credit_amount) STORED,

    -- Multi-currency
    local_currency VARCHAR(3),
    local_amount DECIMAL(15,2),
    exchange_rate DECIMAL(15,6),

    -- Audit
    posted_date DATE,
    posted_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    UNIQUE(journal_id, journal_line)
);

CREATE INDEX idx_fact_gl_date ON facts.fact_gl_journal(date_key);
CREATE INDEX idx_fact_gl_period ON facts.fact_gl_journal(period_key);
CREATE INDEX idx_fact_gl_account ON facts.fact_gl_journal(account_key);
CREATE INDEX idx_fact_gl_cc ON facts.fact_gl_journal(cost_center_key);
CREATE INDEX idx_fact_gl_entity ON facts.fact_gl_journal(entity_key);

-- GL Balances (Period Grain - Aggregated)
CREATE TABLE facts.fact_gl_balance (
    balance_key SERIAL PRIMARY KEY,
    period_key INT NOT NULL REFERENCES dimensions.dim_period(period_key),
    account_key INT NOT NULL REFERENCES dimensions.dim_account(account_key),
    cost_center_key INT REFERENCES dimensions.dim_cost_center(cost_center_key),
    entity_key INT NOT NULL REFERENCES dimensions.dim_entity(entity_key),

    -- Balance measures
    beginning_balance DECIMAL(15,2) DEFAULT 0,
    period_debit DECIMAL(15,2) DEFAULT 0,
    period_credit DECIMAL(15,2) DEFAULT 0,
    period_activity DECIMAL(15,2) GENERATED ALWAYS AS (period_debit - period_credit) STORED,
    ending_balance DECIMAL(15,2) DEFAULT 0,

    -- For income statement accounts
    ytd_debit DECIMAL(15,2) DEFAULT 0,
    ytd_credit DECIMAL(15,2) DEFAULT 0,
    ytd_activity DECIMAL(15,2) GENERATED ALWAYS AS (ytd_debit - ytd_credit) STORED,

    UNIQUE(period_key, account_key, cost_center_key, entity_key)
);

CREATE INDEX idx_fact_balance_period ON facts.fact_gl_balance(period_key);
CREATE INDEX idx_fact_balance_account ON facts.fact_gl_balance(account_key);

-- Budget (Period × Account × Cost Center)
CREATE TABLE facts.fact_budget (
    budget_key SERIAL PRIMARY KEY,
    period_key INT NOT NULL REFERENCES dimensions.dim_period(period_key),
    account_key INT NOT NULL REFERENCES dimensions.dim_account(account_key),
    cost_center_key INT REFERENCES dimensions.dim_cost_center(cost_center_key),
    entity_key INT NOT NULL REFERENCES dimensions.dim_entity(entity_key),
    version_key INT NOT NULL REFERENCES dimensions.dim_budget_version(version_key),

    -- Measures
    budget_amount DECIMAL(15,2) DEFAULT 0,

    -- For comparison
    prior_year_actual DECIMAL(15,2) DEFAULT 0,
    prior_year_budget DECIMAL(15,2) DEFAULT 0,

    UNIQUE(period_key, account_key, cost_center_key, entity_key, version_key)
);

CREATE INDEX idx_fact_budget_period ON facts.fact_budget(period_key);
CREATE INDEX idx_fact_budget_version ON facts.fact_budget(version_key);

-- Forecast (Period × Account × Cost Center)
CREATE TABLE facts.fact_forecast (
    forecast_key SERIAL PRIMARY KEY,
    period_key INT NOT NULL REFERENCES dimensions.dim_period(period_key),
    account_key INT NOT NULL REFERENCES dimensions.dim_account(account_key),
    cost_center_key INT REFERENCES dimensions.dim_cost_center(cost_center_key),
    entity_key INT NOT NULL REFERENCES dimensions.dim_entity(entity_key),
    version_key INT NOT NULL REFERENCES dimensions.dim_budget_version(version_key),

    -- Measures
    forecast_amount DECIMAL(15,2) DEFAULT 0,

    -- Forecast metadata
    forecast_date DATE,
    confidence_level VARCHAR(20),                -- High, Medium, Low

    UNIQUE(period_key, account_key, cost_center_key, entity_key, version_key)
);

-- Sales (Transaction Grain)
CREATE TABLE facts.fact_sales (
    sales_key SERIAL PRIMARY KEY,
    date_key INT NOT NULL REFERENCES dimensions.dim_date(date_key),
    customer_key INT NOT NULL REFERENCES dimensions.dim_customer(customer_key),
    product_key INT NOT NULL REFERENCES dimensions.dim_product(product_key),
    geo_key INT REFERENCES dimensions.dim_geography(geo_key),

    -- Degenerate dimensions
    order_id VARCHAR(50) NOT NULL,
    order_line_number INT NOT NULL,

    -- Measures
    quantity INT DEFAULT 0,
    unit_price DECIMAL(15,4) DEFAULT 0,
    discount_percent DECIMAL(5,2) DEFAULT 0,
    discount_amount DECIMAL(15,2) DEFAULT 0,
    gross_amount DECIMAL(15,2) DEFAULT 0,
    net_amount DECIMAL(15,2) DEFAULT 0,
    cost_amount DECIMAL(15,2) DEFAULT 0,
    margin_amount DECIMAL(15,2) DEFAULT 0,

    UNIQUE(order_id, order_line_number)
);

CREATE INDEX idx_fact_sales_date ON facts.fact_sales(date_key);
CREATE INDEX idx_fact_sales_customer ON facts.fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product ON facts.fact_sales(product_key);

-- Headcount (Monthly Grain)
CREATE TABLE facts.fact_headcount (
    headcount_key SERIAL PRIMARY KEY,
    period_key INT NOT NULL REFERENCES dimensions.dim_period(period_key),
    cost_center_key INT NOT NULL REFERENCES dimensions.dim_cost_center(cost_center_key),
    entity_key INT NOT NULL REFERENCES dimensions.dim_entity(entity_key),

    -- Headcount measures
    beginning_headcount INT DEFAULT 0,
    hires INT DEFAULT 0,
    terminations INT DEFAULT 0,
    transfers_in INT DEFAULT 0,
    transfers_out INT DEFAULT 0,
    ending_headcount INT DEFAULT 0,

    -- Compensation measures
    base_salary DECIMAL(15,2) DEFAULT 0,
    bonus DECIMAL(15,2) DEFAULT 0,
    benefits DECIMAL(15,2) DEFAULT 0,
    total_compensation DECIMAL(15,2) DEFAULT 0,

    -- FTE measures
    fte_count DECIMAL(10,2) DEFAULT 0,
    contractor_count INT DEFAULT 0,

    UNIQUE(period_key, cost_center_key, entity_key)
);

-- Production (Daily Grain - Operational)
CREATE TABLE facts.fact_production (
    production_key SERIAL PRIMARY KEY,
    date_key INT NOT NULL REFERENCES dimensions.dim_date(date_key),
    asset_key INT NOT NULL REFERENCES dimensions.dim_asset(asset_key),
    product_key INT REFERENCES dimensions.dim_product(product_key),

    -- Volume measures
    gross_production DECIMAL(15,4) DEFAULT 0,
    net_production DECIMAL(15,4) DEFAULT 0,
    sales_volume DECIMAL(15,4) DEFAULT 0,

    -- Pricing
    realized_price DECIMAL(15,4),
    benchmark_price DECIMAL(15,4),

    -- Revenue calculation
    gross_revenue DECIMAL(15,2) GENERATED ALWAYS AS (sales_volume * realized_price) STORED,

    -- Operational
    producing_hours DECIMAL(10,2) DEFAULT 0,
    downtime_hours DECIMAL(10,2) DEFAULT 0,
    efficiency_percent DECIMAL(5,2),

    UNIQUE(date_key, asset_key, product_key)
);

CREATE INDEX idx_fact_prod_date ON facts.fact_production(date_key);
CREATE INDEX idx_fact_prod_asset ON facts.fact_production(asset_key);

-- Operating Costs (Monthly by Asset)
CREATE TABLE facts.fact_operating_costs (
    cost_key SERIAL PRIMARY KEY,
    period_key INT NOT NULL REFERENCES dimensions.dim_period(period_key),
    asset_key INT NOT NULL REFERENCES dimensions.dim_asset(asset_key),
    account_key INT NOT NULL REFERENCES dimensions.dim_account(account_key),

    -- Measures
    actual_cost DECIMAL(15,2) DEFAULT 0,
    budget_cost DECIMAL(15,2) DEFAULT 0,
    prior_year_cost DECIMAL(15,2) DEFAULT 0,

    -- For unit cost calculation
    production_volume DECIMAL(15,4),
    cost_per_unit DECIMAL(15,4),

    UNIQUE(period_key, asset_key, account_key)
);

-- Intercompany Transactions
CREATE TABLE facts.fact_intercompany (
    ic_key SERIAL PRIMARY KEY,
    period_key INT NOT NULL REFERENCES dimensions.dim_period(period_key),
    from_entity_key INT NOT NULL REFERENCES dimensions.dim_entity(entity_key),
    to_entity_key INT NOT NULL REFERENCES dimensions.dim_entity(entity_key),
    account_key INT NOT NULL REFERENCES dimensions.dim_account(account_key),

    -- Transaction details
    ic_transaction_id VARCHAR(50),
    description VARCHAR(500),

    -- Measures
    amount DECIMAL(15,2) NOT NULL,

    -- Status
    from_entity_status VARCHAR(20),              -- Posted, Pending
    to_entity_status VARCHAR(20),                -- Posted, Pending
    elimination_status VARCHAR(20),              -- Pending, Eliminated
    matched BOOLEAN DEFAULT FALSE,
    match_difference DECIMAL(15,2)
);

CREATE INDEX idx_fact_ic_period ON facts.fact_intercompany(period_key);
CREATE INDEX idx_fact_ic_from ON facts.fact_intercompany(from_entity_key);
CREATE INDEX idx_fact_ic_to ON facts.fact_intercompany(to_entity_key);

-- ============================================
-- ANALYTICS VIEWS
-- ============================================

-- P&L by Period (uses account hierarchy)
CREATE VIEW analytics.v_income_statement AS
SELECT
    p.period_key,
    p.period_name,
    p.fiscal_year,
    p.fiscal_quarter,
    a.account_type,
    a.level_1,
    a.level_2,
    a.level_3,
    e.entity_name,
    SUM(b.period_debit) as total_debit,
    SUM(b.period_credit) as total_credit,
    SUM(b.period_activity) as net_activity,
    SUM(b.ytd_activity) as ytd_activity
FROM facts.fact_gl_balance b
JOIN dimensions.dim_period p ON b.period_key = p.period_key
JOIN dimensions.dim_account a ON b.account_key = a.account_key
JOIN dimensions.dim_entity e ON b.entity_key = e.entity_key
WHERE a.is_balance_sheet = FALSE
GROUP BY
    p.period_key, p.period_name, p.fiscal_year, p.fiscal_quarter,
    a.account_type, a.level_1, a.level_2, a.level_3,
    e.entity_name;

-- Budget vs Actual by Account
CREATE VIEW analytics.v_budget_variance AS
SELECT
    p.period_key,
    p.period_name,
    a.account_name,
    a.level_1,
    a.level_2,
    cc.cost_center_name,
    cc.department,
    b.budget_amount,
    COALESCE(g.period_activity, 0) as actual_amount,
    COALESCE(g.period_activity, 0) - b.budget_amount as variance_amount,
    CASE
        WHEN b.budget_amount = 0 THEN NULL
        ELSE (COALESCE(g.period_activity, 0) - b.budget_amount) / b.budget_amount * 100
    END as variance_percent
FROM facts.fact_budget b
JOIN dimensions.dim_period p ON b.period_key = p.period_key
JOIN dimensions.dim_account a ON b.account_key = a.account_key
LEFT JOIN dimensions.dim_cost_center cc ON b.cost_center_key = cc.cost_center_key
LEFT JOIN facts.fact_gl_balance g ON
    b.period_key = g.period_key AND
    b.account_key = g.account_key AND
    b.cost_center_key = g.cost_center_key AND
    b.entity_key = g.entity_key;

-- Grant permissions
GRANT USAGE ON SCHEMA dimensions TO dw_admin;
GRANT USAGE ON SCHEMA facts TO dw_admin;
GRANT USAGE ON SCHEMA staging TO dw_admin;
GRANT USAGE ON SCHEMA analytics TO dw_admin;
GRANT SELECT ON ALL TABLES IN SCHEMA dimensions TO dw_admin;
GRANT SELECT ON ALL TABLES IN SCHEMA facts TO dw_admin;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO dw_admin;
