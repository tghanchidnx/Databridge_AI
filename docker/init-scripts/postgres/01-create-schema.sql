-- DataBridge Analytics Researcher - Sample Star Schema
-- Creates a dimensional model for FP&A analytics

-- =============================================================================
-- Create schemas
-- =============================================================================
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS staging;

-- =============================================================================
-- Dimension Tables
-- =============================================================================

-- Date dimension
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20) NOT NULL,
    week_of_year INTEGER NOT NULL,
    day_of_month INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_month_end BOOLEAN NOT NULL,
    is_quarter_end BOOLEAN NOT NULL,
    is_year_end BOOLEAN NOT NULL,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER,
    fiscal_month INTEGER
);

-- Account dimension (Chart of Accounts)
CREATE TABLE IF NOT EXISTS analytics.dim_account (
    account_key SERIAL PRIMARY KEY,
    account_code VARCHAR(50) NOT NULL UNIQUE,
    account_name VARCHAR(255) NOT NULL,
    account_type VARCHAR(50) NOT NULL, -- Asset, Liability, Equity, Revenue, Expense
    account_category VARCHAR(100),
    account_subcategory VARCHAR(100),
    is_balance_sheet BOOLEAN NOT NULL,
    normal_balance VARCHAR(10) NOT NULL, -- Debit, Credit
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Entity dimension (Legal entities / Business units)
CREATE TABLE IF NOT EXISTS analytics.dim_entity (
    entity_key SERIAL PRIMARY KEY,
    entity_code VARCHAR(50) NOT NULL UNIQUE,
    entity_name VARCHAR(255) NOT NULL,
    entity_type VARCHAR(50), -- Corporation, LLC, Branch, Division
    parent_entity_code VARCHAR(50),
    country VARCHAR(100),
    currency_code VARCHAR(3),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Cost Center dimension
CREATE TABLE IF NOT EXISTS analytics.dim_cost_center (
    cost_center_key SERIAL PRIMARY KEY,
    cost_center_code VARCHAR(50) NOT NULL UNIQUE,
    cost_center_name VARCHAR(255) NOT NULL,
    department VARCHAR(100),
    division VARCHAR(100),
    manager VARCHAR(255),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Product dimension
CREATE TABLE IF NOT EXISTS analytics.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_code VARCHAR(50) NOT NULL UNIQUE,
    product_name VARCHAR(255) NOT NULL,
    product_category VARCHAR(100),
    product_subcategory VARCHAR(100),
    product_line VARCHAR(100),
    unit_cost DECIMAL(18,4),
    unit_price DECIMAL(18,4),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Customer dimension
CREATE TABLE IF NOT EXISTS analytics.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_code VARCHAR(50) NOT NULL UNIQUE,
    customer_name VARCHAR(255) NOT NULL,
    customer_type VARCHAR(50),
    industry VARCHAR(100),
    region VARCHAR(100),
    country VARCHAR(100),
    credit_limit DECIMAL(18,2),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Version dimension (Budget versions)
CREATE TABLE IF NOT EXISTS analytics.dim_version (
    version_key SERIAL PRIMARY KEY,
    version_code VARCHAR(50) NOT NULL UNIQUE,
    version_name VARCHAR(255) NOT NULL,
    version_type VARCHAR(50) NOT NULL, -- Actual, Budget, Forecast, Scenario
    fiscal_year INTEGER,
    is_current BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- Fact Tables
-- =============================================================================

-- GL Journal fact (transactional)
CREATE TABLE IF NOT EXISTS analytics.fact_gl_journal (
    journal_id SERIAL PRIMARY KEY,
    journal_entry_id VARCHAR(50) NOT NULL,
    line_number INTEGER NOT NULL,
    date_key INTEGER NOT NULL REFERENCES analytics.dim_date(date_key),
    account_key INTEGER NOT NULL REFERENCES analytics.dim_account(account_key),
    entity_key INTEGER NOT NULL REFERENCES analytics.dim_entity(entity_key),
    cost_center_key INTEGER REFERENCES analytics.dim_cost_center(cost_center_key),
    debit_amount DECIMAL(18,2) DEFAULT 0,
    credit_amount DECIMAL(18,2) DEFAULT 0,
    amount DECIMAL(18,2) GENERATED ALWAYS AS (debit_amount - credit_amount) STORED,
    description VARCHAR(500),
    reference VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(journal_entry_id, line_number)
);

-- GL Balance fact (periodic)
CREATE TABLE IF NOT EXISTS analytics.fact_gl_balance (
    balance_id SERIAL PRIMARY KEY,
    date_key INTEGER NOT NULL REFERENCES analytics.dim_date(date_key),
    account_key INTEGER NOT NULL REFERENCES analytics.dim_account(account_key),
    entity_key INTEGER NOT NULL REFERENCES analytics.dim_entity(entity_key),
    version_key INTEGER NOT NULL REFERENCES analytics.dim_version(version_key),
    beginning_balance DECIMAL(18,2) DEFAULT 0,
    period_activity DECIMAL(18,2) DEFAULT 0,
    ending_balance DECIMAL(18,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date_key, account_key, entity_key, version_key)
);

-- Budget fact
CREATE TABLE IF NOT EXISTS analytics.fact_budget (
    budget_id SERIAL PRIMARY KEY,
    date_key INTEGER NOT NULL REFERENCES analytics.dim_date(date_key),
    account_key INTEGER NOT NULL REFERENCES analytics.dim_account(account_key),
    entity_key INTEGER NOT NULL REFERENCES analytics.dim_entity(entity_key),
    cost_center_key INTEGER REFERENCES analytics.dim_cost_center(cost_center_key),
    version_key INTEGER NOT NULL REFERENCES analytics.dim_version(version_key),
    amount DECIMAL(18,2) NOT NULL,
    quantity DECIMAL(18,4),
    unit_amount DECIMAL(18,4),
    notes VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(date_key, account_key, entity_key, cost_center_key, version_key)
);

-- Sales fact
CREATE TABLE IF NOT EXISTS analytics.fact_sales (
    sale_id SERIAL PRIMARY KEY,
    date_key INTEGER NOT NULL REFERENCES analytics.dim_date(date_key),
    customer_key INTEGER NOT NULL REFERENCES analytics.dim_customer(customer_key),
    product_key INTEGER NOT NULL REFERENCES analytics.dim_product(product_key),
    entity_key INTEGER NOT NULL REFERENCES analytics.dim_entity(entity_key),
    order_number VARCHAR(50),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(18,4) NOT NULL,
    unit_cost DECIMAL(18,4),
    discount_amount DECIMAL(18,2) DEFAULT 0,
    revenue DECIMAL(18,2) GENERATED ALWAYS AS (quantity * unit_price - discount_amount) STORED,
    cost_of_goods DECIMAL(18,2) GENERATED ALWAYS AS (quantity * COALESCE(unit_cost, 0)) STORED,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- Indexes
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_fact_gl_journal_date ON analytics.fact_gl_journal(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_gl_journal_account ON analytics.fact_gl_journal(account_key);
CREATE INDEX IF NOT EXISTS idx_fact_gl_journal_entity ON analytics.fact_gl_journal(entity_key);

CREATE INDEX IF NOT EXISTS idx_fact_gl_balance_date ON analytics.fact_gl_balance(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_gl_balance_account ON analytics.fact_gl_balance(account_key);
CREATE INDEX IF NOT EXISTS idx_fact_gl_balance_version ON analytics.fact_gl_balance(version_key);

CREATE INDEX IF NOT EXISTS idx_fact_budget_date ON analytics.fact_budget(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_budget_account ON analytics.fact_budget(account_key);
CREATE INDEX IF NOT EXISTS idx_fact_budget_version ON analytics.fact_budget(version_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON analytics.fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON analytics.fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON analytics.fact_sales(product_key);

-- =============================================================================
-- Completion message
-- =============================================================================
DO $$
BEGIN
    RAISE NOTICE 'DataBridge Analytics Researcher schema created successfully';
END $$;
