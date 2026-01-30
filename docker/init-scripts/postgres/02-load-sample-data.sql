-- DataBridge Analytics V4 - Sample Data
-- Loads sample data for development and testing

-- =============================================================================
-- Populate Date Dimension (2023-2026)
-- =============================================================================
INSERT INTO analytics.dim_date (date_key, full_date, year, quarter, month, month_name,
    week_of_year, day_of_month, day_of_week, day_name, is_weekend,
    is_month_end, is_quarter_end, is_year_end, fiscal_year, fiscal_quarter, fiscal_month)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER as date_key,
    d as full_date,
    EXTRACT(YEAR FROM d)::INTEGER as year,
    EXTRACT(QUARTER FROM d)::INTEGER as quarter,
    EXTRACT(MONTH FROM d)::INTEGER as month,
    TO_CHAR(d, 'Month') as month_name,
    EXTRACT(WEEK FROM d)::INTEGER as week_of_year,
    EXTRACT(DAY FROM d)::INTEGER as day_of_month,
    EXTRACT(DOW FROM d)::INTEGER as day_of_week,
    TO_CHAR(d, 'Day') as day_name,
    EXTRACT(DOW FROM d) IN (0, 6) as is_weekend,
    d = (DATE_TRUNC('month', d) + INTERVAL '1 month' - INTERVAL '1 day')::DATE as is_month_end,
    d = (DATE_TRUNC('quarter', d) + INTERVAL '3 months' - INTERVAL '1 day')::DATE as is_quarter_end,
    EXTRACT(MONTH FROM d) = 12 AND EXTRACT(DAY FROM d) = 31 as is_year_end,
    EXTRACT(YEAR FROM d)::INTEGER as fiscal_year,
    EXTRACT(QUARTER FROM d)::INTEGER as fiscal_quarter,
    EXTRACT(MONTH FROM d)::INTEGER as fiscal_month
FROM generate_series('2023-01-01'::DATE, '2026-12-31'::DATE, '1 day'::INTERVAL) d
ON CONFLICT (date_key) DO NOTHING;

-- =============================================================================
-- Populate Account Dimension
-- =============================================================================
INSERT INTO analytics.dim_account (account_code, account_name, account_type, account_category, account_subcategory, is_balance_sheet, normal_balance) VALUES
-- Assets
('1000', 'Cash and Cash Equivalents', 'Asset', 'Current Assets', 'Cash', true, 'Debit'),
('1100', 'Accounts Receivable', 'Asset', 'Current Assets', 'Receivables', true, 'Debit'),
('1200', 'Inventory', 'Asset', 'Current Assets', 'Inventory', true, 'Debit'),
('1300', 'Prepaid Expenses', 'Asset', 'Current Assets', 'Prepaid', true, 'Debit'),
('1500', 'Fixed Assets', 'Asset', 'Non-Current Assets', 'Property Plant Equipment', true, 'Debit'),
('1510', 'Accumulated Depreciation', 'Asset', 'Non-Current Assets', 'Property Plant Equipment', true, 'Credit'),
-- Liabilities
('2000', 'Accounts Payable', 'Liability', 'Current Liabilities', 'Payables', true, 'Credit'),
('2100', 'Accrued Expenses', 'Liability', 'Current Liabilities', 'Accruals', true, 'Credit'),
('2200', 'Deferred Revenue', 'Liability', 'Current Liabilities', 'Deferred', true, 'Credit'),
('2500', 'Long-term Debt', 'Liability', 'Non-Current Liabilities', 'Debt', true, 'Credit'),
-- Equity
('3000', 'Common Stock', 'Equity', 'Equity', 'Contributed Capital', true, 'Credit'),
('3100', 'Retained Earnings', 'Equity', 'Equity', 'Retained Earnings', true, 'Credit'),
-- Revenue
('4000', 'Product Revenue', 'Revenue', 'Operating Revenue', 'Product Sales', false, 'Credit'),
('4100', 'Service Revenue', 'Revenue', 'Operating Revenue', 'Services', false, 'Credit'),
('4200', 'Other Revenue', 'Revenue', 'Non-Operating Revenue', 'Other', false, 'Credit'),
-- Cost of Sales
('5000', 'Cost of Goods Sold', 'Expense', 'Cost of Sales', 'Direct Costs', false, 'Debit'),
('5100', 'Cost of Services', 'Expense', 'Cost of Sales', 'Direct Labor', false, 'Debit'),
-- Operating Expenses
('6000', 'Salaries and Wages', 'Expense', 'Operating Expenses', 'Personnel', false, 'Debit'),
('6100', 'Employee Benefits', 'Expense', 'Operating Expenses', 'Personnel', false, 'Debit'),
('6200', 'Rent Expense', 'Expense', 'Operating Expenses', 'Facilities', false, 'Debit'),
('6300', 'Utilities Expense', 'Expense', 'Operating Expenses', 'Facilities', false, 'Debit'),
('6400', 'Depreciation Expense', 'Expense', 'Operating Expenses', 'Depreciation', false, 'Debit'),
('6500', 'Marketing Expense', 'Expense', 'Operating Expenses', 'Sales & Marketing', false, 'Debit'),
('6600', 'Travel Expense', 'Expense', 'Operating Expenses', 'Travel', false, 'Debit'),
('6700', 'Professional Fees', 'Expense', 'Operating Expenses', 'Professional Services', false, 'Debit'),
('6800', 'Insurance Expense', 'Expense', 'Operating Expenses', 'Insurance', false, 'Debit'),
('6900', 'Office Supplies', 'Expense', 'Operating Expenses', 'Supplies', false, 'Debit'),
-- Other Expenses
('7000', 'Interest Expense', 'Expense', 'Non-Operating Expenses', 'Financing', false, 'Debit'),
('7100', 'Income Tax Expense', 'Expense', 'Non-Operating Expenses', 'Taxes', false, 'Debit')
ON CONFLICT (account_code) DO NOTHING;

-- =============================================================================
-- Populate Entity Dimension
-- =============================================================================
INSERT INTO analytics.dim_entity (entity_code, entity_name, entity_type, country, currency_code) VALUES
('US-CORP', 'DataBridge US Corp', 'Corporation', 'United States', 'USD'),
('US-WEST', 'West Region Division', 'Division', 'United States', 'USD'),
('US-EAST', 'East Region Division', 'Division', 'United States', 'USD'),
('UK-LTD', 'DataBridge UK Ltd', 'Corporation', 'United Kingdom', 'GBP'),
('DE-GMBH', 'DataBridge Germany GmbH', 'Corporation', 'Germany', 'EUR')
ON CONFLICT (entity_code) DO NOTHING;

-- =============================================================================
-- Populate Cost Center Dimension
-- =============================================================================
INSERT INTO analytics.dim_cost_center (cost_center_code, cost_center_name, department, division) VALUES
('CC-100', 'Executive Office', 'Executive', 'Corporate'),
('CC-200', 'Finance', 'Finance', 'Corporate'),
('CC-300', 'Human Resources', 'HR', 'Corporate'),
('CC-400', 'Information Technology', 'IT', 'Corporate'),
('CC-500', 'Sales - North', 'Sales', 'Commercial'),
('CC-510', 'Sales - South', 'Sales', 'Commercial'),
('CC-600', 'Marketing', 'Marketing', 'Commercial'),
('CC-700', 'Operations', 'Operations', 'Operations'),
('CC-800', 'Customer Support', 'Support', 'Operations'),
('CC-900', 'Research & Development', 'R&D', 'Innovation')
ON CONFLICT (cost_center_code) DO NOTHING;

-- =============================================================================
-- Populate Product Dimension
-- =============================================================================
INSERT INTO analytics.dim_product (product_code, product_name, product_category, product_subcategory, product_line, unit_cost, unit_price) VALUES
('PROD-001', 'Analytics Platform - Basic', 'Software', 'SaaS', 'Analytics', 50.00, 99.00),
('PROD-002', 'Analytics Platform - Pro', 'Software', 'SaaS', 'Analytics', 100.00, 249.00),
('PROD-003', 'Analytics Platform - Enterprise', 'Software', 'SaaS', 'Analytics', 250.00, 599.00),
('PROD-004', 'Data Integration Module', 'Software', 'Add-on', 'Integration', 25.00, 79.00),
('PROD-005', 'Custom Reporting Module', 'Software', 'Add-on', 'Reporting', 30.00, 99.00),
('SVC-001', 'Implementation Services', 'Services', 'Professional Services', 'Implementation', 100.00, 175.00),
('SVC-002', 'Training Services', 'Services', 'Professional Services', 'Training', 75.00, 150.00),
('SVC-003', 'Support - Premium', 'Services', 'Support', 'Support', 20.00, 49.00)
ON CONFLICT (product_code) DO NOTHING;

-- =============================================================================
-- Populate Customer Dimension
-- =============================================================================
INSERT INTO analytics.dim_customer (customer_code, customer_name, customer_type, industry, region, country) VALUES
('CUST-001', 'Acme Corporation', 'Enterprise', 'Manufacturing', 'North America', 'United States'),
('CUST-002', 'Global Tech Inc', 'Enterprise', 'Technology', 'North America', 'United States'),
('CUST-003', 'Retail Giants LLC', 'Enterprise', 'Retail', 'North America', 'United States'),
('CUST-004', 'Financial Services Co', 'Enterprise', 'Financial Services', 'North America', 'United States'),
('CUST-005', 'Healthcare Partners', 'Mid-Market', 'Healthcare', 'North America', 'United States'),
('CUST-006', 'Energy Solutions Ltd', 'Enterprise', 'Energy', 'Europe', 'United Kingdom'),
('CUST-007', 'Auto Motors GmbH', 'Enterprise', 'Automotive', 'Europe', 'Germany'),
('CUST-008', 'Small Biz Inc', 'SMB', 'Professional Services', 'North America', 'United States'),
('CUST-009', 'Startup Ventures', 'SMB', 'Technology', 'North America', 'United States'),
('CUST-010', 'Construction Co', 'Mid-Market', 'Construction', 'North America', 'United States')
ON CONFLICT (customer_code) DO NOTHING;

-- =============================================================================
-- Populate Version Dimension
-- =============================================================================
INSERT INTO analytics.dim_version (version_code, version_name, version_type, fiscal_year, is_current) VALUES
('ACT-2024', 'Actuals 2024', 'Actual', 2024, true),
('ACT-2023', 'Actuals 2023', 'Actual', 2023, false),
('BUD-2024', 'Budget 2024', 'Budget', 2024, true),
('BUD-2025', 'Budget 2025', 'Budget', 2025, false),
('FC-2024-Q3', 'Forecast 2024 Q3', 'Forecast', 2024, false),
('FC-2024-Q4', 'Forecast 2024 Q4', 'Forecast', 2024, true),
('SC-BASE', 'Base Case Scenario', 'Scenario', 2024, false),
('SC-UPSIDE', 'Upside Scenario', 'Scenario', 2024, false),
('SC-DOWNSIDE', 'Downside Scenario', 'Scenario', 2024, false)
ON CONFLICT (version_code) DO NOTHING;

-- =============================================================================
-- Completion message
-- =============================================================================
DO $$
BEGIN
    RAISE NOTICE 'Sample data loaded successfully';
END $$;
