-- ============================================
-- DataBridge Analytics - Sample Data Load
-- ============================================
-- Generates 3 years of realistic financial data
-- for testing FP&A workflows.
-- ============================================

-- ============================================
-- POPULATE DATE DIMENSION (2022-2025)
-- ============================================
INSERT INTO dimensions.dim_date (
    date_key, full_date, day_of_week, day_name, day_of_month, day_of_year,
    week_of_year, month_number, month_name, quarter, quarter_name,
    year, fiscal_year, fiscal_quarter, fiscal_month,
    is_weekend, is_holiday, is_month_end, is_quarter_end, is_year_end
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INT as date_key,
    d as full_date,
    EXTRACT(DOW FROM d)::INT as day_of_week,
    TO_CHAR(d, 'Day') as day_name,
    EXTRACT(DAY FROM d)::INT as day_of_month,
    EXTRACT(DOY FROM d)::INT as day_of_year,
    EXTRACT(WEEK FROM d)::INT as week_of_year,
    EXTRACT(MONTH FROM d)::INT as month_number,
    TO_CHAR(d, 'Month') as month_name,
    EXTRACT(QUARTER FROM d)::INT as quarter,
    'Q' || EXTRACT(QUARTER FROM d) || ' ' || EXTRACT(YEAR FROM d) as quarter_name,
    EXTRACT(YEAR FROM d)::INT as year,
    EXTRACT(YEAR FROM d)::INT as fiscal_year,  -- Assuming calendar = fiscal
    EXTRACT(QUARTER FROM d)::INT as fiscal_quarter,
    EXTRACT(MONTH FROM d)::INT as fiscal_month,
    EXTRACT(DOW FROM d) IN (0, 6) as is_weekend,
    FALSE as is_holiday,
    d = (DATE_TRUNC('month', d) + INTERVAL '1 month' - INTERVAL '1 day')::DATE as is_month_end,
    d = (DATE_TRUNC('quarter', d) + INTERVAL '3 months' - INTERVAL '1 day')::DATE as is_quarter_end,
    d = (DATE_TRUNC('year', d) + INTERVAL '1 year' - INTERVAL '1 day')::DATE as is_year_end
FROM generate_series('2022-01-01'::DATE, '2025-12-31'::DATE, '1 day'::INTERVAL) as d;

-- ============================================
-- POPULATE PERIOD DIMENSION
-- ============================================
INSERT INTO dimensions.dim_period (
    period_key, period_name, month_number, month_name, quarter, quarter_name,
    year, fiscal_year, fiscal_quarter, first_day_of_period, last_day_of_period,
    days_in_period, is_current_period, is_closed
)
SELECT
    TO_CHAR(d, 'YYYYMM')::INT as period_key,
    TO_CHAR(d, 'Month YYYY') as period_name,
    EXTRACT(MONTH FROM d)::INT as month_number,
    TO_CHAR(d, 'Month') as month_name,
    EXTRACT(QUARTER FROM d)::INT as quarter,
    'Q' || EXTRACT(QUARTER FROM d) || ' ' || EXTRACT(YEAR FROM d) as quarter_name,
    EXTRACT(YEAR FROM d)::INT as year,
    EXTRACT(YEAR FROM d)::INT as fiscal_year,
    EXTRACT(QUARTER FROM d)::INT as fiscal_quarter,
    d as first_day_of_period,
    (d + INTERVAL '1 month' - INTERVAL '1 day')::DATE as last_day_of_period,
    EXTRACT(DAY FROM d + INTERVAL '1 month' - INTERVAL '1 day')::INT as days_in_period,
    d = DATE_TRUNC('month', CURRENT_DATE)::DATE as is_current_period,
    d < DATE_TRUNC('month', CURRENT_DATE)::DATE as is_closed
FROM generate_series('2022-01-01'::DATE, '2025-12-31'::DATE, '1 month'::INTERVAL) as d;

-- ============================================
-- POPULATE ACCOUNT DIMENSION (Chart of Accounts)
-- ============================================
INSERT INTO dimensions.dim_account (account_id, account_name, account_type, account_subtype, is_balance_sheet, is_debit_normal, level_1, level_2, level_3, sort_order) VALUES
-- REVENUE
('4000', 'Product Revenue', 'Revenue', 'Operating', FALSE, FALSE, 'Total Revenue', 'Product Revenue', NULL, 100),
('4010', 'Hardware Revenue', 'Revenue', 'Operating', FALSE, FALSE, 'Total Revenue', 'Product Revenue', 'Hardware', 101),
('4020', 'Software Revenue', 'Revenue', 'Operating', FALSE, FALSE, 'Total Revenue', 'Product Revenue', 'Software', 102),
('4030', 'Subscription Revenue', 'Revenue', 'Operating', FALSE, FALSE, 'Total Revenue', 'Product Revenue', 'Subscription', 103),
('4100', 'Service Revenue', 'Revenue', 'Operating', FALSE, FALSE, 'Total Revenue', 'Service Revenue', NULL, 110),
('4110', 'Professional Services', 'Revenue', 'Operating', FALSE, FALSE, 'Total Revenue', 'Service Revenue', 'Professional Services', 111),
('4120', 'Support & Maintenance', 'Revenue', 'Operating', FALSE, FALSE, 'Total Revenue', 'Service Revenue', 'Support', 112),
('4200', 'Other Revenue', 'Revenue', 'Non-Operating', FALSE, FALSE, 'Total Revenue', 'Other Revenue', NULL, 120),

-- COST OF GOODS SOLD
('5000', 'Cost of Goods Sold', 'Expense', 'COGS', FALSE, TRUE, 'Cost of Goods Sold', NULL, NULL, 200),
('5010', 'Direct Materials', 'Expense', 'COGS', FALSE, TRUE, 'Cost of Goods Sold', 'Direct Costs', 'Materials', 201),
('5020', 'Direct Labor', 'Expense', 'COGS', FALSE, TRUE, 'Cost of Goods Sold', 'Direct Costs', 'Labor', 202),
('5030', 'Manufacturing Overhead', 'Expense', 'COGS', FALSE, TRUE, 'Cost of Goods Sold', 'Direct Costs', 'Overhead', 203),
('5100', 'Cost of Services', 'Expense', 'COS', FALSE, TRUE, 'Cost of Goods Sold', 'Cost of Services', NULL, 210),

-- OPERATING EXPENSES
('6000', 'Sales & Marketing', 'Expense', 'OpEx', FALSE, TRUE, 'Operating Expenses', 'Sales & Marketing', NULL, 300),
('6010', 'Sales Compensation', 'Expense', 'OpEx', FALSE, TRUE, 'Operating Expenses', 'Sales & Marketing', 'Compensation', 301),
('6020', 'Marketing Programs', 'Expense', 'OpEx', FALSE, TRUE, 'Operating Expenses', 'Sales & Marketing', 'Programs', 302),
('6030', 'Travel & Entertainment', 'Expense', 'OpEx', FALSE, TRUE, 'Operating Expenses', 'Sales & Marketing', 'T&E', 303),

('6100', 'Research & Development', 'Expense', 'OpEx', FALSE, TRUE, 'Operating Expenses', 'R&D', NULL, 310),
('6110', 'R&D Compensation', 'Expense', 'OpEx', FALSE, TRUE, 'Operating Expenses', 'R&D', 'Compensation', 311),
('6120', 'R&D Materials', 'Expense', 'OpEx', FALSE, TRUE, 'Operating Expenses', 'R&D', 'Materials', 312),

('6200', 'General & Administrative', 'Expense', 'OpEx', FALSE, TRUE, 'Operating Expenses', 'G&A', NULL, 320),
('6210', 'Executive Compensation', 'Expense', 'OpEx', FALSE, TRUE, 'Operating Expenses', 'G&A', 'Compensation', 321),
('6220', 'Facilities', 'Expense', 'OpEx', FALSE, TRUE, 'Operating Expenses', 'G&A', 'Facilities', 322),
('6230', 'Professional Fees', 'Expense', 'OpEx', FALSE, TRUE, 'Operating Expenses', 'G&A', 'Professional Fees', 323),
('6240', 'Insurance', 'Expense', 'OpEx', FALSE, TRUE, 'Operating Expenses', 'G&A', 'Insurance', 324),
('6250', 'Depreciation', 'Expense', 'OpEx', FALSE, TRUE, 'Operating Expenses', 'G&A', 'Depreciation', 325),

-- OTHER INCOME/EXPENSE
('7000', 'Interest Expense', 'Expense', 'Non-Operating', FALSE, TRUE, 'Other Income/Expense', 'Interest', 'Expense', 400),
('7100', 'Interest Income', 'Revenue', 'Non-Operating', FALSE, FALSE, 'Other Income/Expense', 'Interest', 'Income', 401),
('7200', 'Other Expense', 'Expense', 'Non-Operating', FALSE, TRUE, 'Other Income/Expense', 'Other', NULL, 410),

-- TAXES
('8000', 'Income Tax Expense', 'Expense', 'Tax', FALSE, TRUE, 'Income Tax', NULL, NULL, 500),

-- BALANCE SHEET - ASSETS
('1000', 'Cash and Equivalents', 'Asset', 'Current', TRUE, TRUE, 'Assets', 'Current Assets', 'Cash', 10),
('1100', 'Accounts Receivable', 'Asset', 'Current', TRUE, TRUE, 'Assets', 'Current Assets', 'Receivables', 11),
('1200', 'Inventory', 'Asset', 'Current', TRUE, TRUE, 'Assets', 'Current Assets', 'Inventory', 12),
('1300', 'Prepaid Expenses', 'Asset', 'Current', TRUE, TRUE, 'Assets', 'Current Assets', 'Prepaids', 13),
('1500', 'Property & Equipment', 'Asset', 'Fixed', TRUE, TRUE, 'Assets', 'Fixed Assets', 'PP&E', 15),
('1600', 'Accumulated Depreciation', 'Asset', 'Fixed', TRUE, FALSE, 'Assets', 'Fixed Assets', 'Accum Depr', 16),

-- BALANCE SHEET - LIABILITIES
('2000', 'Accounts Payable', 'Liability', 'Current', TRUE, FALSE, 'Liabilities', 'Current Liabilities', 'Payables', 20),
('2100', 'Accrued Expenses', 'Liability', 'Current', TRUE, FALSE, 'Liabilities', 'Current Liabilities', 'Accruals', 21),
('2200', 'Deferred Revenue', 'Liability', 'Current', TRUE, FALSE, 'Liabilities', 'Current Liabilities', 'Deferred Rev', 22),
('2500', 'Long-term Debt', 'Liability', 'Long-term', TRUE, FALSE, 'Liabilities', 'Long-term Liabilities', 'Debt', 25),

-- BALANCE SHEET - EQUITY
('3000', 'Common Stock', 'Equity', 'Equity', TRUE, FALSE, 'Equity', 'Contributed Capital', NULL, 30),
('3100', 'Retained Earnings', 'Equity', 'Equity', TRUE, FALSE, 'Equity', 'Retained Earnings', NULL, 31);

-- ============================================
-- POPULATE COST CENTER DIMENSION
-- ============================================
INSERT INTO dimensions.dim_cost_center (cost_center_id, cost_center_name, department, division, business_unit, region, is_active) VALUES
('CC100', 'Corporate Headquarters', 'Executive', 'Corporate', 'Corporate', 'All', TRUE),
('CC110', 'Finance', 'Finance', 'Corporate', 'Corporate', 'All', TRUE),
('CC120', 'HR', 'Human Resources', 'Corporate', 'Corporate', 'All', TRUE),
('CC130', 'IT', 'Information Technology', 'Corporate', 'Corporate', 'All', TRUE),

('CC200', 'Sales - Northeast', 'Sales', 'Sales', 'Commercial', 'Northeast', TRUE),
('CC210', 'Sales - Southeast', 'Sales', 'Sales', 'Commercial', 'Southeast', TRUE),
('CC220', 'Sales - Central', 'Sales', 'Sales', 'Commercial', 'Central', TRUE),
('CC230', 'Sales - West', 'Sales', 'Sales', 'Commercial', 'West', TRUE),

('CC300', 'Marketing', 'Marketing', 'Sales', 'Commercial', 'All', TRUE),

('CC400', 'Engineering', 'R&D', 'Engineering', 'R&D', 'All', TRUE),
('CC410', 'Product Development', 'R&D', 'Engineering', 'R&D', 'All', TRUE),
('CC420', 'Quality Assurance', 'R&D', 'Engineering', 'R&D', 'All', TRUE),

('CC500', 'Manufacturing', 'Operations', 'Operations', 'Operations', 'All', TRUE),
('CC510', 'Assembly', 'Operations', 'Operations', 'Operations', 'All', TRUE),
('CC520', 'Logistics', 'Operations', 'Operations', 'Operations', 'All', TRUE),

('CC600', 'Customer Support', 'Support', 'Services', 'Services', 'All', TRUE),
('CC610', 'Professional Services', 'Services', 'Services', 'Services', 'All', TRUE);

-- ============================================
-- POPULATE ENTITY DIMENSION
-- ============================================
INSERT INTO dimensions.dim_entity (entity_id, entity_name, entity_type, country, currency_code, consolidation_method, is_active) VALUES
('E100', 'DataBridge Corp', 'Operating', 'USA', 'USD', 'Full', TRUE),
('E110', 'DataBridge East', 'Operating', 'USA', 'USD', 'Full', TRUE),
('E120', 'DataBridge West', 'Operating', 'USA', 'USD', 'Full', TRUE),
('E200', 'DataBridge Canada', 'Operating', 'Canada', 'CAD', 'Full', TRUE),
('E300', 'DataBridge UK', 'Operating', 'UK', 'GBP', 'Full', TRUE),
('E999', 'Eliminations', 'Eliminated', 'USA', 'USD', 'Full', TRUE);

UPDATE dimensions.dim_entity SET parent_entity_id = 'E100' WHERE entity_id IN ('E110', 'E120', 'E200', 'E300');
UPDATE dimensions.dim_entity SET is_eliminating_entity = TRUE WHERE entity_id = 'E999';

-- ============================================
-- POPULATE BUDGET VERSION DIMENSION
-- ============================================
INSERT INTO dimensions.dim_budget_version (version_id, version_name, version_type, effective_date, is_current) VALUES
('BUD-2024-ORIG', '2024 Original Budget', 'Original', '2023-11-01', FALSE),
('BUD-2024-Q1', '2024 Q1 Forecast', 'Forecast', '2024-04-01', FALSE),
('BUD-2024-Q2', '2024 Q2 Forecast', 'Forecast', '2024-07-01', FALSE),
('BUD-2024-Q3', '2024 Q3 Forecast', 'Forecast', '2024-10-01', TRUE),
('BUD-2025-ORIG', '2025 Original Budget', 'Original', '2024-11-01', FALSE);

-- ============================================
-- POPULATE PRODUCT DIMENSION
-- ============================================
INSERT INTO dimensions.dim_product (product_id, product_name, product_category, product_subcategory, product_line, standard_cost, standard_price, is_active) VALUES
('HW-SRV-001', 'Enterprise Server X1', 'Hardware', 'Servers', 'Enterprise', 2500.00, 4500.00, TRUE),
('HW-SRV-002', 'Enterprise Server X2', 'Hardware', 'Servers', 'Enterprise', 4000.00, 7500.00, TRUE),
('HW-WKS-001', 'Workstation Pro', 'Hardware', 'Workstations', 'Professional', 1200.00, 2200.00, TRUE),
('HW-NET-001', 'Network Switch 48P', 'Hardware', 'Networking', 'Infrastructure', 800.00, 1500.00, TRUE),
('SW-ERP-001', 'ERP Suite - Standard', 'Software', 'ERP', 'Enterprise', 0.00, 50000.00, TRUE),
('SW-ERP-002', 'ERP Suite - Professional', 'Software', 'ERP', 'Enterprise', 0.00, 100000.00, TRUE),
('SW-BI-001', 'Analytics Platform', 'Software', 'Analytics', 'Professional', 0.00, 25000.00, TRUE),
('SVC-IMP-001', 'Implementation Services', 'Services', 'Professional', 'Services', 150.00, 250.00, TRUE),
('SVC-SUP-001', 'Annual Support', 'Services', 'Support', 'Services', 0.00, 5000.00, TRUE),
('SVC-TRN-001', 'Training Package', 'Services', 'Training', 'Services', 500.00, 2000.00, TRUE);

-- ============================================
-- POPULATE CUSTOMER DIMENSION
-- ============================================
INSERT INTO dimensions.dim_customer (customer_id, customer_name, customer_type, industry, segment, region, country, is_active, effective_from, is_current)
SELECT
    'CUST-' || LPAD(n::TEXT, 4, '0'),
    'Customer ' || n,
    (ARRAY['Enterprise', 'SMB', 'Consumer'])[1 + (n % 3)],
    (ARRAY['Technology', 'Manufacturing', 'Healthcare', 'Finance', 'Retail', 'Energy'])[1 + (n % 6)],
    (ARRAY['Strategic', 'Growth', 'Emerging'])[1 + (n % 3)],
    (ARRAY['Northeast', 'Southeast', 'Central', 'West'])[1 + (n % 4)],
    'USA',
    TRUE,
    '2022-01-01'::DATE,
    TRUE
FROM generate_series(1, 200) as n;

-- ============================================
-- POPULATE GEOGRAPHY DIMENSION
-- ============================================
INSERT INTO dimensions.dim_geography (geo_id, region, country, state, city) VALUES
('GEO-NE-NY', 'Northeast', 'USA', 'New York', 'New York City'),
('GEO-NE-MA', 'Northeast', 'USA', 'Massachusetts', 'Boston'),
('GEO-SE-FL', 'Southeast', 'USA', 'Florida', 'Miami'),
('GEO-SE-GA', 'Southeast', 'USA', 'Georgia', 'Atlanta'),
('GEO-CE-TX', 'Central', 'USA', 'Texas', 'Dallas'),
('GEO-CE-IL', 'Central', 'USA', 'Illinois', 'Chicago'),
('GEO-WE-CA', 'West', 'USA', 'California', 'San Francisco'),
('GEO-WE-WA', 'West', 'USA', 'Washington', 'Seattle');

-- ============================================
-- GENERATE GL BALANCE DATA
-- ============================================
-- Generate monthly GL balances for 2022-2024
INSERT INTO facts.fact_gl_balance (period_key, account_key, cost_center_key, entity_key, period_debit, period_credit, beginning_balance, ending_balance, ytd_debit, ytd_credit)
SELECT
    p.period_key,
    a.account_key,
    cc.cost_center_key,
    e.entity_key,
    -- Generate realistic amounts with seasonality
    CASE
        WHEN a.account_type = 'Revenue' THEN 0
        ELSE ROUND((RANDOM() * 100000 + 50000) *
             (1 + 0.1 * SIN(p.month_number * 3.14159 / 6)) *  -- Seasonality
             (1 + 0.05 * (p.year - 2022)))::NUMERIC, 2)      -- Growth
    END as period_debit,
    CASE
        WHEN a.account_type = 'Revenue' THEN
            ROUND((RANDOM() * 200000 + 100000) *
             (1 + 0.15 * SIN(p.month_number * 3.14159 / 6)) *
             (1 + 0.08 * (p.year - 2022)))::NUMERIC, 2)
        ELSE 0
    END as period_credit,
    0 as beginning_balance,
    0 as ending_balance,
    0 as ytd_debit,
    0 as ytd_credit
FROM dimensions.dim_period p
CROSS JOIN dimensions.dim_account a
CROSS JOIN dimensions.dim_cost_center cc
CROSS JOIN dimensions.dim_entity e
WHERE p.year BETWEEN 2022 AND 2024
  AND a.is_balance_sheet = FALSE
  AND e.entity_type = 'Operating'
  AND (
    (a.account_type = 'Revenue' AND cc.department IN ('Sales', 'Services'))
    OR (a.account_type = 'Expense' AND a.level_2 = 'Sales & Marketing' AND cc.department = 'Sales')
    OR (a.account_type = 'Expense' AND a.level_2 = 'R&D' AND cc.department = 'R&D')
    OR (a.account_type = 'Expense' AND a.level_2 = 'G&A' AND cc.department IN ('Executive', 'Finance', 'Human Resources', 'Information Technology'))
    OR (a.account_type = 'Expense' AND a.account_subtype = 'COGS' AND cc.department = 'Operations')
  )
LIMIT 50000;  -- Limit for reasonable data size

-- ============================================
-- GENERATE BUDGET DATA
-- ============================================
INSERT INTO facts.fact_budget (period_key, account_key, cost_center_key, entity_key, version_key, budget_amount, prior_year_actual)
SELECT
    p.period_key,
    a.account_key,
    cc.cost_center_key,
    e.entity_key,
    v.version_key,
    -- Budget is typically planned before actuals, so use similar logic
    ROUND(
        CASE
            WHEN a.account_type = 'Revenue' THEN
                (RANDOM() * 180000 + 90000) *
                (1 + 0.12 * SIN(p.month_number * 3.14159 / 6)) *
                (1 + 0.10 * (p.year - 2023))
            ELSE
                (RANDOM() * 90000 + 45000) *
                (1 + 0.08 * SIN(p.month_number * 3.14159 / 6)) *
                (1 + 0.06 * (p.year - 2023))
        END
    ::NUMERIC, 2) as budget_amount,
    0 as prior_year_actual
FROM dimensions.dim_period p
CROSS JOIN dimensions.dim_account a
CROSS JOIN dimensions.dim_cost_center cc
CROSS JOIN dimensions.dim_entity e
CROSS JOIN dimensions.dim_budget_version v
WHERE p.year IN (2024, 2025)
  AND v.version_type = 'Original'
  AND a.is_balance_sheet = FALSE
  AND e.entity_type = 'Operating'
  AND (
    (a.account_type = 'Revenue' AND cc.department IN ('Sales', 'Services'))
    OR (a.account_type = 'Expense')
  )
LIMIT 20000;

-- ============================================
-- SUMMARY STATISTICS
-- ============================================
DO $$
BEGIN
    RAISE NOTICE 'Sample Data Load Complete';
    RAISE NOTICE '-------------------------';
    RAISE NOTICE 'dim_date: % rows', (SELECT COUNT(*) FROM dimensions.dim_date);
    RAISE NOTICE 'dim_period: % rows', (SELECT COUNT(*) FROM dimensions.dim_period);
    RAISE NOTICE 'dim_account: % rows', (SELECT COUNT(*) FROM dimensions.dim_account);
    RAISE NOTICE 'dim_cost_center: % rows', (SELECT COUNT(*) FROM dimensions.dim_cost_center);
    RAISE NOTICE 'dim_entity: % rows', (SELECT COUNT(*) FROM dimensions.dim_entity);
    RAISE NOTICE 'dim_customer: % rows', (SELECT COUNT(*) FROM dimensions.dim_customer);
    RAISE NOTICE 'dim_product: % rows', (SELECT COUNT(*) FROM dimensions.dim_product);
    RAISE NOTICE 'fact_gl_balance: % rows', (SELECT COUNT(*) FROM facts.fact_gl_balance);
    RAISE NOTICE 'fact_budget: % rows', (SELECT COUNT(*) FROM facts.fact_budget);
END $$;
