/**
 * DataBridge AI — Autonomous Demo Engine
 * Fully client-side demo system with embedded sample data.
 * Each scenario is a scripted walkthrough that showcases real capabilities.
 */
const DemoEngine = (function () {
    'use strict';

    let speedMultiplier = 1;
    let isRunning = false;
    let shouldCancel = false;

    // ================================================================
    // EMBEDDED SAMPLE DATA
    // ================================================================

    const DEMO_DATA = {
        source_a: {
            headers: ['customer_id', 'name', 'email', 'address', 'phone', 'status'],
            rows: [
                ['101', 'John Smith', 'john.smith@email.com', '123 Main St, Dallas TX', '214-555-0101', 'Active'],
                ['102', 'Jane Doe', 'jane.doe@email.com', '456 Oak Ave, Houston TX', '713-555-0202', 'Active'],
                ['103', 'Peter Jones', 'peter.j@email.com', '789 Pine Rd, Austin TX', '512-555-0303', 'Active'],
                ['104', 'Mary Brown', 'mary.b@email.com', '321 Elm St, Denver CO', '303-555-0404', 'Inactive']
            ]
        },
        source_b: {
            headers: ['customer_id', 'name', 'email', 'address', 'phone', 'status'],
            rows: [
                ['101', 'Jon Smith', 'john.smith@email.com', '123 Main Street, Dallas TX', '214-555-0101', 'Active'],
                ['102', 'Jane D.', 'jane.doe@email.com', '456 Oak Avenue, Houston TX', '713-555-0202', 'Active'],
                ['105', 'Sarah Wilson', 'sarah.w@email.com', '555 Birch Ln, Seattle WA', '206-555-0505', 'Active'],
                ['106', 'Tom Garcia', 'tom.g@email.com', '777 Cedar Dr, Portland OR', '503-555-0606', 'Active']
            ]
        },
        pizza_orders: {
            headers: ['order_id', 'customer_name', 'pizza_type', 'size', 'price', 'order_date', 'delivery'],
            rows: [
                ['P001', 'Alice Cooper', 'Margherita', 'Large', '18.99', '2024-01-15', 'Yes'],
                ['P002', 'Bob Dylan', 'Pepperoni', 'Medium', '14.99', '2024-01-15', 'No'],
                ['P003', 'Carol King', 'Hawaiian', 'Small', '10.99', '2024-01-16', 'Yes'],
                ['P004', 'Dave Clark', 'Veggie Supreme', 'Large', '20.99', '2024-01-16', 'Yes'],
                ['P005', 'Eve Stone', 'BBQ Chicken', 'Medium', '16.99', '2024-01-17', 'No'],
                ['P006', null, 'Pepperoni', 'Large', '18.99', '2024-01-17', 'Yes'],
                ['P007', 'Grace Lee', 'Margherita', 'Small', '9.99', '2024-01-18', 'No'],
                ['P008', 'Hank Hill', 'Meat Lovers', 'Large', '22.99', '2024-01-18', 'Yes'],
                ['P009', 'Iris Chen', 'Four Cheese', 'Medium', '15.99', '2024-01-19', 'No'],
                ['P010', 'Jack Black', 'Pepperoni', 'Large', '18.99', '2024-01-19', 'Yes'],
                ['P011', 'Kate Moss', 'Hawaiian', 'Medium', '13.99', '2024-01-20', 'Yes'],
                ['P012', 'Leo Park', 'BBQ Chicken', null, '16.99', '2024-01-20', 'No'],
                ['P013', 'Mia Fox', 'Veggie Supreme', 'Small', '11.99', '2024-01-21', 'Yes'],
                ['P014', 'Nate Gold', 'Margherita', 'Large', '18.99', '2024-01-21', 'No'],
                ['P015', 'Olga Ray', 'Meat Lovers', 'Large', '28.99', '2024-01-22', 'Yes']
            ]
        },
        pl_hierarchy: {
            headers: ['node_id', 'name', 'parent_id', 'level', 'formula_type', 'gl_account', 'description', 'sort_order', 'is_leaf'],
            rows: [
                ['PL001', 'Income Statement', null, '1', null, null, 'Top-level P&L', '1', 'N'],
                ['PL010', 'Revenue', 'PL001', '2', 'SUM', null, 'Total Revenue', '1', 'N'],
                ['PL011', 'Product Revenue', 'PL010', '3', null, '4000', 'Product sales', '1', 'Y'],
                ['PL012', 'Service Revenue', 'PL010', '3', null, '4100', 'Service income', '2', 'Y'],
                ['PL013', 'Other Revenue', 'PL010', '3', null, '4200', 'Misc revenue', '3', 'Y'],
                ['PL020', 'COGS', 'PL001', '2', 'SUM', null, 'Cost of Goods Sold', '2', 'N'],
                ['PL021', 'Material Cost', 'PL020', '3', null, '5000', 'Raw materials', '1', 'Y'],
                ['PL022', 'Labor Cost', 'PL020', '3', null, '5100', 'Direct labor', '2', 'Y'],
                ['PL023', 'Manufacturing OH', 'PL020', '3', null, '5200', 'Factory overhead', '3', 'Y'],
                ['PL030', 'Gross Profit', 'PL001', '2', 'SUBTRACT', null, 'Revenue - COGS', '3', 'N'],
                ['PL040', 'Operating Expenses', 'PL001', '2', 'SUM', null, 'Total OpEx', '4', 'N'],
                ['PL041', 'Salaries & Wages', 'PL040', '3', null, '6000', 'Employee compensation', '1', 'Y'],
                ['PL042', 'Rent & Utilities', 'PL040', '3', null, '6100', 'Facilities', '2', 'Y'],
                ['PL043', 'Marketing', 'PL040', '3', null, '6200', 'Advertising & promos', '3', 'Y'],
                ['PL044', 'Insurance', 'PL040', '3', null, '6300', 'Business insurance', '4', 'Y'],
                ['PL045', 'Depreciation', 'PL040', '3', null, '6400', 'Asset depreciation', '5', 'Y'],
                ['PL050', 'Operating Income', 'PL001', '2', 'SUBTRACT', null, 'Gross Profit - OpEx', '5', 'N'],
                ['PL060', 'Other Income/Expense', 'PL001', '2', 'SUM', null, 'Non-operating items', '6', 'N'],
                ['PL061', 'Interest Income', 'PL060', '3', null, '7000', 'Bank interest', '1', 'Y'],
                ['PL062', 'Interest Expense', 'PL060', '3', null, '7100', 'Loan interest', '2', 'Y']
            ]
        },
        gl_journal: {
            headers: ['txn_id', 'period', 'account_code', 'account_name', 'department', 'debit', 'credit', 'description'],
            rows: [
                ['GL001', '2024-01', '4000', 'Product Revenue', 'Sales', '0', '125000', 'Jan product sales'],
                ['GL002', '2024-01', '4100', 'Service Revenue', 'Consulting', '0', '45000', 'Jan consulting fees'],
                ['GL003', '2024-01', '5000', 'Material Cost', 'Production', '52000', '0', 'Jan raw materials'],
                ['GL004', '2024-01', '5100', 'Labor Cost', 'Production', '38000', '0', 'Jan direct labor'],
                ['GL005', '2024-01', '6000', 'Salaries', 'HR', '85000', '0', 'Jan payroll'],
                ['GL006', '2024-01', '6100', 'Rent', 'Facilities', '12000', '0', 'Jan office rent'],
                ['GL007', '2024-01', '6200', 'Marketing', 'Marketing', '18000', '0', 'Jan ad spend'],
                ['GL008', '2024-01', '6400', 'Depreciation', 'Finance', '5500', '0', 'Jan depreciation'],
                ['GL009', '2024-01', '7000', 'Interest Income', 'Treasury', '0', '2200', 'Jan bank interest'],
                ['GL010', '2024-02', '4000', 'Product Revenue', 'Sales', '0', '138000', 'Feb product sales'],
                ['GL011', '2024-02', '4100', 'Service Revenue', 'Consulting', '0', '51000', 'Feb consulting fees'],
                ['GL012', '2024-02', '5000', 'Material Cost', 'Production', '56000', '0', 'Feb raw materials'],
                ['GL013', '2024-02', '5100', 'Labor Cost', 'Production', '40000', '0', 'Feb direct labor'],
                ['GL014', '2024-02', '6000', 'Salaries', 'HR', '85000', '0', 'Feb payroll'],
                ['GL015', '2024-02', '6100', 'Rent', 'Facilities', '12000', '0', 'Feb office rent'],
                ['GL016', '2024-02', '6200', 'Marketing', 'Marketing', '22000', '0', 'Feb ad spend'],
                ['GL017', '2024-02', '6400', 'Depreciation', 'Finance', '5500', '0', 'Feb depreciation'],
                ['GL018', '2024-02', '7000', 'Interest Income', 'Treasury', '0', '2300', 'Feb bank interest'],
                ['GL019', '2024-03', '4000', 'Product Revenue', 'Sales', '0', '142000', 'Mar product sales'],
                ['GL020', '2024-03', '4100', 'Service Revenue', 'Consulting', '0', '48000', 'Mar consulting fees'],
                ['GL021', '2024-03', '5000', 'Material Cost', 'Production', '58000', '0', 'Mar raw materials'],
                ['GL022', '2024-03', '5100', 'Labor Cost', 'Production', '41000', '0', 'Mar direct labor'],
                ['GL023', '2024-03', '6000', 'Salaries', 'HR', '87000', '0', 'Mar payroll'],
                ['GL024', '2024-03', '6100', 'Rent', 'Facilities', '12000', '0', 'Mar office rent'],
                ['GL025', '2024-03', '6200', 'Marketing', 'Marketing', '20000', '0', 'Mar ad spend'],
                ['GL026', '2024-03', '6400', 'Depreciation', 'Finance', '5500', '0', 'Mar depreciation'],
                ['GL027', '2024-03', '7000', 'Interest Income', 'Treasury', '0', '2500', 'Mar bank interest']
            ]
        },
        oil_gas: {
            headers: ['well_id', 'well_name', 'operator', 'basin', 'period', 'gross_oil_bbl', 'gross_gas_mcf', 'net_oil_bbl', 'net_gas_mcf', 'oil_price', 'gas_price', 'gross_revenue', 'severance_tax', 'ad_valorem', 'loe', 'gathering', 'transportation', 'net_revenue', 'capex', 'noi', 'decline_rate'],
            rows: [
                ['W001', 'Eagle Ford #1', 'ABC Energy', 'Permian', '2024-01', '4500', '12000', '3600', '9600', '72.50', '2.85', '360450', '18022', '7209', '25000', '8400', '5600', '296219', '45000', '251219', '0.03'],
                ['W001', 'Eagle Ford #1', 'ABC Energy', 'Permian', '2024-02', '4365', '11640', '3492', '9312', '74.10', '2.92', '285839', '14292', '5717', '24500', '8200', '5500', '227630', '0', '227630', '0.03'],
                ['W002', 'Wolfcamp A-1', 'ABC Energy', 'Permian', '2024-01', '3800', '15200', '3040', '12160', '72.50', '2.85', '254980', '12749', '5100', '22000', '10640', '7100', '197391', '120000', '77391', '0.04'],
                ['W002', 'Wolfcamp A-1', 'ABC Energy', 'Permian', '2024-02', '3648', '14592', '2918', '11674', '74.10', '2.92', '250252', '12513', '5005', '21500', '10300', '6900', '194034', '0', '194034', '0.04'],
                ['W003', 'Bakken #7', 'ABC Energy', 'Anadarko', '2024-01', '4580', '8200', '3664', '6560', '72.50', '2.85', '284310', '14216', '5686', '20000', '5740', '3800', '234868', '0', '234868', '0.02'],
                ['W003', 'Bakken #7', 'ABC Energy', 'Anadarko', '2024-02', '4488', '8036', '3590', '6429', '74.10', '2.92', '284694', '14235', '5694', '19500', '5625', '3700', '235940', '0', '235940', '0.02'],
                ['W004', 'Three Forks #3', 'XYZ Oil', 'Williston', '2024-01', '5200', '18000', '4160', '14400', '72.50', '2.85', '342900', '17145', '6858', '28000', '12600', '8400', '269897', '85000', '184897', '0.05'],
                ['W004', 'Three Forks #3', 'XYZ Oil', 'Williston', '2024-02', '4940', '17100', '3952', '13680', '74.10', '2.92', '332831', '16642', '6657', '27000', '12000', '8000', '262532', '0', '262532', '0.05'],
                ['W005', 'Spraberry #12', 'XYZ Oil', 'Williston', '2024-01', '6400', '9800', '5120', '7840', '72.50', '2.85', '393540', '19677', '7871', '30000', '6860', '4600', '324532', '0', '324532', '0.03'],
                ['W005', 'Spraberry #12', 'XYZ Oil', 'Williston', '2024-02', '6208', '9506', '4966', '7605', '74.10', '2.92', '390009', '19500', '7800', '29000', '6670', '4450', '322589', '0', '322589', '0.03']
            ]
        }
    };

    // ================================================================
    // DESCRIPTIONS
    // ================================================================

    const DESCRIPTIONS = {
        reconciliation: 'Reconcile customer records from two source systems. Fuzzy-match names, detect orphans, and generate a match report with similarity scores.',
        hierarchy: 'Build a complete P&L Income Statement hierarchy with 3 levels, SUM/SUBTRACT formulas, and GL account mappings from CSV data.',
        data_quality: 'Profile pizza order data, auto-detect quality issues (nulls, outliers), generate validation rules, and run a data quality assessment.',
        dbt_pipeline: 'Generate a complete dbt project from GL journal entries: sources, staging models, mart aggregations, and schema tests.',
        oil_gas: 'Analyze well economics across 5 wells and 2 operators: production profiles, revenue rollups, NOI calculations, and decline analysis.',
        full_workflow: 'End-to-end walkthrough combining reconciliation, hierarchy building, dbt pipeline, and data quality in a single orchestrated workflow.'
    };

    // ================================================================
    // RENDERING HELPERS
    // ================================================================

    function formatTable(headers, rows, options) {
        options = options || {};
        var maxRows = options.maxRows || 10;
        var displayRows = rows.slice(0, maxRows);
        var html = '<table class="demo-table"><thead><tr>';
        for (var i = 0; i < headers.length; i++) {
            html += '<th>' + esc(headers[i]) + '</th>';
        }
        html += '</tr></thead><tbody>';
        for (var r = 0; r < displayRows.length; r++) {
            html += '<tr>';
            for (var c = 0; c < displayRows[r].length; c++) {
                var val = displayRows[r][c];
                if (val === null || val === undefined || val === '') {
                    html += '<td class="null-cell">NULL</td>';
                } else {
                    var cls = '';
                    if (options.colorCol && options.colorCol[headers[c]]) {
                        cls = options.colorCol[headers[c]](val);
                    }
                    html += '<td' + (cls ? ' class="' + cls + '"' : '') + '>' + esc(String(val)) + '</td>';
                }
            }
            html += '</tr>';
        }
        html += '</tbody></table>';
        if (rows.length > maxRows) {
            html += '<div style="color:var(--text-dim);font-size:0.78rem;margin-top:4px;">... and ' + (rows.length - maxRows) + ' more rows</div>';
        }
        return html;
    }

    function formatToolCall(name, params) {
        return '<div class="demo-tool-call">' +
            '<div class="tool-name">' + esc(name) + '()</div>' +
            '<div class="tool-params">' + esc(JSON.stringify(params, null, 2)) + '</div>' +
            '</div>';
    }

    function formatCodeBlock(code, lang) {
        return '<div class="demo-code">' +
            (lang ? '<div style="color:var(--text-dim);font-size:0.75rem;margin-bottom:4px;">' + esc(lang) + '</div>' : '') +
            esc(code) +
            '</div>';
    }

    function formatTree(nodes, prefix) {
        prefix = prefix || '';
        var html = '';
        for (var i = 0; i < nodes.length; i++) {
            var isLast = (i === nodes.length - 1);
            var connector = isLast ? '\u2514\u2500\u2500 ' : '\u251C\u2500\u2500 ';
            var childPrefix = prefix + (isLast ? '    ' : '\u2502   ');
            html += prefix + connector + esc(nodes[i].name);
            if (nodes[i].meta) html += ' <span style="color:var(--text-dim)">(' + esc(nodes[i].meta) + ')</span>';
            html += '\n';
            if (nodes[i].children && nodes[i].children.length > 0) {
                html += formatTree(nodes[i].children, childPrefix);
            }
        }
        return html;
    }

    function formatDiffTable(comparisons) {
        var html = '<table class="demo-table"><thead><tr><th>Field</th><th>Source A</th><th>Source B</th><th>Score</th><th>Status</th></tr></thead><tbody>';
        for (var i = 0; i < comparisons.length; i++) {
            var c = comparisons[i];
            var cls = c.score >= 95 ? 'demo-match-good' : (c.score >= 70 ? 'demo-match-fuzzy' : 'demo-match-bad');
            var label = c.score >= 95 ? 'Exact' : (c.score >= 70 ? 'Fuzzy' : 'Mismatch');
            html += '<tr><td>' + esc(c.field) + '</td><td>' + esc(c.a) + '</td><td>' + esc(c.b) + '</td>' +
                '<td class="' + cls + '">' + c.score + '%</td><td class="' + cls + '">' + label + '</td></tr>';
        }
        html += '</tbody></table>';
        return html;
    }

    function formatStats(stats) {
        var html = '<div style="display:flex;flex-wrap:wrap;gap:8px;margin:8px 0;">';
        for (var i = 0; i < stats.length; i++) {
            html += '<span class="demo-stat-highlight">' + esc(stats[i].label) + ': ' + esc(stats[i].value) + '</span>';
        }
        html += '</div>';
        return html;
    }

    function levenshtein(a, b) {
        a = a.toLowerCase();
        b = b.toLowerCase();
        var matrix = [];
        for (var i = 0; i <= b.length; i++) matrix[i] = [i];
        for (var j = 0; j <= a.length; j++) matrix[0][j] = j;
        for (var i = 1; i <= b.length; i++) {
            for (var j = 1; j <= a.length; j++) {
                if (b.charAt(i - 1) === a.charAt(j - 1)) {
                    matrix[i][j] = matrix[i - 1][j - 1];
                } else {
                    matrix[i][j] = Math.min(
                        matrix[i - 1][j - 1] + 1,
                        matrix[i][j - 1] + 1,
                        matrix[i - 1][j] + 1
                    );
                }
            }
        }
        return matrix[b.length][a.length];
    }

    function similarity(a, b) {
        if (!a || !b) return 0;
        var maxLen = Math.max(a.length, b.length);
        if (maxLen === 0) return 100;
        return Math.round((1 - levenshtein(a, b) / maxLen) * 100);
    }

    function computeProfile(data) {
        var profile = [];
        for (var c = 0; c < data.headers.length; c++) {
            var col = data.headers[c];
            var vals = [];
            var nullCount = 0;
            var distinct = {};
            for (var r = 0; r < data.rows.length; r++) {
                var v = data.rows[r][c];
                if (v === null || v === undefined || v === '') {
                    nullCount++;
                } else {
                    vals.push(v);
                    distinct[v] = true;
                }
            }
            var numVals = vals.filter(function (v) { return !isNaN(parseFloat(v)); }).map(parseFloat);
            profile.push({
                column: col,
                total: data.rows.length,
                nulls: nullCount,
                completeness: Math.round((1 - nullCount / data.rows.length) * 100) + '%',
                distinct: Object.keys(distinct).length,
                type: numVals.length === vals.length && vals.length > 0 ? 'numeric' : 'string',
                min: numVals.length > 0 ? Math.min.apply(null, numVals) : vals.sort()[0] || '-',
                max: numVals.length > 0 ? Math.max.apply(null, numVals) : vals.sort()[vals.length - 1] || '-'
            });
        }
        return profile;
    }

    function esc(s) {
        if (s === null || s === undefined) return '';
        return String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    function num(n) {
        return Number(n).toLocaleString();
    }

    // ================================================================
    // SCENARIO DEFINITIONS
    // ================================================================

    var SCENARIOS = {};

    // ----- Scenario 1: Data Reconciliation -----
    SCENARIOS.reconciliation = function () {
        var sa = DEMO_DATA.source_a;
        var sb = DEMO_DATA.source_b;
        var profA = computeProfile(sa);
        var profB = computeProfile(sb);

        // Compute fuzzy matches for shared IDs
        var nameSim101 = similarity(sa.rows[0][1], sb.rows[0][1]);
        var addrSim101 = similarity(sa.rows[0][3], sb.rows[0][3]);
        var nameSim102 = similarity(sa.rows[1][1], sb.rows[1][1]);
        var addrSim102 = similarity(sa.rows[1][3], sb.rows[1][3]);

        return [
            {
                type: 'system', from: 'System', to: '',
                content: '<strong>Autonomous Demo: Data Reconciliation</strong><br>Comparing customer records across two source systems to find matches, fuzzy matches, and orphans.',
                delay: 800
            },
            {
                type: 'request', from: 'user', to: 'orchestrator',
                content: 'Reconcile customer records from System A and System B. Find matches, near-matches, and orphan records.',
                delay: 1000
            },
            {
                type: 'thinking', from: 'orchestrator', to: 'orchestrator',
                content: 'Detected 2 CSV sources with matching schema (6 columns each). Key field: <code>customer_id</code>. Planning 4-phase reconciliation: Load &rarr; Profile &rarr; Fuzzy Match &rarr; Orphan Detection.',
                delay: 1500
            },
            {
                type: 'plan', from: 'orchestrator', to: 'orchestrator',
                content: '<strong>Execution Plan:</strong><ol>' +
                    '<li>Load both source datasets</li>' +
                    '<li>Profile columns — completeness, distinct values, data types</li>' +
                    '<li>Fuzzy match on <code>name</code> and <code>address</code> (Levenshtein)</li>' +
                    '<li>Detect orphan records (IDs in one source only)</li></ol>',
                delay: 1200
            },
            {
                type: 'tool', from: 'orchestrator', to: 'data_agent',
                content: formatToolCall('load_csv', { file: 'source_a.csv', encoding: 'utf-8' }),
                delay: 1500
            },
            {
                type: 'result', from: 'data_agent', to: 'orchestrator',
                content: '<strong>Source A loaded</strong> &mdash; 4 rows, 6 columns' +
                    formatTable(sa.headers, sa.rows),
                delay: 1800
            },
            {
                type: 'tool', from: 'orchestrator', to: 'data_agent',
                content: formatToolCall('load_csv', { file: 'source_b.csv', encoding: 'utf-8' }),
                delay: 1200
            },
            {
                type: 'result', from: 'data_agent', to: 'orchestrator',
                content: '<strong>Source B loaded</strong> &mdash; 4 rows, 6 columns' +
                    formatTable(sb.headers, sb.rows),
                delay: 1800
            },
            {
                type: 'tool', from: 'orchestrator', to: 'data_agent',
                content: formatToolCall('profile_data', { sources: ['source_a', 'source_b'], columns: 'all' }),
                delay: 1500
            },
            {
                type: 'result', from: 'data_agent', to: 'orchestrator',
                content: function () {
                    var profileHeaders = ['Column', 'Nulls (A)', 'Nulls (B)', 'Distinct (A)', 'Distinct (B)', 'Type'];
                    var profileRows = [];
                    for (var i = 0; i < profA.length; i++) {
                        profileRows.push([profA[i].column, profA[i].nulls, profB[i].nulls, profA[i].distinct, profB[i].distinct, profA[i].type]);
                    }
                    return '<strong>Profile Comparison</strong>' + formatTable(profileHeaders, profileRows) +
                        formatStats([
                            { label: 'Source A rows', value: '4' },
                            { label: 'Source B rows', value: '4' },
                            { label: 'Common IDs', value: '2 (101, 102)' },
                            { label: 'Schema match', value: '100%' }
                        ]);
                },
                delay: 2000
            },
            {
                type: 'tool', from: 'orchestrator', to: 'data_agent',
                content: formatToolCall('fuzzy_match_columns', { columns: ['name', 'address'], threshold: 0.7, algorithm: 'levenshtein' }),
                delay: 1500
            },
            {
                type: 'result', from: 'data_agent', to: 'orchestrator',
                content: function () {
                    return '<strong>Fuzzy Match Results (ID 101)</strong>' +
                        formatDiffTable([
                            { field: 'name', a: 'John Smith', b: 'Jon Smith', score: nameSim101 },
                            { field: 'email', a: 'john.smith@email.com', b: 'john.smith@email.com', score: 100 },
                            { field: 'address', a: '123 Main St, Dallas TX', b: '123 Main Street, Dallas TX', score: addrSim101 },
                            { field: 'phone', a: '214-555-0101', b: '214-555-0101', score: 100 }
                        ]) +
                        '<strong>Fuzzy Match Results (ID 102)</strong>' +
                        formatDiffTable([
                            { field: 'name', a: 'Jane Doe', b: 'Jane D.', score: nameSim102 },
                            { field: 'email', a: 'jane.doe@email.com', b: 'jane.doe@email.com', score: 100 },
                            { field: 'address', a: '456 Oak Ave, Houston TX', b: '456 Oak Avenue, Houston TX', score: addrSim102 },
                            { field: 'phone', a: '713-555-0202', b: '713-555-0202', score: 100 }
                        ]);
                },
                delay: 2200
            },
            {
                type: 'tool', from: 'orchestrator', to: 'data_agent',
                content: formatToolCall('compare_hashes', { key: 'customer_id', detect_orphans: true }),
                delay: 1200
            },
            {
                type: 'response', from: 'orchestrator', to: 'user',
                content: '<strong>Reconciliation Complete</strong>' +
                    formatStats([
                        { label: 'Exact Matches', value: '0' },
                        { label: 'Fuzzy Matches', value: '2 (IDs 101, 102)' },
                        { label: 'Orphans in A', value: '2 (IDs 103, 104)' },
                        { label: 'Orphans in B', value: '2 (IDs 105, 106)' }
                    ]) +
                    '<br><strong>Recommendations:</strong><ul>' +
                    '<li>IDs 101 &amp; 102: Review fuzzy matches &mdash; name variations detected</li>' +
                    '<li>IDs 103, 104: Exist only in System A &mdash; verify if migrated</li>' +
                    '<li>IDs 105, 106: Exist only in System B &mdash; new customers or data entry gap</li></ul>',
                delay: 1500
            }
        ];
    };

    // ----- Scenario 2: Financial Hierarchy Builder -----
    SCENARIOS.hierarchy = function () {
        var pl = DEMO_DATA.pl_hierarchy;

        var tree = [
            {
                name: 'Income Statement', meta: 'PL001', children: [
                    {
                        name: 'Revenue', meta: 'SUM', children: [
                            { name: 'Product Revenue', meta: 'GL 4000' },
                            { name: 'Service Revenue', meta: 'GL 4100' },
                            { name: 'Other Revenue', meta: 'GL 4200' }
                        ]
                    },
                    {
                        name: 'COGS', meta: 'SUM', children: [
                            { name: 'Material Cost', meta: 'GL 5000' },
                            { name: 'Labor Cost', meta: 'GL 5100' },
                            { name: 'Manufacturing OH', meta: 'GL 5200' }
                        ]
                    },
                    { name: 'Gross Profit', meta: 'Revenue - COGS' },
                    {
                        name: 'Operating Expenses', meta: 'SUM', children: [
                            { name: 'Salaries & Wages', meta: 'GL 6000' },
                            { name: 'Rent & Utilities', meta: 'GL 6100' },
                            { name: 'Marketing', meta: 'GL 6200' },
                            { name: 'Insurance', meta: 'GL 6300' },
                            { name: 'Depreciation', meta: 'GL 6400' }
                        ]
                    },
                    { name: 'Operating Income', meta: 'Gross Profit - OpEx' },
                    {
                        name: 'Other Income/Expense', meta: 'SUM', children: [
                            { name: 'Interest Income', meta: 'GL 7000' },
                            { name: 'Interest Expense', meta: 'GL 7100' }
                        ]
                    }
                ]
            }
        ];

        return [
            {
                type: 'system', from: 'System', to: '',
                content: '<strong>Autonomous Demo: Financial Hierarchy Builder</strong><br>Building a P&L Income Statement hierarchy with formulas and GL account mappings.',
                delay: 800
            },
            {
                type: 'request', from: 'user', to: 'hierarchy_builder',
                content: 'Build a complete P&L hierarchy from income statement CSV data with SUM/SUBTRACT formulas.',
                delay: 1000
            },
            {
                type: 'thinking', from: 'hierarchy_builder', to: 'hierarchy_builder',
                content: 'Analyzing CSV structure &mdash; 20 rows, 9 columns, 3 hierarchy levels detected. Node IDs follow PL### pattern. Formula types: SUM (5 nodes), SUBTRACT (2 nodes). GL accounts mapped on leaf nodes.',
                delay: 1500
            },
            {
                type: 'plan', from: 'hierarchy_builder', to: 'hierarchy_builder',
                content: '<strong>Build Plan:</strong><ol>' +
                    '<li>Create hierarchy project "Income Statement Demo"</li>' +
                    '<li>Import 20 nodes from CSV (Tier 3 format)</li>' +
                    '<li>Validate formula chains (SUM/SUBTRACT)</li>' +
                    '<li>Visualize hierarchy tree</li></ol>',
                delay: 1200
            },
            {
                type: 'tool', from: 'hierarchy_builder', to: 'hierarchy_builder',
                content: formatToolCall('create_hierarchy_project', { name: 'Income Statement Demo', description: 'P&L hierarchy with 3 levels and formula chains' }),
                delay: 1500
            },
            {
                type: 'result', from: 'hierarchy_builder', to: 'hierarchy_builder',
                content: formatStats([
                    { label: 'Project ID', value: 'PRJ-001' },
                    { label: 'Status', value: 'Created' },
                    { label: 'Name', value: 'Income Statement Demo' }
                ]),
                delay: 1200
            },
            {
                type: 'tool', from: 'hierarchy_builder', to: 'hierarchy_builder',
                content: formatToolCall('import_flexible_hierarchy', {
                    project: 'PRJ-001',
                    format: 'tier3',
                    columns: ['node_id', 'name', 'parent_id', 'level', 'formula_type', 'gl_account', 'description', 'sort_order', 'is_leaf'],
                    row_count: 20
                }),
                delay: 1800
            },
            {
                type: 'result', from: 'hierarchy_builder', to: 'hierarchy_builder',
                content: function () {
                    return '<strong>20 hierarchy nodes imported across 3 levels</strong>' +
                        formatStats([
                            { label: 'Level 1', value: '1 node' },
                            { label: 'Level 2', value: '6 nodes' },
                            { label: 'Level 3', value: '13 leaf nodes' },
                            { label: 'GL Mappings', value: '13 accounts' }
                        ]) +
                        '<div class="demo-code" style="white-space:pre-wrap">' + formatTree(tree) + '</div>';
                },
                delay: 2200
            },
            {
                type: 'tool', from: 'hierarchy_builder', to: 'hierarchy_builder',
                content: formatToolCall('validate_project', { project: 'PRJ-001', check_formulas: true, check_orphans: true }),
                delay: 1500
            },
            {
                type: 'result', from: 'hierarchy_builder', to: 'hierarchy_builder',
                content: '<strong>Validation Passed</strong>' +
                    formatStats([
                        { label: 'SUM nodes', value: '5' },
                        { label: 'SUBTRACT nodes', value: '2' },
                        { label: 'Orphan nodes', value: '0' },
                        { label: 'Circular refs', value: '0' }
                    ]) +
                    '<br><strong>Formula Chain:</strong><ul>' +
                    '<li><code>Revenue</code> = SUM(Product Revenue, Service Revenue, Other Revenue)</li>' +
                    '<li><code>COGS</code> = SUM(Material Cost, Labor Cost, Manufacturing OH)</li>' +
                    '<li><code>Gross Profit</code> = Revenue &minus; COGS</li>' +
                    '<li><code>Operating Expenses</code> = SUM(Salaries, Rent, Marketing, Insurance, Depreciation)</li>' +
                    '<li><code>Operating Income</code> = Gross Profit &minus; Operating Expenses</li></ul>',
                delay: 2000
            },
            {
                type: 'tool', from: 'hierarchy_builder', to: 'hierarchy_builder',
                content: formatToolCall('export_hierarchy_csv', { project: 'PRJ-001', format: 'full', include_formulas: true }),
                delay: 1200
            },
            {
                type: 'response', from: 'hierarchy_builder', to: 'user',
                content: '<strong>Hierarchy Build Complete</strong><br><br>' +
                    'Income Statement hierarchy with <span class="demo-stat-highlight">20 nodes</span>, ' +
                    '<span class="demo-stat-highlight">13 GL mappings</span>, and ' +
                    '<span class="demo-stat-highlight">7 formula rules</span> is ready.<br><br>' +
                    '<strong>Next steps:</strong><ul>' +
                    '<li>Deploy to Snowflake via Wright pipeline (<code>wright_from_hierarchy</code>)</li>' +
                    '<li>Auto-sync to GraphRAG for semantic search</li>' +
                    '<li>Version snapshot for change tracking</li></ul>',
                delay: 1500
            }
        ];
    };

    // ----- Scenario 3: Data Quality Analysis -----
    SCENARIOS.data_quality = function () {
        var pz = DEMO_DATA.pizza_orders;
        var profile = computeProfile(pz);

        return [
            {
                type: 'system', from: 'System', to: '',
                content: '<strong>Autonomous Demo: Data Quality Analysis</strong><br>Profiling pizza order data, detecting quality issues, and running automated validation.',
                delay: 800
            },
            {
                type: 'request', from: 'user', to: 'orchestrator',
                content: 'Analyze data quality for the pizza order records. Find issues and generate validation rules.',
                delay: 1000
            },
            {
                type: 'thinking', from: 'orchestrator', to: 'orchestrator',
                content: '15 rows, 7 columns detected. Delegating to Quality Agent for profiling, issue detection, and rule generation.',
                delay: 1200
            },
            {
                type: 'plan', from: 'orchestrator', to: 'quality_agent',
                content: '<strong>Quality Plan:</strong><ol>' +
                    '<li>Load and display sample data</li>' +
                    '<li>Profile all columns (nulls, distinct, types, ranges)</li>' +
                    '<li>Detect quality issues</li>' +
                    '<li>Generate expectation suite (5 rules)</li>' +
                    '<li>Run validation and score</li></ol>',
                delay: 1200
            },
            {
                type: 'tool', from: 'quality_agent', to: 'quality_agent',
                content: formatToolCall('load_csv', { file: 'pizza_orders.csv', encoding: 'utf-8' }),
                delay: 1500
            },
            {
                type: 'result', from: 'quality_agent', to: 'quality_agent',
                content: '<strong>Pizza Orders</strong> &mdash; 15 rows, 7 columns' +
                    formatTable(pz.headers, pz.rows, { maxRows: 8 }),
                delay: 2000
            },
            {
                type: 'tool', from: 'quality_agent', to: 'quality_agent',
                content: formatToolCall('profile_data', { source: 'pizza_orders', columns: 'all' }),
                delay: 1500
            },
            {
                type: 'result', from: 'quality_agent', to: 'quality_agent',
                content: function () {
                    var headers = ['Column', 'Nulls', 'Completeness', 'Distinct', 'Type', 'Min', 'Max'];
                    var rows = profile.map(function (p) {
                        return [p.column, p.nulls, p.completeness, p.distinct, p.type, String(p.min), String(p.max)];
                    });
                    return '<strong>Column Profile</strong>' + formatTable(headers, rows);
                },
                delay: 2000
            },
            {
                type: 'thinking', from: 'quality_agent', to: 'quality_agent',
                content: '<strong>Issues Detected:</strong><ul>' +
                    '<li>Row 6 (P006): <code>customer_name</code> is <span class="demo-match-bad">NULL</span></li>' +
                    '<li>Row 12 (P012): <code>size</code> is <span class="demo-match-bad">NULL</span></li>' +
                    '<li>Price range 7.99&ndash;28.99 with possible outlier at 28.99</li></ul>',
                delay: 1500
            },
            {
                type: 'tool', from: 'quality_agent', to: 'quality_agent',
                content: formatToolCall('generate_expectation_suite', {
                    source: 'pizza_orders',
                    auto_detect: true,
                    rules: ['not_null', 'value_set', 'range', 'unique']
                }),
                delay: 1500
            },
            {
                type: 'result', from: 'quality_agent', to: 'quality_agent',
                content: function () {
                    var ruleHeaders = ['#', 'Rule', 'Column', 'Parameters', 'Severity'];
                    var ruleRows = [
                        ['1', 'not_null', 'customer_name', 'required: true', 'ERROR'],
                        ['2', 'not_null', 'size', 'required: true', 'ERROR'],
                        ['3', 'value_set', 'size', 'values: [Small, Medium, Large]', 'WARNING'],
                        ['4', 'range', 'price', 'min: 5.00, max: 35.00', 'WARNING'],
                        ['5', 'unique', 'order_id', 'allow_duplicates: false', 'ERROR']
                    ];
                    return '<strong>Auto-Generated Expectation Suite (5 rules)</strong>' +
                        formatTable(ruleHeaders, ruleRows);
                },
                delay: 1800
            },
            {
                type: 'tool', from: 'quality_agent', to: 'quality_agent',
                content: formatToolCall('run_validation', { suite: 'pizza_orders_suite', source: 'pizza_orders' }),
                delay: 1500
            },
            {
                type: 'response', from: 'quality_agent', to: 'user',
                content: '<strong>Data Quality Report</strong>' +
                    formatStats([
                        { label: 'Rules Passed', value: '3/5' },
                        { label: 'Rules Failed', value: '2/5' },
                        { label: 'Overall Score', value: '86.7%' },
                        { label: 'Records Affected', value: '2 of 15' }
                    ]) +
                    '<br><strong>Failed Rules:</strong><ul>' +
                    '<li><span class="demo-match-bad">FAIL</span> not_null(customer_name) &mdash; 1 null at row 6</li>' +
                    '<li><span class="demo-match-bad">FAIL</span> not_null(size) &mdash; 1 null at row 12</li></ul>' +
                    '<strong>Passed Rules:</strong><ul>' +
                    '<li><span class="demo-match-good">PASS</span> value_set(size) &mdash; all non-null values valid</li>' +
                    '<li><span class="demo-match-good">PASS</span> range(price) &mdash; all within 5.00-35.00</li>' +
                    '<li><span class="demo-match-good">PASS</span> unique(order_id) &mdash; all IDs unique</li></ul>',
                delay: 1500
            }
        ];
    };

    // ----- Scenario 4: dbt Pipeline Generation -----
    SCENARIOS.dbt_pipeline = function () {
        var gl = DEMO_DATA.gl_journal;

        var totalDebit = 0, totalCredit = 0;
        gl.rows.forEach(function (r) { totalDebit += parseFloat(r[5]); totalCredit += parseFloat(r[6]); });

        return [
            {
                type: 'system', from: 'System', to: '',
                content: '<strong>Autonomous Demo: dbt Pipeline Generation</strong><br>Generating a complete dbt project from GL journal entries.',
                delay: 800
            },
            {
                type: 'request', from: 'user', to: 'orchestrator',
                content: 'Generate a dbt project from the GL journal data. Include sources, staging models, and tests.',
                delay: 1000
            },
            {
                type: 'thinking', from: 'orchestrator', to: 'dbt_agent',
                content: '27 transactions across 3 periods (Jan&ndash;Mar 2024), 11 accounts, 6 departments. Total debits: $' + num(totalDebit) + ', total credits: $' + num(totalCredit) + '.',
                delay: 1500
            },
            {
                type: 'plan', from: 'dbt_agent', to: 'dbt_agent',
                content: '<strong>dbt Build Plan:</strong><ol>' +
                    '<li>Profile GL journal schema and summary stats</li>' +
                    '<li>Create dbt project structure</li>' +
                    '<li>Generate sources YAML + staging SQL</li>' +
                    '<li>Generate mart model with aggregations</li>' +
                    '<li>Add schema tests</li></ol>',
                delay: 1200
            },
            {
                type: 'tool', from: 'dbt_agent', to: 'dbt_agent',
                content: formatToolCall('profile_data', { source: 'gl_journal', focus: ['account_code', 'period', 'department'] }),
                delay: 1500
            },
            {
                type: 'result', from: 'dbt_agent', to: 'dbt_agent',
                content: '<strong>GL Journal Schema</strong>' +
                    formatTable(gl.headers, gl.rows, { maxRows: 6 }) +
                    formatStats([
                        { label: 'Total Debits', value: '$' + num(totalDebit) },
                        { label: 'Total Credits', value: '$' + num(totalCredit) },
                        { label: 'Periods', value: '3 (Jan-Mar)' },
                        { label: 'Accounts', value: '11' },
                        { label: 'Departments', value: '6' }
                    ]),
                delay: 2000
            },
            {
                type: 'tool', from: 'dbt_agent', to: 'dbt_agent',
                content: formatToolCall('create_dbt_project', { name: 'finance_analytics', target: 'snowflake', schema: 'analytics' }),
                delay: 1500
            },
            {
                type: 'result', from: 'dbt_agent', to: 'dbt_agent',
                content: '<strong>Project Created: finance_analytics</strong>' +
                    formatCodeBlock(
                        'finance_analytics/\n' +
                        '\u251C\u2500\u2500 dbt_project.yml\n' +
                        '\u251C\u2500\u2500 models/\n' +
                        '\u2502   \u251C\u2500\u2500 staging/\n' +
                        '\u2502   \u2502   \u251C\u2500\u2500 sources.yml\n' +
                        '\u2502   \u2502   \u2514\u2500\u2500 stg_gl_journal.sql\n' +
                        '\u2502   \u2514\u2500\u2500 marts/\n' +
                        '\u2502       \u2514\u2500\u2500 fct_gl_summary.sql\n' +
                        '\u251C\u2500\u2500 macros/\n' +
                        '\u251C\u2500\u2500 seeds/\n' +
                        '\u2514\u2500\u2500 tests/', 'Directory structure'
                    ),
                delay: 1800
            },
            {
                type: 'tool', from: 'dbt_agent', to: 'dbt_agent',
                content: formatToolCall('generate_dbt_sources', { project: 'finance_analytics', tables: ['gl_journal'] }),
                delay: 1500
            },
            {
                type: 'result', from: 'dbt_agent', to: 'dbt_agent',
                content: '<strong>Generated: sources.yml + stg_gl_journal.sql</strong>' +
                    formatCodeBlock(
                        'version: 2\n\n' +
                        'sources:\n' +
                        '  - name: raw_finance\n' +
                        '    schema: raw\n' +
                        '    tables:\n' +
                        '      - name: gl_journal\n' +
                        '        columns:\n' +
                        '          - name: txn_id\n' +
                        '            tests: [not_null, unique]\n' +
                        '          - name: period\n' +
                        '            tests: [not_null]\n' +
                        '          - name: account_code\n' +
                        '            tests: [not_null]', 'sources.yml'
                    ) +
                    formatCodeBlock(
                        '-- stg_gl_journal.sql\n' +
                        'WITH source AS (\n' +
                        '    SELECT * FROM {{ source(\'raw_finance\', \'gl_journal\') }}\n' +
                        '),\n' +
                        'renamed AS (\n' +
                        '    SELECT\n' +
                        '        txn_id,\n' +
                        '        period,\n' +
                        '        account_code,\n' +
                        '        account_name,\n' +
                        '        department,\n' +
                        '        COALESCE(debit, 0) AS debit_amount,\n' +
                        '        COALESCE(credit, 0) AS credit_amount,\n' +
                        '        debit - credit AS net_amount,\n' +
                        '        description\n' +
                        '    FROM source\n' +
                        ')\n' +
                        'SELECT * FROM renamed', 'stg_gl_journal.sql'
                    ),
                delay: 2200
            },
            {
                type: 'response', from: 'dbt_agent', to: 'user',
                content: '<strong>dbt Project Ready: finance_analytics</strong>' +
                    formatStats([
                        { label: 'Sources', value: '1 (gl_journal)' },
                        { label: 'Staging Models', value: '1 (stg_gl_journal)' },
                        { label: 'Mart Models', value: '1 (fct_gl_summary)' },
                        { label: 'Schema Tests', value: '5 (not_null, unique)' }
                    ]) +
                    '<br><strong>Next steps:</strong><ul>' +
                    '<li>Run <code>dbt build</code> to materialize models</li>' +
                    '<li>Add incremental strategy for production</li>' +
                    '<li>Connect to DataBridge catalog for lineage tracking</li></ul>',
                delay: 1500
            }
        ];
    };

    // ----- Scenario 5: Oil & Gas Well Analysis -----
    SCENARIOS.oil_gas = function () {
        var og = DEMO_DATA.oil_gas;

        // Aggregate by operator
        var operators = {};
        og.rows.forEach(function (r) {
            var op = r[2];
            if (!operators[op]) operators[op] = { wells: {}, totalRev: 0, totalNOI: 0, totalOil: 0, totalGas: 0, periods: 0 };
            operators[op].wells[r[1]] = true;
            operators[op].totalRev += parseFloat(r[17]);
            operators[op].totalNOI += parseFloat(r[19]);
            operators[op].totalOil += parseFloat(r[5]);
            operators[op].totalGas += parseFloat(r[6]);
            operators[op].periods++;
        });

        var totalProduction = 0, totalRevenue = 0, totalNOI = 0;
        og.rows.forEach(function (r) {
            totalProduction += parseFloat(r[5]);
            totalRevenue += parseFloat(r[17]);
            totalNOI += parseFloat(r[19]);
        });

        return [
            {
                type: 'system', from: 'System', to: '',
                content: '<strong>Autonomous Demo: Oil &amp; Gas Well Analysis</strong><br>Analyzing well economics across 5 wells and 2 operators with production and revenue data.',
                delay: 800
            },
            {
                type: 'request', from: 'user', to: 'orchestrator',
                content: 'Analyze well economics from the production data. Show operator comparisons, NOI by well, and decline rates.',
                delay: 1000
            },
            {
                type: 'thinking', from: 'orchestrator', to: 'cortex_analyst',
                content: '10 records, 5 wells, 2 operators (ABC Energy, XYZ Oil), 2 periods (Jan&ndash;Feb 2024), 21 economic metrics per record.',
                delay: 1500
            },
            {
                type: 'plan', from: 'cortex_analyst', to: 'cortex_analyst',
                content: '<strong>Analysis Plan:</strong><ol>' +
                    '<li>Load production data and display key columns</li>' +
                    '<li>Profile and summarize by operator</li>' +
                    '<li>Build operator &rarr; basin &rarr; well hierarchy</li>' +
                    '<li>Calculate KPIs: NOI, LOE/BOE, decline rate</li>' +
                    '<li>Executive dashboard summary</li></ol>',
                delay: 1200
            },
            {
                type: 'tool', from: 'cortex_analyst', to: 'cortex_analyst',
                content: formatToolCall('load_csv', { file: 'oil_gas_los.csv', encoding: 'utf-8' }),
                delay: 1500
            },
            {
                type: 'result', from: 'cortex_analyst', to: 'cortex_analyst',
                content: function () {
                    // Show subset of columns
                    var subHeaders = ['well_name', 'operator', 'period', 'gross_oil_bbl', 'net_revenue', 'noi'];
                    var subRows = og.rows.map(function (r) {
                        return [r[1], r[2], r[4], num(parseFloat(r[5])), '$' + num(parseFloat(r[17])), '$' + num(parseFloat(r[19]))];
                    });
                    return '<strong>Well Production Data</strong> &mdash; 10 records, 5 wells' +
                        formatTable(subHeaders, subRows, { maxRows: 6 });
                },
                delay: 2000
            },
            {
                type: 'tool', from: 'cortex_analyst', to: 'cortex_analyst',
                content: formatToolCall('profile_data', { source: 'oil_gas_los', group_by: 'operator' }),
                delay: 1500
            },
            {
                type: 'result', from: 'cortex_analyst', to: 'cortex_analyst',
                content: function () {
                    var opHeaders = ['Operator', 'Wells', 'Avg Gross Oil (bbl/mo)', 'Total Net Revenue', 'Total NOI'];
                    var opRows = Object.keys(operators).map(function (op) {
                        var d = operators[op];
                        var wellCount = Object.keys(d.wells).length;
                        return [op, wellCount, num(Math.round(d.totalOil / d.periods)), '$' + num(Math.round(d.totalRev)), '$' + num(Math.round(d.totalNOI))];
                    });
                    return '<strong>Operator Summary</strong>' + formatTable(opHeaders, opRows);
                },
                delay: 1800
            },
            {
                type: 'tool', from: 'cortex_analyst', to: 'hierarchy_builder',
                content: formatToolCall('create_hierarchy_project', { name: 'Well Economics', structure: 'Operator > Basin > Well' }),
                delay: 1500
            },
            {
                type: 'result', from: 'hierarchy_builder', to: 'cortex_analyst',
                content: function () {
                    var wellTree = [
                        {
                            name: 'ABC Energy', meta: '3 wells', children: [
                                {
                                    name: 'Permian Basin', children: [
                                        { name: 'Eagle Ford #1', meta: 'W001' },
                                        { name: 'Wolfcamp A-1', meta: 'W002' }
                                    ]
                                },
                                {
                                    name: 'Anadarko Basin', children: [
                                        { name: 'Bakken #7', meta: 'W003' }
                                    ]
                                }
                            ]
                        },
                        {
                            name: 'XYZ Oil', meta: '2 wells', children: [
                                {
                                    name: 'Williston Basin', children: [
                                        { name: 'Three Forks #3', meta: 'W004' },
                                        { name: 'Spraberry #12', meta: 'W005' }
                                    ]
                                }
                            ]
                        }
                    ];
                    return '<strong>Well Hierarchy</strong>' +
                        '<div class="demo-code" style="white-space:pre-wrap">' + formatTree(wellTree) + '</div>';
                },
                delay: 2000
            },
            {
                type: 'tool', from: 'cortex_analyst', to: 'cortex_analyst',
                content: formatToolCall('aggregate', { metrics: ['noi_by_operator', 'loe_per_boe', 'decline_rate'], group_by: 'well_id' }),
                delay: 1500
            },
            {
                type: 'result', from: 'cortex_analyst', to: 'cortex_analyst',
                content: function () {
                    var kpiHeaders = ['Well', 'Jan NOI', 'Feb NOI', 'Delta', 'Decline Rate'];
                    var kpiRows = [
                        ['Eagle Ford #1', '$251,219', '$227,630', '-$23,589', '3.0%'],
                        ['Wolfcamp A-1', '$77,391', '$194,034', '+$116,643', '4.0%'],
                        ['Bakken #7', '$234,868', '$235,940', '+$1,072', '2.0%'],
                        ['Three Forks #3', '$184,897', '$262,532', '+$77,635', '5.0%'],
                        ['Spraberry #12', '$324,532', '$322,589', '-$1,943', '3.0%']
                    ];
                    return '<strong>Well KPIs &mdash; Period-over-Period</strong>' + formatTable(kpiHeaders, kpiRows);
                },
                delay: 2000
            },
            {
                type: 'response', from: 'cortex_analyst', to: 'user',
                content: function () {
                    return '<strong>Executive Dashboard &mdash; Well Economics</strong>' +
                        formatStats([
                            { label: 'Total Production', value: num(Math.round(totalProduction)) + ' bbl' },
                            { label: 'Total Net Revenue', value: '$' + num(Math.round(totalRevenue)) },
                            { label: 'Total NOI', value: '$' + num(Math.round(totalNOI)) },
                            { label: 'Active Wells', value: '5' },
                            { label: 'Operators', value: '2' }
                        ]) +
                        '<br><strong>Key Insights:</strong><ul>' +
                        '<li>XYZ Oil wells generate higher per-well NOI ($' + num(Math.round(operators['XYZ Oil'].totalNOI / 2)) + ' avg)</li>' +
                        '<li>Wolfcamp A-1 had $120K capex in Jan; Feb NOI recovered strongly</li>' +
                        '<li>Bakken #7 shows lowest decline rate (2%) &mdash; most stable producer</li>' +
                        '<li>Three Forks #3 has highest decline (5%) but also highest single-well NOI</li></ul>';
                },
                delay: 1500
            }
        ];
    };

    // ----- Scenario 6: Full End-to-End Workflow -----
    SCENARIOS.full_workflow = function () {
        var sa = DEMO_DATA.source_a;
        var sb = DEMO_DATA.source_b;
        var pz = DEMO_DATA.pizza_orders;
        var gl = DEMO_DATA.gl_journal;

        var nameSim101 = similarity(sa.rows[0][1], sb.rows[0][1]);
        var nameSim102 = similarity(sa.rows[1][1], sb.rows[1][1]);

        var totalDebit = 0, totalCredit = 0;
        gl.rows.forEach(function (r) { totalDebit += parseFloat(r[5]); totalCredit += parseFloat(r[6]); });

        var tree = [
            {
                name: 'Income Statement', children: [
                    { name: 'Revenue', meta: 'SUM (3 children)' },
                    { name: 'COGS', meta: 'SUM (3 children)' },
                    { name: 'Gross Profit', meta: 'SUBTRACT' },
                    { name: 'Operating Expenses', meta: 'SUM (5 children)' },
                    { name: 'Operating Income', meta: 'SUBTRACT' }
                ]
            }
        ];

        return [
            {
                type: 'system', from: 'System', to: '',
                content: '<strong>Autonomous Demo: Full End-to-End Workflow</strong><br>Orchestrated walkthrough covering reconciliation, hierarchy building, dbt pipelines, and data quality.',
                delay: 800
            },
            {
                type: 'request', from: 'user', to: 'orchestrator',
                content: 'Run a full end-to-end workflow: reconcile data, build hierarchies, generate dbt models, and validate quality.',
                delay: 1000
            },
            {
                type: 'plan', from: 'orchestrator', to: 'orchestrator',
                content: '<strong>Orchestrated 4-Phase Workflow:</strong><ol>' +
                    '<li><strong>Phase 1:</strong> Data Reconciliation (2 sources)</li>' +
                    '<li><strong>Phase 2:</strong> Hierarchy Builder (P&L structure)</li>' +
                    '<li><strong>Phase 3:</strong> dbt Pipeline (GL journal &rarr; analytics)</li>' +
                    '<li><strong>Phase 4:</strong> Data Quality (validation &amp; scoring)</li></ol>',
                delay: 1500
            },
            // Phase 1: Reconciliation
            {
                type: 'system', from: 'System', to: '',
                content: '<strong>&mdash; Phase 1: Data Reconciliation &mdash;</strong>',
                delay: 800
            },
            {
                type: 'tool', from: 'orchestrator', to: 'data_agent',
                content: formatToolCall('load_csv', { files: ['source_a.csv', 'source_b.csv'] }),
                delay: 1200
            },
            {
                type: 'result', from: 'data_agent', to: 'orchestrator',
                content: 'Loaded 2 sources: Source A (4 rows), Source B (4 rows). Common IDs: 101, 102.' +
                    formatTable(sa.headers, sa.rows.slice(0, 2), { maxRows: 2 }),
                delay: 1500
            },
            {
                type: 'tool', from: 'orchestrator', to: 'data_agent',
                content: formatToolCall('fuzzy_match_columns', { columns: ['name'], key: 'customer_id' }),
                delay: 1200
            },
            {
                type: 'result', from: 'data_agent', to: 'orchestrator',
                content: '<strong>Fuzzy Match:</strong> ID 101 "John Smith"/"Jon Smith" = <span class="demo-match-fuzzy">' + nameSim101 + '%</span>, ID 102 "Jane Doe"/"Jane D." = <span class="demo-match-fuzzy">' + nameSim102 + '%</span>' +
                    formatStats([
                        { label: 'Fuzzy Matches', value: '2' },
                        { label: 'Orphans A', value: '2' },
                        { label: 'Orphans B', value: '2' }
                    ]),
                delay: 1500
            },
            // Phase 2: Hierarchy
            {
                type: 'system', from: 'System', to: '',
                content: '<strong>&mdash; Phase 2: Hierarchy Builder &mdash;</strong>',
                delay: 800
            },
            {
                type: 'tool', from: 'orchestrator', to: 'hierarchy_builder',
                content: formatToolCall('import_flexible_hierarchy', { source: 'pl_hierarchy.csv', format: 'tier3', row_count: 20 }),
                delay: 1500
            },
            {
                type: 'result', from: 'hierarchy_builder', to: 'orchestrator',
                content: function () {
                    return '<strong>P&L Hierarchy: 20 nodes, 3 levels</strong>' +
                        '<div class="demo-code" style="white-space:pre-wrap">' + formatTree(tree) + '</div>' +
                        formatStats([{ label: 'Formulas', value: '7 (5 SUM, 2 SUBTRACT)' }, { label: 'GL Mappings', value: '13' }]);
                },
                delay: 1800
            },
            {
                type: 'tool', from: 'orchestrator', to: 'hierarchy_builder',
                content: formatToolCall('validate_project', { check_formulas: true }),
                delay: 1000
            },
            {
                type: 'result', from: 'hierarchy_builder', to: 'orchestrator',
                content: '<span class="demo-match-good">Validation Passed</span> &mdash; 0 orphans, 0 circular refs, all formula chains valid.',
                delay: 1200
            },
            // Phase 3: dbt
            {
                type: 'system', from: 'System', to: '',
                content: '<strong>&mdash; Phase 3: dbt Pipeline &mdash;</strong>',
                delay: 800
            },
            {
                type: 'tool', from: 'orchestrator', to: 'dbt_agent',
                content: formatToolCall('create_dbt_project', { name: 'finance_analytics', source: 'gl_journal' }),
                delay: 1200
            },
            {
                type: 'result', from: 'dbt_agent', to: 'orchestrator',
                content: '<strong>dbt project created:</strong> 1 source, 1 staging model, 1 mart model, 5 tests.' +
                    formatStats([
                        { label: 'Total Debits', value: '$' + num(totalDebit) },
                        { label: 'Total Credits', value: '$' + num(totalCredit) },
                        { label: 'Periods', value: '3' }
                    ]),
                delay: 1500
            },
            // Phase 4: Quality
            {
                type: 'system', from: 'System', to: '',
                content: '<strong>&mdash; Phase 4: Data Quality &mdash;</strong>',
                delay: 800
            },
            {
                type: 'tool', from: 'orchestrator', to: 'quality_agent',
                content: formatToolCall('run_validation', { source: 'pizza_orders', auto_suite: true }),
                delay: 1200
            },
            {
                type: 'result', from: 'quality_agent', to: 'orchestrator',
                content: '<strong>Quality Score:</strong> <span class="demo-stat-highlight">86.7%</span> &mdash; 3/5 rules passed, 2 null violations (P006: customer_name, P012: size).',
                delay: 1500
            },
            // Deployment
            {
                type: 'system', from: 'System', to: '',
                content: '<strong>&mdash; Deployment &amp; Versioning &mdash;</strong>',
                delay: 800
            },
            {
                type: 'tool', from: 'orchestrator', to: 'orchestrator',
                content: formatToolCall('wright_from_hierarchy', { project: 'PRJ-001', target: 'snowflake', pipeline: 'full' }),
                delay: 1200
            },
            {
                type: 'result', from: 'orchestrator', to: 'orchestrator',
                content: '<strong>Wright Pipeline Generated</strong> &mdash; 4 objects (VW_1 Translation View, DT_2 Granularity, DT_3A Pre-Agg, DT_3 Mart)' +
                    formatStats([{ label: 'Objects', value: '4' }, { label: 'Target', value: 'Snowflake' }, { label: 'Status', value: 'Ready' }]),
                delay: 1500
            },
            {
                type: 'tool', from: 'orchestrator', to: 'orchestrator',
                content: formatToolCall('version_create', { label: 'v1.0-demo', snapshot: 'full', assets: ['hierarchy', 'dbt', 'quality_report'] }),
                delay: 1000
            },
            {
                type: 'response', from: 'orchestrator', to: 'user',
                content: '<strong>End-to-End Workflow Complete</strong>' +
                    formatStats([
                        { label: 'Reconciliation', value: '2 fuzzy + 4 orphans' },
                        { label: 'Hierarchy', value: '20 nodes, 7 formulas' },
                        { label: 'dbt Pipeline', value: '3 models, 5 tests' },
                        { label: 'Quality Score', value: '86.7%' },
                        { label: 'Wright Pipeline', value: '4 objects' },
                        { label: 'Version', value: 'v1.0-demo' }
                    ]) +
                    '<br>All phases completed successfully. Data pipeline is deployment-ready.',
                delay: 1500
            }
        ];
    };

    // ================================================================
    // EXECUTION ENGINE
    // ================================================================

    function wait(ms) {
        return new Promise(function (resolve) {
            setTimeout(resolve, ms / speedMultiplier);
        });
    }

    function updateProgress(current, total) {
        var fill = document.getElementById('demoProgressFill');
        var text = document.getElementById('demoProgressText');
        if (fill) fill.style.width = Math.round((current / total) * 100) + '%';
        if (text) text.textContent = 'Step ' + current + '/' + total;
    }

    function updateUIState(running) {
        var launchBtn = document.getElementById('launchDemoBtn');
        var cancelBtn = document.getElementById('cancelDemoBtn');
        var select = document.getElementById('demoSelect');
        var queryInput = document.getElementById('queryInput');
        var progress = document.getElementById('demoProgress');

        if (running) {
            if (launchBtn) { launchBtn.disabled = true; launchBtn.textContent = 'Running...'; }
            if (cancelBtn) cancelBtn.style.display = 'block';
            if (select) select.disabled = true;
            if (queryInput) queryInput.disabled = true;
            if (progress) progress.style.display = 'block';
        } else {
            if (launchBtn) { launchBtn.disabled = false; launchBtn.textContent = 'Launch Demo'; }
            if (cancelBtn) cancelBtn.style.display = 'none';
            if (select) select.disabled = false;
            if (queryInput) queryInput.disabled = false;
            if (progress) progress.style.display = 'none';
            updateProgress(0, 1);
        }
    }

    async function executeStep(step, index, total) {
        if (shouldCancel) return false;

        addTypingIndicator();
        await wait(step.delay || 1200);
        removeTypingIndicator();

        if (shouldCancel) return false;

        var content = typeof step.content === 'function' ? step.content() : step.content;
        await addMessage(step.type, step.from, step.to || '', content);
        updateProgress(index + 1, total);
        return true;
    }

    async function runScenario(scenarioId) {
        var scenarioFn = SCENARIOS[scenarioId];
        if (!scenarioFn) return;

        var steps = scenarioFn();
        clearConsole();
        isRunning = true;
        shouldCancel = false;
        updateUIState(true);

        var startTime = Date.now();

        for (var i = 0; i < steps.length; i++) {
            if (!await executeStep(steps[i], i, steps.length)) break;
        }

        var elapsed = ((Date.now() - startTime) / 1000).toFixed(1);

        if (shouldCancel) {
            await addMessage('system', 'System', '', 'Demo cancelled by user after ' + (i) + ' of ' + steps.length + ' steps.');
            if (typeof showNotification === 'function') showNotification('Demo cancelled', 'info');
        } else {
            await addMessage('system', 'System', '', 'Demo complete. ' + steps.length + ' steps executed in ' + elapsed + 's.');
            if (typeof showNotification === 'function') showNotification('Demo complete!', 'success');
        }

        isRunning = false;
        shouldCancel = false;
        updateUIState(false);
    }

    // ================================================================
    // PUBLIC API
    // ================================================================

    function launch() {
        if (isRunning) return;
        var select = document.getElementById('demoSelect');
        var scenarioId = select ? select.value : '';
        if (!scenarioId) return;
        runScenario(scenarioId);
    }

    function cancel() {
        if (isRunning) {
            shouldCancel = true;
        }
    }

    function setSpeed(speed, btn) {
        speedMultiplier = speed;
        document.querySelectorAll('.demo-speed-btn').forEach(function (b) {
            b.classList.remove('active');
        });
        if (btn) btn.classList.add('active');
    }

    function onSelectChange() {
        var select = document.getElementById('demoSelect');
        var desc = document.getElementById('demoDescription');
        var btn = document.getElementById('launchDemoBtn');
        var val = select ? select.value : '';

        if (val && DESCRIPTIONS[val]) {
            if (desc) desc.textContent = DESCRIPTIONS[val];
            if (btn) btn.disabled = false;
        } else {
            if (desc) desc.textContent = 'Select a demo to see a hands-off walkthrough of DataBridge AI capabilities.';
            if (btn) btn.disabled = true;
        }
    }

    return {
        launch: launch,
        cancel: cancel,
        setSpeed: setSpeed,
        onSelectChange: onSelectChange
    };
})();
