# DataBridge FP&A Workflow: End-to-End Analysis Engine

## Vision: Unified FP&A Platform

V3 + V4 together create a complete **Financial Planning & Analysis** workflow:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        END-TO-END FP&A DATA FLOW                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐        │
│  │ General     │  │ Planning &  │  │ Operations  │  │ Field       │        │
│  │ Ledger      │  │ Budgeting   │  │ Systems     │  │ Systems     │        │
│  │ (ERP)       │  │ (EPM)       │  │ (MES/SCADA) │  │ (ARIES/etc) │        │
│  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘        │
│         │                │                │                │               │
│         └────────────────┼────────────────┼────────────────┘               │
│                          │                │                                 │
│                          ▼                ▼                                 │
│              ┌──────────────────────────────────────────┐                  │
│              │           V4: ANALYTICS ENGINE           │                  │
│              │                                          │                  │
│              │  • Connect to financial sources          │                  │
│              │  • Extract metadata & schemas            │                  │
│              │  • Profile fact tables                   │                  │
│              │  • Push analysis to data warehouse       │                  │
│              └─────────────────┬────────────────────────┘                  │
│                                │                                            │
│                                │ Dimension Context                          │
│                                │ (Hierarchies)                              │
│                                │                                            │
│              ┌─────────────────┴────────────────────────┐                  │
│              │           V3: HIERARCHY BUILDER          │                  │
│              │                                          │                  │
│              │  • P&L Hierarchy (Revenue, Expenses)     │                  │
│              │  • Balance Sheet Structure               │                  │
│              │  • Cost Center Hierarchy                 │                  │
│              │  • Product/Service Hierarchy             │                  │
│              │  • Geographic Hierarchy                  │                  │
│              │  • Legal Entity Structure                │                  │
│              └─────────────────┬────────────────────────┘                  │
│                                │                                            │
│                                ▼                                            │
│              ┌──────────────────────────────────────────┐                  │
│              │           FP&A OUTPUTS                   │                  │
│              │                                          │                  │
│              │  • Variance Analysis (BvA, Prior Year)   │                  │
│              │  • Rolling Forecasts                     │                  │
│              │  • Management Reporting                  │                  │
│              │  • Month-End Close Support               │                  │
│              │  • Board Packages                        │                  │
│              │  • Operational KPIs                      │                  │
│              └──────────────────────────────────────────┘                  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 1. Source Systems Integration

### 1.1 Financial Systems (Primary)

| System Type | Examples | Data Captured | Frequency |
|-------------|----------|---------------|-----------|
| **General Ledger** | SAP, Oracle, NetSuite, Dynamics | Trial balance, journal entries, account balances | Daily/Monthly |
| **Accounts Receivable** | ERP AR module | Customer invoices, aging, collections | Daily |
| **Accounts Payable** | ERP AP module | Vendor invoices, payment schedule | Daily |
| **Fixed Assets** | ERP FA module | Asset register, depreciation | Monthly |
| **Payroll** | ADP, Workday, Paylocity | Labor costs, headcount, benefits | Bi-weekly |

### 1.2 Planning & Budgeting Systems

| System Type | Examples | Data Captured | Frequency |
|-------------|----------|---------------|-----------|
| **EPM/CPM** | Anaplan, Hyperion, Adaptive | Annual budgets, forecasts | Monthly |
| **Workforce Planning** | Workday Adaptive, Anaplan | Headcount plans, salary budgets | Monthly |
| **Capital Planning** | EPM tools, Excel | CAPEX budgets, project forecasts | Quarterly |
| **Revenue Planning** | Salesforce, CRM | Sales pipeline, bookings forecast | Weekly |

### 1.3 Operations Systems

| System Type | Examples | Data Captured | Frequency |
|-------------|----------|---------------|-----------|
| **Manufacturing** | SAP MES, Rockwell, Siemens | Production volumes, yields, costs | Real-time |
| **Inventory** | ERP Inventory, WMS | Stock levels, movements, valuations | Daily |
| **Quality** | QMS systems | Defect rates, rework costs | Daily |
| **Maintenance** | CMMS (Maximo, SAP PM) | Maintenance costs, downtime | Daily |

### 1.4 Industry-Specific Field Systems

**Oil & Gas:**
| System | Data Captured | Use in FP&A |
|--------|---------------|-------------|
| ARIES | Reserves, decline curves | Reserve valuation, impairment |
| ComboCurve/PHDWin | Production forecasts | Revenue projections |
| ProCount/Enertia | Production accounting, JIB | LOE analysis, NRI calculation |
| WellView | Well operations, costs | CAPEX tracking |
| Quorum/OGsys | Revenue/expense allocation | Joint interest billing |

**Manufacturing:**
| System | Data Captured | Use in FP&A |
|--------|---------------|-------------|
| MES | Production output, efficiency | Cost variance analysis |
| PLM | Product costs, BOMs | Standard costing |
| EAM | Asset utilization, maintenance | Depreciation, maintenance budgets |

**Healthcare:**
| System | Data Captured | Use in FP&A |
|--------|---------------|-------------|
| Epic/Cerner | Patient volumes, charges | Revenue by service line |
| Kronos | Labor hours, overtime | Labor cost analysis |
| Supply Chain | Materials usage | Cost per procedure |

---

## 2. V3 + V4 Integration: The FP&A Workflow

### 2.1 Hierarchy-Driven Analysis

V3 hierarchies define **how** data is organized for analysis:

```
V3 Hierarchy: P&L Revenue Structure
├── Total Revenue
│   ├── Product Revenue
│   │   ├── Hardware
│   │   │   └── [Source Mapping: GL 4000-4199]
│   │   ├── Software
│   │   │   └── [Source Mapping: GL 4200-4399]
│   │   └── Services
│   │       └── [Source Mapping: GL 4400-4599]
│   └── Other Revenue
│       ├── Interest Income
│       │   └── [Source Mapping: GL 4800]
│       └── Gain on Sale
│           └── [Source Mapping: GL 4900]
```

V4 uses this hierarchy to:
1. **Structure queries** - Group GL accounts by hierarchy nodes
2. **Calculate rollups** - Sum child nodes to parents
3. **Apply formulas** - Calculate Gross Profit, Operating Income, etc.
4. **Enable drill-down** - Start at Total Revenue, drill to Hardware → specific GL accounts

### 2.2 Multi-Source Reconciliation

```python
# V4 reconciles data across sources using V3 hierarchy as the structure

class FPAReconciler:
    """Reconcile GL to operational sources using V3 hierarchy."""

    def reconcile_revenue(
        self,
        hierarchy_id: str,
        gl_connection: str,
        ops_connection: str,
        period: str
    ) -> ReconciliationResult:
        """
        Reconcile GL revenue to operational revenue (e.g., production × price).

        Uses V3 hierarchy to map GL accounts to operational metrics.
        """
        # Get V3 hierarchy structure
        hierarchy = self.v3_client.get_hierarchy_tree(hierarchy_id)

        # Query GL for booked revenue
        gl_revenue = self.query_gl_by_hierarchy(
            connection=gl_connection,
            hierarchy=hierarchy,
            period=period
        )
        # Returns: {hierarchy_node: gl_amount}

        # Query operations for calculated revenue
        ops_revenue = self.query_ops_revenue(
            connection=ops_connection,
            hierarchy=hierarchy,
            period=period
        )
        # Returns: {hierarchy_node: ops_calculated_amount}

        # Reconcile at each hierarchy level
        variances = self.calculate_variances(gl_revenue, ops_revenue)

        return ReconciliationResult(
            period=period,
            gl_total=sum(gl_revenue.values()),
            ops_total=sum(ops_revenue.values()),
            variance=gl_total - ops_total,
            node_variances=variances,
            materiality_threshold=self.config.materiality,
            exceptions=[v for v in variances if v.is_material]
        )
```

### 2.3 Complete FP&A Analysis Cycle

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                      MONTHLY FP&A CLOSE CYCLE                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  DAY 1-2: Data Collection                                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ V4: sync_sources()                                                   │   │
│  │   • Extract GL trial balance                                         │   │
│  │   • Pull budget/forecast from EPM                                    │   │
│  │   • Load operational actuals                                         │   │
│  │   • Validate data completeness                                       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  DAY 3-4: Reconciliation                                                    │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ V4: reconcile_close()                                                │   │
│  │   • GL to subledger tie-out (using V3 account hierarchy)             │   │
│  │   • Intercompany eliminations                                        │   │
│  │   • Revenue recognition review                                       │   │
│  │   • Flag exceptions for investigation                                │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  DAY 5-6: Variance Analysis                                                 │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ V4: analyze_variances()                                              │   │
│  │   • Actual vs. Budget (using V3 P&L hierarchy)                       │   │
│  │   • Actual vs. Prior Year                                            │   │
│  │   • Actual vs. Forecast                                              │   │
│  │   • Decompose into price/volume/mix drivers                          │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  DAY 7-8: Commentary & Reporting                                            │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ V4: generate_commentary()                                            │   │
│  │   • Auto-generate variance explanations                              │   │
│  │   • Build management report package                                  │   │
│  │   • Create executive summary                                         │   │
│  │   • Prepare board materials                                          │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
│  DAY 9-10: Forecast Update                                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐   │
│  │ V4: update_forecast()                                                │   │
│  │   • Apply actuals to YTD                                             │   │
│  │   • Reforecast remaining periods                                     │   │
│  │   • Model scenarios (upside/downside)                                │   │
│  │   • Generate full-year outlook                                       │   │
│  └─────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. FP&A-Specific Analysis Modules

### 3.1 Variance Analysis Engine

```python
class VarianceAnalyzer:
    """Comprehensive variance analysis using V3 hierarchies."""

    def analyze_budget_variance(
        self,
        hierarchy_id: str,
        actuals_table: str,
        budget_table: str,
        period: str,
        dimensions: List[str] = None
    ) -> VarianceReport:
        """
        Perform Budget vs. Actual analysis at each hierarchy level.

        Args:
            hierarchy_id: V3 P&L or Balance Sheet hierarchy
            actuals_table: GL fact table
            budget_table: Budget fact table
            period: Period to analyze (e.g., "2024-01")
            dimensions: Optional additional dimensions (region, product)

        Returns:
            VarianceReport with node-level variances and drill-down
        """
        hierarchy = self.v3_client.get_hierarchy_tree(hierarchy_id)

        # Build queries using V3 source mappings
        actual_query = self.build_hierarchy_query(
            hierarchy, actuals_table, period, dimensions
        )
        budget_query = self.build_hierarchy_query(
            hierarchy, budget_table, period, dimensions
        )

        # Execute with pushdown
        actuals = self.execute(actual_query)
        budgets = self.execute(budget_query)

        # Calculate variances at each hierarchy level
        variances = []
        for node in hierarchy.traverse():
            actual = actuals.get(node.hierarchy_id, 0)
            budget = budgets.get(node.hierarchy_id, 0)

            variances.append(VarianceDetail(
                hierarchy_node=node.hierarchy_name,
                level=node.level,
                actual=actual,
                budget=budget,
                variance_amount=actual - budget,
                variance_percent=(actual - budget) / budget if budget else None,
                is_favorable=self.is_favorable(node, actual - budget),
                children=[...]  # Recursive for drill-down
            ))

        return VarianceReport(
            period=period,
            hierarchy_name=hierarchy.name,
            total_actual=sum(actuals.values()),
            total_budget=sum(budgets.values()),
            total_variance=total_actual - total_budget,
            node_variances=variances,
            top_drivers=self.identify_top_drivers(variances, n=5),
            commentary=self.generate_commentary(variances)
        )

    def decompose_variance(
        self,
        hierarchy_node: str,
        actuals: DataFrame,
        budgets: DataFrame,
        decomposition_type: str = "price_volume_mix"
    ) -> VarianceDecomposition:
        """
        Decompose variance into price, volume, and mix components.

        For revenue: Price × Volume = Revenue
        - Price variance: (Actual Price - Budget Price) × Actual Volume
        - Volume variance: (Actual Volume - Budget Volume) × Budget Price
        - Mix variance: Shift in product/customer mix

        For costs: similar decomposition with rates and hours/units.
        """
        if decomposition_type == "price_volume_mix":
            return self.price_volume_mix_decomposition(actuals, budgets)
        elif decomposition_type == "rate_efficiency":
            return self.rate_efficiency_decomposition(actuals, budgets)
        elif decomposition_type == "spend_volume":
            return self.spend_volume_decomposition(actuals, budgets)
```

### 3.2 Rolling Forecast Engine

```python
class ForecastEngine:
    """Maintain rolling forecasts using V3 hierarchies."""

    def update_rolling_forecast(
        self,
        hierarchy_id: str,
        current_period: str,
        actuals_table: str,
        forecast_table: str,
        method: str = "replace_actuals"
    ) -> ForecastUpdate:
        """
        Update rolling forecast with latest actuals.

        Methods:
        - replace_actuals: Replace forecast with actuals for closed periods
        - trend_forward: Apply recent trends to future periods
        - driver_based: Recalculate based on updated drivers
        """
        hierarchy = self.v3_client.get_hierarchy_tree(hierarchy_id)

        # Get YTD actuals
        ytd_actuals = self.query_ytd_actuals(hierarchy, actuals_table, current_period)

        # Get current forecast
        current_forecast = self.query_forecast(hierarchy, forecast_table)

        if method == "replace_actuals":
            updated = self.replace_with_actuals(current_forecast, ytd_actuals)
        elif method == "trend_forward":
            updated = self.apply_trend_to_forecast(current_forecast, ytd_actuals)
        elif method == "driver_based":
            updated = self.recalculate_from_drivers(hierarchy, ytd_actuals)

        # Calculate full-year outlook
        full_year = self.calculate_full_year_outlook(updated)

        return ForecastUpdate(
            period=current_period,
            prior_full_year=current_forecast.full_year_total,
            updated_full_year=full_year,
            change=full_year - current_forecast.full_year_total,
            change_percent=(full_year - current_forecast.full_year_total) / current_forecast.full_year_total,
            ytd_actual=sum(ytd_actuals.values()),
            remaining_forecast=full_year - sum(ytd_actuals.values()),
            node_updates=updated
        )

    def model_scenario(
        self,
        base_forecast: ForecastUpdate,
        scenario_name: str,
        adjustments: Dict[str, float]
    ) -> ScenarioResult:
        """
        Create scenario by adjusting base forecast.

        Example adjustments:
        - {"Revenue.Product Sales": 1.10} → 10% increase
        - {"Expenses.Labor": 1.05} → 5% increase
        - {"Expenses.Materials": 0.95} → 5% decrease
        """
        scenario = base_forecast.copy()

        for hierarchy_path, multiplier in adjustments.items():
            node = scenario.get_node(hierarchy_path)
            node.apply_adjustment(multiplier, cascade_to_children=True)

        return ScenarioResult(
            name=scenario_name,
            base_full_year=base_forecast.full_year_total,
            scenario_full_year=scenario.full_year_total,
            impact=scenario.full_year_total - base_forecast.full_year_total,
            adjustments_applied=adjustments,
            node_impacts=scenario.calculate_impacts(base_forecast)
        )
```

### 3.3 Operational Metrics Integration

```python
class OperationalMetricsIntegrator:
    """Integrate operational data with financial hierarchies."""

    def map_operational_to_financial(
        self,
        ops_table: str,
        financial_hierarchy_id: str,
        mapping_rules: Dict[str, str]
    ) -> MappedMetrics:
        """
        Map operational metrics to financial hierarchy nodes.

        Example mapping_rules:
        - "production_volume" → "Revenue.Oil Sales" (volume driver)
        - "avg_price_per_bbl" → "Revenue.Oil Sales" (price driver)
        - "operating_hours" → "Expenses.Direct Labor" (driver)
        """
        hierarchy = self.v3_client.get_hierarchy_tree(financial_hierarchy_id)
        ops_data = self.query_operational_data(ops_table)

        mapped = {}
        for ops_metric, hierarchy_path in mapping_rules.items():
            node = hierarchy.get_node(hierarchy_path)
            mapped[node.hierarchy_id] = {
                "financial_node": node.hierarchy_name,
                "operational_metric": ops_metric,
                "value": ops_data[ops_metric],
                "relationship": self.detect_relationship(node, ops_metric)
            }

        return MappedMetrics(
            hierarchy_id=financial_hierarchy_id,
            mappings=mapped,
            correlation_analysis=self.analyze_correlations(mapped),
            driver_insights=self.generate_driver_insights(mapped)
        )

    def calculate_unit_economics(
        self,
        hierarchy_id: str,
        volume_source: str,
        volume_column: str
    ) -> UnitEconomics:
        """
        Calculate per-unit metrics using operational volumes.

        Example: Revenue per BOE, Cost per Unit, LOE per Well
        """
        hierarchy = self.v3_client.get_hierarchy_tree(hierarchy_id)
        volume = self.query_total_volume(volume_source, volume_column)

        unit_metrics = {}
        for node in hierarchy.traverse():
            if node.is_monetary:
                unit_metrics[node.hierarchy_name] = {
                    "total": node.value,
                    "per_unit": node.value / volume if volume else None,
                    "unit_label": self.get_unit_label(volume_source)
                }

        return UnitEconomics(
            total_volume=volume,
            unit_label=self.get_unit_label(volume_source),
            metrics=unit_metrics,
            industry_benchmarks=self.get_benchmarks(hierarchy_id)
        )
```

---

## 4. FP&A-Specific MCP Tools

### 4.1 Close Management Tools

```python
@mcp.tool()
def sync_period_data(
    period: str,
    sources: List[str] = ["gl", "budget", "forecast", "operations"]
) -> str:
    """
    Sync all data sources for a period.

    Args:
        period: Period to sync (e.g., "2024-01")
        sources: Which sources to sync

    Returns:
        Sync status with row counts and data quality flags
    """

@mcp.tool()
def validate_close_readiness(period: str) -> str:
    """
    Check if period is ready for close.

    Validates:
    - All subledgers posted
    - Intercompany balanced
    - Required accruals booked
    - No orphan transactions
    """

@mcp.tool()
def reconcile_subledger_to_gl(
    subledger: str,
    gl_account: str,
    period: str
) -> str:
    """
    Reconcile a subledger to its GL control account.

    Returns differences and suggested adjustments.
    """

@mcp.tool()
def generate_close_checklist(period: str) -> str:
    """
    Generate close checklist with completion status.

    Uses V3 hierarchy to organize by account area.
    """
```

### 4.2 Variance Analysis Tools

```python
@mcp.tool()
def analyze_budget_variance(
    hierarchy_id: str,
    period: str,
    dimensions: List[str] = None,
    materiality_threshold: float = 10000
) -> str:
    """
    Analyze budget vs. actual variance using V3 hierarchy.

    Returns variance at each hierarchy level with drill-down.
    """

@mcp.tool()
def analyze_prior_year_variance(
    hierarchy_id: str,
    current_period: str,
    dimensions: List[str] = None
) -> str:
    """
    Analyze current period vs. same period prior year.
    """

@mcp.tool()
def decompose_revenue_variance(
    hierarchy_node: str,
    period: str,
    decomposition: str = "price_volume_mix"
) -> str:
    """
    Decompose revenue variance into price, volume, and mix.
    """

@mcp.tool()
def identify_variance_drivers(
    hierarchy_id: str,
    period: str,
    top_n: int = 5
) -> str:
    """
    Identify the top N drivers of variance.

    Returns hierarchy nodes with largest variances and business context.
    """

@mcp.tool()
def generate_variance_commentary(
    hierarchy_id: str,
    period: str,
    style: str = "executive"  # or "detailed"
) -> str:
    """
    Generate natural language variance commentary.

    Uses V3 hierarchy structure for organized narrative.
    """
```

### 4.3 Forecasting Tools

```python
@mcp.tool()
def update_rolling_forecast(
    hierarchy_id: str,
    current_period: str,
    method: str = "replace_actuals"
) -> str:
    """
    Update rolling forecast with latest actuals.
    """

@mcp.tool()
def calculate_full_year_outlook(
    hierarchy_id: str,
    as_of_period: str
) -> str:
    """
    Calculate full-year outlook (YTD actual + remaining forecast).
    """

@mcp.tool()
def model_scenario(
    base_forecast_id: str,
    scenario_name: str,
    adjustments: Dict[str, float]
) -> str:
    """
    Create forecast scenario with adjustments.

    adjustments: {"Revenue.Product Sales": 1.10} for 10% increase
    """

@mcp.tool()
def compare_forecast_versions(
    hierarchy_id: str,
    version_a: str,
    version_b: str
) -> str:
    """
    Compare two forecast versions (e.g., Original Budget vs. Latest Forecast).
    """
```

### 4.4 Reporting Tools

```python
@mcp.tool()
def generate_management_report(
    hierarchy_id: str,
    period: str,
    format: str = "summary"  # or "detailed"
) -> str:
    """
    Generate management reporting package.

    Includes: P&L, variance analysis, KPIs, commentary
    """

@mcp.tool()
def generate_board_package(
    period: str,
    sections: List[str] = ["financial_summary", "kpis", "outlook", "risks"]
) -> str:
    """
    Generate board-level reporting package.
    """

@mcp.tool()
def generate_flash_report(
    period: str,
    days_into_period: int
) -> str:
    """
    Generate flash report with preliminary results.

    Includes run-rate projections for full period.
    """

@mcp.tool()
def export_to_excel(
    hierarchy_id: str,
    period: str,
    template: str = "standard_pl"
) -> str:
    """
    Export analysis to Excel using template formatting.
    """
```

### 4.5 Operational Integration Tools

```python
@mcp.tool()
def reconcile_revenue_to_operations(
    financial_hierarchy: str,
    operational_table: str,
    period: str
) -> str:
    """
    Reconcile GL revenue to operational calculation (volume × price).
    """

@mcp.tool()
def calculate_unit_economics(
    hierarchy_id: str,
    volume_metric: str,
    period: str
) -> str:
    """
    Calculate per-unit metrics (Revenue/BOE, Cost/Unit, etc.).
    """

@mcp.tool()
def analyze_operational_drivers(
    financial_node: str,
    period: str
) -> str:
    """
    Identify operational drivers for a financial line item.
    """

@mcp.tool()
def forecast_from_operations(
    operational_forecast: str,
    financial_hierarchy: str,
    conversion_rules: Dict
) -> str:
    """
    Convert operational forecast to financial forecast.

    Example: Production volume × expected price = Revenue forecast
    """
```

---

## 5. Sample Docker Dataset: Financial Focus

### 5.1 Enhanced Schema for FP&A

```sql
-- ============================================
-- FINANCIAL FACT TABLES
-- ============================================

-- GL Journal Entries (Transaction Grain)
CREATE TABLE fact_gl_journal (
    journal_key SERIAL PRIMARY KEY,
    date_key INT REFERENCES dim_date(date_key),
    account_key INT REFERENCES dim_account(account_key),
    cost_center_key INT REFERENCES dim_cost_center(cost_center_key),
    entity_key INT REFERENCES dim_entity(entity_key),

    -- Transaction details
    journal_id VARCHAR(50),
    journal_line INT,
    source_system VARCHAR(50),
    document_type VARCHAR(50),

    -- Measures
    debit_amount DECIMAL(15,2),
    credit_amount DECIMAL(15,2),
    local_currency_amount DECIMAL(15,2),
    reporting_currency_amount DECIMAL(15,2),

    -- Audit
    posted_date DATE,
    posted_by VARCHAR(100),
    approved_by VARCHAR(100)
);

-- Monthly GL Balances (Period Grain - Aggregated)
CREATE TABLE fact_gl_balance (
    balance_key SERIAL PRIMARY KEY,
    period_key INT,  -- YYYYMM
    account_key INT REFERENCES dim_account(account_key),
    cost_center_key INT REFERENCES dim_cost_center(cost_center_key),
    entity_key INT REFERENCES dim_entity(entity_key),

    -- Balance measures
    beginning_balance DECIMAL(15,2),
    period_activity DECIMAL(15,2),
    ending_balance DECIMAL(15,2),

    -- For income statement accounts
    ytd_activity DECIMAL(15,2)
);

-- Budget (Period × Account × Cost Center)
CREATE TABLE fact_budget (
    budget_key SERIAL PRIMARY KEY,
    period_key INT,  -- YYYYMM
    account_key INT REFERENCES dim_account(account_key),
    cost_center_key INT REFERENCES dim_cost_center(cost_center_key),
    entity_key INT REFERENCES dim_entity(entity_key),

    -- Version
    budget_version VARCHAR(50),  -- Original, Revised Q1, Latest Forecast
    version_date DATE,

    -- Measures
    budget_amount DECIMAL(15,2),
    prior_year_actual DECIMAL(15,2)  -- For comparison
);

-- Forecast (Period × Account × Cost Center)
CREATE TABLE fact_forecast (
    forecast_key SERIAL PRIMARY KEY,
    period_key INT,  -- YYYYMM
    account_key INT REFERENCES dim_account(account_key),
    cost_center_key INT REFERENCES dim_cost_center(cost_center_key),
    entity_key INT REFERENCES dim_entity(entity_key),

    -- Version
    forecast_version VARCHAR(50),  -- Rolling, Q1 Outlook, Board Approved
    forecast_date DATE,

    -- Measures
    forecast_amount DECIMAL(15,2)
);

-- Headcount & Compensation (Period Grain)
CREATE TABLE fact_headcount (
    headcount_key SERIAL PRIMARY KEY,
    period_key INT,  -- YYYYMM
    cost_center_key INT REFERENCES dim_cost_center(cost_center_key),
    job_family_key INT,

    -- Headcount measures
    beginning_headcount INT,
    hires INT,
    terminations INT,
    transfers_in INT,
    transfers_out INT,
    ending_headcount INT,

    -- Compensation measures
    base_salary DECIMAL(15,2),
    bonus DECIMAL(15,2),
    benefits DECIMAL(15,2),
    total_compensation DECIMAL(15,2)
);

-- Intercompany Transactions
CREATE TABLE fact_intercompany (
    ic_key SERIAL PRIMARY KEY,
    period_key INT,
    from_entity_key INT REFERENCES dim_entity(entity_key),
    to_entity_key INT REFERENCES dim_entity(entity_key),
    account_key INT REFERENCES dim_account(account_key),

    -- Measures
    amount DECIMAL(15,2),
    elimination_status VARCHAR(20)  -- Pending, Eliminated
);

-- ============================================
-- OPERATIONAL FACT TABLES
-- ============================================

-- Production (Daily - Oil & Gas / Manufacturing)
CREATE TABLE fact_production (
    production_key SERIAL PRIMARY KEY,
    date_key INT REFERENCES dim_date(date_key),
    asset_key INT REFERENCES dim_asset(asset_key),
    product_key INT REFERENCES dim_product(product_key),

    -- Volume measures
    gross_production DECIMAL(15,4),
    net_production DECIMAL(15,4),  -- After royalties
    sales_volume DECIMAL(15,4),

    -- Pricing
    realized_price DECIMAL(15,4),

    -- Calculated
    gross_revenue DECIMAL(15,2),
    net_revenue DECIMAL(15,2),

    -- Operational
    producing_days INT,
    downtime_days DECIMAL(5,2)
);

-- Operating Costs (Monthly)
CREATE TABLE fact_operating_costs (
    cost_key SERIAL PRIMARY KEY,
    period_key INT,
    asset_key INT REFERENCES dim_asset(asset_key),
    cost_category_key INT REFERENCES dim_cost_category(cost_category_key),

    -- Measures
    actual_cost DECIMAL(15,2),
    budget_cost DECIMAL(15,2),
    prior_year_cost DECIMAL(15,2),

    -- Metrics
    production_volume DECIMAL(15,4),
    cost_per_unit DECIMAL(15,4)
);
```

### 5.2 Sample Data Characteristics

| Table | Rows | Time Range | Purpose |
|-------|------|------------|---------|
| `fact_gl_journal` | ~200,000 | 3 years | Transaction-level detail |
| `fact_gl_balance` | ~15,000 | 36 months | Period balances for reporting |
| `fact_budget` | ~5,000 | 24 months | Budget by account/cost center |
| `fact_forecast` | ~8,000 | 24 months | Multiple forecast versions |
| `fact_headcount` | ~1,000 | 36 months | Workforce data |
| `fact_production` | ~30,000 | 3 years | Daily operational data |
| `fact_operating_costs` | ~3,000 | 36 months | Monthly cost detail |

---

## 6. End-to-End Example: Monthly Close Workflow

```python
# Complete monthly close workflow using V3 + V4

# 1. SYNC DATA SOURCES
sync_result = await v4.sync_period_data(
    period="2024-12",
    sources=["gl", "budget", "forecast", "operations"]
)
# Synced 15,234 GL transactions, 500 budget lines, 12,000 production records

# 2. VALIDATE CLOSE READINESS
validation = await v4.validate_close_readiness(period="2024-12")
# Status: 3 items require attention
# - AR subledger: $45K difference (timing)
# - Intercompany: Entity 103 unbalanced by $12K
# - Accrual: December utilities not yet accrued

# 3. RECONCILE SUBLEDGERS
for subledger in ["AR", "AP", "FA", "Inventory"]:
    recon = await v4.reconcile_subledger_to_gl(
        subledger=subledger,
        period="2024-12"
    )

# 4. ANALYZE VARIANCES (Using V3 P&L Hierarchy)
variance_report = await v4.analyze_budget_variance(
    hierarchy_id="pl-hierarchy-2024",
    period="2024-12",
    dimensions=["region", "product_line"]
)
# Total Revenue: $12.3M actual vs $11.8M budget (+4.2%)
# Total Expenses: $9.8M actual vs $9.5M budget (+3.2%)
# Net Income: $2.5M actual vs $2.3M budget (+8.7%)

# 5. IDENTIFY DRIVERS
drivers = await v4.identify_variance_drivers(
    hierarchy_id="pl-hierarchy-2024",
    period="2024-12",
    top_n=5
)
# Top drivers:
# 1. Product Sales - Hardware: +$320K (new product launch)
# 2. Labor Costs: +$180K (overtime for year-end push)
# 3. Marketing Expense: -$95K (campaign deferred to January)

# 6. GENERATE COMMENTARY
commentary = await v4.generate_variance_commentary(
    hierarchy_id="pl-hierarchy-2024",
    period="2024-12",
    style="executive"
)
# "December 2024 results exceeded budget by $200K (8.7%).
#  Revenue outperformed by $500K driven by strong hardware sales
#  from the Q4 product launch. This was partially offset by
#  $280K higher labor costs due to overtime..."

# 7. UPDATE FORECAST
forecast_update = await v4.update_rolling_forecast(
    hierarchy_id="pl-hierarchy-2024",
    current_period="2024-12",
    method="replace_actuals"
)
# FY2024 Forecast Updated:
# - Prior forecast: $28.5M
# - Updated forecast: $29.2M
# - Change: +$700K (+2.5%)

# 8. GENERATE BOARD PACKAGE
board_package = await v4.generate_board_package(
    period="2024-12",
    sections=["financial_summary", "kpis", "outlook", "risks"]
)
# Generated 12-page board package with executive summary,
# financial statements, KPI dashboard, and FY2025 outlook
```

---

This enhanced workflow shows how **V3 hierarchies structure the analysis** while **V4 executes the FP&A process** against actual financial and operational data from multiple source systems.
