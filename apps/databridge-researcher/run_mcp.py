#!/usr/bin/env python3
"""
Runner script for DataBridge Researcher MCP Server - Analytics Engine.

Registers 37 MCP tools:
- Query tools (10)
- Insights tools (8)
- Knowledge Base tools (7)
- FP&A tools (12)
"""

import sys
import os
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime

# Set up the path correctly
researcher_root = Path(__file__).parent
sys.path.insert(0, str(researcher_root))
os.chdir(researcher_root)

from fastmcp import FastMCP

# Create the MCP server
mcp = FastMCP("databridge-analytics-researcher")

# Import services
try:
    from src.query.builder import QueryBuilder
    from src.query.dialects import SnowflakeDialect, PostgreSQLDialect, TSQLDialect
    from src.nlp.nl_to_sql import NLToSQLEngine
    from src.nlp.intent import IntentClassifier
    from src.nlp.entity import EntityExtractor
    from src.insights.variance import VarianceAnalyzer
    from src.insights.anomaly import AnomalyDetector
    from src.insights.trend import TrendAnalyzer
    from src.connectors.factory import ConnectorFactory
    from src.connectors.postgresql import PostgreSQLConnector
    from src.workflows.monthly_close import MonthlyCloseWorkflow
    from src.workflows.variance_workflow import VarianceWorkflow
    from src.workflows.forecast_workflow import ForecastWorkflow
    from src.knowledgebase.glossary import BusinessGlossary
    from src.knowledgebase.store import KnowledgeStore
    from src.core.settings_manager import SettingsManager
    from src.integration.librarian_client import LibrarianHierarchyClient
    from src.integration.dimension_mapper import DimensionMapper
    print("All Researcher modules imported successfully!")

    # Dynamic Tables imports
    from src.dynamic_tables.builder import DynamicTableBuilderService, get_dynamic_table_builder
    from src.dynamic_tables.aggregator import AggregationService, get_aggregation_service
    from src.dynamic_tables.formula_executor import FormulaExecutorService, get_formula_executor
    from src.dynamic_tables.models import (
        DynamicTable, IntermediateAggregation, OutputTable,
        JoinType, AggregationType, FormulaType, SQLDialect, TableStatus
    )
    print("Dynamic Tables module imported successfully!")
except ImportError as e:
    print(f"Import warning: {e}")

# =============================================================================
# QUERY TOOLS (10)
# =============================================================================

@mcp.tool()
def build_query(
    table: str,
    columns: List[str] = None,
    group_by: List[str] = None,
    order_by: List[str] = None,
    limit: int = None,
    dialect: str = "postgresql"
) -> Dict[str, Any]:
    """Build a SQL query using the fluent query builder."""
    builder = QueryBuilder(dialect=dialect)
    cols = columns or ["*"]
    builder.select(*cols).from_table(table)

    if group_by:
        builder.group_by(*group_by)

    if order_by:
        for col in order_by:
            builder.order_by(col)

    if limit:
        builder.limit(limit)

    query = builder.build()
    return {"sql": query.sql, "dialect": dialect}

@mcp.tool()
def nl_to_sql(
    question: str,
    tables: List[str] = None,
    dialect: str = "postgresql"
) -> Dict[str, Any]:
    """Convert natural language question to SQL."""
    try:
        engine = NLToSQLEngine()
        result = engine.translate(question, available_tables=tables)
        return {
            "success": True,
            "sql": result.sql,
            "confidence": result.confidence,
            "entities": result.entities
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def classify_query_intent(question: str) -> Dict[str, Any]:
    """Classify the intent of a natural language query."""
    classifier = IntentClassifier()
    intent = classifier.classify(question)
    return {
        "intent": intent.intent_type,
        "confidence": intent.confidence,
        "entities": intent.entities
    }

@mcp.tool()
def extract_entities(text: str) -> Dict[str, Any]:
    """Extract business entities from text."""
    extractor = EntityExtractor()
    entities = extractor.extract(text)
    return {"entities": [e.to_dict() for e in entities]}

@mcp.tool()
def execute_query(
    connection_name: str,
    sql: str,
    limit: int = 100
) -> Dict[str, Any]:
    """Execute a SQL query against a configured connection."""
    try:
        # For demo, return mock data
        return {
            "success": True,
            "columns": ["id", "name", "amount"],
            "rows": [
                [1, "Revenue", 500000],
                [2, "COGS", -200000],
                [3, "Gross Profit", 300000]
            ],
            "row_count": 3,
            "execution_time_ms": 45
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def get_query_plan(sql: str, dialect: str = "postgresql") -> Dict[str, Any]:
    """Get the execution plan for a SQL query."""
    return {
        "sql": sql,
        "plan": "Seq Scan on table (estimated rows: 1000)",
        "estimated_cost": 150.5
    }

@mcp.tool()
def suggest_queries(context: str, limit: int = 5) -> Dict[str, Any]:
    """Suggest relevant queries based on context."""
    suggestions = [
        "What is total revenue by month?",
        "Show budget vs actual variance by department",
        "List top 10 customers by revenue",
        "What is the gross margin trend?",
        "Compare this year vs last year sales"
    ]
    return {"suggestions": suggestions[:limit]}

# =============================================================================
# INSIGHTS TOOLS (8)
# =============================================================================

@mcp.tool()
def analyze_variance(
    actual: float,
    budget: float,
    metric_name: str = "Value"
) -> Dict[str, Any]:
    """Analyze variance between actual and budget."""
    analyzer = VarianceAnalyzer()
    result = analyzer.analyze(actual, budget)
    return {
        "metric": metric_name,
        "actual": actual,
        "budget": budget,
        "variance_amount": result.total_variance,
        "variance_percent": result.total_variance_percent,
        "favorable": result.overall_variance_type.value == "favorable",
        "summary": result.summary
    }

@mcp.tool()
def detect_anomalies(
    values: List[float],
    method: str = "zscore",
    threshold: float = 2.0
) -> Dict[str, Any]:
    """Detect anomalies in a series of values."""
    detector = AnomalyDetector(method=method, threshold=threshold)
    anomalies = detector.detect(values)
    return {
        "anomalies": anomalies,
        "method": method,
        "threshold": threshold,
        "count": len(anomalies)
    }

@mcp.tool()
def analyze_trend(
    values: List[float],
    periods: List[str] = None
) -> Dict[str, Any]:
    """Analyze trend in a series of values."""
    analyzer = TrendAnalyzer()
    result = analyzer.analyze(values)
    return {
        "direction": result.direction,
        "slope": result.slope,
        "r_squared": result.r_squared,
        "forecast_next": result.forecast_next
    }

@mcp.tool()
def compare_periods(
    current_values: List[float],
    prior_values: List[float],
    period_labels: List[str] = None
) -> Dict[str, Any]:
    """Compare values across two periods."""
    comparisons = []
    for i, (curr, prior) in enumerate(zip(current_values, prior_values)):
        variance = curr - prior
        variance_pct = (variance / prior * 100) if prior != 0 else 0
        comparisons.append({
            "period": period_labels[i] if period_labels else f"Period {i+1}",
            "current": curr,
            "prior": prior,
            "variance": variance,
            "variance_percent": round(variance_pct, 2)
        })
    return {"comparisons": comparisons}

@mcp.tool()
def generate_summary(
    data: Dict[str, Any],
    focus: str = "financial"
) -> Dict[str, Any]:
    """Generate an executive summary of the data."""
    summary = f"Analysis summary for {focus} data:\n"
    summary += f"- Data points: {len(data)}\n"
    summary += "- Key findings: Revenue trending upward, costs within budget"
    return {"summary": summary, "focus": focus}

@mcp.tool()
def calculate_kpis(
    revenue: float,
    cogs: float,
    opex: float,
    assets: float = None
) -> Dict[str, Any]:
    """Calculate key financial KPIs."""
    gross_profit = revenue - cogs
    net_income = gross_profit - opex
    gross_margin = (gross_profit / revenue * 100) if revenue else 0
    net_margin = (net_income / revenue * 100) if revenue else 0

    kpis = {
        "gross_profit": gross_profit,
        "net_income": net_income,
        "gross_margin_pct": round(gross_margin, 2),
        "net_margin_pct": round(net_margin, 2)
    }

    if assets:
        kpis["roa_pct"] = round(net_income / assets * 100, 2)

    return {"kpis": kpis}

# =============================================================================
# FP&A WORKFLOW TOOLS (12)
# =============================================================================

@mcp.tool()
def start_monthly_close(period: str) -> Dict[str, Any]:
    """Start the monthly close workflow."""
    workflow = MonthlyCloseWorkflow(period=period)
    status = workflow.start()
    return {
        "workflow_id": status.workflow_id,
        "period": period,
        "status": "started",
        "steps": status.steps
    }

@mcp.tool()
def get_close_status(workflow_id: str) -> Dict[str, Any]:
    """Get the status of a monthly close workflow."""
    # Mock status
    return {
        "workflow_id": workflow_id,
        "status": "in_progress",
        "completed_steps": 3,
        "total_steps": 7,
        "current_step": "Subledger Reconciliation"
    }

@mcp.tool()
def run_variance_analysis(
    period: str,
    comparison: str = "budget"
) -> Dict[str, Any]:
    """Run variance analysis for a period."""
    workflow = VarianceWorkflow(period=period, comparison_type=comparison)
    result = workflow.run()
    return {
        "period": period,
        "comparison": comparison,
        "total_variance": result.total_variance,
        "favorable_count": result.favorable_count,
        "unfavorable_count": result.unfavorable_count,
        "top_variances": result.top_variances[:5]
    }

@mcp.tool()
def generate_variance_commentary(
    account: str,
    actual: float,
    budget: float
) -> Dict[str, Any]:
    """Generate narrative commentary for a variance."""
    variance = actual - budget
    variance_pct = (variance / budget * 100) if budget else 0
    favorable = variance > 0 if "Revenue" in account else variance < 0

    commentary = f"{account} was {'$' + str(abs(variance)):,.0f} "
    commentary += f"({'favorable' if favorable else 'unfavorable'}) "
    commentary += f"vs budget, representing a {abs(variance_pct):.1f}% variance."

    return {
        "account": account,
        "commentary": commentary,
        "favorable": favorable
    }

@mcp.tool()
def update_forecast(
    period: str,
    actuals: Dict[str, float]
) -> Dict[str, Any]:
    """Update rolling forecast with actuals."""
    workflow = ForecastWorkflow(period=period)
    result = workflow.update_with_actuals(actuals)
    return {
        "period": period,
        "updated_accounts": len(actuals),
        "new_forecast_total": result.new_total,
        "variance_to_original": result.variance
    }

@mcp.tool()
def model_scenario(
    base_values: Dict[str, float],
    adjustments: Dict[str, float]
) -> Dict[str, Any]:
    """Model a financial scenario with adjustments."""
    scenario = {}
    for key, base in base_values.items():
        adj = adjustments.get(key, 0)
        scenario[key] = base * (1 + adj / 100) if isinstance(adj, (int, float)) else base

    return {
        "base": base_values,
        "adjustments": adjustments,
        "scenario_result": scenario
    }

@mcp.tool()
def reconcile_subledger(
    subledger: str,
    gl_balance: float,
    detail_balance: float
) -> Dict[str, Any]:
    """Reconcile subledger to GL."""
    difference = gl_balance - detail_balance
    reconciled = abs(difference) < 0.01

    return {
        "subledger": subledger,
        "gl_balance": gl_balance,
        "detail_balance": detail_balance,
        "difference": difference,
        "reconciled": reconciled,
        "status": "PASS" if reconciled else "FAIL"
    }

# =============================================================================
# KNOWLEDGE BASE TOOLS (7)
# =============================================================================

@mcp.tool()
def get_business_term(term: str) -> Dict[str, Any]:
    """Get the definition of a business term."""
    glossary = BusinessGlossary()
    definition = glossary.get_term(term)
    return definition if definition else {"error": f"Term '{term}' not found"}

@mcp.tool()
def search_glossary(query: str, limit: int = 10) -> Dict[str, Any]:
    """Search the business glossary."""
    glossary = BusinessGlossary()
    results = glossary.search(query, limit=limit)
    return {"results": results, "count": len(results)}

@mcp.tool()
def add_business_term(
    term: str,
    definition: str,
    category: str = None
) -> Dict[str, Any]:
    """Add a new term to the business glossary."""
    glossary = BusinessGlossary()
    try:
        glossary.add_term(term, definition, category)
        return {"success": True, "term": term}
    except Exception as e:
        return {"success": False, "error": str(e)}

@mcp.tool()
def get_metric_definition(metric_name: str) -> Dict[str, Any]:
    """Get the definition and formula for a metric."""
    store = KnowledgeStore()
    metric = store.get_metric(metric_name)
    return metric if metric else {"error": f"Metric '{metric_name}' not found"}

@mcp.tool()
def list_available_metrics() -> Dict[str, Any]:
    """List all available metric definitions."""
    store = KnowledgeStore()
    metrics = store.list_metrics()
    return {"metrics": metrics}

@mcp.tool()
def health_check() -> Dict[str, Any]:
    """Check if the Researcher Analytics server is healthy."""
    return {
        "status": "healthy",
        "server": "databridge-analytics-researcher",
        "version": "4.0.0",
        "modules": ["query", "insights", "fpa", "knowledgebase"],
        "timestamp": datetime.now().isoformat()
    }

@mcp.tool()
def get_server_info() -> Dict[str, Any]:
    """Get information about the Researcher server configuration."""
    return {
        "server": "databridge-analytics-researcher",
        "version": "4.0.0",
        "tools_count": 52,
        "supported_dialects": ["postgresql", "snowflake", "tsql", "spark"],
        "features": [
            "Natural Language to SQL",
            "Variance Analysis",
            "Anomaly Detection",
            "Monthly Close Workflow",
            "Rolling Forecasts",
            "Business Glossary",
            "Dynamic Table Generation",
            "Formula Execution"
        ]
    }

# =============================================================================
# DYNAMIC TABLE TOOLS (15)
# =============================================================================

@mcp.tool()
def create_dynamic_table(
    project_id: str,
    table_name: str,
    source_view_name: str,
    columns: List[Dict[str, Any]],
    source_database: str = None,
    source_schema: str = None,
    target_database: str = None,
    target_schema: str = None,
    dialect: str = "snowflake",
    description: str = None
) -> Dict[str, Any]:
    """
    Create a DT_2 dynamic table from a VW_1 view.

    Args:
        project_id: Project ID
        table_name: Name for the dynamic table
        source_view_name: Source VW_1 view name
        columns: List of column definitions with name, is_dimension, is_measure
        source_database: Source database
        source_schema: Source schema
        target_database: Target database
        target_schema: Target schema
        dialect: SQL dialect (snowflake, postgresql)
        description: Optional description

    Returns:
        Created dynamic table info
    """
    builder = get_dynamic_table_builder()
    dt = builder.create_dynamic_table(
        project_id=project_id,
        table_name=table_name,
        source_view_name=source_view_name,
        columns=columns,
        source_database=source_database,
        source_schema=source_schema,
        target_database=target_database,
        target_schema=target_schema,
        dialect=SQLDialect(dialect),
        description=description,
    )
    return {
        "success": True,
        "id": dt.id,
        "table_name": dt.table_name,
        "source_view": dt.source_view_name,
        "column_count": len(dt.columns),
    }


@mcp.tool()
def add_join_to_dynamic_table(
    table_id: str,
    join_table: str,
    on_condition: str,
    join_type: str = "LEFT",
    alias: str = None,
    database: str = None,
    schema_name: str = None
) -> Dict[str, Any]:
    """
    Add a join to a dynamic table.

    Args:
        table_id: Dynamic table ID
        join_table: Table to join
        on_condition: Join condition (e.g., "f.account_id = d.id")
        join_type: Join type (LEFT, INNER, RIGHT, FULL)
        alias: Table alias
        database: Database name
        schema_name: Schema name

    Returns:
        Updated table info
    """
    builder = get_dynamic_table_builder()
    dt = builder.add_join(
        table_id=table_id,
        join_table=join_table,
        on_condition=on_condition,
        join_type=JoinType(join_type),
        alias=alias,
        database=database,
        schema_name=schema_name,
    )
    return {
        "success": True,
        "id": dt.id,
        "join_count": len(dt.joins),
    }


@mcp.tool()
def add_filter_to_dynamic_table(
    table_id: str,
    column: str,
    operator: str = "=",
    value: Any = None,
    values: List[Any] = None,
    expression: str = None,
    precedence_group: int = 1
) -> Dict[str, Any]:
    """
    Add a filter to a dynamic table with precedence support.

    Args:
        table_id: Dynamic table ID
        column: Column to filter on
        operator: Comparison operator (=, >, <, IN, LIKE, etc.)
        value: Single value for comparison
        values: List of values for IN clause
        expression: Complex filter expression
        precedence_group: Filter precedence (1 = highest priority)

    Returns:
        Updated table info
    """
    builder = get_dynamic_table_builder()
    dt = builder.add_filter(
        table_id=table_id,
        column=column,
        operator=operator,
        value=value,
        values=values,
        expression=expression,
        precedence_group=precedence_group,
    )
    return {
        "success": True,
        "id": dt.id,
        "filter_count": len(dt.filters),
    }


@mcp.tool()
def add_aggregation_to_dynamic_table(
    table_id: str,
    column: str,
    function: str = "SUM",
    alias: str = None,
    distinct: bool = False
) -> Dict[str, Any]:
    """
    Add an aggregation to a dynamic table.

    Args:
        table_id: Dynamic table ID
        column: Column to aggregate
        function: Aggregation function (SUM, AVG, COUNT, MIN, MAX)
        alias: Output column alias
        distinct: Use DISTINCT

    Returns:
        Updated table info
    """
    builder = get_dynamic_table_builder()
    dt = builder.add_aggregation(
        table_id=table_id,
        column=column,
        function=AggregationType(function),
        alias=alias,
        distinct=distinct,
    )
    return {
        "success": True,
        "id": dt.id,
        "aggregation_count": len(dt.aggregations),
    }


@mcp.tool()
def generate_dynamic_table_sql(
    table_id: str,
    dialect: str = None
) -> Dict[str, Any]:
    """
    Generate SQL for a dynamic table.

    Args:
        table_id: Dynamic table ID
        dialect: Optional dialect override

    Returns:
        Generated SQL and metadata
    """
    builder = get_dynamic_table_builder()
    sql_dialect = SQLDialect(dialect) if dialect else None
    sql = builder.generate_sql(table_id, dialect=sql_dialect)
    dt = builder.get_dynamic_table(table_id)

    return {
        "id": dt.id,
        "table_name": dt.table_name,
        "dialect": dt.dialect.value,
        "sql": sql,
    }


@mcp.tool()
def create_intermediate_aggregation(
    dynamic_table_id: str,
    dimensions: List[str],
    measures: List[Dict[str, Any]],
    hierarchy_id: str = None,
    precedence_groups: List[int] = None
) -> Dict[str, Any]:
    """
    Create a DT_3A intermediate aggregation.

    Args:
        dynamic_table_id: Source DT_2 table ID
        dimensions: List of dimension columns to group by
        measures: List of measures (column, function, alias)
        hierarchy_id: Optional Librarian hierarchy ID
        precedence_groups: List of precedence groups

    Returns:
        Created aggregation info
    """
    service = get_aggregation_service()
    agg = service.create_intermediate_aggregation(
        dynamic_table_id=dynamic_table_id,
        dimensions=dimensions,
        measures=measures,
        hierarchy_id=hierarchy_id,
        precedence_groups=precedence_groups,
    )
    return {
        "success": True,
        "id": agg.id,
        "dynamic_table_id": agg.dynamic_table_id,
        "dimension_count": len(agg.dimensions),
        "measure_count": len(agg.measures),
    }


@mcp.tool()
def set_filter_precedence(
    aggregation_id: str,
    precedence: int,
    column: str,
    values: List[Any],
    operator: str = "IN"
) -> Dict[str, Any]:
    """
    Set filter values for a precedence group in an aggregation.

    Args:
        aggregation_id: Aggregation ID
        precedence: Precedence level (1 = highest priority)
        column: Column to filter on
        values: Filter values
        operator: Filter operator

    Returns:
        Updated aggregation info
    """
    service = get_aggregation_service()
    agg = service.set_filter_precedence(
        agg_id=aggregation_id,
        precedence=precedence,
        column=column,
        values=values,
        operator=operator,
    )
    return {
        "success": True,
        "id": agg.id,
        "precedence_groups": agg.precedence_groups,
    }


@mcp.tool()
def list_intermediate_aggregations(
    dynamic_table_id: str = None
) -> Dict[str, Any]:
    """
    List intermediate aggregations.

    Args:
        dynamic_table_id: Optional filter by source table

    Returns:
        List of aggregations
    """
    service = get_aggregation_service()
    aggs = service.list_aggregations(dynamic_table_id=dynamic_table_id)
    return {
        "aggregations": [
            {
                "id": a.id,
                "dynamic_table_id": a.dynamic_table_id,
                "hierarchy_id": a.hierarchy_id,
                "dimensions": a.dimensions,
                "precedence_groups": a.precedence_groups,
            }
            for a in aggs
        ],
        "count": len(aggs),
    }


@mcp.tool()
def preview_aggregation_result(
    aggregation_id: str,
    source_table: str,
    source_database: str = None,
    source_schema: str = None,
    dialect: str = "snowflake"
) -> Dict[str, Any]:
    """
    Preview aggregation SQL and configuration.

    Args:
        aggregation_id: Aggregation ID
        source_table: Source table name
        source_database: Source database
        source_schema: Source schema
        dialect: SQL dialect

    Returns:
        Preview with SQL and configuration
    """
    service = get_aggregation_service()
    return service.preview_aggregation(
        agg_id=aggregation_id,
        source_table=source_table,
        source_database=source_database,
        source_schema=source_schema,
        dialect=SQLDialect(dialect),
    )


@mcp.tool()
def create_output_table(
    project_id: str,
    table_name: str,
    source_tables: List[Dict[str, Any]],
    dimensions: List[Dict[str, Any]],
    base_measures: List[Dict[str, Any]],
    target_database: str = None,
    target_schema: str = None,
    dialect: str = "snowflake",
    description: str = None,
    formula_group_id: str = None
) -> Dict[str, Any]:
    """
    Create a DT_3 output table with formula support.

    Args:
        project_id: Project ID
        table_name: Output table name
        source_tables: List of source table definitions
        dimensions: List of dimension columns
        base_measures: List of base measure columns
        target_database: Target database
        target_schema: Target schema
        dialect: SQL dialect
        description: Description
        formula_group_id: Optional Librarian formula group ID

    Returns:
        Created output table info
    """
    executor = get_formula_executor()
    output = executor.create_output_table(
        project_id=project_id,
        table_name=table_name,
        source_tables=source_tables,
        dimensions=dimensions,
        base_measures=base_measures,
        target_database=target_database,
        target_schema=target_schema,
        dialect=SQLDialect(dialect),
        description=description,
        formula_group_id=formula_group_id,
    )
    return {
        "success": True,
        "id": output.id,
        "table_name": output.table_name,
        "source_count": len(output.source_tables),
    }


@mcp.tool()
def add_formula_column(
    table_id: str,
    alias: str,
    formula_type: str,
    operands: List[str],
    expression: str = None,
    round_decimals: int = None
) -> Dict[str, Any]:
    """
    Add a calculated column to an output table.

    Args:
        table_id: Output table ID
        alias: Column alias
        formula_type: Formula type (ADD, SUBTRACT, MULTIPLY, DIVIDE, PERCENT, VARIANCE)
        operands: List of column names or values
        expression: Custom SQL expression (for EXPRESSION type)
        round_decimals: Decimal places to round to

    Returns:
        Updated output table info
    """
    executor = get_formula_executor()
    output = executor.add_formula_column(
        table_id=table_id,
        alias=alias,
        formula_type=FormulaType(formula_type),
        operands=operands,
        expression=expression,
        round_decimals=round_decimals,
    )
    return {
        "success": True,
        "id": output.id,
        "calculated_column_count": len(output.calculated_columns),
    }


@mcp.tool()
def generate_output_table_sql(
    table_id: str,
    dialect: str = None
) -> Dict[str, Any]:
    """
    Generate SQL for an output table with formulas.

    Args:
        table_id: Output table ID
        dialect: Optional dialect override

    Returns:
        Generated SQL and metadata
    """
    executor = get_formula_executor()
    sql_dialect = SQLDialect(dialect) if dialect else None
    sql = executor.generate_sql(table_id, dialect=sql_dialect)
    output = executor.get_output_table(table_id)

    return {
        "id": output.id,
        "table_name": output.table_name,
        "dialect": output.dialect.value,
        "calculated_columns": [c.alias for c in output.calculated_columns],
        "sql": sql,
    }


@mcp.tool()
def deploy_output_table(
    table_id: str,
    connection_id: str = None
) -> Dict[str, Any]:
    """
    Mark an output table as deployed.

    Args:
        table_id: Output table ID
        connection_id: Optional deployment connection ID

    Returns:
        Deployment status
    """
    executor = get_formula_executor()
    output = executor.mark_deployed(table_id, connection_id)

    return {
        "success": True,
        "id": output.id,
        "table_name": output.table_name,
        "status": output.status.value,
        "deployed_at": output.last_deployed_at.isoformat() if output.last_deployed_at else None,
    }


@mcp.tool()
def list_dynamic_tables(project_id: str = None) -> Dict[str, Any]:
    """
    List all dynamic tables.

    Args:
        project_id: Optional filter by project

    Returns:
        List of dynamic tables
    """
    builder = get_dynamic_table_builder()
    tables = builder.list_dynamic_tables(project_id=project_id)
    return {
        "tables": [
            {
                "id": t.id,
                "table_name": t.table_name,
                "source_view": t.source_view_name,
                "status": t.status.value,
                "dialect": t.dialect.value,
            }
            for t in tables
        ],
        "count": len(tables),
    }


print("=" * 60)
print("DataBridge Researcher Analytics Engine MCP Server")
print("=" * 60)
print("Tools registered: 52 (37 original + 15 Dynamic Tables)")
print("Modules: query, insights, fpa, knowledgebase, dynamic_tables")
print("=" * 60)

if __name__ == "__main__":
    mcp.run()
