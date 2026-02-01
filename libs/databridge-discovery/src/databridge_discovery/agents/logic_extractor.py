"""
Logic Extractor Agent for parsing SQL and extracting business logic.

Capabilities:
- parse_sql: Parse SQL statements
- extract_case: Extract CASE WHEN statements
- identify_calcs: Identify calculations and formulas
- detect_aggregations: Detect aggregation patterns
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from databridge_discovery.agents.base_agent import (
    BaseAgent,
    AgentCapability,
    AgentConfig,
    AgentResult,
    AgentError,
    TaskContext,
)


@dataclass
class ParsedSQL:
    """Result of SQL parsing."""

    query_type: str  # SELECT, INSERT, UPDATE, etc.
    tables: list[dict[str, Any]] = field(default_factory=list)
    columns: list[dict[str, Any]] = field(default_factory=list)
    joins: list[dict[str, Any]] = field(default_factory=list)
    where_conditions: list[str] = field(default_factory=list)
    group_by: list[str] = field(default_factory=list)
    order_by: list[str] = field(default_factory=list)
    ctes: list[dict[str, Any]] = field(default_factory=list)
    subqueries: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "query_type": self.query_type,
            "tables": self.tables,
            "columns": self.columns,
            "joins": self.joins,
            "where_conditions": self.where_conditions,
            "group_by": self.group_by,
            "order_by": self.order_by,
            "ctes": self.ctes,
            "subqueries": self.subqueries,
        }


@dataclass
class ExtractedCalculation:
    """An extracted calculation from SQL."""

    expression: str
    alias: str | None = None
    calc_type: str = "expression"  # expression, aggregation, case, window
    source_columns: list[str] = field(default_factory=list)
    operators: list[str] = field(default_factory=list)
    functions: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "expression": self.expression,
            "alias": self.alias,
            "calc_type": self.calc_type,
            "source_columns": self.source_columns,
            "operators": self.operators,
            "functions": self.functions,
        }


@dataclass
class AggregationPattern:
    """A detected aggregation pattern."""

    function: str  # SUM, COUNT, AVG, etc.
    column: str | None = None
    alias: str | None = None
    group_by_columns: list[str] = field(default_factory=list)
    filters: list[str] = field(default_factory=list)
    is_distinct: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "function": self.function,
            "column": self.column,
            "alias": self.alias,
            "group_by_columns": self.group_by_columns,
            "filters": self.filters,
            "is_distinct": self.is_distinct,
        }


class LogicExtractor(BaseAgent):
    """
    Logic Extractor Agent for parsing SQL and extracting business logic.

    Extracts from SQL:
    - Query structure and components
    - CASE WHEN statements for hierarchy detection
    - Calculations and formulas
    - Aggregation patterns

    Example:
        extractor = LogicExtractor()

        context = TaskContext(
            task_id="extract_1",
            input_data={
                "sql": "SELECT CASE WHEN ... END as category FROM gl",
            }
        )

        result = extractor.execute(
            AgentCapability.EXTRACT_CASE,
            context
        )
    """

    # Aggregation functions to detect
    AGG_FUNCTIONS = ["SUM", "COUNT", "AVG", "MIN", "MAX", "STDDEV", "VARIANCE"]

    # Window functions
    WINDOW_FUNCTIONS = ["ROW_NUMBER", "RANK", "DENSE_RANK", "LAG", "LEAD", "FIRST_VALUE", "LAST_VALUE"]

    def __init__(self, config: AgentConfig | None = None):
        """Initialize Logic Extractor."""
        super().__init__(config or AgentConfig(name="LogicExtractor"))
        self._dialect = "snowflake"

    def get_capabilities(self) -> list[AgentCapability]:
        """Get supported capabilities."""
        return [
            AgentCapability.PARSE_SQL,
            AgentCapability.EXTRACT_CASE,
            AgentCapability.IDENTIFY_CALCS,
            AgentCapability.DETECT_AGGREGATIONS,
        ]

    def execute(
        self,
        capability: AgentCapability,
        context: TaskContext,
        **kwargs: Any,
    ) -> AgentResult:
        """
        Execute a capability.

        Args:
            capability: The capability to execute
            context: Task context with input data
            **kwargs: Additional arguments

        Returns:
            AgentResult with execution results
        """
        if not self.supports(capability):
            raise AgentError(
                f"Capability {capability} not supported",
                self.name,
                capability.value,
            )

        start_time = self._start_execution(capability, context)

        try:
            if capability == AgentCapability.PARSE_SQL:
                data = self._parse_sql(context, **kwargs)
            elif capability == AgentCapability.EXTRACT_CASE:
                data = self._extract_case(context, **kwargs)
            elif capability == AgentCapability.IDENTIFY_CALCS:
                data = self._identify_calcs(context, **kwargs)
            elif capability == AgentCapability.DETECT_AGGREGATIONS:
                data = self._detect_aggregations(context, **kwargs)
            else:
                raise AgentError(f"Unknown capability: {capability}", self.name)

            return self._complete_execution(capability, start_time, True, data)

        except AgentError as e:
            self._handle_error(e)
            return self._complete_execution(capability, start_time, False, error=str(e))
        except Exception as e:
            error = AgentError(str(e), self.name, capability.value)
            self._handle_error(error)
            return self._complete_execution(capability, start_time, False, error=str(e))

    def _parse_sql(self, context: TaskContext, **kwargs: Any) -> dict[str, Any]:
        """
        Parse SQL statement into components.

        Input data:
            - sql: SQL statement to parse
            - dialect: SQL dialect (default: snowflake)
        """
        self._report_progress("Parsing SQL", 0.0)

        input_data = context.input_data
        sql = input_data.get("sql", "")
        dialect = input_data.get("dialect", self._dialect)

        if not sql:
            raise AgentError("No SQL provided", self.name, "parse_sql")

        try:
            from databridge_discovery.parser.sql_parser import SQLParser

            parser = SQLParser(dialect=dialect)
            parsed = parser.parse(sql)

            self._report_progress("SQL parsed successfully", 1.0)

            # Convert Pydantic models to dicts for serialization
            tables_data = [t.model_dump() if hasattr(t, 'model_dump') else t for t in parsed.tables]
            columns_data = [c.model_dump() if hasattr(c, 'model_dump') else c for c in parsed.columns]
            joins_data = [j.model_dump() if hasattr(j, 'model_dump') else j for j in parsed.joins]
            metrics_data = parsed.metrics.model_dump() if hasattr(parsed.metrics, 'model_dump') else parsed.metrics

            return {
                "query_type": parsed.query_type,
                "tables": tables_data,
                "columns": columns_data,
                "joins": joins_data,
                "metrics": metrics_data,
                "has_case": parsed.has_case,
                "has_subquery": parsed.has_subquery,
                "complexity_score": self._calculate_complexity(parsed),
            }

        except ImportError:
            # Fallback to basic parsing
            return self._basic_parse_sql(sql)

    def _extract_case(self, context: TaskContext, **kwargs: Any) -> dict[str, Any]:
        """
        Extract CASE WHEN statements from SQL.

        Input data:
            - sql: SQL statement
            - include_hierarchy: Detect hierarchy patterns
        """
        self._report_progress("Extracting CASE statements", 0.0)

        input_data = context.input_data
        sql = input_data.get("sql", "")
        include_hierarchy = input_data.get("include_hierarchy", True)

        if not sql:
            raise AgentError("No SQL provided", self.name, "extract_case")

        try:
            from databridge_discovery.parser.case_extractor import CaseExtractor

            extractor = CaseExtractor()
            cases = extractor.extract_from_sql(sql)

            result = {
                "case_count": len(cases),
                "case_statements": [],
            }

            for case in cases:
                case_data = {
                    "id": case.id,
                    "source_column": case.source_column,
                    "input_column": case.input_column,
                    "input_table": case.input_table,
                    "when_count": len(case.when_clauses),
                    "when_clauses": [
                        {
                            "condition": wc.condition.raw_condition if wc.condition else "",
                            "result": wc.result_value,
                            "operator": wc.condition.operator.value if wc.condition else None,
                            "values": wc.condition.values if wc.condition else [],
                        }
                        for wc in case.when_clauses
                    ],
                    "else_value": case.else_value,
                    "detected_pattern": case.detected_pattern,
                    "entity_type": case.detected_entity_type.value if case.detected_entity_type else "unknown",
                }
                result["case_statements"].append(case_data)

            # Detect hierarchies if requested
            if include_hierarchy and cases:
                from databridge_discovery.hierarchy.case_to_hierarchy import CaseToHierarchyConverter

                converter = CaseToHierarchyConverter()
                hierarchies = []

                for case in cases:
                    try:
                        hierarchy = converter.convert(case)
                        hierarchies.append({
                            "id": hierarchy.id,
                            "name": hierarchy.name,
                            "entity_type": hierarchy.entity_type,
                            "level_count": hierarchy.level_count,
                            "total_nodes": hierarchy.total_nodes,
                            "confidence": hierarchy.confidence,
                        })
                    except Exception:
                        pass

                result["hierarchies"] = hierarchies

            self._report_progress("CASE extraction complete", 1.0)

            return result

        except ImportError:
            # Fallback to basic extraction
            return self._basic_extract_case(sql)

    def _identify_calcs(self, context: TaskContext, **kwargs: Any) -> dict[str, Any]:
        """
        Identify calculations and formulas in SQL.

        Input data:
            - sql: SQL statement
            - parsed: Optional pre-parsed SQL
        """
        self._report_progress("Identifying calculations", 0.0)

        input_data = context.input_data
        sql = input_data.get("sql", "")

        if not sql:
            raise AgentError("No SQL provided", self.name, "identify_calcs")

        calculations: list[ExtractedCalculation] = []

        # Extract from SELECT clause
        select_calcs = self._extract_select_calculations(sql)
        calculations.extend(select_calcs)

        # Extract from WHERE clause
        where_calcs = self._extract_where_calculations(sql)
        calculations.extend(where_calcs)

        self._report_progress("Calculation identification complete", 1.0)

        return {
            "calculation_count": len(calculations),
            "calculations": [c.to_dict() for c in calculations],
            "by_type": self._group_calculations_by_type(calculations),
        }

    def _detect_aggregations(self, context: TaskContext, **kwargs: Any) -> dict[str, Any]:
        """
        Detect aggregation patterns in SQL.

        Input data:
            - sql: SQL statement
        """
        self._report_progress("Detecting aggregations", 0.0)

        input_data = context.input_data
        sql = input_data.get("sql", "")

        if not sql:
            raise AgentError("No SQL provided", self.name, "detect_aggregations")

        aggregations: list[AggregationPattern] = []

        # Find aggregation functions
        import re

        sql_upper = sql.upper()

        for func in self.AGG_FUNCTIONS:
            # Pattern: FUNC(column) or FUNC(DISTINCT column)
            pattern = rf'{func}\s*\(\s*(DISTINCT\s+)?([^)]+)\)'
            matches = re.findall(pattern, sql_upper)

            for distinct, column in matches:
                # Clean up column name
                col = column.strip()

                # Try to find alias
                alias = self._find_alias_for_expression(sql, f"{func}({column})")

                agg = AggregationPattern(
                    function=func,
                    column=col if col != "*" else None,
                    alias=alias,
                    is_distinct=bool(distinct),
                )
                aggregations.append(agg)

        # Extract GROUP BY columns
        group_by = self._extract_group_by(sql)
        for agg in aggregations:
            agg.group_by_columns = group_by

        self._report_progress("Aggregation detection complete", 1.0)

        return {
            "aggregation_count": len(aggregations),
            "aggregations": [a.to_dict() for a in aggregations],
            "group_by_columns": group_by,
            "has_having": "HAVING" in sql_upper,
        }

    def _basic_parse_sql(self, sql: str) -> dict[str, Any]:
        """Basic SQL parsing fallback."""
        import re

        sql_upper = sql.upper()

        # Determine query type
        if sql_upper.strip().startswith("SELECT"):
            query_type = "SELECT"
        elif sql_upper.strip().startswith("INSERT"):
            query_type = "INSERT"
        elif sql_upper.strip().startswith("UPDATE"):
            query_type = "UPDATE"
        elif sql_upper.strip().startswith("DELETE"):
            query_type = "DELETE"
        elif sql_upper.strip().startswith("CREATE"):
            query_type = "CREATE"
        else:
            query_type = "UNKNOWN"

        # Extract tables
        tables = []
        table_pattern = r'FROM\s+([^\s,()]+)|JOIN\s+([^\s,()]+)'
        for match in re.finditer(table_pattern, sql, re.IGNORECASE):
            table_name = match.group(1) or match.group(2)
            if table_name and table_name.upper() not in ["SELECT", "WHERE", "AND", "OR"]:
                tables.append({"name": table_name.strip('`"[]')})

        # Check for features
        has_case = "CASE" in sql_upper
        has_subquery = sql_upper.count("SELECT") > 1
        has_join = "JOIN" in sql_upper
        has_group_by = "GROUP BY" in sql_upper

        return {
            "query_type": query_type,
            "tables": tables,
            "columns": [],
            "joins": [],
            "metrics": {
                "has_case": has_case,
                "has_subquery": has_subquery,
                "has_join": has_join,
                "has_group_by": has_group_by,
            },
            "has_case": has_case,
            "has_subquery": has_subquery,
            "complexity_score": sum([
                has_case * 2,
                has_subquery * 3,
                has_join * 1,
                has_group_by * 1,
            ]),
        }

    def _basic_extract_case(self, sql: str) -> dict[str, Any]:
        """Basic CASE extraction fallback."""
        import re

        cases = []

        # Simple CASE pattern
        case_pattern = r'CASE\s+(.*?)\s+END(?:\s+AS\s+(\w+))?'
        matches = re.findall(case_pattern, sql, re.IGNORECASE | re.DOTALL)

        for content, alias in matches:
            # Extract WHEN clauses
            when_pattern = r'WHEN\s+(.+?)\s+THEN\s+([^\s]+)'
            when_matches = re.findall(when_pattern, content, re.IGNORECASE)

            when_clauses = [
                {"condition": cond.strip(), "result": result.strip("'")}
                for cond, result in when_matches
            ]

            # Extract ELSE
            else_match = re.search(r'ELSE\s+([^\s]+)', content, re.IGNORECASE)
            else_clause = else_match.group(1).strip("'") if else_match else None

            cases.append({
                "alias": alias or "unknown",
                "when_count": len(when_clauses),
                "when_clauses": when_clauses,
                "else_clause": else_clause,
            })

        return {
            "case_count": len(cases),
            "case_statements": cases,
        }

    def _extract_select_calculations(self, sql: str) -> list[ExtractedCalculation]:
        """Extract calculations from SELECT clause."""
        import re

        calculations = []

        # Find SELECT clause
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        if not select_match:
            return calculations

        select_clause = select_match.group(1)

        # Split by comma (being careful about nested parentheses)
        columns = self._split_select_columns(select_clause)

        for col_expr in columns:
            col_expr = col_expr.strip()

            # Skip simple column references
            if re.match(r'^[\w.]+$', col_expr):
                continue

            # Check for CASE
            if "CASE" in col_expr.upper():
                alias = self._extract_alias(col_expr)
                calculations.append(ExtractedCalculation(
                    expression=col_expr,
                    alias=alias,
                    calc_type="case",
                ))
                continue

            # Check for arithmetic
            if any(op in col_expr for op in ["+", "-", "*", "/", "%"]):
                alias = self._extract_alias(col_expr)
                operators = [op for op in ["+", "-", "*", "/", "%"] if op in col_expr]
                calculations.append(ExtractedCalculation(
                    expression=col_expr,
                    alias=alias,
                    calc_type="expression",
                    operators=operators,
                ))
                continue

            # Check for functions
            func_match = re.search(r'(\w+)\s*\(', col_expr)
            if func_match:
                func_name = func_match.group(1).upper()
                alias = self._extract_alias(col_expr)

                if func_name in self.AGG_FUNCTIONS:
                    calc_type = "aggregation"
                elif func_name in self.WINDOW_FUNCTIONS:
                    calc_type = "window"
                else:
                    calc_type = "function"

                calculations.append(ExtractedCalculation(
                    expression=col_expr,
                    alias=alias,
                    calc_type=calc_type,
                    functions=[func_name],
                ))

        return calculations

    def _extract_where_calculations(self, sql: str) -> list[ExtractedCalculation]:
        """Extract calculations from WHERE clause."""
        import re

        calculations = []

        # Find WHERE clause
        where_match = re.search(
            r'WHERE\s+(.*?)(?:GROUP BY|ORDER BY|HAVING|LIMIT|$)',
            sql,
            re.IGNORECASE | re.DOTALL
        )
        if not where_match:
            return calculations

        where_clause = where_match.group(1)

        # Check for calculations in WHERE
        if any(op in where_clause for op in ["+", "-", "*", "/"]):
            calculations.append(ExtractedCalculation(
                expression=where_clause.strip(),
                calc_type="filter_expression",
            ))

        return calculations

    def _split_select_columns(self, select_clause: str) -> list[str]:
        """Split SELECT columns handling nested parentheses."""
        columns = []
        current = ""
        depth = 0

        for char in select_clause:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif char == "," and depth == 0:
                columns.append(current.strip())
                current = ""
                continue
            current += char

        if current.strip():
            columns.append(current.strip())

        return columns

    def _extract_alias(self, expression: str) -> str | None:
        """Extract alias from expression."""
        import re

        # Pattern: expression AS alias
        match = re.search(r'\s+AS\s+["\']?(\w+)["\']?\s*$', expression, re.IGNORECASE)
        if match:
            return match.group(1)

        return None

    def _extract_group_by(self, sql: str) -> list[str]:
        """Extract GROUP BY columns."""
        import re

        match = re.search(
            r'GROUP\s+BY\s+(.*?)(?:HAVING|ORDER BY|LIMIT|$)',
            sql,
            re.IGNORECASE | re.DOTALL
        )
        if not match:
            return []

        group_clause = match.group(1)
        columns = [c.strip() for c in group_clause.split(",")]

        return columns

    def _find_alias_for_expression(self, sql: str, expression: str) -> str | None:
        """Find alias for an expression in SQL."""
        import re

        # Pattern: expression AS alias
        pattern = re.escape(expression) + r'\s+AS\s+["\']?(\w+)["\']?'
        match = re.search(pattern, sql, re.IGNORECASE)

        if match:
            return match.group(1)
        return None

    def _group_calculations_by_type(
        self,
        calculations: list[ExtractedCalculation],
    ) -> dict[str, int]:
        """Group calculations by type."""
        groups: dict[str, int] = {}

        for calc in calculations:
            groups[calc.calc_type] = groups.get(calc.calc_type, 0) + 1

        return groups

    def _calculate_complexity(self, parsed: Any) -> int:
        """Calculate query complexity score."""
        score = 0

        # Base complexity
        if hasattr(parsed, "metrics"):
            metrics = parsed.metrics
            # Handle both Pydantic model and dict
            if hasattr(metrics, 'table_count'):
                score += metrics.table_count or 0
                score += (metrics.join_count or 0) * 2
                score += (metrics.case_statement_count or 0) * 2
                score += (metrics.nesting_depth or 0) * 3
                score += (metrics.cte_count or 0) * 2
                if metrics.has_group_by:
                    score += 1
                if metrics.aggregation_count and metrics.aggregation_count > 0:
                    score += metrics.aggregation_count
            else:
                # Fallback to dict access
                score += metrics.get("table_count", 0)
                score += metrics.get("join_count", 0) * 2
                score += metrics.get("case_statement_count", 0) * 2
                score += metrics.get("subquery_depth", 0) * 3
                score += metrics.get("cte_count", 0) * 2
                if metrics.get("has_group_by"):
                    score += 1
                if metrics.get("aggregation_count", 0) > 0:
                    score += metrics.get("aggregation_count", 0)

        return score
