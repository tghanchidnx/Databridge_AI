"""
CASE Statement Extractor for hierarchy detection.

This module extracts CASE WHEN statements from SQL AST and analyzes
them to detect potential hierarchies, entity types, and patterns.
"""

from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from typing import Any

import sqlglot
from sqlglot import exp

from databridge_discovery.models.case_statement import (
    CaseCondition,
    CaseStatement,
    CaseWhen,
    ConditionOperator,
    EntityType,
    ExtractedHierarchy,
    HierarchyLevel,
)


# Patterns for entity type detection
ENTITY_PATTERNS = {
    EntityType.ACCOUNT: [
        r"account[_\s]*(code|id|num|number|name)?",
        r"acct[_\s]*(code|id|num)?",
        r"gl[_\s]*(account|code)",
        r"chart[_\s]*of[_\s]*accounts",
    ],
    EntityType.COST_CENTER: [
        r"cost[_\s]*center",
        r"cc[_\s]*(code|id)",
        r"profit[_\s]*center",
        r"responsibility[_\s]*center",
    ],
    EntityType.DEPARTMENT: [
        r"department",
        r"dept[_\s]*(code|id|name)?",
        r"division",
        r"business[_\s]*unit",
    ],
    EntityType.ENTITY: [
        r"entity[_\s]*(code|id|name)?",
        r"legal[_\s]*entity",
        r"company[_\s]*(code|id)?",
        r"corp[_\s]*(code|id|name)?",
        r"subsidiary",
    ],
    EntityType.PROJECT: [
        r"project[_\s]*(code|id|name)?",
        r"work[_\s]*order",
        r"job[_\s]*(code|id)?",
        r"wbs",
    ],
    EntityType.PRODUCT: [
        r"product[_\s]*(code|id|name)?",
        r"sku",
        r"item[_\s]*(code|id)?",
        r"material",
    ],
    EntityType.CUSTOMER: [
        r"customer[_\s]*(code|id|name)?",
        r"client[_\s]*(code|id)?",
        r"buyer",
    ],
    EntityType.VENDOR: [
        r"vendor[_\s]*(code|id|name)?",
        r"supplier",
        r"business[_\s]*associate",
    ],
    EntityType.EMPLOYEE: [
        r"employee[_\s]*(code|id|name)?",
        r"emp[_\s]*(code|id)?",
        r"worker",
        r"staff",
    ],
    EntityType.LOCATION: [
        r"location[_\s]*(code|id)?",
        r"site[_\s]*(code|id)?",
        r"property",
        r"facility",
        r"warehouse",
    ],
    EntityType.TIME_PERIOD: [
        r"period",
        r"fiscal[_\s]*(year|month|quarter)",
        r"date[_\s]*key",
        r"time[_\s]*period",
    ],
    EntityType.CURRENCY: [
        r"currency[_\s]*(code|id)?",
        r"curr[_\s]*(code)?",
        r"fx",
    ],
}

# Financial hierarchy patterns based on result values
FINANCIAL_HIERARCHY_PATTERNS = {
    "balance_sheet": [
        "Cash", "Accounts Receivable", "AR", "Inventory", "Prepaid",
        "Fixed Assets", "Accumulated Depreciation", "Intangible",
        "Accounts Payable", "AP", "Accrued", "Debt", "Notes Payable",
        "Equity", "Retained Earnings", "Capital",
    ],
    "income_statement": [
        "Revenue", "Sales", "Income", "COGS", "Cost of Goods",
        "Gross Profit", "Operating Expenses", "SG&A", "R&D",
        "EBITDA", "Depreciation", "Interest", "Tax", "Net Income",
    ],
    "oil_gas_los": [
        "Oil Sales", "Gas Sales", "NGL Sales", "Severance Tax",
        "Ad Valorem", "LOE", "Lease Operating", "Gathering",
        "Transportation", "Compression", "Processing", "DD&A",
        "G&A", "Interest", "Hedge",
    ],
}


class CaseExtractor:
    """
    Extracts and analyzes CASE statements from SQL for hierarchy detection.

    This class parses SQL to find CASE WHEN statements, extracts their
    conditions and results, detects entity types, and identifies potential
    hierarchies that can be built from the CASE logic.

    Example:
        extractor = CaseExtractor()
        cases = extractor.extract_from_sql(sql_query)
        for case in cases:
            print(f"Found {case.detected_entity_type}: {case.source_column}")
    """

    def __init__(self, dialect: str = "snowflake"):
        """
        Initialize the CASE extractor.

        Args:
            dialect: SQL dialect for parsing
        """
        self.dialect = dialect

    def extract_from_sql(self, sql: str) -> list[CaseStatement]:
        """
        Extract all CASE statements from SQL.

        Args:
            sql: SQL query containing CASE statements

        Returns:
            List of CaseStatement objects
        """
        try:
            statements = sqlglot.parse(sql, dialect=self.dialect)
            if not statements:
                return []

            cases: list[CaseStatement] = []
            for stmt in statements:
                cases.extend(self._extract_from_ast(stmt))

            return cases

        except Exception as e:
            # Log error but return empty list
            return []

    def extract_from_ast(self, ast: exp.Expression) -> list[CaseStatement]:
        """
        Extract CASE statements from a parsed AST.

        Args:
            ast: sqlglot AST expression

        Returns:
            List of CaseStatement objects
        """
        return self._extract_from_ast(ast)

    def _extract_from_ast(self, ast: exp.Expression) -> list[CaseStatement]:
        """Internal method to extract CASE statements from AST."""
        cases: list[CaseStatement] = []

        # Find all CASE expressions
        for idx, case_expr in enumerate(ast.find_all(exp.Case)):
            case_stmt = self._parse_case_expression(case_expr, idx)
            if case_stmt:
                cases.append(case_stmt)

        return cases

    def _parse_case_expression(self, case_expr: exp.Case, position: int) -> CaseStatement | None:
        """Parse a single CASE expression into a CaseStatement."""
        try:
            # Get the raw SQL
            raw_sql = case_expr.sql(dialect=self.dialect)

            # Generate ID from SQL
            case_id = hashlib.md5(raw_sql.encode()).hexdigest()[:12]

            # Try to determine input column (what's being tested)
            input_column = self._detect_input_column(case_expr)
            input_table = self._detect_input_table(case_expr)

            # Try to get output column name from alias
            source_column = self._get_source_column(case_expr, position)

            # Extract WHEN clauses
            when_clauses: list[CaseWhen] = []
            for when_idx, when in enumerate(case_expr.args.get("ifs", [])):
                when_clause = self._parse_when_clause(when, when_idx)
                if when_clause:
                    when_clauses.append(when_clause)

            # Get ELSE value
            else_value = None
            if case_expr.args.get("default"):
                else_expr = case_expr.args["default"]
                else_value = self._extract_literal_value(else_expr)

            # Detect entity type
            entity_type = self._detect_entity_type(input_column, when_clauses)

            # Detect pattern type
            pattern = self._detect_pattern_type(when_clauses)

            # Get unique result values
            result_values = list(set(w.result_value for w in when_clauses))
            if else_value:
                result_values.append(else_value)

            return CaseStatement(
                id=case_id,
                source_column=source_column,
                input_column=input_column or "unknown",
                input_table=input_table,
                when_clauses=when_clauses,
                else_value=else_value,
                detected_entity_type=entity_type,
                detected_pattern=pattern,
                unique_result_values=result_values,
                condition_count=len(when_clauses),
                raw_case_sql=raw_sql,
                position_in_query=position,
            )

        except Exception as e:
            return None

    def _detect_input_column(self, case_expr: exp.Case) -> str | None:
        """Detect the primary input column being tested in CASE."""
        # Look at first WHEN condition to find the tested column
        ifs = case_expr.args.get("ifs", [])
        if not ifs:
            return None

        first_when = ifs[0]
        if hasattr(first_when, "this"):
            condition = first_when.this

            # Look for Column references
            for col in condition.find_all(exp.Column):
                return col.name

        return None

    def _detect_input_table(self, case_expr: exp.Case) -> str | None:
        """Detect the table of the input column."""
        ifs = case_expr.args.get("ifs", [])
        if not ifs:
            return None

        first_when = ifs[0]
        if hasattr(first_when, "this"):
            condition = first_when.this

            for col in condition.find_all(exp.Column):
                if col.table:
                    return col.table

        return None

    def _get_source_column(self, case_expr: exp.Case, position: int) -> str:
        """Get the column name this CASE creates (from alias)."""
        # Check parent for alias
        parent = case_expr.parent
        if isinstance(parent, exp.Alias):
            return parent.alias

        # Generate a default name
        return f"case_column_{position}"

    def _parse_when_clause(self, when: exp.Expression, position: int) -> CaseWhen | None:
        """Parse a single WHEN clause."""
        try:
            # Get condition
            condition_expr = when.this if hasattr(when, "this") else None
            if not condition_expr:
                return None

            condition = self._parse_condition(condition_expr)

            # Get THEN value
            then_expr = when.args.get("true") if hasattr(when, "args") else None
            result_value = self._extract_literal_value(then_expr) if then_expr else "NULL"

            # Get raw SQL
            raw_sql = when.sql(dialect=self.dialect)

            return CaseWhen(
                condition=condition,
                result_value=result_value,
                result_type=self._infer_result_type(result_value),
                position=position,
                raw_when_clause=raw_sql,
            )

        except Exception as e:
            return None

    def _parse_condition(self, expr: exp.Expression) -> CaseCondition:
        """Parse a condition expression into CaseCondition."""
        raw_condition = expr.sql(dialect=self.dialect)

        # Handle AND/OR compound conditions
        if isinstance(expr, exp.And):
            return CaseCondition(
                column="",
                operator=ConditionOperator.AND,
                values=[],
                raw_condition=raw_condition,
                left_condition=self._parse_condition(expr.this),
                right_condition=self._parse_condition(expr.expression),
            )
        elif isinstance(expr, exp.Or):
            return CaseCondition(
                column="",
                operator=ConditionOperator.OR,
                values=[],
                raw_condition=raw_condition,
                left_condition=self._parse_condition(expr.this),
                right_condition=self._parse_condition(expr.expression),
            )

        # Get column name
        column = ""
        for col in expr.find_all(exp.Column):
            column = col.name
            break

        # Determine operator and values
        operator, values = self._extract_operator_and_values(expr)

        # Check for negation
        is_negated = isinstance(expr, exp.Not)

        return CaseCondition(
            column=column,
            operator=operator,
            values=values,
            is_negated=is_negated,
            raw_condition=raw_condition,
        )

    def _extract_operator_and_values(
        self, expr: exp.Expression
    ) -> tuple[ConditionOperator, list[str]]:
        """Extract operator and comparison values from condition."""
        values: list[str] = []

        # Handle different expression types
        if isinstance(expr, exp.EQ):
            values = [self._extract_literal_value(expr.expression)]
            return ConditionOperator.EQUALS, values

        elif isinstance(expr, exp.NEQ):
            values = [self._extract_literal_value(expr.expression)]
            return ConditionOperator.NOT_EQUALS, values

        elif isinstance(expr, exp.Like):
            values = [self._extract_literal_value(expr.expression)]
            return ConditionOperator.LIKE, values

        elif isinstance(expr, exp.ILike):
            values = [self._extract_literal_value(expr.expression)]
            return ConditionOperator.ILIKE, values

        elif isinstance(expr, exp.In):
            # Get all values in the IN list
            in_values = expr.expressions if hasattr(expr, "expressions") else []
            values = [self._extract_literal_value(v) for v in in_values]
            return ConditionOperator.IN, values

        elif isinstance(expr, exp.Between):
            low = self._extract_literal_value(expr.args.get("low"))
            high = self._extract_literal_value(expr.args.get("high"))
            values = [low, high]
            return ConditionOperator.BETWEEN, values

        elif isinstance(expr, exp.Is):
            if expr.expression and isinstance(expr.expression, exp.Null):
                return ConditionOperator.IS_NULL, []
            return ConditionOperator.EQUALS, []

        elif isinstance(expr, exp.GT):
            values = [self._extract_literal_value(expr.expression)]
            return ConditionOperator.GREATER_THAN, values

        elif isinstance(expr, exp.GTE):
            values = [self._extract_literal_value(expr.expression)]
            return ConditionOperator.GREATER_EQUAL, values

        elif isinstance(expr, exp.LT):
            values = [self._extract_literal_value(expr.expression)]
            return ConditionOperator.LESS_THAN, values

        elif isinstance(expr, exp.LTE):
            values = [self._extract_literal_value(expr.expression)]
            return ConditionOperator.LESS_EQUAL, values

        # Handle ANY/ALL with arrays (Snowflake ILIKE ANY)
        elif hasattr(expr, "expression"):
            inner = expr.expression
            if isinstance(inner, exp.Tuple):
                values = [self._extract_literal_value(v) for v in inner.expressions]
                if isinstance(expr, exp.ILike):
                    return ConditionOperator.ILIKE, values
                elif isinstance(expr, exp.Like):
                    return ConditionOperator.LIKE, values

        # Default to EQUALS
        return ConditionOperator.EQUALS, values

    def _extract_literal_value(self, expr: exp.Expression | None) -> str:
        """Extract the literal value from an expression."""
        if expr is None:
            return "NULL"

        if isinstance(expr, exp.Literal):
            return str(expr.this)

        if isinstance(expr, exp.Null):
            return "NULL"

        # For complex expressions, get the SQL
        return expr.sql(dialect=self.dialect)

    def _infer_result_type(self, value: str) -> str:
        """Infer the type of a result value."""
        if value == "NULL":
            return "null"

        # Try to parse as number
        try:
            float(value)
            if "." in value:
                return "decimal"
            return "integer"
        except ValueError:
            pass

        return "string"

    def _detect_entity_type(
        self, column_name: str | None, when_clauses: list[CaseWhen]
    ) -> EntityType:
        """Detect the entity type based on column name and patterns."""
        if not column_name:
            return EntityType.UNKNOWN

        column_lower = column_name.lower()

        # Check column name against patterns
        for entity_type, patterns in ENTITY_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, column_lower, re.IGNORECASE):
                    return entity_type

        # Check result values for financial patterns
        result_values = [w.result_value for w in when_clauses]
        for pattern_type, keywords in FINANCIAL_HIERARCHY_PATTERNS.items():
            matches = sum(1 for v in result_values if any(k.lower() in v.lower() for k in keywords))
            if matches >= 3:  # At least 3 matching keywords
                return EntityType.ACCOUNT

        return EntityType.UNKNOWN

    def _detect_pattern_type(self, when_clauses: list[CaseWhen]) -> str | None:
        """Detect the type of pattern used in conditions."""
        if not when_clauses:
            return None

        # Count pattern types
        pattern_counts = defaultdict(int)

        for when in when_clauses:
            condition = when.condition

            if condition.operator in (ConditionOperator.LIKE, ConditionOperator.ILIKE):
                for value in condition.values:
                    if value.endswith("%") and not value.startswith("%"):
                        pattern_counts["prefix"] += 1
                    elif value.startswith("%") and not value.endswith("%"):
                        pattern_counts["suffix"] += 1
                    elif value.startswith("%") and value.endswith("%"):
                        pattern_counts["contains"] += 1
                    else:
                        pattern_counts["exact"] += 1

            elif condition.operator == ConditionOperator.IN:
                pattern_counts["exact_list"] += 1

            elif condition.operator == ConditionOperator.EQUALS:
                pattern_counts["exact"] += 1

            elif condition.operator == ConditionOperator.BETWEEN:
                pattern_counts["range"] += 1

        if not pattern_counts:
            return None

        # Return most common pattern
        return max(pattern_counts.items(), key=lambda x: x[1])[0]

    def extract_hierarchy(self, case_stmt: CaseStatement) -> ExtractedHierarchy | None:
        """
        Extract a hierarchy structure from a CASE statement.

        Args:
            case_stmt: CaseStatement to analyze

        Returns:
            ExtractedHierarchy if structure detected, None otherwise
        """
        if not case_stmt.when_clauses:
            return None

        # Group conditions by result value to find hierarchy structure
        result_groups: dict[str, list[CaseWhen]] = defaultdict(list)
        for when in case_stmt.when_clauses:
            result_groups[when.result_value].append(when)

        # Create hierarchy levels
        levels: list[HierarchyLevel] = []
        unique_results = list(result_groups.keys())

        # Single level hierarchy with all result values
        level = HierarchyLevel(
            level_number=1,
            level_name=case_stmt.source_column,
            values=unique_results,
            sort_order_map={v: i for i, v in enumerate(unique_results)},
        )
        levels.append(level)

        # Build value to node mapping
        value_to_node: dict[str, dict[str, Any]] = {}
        for when in case_stmt.when_clauses:
            for value in when.condition.values:
                value_to_node[value] = {
                    "result": when.result_value,
                    "level": 1,
                    "position": when.position,
                }

        # Calculate confidence
        confidence, notes = self._calculate_confidence(case_stmt, levels)

        return ExtractedHierarchy(
            id=f"hier_{case_stmt.id}",
            name=case_stmt.source_column,
            source_case_id=case_stmt.id,
            entity_type=case_stmt.detected_entity_type,
            levels=levels,
            total_levels=len(levels),
            total_nodes=len(unique_results),
            value_to_node=value_to_node,
            source_column=case_stmt.input_column,
            source_table=case_stmt.input_table,
            confidence_score=confidence,
            confidence_notes=notes,
        )

    def _calculate_confidence(
        self, case_stmt: CaseStatement, levels: list[HierarchyLevel]
    ) -> tuple[float, list[str]]:
        """Calculate confidence score for hierarchy extraction."""
        score = 0.5  # Base confidence
        notes: list[str] = []

        # More WHEN clauses = higher confidence
        if len(case_stmt.when_clauses) >= 10:
            score += 0.2
            notes.append("Many conditions (10+) indicates comprehensive mapping")
        elif len(case_stmt.when_clauses) >= 5:
            score += 0.1
            notes.append("Moderate conditions (5+)")

        # Known entity type = higher confidence
        if case_stmt.detected_entity_type != EntityType.UNKNOWN:
            score += 0.15
            notes.append(f"Detected entity type: {case_stmt.detected_entity_type.value}")

        # Pattern match = higher confidence
        if case_stmt.detected_pattern:
            score += 0.1
            notes.append(f"Consistent pattern: {case_stmt.detected_pattern}")

        # Unique result values (good for hierarchy)
        unique_ratio = len(case_stmt.unique_result_values) / max(len(case_stmt.when_clauses), 1)
        if unique_ratio < 0.5:
            score += 0.1
            notes.append("Good rollup ratio - multiple inputs map to same outputs")

        return min(score, 1.0), notes

    def find_nested_hierarchies(
        self, cases: list[CaseStatement]
    ) -> list[tuple[CaseStatement, CaseStatement]]:
        """
        Find CASE statements that might form nested hierarchies.

        Returns pairs where the first CASE's output appears in the second CASE's input.
        """
        pairs: list[tuple[CaseStatement, CaseStatement]] = []

        for case1 in cases:
            results1 = set(case1.unique_result_values)

            for case2 in cases:
                if case1.id == case2.id:
                    continue

                # Check if case1's results appear in case2's inputs
                inputs2 = set()
                for when in case2.when_clauses:
                    inputs2.update(when.condition.values)

                overlap = results1 & inputs2
                if overlap:
                    pairs.append((case1, case2))

        return pairs
