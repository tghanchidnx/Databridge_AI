"""
MCP tools for SQL-to-Hierarchy conversion.

Provides tools to extract hierarchies from SQL CASE statements:
- sql_to_hierarchy: Parse SQL and create hierarchies from CASE statements
- analyze_sql_for_hierarchies: Analyze SQL without creating hierarchies
"""

from __future__ import annotations

import hashlib
import re
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from fastmcp import FastMCP


class EntityType(str, Enum):
    """Entity types that can be detected from SQL."""
    ACCOUNT = "account"
    COST_CENTER = "cost_center"
    DEPARTMENT = "department"
    ENTITY = "entity"
    PROJECT = "project"
    PRODUCT = "product"
    CUSTOMER = "customer"
    VENDOR = "vendor"
    EMPLOYEE = "employee"
    LOCATION = "location"
    TIME_PERIOD = "time_period"
    CURRENCY = "currency"
    UNKNOWN = "unknown"


class ConditionOperator(str, Enum):
    """SQL condition operators."""
    EQUALS = "="
    NOT_EQUALS = "<>"
    LIKE = "LIKE"
    ILIKE = "ILIKE"
    IN = "IN"
    BETWEEN = "BETWEEN"
    IS_NULL = "IS NULL"
    GREATER_THAN = ">"
    GREATER_EQUAL = ">="
    LESS_THAN = "<"
    LESS_EQUAL = "<="
    AND = "AND"
    OR = "OR"


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
    ],
    EntityType.PRODUCT: [
        r"product[_\s]*(code|id|name)?",
        r"sku",
        r"item[_\s]*(code|id)?",
    ],
    EntityType.LOCATION: [
        r"location[_\s]*(code|id)?",
        r"site[_\s]*(code|id)?",
        r"facility",
        r"warehouse",
    ],
}

# Financial hierarchy patterns based on result values
FINANCIAL_PATTERNS = [
    "Revenue", "Sales", "Income", "COGS", "Cost of Goods",
    "Gross Profit", "Operating Expenses", "SG&A", "R&D",
    "EBITDA", "Depreciation", "Interest", "Tax", "Net Income",
    "Cash", "Accounts Receivable", "Inventory", "Fixed Assets",
    "Accounts Payable", "Debt", "Equity", "Retained Earnings",
]


@dataclass
class CaseCondition:
    """A condition from a CASE WHEN clause."""
    column: str
    operator: ConditionOperator
    values: List[str]
    raw_condition: str
    is_negated: bool = False


@dataclass
class CaseWhen:
    """A WHEN clause from a CASE statement."""
    condition: CaseCondition
    result_value: str
    position: int
    raw_sql: str


@dataclass
class ExtractedCase:
    """An extracted CASE statement with metadata."""
    id: str
    source_column: str
    input_column: str
    input_table: Optional[str]
    when_clauses: List[CaseWhen]
    else_value: Optional[str]
    entity_type: EntityType
    pattern_type: Optional[str]
    unique_results: List[str]
    raw_sql: str


@dataclass
class HierarchyNode:
    """A node in the extracted hierarchy."""
    id: str
    name: str
    value: str
    level: int
    parent_id: Optional[str] = None
    children: List[str] = field(default_factory=list)
    sort_order: int = 0
    source_values: List[str] = field(default_factory=list)


@dataclass
class ConvertedHierarchy:
    """Result of converting a CASE statement to hierarchy."""
    id: str
    name: str
    entity_type: EntityType
    nodes: Dict[str, HierarchyNode]
    root_nodes: List[str]
    level_count: int
    total_nodes: int
    source_column: str
    source_table: Optional[str]
    mapping: Dict[str, str]  # source_value -> node_id
    confidence: float
    notes: List[str] = field(default_factory=list)


class SimpleCaseExtractor:
    """
    Simple CASE statement extractor using regex patterns.

    This is a fallback when sqlglot is not available, handling common
    CASE WHEN patterns without full SQL parsing.
    """

    def __init__(self, dialect: str = "snowflake"):
        self.dialect = dialect

    def extract_from_sql(self, sql: str) -> List[ExtractedCase]:
        """Extract CASE statements from SQL using regex."""
        cases = []

        # Pattern to match CASE statements
        case_pattern = r"CASE\s+(.*?)\s+END(?:\s+AS\s+(\w+))?"

        for idx, match in enumerate(re.finditer(case_pattern, sql, re.IGNORECASE | re.DOTALL)):
            case_body = match.group(1)
            alias = match.group(2) or f"case_column_{idx}"

            case_stmt = self._parse_case_body(case_body, alias, idx, match.group(0))
            if case_stmt:
                cases.append(case_stmt)

        return cases

    def _parse_case_body(
        self,
        case_body: str,
        alias: str,
        position: int,
        raw_sql: str,
    ) -> Optional[ExtractedCase]:
        """Parse the body of a CASE statement."""
        when_clauses = []
        else_value = None
        input_column = None
        input_table = None

        # Extract WHEN clauses
        when_pattern = r"WHEN\s+(.*?)\s+THEN\s+['\"]?([^'\"]+?)['\"]?\s*(?=WHEN|ELSE|$)"

        for when_idx, when_match in enumerate(re.finditer(when_pattern, case_body, re.IGNORECASE | re.DOTALL)):
            condition_str = when_match.group(1).strip()
            result_value = when_match.group(2).strip().strip("'\"")

            condition = self._parse_condition(condition_str)
            if condition and not input_column:
                input_column = condition.column

            when_clauses.append(CaseWhen(
                condition=condition or CaseCondition(
                    column="unknown",
                    operator=ConditionOperator.EQUALS,
                    values=[],
                    raw_condition=condition_str,
                ),
                result_value=result_value,
                position=when_idx,
                raw_sql=when_match.group(0),
            ))

        # Extract ELSE value
        else_pattern = r"ELSE\s+['\"]?([^'\"]+?)['\"]?\s*$"
        else_match = re.search(else_pattern, case_body, re.IGNORECASE)
        if else_match:
            else_value = else_match.group(1).strip().strip("'\"")

        if not when_clauses:
            return None

        # Generate ID
        case_id = hashlib.md5(raw_sql.encode()).hexdigest()[:12]

        # Detect entity type
        entity_type = self._detect_entity_type(input_column, when_clauses)

        # Detect pattern type
        pattern_type = self._detect_pattern_type(when_clauses)

        # Get unique results
        unique_results = list(set(w.result_value for w in when_clauses))
        if else_value:
            unique_results.append(else_value)

        return ExtractedCase(
            id=case_id,
            source_column=alias,
            input_column=input_column or "unknown",
            input_table=input_table,
            when_clauses=when_clauses,
            else_value=else_value,
            entity_type=entity_type,
            pattern_type=pattern_type,
            unique_results=unique_results,
            raw_sql=raw_sql,
        )

    def _parse_condition(self, condition_str: str) -> Optional[CaseCondition]:
        """Parse a condition string into CaseCondition."""
        condition_str = condition_str.strip()

        # LIKE pattern: column LIKE 'value%'
        like_pattern = r"(\w+)\s+(I?LIKE)\s+['\"]([^'\"]+)['\"]"
        like_match = re.match(like_pattern, condition_str, re.IGNORECASE)
        if like_match:
            return CaseCondition(
                column=like_match.group(1),
                operator=ConditionOperator.ILIKE if "ILIKE" in like_match.group(2).upper() else ConditionOperator.LIKE,
                values=[like_match.group(3)],
                raw_condition=condition_str,
            )

        # IN pattern: column IN ('val1', 'val2')
        in_pattern = r"(\w+)\s+IN\s*\(([^)]+)\)"
        in_match = re.match(in_pattern, condition_str, re.IGNORECASE)
        if in_match:
            values = [v.strip().strip("'\"") for v in in_match.group(2).split(",")]
            return CaseCondition(
                column=in_match.group(1),
                operator=ConditionOperator.IN,
                values=values,
                raw_condition=condition_str,
            )

        # BETWEEN pattern: column BETWEEN 'val1' AND 'val2'
        between_pattern = r"(\w+)\s+BETWEEN\s+['\"]?([^'\"]+)['\"]?\s+AND\s+['\"]?([^'\"]+)['\"]?"
        between_match = re.match(between_pattern, condition_str, re.IGNORECASE)
        if between_match:
            return CaseCondition(
                column=between_match.group(1),
                operator=ConditionOperator.BETWEEN,
                values=[between_match.group(2), between_match.group(3)],
                raw_condition=condition_str,
            )

        # Equals pattern: column = 'value'
        eq_pattern = r"(\w+)\s*=\s*['\"]?([^'\"]+)['\"]?"
        eq_match = re.match(eq_pattern, condition_str, re.IGNORECASE)
        if eq_match:
            return CaseCondition(
                column=eq_match.group(1),
                operator=ConditionOperator.EQUALS,
                values=[eq_match.group(2).strip()],
                raw_condition=condition_str,
            )

        return None

    def _detect_entity_type(
        self,
        column_name: Optional[str],
        when_clauses: List[CaseWhen],
    ) -> EntityType:
        """Detect entity type from column name and patterns."""
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
        matches = sum(
            1 for v in result_values
            if any(p.lower() in v.lower() for p in FINANCIAL_PATTERNS)
        )
        if matches >= 3:
            return EntityType.ACCOUNT

        return EntityType.UNKNOWN

    def _detect_pattern_type(self, when_clauses: List[CaseWhen]) -> Optional[str]:
        """Detect the pattern type used in conditions."""
        if not when_clauses:
            return None

        pattern_counts: Dict[str, int] = defaultdict(int)

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

        return max(pattern_counts.items(), key=lambda x: x[1])[0]


class CaseToHierarchyConverter:
    """Converts CASE statements into hierarchies."""

    def convert(self, case_stmt: ExtractedCase) -> ConvertedHierarchy:
        """Convert a CASE statement to a hierarchy."""
        hierarchy_id = f"hier_{case_stmt.id}"
        nodes: Dict[str, HierarchyNode] = {}
        mapping: Dict[str, str] = {}
        notes: List[str] = []

        # Group WHEN clauses by result value
        result_groups: Dict[str, List[CaseWhen]] = defaultdict(list)
        for when in case_stmt.when_clauses:
            result_groups[when.result_value].append(when)

        # Create nodes for each unique result
        sort_order = 0
        for result_value, when_clauses in result_groups.items():
            node_id = f"{hierarchy_id}_{len(nodes)}"

            source_values = []
            for when in when_clauses:
                source_values.extend(when.condition.values)

            node = HierarchyNode(
                id=node_id,
                name=result_value,
                value=result_value,
                level=1,
                sort_order=sort_order,
                source_values=source_values,
            )
            nodes[node_id] = node

            for source_val in source_values:
                mapping[source_val] = node_id

            sort_order += 1

        # Add ELSE as a node
        if case_stmt.else_value:
            else_node_id = f"{hierarchy_id}_else"
            nodes[else_node_id] = HierarchyNode(
                id=else_node_id,
                name=case_stmt.else_value,
                value=case_stmt.else_value,
                level=1,
                sort_order=sort_order,
            )

        # Infer sort orders from numeric prefixes
        self._infer_sort_orders(nodes)

        root_nodes = list(nodes.keys())
        confidence = self._calculate_confidence(case_stmt, nodes)

        return ConvertedHierarchy(
            id=hierarchy_id,
            name=case_stmt.source_column,
            entity_type=case_stmt.entity_type,
            nodes=nodes,
            root_nodes=root_nodes,
            level_count=1,
            total_nodes=len(nodes),
            source_column=case_stmt.input_column,
            source_table=case_stmt.input_table,
            mapping=mapping,
            confidence=confidence,
            notes=notes,
        )

    def _infer_sort_orders(self, nodes: Dict[str, HierarchyNode]) -> None:
        """Infer sort orders from patterns in values."""
        numeric_nodes = []
        for node in nodes.values():
            prefix = self._extract_numeric_prefix(node.value)
            if prefix is not None:
                numeric_nodes.append((prefix, node))

        if len(numeric_nodes) == len(nodes):
            numeric_nodes.sort(key=lambda x: x[0])
            for idx, (_, node) in enumerate(numeric_nodes):
                node.sort_order = idx

    def _extract_numeric_prefix(self, value: str) -> Optional[int]:
        """Extract leading numeric prefix from a value."""
        match = re.match(r'^(\d+)', value)
        if match:
            return int(match.group(1))
        return None

    def _calculate_confidence(
        self,
        case_stmt: ExtractedCase,
        nodes: Dict[str, HierarchyNode],
    ) -> float:
        """Calculate confidence score."""
        confidence = 0.5

        if len(case_stmt.when_clauses) >= 10:
            confidence += 0.15
        elif len(case_stmt.when_clauses) >= 5:
            confidence += 0.1

        if case_stmt.entity_type != EntityType.UNKNOWN:
            confidence += 0.15

        if case_stmt.pattern_type:
            confidence += 0.1

        return min(confidence, 1.0)

    def to_librarian_hierarchy_rows(
        self,
        converted: ConvertedHierarchy,
        hierarchy_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Convert to Librarian HIERARCHY.CSV format."""
        rows = []
        hier_name = hierarchy_name or converted.name

        sorted_nodes = sorted(
            converted.nodes.values(),
            key=lambda n: (n.level, n.sort_order),
        )

        for node in sorted_nodes:
            row: Dict[str, Any] = {
                "hierarchy_id": node.id,
                "hierarchy_name": hier_name,
                "parent_id": node.parent_id,
                "description": "",
                "include_flag": True,
                "sort_order": node.sort_order,
            }

            for i in range(1, 11):
                if i == node.level:
                    row[f"level_{i}"] = node.value
                    row[f"level_{i}_sort"] = node.sort_order
                else:
                    row[f"level_{i}"] = None
                    row[f"level_{i}_sort"] = None

            rows.append(row)

        return rows

    def to_librarian_mapping_rows(
        self,
        converted: ConvertedHierarchy,
        source_database: str = "",
        source_schema: str = "",
        source_table: str = "",
        source_column: str = "",
    ) -> List[Dict[str, Any]]:
        """Convert to Librarian MAPPING.CSV format."""
        rows = []

        table = source_table or converted.source_table or ""
        column = source_column or converted.source_column or ""

        for mapping_index, (source_value, node_id) in enumerate(converted.mapping.items()):
            rows.append({
                "hierarchy_id": node_id,
                "mapping_index": mapping_index,
                "source_database": source_database,
                "source_schema": source_schema,
                "source_table": table,
                "source_column": column,
                "source_uid": source_value,
                "precedence_group": "1",
                "include_flag": True,
            })

        return rows


def register_sql_discovery_tools(mcp: FastMCP) -> None:
    """Register SQL discovery tools with the MCP server."""

    from ...hierarchy.service import (
        HierarchyService,
        ProjectNotFoundError,
        DuplicateError,
    )

    @mcp.tool()
    def sql_to_hierarchy(
        sql: str,
        project_id: Optional[str] = None,
        hierarchy_name: Optional[str] = None,
        source_database: str = "",
        source_schema: str = "",
        source_table: str = "",
        source_column: str = "",
        dialect: str = "snowflake",
        create_hierarchies: bool = True,
    ) -> Dict[str, Any]:
        """
        Convert SQL CASE statements into hierarchies.

        Parses SQL containing CASE WHEN statements and extracts the mapping logic
        to create a hierarchy structure. Optionally creates the hierarchies in
        a project with source mappings.

        Args:
            sql: SQL containing CASE statements (e.g., "CASE WHEN account LIKE '4%' THEN 'Revenue' ...")
            project_id: Target project ID to create hierarchies in (optional)
            hierarchy_name: Override name for the hierarchy (optional)
            source_database: Database name for source mappings
            source_schema: Schema name for source mappings
            source_table: Table name for source mappings
            source_column: Column name for source mappings
            dialect: SQL dialect (snowflake, postgres, mysql, tsql, bigquery)
            create_hierarchies: If True and project_id provided, create hierarchies in project

        Returns:
            Dictionary containing:
            - success: Whether extraction succeeded
            - case_count: Number of CASE statements found
            - hierarchies: List of extracted hierarchies with:
              - hierarchy_id: Unique identifier
              - name: Hierarchy name
              - entity_type: Detected entity type
              - node_count: Number of nodes
              - confidence: Detection confidence (0-1)
              - nodes: List of hierarchy nodes
              - mappings: List of source mappings
            - created_in_project: Whether hierarchies were created (if project_id provided)

        Example:
            >>> sql = '''
            ... SELECT
            ...   CASE
            ...     WHEN account_code LIKE '4%' THEN 'Revenue'
            ...     WHEN account_code LIKE '5%' THEN 'Cost of Goods Sold'
            ...     WHEN account_code LIKE '6%' THEN 'Operating Expenses'
            ...     ELSE 'Other'
            ...   END AS account_category
            ... FROM gl_transactions
            ... '''
            >>> result = sql_to_hierarchy(sql, project_id="my-project")
            >>> print(result["hierarchies"][0]["nodes"])
            [{"name": "Revenue", "source_values": ["4%"]}, ...]
        """
        # Extract CASE statements
        extractor = SimpleCaseExtractor(dialect=dialect)
        cases = extractor.extract_from_sql(sql)

        if not cases:
            return {
                "success": False,
                "error": "No CASE statements found in SQL",
                "sql_preview": sql[:500] + "..." if len(sql) > 500 else sql,
                "hint": "SQL should contain CASE WHEN ... THEN ... END patterns",
            }

        # Convert to hierarchies
        converter = CaseToHierarchyConverter()
        hierarchies = []

        for case in cases:
            converted = converter.convert(case)

            # Build response
            hierarchy_rows = converter.to_librarian_hierarchy_rows(
                converted,
                hierarchy_name=hierarchy_name,
            )
            mapping_rows = converter.to_librarian_mapping_rows(
                converted,
                source_database=source_database,
                source_schema=source_schema,
                source_table=source_table,
                source_column=source_column,
            )

            hierarchies.append({
                "hierarchy_id": converted.id,
                "name": hierarchy_name or converted.name,
                "entity_type": converted.entity_type.value,
                "node_count": converted.total_nodes,
                "level_count": converted.level_count,
                "confidence": converted.confidence,
                "source_column": converted.source_column,
                "source_table": converted.source_table,
                "pattern_type": case.pattern_type,
                "nodes": [
                    {
                        "id": node.id,
                        "name": node.name,
                        "level": node.level,
                        "sort_order": node.sort_order,
                        "source_values": node.source_values,
                    }
                    for node in sorted(converted.nodes.values(), key=lambda n: n.sort_order)
                ],
                "mappings": mapping_rows,
                "hierarchy_csv_rows": hierarchy_rows,
            })

        result = {
            "success": True,
            "case_count": len(cases),
            "hierarchies": hierarchies,
            "primary_hierarchy": hierarchies[0] if hierarchies else None,
            "created_in_project": False,
        }

        # Create in project if requested
        if project_id and create_hierarchies and hierarchies:
            service = HierarchyService()
            created_count = 0
            create_errors = []

            try:
                # Verify project exists
                project = service.get_project(project_id)

                for hier in hierarchies:
                    # Create each node as a hierarchy
                    for node in hier["nodes"]:
                        try:
                            service.create_hierarchy(
                                project_id=project.id,
                                hierarchy_id=node["id"],
                                hierarchy_name=node["name"],
                                description=f"Auto-generated from SQL. Source values: {', '.join(node['source_values'][:5])}",
                                levels={"level_1": node["name"]},
                                sort_order=node["sort_order"],
                            )
                            created_count += 1

                            # Add source mappings
                            for mapping in hier["mappings"]:
                                if mapping["hierarchy_id"] == node["id"]:
                                    try:
                                        service.add_source_mapping(
                                            hierarchy_id=node["id"],
                                            source_database=mapping["source_database"],
                                            source_schema=mapping["source_schema"],
                                            source_table=mapping["source_table"],
                                            source_column=mapping["source_column"],
                                            source_uid=mapping["source_uid"],
                                            mapping_index=mapping["mapping_index"],
                                            precedence_group=mapping["precedence_group"],
                                        )
                                    except Exception as e:
                                        create_errors.append(f"Mapping error for {node['id']}: {str(e)}")

                        except DuplicateError:
                            create_errors.append(f"Duplicate hierarchy: {node['id']}")
                        except Exception as e:
                            create_errors.append(f"Error creating {node['id']}: {str(e)}")

                result["created_in_project"] = True
                result["project_id"] = project_id
                result["hierarchies_created"] = created_count
                result["create_errors"] = create_errors[:10] if create_errors else []

            except ProjectNotFoundError:
                result["create_error"] = f"Project not found: {project_id}"
            except Exception as e:
                result["create_error"] = str(e)

        return result

    @mcp.tool()
    def analyze_sql_for_hierarchies(
        sql: str,
        dialect: str = "snowflake",
        export_path: str = "",
        export_name: str = "",
    ) -> Dict[str, Any]:
        """
        Analyze SQL to identify potential hierarchies and export comprehensive CSVs.

        Scans SQL for CASE statements, detects entity types, and automatically
        exports results to CSV files including:
        - Summary CSV: High-level overview of all hierarchies found
        - Hierarchy CSV: Full tree structure with parent-child relationships
        - Mapping CSV: All source mappings with conditions

        Args:
            sql: SQL to analyze
            dialect: SQL dialect (snowflake, postgres, mysql, tsql, bigquery)
            export_path: Directory to export CSV files (default: ./result_export)
            export_name: Base name for export files (default: sql_analysis)

        Returns:
            Dictionary containing:
            - case_count: Number of CASE statements found
            - hierarchies: List of extracted hierarchies
            - exported_files: List of generated CSV file paths
            - summary: Quick summary of findings

        Example:
            >>> sql = "SELECT CASE WHEN account LIKE '4%' THEN 'Revenue' ... END"
            >>> result = analyze_sql_for_hierarchies(sql, export_name="my_analysis")
            >>> print(result["exported_files"])
            ["result_export/my_analysis_SUMMARY.csv", "result_export/my_analysis_HIERARCHY.csv", ...]
        """
        import csv
        import os
        from pathlib import Path
        from datetime import datetime

        extractor = SimpleCaseExtractor(dialect=dialect)
        cases = extractor.extract_from_sql(sql)

        if not cases:
            return {
                "success": True,
                "case_count": 0,
                "cases": [],
                "message": "No CASE statements found in SQL",
                "exported_files": [],
            }

        # Set up export directory
        if not export_path:
            export_path = "./result_export"
        Path(export_path).mkdir(parents=True, exist_ok=True)

        # Generate export name with timestamp if not provided
        if not export_name:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            export_name = f"sql_analysis_{timestamp}"

        # Convert cases to hierarchies
        converter = CaseToHierarchyConverter()
        hierarchies_data = []
        all_hierarchy_rows = []
        all_mapping_rows = []
        summary_rows = []

        for case in cases:
            converted = converter.convert(case)

            # Build hierarchy tree rows
            hier_name = case.source_column
            sort_idx = 0

            # Create parent node for the hierarchy
            parent_id = f"{hier_name.upper().replace(' ', '_')}"
            all_hierarchy_rows.append({
                "HIERARCHY_ID": parent_id,
                "HIERARCHY_NAME": hier_name,
                "PARENT_ID": "",
                "DESCRIPTION": f"Auto-extracted from SQL CASE statement on {case.input_column}",
                "LEVEL_1": hier_name,
                "LEVEL_2": "",
                "LEVEL_3": "",
                "CONDITION_TYPE": "",
                "CONDITION_VALUE": "",
                "INCLUDE_FLAG": "true",
                "ACTIVE_FLAG": "true",
                "IS_LEAF_NODE": "false",
                "SORT_ORDER": sort_idx,
            })
            sort_idx += 1

            # Group WHENs by result value to create category nodes
            result_groups: Dict[str, List[CaseWhen]] = defaultdict(list)
            for when in case.when_clauses:
                result_groups[when.result_value].append(when)

            # Create child nodes for each unique result
            for result_value, when_clauses in result_groups.items():
                child_id = f"{parent_id}_{result_value.upper().replace(' ', '_').replace('/', '_')[:20]}"

                # Collect all conditions for this result
                conditions = []
                for when in when_clauses:
                    cond = when.condition
                    op_str = cond.operator.value if hasattr(cond.operator, 'value') else str(cond.operator)
                    val_str = ", ".join(cond.values) if cond.values else ""
                    conditions.append(f"{op_str} {val_str}")

                all_hierarchy_rows.append({
                    "HIERARCHY_ID": child_id,
                    "HIERARCHY_NAME": result_value,
                    "PARENT_ID": parent_id,
                    "DESCRIPTION": f"Maps {case.input_column} to '{result_value}'",
                    "LEVEL_1": hier_name,
                    "LEVEL_2": result_value,
                    "LEVEL_3": "",
                    "CONDITION_TYPE": when_clauses[0].condition.operator.value if when_clauses else "",
                    "CONDITION_VALUE": "; ".join(conditions),
                    "INCLUDE_FLAG": "true",
                    "ACTIVE_FLAG": "true",
                    "IS_LEAF_NODE": "true",
                    "SORT_ORDER": sort_idx,
                })
                sort_idx += 1

                # Create mapping rows for each condition
                for mapping_idx, when in enumerate(when_clauses):
                    cond = when.condition
                    for value in cond.values:
                        all_mapping_rows.append({
                            "HIERARCHY_ID": child_id,
                            "HIERARCHY_NAME": result_value,
                            "PARENT_HIERARCHY": hier_name,
                            "MAPPING_INDEX": mapping_idx,
                            "SOURCE_COLUMN": case.input_column,
                            "CONDITION_TYPE": cond.operator.value if hasattr(cond.operator, 'value') else str(cond.operator),
                            "CONDITION_VALUE": value,
                            "MAPPED_VALUE": result_value,
                            "RAW_CONDITION": cond.raw_condition,
                            "PRECEDENCE_GROUP": "1",
                            "INCLUDE_FLAG": "true",
                            "ACTIVE_FLAG": "true",
                        })

            # Add ELSE value if present
            if case.else_value:
                else_id = f"{parent_id}_ELSE"
                all_hierarchy_rows.append({
                    "HIERARCHY_ID": else_id,
                    "HIERARCHY_NAME": case.else_value,
                    "PARENT_ID": parent_id,
                    "DESCRIPTION": f"ELSE clause - default value when no conditions match",
                    "LEVEL_1": hier_name,
                    "LEVEL_2": case.else_value,
                    "LEVEL_3": "",
                    "CONDITION_TYPE": "ELSE",
                    "CONDITION_VALUE": "All other values",
                    "INCLUDE_FLAG": "true",
                    "ACTIVE_FLAG": "true",
                    "IS_LEAF_NODE": "true",
                    "SORT_ORDER": sort_idx,
                })
                all_mapping_rows.append({
                    "HIERARCHY_ID": else_id,
                    "HIERARCHY_NAME": case.else_value,
                    "PARENT_HIERARCHY": hier_name,
                    "MAPPING_INDEX": 0,
                    "SOURCE_COLUMN": case.input_column,
                    "CONDITION_TYPE": "ELSE",
                    "CONDITION_VALUE": "*",
                    "MAPPED_VALUE": case.else_value,
                    "RAW_CONDITION": "ELSE",
                    "PRECEDENCE_GROUP": "1",
                    "INCLUDE_FLAG": "true",
                    "ACTIVE_FLAG": "true",
                })

            # Generate recommendation
            recommendation = _generate_recommendation(case)
            confidence = _calculate_hierarchy_confidence(case)

            # Build summary row
            summary_rows.append({
                "HIERARCHY_NAME": hier_name,
                "SOURCE_COLUMN": case.input_column,
                "ENTITY_TYPE": case.entity_type.value,
                "PATTERN_TYPE": case.pattern_type or "mixed",
                "TOTAL_CONDITIONS": len(case.when_clauses),
                "UNIQUE_VALUES": len(case.unique_results),
                "HAS_ELSE": "Yes" if case.else_value else "No",
                "ELSE_VALUE": case.else_value or "",
                "CONFIDENCE": f"{confidence:.0%}",
                "SUITABLE_FOR_HIERARCHY": "Yes" if recommendation["suitable_for_hierarchy"] else "No",
                "NOTES": "; ".join(recommendation["notes"]),
            })

            hierarchies_data.append({
                "name": hier_name,
                "source_column": case.input_column,
                "entity_type": case.entity_type.value,
                "pattern_type": case.pattern_type,
                "condition_count": len(case.when_clauses),
                "unique_values": len(case.unique_results),
                "has_else": case.else_value is not None,
                "confidence": confidence,
            })

        # Export CSV files
        exported_files = []

        # 1. Summary CSV
        summary_file = os.path.join(export_path, f"{export_name}_SUMMARY.csv")
        with open(summary_file, "w", newline="", encoding="utf-8") as f:
            if summary_rows:
                writer = csv.DictWriter(f, fieldnames=summary_rows[0].keys())
                writer.writeheader()
                writer.writerows(summary_rows)
        exported_files.append(summary_file)

        # 2. Hierarchy CSV (tree structure)
        hierarchy_file = os.path.join(export_path, f"{export_name}_HIERARCHY.csv")
        with open(hierarchy_file, "w", newline="", encoding="utf-8") as f:
            if all_hierarchy_rows:
                writer = csv.DictWriter(f, fieldnames=all_hierarchy_rows[0].keys())
                writer.writeheader()
                writer.writerows(all_hierarchy_rows)
        exported_files.append(hierarchy_file)

        # 3. Mapping CSV (all conditions)
        mapping_file = os.path.join(export_path, f"{export_name}_MAPPING.csv")
        with open(mapping_file, "w", newline="", encoding="utf-8") as f:
            if all_mapping_rows:
                writer = csv.DictWriter(f, fieldnames=all_mapping_rows[0].keys())
                writer.writeheader()
                writer.writerows(all_mapping_rows)
        exported_files.append(mapping_file)

        return {
            "success": True,
            "case_count": len(cases),
            "hierarchies": hierarchies_data,
            "summary": {
                "total_hierarchies": len(cases),
                "total_mappings": len(all_mapping_rows),
                "total_nodes": len(all_hierarchy_rows),
            },
            "exported_files": exported_files,
            "export_location": export_path,
            "file_details": {
                "summary": f"{export_name}_SUMMARY.csv - High-level overview ({len(summary_rows)} rows)",
                "hierarchy": f"{export_name}_HIERARCHY.csv - Tree structure ({len(all_hierarchy_rows)} rows)",
                "mapping": f"{export_name}_MAPPING.csv - All conditions ({len(all_mapping_rows)} rows)",
            },
        }


def _generate_recommendation(case: ExtractedCase) -> Dict[str, Any]:
    """Generate a recommendation for hierarchy creation."""
    recommendation = {
        "suitable_for_hierarchy": len(case.unique_results) >= 3,
        "suggested_name": case.source_column,
        "suggested_levels": 1,
        "notes": [],
    }

    if case.entity_type != EntityType.UNKNOWN:
        recommendation["notes"].append(
            f"Detected as {case.entity_type.value} hierarchy"
        )

    if case.pattern_type == "prefix":
        recommendation["notes"].append(
            "Uses prefix pattern - good for account code hierarchies"
        )
    elif case.pattern_type == "range":
        recommendation["notes"].append(
            "Uses range pattern - consider if ranges represent hierarchy levels"
        )

    if len(case.unique_results) > 20:
        recommendation["notes"].append(
            "Many unique values - consider grouping into parent categories"
        )

    if case.else_value:
        recommendation["notes"].append(
            f"Has ELSE clause ('{case.else_value}') - will catch unmapped values"
        )

    return recommendation


def _calculate_hierarchy_confidence(case: ExtractedCase) -> float:
    """Calculate confidence that this CASE is suitable for hierarchy."""
    confidence = 0.5

    # More conditions = more comprehensive
    if len(case.when_clauses) >= 10:
        confidence += 0.2
    elif len(case.when_clauses) >= 5:
        confidence += 0.1

    # Known entity type
    if case.entity_type != EntityType.UNKNOWN:
        confidence += 0.15

    # Consistent pattern
    if case.pattern_type:
        confidence += 0.1

    # Good rollup ratio (many inputs -> fewer outputs)
    if len(case.when_clauses) > len(case.unique_results):
        confidence += 0.1

    return min(confidence, 1.0)
