"""
Smart SQL Analyzer MCP Tools - Query Plan Execution with WHERE Clause Filtering.

This module provides intelligent SQL analysis that:
1. Parses CASE statements to extract hierarchies
2. Parses WHERE clause to extract filters (NOT IN, <>, NOT LIKE)
3. Applies filters BEFORE generating mappings
4. Uses reference data (COA) for expansion with filter awareness
5. Generates a query plan for transparency

IMPORTANT: This replaces naive CASE extraction with proper query understanding.
"""

import csv
import os
import re
import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Set
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict


# ============================================================================
# DATA CLASSES
# ============================================================================

@dataclass
class QueryFilter:
    """Represents a WHERE clause filter."""
    column: str
    operator: str  # IN, NOT IN, =, <>, LIKE, NOT LIKE
    values: List[str]
    is_negated: bool = False

    def matches(self, value: str) -> bool:
        """Check if a value passes this filter (returns True if allowed)."""
        value_lower = value.lower() if value else ""

        if self.operator == 'NOT IN':
            # Value is allowed if NOT in the list
            return value not in self.values and value_lower not in [v.lower() for v in self.values]

        elif self.operator == '<>':
            # Value is allowed if NOT equal
            return value != self.values[0] and value_lower != self.values[0].lower()

        elif self.operator == 'NOT LIKE':
            pattern = self.values[0]
            if pattern.endswith('%') and not pattern.startswith('%'):
                return not value_lower.startswith(pattern[:-1].lower())
            elif pattern.startswith('%') and not pattern.endswith('%'):
                return not value_lower.endswith(pattern[1:].lower())
            elif pattern.startswith('%') and pattern.endswith('%'):
                return pattern[1:-1].lower() not in value_lower
            else:
                return value_lower != pattern.lower()

        return True  # Default: allow


@dataclass
class CaseMapping:
    """A single WHEN clause mapping."""
    condition_type: str
    condition_values: List[str]
    result_value: str
    raw_condition: str


@dataclass
class CaseHierarchy:
    """A complete CASE statement hierarchy."""
    name: str
    source_column: str
    mappings: List[CaseMapping] = field(default_factory=list)
    else_value: Optional[str] = None


@dataclass
class QueryPlan:
    """Virtual query execution plan with filters applied."""
    hierarchies: List[CaseHierarchy]
    filters: List[QueryFilter]
    excluded_result_values: Dict[str, Set[str]]  # hierarchy -> excluded values
    excluded_source_values: Dict[str, Set[str]]  # column -> excluded values
    excluded_patterns: Dict[str, List[str]]  # column -> patterns


# ============================================================================
# SMART SQL ANALYZER
# ============================================================================

class SmartSQLAnalyzer:
    """
    Intelligent SQL analyzer with proper WHERE clause handling.
    """

    def __init__(self):
        self.coa_data: Dict[str, dict] = {}

    def load_coa(self, coa_path: str) -> int:
        """Load Chart of Accounts reference data."""
        with open(coa_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = row.get('ACCOUNT_CODE', '').strip()
                if code:
                    self.coa_data[code] = {
                        'ACCOUNT_CODE': code,
                        'ACCOUNT_ID': row.get('ACCOUNT_ID', ''),
                        'ACCOUNT_NAME': row.get('ACCOUNT_NAME', ''),
                        'ACCOUNT_CLASS': row.get('ACCOUNT_CLASS', ''),
                        'ACCOUNT_BILLING_CATEGORY_CODE': row.get('ACCOUNT_BILLING_CATEGORY_CODE', ''),
                        'ACCOUNT_MAJOR': row.get('ACCOUNT_MAJOR', ''),
                        'ACCOUNT_MINOR': row.get('ACCOUNT_MINOR', ''),
                        'ACCOUNT_HOLDER': row.get('ACCOUNT_HOLDER', ''),
                    }
        return len(self.coa_data)

    def parse_where_filters(self, sql: str) -> List[QueryFilter]:
        """Extract WHERE clause filters from SQL."""
        filters = []

        # Find WHERE clause (before GROUP BY, ORDER BY, HAVING)
        where_match = re.search(
            r'\bWHERE\b(.+?)(?:GROUP\s+BY|ORDER\s+BY|HAVING|$)',
            sql, re.IGNORECASE | re.DOTALL
        )
        if not where_match:
            return filters

        where_clause = where_match.group(1)

        # NOT IN filters
        not_in_pattern = r"(\w+(?:\.\w+)?)\s+NOT\s+IN\s*\(([^)]+)\)"
        for match in re.finditer(not_in_pattern, where_clause, re.IGNORECASE):
            column = match.group(1).split('.')[-1]
            values = [v.strip().strip("'\"") for v in match.group(2).split(',')]
            filters.append(QueryFilter(column, 'NOT IN', values, is_negated=True))

        # <> filters
        neq_pattern = r"(\w+(?:\.\w+)?)\s*<>\s*'([^']+)'"
        for match in re.finditer(neq_pattern, where_clause, re.IGNORECASE):
            column = match.group(1).split('.')[-1]
            filters.append(QueryFilter(column, '<>', [match.group(2)], is_negated=True))

        # NOT LIKE filters
        not_like_pattern = r"(\w+(?:\.\w+)?)\s+NOT\s+LIKE\s+'([^']+)'"
        for match in re.finditer(not_like_pattern, where_clause, re.IGNORECASE):
            column = match.group(1).split('.')[-1]
            filters.append(QueryFilter(column, 'NOT LIKE', [match.group(2)], is_negated=True))

        return filters

    def parse_case_statements(self, sql: str) -> List[CaseHierarchy]:
        """Extract CASE statements from SQL."""
        hierarchies = []

        case_pattern = r'CASE\s+(.*?)\s+END\s+AS\s+(\w+)'

        for match in re.finditer(case_pattern, sql, re.IGNORECASE | re.DOTALL):
            case_body = match.group(1)
            alias = match.group(2)

            hierarchy = CaseHierarchy(name=alias, source_column='')

            # Extract WHEN clauses
            when_pattern = r'WHEN\s+(.+?)\s+THEN\s+([\'"]?)([^\'"]+)\2'
            for when_match in re.finditer(when_pattern, case_body, re.IGNORECASE | re.DOTALL):
                condition = when_match.group(1).strip()
                result = when_match.group(3).strip()

                mapping = self._parse_condition(condition, result)
                if mapping:
                    hierarchy.mappings.append(mapping)
                    if not hierarchy.source_column:
                        col_match = re.search(r'(\w+(?:\.\w+)?)\s*(?:ILIKE|LIKE|IN|=|<>|BETWEEN)',
                                            condition, re.IGNORECASE)
                        if col_match:
                            hierarchy.source_column = col_match.group(1).split('.')[-1]

            # ELSE clause
            else_match = re.search(r'ELSE\s+([\'"]?)([^\'"]+)\1\s*$', case_body, re.IGNORECASE)
            if else_match:
                hierarchy.else_value = else_match.group(2).strip()

            if hierarchy.mappings:
                hierarchies.append(hierarchy)

        return hierarchies

    def _parse_condition(self, condition: str, result: str) -> Optional[CaseMapping]:
        """Parse a WHEN condition."""
        condition = condition.strip()

        # ILIKE ANY
        ilike_any = re.search(r"ILIKE\s+ANY\s*\(([^)]+)\)", condition, re.IGNORECASE)
        if ilike_any:
            values = [v.strip().strip("'\"") for v in ilike_any.group(1).split(',')]
            return CaseMapping('ILIKE', values, result, condition)

        # Simple ILIKE
        ilike = re.search(r"ILIKE\s+'([^']+)'", condition, re.IGNORECASE)
        if ilike:
            return CaseMapping('ILIKE', [ilike.group(1)], result, condition)

        # IN clause
        in_match = re.search(r"IN\s*\(([^)]+)\)", condition, re.IGNORECASE)
        if in_match:
            values = [v.strip().strip("'\"") for v in in_match.group(1).split(',')]
            return CaseMapping('IN', values, result, condition)

        # Equals
        eq = re.search(r"=\s*'([^']+)'", condition, re.IGNORECASE)
        if eq:
            return CaseMapping('=', [eq.group(1)], result, condition)

        # BETWEEN
        between = re.search(r"BETWEEN\s+(\d+)\s+AND\s+(\d+)", condition, re.IGNORECASE)
        if between:
            return CaseMapping('BETWEEN', [between.group(1), between.group(2)], result, condition)

        return None

    def build_query_plan(self, sql: str) -> QueryPlan:
        """Build query execution plan with filters."""
        hierarchies = self.parse_case_statements(sql)
        filters = self.parse_where_filters(sql)

        # Build exclusion maps
        excluded_results: Dict[str, Set[str]] = defaultdict(set)
        excluded_values: Dict[str, Set[str]] = defaultdict(set)
        excluded_patterns: Dict[str, List[str]] = defaultdict(list)

        for f in filters:
            col = f.column.lower()
            if f.operator == 'NOT IN':
                # If filtering on a hierarchy result (like 'gl NOT IN ...')
                for h in hierarchies:
                    if h.name.lower() == col:
                        excluded_results[h.name].update(f.values)
                # Also could be filtering on source column
                excluded_values[col].update(f.values)

            elif f.operator == '<>':
                excluded_values[col].update(f.values)

            elif f.operator == 'NOT LIKE':
                excluded_patterns[col].extend(f.values)

        return QueryPlan(
            hierarchies=hierarchies,
            filters=filters,
            excluded_result_values=dict(excluded_results),
            excluded_source_values=dict(excluded_values),
            excluded_patterns=dict(excluded_patterns),
        )

    def apply_filters(self, plan: QueryPlan) -> QueryPlan:
        """Apply WHERE clause filters to remove invalid mappings."""
        for hierarchy in plan.hierarchies:
            # Check if this hierarchy's result values are filtered
            if hierarchy.name in plan.excluded_result_values:
                excluded = plan.excluded_result_values[hierarchy.name]
                original_count = len(hierarchy.mappings)
                hierarchy.mappings = [
                    m for m in hierarchy.mappings
                    if m.result_value not in excluded
                ]
                removed = original_count - len(hierarchy.mappings)
                if removed > 0:
                    print(f"  [{hierarchy.name}] Removed {removed} mappings due to WHERE filter")

        return plan

    def expand_with_coa(self, hierarchy: CaseHierarchy, plan: QueryPlan,
                       detail_columns: List[str] = None) -> List[dict]:
        """Expand hierarchy with COA, applying source column filters."""
        if detail_columns is None:
            detail_columns = ['ACCOUNT_ID', 'ACCOUNT_NAME', 'ACCOUNT_BILLING_CATEGORY_CODE']

        rows = []
        source_col = hierarchy.source_column.lower()

        # Get exclusions for account_code
        excluded_codes = plan.excluded_source_values.get('account_code', set())
        excluded_pats = plan.excluded_patterns.get('account_code', [])

        for mapping in hierarchy.mappings:
            matched = []

            for code, data in self.coa_data.items():
                # Check excluded codes
                if code in excluded_codes:
                    continue

                # Check excluded patterns
                skip = False
                for pat in excluded_pats:
                    if pat.endswith('%') and code.startswith(pat[:-1]):
                        skip = True
                        break
                if skip:
                    continue

                # Check if matches the CASE condition
                if self._matches_condition(code, mapping):
                    matched.append(data)

            for acct in matched:
                row = {
                    'HIERARCHY_NAME': hierarchy.name,
                    'MAPPED_VALUE': mapping.result_value,
                    'SOURCE_COLUMN': hierarchy.source_column,
                    'CONDITION_TYPE': mapping.condition_type,
                    'CONDITION_VALUE': ','.join(mapping.condition_values[:3]),
                }
                for col in detail_columns:
                    val = acct.get(col, '')
                    row[f'EXPANDED_{col}'] = val if val and str(val) != 'nan' else ''
                row['MATCH_TYPE'] = 'COA_MATCH'
                row['FILTER_STATUS'] = 'INCLUDED'
                rows.append(row)

        return rows

    def _matches_condition(self, code: str, mapping: CaseMapping) -> bool:
        """Check if account code matches mapping condition."""
        code_lower = code.lower()

        if mapping.condition_type == 'ILIKE':
            for pattern in mapping.condition_values:
                pat_lower = pattern.lower()
                if pat_lower.endswith('%') and not pat_lower.startswith('%'):
                    if code_lower.startswith(pat_lower[:-1]):
                        return True
                elif pat_lower.startswith('%') and not pat_lower.endswith('%'):
                    if code_lower.endswith(pat_lower[1:]):
                        return True
                elif pat_lower.startswith('%') and pat_lower.endswith('%'):
                    if pat_lower[1:-1] in code_lower:
                        return True
                elif code_lower == pat_lower:
                    return True

        elif mapping.condition_type == 'IN':
            return code in mapping.condition_values

        elif mapping.condition_type == '=':
            return code == mapping.condition_values[0]

        return False

    def analyze(self, sql: str, output_dir: str, export_name: str,
                coa_path: str = None, detail_columns: List[str] = None) -> dict:
        """Full analysis with query plan and filtered exports."""
        # Load COA if provided
        if coa_path:
            count = self.load_coa(coa_path)
            print(f"Loaded {count} COA records")

        # Build and apply query plan
        plan = self.build_query_plan(sql)
        print(f"\nQuery Plan: {len(plan.hierarchies)} hierarchies, {len(plan.filters)} filters")

        if plan.excluded_result_values:
            for h, vals in plan.excluded_result_values.items():
                print(f"  Excluding from {h}: {list(vals)[:5]}...")

        plan = self.apply_filters(plan)

        # Generate outputs
        Path(output_dir).mkdir(parents=True, exist_ok=True)

        summary_rows = []
        hierarchy_rows = []
        mapping_rows = []
        enriched_rows = []

        for hierarchy in plan.hierarchies:
            # Summary
            summary_rows.append({
                'HIERARCHY_NAME': hierarchy.name,
                'SOURCE_COLUMN': hierarchy.source_column,
                'TOTAL_MAPPINGS': len(hierarchy.mappings),
                'HAS_ELSE': 'Yes' if hierarchy.else_value else 'No',
            })

            # Hierarchy structure
            parent_id = hierarchy.name.upper()
            hierarchy_rows.append({
                'HIERARCHY_ID': parent_id,
                'HIERARCHY_NAME': hierarchy.name,
                'PARENT_ID': '',
                'SOURCE_COLUMN': hierarchy.source_column,
                'IS_LEAF_NODE': 'false',
            })

            unique_results = list(set(m.result_value for m in hierarchy.mappings))
            for result in unique_results:
                child_id = f"{parent_id}_{result.upper().replace(' ', '_')[:25]}"
                hierarchy_rows.append({
                    'HIERARCHY_ID': child_id,
                    'HIERARCHY_NAME': result,
                    'PARENT_ID': parent_id,
                    'SOURCE_COLUMN': hierarchy.source_column,
                    'IS_LEAF_NODE': 'true',
                })

            # Mappings
            for mapping in hierarchy.mappings:
                for val in mapping.condition_values:
                    mapping_rows.append({
                        'HIERARCHY_ID': f"{parent_id}_{mapping.result_value.upper().replace(' ', '_')[:25]}",
                        'HIERARCHY_NAME': mapping.result_value,
                        'PARENT_HIERARCHY': hierarchy.name,
                        'SOURCE_COLUMN': hierarchy.source_column,
                        'CONDITION_TYPE': mapping.condition_type,
                        'CONDITION_VALUE': val,
                        'MAPPED_VALUE': mapping.result_value,
                    })

            # Enriched (COA expansion)
            if hierarchy.source_column.lower() in ('account_code', 'acct', 'gl_code'):
                expanded = self.expand_with_coa(hierarchy, plan, detail_columns)
                enriched_rows.extend(expanded)

        # Write files
        files = []

        if summary_rows:
            path = os.path.join(output_dir, f"{export_name}_SUMMARY.csv")
            self._write_csv(path, summary_rows)
            files.append(path)

        if hierarchy_rows:
            path = os.path.join(output_dir, f"{export_name}_HIERARCHY.csv")
            self._write_csv(path, hierarchy_rows)
            files.append(path)

        if mapping_rows:
            path = os.path.join(output_dir, f"{export_name}_MAPPING.csv")
            self._write_csv(path, mapping_rows)
            files.append(path)

        if enriched_rows:
            enriched_dir = os.path.join(output_dir, 'enriched')
            Path(enriched_dir).mkdir(parents=True, exist_ok=True)
            path = os.path.join(enriched_dir, f"{export_name}_MAPPING_ENRICHED.csv")
            self._write_csv(path, enriched_rows)
            files.append(path)

        # Query plan JSON
        plan_path = os.path.join(output_dir, f"{export_name}_QUERY_PLAN.json")
        with open(plan_path, 'w') as f:
            json.dump({
                'hierarchies': [
                    {'name': h.name, 'mappings': len(h.mappings), 'source_column': h.source_column}
                    for h in plan.hierarchies
                ],
                'excluded_result_values': {k: list(v) for k, v in plan.excluded_result_values.items()},
                'excluded_source_values': {k: list(v) for k, v in plan.excluded_source_values.items()},
                'excluded_patterns': plan.excluded_patterns,
                'filters_applied': len(plan.filters),
            }, f, indent=2)
        files.append(plan_path)

        return {
            'success': True,
            'hierarchies': len(plan.hierarchies),
            'mappings': len(mapping_rows),
            'enriched_rows': len(enriched_rows),
            'files': files,
            'excluded_gl_values': list(plan.excluded_result_values.get('gl', [])),
        }

    def _write_csv(self, path: str, rows: List[dict]) -> None:
        """Write rows to CSV."""
        if not rows:
            return
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)


# ============================================================================
# MCP TOOL REGISTRATION
# ============================================================================

def register_smart_sql_tools(mcp):
    """Register smart SQL analyzer tools with MCP server."""

    analyzer = SmartSQLAnalyzer()

    @mcp.tool()
    def smart_analyze_sql(
        sql: str,
        output_dir: str = "./result_export",
        export_name: str = "sql_analysis",
        coa_path: str = "",
        detail_columns: str = ""
    ) -> str:
        """
        Intelligently analyze SQL with proper WHERE clause filtering.

        This tool properly processes SQL queries by:
        1. Extracting CASE statements to create hierarchies
        2. Parsing WHERE clause filters (NOT IN, <>, NOT LIKE)
        3. APPLYING filters BEFORE generating mappings
        4. Using COA reference data for expansion with filter awareness
        5. Generating a query plan for transparency

        IMPORTANT: Unlike basic CASE extraction, this tool respects WHERE clauses
        that exclude certain GL values or account codes from the results.

        Args:
            sql: SQL query containing CASE statements and WHERE clauses
            output_dir: Output directory for CSV exports
            export_name: Base name for export files
            coa_path: Path to DIM_ACCOUNT.csv for COA enrichment (optional)
            detail_columns: Comma-separated COA columns to include (default: ACCOUNT_ID,ACCOUNT_NAME,ACCOUNT_BILLING_CATEGORY_CODE)

        Returns:
            JSON with analysis results including query plan and file paths

        Example:
            smart_analyze_sql(
                sql="SELECT CASE WHEN ... END AS gl FROM ... WHERE gl NOT IN ('Hedge Gains', 'DD&A')",
                coa_path="C:/data/DIM_ACCOUNT.csv"
            )
        """
        # Parse detail columns
        cols = None
        if detail_columns:
            cols = [c.strip() for c in detail_columns.split(',')]

        result = analyzer.analyze(
            sql=sql,
            output_dir=output_dir,
            export_name=export_name,
            coa_path=coa_path if coa_path else None,
            detail_columns=cols,
        )

        return json.dumps(result, indent=2)

    @mcp.tool()
    def parse_sql_query_plan(sql: str) -> str:
        """
        Parse SQL to extract query plan WITHOUT executing.

        Use this to preview what filters will be applied before running analysis.

        Args:
            sql: SQL query to parse

        Returns:
            JSON with query plan showing hierarchies and filters
        """
        plan = analyzer.build_query_plan(sql)

        return json.dumps({
            'hierarchies': [
                {
                    'name': h.name,
                    'source_column': h.source_column,
                    'mappings_count': len(h.mappings),
                    'sample_results': list(set(m.result_value for m in h.mappings))[:10],
                }
                for h in plan.hierarchies
            ],
            'filters': [
                {
                    'column': f.column,
                    'operator': f.operator,
                    'values': f.values[:10],
                }
                for f in plan.filters
            ],
            'excluded_result_values': {k: list(v)[:10] for k, v in plan.excluded_result_values.items()},
            'excluded_source_values': {k: list(v)[:10] for k, v in plan.excluded_source_values.items()},
            'excluded_patterns': plan.excluded_patterns,
        }, indent=2)

    return {
        'smart_analyze_sql': smart_analyze_sql,
        'parse_sql_query_plan': parse_sql_query_plan,
    }
