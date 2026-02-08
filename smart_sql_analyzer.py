"""
Smart SQL Analyzer with Query Plan Execution.

This module properly processes SQL queries by:
1. Extracting CASE statements
2. Parsing WHERE clause filters
3. Applying filters to exclude invalid mappings
4. Using reference data (DIM_ACCOUNT) for validation
5. Creating a virtual query plan for transparency
"""

import csv
import os
import re
import json
from pathlib import Path
from typing import Dict, List, Optional, Any, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict


@dataclass
class QueryFilter:
    """Represents a WHERE clause filter."""
    column: str
    operator: str  # IN, NOT IN, =, <>, LIKE, NOT LIKE, BETWEEN
    values: List[str]
    is_negated: bool = False

    def matches(self, value: str) -> bool:
        """Check if a value matches this filter."""
        value_lower = value.lower() if value else ""

        if self.operator in ('IN', 'NOT IN'):
            match = value in self.values or value_lower in [v.lower() for v in self.values]
            return not match if self.operator == 'NOT IN' else match

        elif self.operator in ('=', '<>'):
            match = value == self.values[0] or value_lower == self.values[0].lower()
            return not match if self.operator == '<>' else match

        elif self.operator in ('LIKE', 'NOT LIKE'):
            pattern = self.values[0]
            if pattern.endswith('%') and not pattern.startswith('%'):
                match = value_lower.startswith(pattern[:-1].lower())
            elif pattern.startswith('%') and not pattern.endswith('%'):
                match = value_lower.endswith(pattern[1:].lower())
            elif pattern.startswith('%') and pattern.endswith('%'):
                match = pattern[1:-1].lower() in value_lower
            else:
                match = value_lower == pattern.lower()
            return not match if self.operator == 'NOT LIKE' else match

        return True


@dataclass
class CaseMapping:
    """A single WHEN clause mapping."""
    condition_type: str  # ILIKE, IN, =, etc.
    condition_values: List[str]
    result_value: str
    raw_condition: str


@dataclass
class CaseHierarchy:
    """A complete CASE statement hierarchy."""
    name: str  # alias (e.g., 'gl', 'fund')
    source_column: str  # column being tested (e.g., 'account_code')
    mappings: List[CaseMapping] = field(default_factory=list)
    else_value: Optional[str] = None
    is_filtered: bool = False  # True if subject to WHERE filters


@dataclass
class QueryPlan:
    """Virtual query execution plan."""
    hierarchies: List[CaseHierarchy]
    filters: List[QueryFilter]
    excluded_gl_values: Set[str]
    excluded_account_codes: Set[str]
    excluded_account_patterns: List[str]
    reference_data: Dict[str, Dict[str, dict]]

    def to_dict(self) -> dict:
        return {
            'hierarchies': [h.name for h in self.hierarchies],
            'filters': [
                {'column': f.column, 'operator': f.operator, 'values': f.values[:5]}
                for f in self.filters
            ],
            'excluded_gl_values': list(self.excluded_gl_values),
            'excluded_account_codes': list(self.excluded_account_codes)[:10],
            'excluded_account_patterns': self.excluded_account_patterns,
        }


class SmartSQLAnalyzer:
    """
    Analyzes SQL queries with proper WHERE clause handling.
    """

    def __init__(self, coa_path: str = None):
        self.coa_path = coa_path
        self.coa_data: Dict[str, dict] = {}
        self.gl_to_accounts: Dict[str, List[str]] = defaultdict(list)

        if coa_path:
            self._load_coa(coa_path)

    def _load_coa(self, path: str) -> None:
        """Load Chart of Accounts and build reverse lookup."""
        print(f"Loading COA from: {path}")
        with open(path, 'r', encoding='utf-8') as f:
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
                    }
        print(f"  Loaded {len(self.coa_data)} account codes")

    def parse_where_filters(self, sql: str) -> List[QueryFilter]:
        """Extract WHERE clause filters from SQL."""
        filters = []

        # Find WHERE clause
        where_match = re.search(r'\bWHERE\b(.+?)(?:GROUP\s+BY|ORDER\s+BY|HAVING|$)',
                               sql, re.IGNORECASE | re.DOTALL)
        if not where_match:
            return filters

        where_clause = where_match.group(1)

        # Parse NOT IN filters
        not_in_pattern = r"(\w+(?:\.\w+)?)\s+NOT\s+IN\s*\(([^)]+)\)"
        for match in re.finditer(not_in_pattern, where_clause, re.IGNORECASE):
            column = match.group(1).split('.')[-1]  # Get column name without table alias
            values_str = match.group(2)
            values = [v.strip().strip("'\"") for v in values_str.split(',')]
            filters.append(QueryFilter(column, 'NOT IN', values, is_negated=True))

        # Parse <> filters
        neq_pattern = r"(\w+(?:\.\w+)?)\s*<>\s*'([^']+)'"
        for match in re.finditer(neq_pattern, where_clause, re.IGNORECASE):
            column = match.group(1).split('.')[-1]
            value = match.group(2)
            filters.append(QueryFilter(column, '<>', [value], is_negated=True))

        # Parse NOT LIKE filters
        not_like_pattern = r"(\w+(?:\.\w+)?)\s+NOT\s+LIKE\s+'([^']+)'"
        for match in re.finditer(not_like_pattern, where_clause, re.IGNORECASE):
            column = match.group(1).split('.')[-1]
            pattern = match.group(2)
            filters.append(QueryFilter(column, 'NOT LIKE', [pattern], is_negated=True))

        return filters

    def parse_case_statements(self, sql: str) -> List[CaseHierarchy]:
        """
        Extract CASE statements from SQL, supporting UNION ALL.
        Each SELECT statement in a UNION ALL can contain its own CASE statements.
        Hierarchies with the same alias are merged.
        """
        all_hierarchies: List[CaseHierarchy] = []

        # Split SQL by UNION ALL to process each SELECT statement independently
        # Use regex to split, but not if UNION ALL is inside parentheses (subqueries)
        select_statements = re.split(r'\s+UNION\s+ALL\s+(?![^()]*\))', sql, flags=re.IGNORECASE | re.DOTALL)

        for statement in select_statements:
            # Pattern for CASE ... END AS alias
            case_pattern = r'CASE\s+(.*?)\s+END\s+AS\s+(\w+)'

            for match in re.finditer(case_pattern, statement, re.IGNORECASE | re.DOTALL):
                case_body = match.group(1)
                alias = match.group(2)

                hierarchy = CaseHierarchy(name=alias, source_column='')

                # Extract WHEN clauses
                when_pattern = r'WHEN\s+(.+?)\s+THEN\s+([\'"]?)([^\'"]+)\2'
                for when_match in re.finditer(when_pattern, case_body, re.IGNORECASE | re.DOTALL):
                    condition = when_match.group(1).strip()
                    result = when_match.group(3).strip()

                    # Parse condition to get column, operator, values
                    mapping = self._parse_condition(condition, result)
                    if mapping:
                        hierarchy.mappings.append(mapping)
                        if not hierarchy.source_column and mapping.condition_values:
                            # Infer source column from condition
                            col_match = re.search(r'(\w+(?:\.\w+)?)\s*(?:ILIKE|LIKE|IN|=|<>)', condition, re.IGNORECASE)
                            if col_match:
                                hierarchy.source_column = col_match.group(1).split('.')[-1]

                # Extract ELSE clause
                else_match = re.search(r'ELSE\s+([\'"]?)([^\'"]+)\1\s*$', case_body, re.IGNORECASE)
                if else_match:
                    hierarchy.else_value = else_match.group(2).strip()

                if hierarchy.mappings or hierarchy.else_value:
                    all_hierarchies.append(hierarchy)

        # Merge hierarchies with the same alias (from different UNION ALL blocks)
        merged: Dict[str, CaseHierarchy] = {}
        for h in all_hierarchies:
            if h.name in merged:
                # Merge mappings
                merged[h.name].mappings.extend(h.mappings)
                # Keep first else_value found
                if h.else_value and not merged[h.name].else_value:
                    merged[h.name].else_value = h.else_value
                # Keep first source_column found
                if h.source_column and not merged[h.name].source_column:
                    merged[h.name].source_column = h.source_column
            else:
                merged[h.name] = h

        return list(merged.values())

    def _parse_condition(self, condition: str, result: str) -> Optional[CaseMapping]:
        """Parse a WHEN condition into a CaseMapping."""
        condition = condition.strip()

        # ILIKE ANY pattern
        ilike_any_match = re.search(
            r"(\w+(?:\.\w+)?)\s+ILIKE\s+ANY\s*\(([^)]+)\)",
            condition, re.IGNORECASE
        )
        if ilike_any_match:
            values = [v.strip().strip("'\"") for v in ilike_any_match.group(2).split(',')]
            return CaseMapping('ILIKE', values, result, condition)

        # Simple ILIKE pattern
        ilike_match = re.search(
            r"(\w+(?:\.\w+)?)\s+ILIKE\s+'([^']+)'",
            condition, re.IGNORECASE
        )
        if ilike_match:
            return CaseMapping('ILIKE', [ilike_match.group(2)], result, condition)

        # IN pattern
        in_match = re.search(
            r"(\w+(?:\.\w+)?)\s+IN\s*\(([^)]+)\)",
            condition, re.IGNORECASE
        )
        if in_match:
            values = [v.strip().strip("'\"") for v in in_match.group(2).split(',')]
            return CaseMapping('IN', values, result, condition)

        # Equals pattern
        eq_match = re.search(
            r"(\w+(?:\.\w+)?)\s*=\s*'([^']+)'",
            condition, re.IGNORECASE
        )
        if eq_match:
            return CaseMapping('=', [eq_match.group(2)], result, condition)

        # BETWEEN pattern
        between_match = re.search(
            r"(\w+(?:\.\w+)?)\s+BETWEEN\s+(\d+)\s+AND\s+(\d+)",
            condition, re.IGNORECASE
        )
        if between_match:
            return CaseMapping('BETWEEN', [between_match.group(2), between_match.group(3)], result, condition)

        return None

    def build_query_plan(self, sql: str) -> QueryPlan:
        """Build a virtual query execution plan."""
        print("\nBuilding query plan...")

        # Parse components
        hierarchies = self.parse_case_statements(sql)
        filters = self.parse_where_filters(sql)

        print(f"  Found {len(hierarchies)} CASE hierarchies")
        print(f"  Found {len(filters)} WHERE filters")

        # Extract exclusions
        excluded_gl = set()
        excluded_codes = set()
        excluded_patterns = []

        for f in filters:
            if f.column.lower() == 'gl' and f.operator == 'NOT IN':
                excluded_gl.update(f.values)
                print(f"  GL exclusions: {f.values}")
            elif f.column.lower() == 'account_code':
                if f.operator == '<>':
                    excluded_codes.update(f.values)
                    print(f"  Account code exclusion: {f.values}")
                elif f.operator == 'NOT LIKE':
                    excluded_patterns.extend(f.values)
                    print(f"  Account pattern exclusion: {f.values}")

        # Mark hierarchies affected by filters
        for h in hierarchies:
            if h.name.lower() == 'gl':
                h.is_filtered = True

        return QueryPlan(
            hierarchies=hierarchies,
            filters=filters,
            excluded_gl_values=excluded_gl,
            excluded_account_codes=excluded_codes,
            excluded_account_patterns=excluded_patterns,
            reference_data={'coa': self.coa_data},
        )

    def apply_filters_to_hierarchy(self, hierarchy: CaseHierarchy,
                                   plan: QueryPlan) -> CaseHierarchy:
        """Apply WHERE clause filters to a hierarchy."""
        if hierarchy.name.lower() == 'gl':
            # Remove mappings for excluded GL values
            filtered_mappings = [
                m for m in hierarchy.mappings
                if m.result_value not in plan.excluded_gl_values
            ]
            removed = len(hierarchy.mappings) - len(filtered_mappings)
            if removed > 0:
                print(f"  Filtered out {removed} GL mappings due to WHERE clause")
            hierarchy.mappings = filtered_mappings

        return hierarchy

    def expand_with_coa(self, hierarchy: CaseHierarchy,
                        plan: QueryPlan) -> List[dict]:
        """Expand hierarchy mappings with COA data, applying filters."""
        rows = []

        for mapping in hierarchy.mappings:
            # Find matching account codes
            matched_accounts = []

            for code, data in self.coa_data.items():
                # Check exclusion filters first
                if code in plan.excluded_account_codes:
                    continue

                # Check pattern exclusions (NOT LIKE '242%')
                excluded = False
                for pattern in plan.excluded_account_patterns:
                    if pattern.endswith('%') and code.startswith(pattern[:-1]):
                        excluded = True
                        break
                if excluded:
                    continue

                # Check if this account matches the CASE condition
                matches = False
                if mapping.condition_type == 'ILIKE':
                    for pattern in mapping.condition_values:
                        if pattern.endswith('%') and code.lower().startswith(pattern[:-1].lower()):
                            matches = True
                            break
                        elif pattern.startswith('%') and code.lower().endswith(pattern[1:].lower()):
                            matches = True
                            break
                        elif '%' in pattern:
                            if pattern[1:-1].lower() in code.lower():
                                matches = True
                                break
                elif mapping.condition_type == 'IN':
                    matches = code in mapping.condition_values
                elif mapping.condition_type == '=':
                    matches = code == mapping.condition_values[0]

                if matches:
                    matched_accounts.append(data)

            # Create rows for matched accounts
            for acct in matched_accounts:
                rows.append({
                    'HIERARCHY_NAME': hierarchy.name,
                    'MAPPED_VALUE': mapping.result_value,
                    'SOURCE_COLUMN': hierarchy.source_column,
                    'CONDITION_TYPE': mapping.condition_type,
                    'CONDITION_VALUE': ','.join(mapping.condition_values[:3]),
                    'EXPANDED_ACCOUNT_CODE': acct['ACCOUNT_CODE'],
                    'EXPANDED_ACCOUNT_ID': acct['ACCOUNT_ID'],
                    'EXPANDED_ACCOUNT_NAME': acct['ACCOUNT_NAME'],
                    'EXPANDED_ACCOUNT_CLASS': acct['ACCOUNT_CLASS'],
                    'EXPANDED_BILLING_CATEGORY_CODE': acct['ACCOUNT_BILLING_CATEGORY_CODE'],
                    'MATCH_TYPE': 'COA_MATCH',
                    'FILTER_STATUS': 'INCLUDED',
                })

        return rows

    def analyze_and_export(self, sql: str, output_dir: str,
                          export_name: str) -> dict:
        """Full analysis with query plan and filtered exports."""
        print("=" * 80)
        print("SMART SQL ANALYZER - WITH QUERY PLAN EXECUTION")
        print("=" * 80)

        # Build query plan
        plan = self.build_query_plan(sql)

        # Print query plan
        print("\n" + "=" * 60)
        print("QUERY PLAN")
        print("=" * 60)
        print(f"\nHierarchies to extract: {len(plan.hierarchies)}")
        for h in plan.hierarchies:
            print(f"  - {h.name} ({len(h.mappings)} mappings)")

        print(f"\nGL values EXCLUDED by WHERE clause:")
        for gl in sorted(plan.excluded_gl_values):
            print(f"  - {gl}")

        print(f"\nAccount codes EXCLUDED:")
        print(f"  - Exact: {list(plan.excluded_account_codes)}")
        print(f"  - Patterns: {plan.excluded_account_patterns}")

        # Apply filters and expand
        print("\n" + "=" * 60)
        print("EXECUTING QUERY PLAN")
        print("=" * 60)

        Path(output_dir).mkdir(parents=True, exist_ok=True)

        all_summary_rows = []
        all_hierarchy_rows = []
        all_mapping_rows = []
        all_enriched_rows = []

        for hierarchy in plan.hierarchies:
            print(f"\nProcessing: {hierarchy.name}")

            # Apply WHERE filters
            hierarchy = self.apply_filters_to_hierarchy(hierarchy, plan)

            # Summary row
            all_summary_rows.append({
                'HIERARCHY_NAME': hierarchy.name,
                'SOURCE_COLUMN': hierarchy.source_column,
                'TOTAL_MAPPINGS': len(hierarchy.mappings),
                'HAS_ELSE': 'Yes' if hierarchy.else_value else 'No',
                'IS_FILTERED': 'Yes' if hierarchy.is_filtered else 'No',
            })

            # Hierarchy structure rows
            parent_id = hierarchy.name.upper()
            all_hierarchy_rows.append({
                'HIERARCHY_ID': parent_id,
                'HIERARCHY_NAME': hierarchy.name,
                'PARENT_ID': '',
                'SOURCE_COLUMN': hierarchy.source_column,
                'IS_LEAF_NODE': 'false',
            })

            # Get unique result values
            result_values = list(set(m.result_value for m in hierarchy.mappings))

            for result in result_values:
                child_id = f"{parent_id}_{result.upper().replace(' ', '_')[:25]}"
                all_hierarchy_rows.append({
                    'HIERARCHY_ID': child_id,
                    'HIERARCHY_NAME': result,
                    'PARENT_ID': parent_id,
                    'SOURCE_COLUMN': hierarchy.source_column,
                    'IS_LEAF_NODE': 'true',
                })

            # Mapping rows
            for mapping in hierarchy.mappings:
                for val in mapping.condition_values:
                    all_mapping_rows.append({
                        'HIERARCHY_ID': f"{parent_id}_{mapping.result_value.upper().replace(' ', '_')[:25]}",
                        'HIERARCHY_NAME': mapping.result_value,
                        'PARENT_HIERARCHY': hierarchy.name,
                        'SOURCE_COLUMN': hierarchy.source_column,
                        'CONDITION_TYPE': mapping.condition_type,
                        'CONDITION_VALUE': val,
                        'MAPPED_VALUE': mapping.result_value,
                    })

            # Enriched rows (with COA expansion)
            if hierarchy.source_column.lower() in ('account_code', 'acct', 'gl_code'):
                enriched = self.expand_with_coa(hierarchy, plan)
                all_enriched_rows.extend(enriched)
                print(f"  Expanded to {len(enriched)} COA-matched rows")

        # Write files
        print("\n" + "=" * 60)
        print("EXPORTING FILES")
        print("=" * 60)

        files_written = []

        # Summary
        summary_path = os.path.join(output_dir, f"{export_name}_SUMMARY.csv")
        self._write_csv(summary_path, all_summary_rows)
        files_written.append(summary_path)
        print(f"  [OK] {summary_path}")

        # Hierarchy
        hierarchy_path = os.path.join(output_dir, f"{export_name}_HIERARCHY.csv")
        self._write_csv(hierarchy_path, all_hierarchy_rows)
        files_written.append(hierarchy_path)
        print(f"  [OK] {hierarchy_path}")

        # Mapping
        mapping_path = os.path.join(output_dir, f"{export_name}_MAPPING.csv")
        self._write_csv(mapping_path, all_mapping_rows)
        files_written.append(mapping_path)
        print(f"  [OK] {mapping_path}")

        # Enriched (with COA)
        if all_enriched_rows:
            enriched_dir = os.path.join(output_dir, 'enriched')
            Path(enriched_dir).mkdir(parents=True, exist_ok=True)
            enriched_path = os.path.join(enriched_dir, f"{export_name}_MAPPING_ENRICHED.csv")
            self._write_csv(enriched_path, all_enriched_rows)
            files_written.append(enriched_path)
            print(f"  [OK] {enriched_path}")

        # Query plan JSON
        plan_path = os.path.join(output_dir, f"{export_name}_QUERY_PLAN.json")
        with open(plan_path, 'w') as f:
            json.dump({
                'hierarchies': [
                    {'name': h.name, 'mappings': len(h.mappings), 'source_column': h.source_column}
                    for h in plan.hierarchies
                ],
                'excluded_gl_values': list(plan.excluded_gl_values),
                'excluded_account_codes': list(plan.excluded_account_codes),
                'excluded_account_patterns': plan.excluded_account_patterns,
                'total_coa_records': len(self.coa_data),
            }, f, indent=2)
        files_written.append(plan_path)
        print(f"  [OK] {plan_path}")

        # Statistics
        stats = {
            'hierarchies_processed': len(plan.hierarchies),
            'gl_values_excluded': len(plan.excluded_gl_values),
            'total_mappings': len(all_mapping_rows),
            'total_enriched_rows': len(all_enriched_rows),
            'files_written': files_written,
        }

        print("\n" + "=" * 60)
        print("ANALYSIS COMPLETE")
        print("=" * 60)
        print(f"\n  Hierarchies: {stats['hierarchies_processed']}")
        print(f"  GL exclusions applied: {stats['gl_values_excluded']}")
        print(f"  Mapping rows: {stats['total_mappings']}")
        print(f"  Enriched rows: {stats['total_enriched_rows']}")

        return stats

    def _write_csv(self, path: str, rows: List[dict]) -> None:
        """Write rows to CSV."""
        if not rows:
            return
        with open(path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    # The full LOS SQL query
    LOS_SQL = """
SELECT
    CASE
        WHEN accts.account_code = '640-990'
        AND entries.transaction_description ILIKE '%COPAS%' THEN '640-992'
        ELSE accts.account_code
    END AS acctcode,
    CASE
        WHEN entries.transaction_description LIKE '%PAC %'
        AND accts.account_code = '641-990' THEN 'PAC990'
        WHEN entries.transaction_description LIKE '%NONOP LOE %'
        AND accts.account_code IN ('641-990', '640-990') THEN 'NLOE990'
        ELSE billcats.billcat
    END AS adjbillcat,
    CASE
        WHEN props.cost_center_state ILIKE ANY ('%UNKNOWN%', '%N/A%') THEN NULL
        WHEN props.cost_center_state IN ('AR', 'MS') THEN 'LA'
        ELSE props.cost_center_state
    END AS state,
    CASE
        WHEN entries.product_code = 'N/A' THEN NULL
        ELSE entries.product_code
    END AS productid,
    CASE
        WHEN account_code ILIKE '101%' THEN 'Cash'
        WHEN account_code ILIKE '125%' THEN 'Affiliate AR'
        WHEN account_code ILIKE ANY ('11%', '12%') THEN 'AR'
        WHEN account_code ILIKE '13%' THEN 'Prepaid Expenses'
        WHEN account_code ILIKE '14%' THEN 'Inventory'
        WHEN account_code ILIKE ANY ('15%', '16%') THEN 'Other Current Assets'
        WHEN account_code ILIKE '17%' THEN 'Derivative Assets'
        WHEN account_code ILIKE '18%' THEN 'Deferred Tax Assets'
        WHEN account_code IN (
            '205-102', '205-106', '205-112', '205-116', '205-117',
            '205-152', '205-190', '205-202', '205-206', '205-252',
            '205-990', '210-110', '210-140', '210-990', '215-110',
            '215-990', '220-110', '220-990', '225-110', '225-140',
            '225-990', '230-110', '230-140', '230-990', '232-200',
            '232-210', '235-105', '235-110', '235-115', '235-116',
            '235-120', '235-250', '235-275', '240-110', '240-140',
            '240-990', '242-150', '242-160', '244-110', '244-140',
            '244-410', '244-440', '244-560', '244-590', '244-610',
            '244-640', '244-990', '244-995', '244-998', '245-202',
            '245-206', '245-227', '245-252', '245-302', '245-306',
            '245-402', '245-412', '245-602', '245-902', '245-906',
            '246-312', '246-346', '246-322', '246-316'
        ) THEN 'Capex'
        WHEN account_code ILIKE ANY ('25%', '26%', '27%', '28%') THEN 'Other Assets'
        WHEN account_code ILIKE '29%' THEN 'Accumulated DD&A'
        WHEN account_code ILIKE ANY ('30%', '31%', '32%', '33%', '34%') THEN 'AP'
        WHEN account_code ILIKE ANY ('35%', '45%') THEN 'Total Debt'
        WHEN account_code ILIKE ANY ('36%', '37%', '38%') THEN 'Other Current Liabilities'
        WHEN account_code ILIKE ANY ('44%', '46%', '47%', '48%') THEN 'Other Liabilities'
        WHEN account_code ILIKE '49%' THEN 'Equity'
        WHEN account_code ILIKE '501%' THEN 'Oil Sales'
        WHEN account_code ILIKE ANY ('502%', '510%') THEN 'Gas Sales'
        WHEN account_code ILIKE ANY ('503%', '512%', '513%') THEN 'NGL Sales'
        WHEN account_code ILIKE ANY ('504%', '520%', '570%', '590-100', '590-110', '590-410', '590-510', '590-900') THEN 'Other Income'
        WHEN account_code ILIKE ANY ('514%', '519%', '518-120', '590-710', '674%', '695%') THEN 'Cashouts'
        WHEN account_code IN ('515-100', '515-990') THEN 'Service Revenue'
        WHEN account_code IN ('515-110', '515-199', '610-110', '610-120', '610-130') THEN 'Gathering Fees'
        WHEN account_code IN ('515-120', '613-110', '613-120') THEN 'Compression Fees'
        WHEN account_code IN ('515-130', '515-140', '612-110', '612-120', '614-110', '614-120', '619-990') THEN 'Treating Fees'
        WHEN account_code IN ('515-210', '610-210', '610-220') THEN 'Capital Recovery Fees'
        WHEN account_code = '517-110' THEN 'Demand Fees'
        WHEN account_code ILIKE ANY ('517%', '611-210', '611-220', '613-130', '613-140', '619-110', '619-120', '619-275', '619-991') THEN 'Transportation Fees'
        WHEN account_code ILIKE '518%' THEN 'Gas Sales'
        WHEN account_code = '530-100' THEN 'Service Income'
        WHEN account_code IN ('530-110', '530-111', '530-990') THEN 'Sand Sales'
        WHEN account_code IN ('515-205', '530-120', '530-140', '530-720', '530-990', '530-991', '530-993', '590-310') THEN 'Rental Income'
        WHEN account_code IN ('530-150', '530-994', '590-620') THEN 'Water Income'
        WHEN account_code IN ('530-160', '530-995', '590-610') THEN 'SWD Income'
        WHEN account_code IN ('530-170', '530-996') THEN 'Consulting Income'
        WHEN account_code IN ('530-180', '530-997') THEN 'Fuel Income'
        WHEN account_code ILIKE '580%' THEN 'Hedge Gains'
        WHEN account_code = '581-540' THEN 'Interest Hedge Gains'
        WHEN account_code ILIKE '581%' THEN 'Unrealized Hedge Gains'
        WHEN account_code IN ('590-210', '590-211', '590-990', '640-992') THEN 'COPAS'
        WHEN account_code = '590-555' THEN 'Compressor Recovery Income'
        WHEN account_code = '590-850' THEN 'Rig Termination Penalties'
        WHEN account_code = '590-991' THEN 'Gathering Fee Income'
        WHEN account_code IN ('601-100', '601-110', '601-113', '601-120', '601-123', '601-275', '601-990') THEN 'Oil Severance Taxes'
        WHEN account_code IN ('602-100', '602-110', '602-113', '602-120', '602-123', '602-275', '602-990') THEN 'Gas Severance Taxes'
        WHEN account_code IN ('603-100', '603-110', '603-113', '603-120', '603-123', '603-275', '603-990') THEN 'NGL Severance Taxes'
        WHEN account_code IN ('601-112', '601-122', '602-112', '602-122', '603-112', '603-122', '640-120', '640-991') THEN 'Ad Valorem Taxes'
        WHEN account_code IN ('601-111', '601-121') THEN 'Oil Conservation Taxes'
        WHEN account_code IN ('602-111', '602-121') THEN 'Gas Conservation Taxes'
        WHEN account_code IN ('603-111', '603-121') THEN 'NGL Conservation Taxes'
        WHEN account_code = '611-110' THEN 'Commodity Fees'
        WHEN account_code ILIKE ANY ('611-120', '62%') THEN 'Non-Op Fees'
        WHEN account_code ILIKE ANY ('630-1%', '710-170', '710-996') THEN 'Consulting Expenses'
        WHEN account_code IN ('640-110', '640-100', '640-275', '640-300', '640-990', '641-110', '641-100', '641-990') THEN 'Lease Operating Expenses'
        WHEN account_code IN ('641-150', '963-100') THEN 'Accretion Expense'
        WHEN account_code ILIKE '642%' THEN 'Leasehold Expenses'
        WHEN account_code ILIKE '645%' THEN 'Exploration Expenses'
        WHEN account_code ILIKE ANY ('650%', '660%', '667%', '668%') THEN 'Third Party Fees Paid'
        WHEN account_code ILIKE ANY ('665%', '666%', '669%') THEN 'Midstream Operating Expenses'
        WHEN account_code ILIKE ANY ('670%', '680%', '690%', '691%') THEN 'Cost of Purchased Gas'
        WHEN account_code IN ('700-100', '700-110', '700-800', '700-990', '701-100', '701-110') THEN 'Sand Purchases'
        WHEN account_code IN ('700-150', '700-994') THEN 'Water Purchases'
        WHEN account_code IN ('700-180', '700-997') THEN 'Fuel Purchases'
        WHEN account_code IN ('710-100', '710-120', '710-140', '710-300', '710-301', '710-991', '710-992', '710-993', '720-120', '720-985') THEN 'Rental Expenses'
        WHEN account_code IN ('710-110', '710-990') THEN 'Sand Expenses'
        WHEN account_code IN ('710-150', '710-994') THEN 'Water Expenses'
        WHEN account_code IN ('710-160', '710-995') THEN 'SWD Expenses'
        WHEN account_code IN ('710-180', '710-997') THEN 'Fuel Expenses'
        WHEN account_code ILIKE '8%' THEN 'General & Administrative'
        WHEN account_code IN ('910-100', '920-100') THEN 'Interest Income'
        WHEN account_code ILIKE ANY ('93%', '94%') THEN 'Other Gains/Losses'
        WHEN account_code ILIKE ANY ('950-1%', '950-300') THEN 'Interest Expense'
        WHEN account_code ILIKE ANY ('950-8%', '955%', '970%', '972%', '974%', '975%') THEN 'DD&A'
        WHEN account_code = '971-210' THEN 'Impairment Expense'
        WHEN account_code = '960-100' THEN 'Bad Debt Expense'
        WHEN account_code = '965-200' THEN 'Other Expenses'
    END AS gl,
    CASE
        WHEN account_billing_category_code ILIKE ANY ('%CC85%', '%DC85%', 'FC85%', 'NICC%', 'NIDC%') THEN 'CNOP'
        WHEN account_billing_category_code ILIKE ANY ('%C990%') THEN 'CACR'
        WHEN account_billing_category_type_code IN ('ICC', 'TCC') THEN 'CFRC'
        WHEN account_billing_category_type_code IN ('IDC', 'TDC') THEN 'CDRL'
        WHEN account_billing_category_code ILIKE ANY ('LOE10%', 'MOE10%') THEN 'LBR'
        WHEN account_billing_category_code ILIKE ANY ('LOE11%', 'LOE320') THEN 'OHD'
        WHEN account_billing_category_code ILIKE ANY ('LOE140', 'LOE160') THEN 'SVC'
        WHEN account_billing_category_code ILIKE ANY ('LOE24%', 'LOE25%') THEN 'CHM'
        WHEN account_billing_category_code IN ('LOE273', 'LOE274', 'LOE275') THEN 'SWD'
        WHEN account_billing_category_code IN ('LOE295', 'LOE300', 'LOE301') THEN 'RNM'
        WHEN account_billing_category_code IN ('MOE555', 'MOE556') THEN 'CMP'
        WHEN account_billing_category_code ILIKE 'LOE6%' THEN 'COPAS'
        WHEN account_billing_category_code IN ('LOE720', 'LOE721') THEN 'SEV'
        WHEN account_billing_category_code IN ('LOE750', 'LOE751', 'NLOE750') THEN 'ADV'
        WHEN account_billing_category_code ILIKE ANY ('LOE850', 'LOE990', 'NLOE%') THEN 'NLOE'
        WHEN account_billing_category_type_code IN ('PAC', 'WOX', 'MOX') THEN account_billing_category_type_code
    END AS los_map,
    CASE
        WHEN cost_center_area_code IN ('AREA0100', 'AREA0200', 'AREA0220') THEN TRUE
        ELSE FALSE
    END AS AU_Op,
    CASE
        WHEN cost_center_area_code IN ('AREA0210', 'AREA0320', 'AREA0440') THEN TRUE
        ELSE FALSE
    END AS A3_Op,
    CASE
        WHEN corp_code IN (551, 561, 565, 578, 586, 587, 588, 598, 702, 755) THEN 'A3'
        WHEN corp_code IN (410, 420, 550, 560, 580, 585, 586, 590, 595, 599, 600, 650, 700, 701, 750, 751) THEN 'AU'
        WHEN corp_code IN (012, 043, 049, 052) THEN '4HC'
        ELSE NULL
    END AS fund,
    CASE
        WHEN corp_code IN (12, 43, 44, 540, 550, 551, 560, 561, 565, 650, 750, 751, 755) THEN 'Upstream'
        WHEN corp_code IN (41, 410, 578, 580, 585, 586, 587, 588, 590, 595) THEN 'Midstream'
        WHEN corp_code = 600 THEN 'Marketing'
        WHEN corp_code BETWEEN 700 AND 702 THEN 'Services'
        ELSE NULL
    END AS Segment,
    CASE
        WHEN corp_code IN (550, 560, 580, 585, 590, 595, 599, 600, 650, 700, 701, 750, 751) THEN 1
        WHEN corp_code IN (410, 420) THEN 0.9
        WHEN corp_code = 586 THEN 0.225
        ELSE 0
    END AS AU_Stake,
    CASE
        WHEN corp_code IN (551, 561, 565, 587, 598, 702, 755) THEN 1
        WHEN corp_code IN (578, 587) THEN 0.9
        WHEN corp_code = 586 THEN 0.675
        ELSE 0
    END AS A3_Stake
FROM fact_financial_details
WHERE
    gl NOT IN (
        'Omit',
        'Hedge Gains',
        'General & Administrative',
        'Impairment Expense',
        'DD&A',
        'Accretion Expense',
        'Interest Expense',
        'Interest Income',
        'Other Gains/Losses',
        'Interest Hedge Gains',
        'Unrealized Hedge Gains'
    )
    AND account_code <> '570-115'
    AND account_code NOT LIKE '242%'
"""

    # Run analysis
    analyzer = SmartSQLAnalyzer(
        coa_path=r"C:\Users\telha\Databridge_AI\Gemini\Uploads\DIM_ACCOUNT.csv"
    )

    stats = analyzer.analyze_and_export(
        sql=LOS_SQL,
        output_dir="./result_export/smart_analysis",
        export_name="los_filtered"
    )
