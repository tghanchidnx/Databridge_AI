"""
DDL Diff Comparator.

Phase 31: Compares generated DDL against existing/baseline DDL.

From docs/databridge_complete_analysis.md Section 13 (Validation Milestones):
- Generate GROSS VW_1 DDL from template and diff against production script
- Verify row counts match production
- Validate DDL before deployment

This module:
1. Compares generated DDL to existing baseline files
2. Identifies structural differences (columns, joins, aggregations)
3. Highlights semantic changes that could affect data
4. Integrates with the Diff module for detailed comparison
"""

import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field

from ..diff.core import (
    compute_similarity,
    unified_diff,
    diff_lists,
    get_opcodes,
    explain_diff_human_readable,
)

logger = logging.getLogger(__name__)


@dataclass
class DDLSection:
    """A parsed section of DDL."""
    section_type: str  # CREATE, SELECT, FROM, WHERE, GROUP_BY, etc.
    content: str
    start_line: int
    end_line: int


@dataclass
class ColumnDiff:
    """Difference in a column definition."""
    column_name: str
    status: str  # "added", "removed", "modified"
    old_definition: Optional[str] = None
    new_definition: Optional[str] = None
    explanation: Optional[str] = None


@dataclass
class DDLDiffResult:
    """Result of DDL comparison."""
    generated_file: str
    baseline_file: Optional[str]

    # Overall comparison
    similarity: float = 0.0
    is_identical: bool = False

    # Structural differences
    column_diffs: List[ColumnDiff] = field(default_factory=list)
    join_diffs: List[str] = field(default_factory=list)
    filter_diffs: List[str] = field(default_factory=list)
    aggregation_diffs: List[str] = field(default_factory=list)

    # Semantic impact
    breaking_changes: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    info: List[str] = field(default_factory=list)

    # Raw diff
    unified_diff: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "generated_file": self.generated_file,
            "baseline_file": self.baseline_file,
            "similarity": self.similarity,
            "similarity_percent": f"{self.similarity * 100:.1f}%",
            "is_identical": self.is_identical,
            "column_diff_count": len(self.column_diffs),
            "join_diff_count": len(self.join_diffs),
            "filter_diff_count": len(self.filter_diffs),
            "breaking_change_count": len(self.breaking_changes),
            "warning_count": len(self.warnings),
            "column_diffs": [
                {
                    "column": cd.column_name,
                    "status": cd.status,
                    "explanation": cd.explanation,
                }
                for cd in self.column_diffs
            ],
            "breaking_changes": self.breaking_changes,
            "warnings": self.warnings,
        }


class DDLDiffComparator:
    """
    Compares generated DDL against baseline DDL.

    Performs:
    1. Text similarity analysis
    2. Column extraction and comparison
    3. JOIN clause analysis
    4. WHERE clause analysis
    5. Semantic impact assessment
    """

    # Patterns for DDL parsing
    PATTERNS = {
        "create_view": re.compile(r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+(\w+)", re.I),
        "create_table": re.compile(r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:DYNAMIC\s+)?TABLE\s+(\w+)", re.I),
        "select_columns": re.compile(r"SELECT\s+(.*?)\s+FROM", re.I | re.S),
        "from_clause": re.compile(r"FROM\s+(.*?)(?:WHERE|GROUP\s+BY|ORDER\s+BY|;|$)", re.I | re.S),
        "where_clause": re.compile(r"WHERE\s+(.*?)(?:GROUP\s+BY|ORDER\s+BY|;|$)", re.I | re.S),
        "group_by": re.compile(r"GROUP\s+BY\s+(.*?)(?:ORDER\s+BY|HAVING|;|$)", re.I | re.S),
        "join": re.compile(r"((?:LEFT|RIGHT|INNER|FULL|CROSS)?\s*JOIN\s+\w+\s+\w*\s*ON\s+[^)]+)", re.I),
    }

    def __init__(self):
        """Initialize the comparator."""
        pass

    def compare_ddl(
        self,
        generated_ddl: str,
        baseline_ddl: str,
        generated_file: str = "generated.sql",
        baseline_file: str = "baseline.sql",
    ) -> DDLDiffResult:
        """
        Compare generated DDL against baseline.

        Args:
            generated_ddl: The generated DDL content
            baseline_ddl: The baseline DDL to compare against
            generated_file: Name of generated file
            baseline_file: Name of baseline file

        Returns:
            DDLDiffResult with detailed comparison
        """
        result = DDLDiffResult(
            generated_file=generated_file,
            baseline_file=baseline_file,
        )

        # Normalize DDL for comparison
        gen_normalized = self._normalize_ddl(generated_ddl)
        base_normalized = self._normalize_ddl(baseline_ddl)

        # Compute overall similarity
        result.similarity = compute_similarity(gen_normalized, base_normalized)
        result.is_identical = gen_normalized == base_normalized

        if result.is_identical:
            result.info.append("DDL is identical")
            return result

        # Generate unified diff
        result.unified_diff = unified_diff(
            baseline_ddl,
            generated_ddl,
            from_label=baseline_file,
            to_label=generated_file,
        )

        # Compare columns
        gen_columns = self._extract_columns(generated_ddl)
        base_columns = self._extract_columns(baseline_ddl)
        self._compare_columns(gen_columns, base_columns, result)

        # Compare JOINs
        gen_joins = self._extract_joins(generated_ddl)
        base_joins = self._extract_joins(baseline_ddl)
        self._compare_joins(gen_joins, base_joins, result)

        # Compare WHERE clauses
        gen_where = self._extract_where(generated_ddl)
        base_where = self._extract_where(baseline_ddl)
        self._compare_filters(gen_where, base_where, result)

        # Assess semantic impact
        self._assess_impact(result)

        return result

    def compare_file(
        self,
        generated_path: str,
        baseline_path: str,
    ) -> DDLDiffResult:
        """
        Compare DDL files.

        Args:
            generated_path: Path to generated DDL file
            baseline_path: Path to baseline DDL file

        Returns:
            DDLDiffResult
        """
        gen_path = Path(generated_path)
        base_path = Path(baseline_path)

        if not gen_path.exists():
            raise FileNotFoundError(f"Generated file not found: {generated_path}")
        if not base_path.exists():
            raise FileNotFoundError(f"Baseline file not found: {baseline_path}")

        generated_ddl = gen_path.read_text()
        baseline_ddl = base_path.read_text()

        return self.compare_ddl(
            generated_ddl=generated_ddl,
            baseline_ddl=baseline_ddl,
            generated_file=str(gen_path.name),
            baseline_file=str(base_path.name),
        )

    def compare_pipeline(
        self,
        generated_objects: List[Dict[str, Any]],
        baseline_dir: str,
    ) -> Dict[str, DDLDiffResult]:
        """
        Compare a full pipeline against baseline files.

        Args:
            generated_objects: List of generated PipelineObject dicts
            baseline_dir: Directory containing baseline DDL files

        Returns:
            Dict mapping object name to DDLDiffResult
        """
        results = {}
        base_dir = Path(baseline_dir)

        for obj in generated_objects:
            obj_name = obj.get("object_name", "unknown")
            ddl = obj.get("ddl", "")

            # Look for matching baseline file
            baseline_file = None
            for ext in [".sql", ".ddl"]:
                candidate = base_dir / f"{obj_name}{ext}"
                if candidate.exists():
                    baseline_file = candidate
                    break

            if baseline_file:
                result = self.compare_ddl(
                    generated_ddl=ddl,
                    baseline_ddl=baseline_file.read_text(),
                    generated_file=f"{obj_name}.sql",
                    baseline_file=str(baseline_file.name),
                )
            else:
                result = DDLDiffResult(
                    generated_file=f"{obj_name}.sql",
                    baseline_file=None,
                )
                result.info.append("No baseline file found for comparison")

            results[obj_name] = result

        return results

    def _normalize_ddl(self, ddl: str) -> str:
        """Normalize DDL for comparison."""
        # Remove comments
        lines = []
        for line in ddl.split("\n"):
            # Remove single-line comments
            if "--" in line:
                line = line[:line.index("--")]
            lines.append(line)
        ddl = "\n".join(lines)

        # Normalize whitespace
        ddl = re.sub(r"\s+", " ", ddl)
        ddl = ddl.strip()

        # Normalize case for keywords
        keywords = [
            "CREATE", "OR", "REPLACE", "VIEW", "TABLE", "DYNAMIC",
            "SELECT", "FROM", "WHERE", "AND", "OR", "NOT",
            "LEFT", "RIGHT", "INNER", "OUTER", "JOIN", "ON",
            "GROUP", "BY", "ORDER", "ASC", "DESC", "HAVING",
            "CASE", "WHEN", "THEN", "ELSE", "END", "AS",
            "SUM", "COUNT", "AVG", "MIN", "MAX",
            "UNION", "ALL", "DISTINCT",
        ]
        for kw in keywords:
            ddl = re.sub(rf"\b{kw}\b", kw.upper(), ddl, flags=re.I)

        return ddl

    def _extract_columns(self, ddl: str) -> List[str]:
        """Extract column definitions from SELECT clause."""
        match = self.PATTERNS["select_columns"].search(ddl)
        if not match:
            return []

        select_content = match.group(1)

        # Split by comma, respecting parentheses
        columns = []
        depth = 0
        current = []

        for char in select_content:
            if char == "(":
                depth += 1
            elif char == ")":
                depth -= 1
            elif char == "," and depth == 0:
                col = "".join(current).strip()
                if col:
                    columns.append(col)
                current = []
                continue
            current.append(char)

        # Add last column
        col = "".join(current).strip()
        if col:
            columns.append(col)

        return columns

    def _extract_joins(self, ddl: str) -> List[str]:
        """Extract JOIN clauses."""
        return self.PATTERNS["join"].findall(ddl)

    def _extract_where(self, ddl: str) -> str:
        """Extract WHERE clause."""
        match = self.PATTERNS["where_clause"].search(ddl)
        return match.group(1).strip() if match else ""

    def _compare_columns(
        self,
        gen_columns: List[str],
        base_columns: List[str],
        result: DDLDiffResult,
    ) -> None:
        """Compare column lists."""
        # Extract column names (handle AS aliases)
        def get_name(col: str) -> str:
            # Look for AS alias
            match = re.search(r"\bAS\s+(\w+)\s*$", col, re.I)
            if match:
                return match.group(1).upper()
            # Use last identifier
            identifiers = re.findall(r"(\w+)", col)
            return identifiers[-1].upper() if identifiers else col.upper()

        gen_names = {get_name(c): c for c in gen_columns}
        base_names = {get_name(c): c for c in base_columns}

        # Find added columns
        for name in set(gen_names.keys()) - set(base_names.keys()):
            result.column_diffs.append(ColumnDiff(
                column_name=name,
                status="added",
                new_definition=gen_names[name],
                explanation=f"New column added: {name}",
            ))

        # Find removed columns
        for name in set(base_names.keys()) - set(gen_names.keys()):
            result.column_diffs.append(ColumnDiff(
                column_name=name,
                status="removed",
                old_definition=base_names[name],
                explanation=f"Column removed: {name}",
            ))

        # Find modified columns
        for name in set(gen_names.keys()) & set(base_names.keys()):
            gen_def = gen_names[name]
            base_def = base_names[name]

            # Normalize for comparison
            gen_norm = re.sub(r"\s+", " ", gen_def).strip().upper()
            base_norm = re.sub(r"\s+", " ", base_def).strip().upper()

            if gen_norm != base_norm:
                similarity = compute_similarity(gen_norm, base_norm)
                result.column_diffs.append(ColumnDiff(
                    column_name=name,
                    status="modified",
                    old_definition=base_def,
                    new_definition=gen_def,
                    explanation=f"Column modified ({similarity:.0%} similar)",
                ))

    def _compare_joins(
        self,
        gen_joins: List[str],
        base_joins: List[str],
        result: DDLDiffResult,
    ) -> None:
        """Compare JOIN clauses."""
        list_diff = diff_lists(base_joins, gen_joins)

        for j in list_diff.added:
            result.join_diffs.append(f"Added: {j}")

        for j in list_diff.removed:
            result.join_diffs.append(f"Removed: {j}")

    def _compare_filters(
        self,
        gen_where: str,
        base_where: str,
        result: DDLDiffResult,
    ) -> None:
        """Compare WHERE clauses."""
        if not gen_where and not base_where:
            return

        if gen_where != base_where:
            similarity = compute_similarity(gen_where, base_where)
            result.filter_diffs.append(
                f"WHERE clause changed ({similarity:.0%} similar)"
            )

            if gen_where and not base_where:
                result.filter_diffs.append("Added WHERE clause")
            elif base_where and not gen_where:
                result.filter_diffs.append("Removed WHERE clause")

    def _assess_impact(self, result: DDLDiffResult) -> None:
        """Assess the semantic impact of differences."""
        # Breaking changes
        for cd in result.column_diffs:
            if cd.status == "removed":
                result.breaking_changes.append(
                    f"Column {cd.column_name} removed - may break dependent queries"
                )

        for jd in result.join_diffs:
            if "Removed:" in jd:
                result.breaking_changes.append(
                    f"JOIN removed - may affect row counts: {jd}"
                )

        # Warnings
        for cd in result.column_diffs:
            if cd.status == "modified":
                result.warnings.append(
                    f"Column {cd.column_name} definition changed - verify calculation logic"
                )

        for fd in result.filter_diffs:
            if "Removed WHERE" in fd or "changed" in fd:
                result.warnings.append(
                    f"Filter logic changed - may affect data volume: {fd}"
                )


def compare_generated_ddl(
    generated_ddl: str,
    baseline_ddl: str,
) -> Dict[str, Any]:
    """
    Convenience function to compare DDL.

    Args:
        generated_ddl: Generated DDL content
        baseline_ddl: Baseline DDL content

    Returns:
        Comparison result dictionary
    """
    comparator = DDLDiffComparator()
    result = comparator.compare_ddl(generated_ddl, baseline_ddl)
    return result.to_dict()
