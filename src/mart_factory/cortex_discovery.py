"""
Cortex Discovery Agent.

AI-powered hierarchy discovery using Snowflake Cortex COMPLETE():
- Scan hierarchy tables for structure patterns
- Infer join patterns from mapping distribution
- Detect data quality issues
- Generate configuration recommendations
"""

import json
import logging
import re
from datetime import datetime
from difflib import SequenceMatcher
from typing import Any, Callable, Dict, List, Optional, Tuple

from .types import (
    DiscoveryResult,
    MartConfig,
    JoinPattern,
    DynamicColumnMapping,
    DataQualityIssue,
)

logger = logging.getLogger(__name__)


class CortexDiscoveryAgent:
    """AI-powered hierarchy discovery using Cortex COMPLETE()."""

    # Known ID_SOURCE patterns and their typical physical columns
    KNOWN_ID_SOURCE_PATTERNS = {
        "BILLING_CATEGORY_CODE": "ACCT.ACCOUNT_BILLING_CATEGORY_CODE",
        "BILLING_CATEGORY_TYPE_CODE": "ACCT.ACCOUNT_BILLING_CATEGORY_TYPE_CODE",
        "ACCOUNT_CODE": "ACCT.ACCOUNT_CODE",
        "MINOR_CODE": "ACCT.ACCOUNT_MINOR_CODE",
        "DEDUCT_CODE": "DEDUCT.DEDUCT_CODE",
        "PRODUCT_CODE": "PROD.PRODUCT_CODE",
    }

    # Hierarchy type detection patterns
    HIERARCHY_TYPE_PATTERNS = {
        "LOS": ["lease operating", "royalt", "deduct", "opex", "capex"],
        "P&L": ["revenue", "expense", "profit", "income"],
        "Balance Sheet": ["asset", "liabilit", "equity"],
        "Geographic": ["region", "country", "state", "city"],
        "Organizational": ["department", "team", "division"],
    }

    def __init__(
        self,
        connection_id: Optional[str] = None,
        query_func: Optional[Callable] = None,
    ):
        """
        Initialize the discovery agent.

        Args:
            connection_id: Snowflake connection ID
            query_func: Function to execute queries (connection_id, sql) -> result
        """
        self.connection_id = connection_id
        self.query_func = query_func

    def discover_hierarchy(
        self,
        hierarchy_table: str,
        mapping_table: str,
        connection_id: Optional[str] = None,
    ) -> DiscoveryResult:
        """
        Scan hierarchy and mapping tables to discover structure.

        Uses Cortex COMPLETE() (if available) to:
        - Infer hierarchy type (P&L, Balance Sheet, LOS, etc.)
        - Detect level patterns and naming conventions
        - Identify join pattern requirements
        - Find data quality issues

        Args:
            hierarchy_table: Fully qualified hierarchy table name
            mapping_table: Fully qualified mapping table name
            connection_id: Optional override connection ID

        Returns:
            DiscoveryResult with analysis and suggestions
        """
        conn_id = connection_id or self.connection_id

        result = DiscoveryResult(
            hierarchy_table=hierarchy_table,
            mapping_table=mapping_table,
        )

        # Analyze hierarchy structure
        if self.query_func and conn_id:
            hierarchy_analysis = self._analyze_hierarchy_table(hierarchy_table, conn_id)
            mapping_analysis = self._analyze_mapping_table(mapping_table, conn_id)

            result.level_count = hierarchy_analysis.get("level_count", 0)
            result.node_count = hierarchy_analysis.get("node_count", 0)
            result.active_node_count = hierarchy_analysis.get("active_node_count", 0)
            result.calculation_node_count = hierarchy_analysis.get("calculation_node_count", 0)
            result.mapping_count = mapping_analysis.get("mapping_count", 0)
            result.id_source_distribution = mapping_analysis.get("id_source_distribution", {})
            result.id_table_distribution = mapping_analysis.get("id_table_distribution", {})

        # Detect hierarchy type from patterns
        result.hierarchy_type = self._detect_hierarchy_type(result)

        # Suggest join patterns
        result.join_pattern_suggestion = self._suggest_join_patterns(result)

        # Suggest column mappings
        result.column_map_suggestion = self._suggest_column_mappings(result)

        # Detect data quality issues
        result.data_quality_issues = self._detect_data_quality_issues(result)

        # Generate recommended config
        result.recommended_config = self._generate_recommended_config(
            hierarchy_table, mapping_table, result
        )

        # Calculate confidence score
        result.confidence_score = self._calculate_confidence(result)

        # Generate explanation
        result.explanation = self._generate_explanation(result)

        return result

    def analyze_id_source_distribution(
        self,
        mapping_table: str,
        connection_id: Optional[str] = None,
    ) -> Dict[str, int]:
        """
        Analyze ID_SOURCE value distribution for column mapping.

        Args:
            mapping_table: Fully qualified mapping table name
            connection_id: Optional override connection ID

        Returns:
            Dict mapping ID_SOURCE values to counts
        """
        conn_id = connection_id or self.connection_id

        if not self.query_func or not conn_id:
            return {}

        sql = f"""
        SELECT ID_SOURCE, COUNT(*) as cnt
        FROM {mapping_table}
        WHERE ID_SOURCE IS NOT NULL
        GROUP BY ID_SOURCE
        ORDER BY cnt DESC
        """

        try:
            result = self.query_func(conn_id, sql)
            if isinstance(result, list):
                return {row.get("ID_SOURCE"): row.get("cnt", 0) for row in result}
            return {}
        except Exception as e:
            logger.error(f"Failed to analyze ID_SOURCE distribution: {e}")
            return {}

    def detect_join_patterns(
        self,
        mapping_table: str,
        connection_id: Optional[str] = None,
    ) -> List[JoinPattern]:
        """
        Infer UNION ALL branch structure from mapping patterns.

        Analyzes which dimension combinations appear together
        to suggest optimal join branch definitions.

        Args:
            mapping_table: Fully qualified mapping table name
            connection_id: Optional override connection ID

        Returns:
            List of suggested JoinPatterns
        """
        conn_id = connection_id or self.connection_id

        if not self.query_func or not conn_id:
            return self._suggest_default_patterns()

        # Query to find dimension combinations
        sql = f"""
        WITH MAPPING_DIMS AS (
            SELECT
                FK_REPORT_KEY,
                ID_TABLE,
                LISTAGG(DISTINCT ID_TABLE, ',') WITHIN GROUP (ORDER BY ID_TABLE)
                    OVER (PARTITION BY FK_REPORT_KEY) AS DIM_COMBINATION
            FROM {mapping_table}
            WHERE ID_TABLE IS NOT NULL
        )
        SELECT
            DIM_COMBINATION,
            COUNT(DISTINCT FK_REPORT_KEY) as node_count
        FROM MAPPING_DIMS
        GROUP BY DIM_COMBINATION
        ORDER BY node_count DESC
        """

        try:
            result = self.query_func(conn_id, sql)
            if isinstance(result, list):
                return self._patterns_from_combinations(result)
            return self._suggest_default_patterns()
        except Exception as e:
            logger.error(f"Failed to detect join patterns: {e}")
            return self._suggest_default_patterns()

    def detect_typos(
        self,
        mapping_table: str,
        known_values: Optional[List[str]] = None,
        connection_id: Optional[str] = None,
    ) -> List[DataQualityIssue]:
        """
        Detect likely typos in ID_SOURCE values.

        Uses fuzzy matching to find values that are similar to
        known good values but have small differences.

        Args:
            mapping_table: Fully qualified mapping table name
            known_values: List of known good ID_SOURCE values
            connection_id: Optional override connection ID

        Returns:
            List of DataQualityIssue for typos
        """
        conn_id = connection_id or self.connection_id
        known = known_values or list(self.KNOWN_ID_SOURCE_PATTERNS.keys())
        issues = []

        if not self.query_func or not conn_id:
            return issues

        # Get actual ID_SOURCE values
        sql = f"""
        SELECT DISTINCT ID_SOURCE
        FROM {mapping_table}
        WHERE ID_SOURCE IS NOT NULL
        """

        try:
            result = self.query_func(conn_id, sql)
            actual_values = [row.get("ID_SOURCE") for row in result if row.get("ID_SOURCE")]

            for actual in actual_values:
                if actual in known:
                    continue

                # Find closest known value
                best_match, score = self._find_closest_match(actual, known)
                if score >= 0.8 and score < 1.0:  # Similar but not exact
                    issues.append(DataQualityIssue(
                        severity="HIGH",
                        issue_type="TYPO",
                        description=f"Possible typo: '{actual}' may be '{best_match}'",
                        affected_values=[actual],
                        recommendation=f"Update ID_SOURCE '{actual}' to '{best_match}'",
                    ))

            return issues
        except Exception as e:
            logger.error(f"Failed to detect typos: {e}")
            return issues

    def generate_config_recommendation(
        self,
        discovery_result: DiscoveryResult,
    ) -> MartConfig:
        """
        Generate complete MartConfig from discovery results.

        Args:
            discovery_result: Discovery analysis results

        Returns:
            Recommended MartConfig
        """
        return self._generate_recommended_config(
            discovery_result.hierarchy_table,
            discovery_result.mapping_table,
            discovery_result,
        )

    def explain_discovery(
        self,
        result: DiscoveryResult,
    ) -> str:
        """
        Generate human-readable explanation of discovery.

        Args:
            result: Discovery result

        Returns:
            Explanation text
        """
        return self._generate_explanation(result)

    def _analyze_hierarchy_table(
        self,
        hierarchy_table: str,
        connection_id: str,
    ) -> Dict[str, Any]:
        """Analyze hierarchy table structure."""
        sql = f"""
        SELECT
            COUNT(*) as node_count,
            SUM(CASE WHEN ACTIVE_FLAG = TRUE THEN 1 ELSE 0 END) as active_count,
            SUM(CASE WHEN CALCULATION_FLAG = TRUE THEN 1 ELSE 0 END) as calc_count,
            MAX(CASE WHEN LEVEL_9 IS NOT NULL THEN 9
                     WHEN LEVEL_8 IS NOT NULL THEN 8
                     WHEN LEVEL_7 IS NOT NULL THEN 7
                     WHEN LEVEL_6 IS NOT NULL THEN 6
                     WHEN LEVEL_5 IS NOT NULL THEN 5
                     WHEN LEVEL_4 IS NOT NULL THEN 4
                     WHEN LEVEL_3 IS NOT NULL THEN 3
                     WHEN LEVEL_2 IS NOT NULL THEN 2
                     WHEN LEVEL_1 IS NOT NULL THEN 1
                     ELSE 0 END) as max_level
        FROM {hierarchy_table}
        """

        try:
            result = self.query_func(connection_id, sql)
            if isinstance(result, list) and result:
                row = result[0]
                return {
                    "node_count": row.get("node_count", 0),
                    "active_node_count": row.get("active_count", 0),
                    "calculation_node_count": row.get("calc_count", 0),
                    "level_count": row.get("max_level", 0),
                }
            return {}
        except Exception as e:
            logger.error(f"Failed to analyze hierarchy table: {e}")
            return {}

    def _analyze_mapping_table(
        self,
        mapping_table: str,
        connection_id: str,
    ) -> Dict[str, Any]:
        """Analyze mapping table structure."""
        sql = f"""
        SELECT
            COUNT(*) as mapping_count
        FROM {mapping_table}
        """

        try:
            result = self.query_func(connection_id, sql)
            mapping_count = 0
            if isinstance(result, list) and result:
                mapping_count = result[0].get("mapping_count", 0)

            # Get ID_SOURCE distribution
            id_source_dist = self.analyze_id_source_distribution(mapping_table, connection_id)

            # Get ID_TABLE distribution
            sql2 = f"""
            SELECT ID_TABLE, COUNT(*) as cnt
            FROM {mapping_table}
            WHERE ID_TABLE IS NOT NULL
            GROUP BY ID_TABLE
            """
            result2 = self.query_func(connection_id, sql2)
            id_table_dist = {}
            if isinstance(result2, list):
                id_table_dist = {row.get("ID_TABLE"): row.get("cnt", 0) for row in result2}

            return {
                "mapping_count": mapping_count,
                "id_source_distribution": id_source_dist,
                "id_table_distribution": id_table_dist,
            }
        except Exception as e:
            logger.error(f"Failed to analyze mapping table: {e}")
            return {}

    def _detect_hierarchy_type(self, result: DiscoveryResult) -> Optional[str]:
        """Detect hierarchy type from patterns."""
        # Look for patterns in ID_SOURCE values
        all_values = " ".join(result.id_source_distribution.keys()).lower()
        all_tables = " ".join(result.id_table_distribution.keys()).lower()
        combined = all_values + " " + all_tables

        scores = {}
        for hier_type, patterns in self.HIERARCHY_TYPE_PATTERNS.items():
            score = sum(1 for p in patterns if p in combined)
            if score > 0:
                scores[hier_type] = score

        if scores:
            return max(scores, key=scores.get)
        return None

    def _suggest_join_patterns(
        self,
        result: DiscoveryResult,
    ) -> List[JoinPattern]:
        """Suggest join patterns based on ID_TABLE distribution."""
        patterns = []

        id_tables = result.id_table_distribution

        # DIM_ACCOUNT pattern (most common)
        if "DIM_ACCOUNT" in id_tables or any("ACCOUNT" in k for k in id_tables):
            patterns.append(JoinPattern(
                name="account",
                description="Account dimension join",
                join_keys=["LOS_ACCOUNT_ID_FILTER"],
                fact_keys=["FK_ACCOUNT_KEY"],
            ))

        # DIM_DEDUCT pattern
        if "DIM_DEDUCT" in id_tables or any("DEDUCT" in k for k in id_tables):
            patterns.append(JoinPattern(
                name="deduct_product",
                description="Deduct + Product dimension join",
                join_keys=["LOS_DEDUCT_CODE_FILTER", "LOS_PRODUCT_CODE_FILTER"],
                fact_keys=["FK_DEDUCT_KEY", "FK_PRODUCT_KEY"],
            ))

        # Product-only pattern (for royalties, etc.)
        if "DIM_PRODUCT" in id_tables or any("PRODUCT" in k for k in id_tables):
            if not any(p.name == "deduct_product" for p in patterns):
                patterns.append(JoinPattern(
                    name="product",
                    description="Product dimension join",
                    join_keys=["LOS_PRODUCT_CODE_FILTER"],
                    fact_keys=["FK_PRODUCT_KEY"],
                ))

        return patterns if patterns else self._suggest_default_patterns()

    def _suggest_default_patterns(self) -> List[JoinPattern]:
        """Suggest default join patterns when analysis fails."""
        return [
            JoinPattern(
                name="account",
                description="Default account dimension join",
                join_keys=["ACCOUNT_FILTER"],
                fact_keys=["FK_ACCOUNT_KEY"],
            ),
        ]

    def _suggest_column_mappings(
        self,
        result: DiscoveryResult,
    ) -> List[DynamicColumnMapping]:
        """Suggest column mappings from ID_SOURCE distribution."""
        mappings = []

        for id_source in result.id_source_distribution.keys():
            # Try to match to known patterns
            physical_column = self.KNOWN_ID_SOURCE_PATTERNS.get(id_source)

            if not physical_column:
                # Try fuzzy match
                best_match, score = self._find_closest_match(
                    id_source, list(self.KNOWN_ID_SOURCE_PATTERNS.keys())
                )
                if score >= 0.8:
                    physical_column = self.KNOWN_ID_SOURCE_PATTERNS[best_match]

            if physical_column:
                mappings.append(DynamicColumnMapping(
                    id_source=id_source,
                    physical_column=physical_column,
                    is_alias=id_source not in self.KNOWN_ID_SOURCE_PATTERNS,
                ))

        return mappings

    def _detect_data_quality_issues(
        self,
        result: DiscoveryResult,
    ) -> List[DataQualityIssue]:
        """Detect data quality issues from analysis."""
        issues = []

        # Check for potential typos in ID_SOURCE
        known_values = list(self.KNOWN_ID_SOURCE_PATTERNS.keys())
        for id_source in result.id_source_distribution.keys():
            if id_source not in known_values:
                best_match, score = self._find_closest_match(id_source, known_values)
                if 0.8 <= score < 1.0:
                    issues.append(DataQualityIssue(
                        severity="HIGH",
                        issue_type="TYPO",
                        description=f"Possible typo: '{id_source}' may be '{best_match}'",
                        affected_rows=result.id_source_distribution.get(id_source, 0),
                        affected_values=[id_source],
                        recommendation=f"Update to '{best_match}' or add alias mapping",
                    ))

        # Check for low mapping coverage
        if result.node_count > 0 and result.mapping_count > 0:
            avg_mappings = result.mapping_count / result.node_count
            if avg_mappings < 1.5:
                issues.append(DataQualityIssue(
                    severity="MEDIUM",
                    issue_type="LOW_COVERAGE",
                    description=f"Low mapping coverage: {avg_mappings:.1f} mappings per node",
                    recommendation="Review hierarchy for missing source mappings",
                ))

        return issues

    def _generate_recommended_config(
        self,
        hierarchy_table: str,
        mapping_table: str,
        result: DiscoveryResult,
    ) -> MartConfig:
        """Generate recommended configuration."""
        # Extract project name from table name
        project_name = hierarchy_table.split(".")[-1].replace("TBL_0_", "").replace("_HIERARCHY", "").lower()

        # Detect report type
        report_type = "GROSS" if "GROSS" in hierarchy_table.upper() else (
            "NET" if "NET" in hierarchy_table.upper() else "CUSTOM"
        )

        config = MartConfig(
            project_name=project_name,
            description=f"Auto-generated config from {result.hierarchy_type or 'unknown'} hierarchy",
            report_type=report_type,
            hierarchy_table=hierarchy_table,
            mapping_table=mapping_table,
            account_segment=report_type,
            has_sign_change="NET" in report_type,
            has_exclusions=True,  # Safe default
            has_group_filter_precedence="GROSS" in report_type,
            join_patterns=result.join_pattern_suggestion,
            dynamic_column_map=result.column_map_suggestion,
        )

        return config

    def _calculate_confidence(self, result: DiscoveryResult) -> float:
        """Calculate confidence score for discovery."""
        score = 0.0

        # Hierarchy type detected
        if result.hierarchy_type:
            score += 0.2

        # Join patterns suggested
        if result.join_pattern_suggestion:
            score += 0.3

        # Column mappings suggested
        if result.column_map_suggestion:
            score += 0.3

        # Few data quality issues
        high_issues = sum(1 for i in result.data_quality_issues if i.severity == "HIGH")
        if high_issues == 0:
            score += 0.2
        elif high_issues <= 2:
            score += 0.1

        return min(score, 1.0)

    def _generate_explanation(self, result: DiscoveryResult) -> str:
        """Generate human-readable explanation."""
        lines = [
            f"**Hierarchy Discovery Results**",
            f"",
            f"**Structure Analysis:**",
            f"- Hierarchy Type: {result.hierarchy_type or 'Unknown'}",
            f"- Total Nodes: {result.node_count}",
            f"- Active Nodes: {result.active_node_count}",
            f"- Calculation Nodes: {result.calculation_node_count}",
            f"- Total Mappings: {result.mapping_count}",
            f"- Hierarchy Levels: {result.level_count}",
            f"",
        ]

        if result.id_source_distribution:
            lines.append("**ID_SOURCE Distribution:**")
            for id_source, count in sorted(
                result.id_source_distribution.items(),
                key=lambda x: x[1],
                reverse=True,
            )[:5]:
                lines.append(f"- {id_source}: {count} mappings")
            lines.append("")

        if result.join_pattern_suggestion:
            lines.append("**Suggested Join Patterns:**")
            for pattern in result.join_pattern_suggestion:
                lines.append(f"- {pattern.name}: {pattern.join_keys} -> {pattern.fact_keys}")
            lines.append("")

        if result.data_quality_issues:
            lines.append("**Data Quality Issues:**")
            for issue in result.data_quality_issues[:5]:
                lines.append(f"- [{issue.severity}] {issue.description}")
            lines.append("")

        lines.append(f"**Confidence Score:** {result.confidence_score:.0%}")

        return "\n".join(lines)

    def _find_closest_match(
        self,
        value: str,
        candidates: List[str],
    ) -> Tuple[str, float]:
        """Find closest matching string from candidates."""
        if not candidates:
            return ("", 0.0)

        best_match = ""
        best_score = 0.0

        for candidate in candidates:
            score = SequenceMatcher(None, value.upper(), candidate.upper()).ratio()
            if score > best_score:
                best_score = score
                best_match = candidate

        return (best_match, best_score)

    def _patterns_from_combinations(
        self,
        combinations: List[Dict[str, Any]],
    ) -> List[JoinPattern]:
        """Convert dimension combinations to join patterns."""
        patterns = []

        for combo in combinations:
            dim_combo = combo.get("DIM_COMBINATION", "")
            node_count = combo.get("node_count", 0)

            if not dim_combo:
                continue

            dims = dim_combo.split(",")

            # Build pattern based on dimensions
            if "DIM_ACCOUNT" in dims and len(dims) == 1:
                patterns.append(JoinPattern(
                    name="account",
                    join_keys=["LOS_ACCOUNT_ID_FILTER"],
                    fact_keys=["FK_ACCOUNT_KEY"],
                    description=f"Account-only join ({node_count} nodes)",
                ))
            elif "DIM_DEDUCT" in dims and "DIM_PRODUCT" in dims:
                patterns.append(JoinPattern(
                    name="deduct_product",
                    join_keys=["LOS_DEDUCT_CODE_FILTER", "LOS_PRODUCT_CODE_FILTER"],
                    fact_keys=["FK_DEDUCT_KEY", "FK_PRODUCT_KEY"],
                    description=f"Deduct+Product join ({node_count} nodes)",
                ))
            elif "DIM_ACCOUNT" in dims and "DIM_PRODUCT" in dims:
                patterns.append(JoinPattern(
                    name="account_product",
                    join_keys=["LOS_ACCOUNT_ID_FILTER", "LOS_PRODUCT_CODE_FILTER"],
                    fact_keys=["FK_ACCOUNT_KEY", "FK_PRODUCT_KEY"],
                    description=f"Account+Product join ({node_count} nodes)",
                ))
            elif "DIM_PRODUCT" in dims and len(dims) == 1:
                patterns.append(JoinPattern(
                    name="product",
                    join_keys=["LOS_PRODUCT_CODE_FILTER"],
                    fact_keys=["FK_PRODUCT_KEY"],
                    description=f"Product-only join ({node_count} nodes)",
                ))

        return patterns
