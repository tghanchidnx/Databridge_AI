"""
GROUP_FILTER_PRECEDENCE Multi-Round Filter Engine.

Phase 31: Implements the GROSS-only multi-round filtering pattern.

From docs/databridge_complete_analysis.md Section 10:
- GROUP_FILTER_PRECEDENCE column (values 1-3) drives multi-round filtering
- Precedence 1: Primary dimension join
- Precedence 2: Secondary filter dimension (applied after primary resolves)
- Precedence 3: Tertiary filter dimension

This is the architectural pattern that allows hierarchies to define
node membership through sequential dimension value combinations.

Example Distribution:
- Revenue, Volumes, Royalties: Precedence 1 only (single-round)
- Capital Spend, Operating Expense, Taxes: Precedence 1, 2 (two-round)
- Deducts: Precedence 1, 2, 3 (three-round filtering)
"""

import logging
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class FilterPrecedence(int, Enum):
    """Filter precedence levels."""
    PRIMARY = 1  # Primary dimension join
    SECONDARY = 2  # Secondary filter after primary
    TERTIARY = 3  # Tertiary filter after secondary


@dataclass
class FilterRound:
    """A single round of filtering."""
    precedence: int
    dimension_table: str
    filter_column: str
    join_column: str
    filter_values: List[str] = field(default_factory=list)
    is_optional: bool = False  # Some nodes skip higher precedences


@dataclass
class FilterPattern:
    """A complete multi-round filter pattern for a hierarchy category."""
    category: str  # e.g., "Deducts", "Operating Expense"
    rounds: List[FilterRound] = field(default_factory=list)
    max_precedence: int = 1

    @property
    def round_count(self) -> int:
        """Get number of filter rounds."""
        return len(self.rounds)

    def add_round(self, round: FilterRound) -> None:
        """Add a filter round."""
        self.rounds.append(round)
        self.max_precedence = max(self.max_precedence, round.precedence)


@dataclass
class MappingWithPrecedence:
    """A mapping row with GROUP_FILTER_PRECEDENCE."""
    hierarchy_key: str
    id_source: str
    filter_value: str
    precedence: int
    id_table: str
    filter_group_1: Optional[str] = None


class GroupFilterPrecedenceEngine:
    """
    Implements GROUP_FILTER_PRECEDENCE multi-round filtering logic.

    This engine:
    1. Analyzes mappings to detect filter patterns by category
    2. Generates multi-round SQL for VW_1/DT_2
    3. Ensures correct order of dimension joins
    4. Handles categories with different precedence depths
    """

    # Default precedence interpretation
    PRECEDENCE_DESCRIPTIONS = {
        1: "Primary dimension join - base filter criteria",
        2: "Secondary filter - applied after primary resolves",
        3: "Tertiary filter - applied after secondary resolves",
    }

    def __init__(self):
        """Initialize the filter engine."""
        self._patterns: Dict[str, FilterPattern] = {}
        self._mappings_by_key: Dict[str, List[MappingWithPrecedence]] = {}

    def analyze_mappings(
        self,
        mappings: List[Dict[str, Any]],
    ) -> Dict[str, FilterPattern]:
        """
        Analyze mappings to detect GROUP_FILTER_PRECEDENCE patterns.

        Args:
            mappings: List of mapping records with GROUP_FILTER_PRECEDENCE

        Returns:
            Dict of category -> FilterPattern
        """
        # Group by FILTER_GROUP_1 (category)
        by_category: Dict[str, List[Dict[str, Any]]] = {}

        for m in mappings:
            fg1 = m.get("FILTER_GROUP_1") or m.get("filter_group_1") or "Unknown"
            precedence = m.get("GROUP_FILTER_PRECEDENCE") or m.get("group_filter_precedence")

            if precedence is None:
                continue  # Skip mappings without precedence

            if fg1 not in by_category:
                by_category[fg1] = []
            by_category[fg1].append(m)

        # Analyze each category
        self._patterns = {}

        for category, category_mappings in by_category.items():
            pattern = self._analyze_category(category, category_mappings)
            self._patterns[category] = pattern

        logger.info(f"Analyzed {len(self._patterns)} filter patterns from {len(mappings)} mappings")
        return self._patterns

    def _analyze_category(
        self,
        category: str,
        mappings: List[Dict[str, Any]],
    ) -> FilterPattern:
        """Analyze a single category's filter pattern."""
        pattern = FilterPattern(category=category)

        # Group by precedence
        by_precedence: Dict[int, List[Dict[str, Any]]] = {}
        for m in mappings:
            prec = int(m.get("GROUP_FILTER_PRECEDENCE") or m.get("group_filter_precedence") or 1)
            if prec not in by_precedence:
                by_precedence[prec] = []
            by_precedence[prec].append(m)

        # Analyze each precedence level
        for prec in sorted(by_precedence.keys()):
            prec_mappings = by_precedence[prec]

            # Determine dimension table and columns
            id_tables: Dict[str, int] = {}
            for m in prec_mappings:
                id_table = m.get("ID_TABLE") or m.get("id_table") or "UNKNOWN"
                id_tables[id_table] = id_tables.get(id_table, 0) + 1

            # Use most common table
            primary_table = max(id_tables.items(), key=lambda x: x[1])[0]

            # Collect filter values
            filter_values = []
            for m in prec_mappings:
                val = m.get("ID") or m.get("id") or m.get("SOURCE_UID") or m.get("source_uid")
                if val:
                    filter_values.append(str(val))

            round = FilterRound(
                precedence=prec,
                dimension_table=primary_table,
                filter_column=self._get_filter_column(primary_table),
                join_column=self._get_join_column(primary_table),
                filter_values=filter_values,
            )

            pattern.add_round(round)

        return pattern

    def _get_filter_column(self, id_table: str) -> str:
        """Get the filter column for a dimension table."""
        table_upper = id_table.upper()
        if "ACCOUNT" in table_upper:
            return "LOS_ACCOUNT_ID_FILTER"
        elif "DEDUCT" in table_upper:
            return "LOS_DEDUCT_CODE_FILTER"
        elif "PRODUCT" in table_upper:
            return "LOS_PRODUCT_CODE_FILTER"
        else:
            return f"LOS_{table_upper.split('_')[-1]}_FILTER"

    def _get_join_column(self, id_table: str) -> str:
        """Get the fact table join column for a dimension table."""
        table_upper = id_table.upper()
        if "ACCOUNT" in table_upper:
            return "FK_ACCOUNT_KEY"
        elif "DEDUCT" in table_upper:
            return "FK_DEDUCT_KEY"
        elif "PRODUCT" in table_upper:
            return "FK_PRODUCT_KEY"
        else:
            return f"FK_{table_upper.split('_')[-1]}_KEY"

    def get_pattern(self, category: str) -> Optional[FilterPattern]:
        """Get the filter pattern for a category."""
        return self._patterns.get(category)

    def generate_dt2_ctes(
        self,
        patterns: Optional[Dict[str, FilterPattern]] = None,
    ) -> str:
        """
        Generate DT_2 CTEs for multi-round filtering.

        Args:
            patterns: Optional patterns to use (defaults to analyzed)

        Returns:
            SQL CTE statements for multi-round filtering
        """
        patterns = patterns or self._patterns

        if not patterns:
            return "-- No GROUP_FILTER_PRECEDENCE patterns detected"

        ctes = []

        # Determine max precedence across all patterns
        max_prec = max(p.max_precedence for p in patterns.values())

        for prec in range(1, max_prec + 1):
            source = "VW1" if prec == 1 else f"PRECEDENCE_{prec - 1}_RESOLVED"

            cte = f"""-- Precedence {prec}: {self.PRECEDENCE_DESCRIPTIONS.get(prec, 'Filter round')}
PRECEDENCE_{prec}_RESOLVED AS (
    SELECT
        SRC.*,
        -- Resolve dimension values at this precedence level
        CASE WHEN SRC.GROUP_FILTER_PRECEDENCE = {prec} THEN SRC.RESOLVED_VALUE ELSE NULL END AS PREC_{prec}_VALUE
    FROM {source} SRC
    WHERE SRC.GROUP_FILTER_PRECEDENCE >= {prec}
       OR SRC.GROUP_FILTER_PRECEDENCE IS NULL
)"""
            ctes.append(cte)

        return ",\n\n".join(ctes)

    def generate_union_branches(
        self,
        patterns: Optional[Dict[str, FilterPattern]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Generate UNION ALL branch definitions based on patterns.

        Args:
            patterns: Optional patterns to use

        Returns:
            List of branch definitions for DT_3A
        """
        patterns = patterns or self._patterns
        branches = []

        # Group patterns by their join structure
        join_signatures: Dict[str, List[str]] = {}

        for category, pattern in patterns.items():
            # Create signature from dimension tables used
            sig = "|".join(r.dimension_table for r in pattern.rounds)
            if sig not in join_signatures:
                join_signatures[sig] = []
            join_signatures[sig].append(category)

        # Create a branch for each unique join structure
        for sig, categories in join_signatures.items():
            tables = sig.split("|")

            # Build join keys
            join_keys = []
            fact_keys = []

            for table in tables:
                filter_col = self._get_filter_column(table)
                join_col = self._get_join_column(table)
                if filter_col not in join_keys:
                    join_keys.append(filter_col)
                    fact_keys.append(join_col)

            branches.append({
                "name": "_".join(t.split("_")[-1].lower() for t in tables),
                "join_keys": join_keys,
                "fact_keys": fact_keys,
                "categories": categories,
                "description": f"Multi-round join for: {', '.join(categories)}",
            })

        return branches

    def get_pattern_summary(self) -> Dict[str, Any]:
        """
        Get a summary of all detected patterns.

        Returns:
            Summary dictionary
        """
        summary = {
            "pattern_count": len(self._patterns),
            "patterns": {},
        }

        for category, pattern in self._patterns.items():
            summary["patterns"][category] = {
                "round_count": pattern.round_count,
                "max_precedence": pattern.max_precedence,
                "rounds": [
                    {
                        "precedence": r.precedence,
                        "dimension_table": r.dimension_table,
                        "filter_column": r.filter_column,
                        "value_count": len(r.filter_values),
                    }
                    for r in pattern.rounds
                ],
            }

        # Summarize by precedence distribution
        prec_dist = {1: 0, 2: 0, 3: 0}
        for pattern in self._patterns.values():
            prec_dist[pattern.max_precedence] = prec_dist.get(pattern.max_precedence, 0) + 1

        summary["precedence_distribution"] = {
            "single_round": prec_dist.get(1, 0),
            "two_round": prec_dist.get(2, 0),
            "three_round": prec_dist.get(3, 0),
        }

        return summary


def analyze_group_filter_precedence(
    mappings: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Convenience function to analyze GROUP_FILTER_PRECEDENCE patterns.

    Args:
        mappings: List of mapping records

    Returns:
        Analysis result with patterns and SQL snippets
    """
    engine = GroupFilterPrecedenceEngine()
    patterns = engine.analyze_mappings(mappings)

    return {
        "patterns": {cat: {
            "round_count": p.round_count,
            "max_precedence": p.max_precedence,
        } for cat, p in patterns.items()},
        "dt2_ctes": engine.generate_dt2_ctes(patterns),
        "union_branches": engine.generate_union_branches(patterns),
        "summary": engine.get_pattern_summary(),
    }
