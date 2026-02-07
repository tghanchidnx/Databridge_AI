"""
Hierarchy Data Quality Validator.

Phase 31: Validates hierarchy and mapping data for common issues:
- ID_SOURCE typos (e.g., BILLING_CATEGRY_CODE vs BILLING_CATEGORY_CODE)
- Duplicate hierarchy keys
- Orphan nodes (nodes with no mappings)
- FILTER_GROUP mismatches
- Missing formula references

Integrates with DataBridge Data Quality module for consistent validation.
"""

import logging
from typing import Any, Dict, List, Optional, Set
from dataclasses import dataclass, field
from enum import Enum

from ..diff.core import compute_similarity, find_close_matches
from .types import DataQualityIssue

logger = logging.getLogger(__name__)


class HierarchyIssueType(str, Enum):
    """Types of hierarchy data quality issues."""
    ID_SOURCE_TYPO = "ID_SOURCE_TYPO"
    DUPLICATE_KEY = "DUPLICATE_KEY"
    ORPHAN_NODE = "ORPHAN_NODE"
    ORPHAN_MAPPING = "ORPHAN_MAPPING"
    FILTER_GROUP_MISMATCH = "FILTER_GROUP_MISMATCH"
    MISSING_FORMULA_REF = "MISSING_FORMULA_REF"
    INVALID_PRECEDENCE = "INVALID_PRECEDENCE"
    DUPLICATE_FORMULA = "DUPLICATE_FORMULA"
    CALCULATION_WITHOUT_FORMULA = "CALCULATION_WITHOUT_FORMULA"


@dataclass
class HierarchyValidationResult:
    """Result of hierarchy validation."""
    hierarchy_table: str
    mapping_table: str
    is_valid: bool = True

    # Statistics
    hierarchy_count: int = 0
    mapping_count: int = 0
    active_count: int = 0
    calculation_count: int = 0

    # Issues
    issues: List[DataQualityIssue] = field(default_factory=list)

    # Warnings (not blocking)
    warnings: List[str] = field(default_factory=list)

    # ID_SOURCE analysis
    id_source_values: Set[str] = field(default_factory=set)
    typo_suggestions: Dict[str, str] = field(default_factory=dict)

    def add_issue(self, issue: DataQualityIssue) -> None:
        """Add an issue."""
        self.issues.append(issue)
        if issue.severity in ("HIGH", "CRITICAL"):
            self.is_valid = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "hierarchy_table": self.hierarchy_table,
            "mapping_table": self.mapping_table,
            "is_valid": self.is_valid,
            "hierarchy_count": self.hierarchy_count,
            "mapping_count": self.mapping_count,
            "active_count": self.active_count,
            "calculation_count": self.calculation_count,
            "issue_count": len(self.issues),
            "warning_count": len(self.warnings),
            "issues": [i.to_dict() for i in self.issues],
            "warnings": self.warnings[:10],
            "typo_suggestions": self.typo_suggestions,
        }


class HierarchyQualityValidator:
    """
    Validates hierarchy and mapping data for quality issues.

    Detects:
    1. ID_SOURCE typos using fuzzy matching
    2. Duplicate hierarchy keys
    3. Orphan nodes and mappings
    4. FILTER_GROUP mismatches
    5. Formula reference issues
    """

    # Known valid ID_SOURCE values (canonical forms)
    KNOWN_ID_SOURCES = {
        "BILLING_CATEGORY_CODE",
        "BILLING_CATEGORY_TYPE_CODE",
        "ACCOUNT_CODE",
        "MINOR_CODE",
        "DEDUCT_CODE",
        "PRODUCT_CODE",
        "ROYALTY_FILTER",
    }

    # Common typo patterns
    TYPO_CORRECTIONS = {
        "BILLING_CATEGRY_CODE": "BILLING_CATEGORY_CODE",  # Missing 'O'
        "BILLING_CATEGORY_TYPE": "BILLING_CATEGORY_TYPE_CODE",  # Missing '_CODE'
        "BILLINGCATEGORYCODE": "BILLING_CATEGORY_CODE",  # Missing underscores
        "BILLING_CAT_CODE": "BILLING_CATEGORY_CODE",  # Abbreviated
        "ACCOUNT": "ACCOUNT_CODE",  # Missing '_CODE'
        "DEDUCT": "DEDUCT_CODE",  # Missing '_CODE'
        "PRODUCT": "PRODUCT_CODE",  # Missing '_CODE'
    }

    def __init__(
        self,
        similarity_threshold: float = 0.85,
        known_id_sources: Optional[Set[str]] = None,
    ):
        """
        Initialize the validator.

        Args:
            similarity_threshold: Minimum similarity for typo detection
            known_id_sources: Additional known valid ID_SOURCE values
        """
        self.similarity_threshold = similarity_threshold
        self.known_id_sources = self.KNOWN_ID_SOURCES.copy()
        if known_id_sources:
            self.known_id_sources.update(known_id_sources)

    def validate_hierarchy_data(
        self,
        hierarchies: List[Dict[str, Any]],
        mappings: List[Dict[str, Any]],
        hierarchy_table: str = "HIERARCHY",
        mapping_table: str = "MAPPING",
    ) -> HierarchyValidationResult:
        """
        Validate hierarchy and mapping data.

        Args:
            hierarchies: List of hierarchy records
            mappings: List of mapping records
            hierarchy_table: Name of hierarchy table
            mapping_table: Name of mapping table

        Returns:
            HierarchyValidationResult with all detected issues
        """
        result = HierarchyValidationResult(
            hierarchy_table=hierarchy_table,
            mapping_table=mapping_table,
        )

        result.hierarchy_count = len(hierarchies)
        result.mapping_count = len(mappings)

        # Build indexes
        hierarchy_keys = set()
        active_keys = set()
        calculation_keys = set()
        formula_refs = set()

        for h in hierarchies:
            key = h.get("HIERARCHY_ID") or h.get("hierarchy_id")
            if key:
                hierarchy_keys.add(str(key))

            if h.get("ACTIVE_FLAG") or h.get("active_flag"):
                active_keys.add(str(key))

            is_calc = h.get("CALCULATION_FLAG") or h.get("calculation_flag")
            if is_calc:
                calculation_keys.add(str(key))

            # Track formula references
            param_ref = h.get("FORMULA_PARAM_REF") or h.get("formula_param_ref")
            if param_ref:
                formula_refs.add(str(param_ref))

        result.active_count = len(active_keys)
        result.calculation_count = len(calculation_keys)

        # 1. Validate ID_SOURCE values
        self._validate_id_sources(mappings, result)

        # 2. Check for duplicate hierarchy keys
        self._validate_duplicate_keys(hierarchies, result)

        # 3. Find orphan nodes (active, non-calculation, no mappings)
        self._validate_orphan_nodes(hierarchies, mappings, result)

        # 4. Find orphan mappings (mappings without hierarchy)
        self._validate_orphan_mappings(hierarchies, mappings, result)

        # 5. Validate FILTER_GROUP consistency
        self._validate_filter_groups(hierarchies, mappings, result)

        # 6. Validate formula references
        self._validate_formula_references(hierarchies, result)

        # 7. Check for duplicate formulas
        self._validate_duplicate_formulas(hierarchies, result)

        logger.info(
            f"Validated {result.hierarchy_count} hierarchies, "
            f"{result.mapping_count} mappings: {len(result.issues)} issues found"
        )

        return result

    def _validate_id_sources(
        self,
        mappings: List[Dict[str, Any]],
        result: HierarchyValidationResult,
    ) -> None:
        """Validate ID_SOURCE values for typos."""
        id_source_counts: Dict[str, int] = {}

        for m in mappings:
            id_source = m.get("ID_SOURCE") or m.get("id_source") or ""
            if id_source:
                id_source_counts[id_source] = id_source_counts.get(id_source, 0) + 1
                result.id_source_values.add(id_source)

        # Check each ID_SOURCE value
        for id_source, count in id_source_counts.items():
            # Check if it's a known valid value
            if id_source.upper() in self.known_id_sources:
                continue

            # Check if it's a known typo
            if id_source.upper() in self.TYPO_CORRECTIONS:
                correct = self.TYPO_CORRECTIONS[id_source.upper()]
                result.typo_suggestions[id_source] = correct
                result.add_issue(DataQualityIssue(
                    severity="HIGH",
                    issue_type=HierarchyIssueType.ID_SOURCE_TYPO.value,
                    description=f"ID_SOURCE typo detected: '{id_source}' should be '{correct}'",
                    affected_rows=count,
                    affected_values=[id_source],
                    recommendation=f"Update ID_SOURCE to '{correct}' or add alias mapping",
                ))
                continue

            # Use fuzzy matching to detect potential typos
            matches = find_close_matches(
                id_source.upper(),
                list(self.known_id_sources),
                n=1,
                cutoff=self.similarity_threshold,
            )

            if matches:
                best_match = matches[0].candidate
                similarity = matches[0].similarity
                result.typo_suggestions[id_source] = best_match
                result.add_issue(DataQualityIssue(
                    severity="MEDIUM" if similarity > 0.9 else "LOW",
                    issue_type=HierarchyIssueType.ID_SOURCE_TYPO.value,
                    description=f"Possible ID_SOURCE typo: '{id_source}' (similar to '{best_match}' at {similarity:.0%})",
                    affected_rows=count,
                    affected_values=[id_source],
                    recommendation=f"Verify if '{id_source}' should be '{best_match}'",
                ))

    def _validate_duplicate_keys(
        self,
        hierarchies: List[Dict[str, Any]],
        result: HierarchyValidationResult,
    ) -> None:
        """Check for duplicate hierarchy keys."""
        key_counts: Dict[str, int] = {}

        for h in hierarchies:
            key = str(h.get("HIERARCHY_ID") or h.get("hierarchy_id") or "")
            if key:
                key_counts[key] = key_counts.get(key, 0) + 1

        duplicates = [k for k, v in key_counts.items() if v > 1]

        if duplicates:
            result.add_issue(DataQualityIssue(
                severity="HIGH",
                issue_type=HierarchyIssueType.DUPLICATE_KEY.value,
                description=f"Duplicate hierarchy keys detected: {len(duplicates)} keys appear multiple times",
                affected_rows=sum(key_counts[k] for k in duplicates),
                affected_values=duplicates[:10],
                recommendation="Remove duplicate hierarchy entries",
            ))

    def _validate_orphan_nodes(
        self,
        hierarchies: List[Dict[str, Any]],
        mappings: List[Dict[str, Any]],
        result: HierarchyValidationResult,
    ) -> None:
        """Find nodes that should have mappings but don't."""
        # Get all keys with mappings
        mapped_keys = set()
        for m in mappings:
            fk = m.get("FK_REPORT_KEY") or m.get("fk_report_key") or m.get("HIERARCHY_ID") or m.get("hierarchy_id")
            if fk:
                mapped_keys.add(str(fk))

        # Find active, non-calculation nodes without mappings
        orphan_nodes = []
        for h in hierarchies:
            key = str(h.get("HIERARCHY_ID") or h.get("hierarchy_id") or "")
            is_active = h.get("ACTIVE_FLAG") or h.get("active_flag")
            is_calc = h.get("CALCULATION_FLAG") or h.get("calculation_flag")

            if is_active and not is_calc and key and key not in mapped_keys:
                name = h.get("HIERARCHY_NAME") or h.get("hierarchy_name") or key
                orphan_nodes.append(f"{key} ({name})")

        if orphan_nodes:
            result.add_issue(DataQualityIssue(
                severity="LOW",
                issue_type=HierarchyIssueType.ORPHAN_NODE.value,
                description=f"Active non-calculation nodes without mappings: {len(orphan_nodes)} nodes",
                affected_rows=len(orphan_nodes),
                affected_values=orphan_nodes[:10],
                recommendation="Add source mappings or mark as calculation nodes",
            ))

    def _validate_orphan_mappings(
        self,
        hierarchies: List[Dict[str, Any]],
        mappings: List[Dict[str, Any]],
        result: HierarchyValidationResult,
    ) -> None:
        """Find mappings that reference non-existent hierarchies."""
        hierarchy_keys = set()
        for h in hierarchies:
            key = str(h.get("HIERARCHY_ID") or h.get("hierarchy_id") or "")
            if key:
                hierarchy_keys.add(key)

        orphan_mappings = []
        for m in mappings:
            fk = str(m.get("FK_REPORT_KEY") or m.get("fk_report_key") or m.get("HIERARCHY_ID") or m.get("hierarchy_id") or "")
            if fk and fk not in hierarchy_keys:
                orphan_mappings.append(fk)

        if orphan_mappings:
            result.add_issue(DataQualityIssue(
                severity="HIGH",
                issue_type=HierarchyIssueType.ORPHAN_MAPPING.value,
                description=f"Mappings reference non-existent hierarchies: {len(orphan_mappings)} mappings",
                affected_rows=len(orphan_mappings),
                affected_values=list(set(orphan_mappings))[:10],
                recommendation="Remove orphan mappings or create missing hierarchies",
            ))

    def _validate_filter_groups(
        self,
        hierarchies: List[Dict[str, Any]],
        mappings: List[Dict[str, Any]],
        result: HierarchyValidationResult,
    ) -> None:
        """Validate FILTER_GROUP consistency between hierarchy and mapping."""
        # Collect FILTER_GROUP values from both tables
        hierarchy_fg2 = set()
        mapping_fg2 = set()

        for h in hierarchies:
            fg2 = h.get("FILTER_GROUP_2") or h.get("filter_group_2")
            if fg2:
                hierarchy_fg2.add(str(fg2))

        for m in mappings:
            fg2 = m.get("FILTER_GROUP_2") or m.get("filter_group_2")
            if fg2:
                mapping_fg2.add(str(fg2))

        # Find mismatches
        in_hier_not_map = hierarchy_fg2 - mapping_fg2
        in_map_not_hier = mapping_fg2 - hierarchy_fg2

        if in_map_not_hier:
            result.warnings.append(
                f"FILTER_GROUP_2 in mapping but not hierarchy: {len(in_map_not_hier)} values"
            )

        if in_hier_not_map:
            result.warnings.append(
                f"FILTER_GROUP_2 in hierarchy but not mapping: {len(in_hier_not_map)} values"
            )

    def _validate_formula_references(
        self,
        hierarchies: List[Dict[str, Any]],
        result: HierarchyValidationResult,
    ) -> None:
        """Validate formula parameter references exist."""
        # Collect all formula groups
        formula_groups = set()
        for h in hierarchies:
            group = h.get("FORMULA_GROUP") or h.get("formula_group")
            if group:
                formula_groups.add(str(group))

        # Check FORMULA_PARAM_REF and FORMULA_PARAM2_REF
        for h in hierarchies:
            is_calc = h.get("CALCULATION_FLAG") or h.get("calculation_flag")
            if not is_calc:
                continue

            key = str(h.get("HIERARCHY_ID") or h.get("hierarchy_id") or "")

            param_ref = h.get("FORMULA_PARAM_REF") or h.get("formula_param_ref")
            if param_ref and str(param_ref) not in formula_groups:
                result.add_issue(DataQualityIssue(
                    severity="MEDIUM",
                    issue_type=HierarchyIssueType.MISSING_FORMULA_REF.value,
                    description=f"FORMULA_PARAM_REF '{param_ref}' not found in FORMULA_GROUP values",
                    affected_rows=1,
                    affected_values=[key],
                    recommendation=f"Check if '{param_ref}' should reference an existing group",
                ))

            param2_ref = h.get("FORMULA_PARAM2_REF") or h.get("formula_param2_ref")
            if param2_ref and str(param2_ref) not in formula_groups:
                result.add_issue(DataQualityIssue(
                    severity="MEDIUM",
                    issue_type=HierarchyIssueType.MISSING_FORMULA_REF.value,
                    description=f"FORMULA_PARAM2_REF '{param2_ref}' not found in FORMULA_GROUP values",
                    affected_rows=1,
                    affected_values=[key],
                    recommendation=f"Check if '{param2_ref}' should reference an existing group",
                ))

            # Check calculation nodes without formula
            formula_logic = h.get("FORMULA_LOGIC") or h.get("formula_logic")
            if not formula_logic:
                result.add_issue(DataQualityIssue(
                    severity="MEDIUM",
                    issue_type=HierarchyIssueType.CALCULATION_WITHOUT_FORMULA.value,
                    description=f"Calculation node '{key}' has no FORMULA_LOGIC",
                    affected_rows=1,
                    affected_values=[key],
                    recommendation="Add FORMULA_LOGIC (SUM, SUBTRACT, etc.)",
                ))

    def _validate_duplicate_formulas(
        self,
        hierarchies: List[Dict[str, Any]],
        result: HierarchyValidationResult,
    ) -> None:
        """Check for duplicate formula definitions."""
        formula_signatures = {}

        for h in hierarchies:
            is_calc = h.get("CALCULATION_FLAG") or h.get("calculation_flag")
            if not is_calc:
                continue

            key = str(h.get("HIERARCHY_ID") or h.get("hierarchy_id") or "")
            precedence = h.get("FORMULA_PRECEDENCE") or h.get("formula_precedence") or 0
            logic = h.get("FORMULA_LOGIC") or h.get("formula_logic") or ""
            param_ref = h.get("FORMULA_PARAM_REF") or h.get("formula_param_ref") or ""
            param2_ref = h.get("FORMULA_PARAM2_REF") or h.get("formula_param2_ref") or ""

            # Create signature
            sig = f"{precedence}|{logic}|{param_ref}|{param2_ref}"

            if sig in formula_signatures:
                existing = formula_signatures[sig]
                result.add_issue(DataQualityIssue(
                    severity="MEDIUM",
                    issue_type=HierarchyIssueType.DUPLICATE_FORMULA.value,
                    description=f"Duplicate formula at precedence {precedence}: keys {existing} and {key}",
                    affected_rows=2,
                    affected_values=[existing, key],
                    recommendation="Review if both formula definitions are needed",
                ))
            else:
                formula_signatures[sig] = key


def validate_hierarchy_quality(
    hierarchies: List[Dict[str, Any]],
    mappings: List[Dict[str, Any]],
    hierarchy_table: str = "HIERARCHY",
    mapping_table: str = "MAPPING",
    similarity_threshold: float = 0.85,
) -> HierarchyValidationResult:
    """
    Convenience function to validate hierarchy data quality.

    Args:
        hierarchies: List of hierarchy records
        mappings: List of mapping records
        hierarchy_table: Name of hierarchy table
        mapping_table: Name of mapping table
        similarity_threshold: Minimum similarity for typo detection

    Returns:
        HierarchyValidationResult
    """
    validator = HierarchyQualityValidator(similarity_threshold=similarity_threshold)
    return validator.validate_hierarchy_data(
        hierarchies=hierarchies,
        mappings=mappings,
        hierarchy_table=hierarchy_table,
        mapping_table=mapping_table,
    )
