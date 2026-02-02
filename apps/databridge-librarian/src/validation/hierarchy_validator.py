"""
Hierarchy Validation for DataBridge AI Librarian.

Validates hierarchy structures for:
- Orphaned hierarchies (invalid parent references)
- Circular dependencies
- Duplicate hierarchy IDs
- Level consistency
- Formula references
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, List, Dict, Any, Set
from collections import defaultdict


class IssueSeverity(str, Enum):
    """Severity levels for validation issues."""

    ERROR = "error"  # Must be fixed before deployment
    WARNING = "warning"  # Should be reviewed
    INFO = "info"  # Informational only


class IssueType(str, Enum):
    """Types of validation issues."""

    ORPHANED_HIERARCHY = "orphaned_hierarchy"
    CIRCULAR_DEPENDENCY = "circular_dependency"
    DUPLICATE_ID = "duplicate_id"
    INVALID_PARENT = "invalid_parent"
    MISSING_MAPPING = "missing_mapping"
    INVALID_FORMULA = "invalid_formula"
    INCONSISTENT_LEVELS = "inconsistent_levels"
    DUPLICATE_NAME_SIBLING = "duplicate_name_sibling"
    EMPTY_PROJECT = "empty_project"
    MISSING_ROOT = "missing_root"


@dataclass
class ValidationIssue:
    """Represents a single validation issue."""

    issue_type: IssueType
    severity: IssueSeverity
    message: str
    hierarchy_id: Optional[str] = None
    project_id: Optional[str] = None
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "issue_type": self.issue_type.value,
            "severity": self.severity.value,
            "message": self.message,
            "hierarchy_id": self.hierarchy_id,
            "project_id": self.project_id,
            "details": self.details,
        }


@dataclass
class ValidationResult:
    """Result of a validation run."""

    project_id: str
    is_valid: bool
    issues: List[ValidationIssue] = field(default_factory=list)
    checked_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    hierarchy_count: int = 0
    mapping_count: int = 0
    formula_count: int = 0

    @property
    def error_count(self) -> int:
        """Count of ERROR severity issues."""
        return sum(1 for i in self.issues if i.severity == IssueSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        """Count of WARNING severity issues."""
        return sum(1 for i in self.issues if i.severity == IssueSeverity.WARNING)

    @property
    def info_count(self) -> int:
        """Count of INFO severity issues."""
        return sum(1 for i in self.issues if i.severity == IssueSeverity.INFO)

    def add_issue(self, issue: ValidationIssue) -> None:
        """Add an issue to the result."""
        self.issues.append(issue)
        if issue.severity == IssueSeverity.ERROR:
            self.is_valid = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "project_id": self.project_id,
            "is_valid": self.is_valid,
            "checked_at": self.checked_at.isoformat(),
            "hierarchy_count": self.hierarchy_count,
            "mapping_count": self.mapping_count,
            "formula_count": self.formula_count,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "info_count": self.info_count,
            "issues": [i.to_dict() for i in self.issues],
        }


class HierarchyValidator:
    """
    Validates hierarchy project structures for consistency.

    Checks for:
    - Orphaned hierarchies (parent doesn't exist)
    - Circular dependencies in parent-child relationships
    - Duplicate hierarchy IDs
    - Level value consistency
    - Leaf nodes without mappings
    - Invalid formula references
    """

    def __init__(self):
        """Initialize the validator."""
        self._cache: Dict[str, ValidationResult] = {}

    def validate_project(
        self,
        project_id: str,
        hierarchies: List[Dict[str, Any]],
        mappings: Optional[List[Dict[str, Any]]] = None,
        formula_groups: Optional[List[Dict[str, Any]]] = None,
    ) -> ValidationResult:
        """
        Validate a complete hierarchy project.

        Args:
            project_id: The project ID being validated.
            hierarchies: List of hierarchy dictionaries.
            mappings: Optional list of source mappings.
            formula_groups: Optional list of formula groups.

        Returns:
            ValidationResult with all found issues.
        """
        mappings = mappings or []
        formula_groups = formula_groups or []

        result = ValidationResult(
            project_id=project_id,
            is_valid=True,
            hierarchy_count=len(hierarchies),
            mapping_count=len(mappings),
            formula_count=len(formula_groups),
        )

        if not hierarchies:
            result.add_issue(ValidationIssue(
                issue_type=IssueType.EMPTY_PROJECT,
                severity=IssueSeverity.WARNING,
                message="Project has no hierarchies",
                project_id=project_id,
            ))
            return result

        # Build lookup structures
        hierarchy_ids = set()
        hierarchy_map = {}
        parent_map = defaultdict(list)

        for h in hierarchies:
            h_id = h.get("hierarchy_id")
            parent_id = h.get("parent_id")

            hierarchy_ids.add(h_id)
            hierarchy_map[h_id] = h

            if parent_id:
                parent_map[parent_id].append(h_id)

        # Run all checks
        self._check_duplicate_ids(result, hierarchies)
        self._check_orphaned_hierarchies(result, hierarchies, hierarchy_ids)
        self._check_circular_dependencies(result, hierarchies, parent_map)
        self._check_root_nodes(result, hierarchies)
        self._check_level_consistency(result, hierarchies, hierarchy_map)
        self._check_sibling_duplicates(result, hierarchies, parent_map)
        self._check_leaf_mappings(result, hierarchies, mappings, parent_map)
        self._check_formula_references(result, formula_groups, hierarchy_ids)

        # Cache the result
        self._cache[project_id] = result

        return result

    def _check_duplicate_ids(
        self,
        result: ValidationResult,
        hierarchies: List[Dict[str, Any]],
    ) -> None:
        """Check for duplicate hierarchy IDs."""
        seen_ids: Dict[str, int] = {}

        for h in hierarchies:
            h_id = h.get("hierarchy_id")
            if not h_id:
                continue

            if h_id in seen_ids:
                result.add_issue(ValidationIssue(
                    issue_type=IssueType.DUPLICATE_ID,
                    severity=IssueSeverity.ERROR,
                    message=f"Duplicate hierarchy ID: {h_id}",
                    hierarchy_id=h_id,
                    project_id=result.project_id,
                    details={"occurrences": seen_ids[h_id] + 1},
                ))
            seen_ids[h_id] = seen_ids.get(h_id, 0) + 1

    def _check_orphaned_hierarchies(
        self,
        result: ValidationResult,
        hierarchies: List[Dict[str, Any]],
        hierarchy_ids: Set[str],
    ) -> None:
        """Check for hierarchies with invalid parent references."""
        for h in hierarchies:
            parent_id = h.get("parent_id")
            if parent_id and parent_id not in hierarchy_ids:
                result.add_issue(ValidationIssue(
                    issue_type=IssueType.ORPHANED_HIERARCHY,
                    severity=IssueSeverity.ERROR,
                    message=f"Hierarchy '{h.get('hierarchy_id')}' references non-existent parent '{parent_id}'",
                    hierarchy_id=h.get("hierarchy_id"),
                    project_id=result.project_id,
                    details={"missing_parent": parent_id},
                ))

    def _check_circular_dependencies(
        self,
        result: ValidationResult,
        hierarchies: List[Dict[str, Any]],
        parent_map: Dict[str, List[str]],
    ) -> None:
        """Check for circular parent-child relationships."""
        # Build parent lookup
        parent_lookup = {h.get("hierarchy_id"): h.get("parent_id") for h in hierarchies}

        # Check each hierarchy for cycles
        for h in hierarchies:
            h_id = h.get("hierarchy_id")
            visited = set()
            current = h_id

            while current:
                if current in visited:
                    # Found a cycle
                    cycle_path = self._build_cycle_path(h_id, parent_lookup)
                    result.add_issue(ValidationIssue(
                        issue_type=IssueType.CIRCULAR_DEPENDENCY,
                        severity=IssueSeverity.ERROR,
                        message=f"Circular dependency detected starting from '{h_id}'",
                        hierarchy_id=h_id,
                        project_id=result.project_id,
                        details={"cycle_path": cycle_path},
                    ))
                    break

                visited.add(current)
                current = parent_lookup.get(current)

    def _build_cycle_path(
        self,
        start_id: str,
        parent_lookup: Dict[str, Optional[str]],
    ) -> List[str]:
        """Build the path of a circular dependency."""
        path = [start_id]
        current = parent_lookup.get(start_id)

        while current and current != start_id and len(path) < 50:
            path.append(current)
            current = parent_lookup.get(current)

        if current == start_id:
            path.append(start_id)

        return path

    def _check_root_nodes(
        self,
        result: ValidationResult,
        hierarchies: List[Dict[str, Any]],
    ) -> None:
        """Check for missing root nodes."""
        root_nodes = [h for h in hierarchies if not h.get("parent_id")]

        if not root_nodes:
            result.add_issue(ValidationIssue(
                issue_type=IssueType.MISSING_ROOT,
                severity=IssueSeverity.ERROR,
                message="Project has no root hierarchies (all have parents)",
                project_id=result.project_id,
            ))

    def _check_level_consistency(
        self,
        result: ValidationResult,
        hierarchies: List[Dict[str, Any]],
        hierarchy_map: Dict[str, Dict[str, Any]],
    ) -> None:
        """Check for inconsistent level values."""
        for h in hierarchies:
            h_id = h.get("hierarchy_id")
            parent_id = h.get("parent_id")

            if not parent_id:
                continue

            parent = hierarchy_map.get(parent_id)
            if not parent:
                continue

            # Check level progression
            child_depth = self._get_hierarchy_depth(h)
            parent_depth = self._get_hierarchy_depth(parent)

            if child_depth <= parent_depth:
                result.add_issue(ValidationIssue(
                    issue_type=IssueType.INCONSISTENT_LEVELS,
                    severity=IssueSeverity.WARNING,
                    message=f"Hierarchy '{h_id}' depth ({child_depth}) should be greater than parent '{parent_id}' ({parent_depth})",
                    hierarchy_id=h_id,
                    project_id=result.project_id,
                    details={"child_depth": child_depth, "parent_depth": parent_depth},
                ))

    def _get_hierarchy_depth(self, hierarchy: Dict[str, Any]) -> int:
        """Get the depth (number of filled levels) of a hierarchy."""
        depth = 0
        for i in range(1, 16):
            if hierarchy.get(f"level_{i}"):
                depth = i
        return depth

    def _check_sibling_duplicates(
        self,
        result: ValidationResult,
        hierarchies: List[Dict[str, Any]],
        parent_map: Dict[str, List[str]],
    ) -> None:
        """Check for duplicate names among siblings."""
        # Group by parent
        parent_children: Dict[Optional[str], List[Dict[str, Any]]] = defaultdict(list)

        for h in hierarchies:
            parent_id = h.get("parent_id")
            parent_children[parent_id].append(h)

        # Check each group for duplicate names
        for parent_id, children in parent_children.items():
            names_seen: Dict[str, str] = {}

            for child in children:
                name = child.get("hierarchy_name", "").lower()
                h_id = child.get("hierarchy_id")

                if name in names_seen:
                    result.add_issue(ValidationIssue(
                        issue_type=IssueType.DUPLICATE_NAME_SIBLING,
                        severity=IssueSeverity.WARNING,
                        message=f"Sibling hierarchies have same name: '{child.get('hierarchy_name')}'",
                        hierarchy_id=h_id,
                        project_id=result.project_id,
                        details={
                            "duplicate_of": names_seen[name],
                            "parent_id": parent_id,
                        },
                    ))
                else:
                    names_seen[name] = h_id

    def _check_leaf_mappings(
        self,
        result: ValidationResult,
        hierarchies: List[Dict[str, Any]],
        mappings: List[Dict[str, Any]],
        parent_map: Dict[str, List[str]],
    ) -> None:
        """Check that leaf nodes have source mappings."""
        # Build set of hierarchy IDs with mappings
        mapped_ids = {m.get("hierarchy_id") for m in mappings}

        # Find leaf nodes (hierarchies that are not parents)
        all_parents = set(parent_map.keys())

        for h in hierarchies:
            h_id = h.get("hierarchy_id")
            is_leaf = h.get("is_leaf_node", False)

            # If marked as leaf or has no children
            if is_leaf or (h_id and h_id not in all_parents):
                # Check if it has calculation_flag
                if h.get("calculation_flag"):
                    continue  # Calculated nodes don't need mappings

                # Check if it has mappings
                if h_id not in mapped_ids:
                    result.add_issue(ValidationIssue(
                        issue_type=IssueType.MISSING_MAPPING,
                        severity=IssueSeverity.WARNING,
                        message=f"Leaf hierarchy '{h_id}' has no source mappings",
                        hierarchy_id=h_id,
                        project_id=result.project_id,
                    ))

    def _check_formula_references(
        self,
        result: ValidationResult,
        formula_groups: List[Dict[str, Any]],
        hierarchy_ids: Set[str],
    ) -> None:
        """Check that formula groups reference valid hierarchies."""
        for formula in formula_groups:
            main_id = formula.get("main_hierarchy_id")

            if main_id and main_id not in hierarchy_ids:
                result.add_issue(ValidationIssue(
                    issue_type=IssueType.INVALID_FORMULA,
                    severity=IssueSeverity.ERROR,
                    message=f"Formula group references non-existent hierarchy '{main_id}'",
                    hierarchy_id=main_id,
                    project_id=result.project_id,
                    details={"formula_group": formula.get("group_name")},
                ))

            # Check rules
            for rule in formula.get("rules", []):
                source_id = rule.get("source_hierarchy_id")
                if source_id and source_id not in hierarchy_ids:
                    result.add_issue(ValidationIssue(
                        issue_type=IssueType.INVALID_FORMULA,
                        severity=IssueSeverity.ERROR,
                        message=f"Formula rule references non-existent hierarchy '{source_id}'",
                        hierarchy_id=source_id,
                        project_id=result.project_id,
                        details={
                            "formula_group": formula.get("group_name"),
                            "operation": rule.get("operation"),
                        },
                    ))

    def get_cached_result(self, project_id: str) -> Optional[ValidationResult]:
        """Get a cached validation result if available."""
        return self._cache.get(project_id)

    def clear_cache(self, project_id: Optional[str] = None) -> None:
        """Clear the validation cache."""
        if project_id:
            self._cache.pop(project_id, None)
        else:
            self._cache.clear()


# Singleton instance
_validator_instance: Optional[HierarchyValidator] = None


def get_hierarchy_validator() -> HierarchyValidator:
    """Get the singleton HierarchyValidator instance."""
    global _validator_instance
    if _validator_instance is None:
        _validator_instance = HierarchyValidator()
    return _validator_instance


def reset_hierarchy_validator() -> None:
    """Reset the singleton HierarchyValidator instance."""
    global _validator_instance
    _validator_instance = None
