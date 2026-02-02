"""
Unit tests for hierarchy validator.

Tests validation of hierarchy structures for consistency issues.
"""

import pytest

from src.validation.hierarchy_validator import (
    HierarchyValidator,
    ValidationResult,
    ValidationIssue,
    IssueSeverity,
    IssueType,
    get_hierarchy_validator,
    reset_hierarchy_validator,
)


class TestValidationResult:
    """Tests for ValidationResult dataclass."""

    def test_basic_creation(self):
        """Test creating a basic validation result."""
        result = ValidationResult(project_id="test-project", is_valid=True)

        assert result.project_id == "test-project"
        assert result.is_valid is True
        assert result.error_count == 0
        assert result.warning_count == 0

    def test_add_error_invalidates(self):
        """Test that adding an error makes result invalid."""
        result = ValidationResult(project_id="test", is_valid=True)

        result.add_issue(ValidationIssue(
            issue_type=IssueType.ORPHANED_HIERARCHY,
            severity=IssueSeverity.ERROR,
            message="Test error",
        ))

        assert result.is_valid is False
        assert result.error_count == 1

    def test_warning_doesnt_invalidate(self):
        """Test that warnings don't invalidate the result."""
        result = ValidationResult(project_id="test", is_valid=True)

        result.add_issue(ValidationIssue(
            issue_type=IssueType.MISSING_MAPPING,
            severity=IssueSeverity.WARNING,
            message="Test warning",
        ))

        assert result.is_valid is True
        assert result.warning_count == 1

    def test_to_dict(self):
        """Test serialization to dictionary."""
        result = ValidationResult(
            project_id="test",
            is_valid=True,
            hierarchy_count=10,
        )

        data = result.to_dict()

        assert data["project_id"] == "test"
        assert data["is_valid"] is True
        assert data["hierarchy_count"] == 10
        assert "checked_at" in data


class TestValidationIssue:
    """Tests for ValidationIssue dataclass."""

    def test_basic_creation(self):
        """Test creating a basic issue."""
        issue = ValidationIssue(
            issue_type=IssueType.ORPHANED_HIERARCHY,
            severity=IssueSeverity.ERROR,
            message="Test message",
            hierarchy_id="test-hier",
        )

        assert issue.issue_type == IssueType.ORPHANED_HIERARCHY
        assert issue.severity == IssueSeverity.ERROR
        assert issue.hierarchy_id == "test-hier"

    def test_to_dict(self):
        """Test serialization."""
        issue = ValidationIssue(
            issue_type=IssueType.CIRCULAR_DEPENDENCY,
            severity=IssueSeverity.ERROR,
            message="Cycle detected",
            details={"cycle": ["a", "b", "a"]},
        )

        data = issue.to_dict()

        assert data["issue_type"] == "circular_dependency"
        assert data["severity"] == "error"
        assert data["details"]["cycle"] == ["a", "b", "a"]


class TestHierarchyValidator:
    """Tests for HierarchyValidator class."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Set up test fixtures."""
        reset_hierarchy_validator()
        self.validator = HierarchyValidator()

    def test_empty_project_warning(self):
        """Test that empty projects generate a warning."""
        result = self.validator.validate_project(
            project_id="empty",
            hierarchies=[],
        )

        assert result.is_valid is True  # Warning only
        assert result.warning_count == 1
        assert any(i.issue_type == IssueType.EMPTY_PROJECT for i in result.issues)

    def test_valid_simple_hierarchy(self):
        """Test validating a simple valid hierarchy."""
        hierarchies = [
            {"hierarchy_id": "ROOT", "hierarchy_name": "Root", "parent_id": None, "level_1": "Root"},
            {"hierarchy_id": "CHILD_1", "hierarchy_name": "Child 1", "parent_id": "ROOT", "level_1": "Root", "level_2": "Child 1"},
        ]

        result = self.validator.validate_project(
            project_id="valid",
            hierarchies=hierarchies,
        )

        # Should only have warnings about missing mappings
        assert all(i.severity != IssueSeverity.ERROR for i in result.issues)

    def test_orphaned_hierarchy_detection(self):
        """Test detection of orphaned hierarchies."""
        hierarchies = [
            {"hierarchy_id": "ROOT", "hierarchy_name": "Root", "parent_id": None},
            {"hierarchy_id": "ORPHAN", "hierarchy_name": "Orphan", "parent_id": "NONEXISTENT"},
        ]

        result = self.validator.validate_project(
            project_id="orphan-test",
            hierarchies=hierarchies,
        )

        assert result.is_valid is False
        orphan_issues = [i for i in result.issues if i.issue_type == IssueType.ORPHANED_HIERARCHY]
        assert len(orphan_issues) == 1
        assert orphan_issues[0].hierarchy_id == "ORPHAN"

    def test_duplicate_id_detection(self):
        """Test detection of duplicate hierarchy IDs."""
        hierarchies = [
            {"hierarchy_id": "DUP", "hierarchy_name": "First", "parent_id": None},
            {"hierarchy_id": "DUP", "hierarchy_name": "Duplicate", "parent_id": None},
        ]

        result = self.validator.validate_project(
            project_id="dup-test",
            hierarchies=hierarchies,
        )

        assert result.is_valid is False
        dup_issues = [i for i in result.issues if i.issue_type == IssueType.DUPLICATE_ID]
        assert len(dup_issues) == 1

    def test_circular_dependency_detection(self):
        """Test detection of circular dependencies."""
        hierarchies = [
            {"hierarchy_id": "A", "hierarchy_name": "A", "parent_id": "C"},
            {"hierarchy_id": "B", "hierarchy_name": "B", "parent_id": "A"},
            {"hierarchy_id": "C", "hierarchy_name": "C", "parent_id": "B"},
        ]

        result = self.validator.validate_project(
            project_id="cycle-test",
            hierarchies=hierarchies,
        )

        assert result.is_valid is False
        cycle_issues = [i for i in result.issues if i.issue_type == IssueType.CIRCULAR_DEPENDENCY]
        assert len(cycle_issues) >= 1

    def test_missing_root_detection(self):
        """Test detection of missing root nodes."""
        hierarchies = [
            {"hierarchy_id": "A", "hierarchy_name": "A", "parent_id": "B"},
            {"hierarchy_id": "B", "hierarchy_name": "B", "parent_id": "A"},
        ]

        result = self.validator.validate_project(
            project_id="no-root",
            hierarchies=hierarchies,
        )

        assert result.is_valid is False
        root_issues = [i for i in result.issues if i.issue_type == IssueType.MISSING_ROOT]
        assert len(root_issues) == 1

    def test_sibling_duplicate_names(self):
        """Test detection of duplicate names among siblings."""
        hierarchies = [
            {"hierarchy_id": "ROOT", "hierarchy_name": "Root", "parent_id": None},
            {"hierarchy_id": "CHILD_1", "hierarchy_name": "Revenue", "parent_id": "ROOT"},
            {"hierarchy_id": "CHILD_2", "hierarchy_name": "Revenue", "parent_id": "ROOT"},
        ]

        result = self.validator.validate_project(
            project_id="sibling-dup",
            hierarchies=hierarchies,
        )

        # Should have warning
        dup_issues = [i for i in result.issues if i.issue_type == IssueType.DUPLICATE_NAME_SIBLING]
        assert len(dup_issues) == 1

    def test_leaf_without_mapping_warning(self):
        """Test warning for leaf nodes without mappings."""
        hierarchies = [
            {"hierarchy_id": "ROOT", "hierarchy_name": "Root", "parent_id": None},
            {"hierarchy_id": "LEAF", "hierarchy_name": "Leaf", "parent_id": "ROOT", "is_leaf_node": True},
        ]

        result = self.validator.validate_project(
            project_id="leaf-test",
            hierarchies=hierarchies,
            mappings=[],
        )

        # Should have warning about missing mapping
        mapping_issues = [i for i in result.issues if i.issue_type == IssueType.MISSING_MAPPING]
        assert len(mapping_issues) >= 1

    def test_calculated_node_no_mapping_required(self):
        """Test that calculated nodes don't need mappings."""
        hierarchies = [
            {"hierarchy_id": "ROOT", "hierarchy_name": "Root", "parent_id": None},
            {"hierarchy_id": "CALC", "hierarchy_name": "Calculated", "parent_id": "ROOT", "calculation_flag": True},
        ]

        result = self.validator.validate_project(
            project_id="calc-test",
            hierarchies=hierarchies,
            mappings=[],
        )

        # Should not have mapping warning for calculated node
        mapping_issues = [
            i for i in result.issues
            if i.issue_type == IssueType.MISSING_MAPPING and i.hierarchy_id == "CALC"
        ]
        assert len(mapping_issues) == 0

    def test_invalid_formula_reference(self):
        """Test detection of invalid formula references."""
        hierarchies = [
            {"hierarchy_id": "ROOT", "hierarchy_name": "Root", "parent_id": None},
        ]

        formula_groups = [
            {
                "group_name": "Test Formula",
                "main_hierarchy_id": "NONEXISTENT",
                "rules": [],
            },
        ]

        result = self.validator.validate_project(
            project_id="formula-test",
            hierarchies=hierarchies,
            formula_groups=formula_groups,
        )

        assert result.is_valid is False
        formula_issues = [i for i in result.issues if i.issue_type == IssueType.INVALID_FORMULA]
        assert len(formula_issues) == 1

    def test_formula_rule_invalid_reference(self):
        """Test detection of invalid formula rule references."""
        hierarchies = [
            {"hierarchy_id": "ROOT", "hierarchy_name": "Root", "parent_id": None},
            {"hierarchy_id": "RESULT", "hierarchy_name": "Result", "parent_id": "ROOT"},
        ]

        formula_groups = [
            {
                "group_name": "Test Formula",
                "main_hierarchy_id": "RESULT",
                "rules": [
                    {"operation": "SUM", "source_hierarchy_id": "MISSING"},
                ],
            },
        ]

        result = self.validator.validate_project(
            project_id="rule-test",
            hierarchies=hierarchies,
            formula_groups=formula_groups,
        )

        assert result.is_valid is False
        formula_issues = [i for i in result.issues if i.issue_type == IssueType.INVALID_FORMULA]
        assert len(formula_issues) == 1

    def test_caching(self):
        """Test that validation results are cached."""
        hierarchies = [
            {"hierarchy_id": "ROOT", "hierarchy_name": "Root", "parent_id": None},
        ]

        result1 = self.validator.validate_project(
            project_id="cache-test",
            hierarchies=hierarchies,
        )

        cached = self.validator.get_cached_result("cache-test")
        assert cached is result1

    def test_clear_cache(self):
        """Test clearing the validation cache."""
        hierarchies = [
            {"hierarchy_id": "ROOT", "hierarchy_name": "Root", "parent_id": None},
        ]

        self.validator.validate_project(
            project_id="clear-test",
            hierarchies=hierarchies,
        )

        self.validator.clear_cache("clear-test")

        assert self.validator.get_cached_result("clear-test") is None


class TestSingletonBehavior:
    """Tests for singleton behavior."""

    @pytest.fixture(autouse=True)
    def setup(self):
        """Reset singleton before each test."""
        reset_hierarchy_validator()

    def test_get_returns_singleton(self):
        """Test that get_hierarchy_validator returns singleton."""
        v1 = get_hierarchy_validator()
        v2 = get_hierarchy_validator()

        assert v1 is v2

    def test_reset_creates_new_instance(self):
        """Test that reset creates a new instance."""
        v1 = get_hierarchy_validator()
        reset_hierarchy_validator()
        v2 = get_hierarchy_validator()

        assert v1 is not v2
