"""
Tests for Variance Analysis Workflow.

Tests the budget vs actual and prior year variance analysis automation.
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock

from src.workflows.variance_workflow import (
    VarianceWorkflow,
    VarianceComparisonType,
    VarianceDriver,
    VarianceCommentary,
    VarianceWorkflowResult,
)
from src.insights.variance import VarianceType


class TestVarianceComparisonType:
    """Tests for VarianceComparisonType enum."""

    def test_comparison_types(self):
        """Test all comparison types exist."""
        assert VarianceComparisonType.BUDGET.value == "budget"
        assert VarianceComparisonType.PRIOR_YEAR.value == "prior_year"
        assert VarianceComparisonType.PRIOR_PERIOD.value == "prior_period"
        assert VarianceComparisonType.FORECAST.value == "forecast"
        assert VarianceComparisonType.PLAN.value == "plan"


class TestVarianceDriver:
    """Tests for VarianceDriver dataclass."""

    def test_driver_creation(self):
        """Test creating a variance driver."""
        driver = VarianceDriver(
            dimension="Product",
            dimension_value="Widget A",
            variance_amount=10000.0,
            variance_percent=5.5,
            contribution_percent=25.0,
            variance_type=VarianceType.FAVORABLE,
        )

        assert driver.dimension == "Product"
        assert driver.dimension_value == "Widget A"
        assert driver.variance_amount == 10000.0
        assert driver.variance_type == VarianceType.FAVORABLE

    def test_driver_with_explanation(self):
        """Test driver with explanation and actions."""
        driver = VarianceDriver(
            dimension="Region",
            dimension_value="North",
            variance_amount=-5000.0,
            variance_percent=-3.2,
            contribution_percent=15.0,
            variance_type=VarianceType.UNFAVORABLE,
            explanation="Lower sales due to market conditions",
            action_items=["Review pricing strategy", "Increase marketing"],
        )

        assert driver.explanation != ""
        assert len(driver.action_items) == 2

    def test_driver_to_dict(self):
        """Test driver to dictionary conversion."""
        driver = VarianceDriver(
            dimension="Account",
            dimension_value="4100",
            variance_amount=1234.56,
            variance_percent=2.5,
            contribution_percent=10.0,
            variance_type=VarianceType.FAVORABLE,
        )

        result = driver.to_dict()

        assert result["dimension"] == "Account"
        assert result["variance_amount"] == 1234.56
        assert result["variance_type"] == "favorable"


class TestVarianceCommentary:
    """Tests for VarianceCommentary dataclass."""

    def test_commentary_creation(self):
        """Test creating variance commentary."""
        commentary = VarianceCommentary(
            executive_summary="Revenue exceeded budget by 5%",
            key_findings=["Strong sales in Q4", "New product launch successful"],
        )

        assert commentary.executive_summary != ""
        assert len(commentary.key_findings) == 2

    def test_commentary_full(self):
        """Test full commentary with all fields."""
        commentary = VarianceCommentary(
            executive_summary="Overall favorable variance",
            key_findings=["Finding 1"],
            favorable_highlights=["Product A +10%"],
            unfavorable_concerns=["Region B -5%"],
            action_items=["Review Region B"],
            detailed_narrative="Full narrative here...",
        )

        assert len(commentary.favorable_highlights) == 1
        assert len(commentary.unfavorable_concerns) == 1
        assert len(commentary.action_items) == 1

    def test_commentary_to_dict(self):
        """Test commentary to dictionary conversion."""
        commentary = VarianceCommentary(
            executive_summary="Test summary",
            key_findings=["Finding"],
        )

        result = commentary.to_dict()

        assert "executive_summary" in result
        assert "key_findings" in result
        assert len(result["key_findings"]) == 1

    def test_commentary_to_markdown(self):
        """Test commentary to markdown conversion."""
        commentary = VarianceCommentary(
            executive_summary="Q4 results were favorable",
            key_findings=["Strong performance", "Cost savings"],
            favorable_highlights=["Revenue +10%"],
            unfavorable_concerns=["Marketing -5%"],
            action_items=["Review marketing spend"],
        )

        markdown = commentary.to_markdown()

        assert "## Executive Summary" in markdown
        assert "## Key Findings" in markdown
        assert "### Favorable Variances" in markdown
        assert "### Areas of Concern" in markdown
        assert "## Recommended Actions" in markdown
        assert "- [ ]" in markdown  # Checkbox format


class TestVarianceWorkflowResult:
    """Tests for VarianceWorkflowResult dataclass."""

    def test_result_success(self):
        """Test successful result."""
        result = VarianceWorkflowResult(
            success=True,
            message="Analysis complete",
            comparison_type=VarianceComparisonType.BUDGET,
            period="2024-01",
        )

        assert result.success is True
        assert result.comparison_type == VarianceComparisonType.BUDGET

    def test_result_with_drivers(self):
        """Test result with drivers."""
        drivers = [
            VarianceDriver(
                dimension="Product",
                dimension_value="A",
                variance_amount=100,
                variance_percent=5,
                contribution_percent=50,
                variance_type=VarianceType.FAVORABLE,
            ),
        ]

        result = VarianceWorkflowResult(
            success=True,
            drivers=drivers,
        )

        assert len(result.drivers) == 1

    def test_result_to_dict(self):
        """Test result to dictionary conversion."""
        commentary = VarianceCommentary(executive_summary="Test")
        result = VarianceWorkflowResult(
            success=True,
            message="Done",
            comparison_type=VarianceComparisonType.PRIOR_YEAR,
            commentary=commentary,
        )

        d = result.to_dict()

        assert d["success"] is True
        assert d["comparison_type"] == "prior_year"
        assert d["commentary"]["executive_summary"] == "Test"


class TestVarianceWorkflow:
    """Tests for VarianceWorkflow."""

    def test_workflow_initialization(self):
        """Test workflow initialization."""
        workflow = VarianceWorkflow(
            materiality_threshold=0.10,
            top_drivers_count=3,
        )

        assert workflow.materiality_threshold == 0.10
        assert workflow.top_drivers_count == 3

    def test_workflow_defaults(self):
        """Test workflow default values."""
        workflow = VarianceWorkflow()

        assert workflow.materiality_threshold == 0.05
        assert workflow.top_drivers_count == 5
        assert workflow.generate_commentary is True

    @patch("src.workflows.variance_workflow.VarianceAnalyzer")
    def test_analyze_budget_variance(self, mock_analyzer_class):
        """Test budget variance analysis."""
        # Setup mock
        mock_analyzer = MagicMock()
        mock_analyzer_class.return_value = mock_analyzer

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.total_actual = 100000
        mock_result.total_budget = 95000
        mock_result.total_variance = 5000
        mock_result.total_variance_percent = 5.26
        mock_result.overall_variance_type = VarianceType.FAVORABLE
        mock_result.top_drivers = []
        mock_result.get_favorable.return_value = []
        mock_result.get_unfavorable.return_value = []
        mock_analyzer.analyze.return_value = mock_result
        mock_analyzer.generate_commentary.return_value = "Commentary text"

        # Test
        workflow = VarianceWorkflow()
        workflow._analyzer = mock_analyzer

        df = pd.DataFrame({
            "account": ["4100", "4200", "5100"],
            "actual": [50000, 50000, 40000],
            "budget": [45000, 50000, 42000],
        })

        result = workflow.analyze_budget_variance(
            data=df,
            actual_column="actual",
            budget_column="budget",
            dimension_columns=["account"],
            period="2024-01",
        )

        assert result.success is True
        assert result.comparison_type == VarianceComparisonType.BUDGET

    @patch("src.workflows.variance_workflow.VarianceAnalyzer")
    def test_analyze_budget_variance_failure(self, mock_analyzer_class):
        """Test budget variance analysis failure."""
        mock_analyzer = MagicMock()
        mock_analyzer_class.return_value = mock_analyzer

        mock_result = MagicMock()
        mock_result.success = False
        mock_result.errors = ["Invalid data"]
        mock_analyzer.analyze.return_value = mock_result

        workflow = VarianceWorkflow()
        workflow._analyzer = mock_analyzer

        df = pd.DataFrame({"actual": [], "budget": []})

        result = workflow.analyze_budget_variance(
            data=df,
            actual_column="actual",
            budget_column="budget",
        )

        assert result.success is False

    @patch("src.workflows.variance_workflow.VarianceAnalyzer")
    def test_analyze_prior_year_variance(self, mock_analyzer_class):
        """Test prior year variance analysis."""
        mock_analyzer = MagicMock()
        mock_analyzer_class.return_value = mock_analyzer

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.total_actual = 110000
        mock_result.total_budget = 100000
        mock_result.total_variance = 10000
        mock_result.total_variance_percent = 10.0
        mock_result.overall_variance_type = VarianceType.FAVORABLE
        mock_result.top_drivers = []
        mock_result.get_favorable.return_value = []
        mock_result.get_unfavorable.return_value = []
        mock_analyzer.analyze.return_value = mock_result
        mock_analyzer.generate_commentary.return_value = "YoY Commentary"

        workflow = VarianceWorkflow()
        workflow._analyzer = mock_analyzer

        df = pd.DataFrame({
            "account": ["4100", "4200"],
            "current": [60000, 50000],
            "prior_year": [55000, 45000],
        })

        result = workflow.analyze_prior_year_variance(
            data=df,
            current_column="current",
            prior_column="prior_year",
            dimension_columns=["account"],
            period="2024",
        )

        assert result.success is True
        assert result.comparison_type == VarianceComparisonType.PRIOR_YEAR

    @patch("src.workflows.variance_workflow.VarianceAnalyzer")
    def test_identify_variance_drivers(self, mock_analyzer_class):
        """Test identifying variance drivers across dimensions."""
        mock_analyzer = MagicMock()
        mock_analyzer_class.return_value = mock_analyzer

        mock_driver = MagicMock()
        mock_driver.dimension = "product"
        mock_driver.dimension_value = "Widget"
        mock_driver.variance = 5000
        mock_driver.variance_percent = 10
        mock_driver.contribution_percent = 50
        mock_driver.variance_type = VarianceType.FAVORABLE

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.top_drivers = [mock_driver]
        mock_analyzer.analyze.return_value = mock_result

        workflow = VarianceWorkflow()
        workflow._analyzer = mock_analyzer

        df = pd.DataFrame({
            "product": ["A", "B", "C"],
            "region": ["North", "South", "East"],
            "actual": [100, 200, 150],
            "budget": [90, 180, 160],
        })

        result = workflow.identify_variance_drivers(
            data=df,
            actual_column="actual",
            budget_column="budget",
            dimension_columns=["product", "region"],
            min_contribution=0.05,
        )

        assert result.success is True
        assert "dimensions_analyzed" in result.data

    @patch("src.workflows.variance_workflow.VarianceAnalyzer")
    def test_decompose_variance(self, mock_analyzer_class):
        """Test variance decomposition into price/volume/mix."""
        mock_analyzer = MagicMock()
        mock_analyzer_class.return_value = mock_analyzer

        mock_decomposition = MagicMock()
        mock_decomposition.to_dict.return_value = {
            "price_variance": 1000,
            "volume_variance": 500,
            "mix_variance": 200,
            "total_variance": 1700,
        }
        mock_decomposition.explanation = "Price increased by 5%"
        mock_analyzer.decompose_variance.return_value = mock_decomposition

        workflow = VarianceWorkflow()
        workflow._analyzer = mock_analyzer

        result = workflow.decompose_variance(
            actual_price=105.0,
            actual_volume=1000,
            budget_price=100.0,
            budget_volume=950,
        )

        assert result.success is True
        assert "decomposition" in result.data
        assert "explanation" in result.data

    def test_generate_commentary_structure(self):
        """Test that generated commentary has correct structure."""
        workflow = VarianceWorkflow()

        # Create mock variance result
        mock_result = MagicMock()
        mock_result.total_actual = 100000
        mock_result.total_budget = 95000
        mock_result.total_variance = 5000
        mock_result.total_variance_percent = 5.26
        mock_result.overall_variance_type = VarianceType.FAVORABLE
        mock_result.top_drivers = []
        mock_result.get_favorable.return_value = []
        mock_result.get_unfavorable.return_value = []

        commentary = workflow._generate_commentary(
            result=mock_result,
            comparison_type=VarianceComparisonType.BUDGET,
            period="January 2024",
        )

        assert commentary.executive_summary != ""
        assert isinstance(commentary.key_findings, list)
        assert isinstance(commentary.favorable_highlights, list)
        assert isinstance(commentary.unfavorable_concerns, list)

    def test_commentary_favorable_variance(self):
        """Test commentary for favorable variance."""
        workflow = VarianceWorkflow()

        mock_result = MagicMock()
        mock_result.total_actual = 110000
        mock_result.total_budget = 100000
        mock_result.total_variance = 10000
        mock_result.total_variance_percent = 10.0
        mock_result.overall_variance_type = VarianceType.FAVORABLE
        mock_result.top_drivers = []
        mock_result.get_favorable.return_value = []
        mock_result.get_unfavorable.return_value = []

        commentary = workflow._generate_commentary(
            result=mock_result,
            comparison_type=VarianceComparisonType.BUDGET,
            period="Q1 2024",
        )

        assert "above" in commentary.executive_summary
        assert "favorable" in commentary.executive_summary.lower()

    def test_commentary_unfavorable_variance(self):
        """Test commentary for unfavorable variance."""
        workflow = VarianceWorkflow()

        mock_result = MagicMock()
        mock_result.total_actual = 90000
        mock_result.total_budget = 100000
        mock_result.total_variance = -10000
        mock_result.total_variance_percent = -10.0
        mock_result.overall_variance_type = VarianceType.UNFAVORABLE
        mock_result.top_drivers = []
        mock_result.get_favorable.return_value = []
        mock_result.get_unfavorable.return_value = []

        commentary = workflow._generate_commentary(
            result=mock_result,
            comparison_type=VarianceComparisonType.BUDGET,
            period="Q1 2024",
        )

        assert "below" in commentary.executive_summary
        assert "unfavorable" in commentary.executive_summary.lower()

    def test_without_commentary_generation(self):
        """Test workflow without commentary generation."""
        workflow = VarianceWorkflow(generate_commentary=False)

        assert workflow.generate_commentary is False
