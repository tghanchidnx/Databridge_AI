"""
Tests for Forecast Workflow.

Tests the rolling forecast and scenario modeling automation.
"""

import pytest
import pandas as pd
from datetime import datetime, date
from unittest.mock import Mock, patch

from src.workflows.forecast_workflow import (
    ForecastWorkflow,
    ForecastMethod,
    ScenarioType,
    ForecastAssumption,
    ForecastPeriod,
    Scenario,
    ForecastSummary,
    ForecastWorkflowResult,
)


class TestForecastMethod:
    """Tests for ForecastMethod enum."""

    def test_forecast_methods(self):
        """Test all forecast methods exist."""
        assert ForecastMethod.STRAIGHT_LINE.value == "straight_line"
        assert ForecastMethod.TREND.value == "trend"
        assert ForecastMethod.SEASONAL.value == "seasonal"
        assert ForecastMethod.GROWTH_RATE.value == "growth_rate"
        assert ForecastMethod.MANUAL.value == "manual"
        assert ForecastMethod.YTD_RUN_RATE.value == "ytd_run_rate"
        assert ForecastMethod.PRIOR_YEAR.value == "prior_year"
        assert ForecastMethod.PRIOR_YEAR_GROWTH.value == "prior_year_growth"


class TestScenarioType:
    """Tests for ScenarioType enum."""

    def test_scenario_types(self):
        """Test all scenario types exist."""
        assert ScenarioType.BASE.value == "base"
        assert ScenarioType.UPSIDE.value == "upside"
        assert ScenarioType.DOWNSIDE.value == "downside"
        assert ScenarioType.STRESS.value == "stress"
        assert ScenarioType.CUSTOM.value == "custom"


class TestForecastAssumption:
    """Tests for ForecastAssumption dataclass."""

    def test_assumption_creation(self):
        """Test creating a forecast assumption."""
        assumption = ForecastAssumption(
            name="Revenue Growth",
            category="revenue",
            value=5.0,
            unit="%",
        )

        assert assumption.name == "Revenue Growth"
        assert assumption.value == 5.0
        assert assumption.unit == "%"

    def test_assumption_full(self):
        """Test assumption with all fields."""
        assumption = ForecastAssumption(
            name="Price Increase",
            category="pricing",
            value=3.5,
            unit="%",
            description="Annual price adjustment",
            source="management",
            confidence_level="high",
        )

        assert assumption.description != ""
        assert assumption.source == "management"
        assert assumption.confidence_level == "high"

    def test_assumption_to_dict(self):
        """Test assumption to dictionary conversion."""
        assumption = ForecastAssumption(
            name="Test",
            category="test",
            value=10,
        )

        result = assumption.to_dict()

        assert result["name"] == "Test"
        assert result["value"] == 10


class TestForecastPeriod:
    """Tests for ForecastPeriod dataclass."""

    def test_period_creation(self):
        """Test creating a forecast period."""
        period = ForecastPeriod(
            period="2024-01",
            actual=100000,
            budget=95000,
            forecast=100000,
            is_actual=True,
        )

        assert period.period == "2024-01"
        assert period.actual == 100000
        assert period.is_actual is True

    def test_period_future(self):
        """Test future forecast period."""
        period = ForecastPeriod(
            period="2024-12",
            actual=None,
            budget=120000,
            forecast=125000,
            is_actual=False,
        )

        assert period.actual is None
        assert period.forecast == 125000
        assert period.is_actual is False

    def test_period_to_dict(self):
        """Test period to dictionary conversion."""
        period = ForecastPeriod(
            period="2024-06",
            budget=100000,
            forecast=105000,
            notes="Strong outlook",
        )

        result = period.to_dict()

        assert result["period"] == "2024-06"
        assert result["notes"] == "Strong outlook"


class TestScenario:
    """Tests for Scenario dataclass."""

    def test_scenario_creation(self):
        """Test creating a scenario."""
        scenario = Scenario(
            scenario_id="base-001",
            name="Base Case",
            scenario_type=ScenarioType.BASE,
        )

        assert scenario.scenario_id == "base-001"
        assert scenario.name == "Base Case"
        assert scenario.scenario_type == ScenarioType.BASE

    def test_scenario_with_periods(self):
        """Test scenario with periods."""
        periods = [
            ForecastPeriod(period="2024-01", forecast=100000),
            ForecastPeriod(period="2024-02", forecast=105000),
        ]

        scenario = Scenario(
            scenario_id="s1",
            name="Test",
            scenario_type=ScenarioType.CUSTOM,
            periods=periods,
            total_forecast=205000,
        )

        assert len(scenario.periods) == 2
        assert scenario.total_forecast == 205000

    def test_scenario_with_assumptions(self):
        """Test scenario with assumptions."""
        assumptions = [
            ForecastAssumption(name="Growth", category="revenue", value=5),
        ]

        scenario = Scenario(
            scenario_id="s1",
            name="Growth Case",
            scenario_type=ScenarioType.UPSIDE,
            assumptions=assumptions,
        )

        assert len(scenario.assumptions) == 1

    def test_scenario_to_dict(self):
        """Test scenario to dictionary conversion."""
        scenario = Scenario(
            scenario_id="s1",
            name="Test",
            scenario_type=ScenarioType.DOWNSIDE,
            total_forecast=1000000,
            variance_to_base=-50000,
            variance_percent_to_base=-5.0,
        )

        result = scenario.to_dict()

        assert result["scenario_type"] == "downside"
        assert result["variance_to_base"] == -50000


class TestForecastSummary:
    """Tests for ForecastSummary dataclass."""

    def test_summary_creation(self):
        """Test creating a forecast summary."""
        summary = ForecastSummary(
            fiscal_year="FY2024",
            as_of_date="2024-06",
            ytd_actual=500000,
            ytd_budget=480000,
            ytd_variance=20000,
            ytd_variance_percent=4.17,
        )

        assert summary.fiscal_year == "FY2024"
        assert summary.ytd_actual == 500000
        assert summary.ytd_variance == 20000

    def test_summary_full_year(self):
        """Test summary with full year outlook."""
        summary = ForecastSummary(
            fiscal_year="FY2024",
            as_of_date="2024-06",
            ytd_actual=500000,
            ytd_budget=480000,
            remaining_forecast=550000,
            full_year_outlook=1050000,
            full_year_budget=1000000,
            full_year_variance=50000,
            full_year_variance_percent=5.0,
        )

        assert summary.full_year_outlook == 1050000
        assert summary.full_year_variance == 50000

    def test_summary_to_dict(self):
        """Test summary to dictionary conversion."""
        summary = ForecastSummary(
            fiscal_year="FY2024",
            as_of_date="2024-06",
            ytd_actual=100000,
            ytd_budget=95000,
        )

        result = summary.to_dict()

        assert result["fiscal_year"] == "FY2024"
        assert result["ytd_actual"] == 100000


class TestForecastWorkflowResult:
    """Tests for ForecastWorkflowResult dataclass."""

    def test_result_success(self):
        """Test successful result."""
        summary = ForecastSummary(
            fiscal_year="FY2024",
            as_of_date="2024-06",
        )

        result = ForecastWorkflowResult(
            success=True,
            message="Forecast complete",
            summary=summary,
        )

        assert result.success is True
        assert result.summary is not None

    def test_result_with_scenarios(self):
        """Test result with scenarios."""
        scenarios = [
            Scenario(scenario_id="s1", name="Base", scenario_type=ScenarioType.BASE),
            Scenario(scenario_id="s2", name="Upside", scenario_type=ScenarioType.UPSIDE),
        ]

        result = ForecastWorkflowResult(
            success=True,
            scenarios=scenarios,
        )

        assert len(result.scenarios) == 2

    def test_result_to_dict(self):
        """Test result to dictionary conversion."""
        result = ForecastWorkflowResult(
            success=True,
            message="Done",
            data={"key": "value"},
        )

        d = result.to_dict()

        assert d["success"] is True
        assert d["data"]["key"] == "value"


class TestForecastWorkflow:
    """Tests for ForecastWorkflow."""

    def test_workflow_initialization(self):
        """Test workflow initialization."""
        workflow = ForecastWorkflow(
            fiscal_year_start_month=7,
            forecast_method=ForecastMethod.GROWTH_RATE,
            default_growth_rate=5.0,
        )

        assert workflow.fiscal_year_start_month == 7
        assert workflow.forecast_method == ForecastMethod.GROWTH_RATE
        assert workflow.default_growth_rate == 5.0

    def test_workflow_defaults(self):
        """Test workflow default values."""
        workflow = ForecastWorkflow()

        assert workflow.fiscal_year_start_month == 1
        assert workflow.forecast_method == ForecastMethod.STRAIGHT_LINE
        assert workflow.default_growth_rate == 0.0

    def test_get_current_forecast(self):
        """Test getting current forecast status."""
        workflow = ForecastWorkflow()

        df = pd.DataFrame({
            "period": ["2024-01", "2024-02", "2024-03", "2024-04", "2024-05", "2024-06"],
            "actual": [100, 110, 105, 115, 108, 112],
            "budget": [95, 100, 100, 110, 105, 110],
        })

        result = workflow.get_current_forecast(
            data=df,
            actual_column="actual",
            budget_column="budget",
            period_column="period",
            as_of_period="2024-03",
            fiscal_year="FY2024",
        )

        assert result.success is True
        assert result.summary is not None
        assert result.summary.ytd_actual == 315  # 100 + 110 + 105
        assert result.summary.ytd_budget == 295  # 95 + 100 + 100

    def test_get_current_forecast_with_prior_year(self):
        """Test forecast with prior year comparison."""
        workflow = ForecastWorkflow()

        df = pd.DataFrame({
            "period": ["2024-01", "2024-02", "2024-03"],
            "actual": [100, 110, 105],
            "budget": [95, 100, 100],
            "prior_year": [90, 95, 98],
        })

        result = workflow.get_current_forecast(
            data=df,
            actual_column="actual",
            budget_column="budget",
            period_column="period",
            as_of_period="2024-02",
            prior_year_column="prior_year",
        )

        assert result.success is True
        assert result.summary.prior_year_total == 283

    def test_update_rolling_forecast_straight_line(self):
        """Test rolling forecast with straight line method."""
        workflow = ForecastWorkflow()

        df = pd.DataFrame({
            "period": ["2024-01", "2024-02", "2024-03", "2024-04"],
            "actual": [100, 110, 0, 0],
            "budget": [95, 100, 105, 110],
        })

        result = workflow.update_rolling_forecast(
            data=df,
            actual_column="actual",
            budget_column="budget",
            period_column="period",
            as_of_period="2024-02",
            forecast_method=ForecastMethod.STRAIGHT_LINE,
        )

        assert result.success is True
        assert len(result.periods) == 4
        # First two periods should use actuals
        assert result.periods[0].forecast == 100
        assert result.periods[1].forecast == 110
        # Remaining should use budget
        assert result.periods[2].forecast == 105
        assert result.periods[3].forecast == 110

    def test_update_rolling_forecast_growth_rate(self):
        """Test rolling forecast with growth rate method."""
        workflow = ForecastWorkflow()

        df = pd.DataFrame({
            "period": ["2024-01", "2024-02", "2024-03", "2024-04"],
            "actual": [100, 110, 0, 0],
            "budget": [95, 100, 100, 100],
        })

        result = workflow.update_rolling_forecast(
            data=df,
            actual_column="actual",
            budget_column="budget",
            period_column="period",
            as_of_period="2024-02",
            forecast_method=ForecastMethod.GROWTH_RATE,
            growth_rate=10.0,
        )

        assert result.success is True
        # Remaining periods should have 10% growth applied to budget
        assert result.periods[2].forecast == pytest.approx(110, rel=1e-9)  # 100 * 1.10
        assert result.periods[3].forecast == pytest.approx(110, rel=1e-9)

    def test_update_rolling_forecast_ytd_run_rate(self):
        """Test rolling forecast with YTD run rate method."""
        workflow = ForecastWorkflow()

        df = pd.DataFrame({
            "period": ["2024-01", "2024-02", "2024-03", "2024-04"],
            "actual": [100, 120, 0, 0],
            "budget": [95, 100, 105, 110],
        })

        result = workflow.update_rolling_forecast(
            data=df,
            actual_column="actual",
            budget_column="budget",
            period_column="period",
            as_of_period="2024-02",
            forecast_method=ForecastMethod.YTD_RUN_RATE,
        )

        assert result.success is True
        # YTD run rate = (100 + 120) / 2 = 110
        assert result.periods[2].forecast == 110
        assert result.periods[3].forecast == 110

    def test_update_rolling_forecast_with_adjustments(self):
        """Test rolling forecast with period adjustments."""
        workflow = ForecastWorkflow()

        df = pd.DataFrame({
            "period": ["2024-01", "2024-02", "2024-03"],
            "actual": [100, 0, 0],
            "budget": [95, 100, 105],
        })

        result = workflow.update_rolling_forecast(
            data=df,
            actual_column="actual",
            budget_column="budget",
            period_column="period",
            as_of_period="2024-01",
            adjustment_factors={"2024-03": 1.20},  # 20% increase
        )

        assert result.success is True
        assert result.periods[2].forecast == 126  # 105 * 1.20

    def test_create_scenario(self):
        """Test creating a forecast scenario."""
        workflow = ForecastWorkflow()

        base_periods = [
            ForecastPeriod(period="2024-01", forecast=100000),
            ForecastPeriod(period="2024-02", forecast=105000),
        ]

        result = workflow.create_scenario(
            scenario_id="base-001",
            name="Base Case",
            scenario_type=ScenarioType.BASE,
            base_periods=base_periods,
            description="Most likely outcome",
        )

        assert result.success is True
        assert result.scenario.name == "Base Case"
        assert result.scenario.total_forecast == 205000

    def test_create_scenario_with_assumptions(self):
        """Test creating scenario with assumptions."""
        workflow = ForecastWorkflow()

        assumptions = [
            ForecastAssumption(name="Growth", category="revenue", value=5),
            ForecastAssumption(name="Inflation", category="expense", value=3),
        ]

        result = workflow.create_scenario(
            scenario_id="up-001",
            name="Upside",
            scenario_type=ScenarioType.UPSIDE,
            base_periods=[],
            assumptions=assumptions,
        )

        assert result.success is True
        assert len(result.scenario.assumptions) == 2

    def test_model_scenario(self):
        """Test modeling a scenario with adjustments."""
        workflow = ForecastWorkflow()

        # Create base scenario first
        periods = [
            ForecastPeriod(period="2024-01", forecast=100000, is_actual=True),
            ForecastPeriod(period="2024-02", forecast=100000, is_actual=False),
        ]
        workflow.create_scenario(
            scenario_id="test-001",
            name="Test",
            scenario_type=ScenarioType.CUSTOM,
            base_periods=periods,
        )

        # Model with percent change
        result = workflow.model_scenario(
            scenario_id="test-001",
            adjustments={"percent_change": 10},
        )

        assert result.success is True
        # Only non-actual periods should be adjusted
        assert result.scenario.periods[0].forecast == 100000  # Actual, unchanged
        assert result.scenario.periods[1].forecast == pytest.approx(110000, rel=1e-9)  # 10% increase

    def test_model_scenario_not_found(self):
        """Test modeling non-existent scenario."""
        workflow = ForecastWorkflow()

        result = workflow.model_scenario(
            scenario_id="nonexistent",
            adjustments={},
        )

        assert result.success is False
        assert "not found" in result.message

    def test_compare_scenarios(self):
        """Test comparing multiple scenarios."""
        workflow = ForecastWorkflow()

        # Create base scenario
        base_periods = [ForecastPeriod(period="2024-01", forecast=100000)]
        workflow.create_scenario(
            scenario_id="base",
            name="Base",
            scenario_type=ScenarioType.BASE,
            base_periods=base_periods,
        )

        # Create upside scenario
        upside_periods = [ForecastPeriod(period="2024-01", forecast=110000)]
        workflow.create_scenario(
            scenario_id="upside",
            name="Upside",
            scenario_type=ScenarioType.UPSIDE,
            base_periods=upside_periods,
        )

        result = workflow.compare_scenarios()

        assert result.success is True
        assert len(result.scenarios) == 2
        assert "totals" in result.data
        assert "by_period" in result.data

    def test_compare_scenarios_empty(self):
        """Test comparing when no scenarios exist."""
        workflow = ForecastWorkflow()

        result = workflow.compare_scenarios()

        assert result.success is False
        assert "No scenarios" in result.message

    def test_generate_forecast_report(self):
        """Test generating forecast report."""
        workflow = ForecastWorkflow()

        periods = [
            ForecastPeriod(period="2024-01", actual=100000, budget=95000, forecast=100000, is_actual=True),
            ForecastPeriod(period="2024-02", budget=100000, forecast=105000, is_actual=False),
        ]
        assumptions = [
            ForecastAssumption(name="Growth", category="revenue", value=5, unit="%", confidence_level="high"),
        ]

        workflow.create_scenario(
            scenario_id="base",
            name="Base Case",
            scenario_type=ScenarioType.BASE,
            base_periods=periods,
            assumptions=assumptions,
            description="Most likely scenario",
        )

        report = workflow.generate_forecast_report(
            scenario_id="base",
            include_assumptions=True,
            include_variance=True,
        )

        assert "# Forecast Report: Base Case" in report
        assert "## Summary" in report
        assert "## Period Detail" in report
        assert "## Key Assumptions" in report

    def test_get_scenarios(self):
        """Test getting all scenarios."""
        workflow = ForecastWorkflow()

        workflow.create_scenario("s1", "Scenario 1", ScenarioType.BASE, [])
        workflow.create_scenario("s2", "Scenario 2", ScenarioType.UPSIDE, [])

        scenarios = workflow.get_scenarios()

        assert len(scenarios) == 2

    def test_get_scenario(self):
        """Test getting a specific scenario."""
        workflow = ForecastWorkflow()

        workflow.create_scenario("test", "Test Scenario", ScenarioType.CUSTOM, [])

        scenario = workflow.get_scenario("test")

        assert scenario is not None
        assert scenario.name == "Test Scenario"

    def test_get_scenario_nonexistent(self):
        """Test getting non-existent scenario."""
        workflow = ForecastWorkflow()

        scenario = workflow.get_scenario("nonexistent")

        assert scenario is None

    def test_delete_scenario(self):
        """Test deleting a scenario."""
        workflow = ForecastWorkflow()

        workflow.create_scenario("test", "Test", ScenarioType.CUSTOM, [])

        result = workflow.delete_scenario("test")

        assert result is True
        assert workflow.get_scenario("test") is None

    def test_delete_scenario_nonexistent(self):
        """Test deleting non-existent scenario."""
        workflow = ForecastWorkflow()

        result = workflow.delete_scenario("nonexistent")

        assert result is False

    def test_set_base_scenario(self):
        """Test setting base scenario."""
        workflow = ForecastWorkflow()

        workflow.create_scenario("s1", "First", ScenarioType.CUSTOM, [])
        workflow.create_scenario("s2", "Second", ScenarioType.CUSTOM, [])

        result = workflow.set_base_scenario("s2")

        assert result is True
        assert workflow._base_scenario_id == "s2"

    def test_set_base_scenario_nonexistent(self):
        """Test setting non-existent base scenario."""
        workflow = ForecastWorkflow()

        result = workflow.set_base_scenario("nonexistent")

        assert result is False
