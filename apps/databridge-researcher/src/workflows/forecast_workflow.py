"""
Forecast Workflow for DataBridge Analytics V4.

Automates rolling forecast updates and scenario modeling.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Union
from enum import Enum
from datetime import datetime, date
from copy import deepcopy
import logging

import pandas as pd
import numpy as np

from ..insights.variance import VarianceType


logger = logging.getLogger(__name__)


class ForecastMethod(str, Enum):
    """Method for generating forecasts."""
    STRAIGHT_LINE = "straight_line"  # Linear extrapolation
    TREND = "trend"  # Trend-based projection
    SEASONAL = "seasonal"  # Seasonal adjustment
    GROWTH_RATE = "growth_rate"  # Apply growth rate
    MANUAL = "manual"  # Manual input
    YTD_RUN_RATE = "ytd_run_rate"  # Annualize YTD
    PRIOR_YEAR = "prior_year"  # Prior year values
    PRIOR_YEAR_GROWTH = "prior_year_growth"  # Prior year with growth


class ScenarioType(str, Enum):
    """Type of forecast scenario."""
    BASE = "base"  # Base/most likely scenario
    UPSIDE = "upside"  # Optimistic scenario
    DOWNSIDE = "downside"  # Pessimistic scenario
    STRESS = "stress"  # Stress test scenario
    CUSTOM = "custom"  # Custom scenario


@dataclass
class ForecastAssumption:
    """An assumption driving a forecast."""

    name: str
    category: str  # revenue, expense, volume, price, etc.
    value: float
    unit: str = ""  # %, $, units, etc.
    description: str = ""
    source: str = ""  # manual, calculated, external
    confidence_level: str = "medium"  # low, medium, high

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "name": self.name,
            "category": self.category,
            "value": self.value,
            "unit": self.unit,
            "description": self.description,
            "source": self.source,
            "confidence_level": self.confidence_level,
        }


@dataclass
class ForecastPeriod:
    """A single period in a forecast."""

    period: str  # e.g., "2024-01", "Q1 2024"
    actual: Optional[float] = None
    budget: Optional[float] = None
    forecast: Optional[float] = None
    prior_year: Optional[float] = None
    variance_to_budget: Optional[float] = None
    variance_to_prior: Optional[float] = None
    is_actual: bool = False  # True if this period has actuals
    is_locked: bool = False
    notes: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "period": self.period,
            "actual": self.actual,
            "budget": self.budget,
            "forecast": self.forecast,
            "prior_year": self.prior_year,
            "variance_to_budget": self.variance_to_budget,
            "variance_to_prior": self.variance_to_prior,
            "is_actual": self.is_actual,
            "is_locked": self.is_locked,
            "notes": self.notes,
        }


@dataclass
class Scenario:
    """A forecast scenario."""

    scenario_id: str
    name: str
    scenario_type: ScenarioType
    description: str = ""
    assumptions: List[ForecastAssumption] = field(default_factory=list)
    periods: List[ForecastPeriod] = field(default_factory=list)
    total_forecast: float = 0.0
    variance_to_base: float = 0.0
    variance_percent_to_base: float = 0.0
    created_at: datetime = field(default_factory=datetime.now)
    created_by: str = ""
    is_active: bool = True

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "scenario_id": self.scenario_id,
            "name": self.name,
            "scenario_type": self.scenario_type.value,
            "description": self.description,
            "assumptions": [a.to_dict() for a in self.assumptions],
            "periods": [p.to_dict() for p in self.periods],
            "total_forecast": round(self.total_forecast, 2),
            "variance_to_base": round(self.variance_to_base, 2),
            "variance_percent_to_base": round(self.variance_percent_to_base, 2),
            "created_at": self.created_at.isoformat(),
            "created_by": self.created_by,
            "is_active": self.is_active,
        }


@dataclass
class ForecastSummary:
    """Summary of a forecast."""

    fiscal_year: str
    as_of_date: str
    ytd_actual: float = 0.0
    ytd_budget: float = 0.0
    ytd_variance: float = 0.0
    ytd_variance_percent: float = 0.0
    remaining_forecast: float = 0.0
    full_year_outlook: float = 0.0
    full_year_budget: float = 0.0
    full_year_variance: float = 0.0
    full_year_variance_percent: float = 0.0
    prior_year_total: float = 0.0
    yoy_growth: float = 0.0
    yoy_growth_percent: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "fiscal_year": self.fiscal_year,
            "as_of_date": self.as_of_date,
            "ytd_actual": round(self.ytd_actual, 2),
            "ytd_budget": round(self.ytd_budget, 2),
            "ytd_variance": round(self.ytd_variance, 2),
            "ytd_variance_percent": round(self.ytd_variance_percent, 2),
            "remaining_forecast": round(self.remaining_forecast, 2),
            "full_year_outlook": round(self.full_year_outlook, 2),
            "full_year_budget": round(self.full_year_budget, 2),
            "full_year_variance": round(self.full_year_variance, 2),
            "full_year_variance_percent": round(self.full_year_variance_percent, 2),
            "prior_year_total": round(self.prior_year_total, 2),
            "yoy_growth": round(self.yoy_growth, 2),
            "yoy_growth_percent": round(self.yoy_growth_percent, 2),
        }


@dataclass
class ForecastWorkflowResult:
    """Result from forecast workflow operations."""

    success: bool
    message: str = ""
    summary: Optional[ForecastSummary] = None
    scenario: Optional[Scenario] = None
    scenarios: List[Scenario] = field(default_factory=list)
    periods: List[ForecastPeriod] = field(default_factory=list)
    data: Any = None
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "message": self.message,
            "summary": self.summary.to_dict() if self.summary else None,
            "scenario": self.scenario.to_dict() if self.scenario else None,
            "scenarios": [s.to_dict() for s in self.scenarios],
            "periods": [p.to_dict() for p in self.periods],
            "data": self.data,
            "errors": self.errors,
        }


class ForecastWorkflow:
    """
    Orchestrates forecast workflows.

    Provides:
    - Rolling forecast updates
    - YTD actual replacement
    - Scenario modeling
    - Full year outlook calculation
    - Forecast comparison
    """

    def __init__(
        self,
        fiscal_year_start_month: int = 1,
        forecast_method: ForecastMethod = ForecastMethod.STRAIGHT_LINE,
        default_growth_rate: float = 0.0,
    ):
        """
        Initialize the forecast workflow.

        Args:
            fiscal_year_start_month: Starting month of fiscal year (1=Jan).
            forecast_method: Default method for generating forecasts.
            default_growth_rate: Default growth rate for projections.
        """
        self.fiscal_year_start_month = fiscal_year_start_month
        self.forecast_method = forecast_method
        self.default_growth_rate = default_growth_rate
        self._scenarios: Dict[str, Scenario] = {}
        self._base_scenario_id: Optional[str] = None

    def get_current_forecast(
        self,
        data: pd.DataFrame,
        actual_column: str,
        budget_column: str,
        period_column: str,
        as_of_period: str,
        fiscal_year: str = "",
        prior_year_column: Optional[str] = None,
    ) -> ForecastWorkflowResult:
        """
        Get current forecast status with YTD actuals and outlook.

        Args:
            data: DataFrame with financial data.
            actual_column: Column name for actual values.
            budget_column: Column name for budget values.
            period_column: Column name for period.
            as_of_period: Current period (e.g., "2024-06").
            fiscal_year: Fiscal year label.
            prior_year_column: Optional column for prior year values.

        Returns:
            ForecastWorkflowResult with forecast summary.
        """
        try:
            # Sort by period
            df = data.sort_values(period_column).copy()

            # Determine YTD vs remaining periods
            ytd_mask = df[period_column] <= as_of_period
            remaining_mask = df[period_column] > as_of_period

            # Calculate YTD
            ytd_actual = df.loc[ytd_mask, actual_column].sum()
            ytd_budget = df.loc[ytd_mask, budget_column].sum()
            ytd_variance = ytd_actual - ytd_budget
            ytd_variance_percent = (ytd_variance / ytd_budget * 100) if ytd_budget else 0.0

            # Calculate remaining forecast (use budget for remaining periods)
            remaining_forecast = df.loc[remaining_mask, budget_column].sum()

            # Full year outlook = YTD Actual + Remaining Forecast
            full_year_outlook = ytd_actual + remaining_forecast
            full_year_budget = df[budget_column].sum()
            full_year_variance = full_year_outlook - full_year_budget
            full_year_variance_percent = (full_year_variance / full_year_budget * 100) if full_year_budget else 0.0

            # Prior year comparison
            prior_year_total = 0.0
            yoy_growth = 0.0
            yoy_growth_percent = 0.0
            if prior_year_column and prior_year_column in df.columns:
                prior_year_total = df[prior_year_column].sum()
                yoy_growth = full_year_outlook - prior_year_total
                yoy_growth_percent = (yoy_growth / prior_year_total * 100) if prior_year_total else 0.0

            # Build period detail
            periods = []
            for _, row in df.iterrows():
                period = row[period_column]
                is_actual = period <= as_of_period
                periods.append(ForecastPeriod(
                    period=str(period),
                    actual=row[actual_column] if is_actual else None,
                    budget=row[budget_column],
                    forecast=row[actual_column] if is_actual else row[budget_column],
                    prior_year=row.get(prior_year_column) if prior_year_column else None,
                    is_actual=is_actual,
                ))

            summary = ForecastSummary(
                fiscal_year=fiscal_year or str(as_of_period[:4]),
                as_of_date=as_of_period,
                ytd_actual=ytd_actual,
                ytd_budget=ytd_budget,
                ytd_variance=ytd_variance,
                ytd_variance_percent=ytd_variance_percent,
                remaining_forecast=remaining_forecast,
                full_year_outlook=full_year_outlook,
                full_year_budget=full_year_budget,
                full_year_variance=full_year_variance,
                full_year_variance_percent=full_year_variance_percent,
                prior_year_total=prior_year_total,
                yoy_growth=yoy_growth,
                yoy_growth_percent=yoy_growth_percent,
            )

            return ForecastWorkflowResult(
                success=True,
                message=f"Current forecast as of {as_of_period}",
                summary=summary,
                periods=periods,
            )

        except Exception as e:
            logger.error(f"Failed to get current forecast: {e}")
            return ForecastWorkflowResult(
                success=False,
                message=f"Failed to get current forecast: {str(e)}",
                errors=[str(e)],
            )

    def update_rolling_forecast(
        self,
        data: pd.DataFrame,
        actual_column: str,
        budget_column: str,
        period_column: str,
        as_of_period: str,
        forecast_method: Optional[ForecastMethod] = None,
        growth_rate: Optional[float] = None,
        adjustment_factors: Optional[Dict[str, float]] = None,
    ) -> ForecastWorkflowResult:
        """
        Update rolling forecast with YTD actuals and projected remaining.

        Args:
            data: DataFrame with financial data.
            actual_column: Column name for actual values.
            budget_column: Column name for budget values.
            period_column: Column name for period.
            as_of_period: Current period.
            forecast_method: Method for remaining periods.
            growth_rate: Growth rate to apply.
            adjustment_factors: Period-specific adjustments.

        Returns:
            ForecastWorkflowResult with updated forecast.
        """
        try:
            method = forecast_method or self.forecast_method
            rate = growth_rate if growth_rate is not None else self.default_growth_rate

            df = data.sort_values(period_column).copy()
            df["forecast"] = 0.0

            # YTD periods use actuals
            ytd_mask = df[period_column] <= as_of_period
            df.loc[ytd_mask, "forecast"] = df.loc[ytd_mask, actual_column]

            # Remaining periods use projected values
            remaining_mask = df[period_column] > as_of_period

            if method == ForecastMethod.STRAIGHT_LINE:
                # Use budget for remaining periods
                df.loc[remaining_mask, "forecast"] = df.loc[remaining_mask, budget_column]

            elif method == ForecastMethod.GROWTH_RATE:
                # Apply growth rate to budget
                df.loc[remaining_mask, "forecast"] = df.loc[remaining_mask, budget_column] * (1 + rate / 100)

            elif method == ForecastMethod.YTD_RUN_RATE:
                # Annualize YTD actual
                ytd_actual = df.loc[ytd_mask, actual_column].sum()
                ytd_periods = ytd_mask.sum()
                total_periods = len(df)
                remaining_periods = total_periods - ytd_periods

                if ytd_periods > 0 and remaining_periods > 0:
                    run_rate = ytd_actual / ytd_periods
                    df.loc[remaining_mask, "forecast"] = run_rate

            elif method == ForecastMethod.TREND:
                # Linear trend from YTD
                ytd_values = df.loc[ytd_mask, actual_column].values
                if len(ytd_values) >= 2:
                    x = np.arange(len(ytd_values))
                    coeffs = np.polyfit(x, ytd_values, 1)
                    remaining_x = np.arange(len(ytd_values), len(df))
                    trend_values = np.polyval(coeffs, remaining_x)
                    df.loc[remaining_mask, "forecast"] = trend_values
                else:
                    df.loc[remaining_mask, "forecast"] = df.loc[remaining_mask, budget_column]

            elif method == ForecastMethod.PRIOR_YEAR:
                # Use prior year values if available
                if "prior_year" in df.columns:
                    df.loc[remaining_mask, "forecast"] = df.loc[remaining_mask, "prior_year"]
                else:
                    df.loc[remaining_mask, "forecast"] = df.loc[remaining_mask, budget_column]

            elif method == ForecastMethod.PRIOR_YEAR_GROWTH:
                # Prior year with growth rate
                if "prior_year" in df.columns:
                    df.loc[remaining_mask, "forecast"] = df.loc[remaining_mask, "prior_year"] * (1 + rate / 100)
                else:
                    df.loc[remaining_mask, "forecast"] = df.loc[remaining_mask, budget_column] * (1 + rate / 100)

            else:
                # Default to budget
                df.loc[remaining_mask, "forecast"] = df.loc[remaining_mask, budget_column]

            # Apply adjustment factors
            if adjustment_factors:
                for period, factor in adjustment_factors.items():
                    period_mask = df[period_column] == period
                    if period_mask.any():
                        df.loc[period_mask, "forecast"] = df.loc[period_mask, "forecast"] * factor

            # Build periods
            periods = []
            for _, row in df.iterrows():
                period = row[period_column]
                is_actual = period <= as_of_period
                periods.append(ForecastPeriod(
                    period=str(period),
                    actual=row[actual_column] if is_actual else None,
                    budget=row[budget_column],
                    forecast=row["forecast"],
                    is_actual=is_actual,
                ))

            # Calculate summary
            ytd_actual = df.loc[ytd_mask, actual_column].sum()
            remaining_forecast = df.loc[remaining_mask, "forecast"].sum()
            full_year_outlook = ytd_actual + remaining_forecast
            full_year_budget = df[budget_column].sum()

            summary = ForecastSummary(
                fiscal_year=str(as_of_period[:4]),
                as_of_date=as_of_period,
                ytd_actual=ytd_actual,
                ytd_budget=df.loc[ytd_mask, budget_column].sum(),
                ytd_variance=ytd_actual - df.loc[ytd_mask, budget_column].sum(),
                remaining_forecast=remaining_forecast,
                full_year_outlook=full_year_outlook,
                full_year_budget=full_year_budget,
                full_year_variance=full_year_outlook - full_year_budget,
            )

            return ForecastWorkflowResult(
                success=True,
                message=f"Rolling forecast updated using {method.value} method",
                summary=summary,
                periods=periods,
                data={"method": method.value, "growth_rate": rate},
            )

        except Exception as e:
            logger.error(f"Failed to update rolling forecast: {e}")
            return ForecastWorkflowResult(
                success=False,
                message=f"Failed to update rolling forecast: {str(e)}",
                errors=[str(e)],
            )

    def create_scenario(
        self,
        scenario_id: str,
        name: str,
        scenario_type: ScenarioType,
        base_periods: List[ForecastPeriod],
        assumptions: Optional[List[ForecastAssumption]] = None,
        description: str = "",
        created_by: str = "",
    ) -> ForecastWorkflowResult:
        """
        Create a new forecast scenario.

        Args:
            scenario_id: Unique identifier.
            name: Scenario name.
            scenario_type: Type of scenario.
            base_periods: Base forecast periods to start from.
            assumptions: Assumptions for this scenario.
            description: Scenario description.
            created_by: Creator identifier.

        Returns:
            ForecastWorkflowResult with created scenario.
        """
        try:
            # Copy base periods
            periods = [
                ForecastPeriod(
                    period=p.period,
                    actual=p.actual,
                    budget=p.budget,
                    forecast=p.forecast,
                    prior_year=p.prior_year,
                    is_actual=p.is_actual,
                    is_locked=p.is_locked,
                )
                for p in base_periods
            ]

            scenario = Scenario(
                scenario_id=scenario_id,
                name=name,
                scenario_type=scenario_type,
                description=description,
                assumptions=assumptions or [],
                periods=periods,
                total_forecast=sum(p.forecast or 0 for p in periods),
                created_by=created_by,
            )

            # Store scenario
            self._scenarios[scenario_id] = scenario

            # Set as base if it's the first base scenario
            if scenario_type == ScenarioType.BASE and not self._base_scenario_id:
                self._base_scenario_id = scenario_id

            return ForecastWorkflowResult(
                success=True,
                message=f"Created scenario '{name}'",
                scenario=scenario,
            )

        except Exception as e:
            logger.error(f"Failed to create scenario: {e}")
            return ForecastWorkflowResult(
                success=False,
                message=f"Failed to create scenario: {str(e)}",
                errors=[str(e)],
            )

    def model_scenario(
        self,
        scenario_id: str,
        adjustments: Dict[str, Any],
    ) -> ForecastWorkflowResult:
        """
        Apply adjustments to model a scenario.

        Args:
            scenario_id: Scenario to modify.
            adjustments: Dictionary of adjustments:
                - percent_change: Overall % change
                - period_changes: Dict[period, value]
                - category_changes: Dict[category, %]

        Returns:
            ForecastWorkflowResult with modeled scenario.
        """
        try:
            if scenario_id not in self._scenarios:
                return ForecastWorkflowResult(
                    success=False,
                    message=f"Scenario '{scenario_id}' not found",
                    errors=[f"Scenario not found: {scenario_id}"],
                )

            scenario = self._scenarios[scenario_id]

            # Apply percent change
            if "percent_change" in adjustments:
                pct = adjustments["percent_change"]
                for period in scenario.periods:
                    if not period.is_actual and not period.is_locked:
                        period.forecast = (period.forecast or 0) * (1 + pct / 100)

            # Apply period-specific changes
            if "period_changes" in adjustments:
                for period_name, value in adjustments["period_changes"].items():
                    for period in scenario.periods:
                        if period.period == period_name and not period.is_actual and not period.is_locked:
                            period.forecast = value

            # Add assumptions
            if "assumptions" in adjustments:
                for assumption_data in adjustments["assumptions"]:
                    scenario.assumptions.append(ForecastAssumption(**assumption_data))

            # Recalculate totals
            scenario.total_forecast = sum(p.forecast or 0 for p in scenario.periods)

            # Calculate variance to base
            if self._base_scenario_id and self._base_scenario_id in self._scenarios:
                base = self._scenarios[self._base_scenario_id]
                scenario.variance_to_base = scenario.total_forecast - base.total_forecast
                scenario.variance_percent_to_base = (
                    (scenario.variance_to_base / base.total_forecast * 100)
                    if base.total_forecast else 0.0
                )

            return ForecastWorkflowResult(
                success=True,
                message=f"Applied adjustments to scenario '{scenario.name}'",
                scenario=scenario,
            )

        except Exception as e:
            logger.error(f"Failed to model scenario: {e}")
            return ForecastWorkflowResult(
                success=False,
                message=f"Failed to model scenario: {str(e)}",
                errors=[str(e)],
            )

    def compare_scenarios(
        self,
        scenario_ids: Optional[List[str]] = None,
    ) -> ForecastWorkflowResult:
        """
        Compare multiple forecast scenarios.

        Args:
            scenario_ids: Scenarios to compare. If None, compare all.

        Returns:
            ForecastWorkflowResult with comparison data.
        """
        try:
            if scenario_ids:
                scenarios = [self._scenarios[sid] for sid in scenario_ids if sid in self._scenarios]
            else:
                scenarios = list(self._scenarios.values())

            if not scenarios:
                return ForecastWorkflowResult(
                    success=False,
                    message="No scenarios found to compare",
                    errors=["No scenarios available"],
                )

            # Get base scenario for comparison
            base = None
            if self._base_scenario_id and self._base_scenario_id in self._scenarios:
                base = self._scenarios[self._base_scenario_id]

            # Calculate variance to base for each scenario
            for scenario in scenarios:
                if base and scenario.scenario_id != base.scenario_id:
                    scenario.variance_to_base = scenario.total_forecast - base.total_forecast
                    scenario.variance_percent_to_base = (
                        (scenario.variance_to_base / base.total_forecast * 100)
                        if base.total_forecast else 0.0
                    )

            # Build comparison data
            comparison = {
                "scenarios": [s.name for s in scenarios],
                "totals": {s.name: s.total_forecast for s in scenarios},
                "variance_to_base": {s.name: s.variance_to_base for s in scenarios},
                "variance_percent": {s.name: s.variance_percent_to_base for s in scenarios},
            }

            # Period-by-period comparison
            if scenarios:
                period_comparison = []
                for i, period in enumerate(scenarios[0].periods):
                    period_data = {"period": period.period}
                    for scenario in scenarios:
                        if i < len(scenario.periods):
                            period_data[scenario.name] = scenario.periods[i].forecast
                    period_comparison.append(period_data)
                comparison["by_period"] = period_comparison

            return ForecastWorkflowResult(
                success=True,
                message=f"Compared {len(scenarios)} scenarios",
                scenarios=scenarios,
                data=comparison,
            )

        except Exception as e:
            logger.error(f"Failed to compare scenarios: {e}")
            return ForecastWorkflowResult(
                success=False,
                message=f"Failed to compare scenarios: {str(e)}",
                errors=[str(e)],
            )

    def generate_forecast_report(
        self,
        scenario_id: Optional[str] = None,
        include_assumptions: bool = True,
        include_variance: bool = True,
    ) -> str:
        """
        Generate a markdown forecast report.

        Args:
            scenario_id: Specific scenario to report. If None, use base.
            include_assumptions: Include assumption details.
            include_variance: Include variance analysis.

        Returns:
            Markdown formatted report.
        """
        try:
            # Get scenario
            if scenario_id and scenario_id in self._scenarios:
                scenario = self._scenarios[scenario_id]
            elif self._base_scenario_id:
                scenario = self._scenarios.get(self._base_scenario_id)
            else:
                return "No scenarios available for reporting."

            if not scenario:
                return "Scenario not found."

            lines = []

            # Header
            lines.append(f"# Forecast Report: {scenario.name}")
            lines.append("")
            lines.append(f"**Scenario Type:** {scenario.scenario_type.value.title()}")
            lines.append(f"**Created:** {scenario.created_at.strftime('%Y-%m-%d')}")
            if scenario.created_by:
                lines.append(f"**Created By:** {scenario.created_by}")
            lines.append("")

            if scenario.description:
                lines.append(f"*{scenario.description}*")
                lines.append("")

            # Summary
            lines.append("## Summary")
            lines.append("")
            lines.append(f"**Full Year Forecast:** ${scenario.total_forecast:,.0f}")

            if scenario.variance_to_base:
                direction = "above" if scenario.variance_to_base > 0 else "below"
                lines.append(f"**Variance to Base:** ${abs(scenario.variance_to_base):,.0f} {direction} ({abs(scenario.variance_percent_to_base):.1f}%)")
            lines.append("")

            # Period Detail
            lines.append("## Period Detail")
            lines.append("")
            lines.append("| Period | Actual | Budget | Forecast | Variance |")
            lines.append("|--------|--------|--------|----------|----------|")

            for period in scenario.periods:
                actual_str = f"${period.actual:,.0f}" if period.actual is not None else "-"
                budget_str = f"${period.budget:,.0f}" if period.budget is not None else "-"
                forecast_str = f"${period.forecast:,.0f}" if period.forecast is not None else "-"

                if period.budget and period.forecast:
                    variance = period.forecast - period.budget
                    variance_str = f"${variance:,.0f}"
                else:
                    variance_str = "-"

                lines.append(f"| {period.period} | {actual_str} | {budget_str} | {forecast_str} | {variance_str} |")

            lines.append("")

            # Assumptions
            if include_assumptions and scenario.assumptions:
                lines.append("## Key Assumptions")
                lines.append("")
                for assumption in scenario.assumptions:
                    unit = assumption.unit or ""
                    lines.append(f"- **{assumption.name}:** {assumption.value}{unit} ({assumption.confidence_level} confidence)")
                    if assumption.description:
                        lines.append(f"  - {assumption.description}")
                lines.append("")

            # Variance to base
            if include_variance and scenario.variance_to_base:
                lines.append("## Variance Analysis")
                lines.append("")
                lines.append(f"Compared to base scenario, this {scenario.scenario_type.value} scenario projects:")
                lines.append("")
                if scenario.variance_to_base > 0:
                    lines.append(f"- **${scenario.variance_to_base:,.0f}** higher forecast (+{scenario.variance_percent_to_base:.1f}%)")
                else:
                    lines.append(f"- **${abs(scenario.variance_to_base):,.0f}** lower forecast ({scenario.variance_percent_to_base:.1f}%)")
                lines.append("")

            return "\n".join(lines)

        except Exception as e:
            logger.error(f"Failed to generate forecast report: {e}")
            return f"Error generating report: {str(e)}"

    def get_scenarios(self) -> List[Scenario]:
        """Get all scenarios."""
        return list(self._scenarios.values())

    def get_scenario(self, scenario_id: str) -> Optional[Scenario]:
        """Get a specific scenario."""
        return self._scenarios.get(scenario_id)

    def delete_scenario(self, scenario_id: str) -> bool:
        """Delete a scenario."""
        if scenario_id in self._scenarios:
            del self._scenarios[scenario_id]
            if self._base_scenario_id == scenario_id:
                self._base_scenario_id = None
            return True
        return False

    def set_base_scenario(self, scenario_id: str) -> bool:
        """Set the base scenario for comparisons."""
        if scenario_id in self._scenarios:
            self._base_scenario_id = scenario_id
            return True
        return False
