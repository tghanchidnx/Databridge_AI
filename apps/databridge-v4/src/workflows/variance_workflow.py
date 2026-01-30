"""
Variance Analysis Workflow for DataBridge Analytics V4.

Automates budget vs actual (BvA) and prior year variance analysis
with commentary generation.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Union
from enum import Enum
from datetime import datetime
import logging

import pandas as pd

from ..insights.variance import VarianceAnalyzer, VarianceResult, VarianceType


logger = logging.getLogger(__name__)


class VarianceComparisonType(str, Enum):
    """Type of variance comparison."""
    BUDGET = "budget"  # Actual vs Budget
    PRIOR_YEAR = "prior_year"  # Actual vs Prior Year
    PRIOR_PERIOD = "prior_period"  # Actual vs Prior Period
    FORECAST = "forecast"  # Actual vs Forecast
    PLAN = "plan"  # Actual vs Plan


@dataclass
class VarianceDriver:
    """A driver of variance."""

    dimension: str
    dimension_value: str
    variance_amount: float
    variance_percent: float
    contribution_percent: float
    variance_type: VarianceType
    explanation: str = ""
    action_items: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "dimension": self.dimension,
            "dimension_value": self.dimension_value,
            "variance_amount": round(self.variance_amount, 2),
            "variance_percent": round(self.variance_percent, 2),
            "contribution_percent": round(self.contribution_percent, 2),
            "variance_type": self.variance_type.value,
            "explanation": self.explanation,
            "action_items": self.action_items,
        }


@dataclass
class VarianceCommentary:
    """Generated commentary for variance analysis."""

    executive_summary: str = ""
    key_findings: List[str] = field(default_factory=list)
    favorable_highlights: List[str] = field(default_factory=list)
    unfavorable_concerns: List[str] = field(default_factory=list)
    action_items: List[str] = field(default_factory=list)
    detailed_narrative: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "executive_summary": self.executive_summary,
            "key_findings": self.key_findings,
            "favorable_highlights": self.favorable_highlights,
            "unfavorable_concerns": self.unfavorable_concerns,
            "action_items": self.action_items,
            "detailed_narrative": self.detailed_narrative,
        }

    def to_markdown(self) -> str:
        """Convert to markdown format."""
        lines = []

        lines.append("## Executive Summary")
        lines.append("")
        lines.append(self.executive_summary)
        lines.append("")

        if self.key_findings:
            lines.append("## Key Findings")
            lines.append("")
            for finding in self.key_findings:
                lines.append(f"- {finding}")
            lines.append("")

        if self.favorable_highlights:
            lines.append("### Favorable Variances")
            lines.append("")
            for highlight in self.favorable_highlights:
                lines.append(f"- {highlight}")
            lines.append("")

        if self.unfavorable_concerns:
            lines.append("### Areas of Concern")
            lines.append("")
            for concern in self.unfavorable_concerns:
                lines.append(f"- {concern}")
            lines.append("")

        if self.action_items:
            lines.append("## Recommended Actions")
            lines.append("")
            for action in self.action_items:
                lines.append(f"- [ ] {action}")
            lines.append("")

        return "\n".join(lines)


@dataclass
class VarianceWorkflowResult:
    """Result from variance workflow operations."""

    success: bool
    message: str = ""
    comparison_type: Optional[VarianceComparisonType] = None
    period: str = ""
    variance_result: Optional[VarianceResult] = None
    drivers: List[VarianceDriver] = field(default_factory=list)
    commentary: Optional[VarianceCommentary] = None
    data: Any = None
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "message": self.message,
            "comparison_type": self.comparison_type.value if self.comparison_type else None,
            "period": self.period,
            "variance_result": self.variance_result.to_dict() if self.variance_result else None,
            "drivers": [d.to_dict() for d in self.drivers],
            "commentary": self.commentary.to_dict() if self.commentary else None,
            "data": self.data,
            "errors": self.errors,
        }


class VarianceWorkflow:
    """
    Orchestrates variance analysis workflows.

    Provides:
    - Budget vs Actual analysis
    - Prior year comparison
    - Multi-dimensional variance analysis
    - Automated commentary generation
    - Driver identification
    """

    def __init__(
        self,
        materiality_threshold: float = 0.05,
        top_drivers_count: int = 5,
        generate_commentary: bool = True,
    ):
        """
        Initialize the variance workflow.

        Args:
            materiality_threshold: Threshold for materiality (0.05 = 5%).
            top_drivers_count: Number of top drivers to identify.
            generate_commentary: Whether to auto-generate commentary.
        """
        self.materiality_threshold = materiality_threshold
        self.top_drivers_count = top_drivers_count
        self.generate_commentary = generate_commentary
        self._analyzer = VarianceAnalyzer(
            materiality_threshold=materiality_threshold,
        )

    def analyze_budget_variance(
        self,
        data: pd.DataFrame,
        actual_column: str,
        budget_column: str,
        dimension_columns: Optional[List[str]] = None,
        period: str = "",
        favorable_direction: str = "positive",
    ) -> VarianceWorkflowResult:
        """
        Analyze budget vs actual variance.

        Args:
            data: DataFrame with actual and budget data.
            actual_column: Column name for actual values.
            budget_column: Column name for budget values.
            dimension_columns: Columns to analyze by.
            period: Period label (e.g., "January 2024").
            favorable_direction: Direction for favorable variance.

        Returns:
            VarianceWorkflowResult with analysis.
        """
        try:
            self._analyzer.favorable_direction = favorable_direction

            # Overall variance
            result = self._analyzer.analyze(
                actual=data,
                actual_column=actual_column,
                budget_column=budget_column,
                dimension_column=dimension_columns[0] if dimension_columns else None,
                top_n_drivers=self.top_drivers_count,
            )

            if not result.success:
                return VarianceWorkflowResult(
                    success=False,
                    errors=result.errors,
                )

            # Convert top drivers
            drivers = [
                VarianceDriver(
                    dimension=d.dimension,
                    dimension_value=d.dimension_value,
                    variance_amount=d.variance,
                    variance_percent=d.variance_percent,
                    contribution_percent=d.contribution_percent,
                    variance_type=d.variance_type,
                )
                for d in result.top_drivers
            ]

            # Generate commentary
            commentary = None
            if self.generate_commentary:
                commentary = self._generate_commentary(
                    result=result,
                    comparison_type=VarianceComparisonType.BUDGET,
                    period=period,
                )

            return VarianceWorkflowResult(
                success=True,
                message=f"Budget variance analysis complete for {period}" if period else "Budget variance analysis complete",
                comparison_type=VarianceComparisonType.BUDGET,
                period=period,
                variance_result=result,
                drivers=drivers,
                commentary=commentary,
            )

        except Exception as e:
            logger.error(f"Failed to analyze budget variance: {e}")
            return VarianceWorkflowResult(
                success=False,
                message=f"Failed to analyze budget variance: {str(e)}",
                errors=[str(e)],
            )

    def analyze_prior_year_variance(
        self,
        data: pd.DataFrame,
        current_column: str,
        prior_column: str,
        dimension_columns: Optional[List[str]] = None,
        period: str = "",
    ) -> VarianceWorkflowResult:
        """
        Analyze variance vs prior year.

        Args:
            data: DataFrame with current and prior year data.
            current_column: Column name for current year values.
            prior_column: Column name for prior year values.
            dimension_columns: Columns to analyze by.
            period: Period label.

        Returns:
            VarianceWorkflowResult with analysis.
        """
        try:
            result = self._analyzer.analyze(
                actual=data,
                actual_column=current_column,
                budget_column=prior_column,
                dimension_column=dimension_columns[0] if dimension_columns else None,
                top_n_drivers=self.top_drivers_count,
            )

            if not result.success:
                return VarianceWorkflowResult(
                    success=False,
                    errors=result.errors,
                )

            drivers = [
                VarianceDriver(
                    dimension=d.dimension,
                    dimension_value=d.dimension_value,
                    variance_amount=d.variance,
                    variance_percent=d.variance_percent,
                    contribution_percent=d.contribution_percent,
                    variance_type=d.variance_type,
                )
                for d in result.top_drivers
            ]

            commentary = None
            if self.generate_commentary:
                commentary = self._generate_commentary(
                    result=result,
                    comparison_type=VarianceComparisonType.PRIOR_YEAR,
                    period=period,
                )

            return VarianceWorkflowResult(
                success=True,
                message=f"Prior year variance analysis complete for {period}" if period else "Prior year variance analysis complete",
                comparison_type=VarianceComparisonType.PRIOR_YEAR,
                period=period,
                variance_result=result,
                drivers=drivers,
                commentary=commentary,
            )

        except Exception as e:
            logger.error(f"Failed to analyze prior year variance: {e}")
            return VarianceWorkflowResult(
                success=False,
                message=f"Failed to analyze prior year variance: {str(e)}",
                errors=[str(e)],
            )

    def identify_variance_drivers(
        self,
        data: pd.DataFrame,
        actual_column: str,
        budget_column: str,
        dimension_columns: List[str],
        min_contribution: float = 0.05,
    ) -> VarianceWorkflowResult:
        """
        Identify key drivers of variance across dimensions.

        Args:
            data: DataFrame with actual and budget data.
            actual_column: Column name for actual values.
            budget_column: Column name for budget values.
            dimension_columns: Columns to analyze for drivers.
            min_contribution: Minimum contribution % to include.

        Returns:
            VarianceWorkflowResult with identified drivers.
        """
        try:
            all_drivers = []

            for dimension in dimension_columns:
                result = self._analyzer.analyze(
                    actual=data,
                    actual_column=actual_column,
                    budget_column=budget_column,
                    dimension_column=dimension,
                    top_n_drivers=10,
                )

                if result.success:
                    for d in result.top_drivers:
                        if abs(d.contribution_percent) >= min_contribution * 100:
                            all_drivers.append(VarianceDriver(
                                dimension=d.dimension,
                                dimension_value=d.dimension_value,
                                variance_amount=d.variance,
                                variance_percent=d.variance_percent,
                                contribution_percent=d.contribution_percent,
                                variance_type=d.variance_type,
                            ))

            # Sort by absolute contribution
            all_drivers.sort(key=lambda x: abs(x.contribution_percent), reverse=True)

            return VarianceWorkflowResult(
                success=True,
                message=f"Identified {len(all_drivers)} variance drivers",
                drivers=all_drivers[:self.top_drivers_count * 2],  # Return more for multi-dimensional
                data={"dimensions_analyzed": dimension_columns},
            )

        except Exception as e:
            logger.error(f"Failed to identify variance drivers: {e}")
            return VarianceWorkflowResult(
                success=False,
                message=f"Failed to identify drivers: {str(e)}",
                errors=[str(e)],
            )

    def decompose_variance(
        self,
        actual_price: float,
        actual_volume: float,
        budget_price: float,
        budget_volume: float,
    ) -> VarianceWorkflowResult:
        """
        Decompose variance into price/volume/mix components.

        Args:
            actual_price: Actual unit price.
            actual_volume: Actual volume/quantity.
            budget_price: Budget unit price.
            budget_volume: Budget volume/quantity.

        Returns:
            VarianceWorkflowResult with decomposition.
        """
        try:
            decomposition = self._analyzer.decompose_variance(
                actual_price=actual_price,
                actual_volume=actual_volume,
                budget_price=budget_price,
                budget_volume=budget_volume,
            )

            return VarianceWorkflowResult(
                success=True,
                message="Variance decomposition complete",
                data={
                    "decomposition": decomposition.to_dict(),
                    "explanation": decomposition.explanation,
                },
            )

        except Exception as e:
            logger.error(f"Failed to decompose variance: {e}")
            return VarianceWorkflowResult(
                success=False,
                message=f"Failed to decompose variance: {str(e)}",
                errors=[str(e)],
            )

    def _generate_commentary(
        self,
        result: VarianceResult,
        comparison_type: VarianceComparisonType,
        period: str = "",
    ) -> VarianceCommentary:
        """Generate variance commentary."""
        comparison_label = {
            VarianceComparisonType.BUDGET: "budget",
            VarianceComparisonType.PRIOR_YEAR: "prior year",
            VarianceComparisonType.PRIOR_PERIOD: "prior period",
            VarianceComparisonType.FORECAST: "forecast",
            VarianceComparisonType.PLAN: "plan",
        }.get(comparison_type, "comparison")

        # Executive summary
        direction = "above" if result.total_variance > 0 else "below"
        var_type = result.overall_variance_type.value

        exec_summary = (
            f"For {period if period else 'the period'}, actual results of ${result.total_actual:,.0f} "
            f"came in {abs(result.total_variance_percent):.1f}% {direction} {comparison_label} of ${result.total_budget:,.0f}, "
            f"representing a {var_type} variance of ${abs(result.total_variance):,.0f}."
        )

        # Key findings
        key_findings = []
        if result.top_drivers:
            top_driver = result.top_drivers[0]
            key_findings.append(
                f"The largest driver was {top_driver.dimension_value} with a variance of ${top_driver.variance:,.0f} "
                f"({top_driver.contribution_percent:.1f}% of total variance)"
            )

        favorable = result.get_favorable()
        unfavorable = result.get_unfavorable()

        if favorable:
            key_findings.append(f"{len(favorable)} items showed favorable variance")
        if unfavorable:
            key_findings.append(f"{len(unfavorable)} items showed unfavorable variance")

        # Highlights and concerns
        favorable_highlights = [
            f"{d.dimension_value}: ${d.variance:,.0f} favorable ({d.variance_percent:.1f}%)"
            for d in favorable[:3]
        ]

        unfavorable_concerns = [
            f"{d.dimension_value}: ${abs(d.variance):,.0f} unfavorable ({abs(d.variance_percent):.1f}%)"
            for d in unfavorable[:3]
        ]

        # Action items
        action_items = []
        for concern in unfavorable[:2]:
            action_items.append(f"Review {concern.dimension_value} for corrective actions")

        # Detailed narrative
        detailed = self._analyzer.generate_commentary(result, include_drivers=True)

        return VarianceCommentary(
            executive_summary=exec_summary,
            key_findings=key_findings,
            favorable_highlights=favorable_highlights,
            unfavorable_concerns=unfavorable_concerns,
            action_items=action_items,
            detailed_narrative=detailed,
        )
