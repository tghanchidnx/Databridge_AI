"""
Variance Analysis for DataBridge AI Researcher Analytics Engine.

Provides budget vs actual (BvA) variance analysis with decomposition.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Union
from enum import Enum

import pandas as pd
import numpy as np


class VarianceType(str, Enum):
    """Type of variance."""
    FAVORABLE = "favorable"
    UNFAVORABLE = "unfavorable"
    NEUTRAL = "neutral"


class VarianceCategory(str, Enum):
    """Category of variance for decomposition."""
    PRICE = "price"
    VOLUME = "volume"
    MIX = "mix"
    RATE = "rate"
    EFFICIENCY = "efficiency"
    OTHER = "other"


@dataclass
class VarianceItem:
    """A single variance item."""

    dimension: str
    dimension_value: str
    actual: float
    budget: float
    variance: float
    variance_percent: float
    variance_type: VarianceType
    materiality_flag: bool = False
    rank: int = 0
    contribution_percent: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "dimension": self.dimension,
            "dimension_value": self.dimension_value,
            "actual": round(self.actual, 2),
            "budget": round(self.budget, 2),
            "variance": round(self.variance, 2),
            "variance_percent": round(self.variance_percent, 2),
            "variance_type": self.variance_type.value,
            "materiality_flag": self.materiality_flag,
            "rank": self.rank,
            "contribution_percent": round(self.contribution_percent, 2),
        }


@dataclass
class VarianceDecomposition:
    """Decomposition of variance into components."""

    total_variance: float
    components: Dict[VarianceCategory, float] = field(default_factory=dict)
    explanation: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "total_variance": round(self.total_variance, 2),
            "components": {k.value: round(v, 2) for k, v in self.components.items()},
            "explanation": self.explanation,
        }


@dataclass
class VarianceResult:
    """Result of variance analysis."""

    success: bool
    total_actual: float = 0.0
    total_budget: float = 0.0
    total_variance: float = 0.0
    total_variance_percent: float = 0.0
    overall_variance_type: VarianceType = VarianceType.NEUTRAL
    items: List[VarianceItem] = field(default_factory=list)
    decomposition: Optional[VarianceDecomposition] = None
    top_drivers: List[VarianceItem] = field(default_factory=list)
    summary: str = ""
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "totals": {
                "actual": round(self.total_actual, 2),
                "budget": round(self.total_budget, 2),
                "variance": round(self.total_variance, 2),
                "variance_percent": round(self.total_variance_percent, 2),
                "variance_type": self.overall_variance_type.value,
            },
            "items": [i.to_dict() for i in self.items],
            "decomposition": self.decomposition.to_dict() if self.decomposition else None,
            "top_drivers": [d.to_dict() for d in self.top_drivers],
            "summary": self.summary,
            "errors": self.errors,
        }

    def get_favorable(self) -> List[VarianceItem]:
        """Get favorable variance items."""
        return [i for i in self.items if i.variance_type == VarianceType.FAVORABLE]

    def get_unfavorable(self) -> List[VarianceItem]:
        """Get unfavorable variance items."""
        return [i for i in self.items if i.variance_type == VarianceType.UNFAVORABLE]


class VarianceAnalyzer:
    """
    Variance analyzer for budget vs actual comparison.

    Provides:
    - Simple variance calculation
    - Variance by dimension
    - Variance decomposition (price/volume/mix)
    - Top driver identification
    - Materiality flagging
    """

    def __init__(
        self,
        materiality_threshold: float = 0.05,
        favorable_direction: str = "positive",
        round_decimals: int = 2,
    ):
        """
        Initialize the variance analyzer.

        Args:
            materiality_threshold: Percentage threshold for materiality (0.05 = 5%).
            favorable_direction: Direction considered favorable ("positive" for revenue, "negative" for cost).
            round_decimals: Decimal places for rounding.
        """
        self.materiality_threshold = materiality_threshold
        self.favorable_direction = favorable_direction
        self.round_decimals = round_decimals

    def analyze(
        self,
        actual: Union[float, pd.Series, pd.DataFrame],
        budget: Union[float, pd.Series, pd.DataFrame, None] = None,
        actual_column: Optional[str] = None,
        budget_column: Optional[str] = None,
        dimension_column: Optional[str] = None,
        top_n_drivers: int = 5,
    ) -> VarianceResult:
        """
        Analyze variance between actual and budget.

        Args:
            actual: Actual values (scalar, Series, or DataFrame containing both actual and budget).
            budget: Budget values (scalar, Series, or None if DataFrame contains both).
            actual_column: Column name for actual values (DataFrame only).
            budget_column: Column name for budget values (DataFrame only).
            dimension_column: Column to group by for dimensional analysis.
            top_n_drivers: Number of top drivers to identify.

        Returns:
            VarianceResult with analysis results.
        """
        try:
            # Handle different input types
            if isinstance(actual, (int, float)) and isinstance(budget, (int, float)):
                return self._analyze_scalar(float(actual), float(budget))

            elif isinstance(actual, pd.Series) and isinstance(budget, pd.Series):
                return self._analyze_series(actual, budget, top_n_drivers)

            elif isinstance(actual, pd.DataFrame):
                # DataFrame can contain both actual and budget columns
                if actual_column is None or budget_column is None:
                    return VarianceResult(
                        success=False,
                        errors=["actual_column and budget_column required for DataFrame input"],
                    )
                return self._analyze_dataframe(
                    actual,
                    actual_column,
                    budget_column,
                    dimension_column,
                    top_n_drivers,
                )

            else:
                return VarianceResult(
                    success=False,
                    errors=["Unsupported input types"],
                )

        except Exception as e:
            return VarianceResult(
                success=False,
                errors=[str(e)],
            )

    def _analyze_scalar(self, actual: float, budget: float) -> VarianceResult:
        """Analyze variance for scalar values."""
        variance = actual - budget
        variance_percent = (variance / budget * 100) if budget != 0 else 0
        variance_type = self._determine_variance_type(variance)

        return VarianceResult(
            success=True,
            total_actual=actual,
            total_budget=budget,
            total_variance=variance,
            total_variance_percent=variance_percent,
            overall_variance_type=variance_type,
            summary=self._generate_summary(actual, budget, variance, variance_percent, variance_type),
        )

    def _analyze_series(
        self,
        actual: pd.Series,
        budget: pd.Series,
        top_n_drivers: int,
    ) -> VarianceResult:
        """Analyze variance for Series values."""
        items = []
        total_actual = actual.sum()
        total_budget = budget.sum()
        total_variance = total_actual - total_budget

        for idx in actual.index:
            if idx not in budget.index:
                continue

            act_val = float(actual[idx])
            bud_val = float(budget[idx])
            var_val = act_val - bud_val
            var_pct = (var_val / bud_val * 100) if bud_val != 0 else 0
            var_type = self._determine_variance_type(var_val)

            # Calculate contribution to total variance
            contribution = (var_val / total_variance * 100) if total_variance != 0 else 0

            items.append(VarianceItem(
                dimension="index",
                dimension_value=str(idx),
                actual=act_val,
                budget=bud_val,
                variance=var_val,
                variance_percent=var_pct,
                variance_type=var_type,
                materiality_flag=abs(var_pct) >= self.materiality_threshold * 100,
                contribution_percent=contribution,
            ))

        # Rank items by absolute variance
        items.sort(key=lambda x: abs(x.variance), reverse=True)
        for i, item in enumerate(items):
            item.rank = i + 1

        # Get top drivers
        top_drivers = items[:top_n_drivers]

        total_variance_percent = (total_variance / total_budget * 100) if total_budget != 0 else 0
        overall_type = self._determine_variance_type(total_variance)

        return VarianceResult(
            success=True,
            total_actual=float(total_actual),
            total_budget=float(total_budget),
            total_variance=float(total_variance),
            total_variance_percent=float(total_variance_percent),
            overall_variance_type=overall_type,
            items=items,
            top_drivers=top_drivers,
            summary=self._generate_summary(
                total_actual, total_budget, total_variance,
                total_variance_percent, overall_type
            ),
        )

    def _analyze_dataframe(
        self,
        df: pd.DataFrame,
        actual_column: str,
        budget_column: str,
        dimension_column: Optional[str],
        top_n_drivers: int,
    ) -> VarianceResult:
        """Analyze variance for DataFrame."""
        if dimension_column:
            # Group by dimension
            grouped = df.groupby(dimension_column).agg({
                actual_column: "sum",
                budget_column: "sum",
            }).reset_index()

            items = []
            total_actual = grouped[actual_column].sum()
            total_budget = grouped[budget_column].sum()
            total_variance = total_actual - total_budget

            for _, row in grouped.iterrows():
                act_val = float(row[actual_column])
                bud_val = float(row[budget_column])
                var_val = act_val - bud_val
                var_pct = (var_val / bud_val * 100) if bud_val != 0 else 0
                var_type = self._determine_variance_type(var_val)
                contribution = (var_val / total_variance * 100) if total_variance != 0 else 0

                items.append(VarianceItem(
                    dimension=dimension_column,
                    dimension_value=str(row[dimension_column]),
                    actual=act_val,
                    budget=bud_val,
                    variance=var_val,
                    variance_percent=var_pct,
                    variance_type=var_type,
                    materiality_flag=abs(var_pct) >= self.materiality_threshold * 100,
                    contribution_percent=contribution,
                ))

            # Rank by absolute variance
            items.sort(key=lambda x: abs(x.variance), reverse=True)
            for i, item in enumerate(items):
                item.rank = i + 1

            top_drivers = items[:top_n_drivers]

        else:
            # No grouping, analyze totals
            total_actual = df[actual_column].sum()
            total_budget = df[budget_column].sum()
            total_variance = total_actual - total_budget
            items = []
            top_drivers = []

        total_variance_percent = (total_variance / total_budget * 100) if total_budget != 0 else 0
        overall_type = self._determine_variance_type(total_variance)

        return VarianceResult(
            success=True,
            total_actual=float(total_actual),
            total_budget=float(total_budget),
            total_variance=float(total_variance),
            total_variance_percent=float(total_variance_percent),
            overall_variance_type=overall_type,
            items=items,
            top_drivers=top_drivers,
            summary=self._generate_summary(
                total_actual, total_budget, total_variance,
                total_variance_percent, overall_type
            ),
        )

    def decompose_variance(
        self,
        actual_price: float,
        actual_volume: float,
        budget_price: float,
        budget_volume: float,
    ) -> VarianceDecomposition:
        """
        Decompose variance into price and volume components.

        Uses the standard decomposition:
        - Price variance = (Actual Price - Budget Price) × Actual Volume
        - Volume variance = (Actual Volume - Budget Volume) × Budget Price
        - Mix variance = remaining variance

        Args:
            actual_price: Actual unit price.
            actual_volume: Actual volume/quantity.
            budget_price: Budget unit price.
            budget_volume: Budget volume/quantity.

        Returns:
            VarianceDecomposition with component breakdown.
        """
        actual_total = actual_price * actual_volume
        budget_total = budget_price * budget_volume
        total_variance = actual_total - budget_total

        # Price variance: difference in price × actual volume
        price_variance = (actual_price - budget_price) * actual_volume

        # Volume variance: difference in volume × budget price
        volume_variance = (actual_volume - budget_volume) * budget_price

        # Mix variance: residual
        mix_variance = total_variance - price_variance - volume_variance

        components = {
            VarianceCategory.PRICE: price_variance,
            VarianceCategory.VOLUME: volume_variance,
        }

        if abs(mix_variance) > 0.01:
            components[VarianceCategory.MIX] = mix_variance

        # Generate explanation
        explanations = []
        if abs(price_variance) > 0.01:
            direction = "higher" if price_variance > 0 else "lower"
            explanations.append(f"Price variance of ${abs(price_variance):,.2f} due to {direction} unit price")
        if abs(volume_variance) > 0.01:
            direction = "higher" if volume_variance > 0 else "lower"
            explanations.append(f"Volume variance of ${abs(volume_variance):,.2f} due to {direction} volume")
        if abs(mix_variance) > 0.01:
            explanations.append(f"Mix variance of ${abs(mix_variance):,.2f}")

        return VarianceDecomposition(
            total_variance=total_variance,
            components=components,
            explanation="; ".join(explanations) if explanations else "No significant variance components",
        )

    def _determine_variance_type(self, variance: float) -> VarianceType:
        """Determine if variance is favorable, unfavorable, or neutral."""
        if abs(variance) < 0.01:
            return VarianceType.NEUTRAL

        if self.favorable_direction == "positive":
            return VarianceType.FAVORABLE if variance > 0 else VarianceType.UNFAVORABLE
        else:  # favorable_direction == "negative"
            return VarianceType.FAVORABLE if variance < 0 else VarianceType.UNFAVORABLE

    def _generate_summary(
        self,
        actual: float,
        budget: float,
        variance: float,
        variance_percent: float,
        variance_type: VarianceType,
    ) -> str:
        """Generate human-readable summary."""
        parts = []

        # Overall performance
        type_desc = "on target" if variance_type == VarianceType.NEUTRAL else variance_type.value
        parts.append(f"Overall performance is {type_desc}")

        # Variance description
        direction = "above" if variance > 0 else "below"
        parts.append(f"with actual of ${actual:,.2f} {direction} budget of ${budget:,.2f}")

        # Variance amount and percent
        parts.append(f"(variance: ${variance:,.2f}, {variance_percent:.1f}%)")

        return " ".join(parts) + "."

    def generate_commentary(
        self,
        result: VarianceResult,
        include_drivers: bool = True,
    ) -> str:
        """
        Generate executive-ready variance commentary.

        Args:
            result: VarianceResult from analyze().
            include_drivers: Whether to include top driver details.

        Returns:
            Formatted commentary string.
        """
        lines = []

        # Overall summary
        lines.append(f"## Variance Summary")
        lines.append("")
        lines.append(f"Total actual of ${result.total_actual:,.2f} vs budget of ${result.total_budget:,.2f}")
        lines.append(f"resulting in a {result.overall_variance_type.value} variance of ${result.total_variance:,.2f} ({result.total_variance_percent:.1f}%)")
        lines.append("")

        # Top drivers
        if include_drivers and result.top_drivers:
            lines.append("### Top Variance Drivers")
            lines.append("")
            for driver in result.top_drivers:
                var_type = "F" if driver.variance_type == VarianceType.FAVORABLE else "U"
                lines.append(
                    f"- **{driver.dimension_value}**: ${driver.variance:,.2f} ({var_type}) "
                    f"- {driver.contribution_percent:.1f}% of total variance"
                )
            lines.append("")

        # Decomposition if available
        if result.decomposition:
            lines.append("### Variance Decomposition")
            lines.append("")
            for category, amount in result.decomposition.components.items():
                lines.append(f"- {category.value.title()}: ${amount:,.2f}")
            lines.append("")

        return "\n".join(lines)
