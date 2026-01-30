"""
Trend Analysis for DataBridge AI V4 Analytics Engine.

Provides trend detection and analysis for time series data.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Union, Tuple
from enum import Enum

import pandas as pd
import numpy as np

try:
    from scipy import stats
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False


class TrendDirection(str, Enum):
    """Direction of a trend."""
    INCREASING = "increasing"
    DECREASING = "decreasing"
    STABLE = "stable"
    VOLATILE = "volatile"


class TrendStrength(str, Enum):
    """Strength of a trend."""
    STRONG = "strong"
    MODERATE = "moderate"
    WEAK = "weak"
    NONE = "none"


@dataclass
class TrendInfo:
    """Detailed trend information."""

    direction: TrendDirection
    strength: TrendStrength
    slope: float
    r_squared: float
    p_value: float
    is_significant: bool
    percent_change: float
    start_value: float
    end_value: float
    periods: int

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "direction": self.direction.value,
            "strength": self.strength.value,
            "slope": round(self.slope, 4),
            "r_squared": round(self.r_squared, 4),
            "p_value": round(self.p_value, 4),
            "is_significant": self.is_significant,
            "percent_change": round(self.percent_change, 2),
            "start_value": round(self.start_value, 4),
            "end_value": round(self.end_value, 4),
            "periods": self.periods,
        }


@dataclass
class SeasonalityInfo:
    """Information about seasonality patterns."""

    has_seasonality: bool
    period: Optional[int] = None
    strength: float = 0.0
    peak_periods: List[int] = field(default_factory=list)
    trough_periods: List[int] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "has_seasonality": self.has_seasonality,
            "period": self.period,
            "strength": round(self.strength, 4),
            "peak_periods": self.peak_periods,
            "trough_periods": self.trough_periods,
        }


@dataclass
class ForecastPoint:
    """A single forecast point."""

    period: int
    value: float
    lower_bound: float
    upper_bound: float

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "period": self.period,
            "value": round(self.value, 4),
            "lower_bound": round(self.lower_bound, 4),
            "upper_bound": round(self.upper_bound, 4),
        }


@dataclass
class TrendResult:
    """Result of trend analysis."""

    success: bool
    trend: Optional[TrendInfo] = None
    seasonality: Optional[SeasonalityInfo] = None
    forecast: List[ForecastPoint] = field(default_factory=list)
    summary: str = ""
    statistics: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "trend": self.trend.to_dict() if self.trend else None,
            "seasonality": self.seasonality.to_dict() if self.seasonality else None,
            "forecast": [f.to_dict() for f in self.forecast],
            "summary": self.summary,
            "statistics": self.statistics,
            "errors": self.errors,
        }


class TrendAnalyzer:
    """
    Trend analyzer for time series data.

    Provides:
    - Linear trend detection
    - Trend strength and significance
    - Simple seasonality detection
    - Basic forecasting
    """

    def __init__(
        self,
        min_periods: int = 3,
        significance_level: float = 0.05,
        trend_threshold: float = 0.01,
    ):
        """
        Initialize the trend analyzer.

        Args:
            min_periods: Minimum periods required for analysis.
            significance_level: P-value threshold for significance.
            trend_threshold: Minimum slope to consider a trend.
        """
        self.min_periods = min_periods
        self.significance_level = significance_level
        self.trend_threshold = trend_threshold

    def analyze(
        self,
        data: Union[pd.DataFrame, pd.Series],
        value_column: Optional[str] = None,
        time_column: Optional[str] = None,
        detect_seasonality: bool = True,
        forecast_periods: int = 0,
    ) -> TrendResult:
        """
        Analyze trends in time series data.

        Args:
            data: DataFrame or Series with time series data.
            value_column: Column containing values (for DataFrame).
            time_column: Column containing time periods (for DataFrame).
            detect_seasonality: Whether to detect seasonality.
            forecast_periods: Number of periods to forecast.

        Returns:
            TrendResult with analysis results.
        """
        try:
            # Prepare data
            if isinstance(data, pd.Series):
                values = data.dropna().values
            else:
                if value_column is None:
                    # Try to auto-detect numeric column
                    numeric_cols = data.select_dtypes(include=[np.number]).columns
                    if len(numeric_cols) == 0:
                        return TrendResult(
                            success=False,
                            errors=["No numeric column found"],
                        )
                    value_column = numeric_cols[0]

                if time_column:
                    data = data.sort_values(time_column)

                values = data[value_column].dropna().values

            if len(values) < self.min_periods:
                return TrendResult(
                    success=False,
                    errors=[f"Insufficient data: {len(values)} periods, minimum {self.min_periods}"],
                )

            # Analyze trend
            trend_info = self._analyze_trend(values)

            # Detect seasonality
            seasonality_info = None
            if detect_seasonality and len(values) >= 6:
                seasonality_info = self._detect_seasonality(values)

            # Generate forecast
            forecast = []
            if forecast_periods > 0:
                forecast = self._simple_forecast(values, trend_info, forecast_periods)

            # Generate summary
            summary = self._generate_summary(trend_info, seasonality_info)

            # Statistics
            statistics = {
                "mean": float(np.mean(values)),
                "std": float(np.std(values)),
                "min": float(np.min(values)),
                "max": float(np.max(values)),
                "periods": len(values),
            }

            return TrendResult(
                success=True,
                trend=trend_info,
                seasonality=seasonality_info,
                forecast=forecast,
                summary=summary,
                statistics=statistics,
            )

        except Exception as e:
            return TrendResult(
                success=False,
                errors=[str(e)],
            )

    def _analyze_trend(self, values: np.ndarray) -> TrendInfo:
        """Analyze the linear trend in the data."""
        n = len(values)
        x = np.arange(n)

        # Calculate linear regression
        if SCIPY_AVAILABLE:
            slope, intercept, r_value, p_value, std_err = stats.linregress(x, values)
            r_squared = r_value ** 2
        else:
            # Fallback: simple linear regression
            x_mean = np.mean(x)
            y_mean = np.mean(values)
            slope = np.sum((x - x_mean) * (values - y_mean)) / np.sum((x - x_mean) ** 2)
            intercept = y_mean - slope * x_mean

            # Calculate R-squared
            y_pred = slope * x + intercept
            ss_res = np.sum((values - y_pred) ** 2)
            ss_tot = np.sum((values - y_mean) ** 2)
            r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0

            # Approximate p-value (simplified)
            p_value = 0.05 if abs(r_squared) > 0.5 else 0.5

        # Determine direction
        if abs(slope) < self.trend_threshold:
            direction = TrendDirection.STABLE
        elif slope > 0:
            direction = TrendDirection.INCREASING
        else:
            direction = TrendDirection.DECREASING

        # Check for volatility
        cv = np.std(values) / np.mean(values) if np.mean(values) != 0 else 0
        if cv > 0.5 and r_squared < 0.3:
            direction = TrendDirection.VOLATILE

        # Determine strength
        if r_squared > 0.8:
            strength = TrendStrength.STRONG
        elif r_squared > 0.5:
            strength = TrendStrength.MODERATE
        elif r_squared > 0.2:
            strength = TrendStrength.WEAK
        else:
            strength = TrendStrength.NONE

        # Calculate percent change
        start_value = values[0]
        end_value = values[-1]
        percent_change = ((end_value - start_value) / start_value * 100) if start_value != 0 else 0

        return TrendInfo(
            direction=direction,
            strength=strength,
            slope=float(slope),
            r_squared=float(r_squared),
            p_value=float(p_value),
            is_significant=bool(p_value < self.significance_level),  # Convert numpy bool
            percent_change=float(percent_change),
            start_value=float(start_value),
            end_value=float(end_value),
            periods=n,
        )

    def _detect_seasonality(self, values: np.ndarray) -> SeasonalityInfo:
        """Detect simple seasonality patterns."""
        n = len(values)

        # Try common seasonal periods
        candidate_periods = [4, 7, 12, 52]  # Quarterly, weekly, monthly, yearly
        best_period = None
        best_strength = 0.0

        for period in candidate_periods:
            if n < period * 2:
                continue

            # Calculate autocorrelation at this lag
            if n > period:
                autocorr = np.corrcoef(values[:-period], values[period:])[0, 1]
                if abs(autocorr) > best_strength:
                    best_strength = abs(autocorr)
                    best_period = period

        has_seasonality = bool(best_strength > 0.3)

        # Find peaks and troughs
        peak_periods = []
        trough_periods = []

        if has_seasonality and best_period:
            # Simple peak/trough detection
            seasonal_avg = np.zeros(best_period)
            counts = np.zeros(best_period)

            for i, val in enumerate(values):
                idx = i % best_period
                seasonal_avg[idx] += val
                counts[idx] += 1

            seasonal_avg = seasonal_avg / np.maximum(counts, 1)

            # Find peaks and troughs
            avg_value = np.mean(seasonal_avg)
            for i in range(best_period):
                if seasonal_avg[i] > avg_value * 1.1:
                    peak_periods.append(i + 1)  # 1-indexed
                elif seasonal_avg[i] < avg_value * 0.9:
                    trough_periods.append(i + 1)

        return SeasonalityInfo(
            has_seasonality=has_seasonality,
            period=best_period if has_seasonality else None,
            strength=float(best_strength),
            peak_periods=peak_periods,
            trough_periods=trough_periods,
        )

    def _simple_forecast(
        self,
        values: np.ndarray,
        trend: TrendInfo,
        periods: int,
    ) -> List[ForecastPoint]:
        """Generate simple linear forecast."""
        forecast = []
        n = len(values)
        std = np.std(values)

        for i in range(1, periods + 1):
            # Linear extrapolation
            predicted = trend.end_value + (trend.slope * i)

            # Confidence interval (simplified)
            margin = std * 1.96 * np.sqrt(1 + 1/n + ((n + i - (n + 1)/2) ** 2) / (n * np.var(range(n))))

            forecast.append(ForecastPoint(
                period=n + i,
                value=float(predicted),
                lower_bound=float(predicted - margin),
                upper_bound=float(predicted + margin),
            ))

        return forecast

    def _generate_summary(
        self,
        trend: TrendInfo,
        seasonality: Optional[SeasonalityInfo],
    ) -> str:
        """Generate human-readable summary."""
        parts = []

        # Trend description
        if trend.direction == TrendDirection.INCREASING:
            parts.append(f"Data shows an {trend.strength.value} increasing trend")
        elif trend.direction == TrendDirection.DECREASING:
            parts.append(f"Data shows a {trend.strength.value} decreasing trend")
        elif trend.direction == TrendDirection.VOLATILE:
            parts.append("Data shows volatile behavior with no clear trend")
        else:
            parts.append("Data is relatively stable")

        # Percent change
        if abs(trend.percent_change) > 1:
            direction = "increase" if trend.percent_change > 0 else "decrease"
            parts.append(f"with a {abs(trend.percent_change):.1f}% {direction} over {trend.periods} periods")

        # Significance
        if trend.is_significant:
            parts.append("(statistically significant)")

        # Seasonality
        if seasonality and seasonality.has_seasonality:
            parts.append(f". Seasonal pattern detected with period of {seasonality.period}")

        return " ".join(parts) + "."

    def compare_periods(
        self,
        data: pd.DataFrame,
        value_column: str,
        period_column: str,
        current_period: Any,
        comparison_period: Any,
    ) -> Dict[str, Any]:
        """
        Compare metrics between two periods.

        Args:
            data: DataFrame with data.
            value_column: Column with values.
            period_column: Column with period identifiers.
            current_period: Current period value.
            comparison_period: Comparison period value.

        Returns:
            Dictionary with comparison results.
        """
        current = data[data[period_column] == current_period][value_column]
        comparison = data[data[period_column] == comparison_period][value_column]

        if len(current) == 0 or len(comparison) == 0:
            return {"error": "No data for one or both periods"}

        current_total = current.sum()
        comparison_total = comparison.sum()
        change = current_total - comparison_total
        pct_change = (change / comparison_total * 100) if comparison_total != 0 else 0

        return {
            "current_period": current_period,
            "comparison_period": comparison_period,
            "current_total": float(current_total),
            "comparison_total": float(comparison_total),
            "absolute_change": float(change),
            "percent_change": round(float(pct_change), 2),
            "direction": "increase" if change > 0 else "decrease" if change < 0 else "no change",
        }
