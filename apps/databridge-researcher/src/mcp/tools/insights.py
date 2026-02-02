"""
MCP Tools for Insights and Analysis in DataBridge AI Researcher.

Provides 8 tools for anomaly detection, trend analysis, and variance analysis.
"""

from typing import Optional, List, Dict, Any

import pandas as pd
from fastmcp import FastMCP

from ...insights import (
    AnomalyDetector,
    TrendAnalyzer,
    VarianceAnalyzer,
)
from ...insights.anomaly import AnomalyMethod
from ...insights.variance import VarianceType


def register_insights_tools(mcp: FastMCP) -> None:
    """Register all insights MCP tools."""

    # Data cache for insights
    _data_cache: Dict[str, pd.DataFrame] = {}

    @mcp.tool()
    def cache_data_for_insights(
        cache_key: str,
        data: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Cache data for subsequent insights analysis.

        Args:
            cache_key: Key to store the data under.
            data: List of dictionaries representing rows.

        Returns:
            Dictionary confirming data cached.
        """
        df = pd.DataFrame(data)
        _data_cache[cache_key] = df

        return {
            "success": True,
            "cache_key": cache_key,
            "rows": len(df),
            "columns": list(df.columns),
        }

    @mcp.tool()
    def detect_anomalies(
        cache_key: str,
        columns: Optional[List[str]] = None,
        method: str = "zscore",
        threshold: float = 3.0,
        iqr_multiplier: float = 1.5,
    ) -> Dict[str, Any]:
        """
        Detect statistical anomalies in numeric data.

        Args:
            cache_key: Key of cached data to analyze.
            columns: Specific columns to analyze (None for all numeric).
            method: Detection method ('zscore', 'iqr', or 'modified_zscore').
            threshold: Z-score threshold for anomaly detection.
            iqr_multiplier: IQR multiplier for outlier bounds.

        Returns:
            Dictionary with detected anomalies and statistics.
        """
        if cache_key not in _data_cache:
            return {"success": False, "errors": [f"No cached data found for key: {cache_key}"]}

        df = _data_cache[cache_key]

        method_map = {
            "zscore": AnomalyMethod.ZSCORE,
            "iqr": AnomalyMethod.IQR,
            "modified_zscore": AnomalyMethod.MODIFIED_ZSCORE,
        }
        detection_method = method_map.get(method, AnomalyMethod.ZSCORE)

        detector = AnomalyDetector(
            method=detection_method,
            zscore_threshold=threshold,
            iqr_multiplier=iqr_multiplier,
        )

        result = detector.detect(df, columns=columns)
        return result.to_dict()

    @mcp.tool()
    def detect_time_series_anomalies(
        cache_key: str,
        value_column: str,
        time_column: str,
        window_size: int = 7,
        threshold: float = 3.0,
    ) -> Dict[str, Any]:
        """
        Detect anomalies in time series data using rolling statistics.

        Args:
            cache_key: Key of cached data to analyze.
            value_column: Column with values to analyze.
            time_column: Column with timestamps.
            window_size: Rolling window size for baseline.
            threshold: Z-score threshold for anomaly detection.

        Returns:
            Dictionary with detected anomalies.
        """
        if cache_key not in _data_cache:
            return {"success": False, "errors": [f"No cached data found for key: {cache_key}"]}

        df = _data_cache[cache_key]

        detector = AnomalyDetector(zscore_threshold=threshold)
        result = detector.detect_time_series_anomalies(
            data=df,
            value_column=value_column,
            time_column=time_column,
            window_size=window_size,
        )

        return result.to_dict()

    @mcp.tool()
    def analyze_trend(
        cache_key: str,
        value_column: str,
        time_column: Optional[str] = None,
        detect_seasonality: bool = True,
        forecast_periods: int = 0,
    ) -> Dict[str, Any]:
        """
        Analyze trends in time series data.

        Args:
            cache_key: Key of cached data to analyze.
            value_column: Column with values to analyze.
            time_column: Column with time periods (optional).
            detect_seasonality: Whether to detect seasonal patterns.
            forecast_periods: Number of periods to forecast.

        Returns:
            Dictionary with trend analysis results.
        """
        if cache_key not in _data_cache:
            return {"success": False, "errors": [f"No cached data found for key: {cache_key}"]}

        df = _data_cache[cache_key]

        analyzer = TrendAnalyzer()
        result = analyzer.analyze(
            data=df,
            value_column=value_column,
            time_column=time_column,
            detect_seasonality=detect_seasonality,
            forecast_periods=forecast_periods,
        )

        return result.to_dict()

    @mcp.tool()
    def compare_periods(
        cache_key: str,
        value_column: str,
        period_column: str,
        current_period: str,
        comparison_period: str,
    ) -> Dict[str, Any]:
        """
        Compare metrics between two periods.

        Args:
            cache_key: Key of cached data.
            value_column: Column with values to compare.
            period_column: Column with period identifiers.
            current_period: Current period value.
            comparison_period: Comparison period value.

        Returns:
            Dictionary with period comparison results.
        """
        if cache_key not in _data_cache:
            return {"success": False, "errors": [f"No cached data found for key: {cache_key}"]}

        df = _data_cache[cache_key]

        analyzer = TrendAnalyzer()
        result = analyzer.compare_periods(
            data=df,
            value_column=value_column,
            period_column=period_column,
            current_period=current_period,
            comparison_period=comparison_period,
        )

        return {
            "success": True,
            **result,
        }

    @mcp.tool()
    def analyze_variance(
        cache_key: str,
        actual_column: str,
        budget_column: str,
        dimension_column: Optional[str] = None,
        favorable_direction: str = "positive",
        top_n_drivers: int = 5,
    ) -> Dict[str, Any]:
        """
        Analyze budget vs actual variance.

        Args:
            cache_key: Key of cached data.
            actual_column: Column with actual values.
            budget_column: Column with budget values.
            dimension_column: Column to group by for dimensional analysis.
            favorable_direction: Direction considered favorable ('positive' or 'negative').
            top_n_drivers: Number of top variance drivers to identify.

        Returns:
            Dictionary with variance analysis results.
        """
        if cache_key not in _data_cache:
            return {"success": False, "errors": [f"No cached data found for key: {cache_key}"]}

        df = _data_cache[cache_key]

        analyzer = VarianceAnalyzer(favorable_direction=favorable_direction)
        result = analyzer.analyze(
            actual=df,
            budget=None,  # Using DataFrame mode
            actual_column=actual_column,
            budget_column=budget_column,
            dimension_column=dimension_column,
            top_n_drivers=top_n_drivers,
        )

        return result.to_dict()

    @mcp.tool()
    def decompose_variance(
        actual_price: float,
        actual_volume: float,
        budget_price: float,
        budget_volume: float,
    ) -> Dict[str, Any]:
        """
        Decompose variance into price and volume components.

        Args:
            actual_price: Actual unit price.
            actual_volume: Actual volume/quantity.
            budget_price: Budget unit price.
            budget_volume: Budget volume/quantity.

        Returns:
            Dictionary with price/volume variance decomposition.
        """
        analyzer = VarianceAnalyzer()
        decomposition = analyzer.decompose_variance(
            actual_price=actual_price,
            actual_volume=actual_volume,
            budget_price=budget_price,
            budget_volume=budget_volume,
        )

        return {
            "success": True,
            **decomposition.to_dict(),
        }

    @mcp.tool()
    def generate_variance_commentary(
        cache_key: str,
        actual_column: str,
        budget_column: str,
        dimension_column: Optional[str] = None,
        favorable_direction: str = "positive",
    ) -> Dict[str, Any]:
        """
        Generate executive-ready variance commentary.

        Args:
            cache_key: Key of cached data.
            actual_column: Column with actual values.
            budget_column: Column with budget values.
            dimension_column: Column to group by.
            favorable_direction: Direction considered favorable.

        Returns:
            Dictionary with formatted variance commentary.
        """
        if cache_key not in _data_cache:
            return {"success": False, "errors": [f"No cached data found for key: {cache_key}"]}

        df = _data_cache[cache_key]

        analyzer = VarianceAnalyzer(favorable_direction=favorable_direction)
        result = analyzer.analyze(
            actual=df,
            budget=None,
            actual_column=actual_column,
            budget_column=budget_column,
            dimension_column=dimension_column,
        )

        if not result.success:
            return {"success": False, "errors": result.errors}

        commentary = analyzer.generate_commentary(result, include_drivers=True)

        return {
            "success": True,
            "commentary": commentary,
            "summary": result.summary,
        }
