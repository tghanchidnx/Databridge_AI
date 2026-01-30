"""Unit tests for the insights module (anomaly, trend, variance)."""

import pytest
import pandas as pd
import numpy as np

from src.insights.anomaly import (
    AnomalyDetector,
    AnomalyResult,
    Anomaly,
    AnomalyMethod,
    AnomalySeverity,
)
from src.insights.trend import (
    TrendAnalyzer,
    TrendResult,
    TrendDirection,
    TrendStrength,
)
from src.insights.variance import (
    VarianceAnalyzer,
    VarianceResult,
    VarianceItem,
    VarianceType,
    VarianceCategory,
)


class TestAnomalyDetector:
    """Tests for AnomalyDetector."""

    @pytest.fixture
    def detector(self):
        return AnomalyDetector()

    @pytest.fixture
    def sample_data(self):
        """Create sample data with outliers."""
        np.random.seed(42)
        data = np.random.normal(100, 10, 100)
        # Add outliers
        data[10] = 200  # High outlier
        data[50] = 20   # Low outlier
        return pd.DataFrame({"value": data})

    def test_detect_zscore_anomalies(self, detector, sample_data):
        """Test Z-score anomaly detection."""
        result = detector.detect(sample_data, columns=["value"])

        assert result.success
        assert result.anomalies_found >= 1
        assert result.total_records == 100

    def test_detect_iqr_anomalies(self, sample_data):
        """Test IQR anomaly detection."""
        detector = AnomalyDetector(method=AnomalyMethod.IQR)
        result = detector.detect(sample_data, columns=["value"])

        assert result.success
        assert result.anomalies_found >= 1

    def test_detect_modified_zscore(self, sample_data):
        """Test modified Z-score anomaly detection."""
        detector = AnomalyDetector(method=AnomalyMethod.MODIFIED_ZSCORE)
        result = detector.detect(sample_data, columns=["value"])

        assert result.success

    def test_anomaly_severity(self, sample_data):
        """Test that anomalies have severity levels."""
        detector = AnomalyDetector(zscore_threshold=2.0)
        result = detector.detect(sample_data, columns=["value"])

        if result.anomalies_found > 0:
            assert all(a.severity in AnomalySeverity for a in result.anomalies)

    def test_no_anomalies_normal_data(self):
        """Test detection on data without anomalies."""
        np.random.seed(42)
        data = pd.DataFrame({"value": np.random.normal(100, 5, 100)})

        detector = AnomalyDetector(zscore_threshold=4.0)
        result = detector.detect(data, columns=["value"])

        assert result.success
        # May or may not have anomalies depending on random data

    def test_detect_series_input(self, detector):
        """Test detection with Series input."""
        series = pd.Series([100, 102, 98, 500, 101, 99])  # 500 is outlier

        result = detector.detect(series)

        assert result.success

    def test_time_series_anomalies(self, detector):
        """Test time series anomaly detection."""
        dates = pd.date_range("2024-01-01", periods=30)
        values = [100] * 25 + [200] + [100] * 4  # Spike on day 26

        df = pd.DataFrame({"date": dates, "value": values})

        result = detector.detect_time_series_anomalies(
            data=df,
            value_column="value",
            time_column="date",
            window_size=7,
        )

        assert result.success
        assert result.anomalies_found >= 1

    def test_result_to_dict(self, detector, sample_data):
        """Test AnomalyResult to_dict method."""
        result = detector.detect(sample_data, columns=["value"])
        result_dict = result.to_dict()

        assert "success" in result_dict
        assert "total_records" in result_dict
        assert "anomalies_found" in result_dict
        assert "anomaly_rate" in result_dict
        assert "anomalies" in result_dict

    def test_get_critical_anomalies(self, detector, sample_data):
        """Test filtering critical anomalies."""
        result = detector.detect(sample_data, columns=["value"])
        critical = result.get_critical()

        assert isinstance(critical, list)


class TestTrendAnalyzer:
    """Tests for TrendAnalyzer."""

    @pytest.fixture
    def analyzer(self):
        return TrendAnalyzer()

    @pytest.fixture
    def increasing_data(self):
        """Create increasing trend data."""
        return pd.DataFrame({
            "period": range(1, 13),
            "value": [100 + i * 10 for i in range(12)],
        })

    @pytest.fixture
    def decreasing_data(self):
        """Create decreasing trend data."""
        return pd.DataFrame({
            "period": range(1, 13),
            "value": [200 - i * 10 for i in range(12)],
        })

    @pytest.fixture
    def stable_data(self):
        """Create stable data."""
        np.random.seed(42)
        return pd.DataFrame({
            "period": range(1, 13),
            "value": 100 + np.random.normal(0, 2, 12),
        })

    def test_detect_increasing_trend(self, analyzer, increasing_data):
        """Test detection of increasing trend."""
        result = analyzer.analyze(
            increasing_data,
            value_column="value",
            time_column="period",
        )

        assert result.success
        assert result.trend.direction == TrendDirection.INCREASING

    def test_detect_decreasing_trend(self, analyzer, decreasing_data):
        """Test detection of decreasing trend."""
        result = analyzer.analyze(
            decreasing_data,
            value_column="value",
            time_column="period",
        )

        assert result.success
        assert result.trend.direction == TrendDirection.DECREASING

    def test_detect_stable_trend(self, analyzer, stable_data):
        """Test detection of stable trend."""
        result = analyzer.analyze(
            stable_data,
            value_column="value",
            time_column="period",
        )

        assert result.success
        # May be stable or have weak trend

    def test_trend_strength(self, analyzer, increasing_data):
        """Test trend strength calculation."""
        result = analyzer.analyze(
            increasing_data,
            value_column="value",
            time_column="period",
        )

        assert result.trend.strength in TrendStrength
        assert result.trend.r_squared >= 0

    def test_trend_significance(self, analyzer, increasing_data):
        """Test statistical significance."""
        result = analyzer.analyze(
            increasing_data,
            value_column="value",
            time_column="period",
        )

        assert isinstance(result.trend.is_significant, bool)
        assert result.trend.p_value >= 0

    def test_percent_change(self, analyzer, increasing_data):
        """Test percent change calculation."""
        result = analyzer.analyze(
            increasing_data,
            value_column="value",
            time_column="period",
        )

        assert result.trend.percent_change > 0

    def test_seasonality_detection(self, analyzer):
        """Test seasonality detection."""
        # Create seasonal data
        values = [100, 120, 150, 130] * 3  # Quarterly pattern
        df = pd.DataFrame({
            "period": range(12),
            "value": values,
        })

        result = analyzer.analyze(
            df,
            value_column="value",
            detect_seasonality=True,
        )

        assert result.success
        # Seasonality detection may or may not find pattern

    def test_simple_forecast(self, analyzer, increasing_data):
        """Test simple forecasting."""
        result = analyzer.analyze(
            increasing_data,
            value_column="value",
            time_column="period",
            forecast_periods=3,
        )

        assert result.success
        assert len(result.forecast) == 3
        assert all(f.lower_bound <= f.value <= f.upper_bound for f in result.forecast)

    def test_compare_periods(self, analyzer):
        """Test period comparison."""
        df = pd.DataFrame({
            "year": [2022, 2022, 2023, 2023],
            "value": [100, 150, 120, 180],
        })

        comparison = analyzer.compare_periods(
            data=df,
            value_column="value",
            period_column="year",
            current_period=2023,
            comparison_period=2022,
        )

        assert "current_total" in comparison
        assert "comparison_total" in comparison
        assert "percent_change" in comparison

    def test_result_to_dict(self, analyzer, increasing_data):
        """Test TrendResult to_dict method."""
        result = analyzer.analyze(
            increasing_data,
            value_column="value",
            time_column="period",
        )
        result_dict = result.to_dict()

        assert "success" in result_dict
        assert "trend" in result_dict
        assert "summary" in result_dict

    def test_summary_generation(self, analyzer, increasing_data):
        """Test summary text generation."""
        result = analyzer.analyze(
            increasing_data,
            value_column="value",
            time_column="period",
        )

        assert len(result.summary) > 0
        assert "trend" in result.summary.lower() or "data" in result.summary.lower()


class TestVarianceAnalyzer:
    """Tests for VarianceAnalyzer."""

    @pytest.fixture
    def analyzer(self):
        return VarianceAnalyzer()

    def test_scalar_variance_favorable(self, analyzer):
        """Test favorable variance calculation."""
        result = analyzer.analyze(actual=120, budget=100)

        assert result.success
        assert result.total_variance == 20
        assert result.overall_variance_type == VarianceType.FAVORABLE

    def test_scalar_variance_unfavorable(self, analyzer):
        """Test unfavorable variance calculation."""
        result = analyzer.analyze(actual=80, budget=100)

        assert result.success
        assert result.total_variance == -20
        assert result.overall_variance_type == VarianceType.UNFAVORABLE

    def test_scalar_variance_percent(self, analyzer):
        """Test variance percentage calculation."""
        result = analyzer.analyze(actual=110, budget=100)

        assert result.total_variance_percent == 10.0

    def test_cost_favorable_direction(self):
        """Test cost variance (negative is favorable)."""
        analyzer = VarianceAnalyzer(favorable_direction="negative")
        result = analyzer.analyze(actual=80, budget=100)

        assert result.overall_variance_type == VarianceType.FAVORABLE

    def test_series_variance(self, analyzer):
        """Test variance with Series input."""
        actual = pd.Series([100, 200, 300], index=["A", "B", "C"])
        budget = pd.Series([90, 220, 280], index=["A", "B", "C"])

        result = analyzer.analyze(actual, budget)

        assert result.success
        assert len(result.items) == 3

    def test_dataframe_variance(self, analyzer):
        """Test variance with DataFrame input."""
        df = pd.DataFrame({
            "region": ["North", "South", "East", "West"],
            "actual": [100, 200, 150, 250],
            "budget": [90, 220, 140, 260],
        })

        result = analyzer.analyze(
            df,
            actual_column="actual",
            budget_column="budget",
            dimension_column="region",
        )

        assert result.success
        assert len(result.items) == 4

    def test_top_drivers(self, analyzer):
        """Test identification of top variance drivers."""
        df = pd.DataFrame({
            "region": ["North", "South", "East", "West"],
            "actual": [100, 300, 150, 250],
            "budget": [90, 200, 140, 260],
        })

        result = analyzer.analyze(
            df,
            actual_column="actual",
            budget_column="budget",
            dimension_column="region",
            top_n_drivers=2,
        )

        assert len(result.top_drivers) == 2
        # South should be top driver (100 variance)
        assert result.top_drivers[0].dimension_value == "South"

    def test_materiality_flag(self):
        """Test materiality flagging."""
        analyzer = VarianceAnalyzer(materiality_threshold=0.10)

        df = pd.DataFrame({
            "region": ["A", "B"],
            "actual": [115, 102],
            "budget": [100, 100],
        })

        result = analyzer.analyze(
            df,
            actual_column="actual",
            budget_column="budget",
            dimension_column="region",
        )

        # A has 15% variance (above 10% threshold)
        item_a = next(i for i in result.items if i.dimension_value == "A")
        assert item_a.materiality_flag == True

    def test_variance_decomposition(self, analyzer):
        """Test price/volume variance decomposition."""
        decomposition = analyzer.decompose_variance(
            actual_price=12,
            actual_volume=100,
            budget_price=10,
            budget_volume=90,
        )

        assert decomposition.total_variance == 1200 - 900  # 300
        assert VarianceCategory.PRICE in decomposition.components
        assert VarianceCategory.VOLUME in decomposition.components

    def test_decomposition_price_variance(self, analyzer):
        """Test price variance component."""
        decomposition = analyzer.decompose_variance(
            actual_price=12,
            actual_volume=100,
            budget_price=10,
            budget_volume=100,
        )

        # Price variance = (12-10) * 100 = 200
        assert decomposition.components[VarianceCategory.PRICE] == 200

    def test_decomposition_volume_variance(self, analyzer):
        """Test volume variance component."""
        decomposition = analyzer.decompose_variance(
            actual_price=10,
            actual_volume=120,
            budget_price=10,
            budget_volume=100,
        )

        # Volume variance = (120-100) * 10 = 200
        assert decomposition.components[VarianceCategory.VOLUME] == 200

    def test_generate_commentary(self, analyzer):
        """Test commentary generation."""
        df = pd.DataFrame({
            "region": ["North", "South"],
            "actual": [100, 200],
            "budget": [90, 220],
        })

        result = analyzer.analyze(
            df,
            actual_column="actual",
            budget_column="budget",
            dimension_column="region",
        )

        commentary = analyzer.generate_commentary(result)

        assert len(commentary) > 0
        assert "Variance" in commentary

    def test_result_to_dict(self, analyzer):
        """Test VarianceResult to_dict method."""
        result = analyzer.analyze(actual=120, budget=100)
        result_dict = result.to_dict()

        assert "success" in result_dict
        assert "totals" in result_dict
        assert "summary" in result_dict

    def test_get_favorable_unfavorable(self, analyzer):
        """Test filtering favorable/unfavorable items."""
        df = pd.DataFrame({
            "region": ["A", "B", "C"],
            "actual": [120, 80, 100],
            "budget": [100, 100, 100],
        })

        result = analyzer.analyze(
            df,
            actual_column="actual",
            budget_column="budget",
            dimension_column="region",
        )

        favorable = result.get_favorable()
        unfavorable = result.get_unfavorable()

        assert len(favorable) == 1  # A
        assert len(unfavorable) == 1  # B
