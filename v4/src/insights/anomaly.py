"""
Anomaly Detection for DataBridge AI V4 Analytics Engine.

Provides statistical anomaly detection using Z-score and IQR methods.
"""

from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Union
from enum import Enum

import pandas as pd
import numpy as np


class AnomalyMethod(str, Enum):
    """Anomaly detection methods."""
    ZSCORE = "zscore"
    IQR = "iqr"
    MODIFIED_ZSCORE = "modified_zscore"


class AnomalySeverity(str, Enum):
    """Severity levels for anomalies."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class Anomaly:
    """Represents a detected anomaly."""

    index: Any  # Row index or identifier
    column: str
    value: float
    expected_value: float
    deviation: float  # How far from expected
    severity: AnomalySeverity
    method: AnomalyMethod
    context: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "index": str(self.index),
            "column": self.column,
            "value": round(self.value, 4),
            "expected_value": round(self.expected_value, 4),
            "deviation": round(self.deviation, 4),
            "severity": self.severity.value,
            "method": self.method.value,
            "context": self.context,
        }


@dataclass
class AnomalyResult:
    """Result of anomaly detection."""

    success: bool
    total_records: int = 0
    anomalies_found: int = 0
    anomaly_rate: float = 0.0
    anomalies: List[Anomaly] = field(default_factory=list)
    statistics: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "success": self.success,
            "total_records": self.total_records,
            "anomalies_found": self.anomalies_found,
            "anomaly_rate": round(self.anomaly_rate * 100, 2),
            "anomalies": [a.to_dict() for a in self.anomalies[:100]],  # Limit to 100
            "statistics": self.statistics,
            "errors": self.errors,
        }

    def get_by_severity(self, severity: AnomalySeverity) -> List[Anomaly]:
        """Get anomalies filtered by severity."""
        return [a for a in self.anomalies if a.severity == severity]

    def get_critical(self) -> List[Anomaly]:
        """Get critical anomalies."""
        return self.get_by_severity(AnomalySeverity.CRITICAL)


class AnomalyDetector:
    """
    Statistical anomaly detector for numerical data.

    Supports multiple detection methods:
    - Z-score: Standard deviations from mean
    - IQR: Interquartile range method
    - Modified Z-score: Median-based, robust to outliers
    """

    def __init__(
        self,
        method: AnomalyMethod = AnomalyMethod.ZSCORE,
        zscore_threshold: float = 3.0,
        iqr_multiplier: float = 1.5,
        min_samples: int = 10,
    ):
        """
        Initialize the anomaly detector.

        Args:
            method: Detection method to use.
            zscore_threshold: Z-score threshold for anomaly detection.
            iqr_multiplier: IQR multiplier for outlier bounds.
            min_samples: Minimum samples required for detection.
        """
        self.method = method
        self.zscore_threshold = zscore_threshold
        self.iqr_multiplier = iqr_multiplier
        self.min_samples = min_samples

    def detect(
        self,
        data: Union[pd.DataFrame, pd.Series],
        columns: Optional[List[str]] = None,
        index_column: Optional[str] = None,
    ) -> AnomalyResult:
        """
        Detect anomalies in the data.

        Args:
            data: DataFrame or Series to analyze.
            columns: Specific columns to analyze (DataFrame only).
            index_column: Column to use as record identifier.

        Returns:
            AnomalyResult with detected anomalies.
        """
        try:
            if isinstance(data, pd.Series):
                df = data.to_frame()
                columns = [data.name or "value"]
            else:
                df = data.copy()

            if columns is None:
                # Auto-select numeric columns
                columns = df.select_dtypes(include=[np.number]).columns.tolist()

            if not columns:
                return AnomalyResult(
                    success=False,
                    errors=["No numeric columns found for anomaly detection"],
                )

            all_anomalies = []
            statistics = {}

            for col in columns:
                if col not in df.columns:
                    continue

                series = df[col].dropna()

                if len(series) < self.min_samples:
                    continue

                # Detect anomalies based on method
                if self.method == AnomalyMethod.ZSCORE:
                    anomalies, stats = self._detect_zscore(series, col, index_column, df)
                elif self.method == AnomalyMethod.IQR:
                    anomalies, stats = self._detect_iqr(series, col, index_column, df)
                elif self.method == AnomalyMethod.MODIFIED_ZSCORE:
                    anomalies, stats = self._detect_modified_zscore(series, col, index_column, df)
                else:
                    anomalies, stats = self._detect_zscore(series, col, index_column, df)

                all_anomalies.extend(anomalies)
                statistics[col] = stats

            total_records = len(df)
            anomaly_rate = len(all_anomalies) / total_records if total_records > 0 else 0

            return AnomalyResult(
                success=True,
                total_records=total_records,
                anomalies_found=len(all_anomalies),
                anomaly_rate=anomaly_rate,
                anomalies=all_anomalies,
                statistics=statistics,
            )

        except Exception as e:
            return AnomalyResult(
                success=False,
                errors=[str(e)],
            )

    def _detect_zscore(
        self,
        series: pd.Series,
        column: str,
        index_column: Optional[str],
        df: pd.DataFrame,
    ) -> tuple:
        """Detect anomalies using Z-score method."""
        mean = series.mean()
        std = series.std()

        if std == 0:
            return [], {"mean": mean, "std": 0, "method": "zscore"}

        z_scores = (series - mean) / std
        anomalies = []

        for idx in series.index:
            z = abs(z_scores[idx])
            if z > self.zscore_threshold:
                value = series[idx]
                record_id = df.loc[idx, index_column] if index_column else idx

                severity = self._calculate_severity_zscore(z)

                anomalies.append(Anomaly(
                    index=record_id,
                    column=column,
                    value=float(value),
                    expected_value=float(mean),
                    deviation=float(z),
                    severity=severity,
                    method=AnomalyMethod.ZSCORE,
                    context={"z_score": float(z)},
                ))

        stats = {
            "mean": float(mean),
            "std": float(std),
            "threshold": self.zscore_threshold,
            "method": "zscore",
        }

        return anomalies, stats

    def _detect_iqr(
        self,
        series: pd.Series,
        column: str,
        index_column: Optional[str],
        df: pd.DataFrame,
    ) -> tuple:
        """Detect anomalies using IQR method."""
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1

        lower_bound = q1 - (self.iqr_multiplier * iqr)
        upper_bound = q3 + (self.iqr_multiplier * iqr)

        median = series.median()
        anomalies = []

        for idx in series.index:
            value = series[idx]
            if value < lower_bound or value > upper_bound:
                record_id = df.loc[idx, index_column] if index_column else idx

                # Calculate deviation from nearest bound
                if value < lower_bound:
                    deviation = (lower_bound - value) / iqr if iqr > 0 else 0
                else:
                    deviation = (value - upper_bound) / iqr if iqr > 0 else 0

                severity = self._calculate_severity_iqr(deviation)

                anomalies.append(Anomaly(
                    index=record_id,
                    column=column,
                    value=float(value),
                    expected_value=float(median),
                    deviation=float(deviation),
                    severity=severity,
                    method=AnomalyMethod.IQR,
                    context={
                        "lower_bound": float(lower_bound),
                        "upper_bound": float(upper_bound),
                    },
                ))

        stats = {
            "q1": float(q1),
            "q3": float(q3),
            "iqr": float(iqr),
            "lower_bound": float(lower_bound),
            "upper_bound": float(upper_bound),
            "multiplier": self.iqr_multiplier,
            "method": "iqr",
        }

        return anomalies, stats

    def _detect_modified_zscore(
        self,
        series: pd.Series,
        column: str,
        index_column: Optional[str],
        df: pd.DataFrame,
    ) -> tuple:
        """Detect anomalies using Modified Z-score (median-based)."""
        median = series.median()
        mad = np.median(np.abs(series - median))

        if mad == 0:
            return [], {"median": median, "mad": 0, "method": "modified_zscore"}

        # Modified Z-score: 0.6745 is the 0.75th quantile of the standard normal
        modified_z_scores = 0.6745 * (series - median) / mad
        anomalies = []

        for idx in series.index:
            mz = abs(modified_z_scores[idx])
            if mz > self.zscore_threshold:
                value = series[idx]
                record_id = df.loc[idx, index_column] if index_column else idx

                severity = self._calculate_severity_zscore(mz)

                anomalies.append(Anomaly(
                    index=record_id,
                    column=column,
                    value=float(value),
                    expected_value=float(median),
                    deviation=float(mz),
                    severity=severity,
                    method=AnomalyMethod.MODIFIED_ZSCORE,
                    context={"modified_z_score": float(mz)},
                ))

        stats = {
            "median": float(median),
            "mad": float(mad),
            "threshold": self.zscore_threshold,
            "method": "modified_zscore",
        }

        return anomalies, stats

    def _calculate_severity_zscore(self, z_score: float) -> AnomalySeverity:
        """Calculate severity based on Z-score."""
        if z_score > 5:
            return AnomalySeverity.CRITICAL
        elif z_score > 4:
            return AnomalySeverity.HIGH
        elif z_score > 3.5:
            return AnomalySeverity.MEDIUM
        else:
            return AnomalySeverity.LOW

    def _calculate_severity_iqr(self, deviation: float) -> AnomalySeverity:
        """Calculate severity based on IQR deviation."""
        if deviation > 3:
            return AnomalySeverity.CRITICAL
        elif deviation > 2:
            return AnomalySeverity.HIGH
        elif deviation > 1.5:
            return AnomalySeverity.MEDIUM
        else:
            return AnomalySeverity.LOW

    def detect_time_series_anomalies(
        self,
        data: pd.DataFrame,
        value_column: str,
        time_column: str,
        window_size: int = 7,
    ) -> AnomalyResult:
        """
        Detect anomalies in time series data using rolling statistics.

        Args:
            data: DataFrame with time series data.
            value_column: Column containing values to analyze.
            time_column: Column containing timestamps.
            window_size: Rolling window size for baseline calculation.

        Returns:
            AnomalyResult with detected anomalies.
        """
        try:
            df = data.sort_values(time_column).copy()

            # Calculate rolling statistics using SHIFTED window (excluding current point)
            # This ensures anomalies aren't masked by including themselves in the baseline
            df["rolling_mean"] = df[value_column].shift(1).rolling(
                window=window_size, min_periods=max(1, window_size // 2)
            ).mean()
            df["rolling_std"] = df[value_column].shift(1).rolling(
                window=window_size, min_periods=max(1, window_size // 2)
            ).std()

            # Fill initial values without chained assignment
            overall_mean = df[value_column].mean()
            overall_std = df[value_column].std()
            df["rolling_mean"] = df["rolling_mean"].fillna(overall_mean)
            df["rolling_std"] = df["rolling_std"].fillna(overall_std if overall_std > 0 else 1)

            # Calculate Z-scores against rolling baseline
            df["z_score"] = (df[value_column] - df["rolling_mean"]) / df["rolling_std"].replace(0, 1)

            anomalies = []
            for idx, row in df.iterrows():
                z = abs(row["z_score"])
                if z > self.zscore_threshold:
                    severity = self._calculate_severity_zscore(z)

                    anomalies.append(Anomaly(
                        index=row[time_column],
                        column=value_column,
                        value=float(row[value_column]),
                        expected_value=float(row["rolling_mean"]),
                        deviation=float(z),
                        severity=severity,
                        method=AnomalyMethod.ZSCORE,
                        context={
                            "rolling_mean": float(row["rolling_mean"]),
                            "rolling_std": float(row["rolling_std"]),
                            "window_size": window_size,
                        },
                    ))

            return AnomalyResult(
                success=True,
                total_records=len(df),
                anomalies_found=len(anomalies),
                anomaly_rate=len(anomalies) / len(df) if len(df) > 0 else 0,
                anomalies=anomalies,
                statistics={
                    "window_size": window_size,
                    "method": "rolling_zscore",
                },
            )

        except Exception as e:
            return AnomalyResult(
                success=False,
                errors=[str(e)],
            )
