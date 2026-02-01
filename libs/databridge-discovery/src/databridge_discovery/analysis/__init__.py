"""
Analysis modules for data profiling and entity detection.

This package provides tools for:
- Analyzing CSV results for hierarchy patterns
- Detecting entity types from data
- Profiling data quality
"""

from databridge_discovery.analysis.csv_result_analyzer import CSVResultAnalyzer
from databridge_discovery.analysis.account_detector import AccountDetector

__all__ = [
    "CSVResultAnalyzer",
    "AccountDetector",
]
