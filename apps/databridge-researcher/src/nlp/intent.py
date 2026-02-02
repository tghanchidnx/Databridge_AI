"""
Intent Classifier for DataBridge AI Researcher Analytics Engine.

Classifies natural language queries into structured intents
for SQL generation.
"""

import re
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any, Tuple
from enum import Enum


class IntentType(str, Enum):
    """Types of query intents."""
    AGGREGATION = "aggregation"  # SUM, AVG, COUNT, etc.
    COMPARISON = "comparison"  # Compare values across dimensions
    TREND = "trend"  # Time series analysis
    RANKING = "ranking"  # Top N, Bottom N
    FILTER = "filter"  # Simple filtering
    DETAIL = "detail"  # Detailed records
    DISTRIBUTION = "distribution"  # Group counts, percentages
    UNKNOWN = "unknown"


@dataclass
class Intent:
    """Represents a classified intent from natural language."""

    intent_type: IntentType
    confidence: float
    aggregation: Optional[str] = None  # SUM, AVG, COUNT, etc.
    metric: Optional[str] = None  # The measure being queried
    dimensions: List[str] = field(default_factory=list)  # GROUP BY candidates
    filters: List[Dict[str, Any]] = field(default_factory=list)  # WHERE conditions
    time_filter: Optional[Dict[str, Any]] = None  # Time-based filter
    order_direction: Optional[str] = None  # ASC or DESC
    limit: Optional[int] = None  # TOP N
    comparison_type: Optional[str] = None  # year-over-year, period-over-period, etc.

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "intent_type": self.intent_type.value,
            "confidence": round(self.confidence, 2),
            "aggregation": self.aggregation,
            "metric": self.metric,
            "dimensions": self.dimensions,
            "filters": self.filters,
            "time_filter": self.time_filter,
            "order_direction": self.order_direction,
            "limit": self.limit,
            "comparison_type": self.comparison_type,
        }


class IntentClassifier:
    """
    Classifies natural language queries into structured intents.

    Uses pattern matching and keyword analysis to determine:
    - Query type (aggregation, trend, ranking, etc.)
    - Aggregation function (SUM, AVG, COUNT, etc.)
    - Time filters and dimensions
    - Ordering and limits
    """

    # Aggregation patterns
    AGGREGATION_PATTERNS = {
        "sum": [
            r"\btotal\b", r"\bsum\b", r"\baggregate\b", r"\bcombined\b",
            r"\ball\b.*\bamount", r"\btotaling\b",
        ],
        "avg": [
            r"\baverage\b", r"\bavg\b", r"\bmean\b", r"\bper\b",
        ],
        "count": [
            r"\bhow many\b", r"\bcount\b", r"\bnumber of\b", r"\b#\s*of\b",
        ],
        "min": [
            r"\bminimum\b", r"\bmin\b", r"\blowest\b", r"\bsmallest\b",
        ],
        "max": [
            r"\bmaximum\b", r"\bmax\b", r"\bhighest\b", r"\blargest\b", r"\btop\b",
        ],
    }

    # Time patterns
    TIME_PATTERNS = {
        "year": [r"\byear\b", r"\bannual\b", r"\byearly\b", r"\bYTD\b"],
        "quarter": [r"\bquarter\b", r"\bQ[1-4]\b", r"\bQTD\b"],
        "month": [r"\bmonth\b", r"\bmonthly\b", r"\bMTD\b"],
        "week": [r"\bweek\b", r"\bweekly\b", r"\bWTD\b"],
        "day": [r"\bday\b", r"\bdaily\b", r"\bdate\b"],
    }

    # Specific time references
    TIME_REFERENCES = {
        "this_year": [r"\bthis year\b", r"\bcurrent year\b", r"\bYTD\b"],
        "last_year": [r"\blast year\b", r"\bprevious year\b", r"\bprior year\b"],
        "this_quarter": [r"\bthis quarter\b", r"\bcurrent quarter\b", r"\bQTD\b"],
        "last_quarter": [r"\blast quarter\b", r"\bprevious quarter\b"],
        "this_month": [r"\bthis month\b", r"\bcurrent month\b", r"\bMTD\b"],
        "last_month": [r"\blast month\b", r"\bprevious month\b"],
    }

    # Comparison patterns
    COMPARISON_PATTERNS = {
        "year_over_year": [r"\byear.over.year\b", r"\bYoY\b", r"\bvs.*last year\b"],
        "period_over_period": [r"\bperiod.over.period\b", r"\bPoP\b"],
        "budget_vs_actual": [r"\bbudget\b.*\bactual\b", r"\bBvA\b", r"\bvariance\b"],
        "forecast_vs_actual": [r"\bforecast\b.*\bactual\b", r"\bFvA\b"],
    }

    # Ranking patterns
    RANKING_PATTERNS = {
        "top": [r"\btop\s+(\d+)\b", r"\bhighest\s+(\d+)\b", r"\bbest\s+(\d+)\b"],
        "bottom": [r"\bbottom\s+(\d+)\b", r"\blowest\s+(\d+)\b", r"\bworst\s+(\d+)\b"],
    }

    # Dimension keywords (common grouping dimensions)
    DIMENSION_KEYWORDS = [
        "by", "per", "for each", "grouped by", "broken down by",
        "across", "segment", "category", "region", "department",
    ]

    def __init__(self, confidence_threshold: float = 0.5):
        """
        Initialize the intent classifier.

        Args:
            confidence_threshold: Minimum confidence to return an intent.
        """
        self.confidence_threshold = confidence_threshold

    def classify(self, query: str) -> Intent:
        """
        Classify a natural language query into an intent.

        Args:
            query: Natural language query string.

        Returns:
            Intent object with classified information.
        """
        query_lower = query.lower().strip()

        # Detect aggregation
        aggregation, agg_confidence = self._detect_aggregation(query_lower)

        # Detect time filters
        time_filter = self._detect_time_filter(query_lower)

        # Detect comparison type
        comparison_type = self._detect_comparison(query_lower)

        # Detect ranking (top/bottom N)
        limit, order_direction = self._detect_ranking(query_lower)

        # Determine intent type
        intent_type, confidence = self._determine_intent_type(
            query_lower,
            aggregation,
            agg_confidence,
            time_filter,
            comparison_type,
            limit,
        )

        # Extract dimensions from query
        dimensions = self._extract_dimensions(query_lower)

        return Intent(
            intent_type=intent_type,
            confidence=confidence,
            aggregation=aggregation,
            dimensions=dimensions,
            time_filter=time_filter,
            order_direction=order_direction,
            limit=limit,
            comparison_type=comparison_type,
        )

    def _detect_aggregation(self, query: str) -> Tuple[Optional[str], float]:
        """Detect aggregation function from query."""
        best_match = None
        best_score = 0.0

        for agg_type, patterns in self.AGGREGATION_PATTERNS.items():
            matches = sum(1 for p in patterns if re.search(p, query, re.IGNORECASE))
            if matches > 0:
                score = matches / len(patterns)
                if score > best_score:
                    best_score = score
                    best_match = agg_type

        return best_match, min(best_score * 2, 1.0)  # Scale confidence

    def _detect_time_filter(self, query: str) -> Optional[Dict[str, Any]]:
        """Detect time-related filters from query."""
        time_filter = {}

        # Check for specific time references
        for ref_type, patterns in self.TIME_REFERENCES.items():
            if any(re.search(p, query, re.IGNORECASE) for p in patterns):
                time_filter["reference"] = ref_type
                break

        # Check for time granularity
        for granularity, patterns in self.TIME_PATTERNS.items():
            if any(re.search(p, query, re.IGNORECASE) for p in patterns):
                time_filter["granularity"] = granularity
                break

        # Check for specific year mentions
        year_match = re.search(r"\b(20\d{2})\b", query)
        if year_match:
            time_filter["year"] = int(year_match.group(1))

        # Check for month mentions
        months = [
            "january", "february", "march", "april", "may", "june",
            "july", "august", "september", "october", "november", "december",
        ]
        for i, month in enumerate(months, 1):
            if month in query:
                time_filter["month"] = i
                break

        return time_filter if time_filter else None

    def _detect_comparison(self, query: str) -> Optional[str]:
        """Detect comparison type from query."""
        for comp_type, patterns in self.COMPARISON_PATTERNS.items():
            if any(re.search(p, query, re.IGNORECASE) for p in patterns):
                return comp_type
        return None

    def _detect_ranking(self, query: str) -> Tuple[Optional[int], Optional[str]]:
        """Detect ranking (top/bottom N) from query."""
        # Check for TOP N
        for pattern in self.RANKING_PATTERNS["top"]:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                try:
                    return int(match.group(1)), "DESC"
                except (IndexError, ValueError):
                    return 10, "DESC"  # Default to 10

        # Check for BOTTOM N
        for pattern in self.RANKING_PATTERNS["bottom"]:
            match = re.search(pattern, query, re.IGNORECASE)
            if match:
                try:
                    return int(match.group(1)), "ASC"
                except (IndexError, ValueError):
                    return 10, "ASC"  # Default to 10

        return None, None

    def _determine_intent_type(
        self,
        query: str,
        aggregation: Optional[str],
        agg_confidence: float,
        time_filter: Optional[Dict],
        comparison_type: Optional[str],
        limit: Optional[int],
    ) -> Tuple[IntentType, float]:
        """Determine the overall intent type."""
        # Comparison takes precedence
        if comparison_type:
            return IntentType.COMPARISON, 0.85

        # Ranking
        if limit is not None:
            return IntentType.RANKING, 0.8

        # Trend - explicit keyword or time-based patterns
        trend_keywords = ["trend", "trending", "over time", "growth", "decline", "change over"]
        if any(kw in query for kw in trend_keywords):
            return IntentType.TREND, 0.85

        # Trend (time-based aggregation with time filter)
        if time_filter and time_filter.get("granularity") and aggregation:
            return IntentType.TREND, 0.8

        # Trend if time granularity mentioned even without explicit aggregation
        if time_filter and time_filter.get("granularity"):
            return IntentType.TREND, 0.7

        # Distribution (how many, count by)
        if aggregation == "count" or "distribution" in query or "breakdown" in query:
            return IntentType.DISTRIBUTION, 0.75

        # General aggregation
        if aggregation:
            return IntentType.AGGREGATION, max(0.6, agg_confidence)

        # Detail query (show, list, get)
        if any(w in query for w in ["show", "list", "get", "display", "what are"]):
            return IntentType.DETAIL, 0.6

        # Filter query
        if any(w in query for w in ["where", "filter", "only", "just"]):
            return IntentType.FILTER, 0.5

        return IntentType.UNKNOWN, 0.3

    def _extract_dimensions(self, query: str) -> List[str]:
        """Extract dimension keywords from query."""
        dimensions = []

        # Look for "by X" patterns
        by_pattern = r"\bby\s+(\w+(?:\s+\w+)?)"
        by_matches = re.findall(by_pattern, query, re.IGNORECASE)
        dimensions.extend(by_matches)

        # Look for "per X" patterns
        per_pattern = r"\bper\s+(\w+)"
        per_matches = re.findall(per_pattern, query, re.IGNORECASE)
        dimensions.extend(per_matches)

        # Look for "for each X" patterns
        foreach_pattern = r"\bfor each\s+(\w+)"
        foreach_matches = re.findall(foreach_pattern, query, re.IGNORECASE)
        dimensions.extend(foreach_matches)

        # Clean up and deduplicate
        cleaned = []
        for dim in dimensions:
            dim = dim.strip().lower()
            # Filter out common non-dimension words
            if dim not in ["the", "a", "an", "each", "every", "all"]:
                cleaned.append(dim)

        return list(dict.fromkeys(cleaned))  # Deduplicate preserving order

    def get_suggestions(self, partial_query: str) -> List[str]:
        """
        Get query suggestions based on partial input.

        Args:
            partial_query: Partial query string.

        Returns:
            List of suggested completions.
        """
        suggestions = []
        partial_lower = partial_query.lower()

        # Suggest aggregations
        if any(w in partial_lower for w in ["total", "sum", "show"]):
            suggestions.extend([
                f"{partial_query} by region",
                f"{partial_query} by month",
                f"{partial_query} for this year",
            ])

        # Suggest time filters
        if "this" in partial_lower or "last" in partial_lower:
            suggestions.extend([
                f"{partial_query} year",
                f"{partial_query} quarter",
                f"{partial_query} month",
            ])

        # Suggest comparisons
        if "vs" in partial_lower or "compare" in partial_lower:
            suggestions.extend([
                f"{partial_query} budget vs actual",
                f"{partial_query} year over year",
            ])

        return suggestions[:5]  # Limit suggestions
