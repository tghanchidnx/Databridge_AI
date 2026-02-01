"""
Sort Order Inferrer for hierarchy values.

This module infers the appropriate sort order for hierarchy values
based on patterns, numeric prefixes, financial conventions, and data.
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class SortMethod(str, Enum):
    """Methods for determining sort order."""

    ALPHABETICAL = "alphabetical"
    NUMERIC = "numeric"
    NUMERIC_PREFIX = "numeric_prefix"
    FINANCIAL = "financial"
    CUSTOM = "custom"
    ORIGINAL = "original"


@dataclass
class SortOrderResult:
    """Result of sort order inference."""

    values: list[str]
    sort_orders: dict[str, int]  # value -> sort_order
    method: SortMethod
    confidence: float
    notes: list[str] = field(default_factory=list)


# Standard financial hierarchy sort orders
FINANCIAL_SORT_ORDERS = {
    # Balance Sheet categories
    "Assets": 1,
    "Total Assets": 2,
    "Current Assets": 3,
    "Cash": 4,
    "Accounts Receivable": 5,
    "AR": 5,
    "Inventory": 6,
    "Prepaid": 7,
    "Non-Current Assets": 8,
    "Fixed Assets": 9,
    "PP&E": 9,
    "Accumulated Depreciation": 10,
    "Intangible Assets": 11,
    "Liabilities": 20,
    "Total Liabilities": 21,
    "Current Liabilities": 22,
    "Accounts Payable": 23,
    "AP": 23,
    "Accrued Liabilities": 24,
    "Short-term Debt": 25,
    "Non-Current Liabilities": 26,
    "Long-term Debt": 27,
    "Equity": 30,
    "Stockholders Equity": 31,
    "Common Stock": 32,
    "Retained Earnings": 33,
    "Total Equity": 34,
    "Total Liabilities and Equity": 35,

    # Income Statement categories
    "Revenue": 100,
    "Sales": 101,
    "Net Revenue": 102,
    "Total Revenue": 103,
    "Cost of Goods Sold": 110,
    "COGS": 110,
    "Cost of Sales": 111,
    "Gross Profit": 120,
    "Gross Margin": 121,
    "Operating Expenses": 130,
    "SG&A": 131,
    "Selling Expenses": 132,
    "General and Administrative": 133,
    "G&A": 133,
    "R&D": 134,
    "Research and Development": 134,
    "Depreciation": 135,
    "Amortization": 136,
    "DD&A": 137,
    "Operating Income": 140,
    "EBIT": 141,
    "EBITDA": 142,
    "Interest Expense": 150,
    "Interest Income": 151,
    "Other Income": 152,
    "Other Expense": 153,
    "Pre-tax Income": 160,
    "EBT": 160,
    "Income Tax": 170,
    "Tax Expense": 171,
    "Net Income": 180,
    "Net Profit": 181,

    # Oil & Gas LOS categories
    "Oil Sales": 200,
    "Oil Revenue": 201,
    "Gas Sales": 202,
    "Gas Revenue": 203,
    "NGL Sales": 204,
    "Other Revenue": 205,
    "Gross Revenue": 210,
    "Severance Tax": 220,
    "Ad Valorem Tax": 221,
    "Production Tax": 222,
    "Net Revenue": 230,
    "LOE": 240,
    "Lease Operating Expense": 241,
    "Direct Operating": 242,
    "Workover": 243,
    "Gathering": 250,
    "Transportation": 251,
    "Compression": 252,
    "Processing": 253,
    "Marketing": 254,
    "Operating Margin": 260,
    "NOI": 261,
    "Net Operating Income": 262,
}


class SortOrderInferrer:
    """
    Infers sort order for hierarchy values.

    Analyzes values to determine the most appropriate sorting method
    and assigns sort orders accordingly.

    Example:
        inferrer = SortOrderInferrer()

        # Infer sort order for values
        result = inferrer.infer_sort_order(values)

        # Apply financial conventions
        result = inferrer.infer_financial_sort(values)

        # Get sort orders as dict
        sort_map = result.sort_orders
    """

    def __init__(
        self,
        prefer_numeric: bool = True,
        use_financial_conventions: bool = True,
    ):
        """
        Initialize the inferrer.

        Args:
            prefer_numeric: Prefer numeric sorting when possible
            use_financial_conventions: Apply financial hierarchy conventions
        """
        self.prefer_numeric = prefer_numeric
        self.use_financial_conventions = use_financial_conventions

    def infer_sort_order(
        self,
        values: list[str],
        entity_type: str | None = None,
    ) -> SortOrderResult:
        """
        Infer the best sort order for a list of values.

        Args:
            values: Values to sort
            entity_type: Optional entity type hint (account, product, etc.)

        Returns:
            SortOrderResult with sorted values and sort orders
        """
        if not values:
            return SortOrderResult(
                values=[],
                sort_orders={},
                method=SortMethod.ORIGINAL,
                confidence=0.0,
            )

        # Clean values
        clean_values = [v.strip() for v in values if v.strip()]

        # Try different methods and pick the best
        candidates: list[SortOrderResult] = []

        # Try financial conventions if applicable
        if self.use_financial_conventions and entity_type in ["account", "financial", None]:
            financial_result = self._try_financial_sort(clean_values)
            if financial_result.confidence > 0:
                candidates.append(financial_result)

        # Try numeric prefix sort
        if self.prefer_numeric:
            numeric_result = self._try_numeric_prefix_sort(clean_values)
            if numeric_result.confidence > 0:
                candidates.append(numeric_result)

        # Try pure numeric sort
        numeric_only = self._try_numeric_sort(clean_values)
        if numeric_only.confidence > 0:
            candidates.append(numeric_only)

        # Alphabetical is always an option
        alpha_result = self._try_alphabetical_sort(clean_values)
        candidates.append(alpha_result)

        # Pick the highest confidence result
        best = max(candidates, key=lambda r: r.confidence)
        return best

    def _try_financial_sort(self, values: list[str]) -> SortOrderResult:
        """Try to apply financial hierarchy sort conventions."""
        matched_orders: dict[str, int] = {}
        unmatched: list[str] = []

        for value in values:
            # Try exact match first
            if value in FINANCIAL_SORT_ORDERS:
                matched_orders[value] = FINANCIAL_SORT_ORDERS[value]
                continue

            # Try case-insensitive match
            value_lower = value.lower()
            matched = False
            for key, order in FINANCIAL_SORT_ORDERS.items():
                if key.lower() == value_lower:
                    matched_orders[value] = order
                    matched = True
                    break

            # Try partial match
            if not matched:
                for key, order in FINANCIAL_SORT_ORDERS.items():
                    if key.lower() in value_lower or value_lower in key.lower():
                        matched_orders[value] = order
                        matched = True
                        break

            if not matched:
                unmatched.append(value)

        # If we matched most values, use financial sort
        match_ratio = len(matched_orders) / len(values) if values else 0

        if match_ratio < 0.5:
            return SortOrderResult(
                values=values,
                sort_orders={},
                method=SortMethod.FINANCIAL,
                confidence=0.0,
            )

        # Assign sort orders to unmatched values at the end
        max_order = max(matched_orders.values()) if matched_orders else 0
        for i, value in enumerate(unmatched):
            matched_orders[value] = max_order + i + 1

        # Sort values by their order
        sorted_values = sorted(values, key=lambda v: matched_orders.get(v, 999))

        # Reassign sequential sort orders
        final_orders = {v: i for i, v in enumerate(sorted_values)}

        return SortOrderResult(
            values=sorted_values,
            sort_orders=final_orders,
            method=SortMethod.FINANCIAL,
            confidence=match_ratio,
            notes=[f"Matched {len(matched_orders) - len(unmatched)}/{len(values)} to financial conventions"],
        )

    def _try_numeric_prefix_sort(self, values: list[str]) -> SortOrderResult:
        """Try to sort by numeric prefix."""
        prefix_map: dict[str, int] = {}

        for value in values:
            # Extract leading number
            match = re.match(r'^(\d+)', value.strip())
            if match:
                prefix_map[value] = int(match.group(1))

        # If most values have numeric prefixes, use this method
        match_ratio = len(prefix_map) / len(values) if values else 0

        if match_ratio < 0.7:
            return SortOrderResult(
                values=values,
                sort_orders={},
                method=SortMethod.NUMERIC_PREFIX,
                confidence=0.0,
            )

        # Assign high numbers to non-matched values
        max_prefix = max(prefix_map.values()) if prefix_map else 0
        for value in values:
            if value not in prefix_map:
                max_prefix += 1000
                prefix_map[value] = max_prefix

        # Sort by prefix
        sorted_values = sorted(values, key=lambda v: prefix_map[v])
        final_orders = {v: i for i, v in enumerate(sorted_values)}

        return SortOrderResult(
            values=sorted_values,
            sort_orders=final_orders,
            method=SortMethod.NUMERIC_PREFIX,
            confidence=match_ratio,
            notes=[f"Sorted by numeric prefix ({len(prefix_map)} values)"],
        )

    def _try_numeric_sort(self, values: list[str]) -> SortOrderResult:
        """Try to sort purely numerically."""
        numeric_map: dict[str, float] = {}

        for value in values:
            try:
                numeric_map[value] = float(value.strip())
            except ValueError:
                pass

        # If all values are numeric
        if len(numeric_map) != len(values):
            return SortOrderResult(
                values=values,
                sort_orders={},
                method=SortMethod.NUMERIC,
                confidence=0.0,
            )

        sorted_values = sorted(values, key=lambda v: numeric_map[v])
        final_orders = {v: i for i, v in enumerate(sorted_values)}

        return SortOrderResult(
            values=sorted_values,
            sort_orders=final_orders,
            method=SortMethod.NUMERIC,
            confidence=1.0,
            notes=["All values are numeric"],
        )

    def _try_alphabetical_sort(self, values: list[str]) -> SortOrderResult:
        """Sort alphabetically."""
        sorted_values = sorted(values, key=str.lower)
        final_orders = {v: i for i, v in enumerate(sorted_values)}

        return SortOrderResult(
            values=sorted_values,
            sort_orders=final_orders,
            method=SortMethod.ALPHABETICAL,
            confidence=0.5,  # Always available but lower confidence
            notes=["Alphabetical sort"],
        )

    def infer_from_data(
        self,
        values: list[str],
        amounts: list[float] | None = None,
        frequencies: dict[str, int] | None = None,
    ) -> SortOrderResult:
        """
        Infer sort order from actual data patterns.

        Args:
            values: Values to sort
            amounts: Optional amounts for each value (e.g., totals)
            frequencies: Optional frequency count for each value

        Returns:
            SortOrderResult
        """
        notes: list[str] = []

        if amounts and len(amounts) == len(values):
            # Sort by amount (descending for typical financial reporting)
            value_amounts = list(zip(values, amounts))
            value_amounts.sort(key=lambda x: x[1], reverse=True)
            sorted_values = [v for v, _ in value_amounts]
            final_orders = {v: i for i, v in enumerate(sorted_values)}

            return SortOrderResult(
                values=sorted_values,
                sort_orders=final_orders,
                method=SortMethod.CUSTOM,
                confidence=0.8,
                notes=["Sorted by amount (descending)"],
            )

        if frequencies:
            # Sort by frequency (most common first)
            sorted_by_freq = sorted(values, key=lambda v: frequencies.get(v, 0), reverse=True)
            final_orders = {v: i for i, v in enumerate(sorted_by_freq)}

            return SortOrderResult(
                values=sorted_by_freq,
                sort_orders=final_orders,
                method=SortMethod.CUSTOM,
                confidence=0.7,
                notes=["Sorted by frequency (descending)"],
            )

        # Fall back to default inference
        return self.infer_sort_order(values)

    def apply_custom_order(
        self,
        values: list[str],
        order_map: dict[str, int],
    ) -> SortOrderResult:
        """
        Apply a custom sort order.

        Args:
            values: Values to sort
            order_map: Mapping of value -> sort position

        Returns:
            SortOrderResult
        """
        # Assign high numbers to unmapped values
        max_order = max(order_map.values()) if order_map else 0
        full_map = dict(order_map)

        for value in values:
            if value not in full_map:
                max_order += 1
                full_map[value] = max_order

        sorted_values = sorted(values, key=lambda v: full_map[v])
        final_orders = {v: i for i, v in enumerate(sorted_values)}

        return SortOrderResult(
            values=sorted_values,
            sort_orders=final_orders,
            method=SortMethod.CUSTOM,
            confidence=1.0,
            notes=["Applied custom sort order"],
        )

    def combine_level_orders(
        self,
        level_results: list[SortOrderResult],
    ) -> dict[int, dict[str, int]]:
        """
        Combine sort orders from multiple hierarchy levels.

        Args:
            level_results: List of SortOrderResult for each level

        Returns:
            Dict mapping level_number -> (value -> sort_order)
        """
        combined: dict[int, dict[str, int]] = {}

        for level_num, result in enumerate(level_results, 1):
            combined[level_num] = result.sort_orders

        return combined
