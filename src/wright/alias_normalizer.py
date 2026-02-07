"""
ID_SOURCE Alias Normalizer.

Phase 31: Handles ID_SOURCE typos and aliases in hierarchy mappings.

From docs/databridge_complete_analysis.md:
- NET has 3 ID_SOURCE typos causing silent data loss:
  - BILLING_CATEGRY_CODE → BILLING_CATEGORY_CODE (missing 'O')
  - BILLING_CATEGORY_TYPE → BILLING_CATEGORY_TYPE_CODE (missing '_CODE')

This module:
1. Auto-detects potential typos using fuzzy matching
2. Maintains alias registry for known corrections
3. Generates normalized CASE statements for VW_1
4. Integrates with MartConfigGenerator for automatic alias handling
"""

import logging
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field

from ..diff.core import compute_similarity, find_close_matches

logger = logging.getLogger(__name__)


@dataclass
class AliasMapping:
    """A single ID_SOURCE alias mapping."""
    alias: str  # The typo/variant form
    canonical: str  # The correct/canonical form
    physical_column: str  # The physical dimension column
    dimension_table: Optional[str] = None
    confidence: float = 1.0  # 1.0 for known, lower for auto-detected
    auto_detected: bool = False


@dataclass
class NormalizationResult:
    """Result of ID_SOURCE normalization."""
    original: str
    normalized: str
    was_aliased: bool
    confidence: float
    suggestion: Optional[str] = None  # For unrecognized values


class IDSourceNormalizer:
    """
    Normalizes ID_SOURCE values to canonical forms.

    Handles typo corrections and maintains alias registry for
    consistent column mapping across hierarchy configurations.
    """

    # Default canonical mappings (ID_SOURCE -> physical column)
    DEFAULT_CANONICAL_MAPPINGS = {
        "BILLING_CATEGORY_CODE": "ACCT.ACCOUNT_BILLING_CATEGORY_CODE",
        "BILLING_CATEGORY_TYPE_CODE": "ACCT.ACCOUNT_BILLING_CATEGORY_TYPE_CODE",
        "ACCOUNT_CODE": "ACCT.ACCOUNT_CODE",
        "MINOR_CODE": "ACCT.ACCOUNT_MINOR_CODE",
        "DEDUCT_CODE": "DEDUCT.DEDUCT_CODE",
        "PRODUCT_CODE": "PROD.PRODUCT_CODE",
        "ROYALTY_FILTER": "FACT.ROYALTY_FILTER",
    }

    # Known typos and their corrections
    DEFAULT_ALIAS_MAP = {
        # NET typos from analysis
        "BILLING_CATEGRY_CODE": "BILLING_CATEGORY_CODE",  # Missing 'O'
        "BILLING_CATEGORY_TYPE": "BILLING_CATEGORY_TYPE_CODE",  # Missing '_CODE'
        # Common variations
        "BILLINGCATEGORYCODE": "BILLING_CATEGORY_CODE",
        "BILLING_CAT_CODE": "BILLING_CATEGORY_CODE",
        "BILLING_CAT_TYPE_CODE": "BILLING_CATEGORY_TYPE_CODE",
        "ACCOUNT": "ACCOUNT_CODE",
        "DEDUCT": "DEDUCT_CODE",
        "PRODUCT": "PRODUCT_CODE",
        "ACCT_CODE": "ACCOUNT_CODE",
    }

    def __init__(
        self,
        canonical_mappings: Optional[Dict[str, str]] = None,
        alias_map: Optional[Dict[str, str]] = None,
        auto_detect_threshold: float = 0.85,
    ):
        """
        Initialize the normalizer.

        Args:
            canonical_mappings: ID_SOURCE -> physical column mappings
            alias_map: Known alias -> canonical form mappings
            auto_detect_threshold: Similarity threshold for auto-detection
        """
        self.canonical_mappings = self.DEFAULT_CANONICAL_MAPPINGS.copy()
        if canonical_mappings:
            self.canonical_mappings.update(canonical_mappings)

        self.alias_map = self.DEFAULT_ALIAS_MAP.copy()
        if alias_map:
            self.alias_map.update(alias_map)

        self.auto_detect_threshold = auto_detect_threshold

        # Track auto-detected aliases
        self._auto_detected: Dict[str, str] = {}

    @property
    def canonical_values(self) -> Set[str]:
        """Get all canonical ID_SOURCE values."""
        return set(self.canonical_mappings.keys())

    def add_alias(
        self,
        alias: str,
        canonical: str,
        auto_detected: bool = False,
    ) -> None:
        """
        Add an alias mapping.

        Args:
            alias: The alias/typo form
            canonical: The canonical form
            auto_detected: Whether this was auto-detected
        """
        self.alias_map[alias.upper()] = canonical.upper()
        if auto_detected:
            self._auto_detected[alias.upper()] = canonical.upper()
        logger.info(f"Added ID_SOURCE alias: {alias} -> {canonical}")

    def add_canonical_mapping(
        self,
        id_source: str,
        physical_column: str,
    ) -> None:
        """
        Add a canonical ID_SOURCE to physical column mapping.

        Args:
            id_source: The ID_SOURCE value
            physical_column: The physical column reference
        """
        self.canonical_mappings[id_source.upper()] = physical_column
        logger.info(f"Added canonical mapping: {id_source} -> {physical_column}")

    def normalize(
        self,
        id_source: str,
        auto_detect: bool = True,
    ) -> NormalizationResult:
        """
        Normalize an ID_SOURCE value.

        Args:
            id_source: The ID_SOURCE value to normalize
            auto_detect: Whether to use fuzzy matching for unknown values

        Returns:
            NormalizationResult with normalization details
        """
        upper = id_source.upper()

        # Check if already canonical
        if upper in self.canonical_mappings:
            return NormalizationResult(
                original=id_source,
                normalized=upper,
                was_aliased=False,
                confidence=1.0,
            )

        # Check known aliases
        if upper in self.alias_map:
            return NormalizationResult(
                original=id_source,
                normalized=self.alias_map[upper],
                was_aliased=True,
                confidence=1.0,
            )

        # Try auto-detection
        if auto_detect:
            matches = find_close_matches(
                upper,
                list(self.canonical_values),
                n=1,
                cutoff=self.auto_detect_threshold,
            )

            if matches:
                canonical = matches[0].candidate
                confidence = matches[0].similarity

                # Auto-add for future
                self._auto_detected[upper] = canonical

                return NormalizationResult(
                    original=id_source,
                    normalized=canonical,
                    was_aliased=True,
                    confidence=confidence,
                    suggestion=f"Auto-detected: '{id_source}' -> '{canonical}' ({confidence:.0%} match)",
                )

        # Unknown value
        return NormalizationResult(
            original=id_source,
            normalized=upper,
            was_aliased=False,
            confidence=0.0,
            suggestion=f"Unknown ID_SOURCE: '{id_source}' - add to canonical mappings",
        )

    def get_physical_column(
        self,
        id_source: str,
        auto_detect: bool = True,
    ) -> Tuple[str, bool]:
        """
        Get the physical column for an ID_SOURCE.

        Args:
            id_source: The ID_SOURCE value
            auto_detect: Whether to use fuzzy matching

        Returns:
            Tuple of (physical_column, was_aliased)
        """
        result = self.normalize(id_source, auto_detect)

        if result.normalized in self.canonical_mappings:
            return self.canonical_mappings[result.normalized], result.was_aliased

        # Return a placeholder for unknown
        return f"/* UNKNOWN: {id_source} */ NULL", True

    def normalize_mapping_data(
        self,
        mappings: List[Dict[str, Any]],
        id_source_key: str = "ID_SOURCE",
        auto_detect: bool = True,
    ) -> Tuple[List[Dict[str, Any]], List[NormalizationResult]]:
        """
        Normalize ID_SOURCE values in mapping data.

        Args:
            mappings: List of mapping records
            id_source_key: Key for ID_SOURCE field
            auto_detect: Whether to use fuzzy matching

        Returns:
            Tuple of (normalized mappings, list of normalization results)
        """
        results = []
        normalized_mappings = []

        for mapping in mappings:
            m = mapping.copy()
            id_source = m.get(id_source_key) or m.get(id_source_key.lower()) or ""

            if id_source:
                result = self.normalize(id_source, auto_detect)
                results.append(result)

                if result.was_aliased:
                    m[id_source_key] = result.normalized

            normalized_mappings.append(m)

        aliased_count = sum(1 for r in results if r.was_aliased)
        logger.info(
            f"Normalized {len(mappings)} mappings: {aliased_count} aliases applied"
        )

        return normalized_mappings, results

    def generate_case_statement(
        self,
        include_aliases: bool = True,
        alias_column: str = "ID_SOURCE",
    ) -> str:
        """
        Generate a CASE statement for VW_1 with alias handling.

        Args:
            include_aliases: Whether to include alias mappings
            alias_column: Column name to match against

        Returns:
            SQL CASE statement
        """
        lines = ["CASE"]

        # Add canonical mappings
        for id_source, physical_column in sorted(self.canonical_mappings.items()):
            lines.append(f"    WHEN {alias_column} = '{id_source}' THEN {physical_column}")

        # Add aliases if requested
        if include_aliases:
            for alias, canonical in sorted(self.alias_map.items()):
                if canonical in self.canonical_mappings:
                    physical_column = self.canonical_mappings[canonical]
                    lines.append(f"    -- Alias for {canonical}")
                    lines.append(f"    WHEN {alias_column} = '{alias}' THEN {physical_column}")

        lines.append("    ELSE NULL")
        lines.append("END")

        return "\n".join(lines)

    def get_alias_report(self) -> Dict[str, Any]:
        """
        Get a report of all aliases and auto-detected mappings.

        Returns:
            Dictionary with alias information
        """
        return {
            "canonical_count": len(self.canonical_mappings),
            "alias_count": len(self.alias_map),
            "auto_detected_count": len(self._auto_detected),
            "canonical_mappings": self.canonical_mappings.copy(),
            "alias_mappings": self.alias_map.copy(),
            "auto_detected": self._auto_detected.copy(),
        }

    def export_config(self) -> Dict[str, Any]:
        """
        Export normalizer configuration for persistence.

        Returns:
            Configuration dictionary
        """
        return {
            "canonical_mappings": self.canonical_mappings.copy(),
            "alias_map": self.alias_map.copy(),
            "auto_detect_threshold": self.auto_detect_threshold,
        }

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "IDSourceNormalizer":
        """
        Create normalizer from configuration.

        Args:
            config: Configuration dictionary

        Returns:
            IDSourceNormalizer instance
        """
        return cls(
            canonical_mappings=config.get("canonical_mappings"),
            alias_map=config.get("alias_map"),
            auto_detect_threshold=config.get("auto_detect_threshold", 0.85),
        )


# Global instance for shared use
_normalizer: Optional[IDSourceNormalizer] = None


def get_normalizer() -> IDSourceNormalizer:
    """Get or create the global normalizer instance."""
    global _normalizer
    if _normalizer is None:
        _normalizer = IDSourceNormalizer()
    return _normalizer


def normalize_id_source(id_source: str, auto_detect: bool = True) -> str:
    """
    Convenience function to normalize an ID_SOURCE value.

    Args:
        id_source: The ID_SOURCE value
        auto_detect: Whether to use fuzzy matching

    Returns:
        Normalized ID_SOURCE value
    """
    result = get_normalizer().normalize(id_source, auto_detect)
    return result.normalized
