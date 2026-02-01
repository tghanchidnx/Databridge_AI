"""
Account and Entity Type Detector.

This module detects the 12 standard entity types from data values,
column names, and patterns. The 12 entity types are:
account, cost_center, department, entity, project, product,
customer, vendor, employee, location, time_period, currency
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from databridge_discovery.models.case_statement import EntityType


@dataclass
class EntityDetectionResult:
    """Result of entity type detection."""

    detected_type: EntityType
    confidence: float
    evidence: list[str]
    alternative_types: list[tuple[EntityType, float]]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class MultiColumnDetectionResult:
    """Result of detecting entities across multiple columns."""

    column_results: dict[str, EntityDetectionResult]
    primary_entity: EntityType | None
    entity_coverage: dict[EntityType, list[str]]  # EntityType -> columns
    notes: list[str] = field(default_factory=list)


class AccountDetector:
    """
    Detects entity types from data values and patterns.

    Supports detection of 12 standard entity types used in financial
    and operational hierarchies.

    Example:
        detector = AccountDetector()

        # Detect from values
        result = detector.detect_from_values(values)

        # Detect from column name and values
        result = detector.detect(column_name, values)

        # Detect across DataFrame
        result = detector.detect_from_dataframe(df)
    """

    # Value patterns for each entity type
    VALUE_PATTERNS = {
        EntityType.ACCOUNT: {
            "prefixes": ["1", "2", "3", "4", "5", "6", "7", "8", "9"],
            "keywords": [
                "revenue", "sales", "expense", "cost", "asset", "liability",
                "equity", "income", "cash", "receivable", "payable",
                "depreciation", "amortization", "interest", "tax",
                "cogs", "gross profit", "net income", "ebitda",
            ],
            "patterns": [
                r"^\d{3,}$",  # Numeric account codes
                r"^\d{1,2}-\d{3,}$",  # Formatted codes like 1-1000
                r"^[A-Z]{2,3}\d{3,}$",  # Prefix + numeric like GL5010
            ],
        },
        EntityType.COST_CENTER: {
            "prefixes": ["CC", "PC", "RC"],
            "keywords": [
                "cost center", "profit center", "responsibility",
                "overhead", "direct", "indirect", "admin", "support",
            ],
            "patterns": [
                r"^CC\d{3,}$",
                r"^PC\d{3,}$",
                r"^\d{4}-\d{4}$",
            ],
        },
        EntityType.DEPARTMENT: {
            "prefixes": ["DEPT", "DIV", "BU"],
            "keywords": [
                "department", "division", "business unit", "group",
                "sales", "marketing", "hr", "human resources", "finance",
                "operations", "it", "legal", "accounting", "engineering",
                "r&d", "research", "development", "production", "manufacturing",
            ],
            "patterns": [
                r"^DEPT\d{2,}$",
                r"^D\d{3,}$",
            ],
        },
        EntityType.ENTITY: {
            "prefixes": ["CO", "ENT", "ORG", "SUB"],
            "keywords": [
                "company", "entity", "subsidiary", "affiliate",
                "corporate", "holding", "llc", "inc", "corp", "ltd",
                "group", "international", "domestic",
            ],
            "patterns": [
                r"^ENT\d{2,}$",
                r"^CO\d{2,}$",
            ],
        },
        EntityType.PROJECT: {
            "prefixes": ["PRJ", "WBS", "JOB", "WO"],
            "keywords": [
                "project", "work order", "job", "task", "phase",
                "milestone", "initiative", "program", "capital",
            ],
            "patterns": [
                r"^PRJ-\d+$",
                r"^WBS\.\d+\.\d+",
                r"^JOB\d{4,}$",
            ],
        },
        EntityType.PRODUCT: {
            "prefixes": ["SKU", "PROD", "ITM", "MAT"],
            "keywords": [
                "product", "item", "sku", "material", "goods",
                "merchandise", "inventory", "part", "component",
            ],
            "patterns": [
                r"^SKU-?\d+$",
                r"^\d{10,}$",  # UPC-like
                r"^[A-Z]{2,3}-\d{4,}$",
            ],
        },
        EntityType.CUSTOMER: {
            "prefixes": ["CUST", "CUS", "CLI"],
            "keywords": [
                "customer", "client", "buyer", "account",
                "consumer", "patron", "purchaser",
            ],
            "patterns": [
                r"^CUST\d{4,}$",
                r"^C-\d+$",
            ],
        },
        EntityType.VENDOR: {
            "prefixes": ["VEN", "VND", "SUP"],
            "keywords": [
                "vendor", "supplier", "provider", "contractor",
                "seller", "merchant", "distributor",
            ],
            "patterns": [
                r"^VEN\d{4,}$",
                r"^V-\d+$",
            ],
        },
        EntityType.EMPLOYEE: {
            "prefixes": ["EMP", "EE", "STAFF"],
            "keywords": [
                "employee", "staff", "worker", "associate",
                "team member", "personnel",
            ],
            "patterns": [
                r"^EMP\d{4,}$",
                r"^E\d{5,}$",
            ],
        },
        EntityType.LOCATION: {
            "prefixes": ["LOC", "SITE", "FAC", "WH"],
            "keywords": [
                "location", "site", "facility", "warehouse",
                "plant", "store", "branch", "office",
                "region", "country", "state", "city",
            ],
            "patterns": [
                r"^LOC\d{2,}$",
                r"^WH\d{2,}$",
                r"^[A-Z]{2,3}$",  # Country/state codes
            ],
        },
        EntityType.TIME_PERIOD: {
            "prefixes": ["FY", "FP", "CY"],
            "keywords": [
                "period", "month", "quarter", "year", "fiscal",
                "ytd", "mtd", "qtd", "calendar",
                "january", "february", "march", "april", "may", "june",
                "july", "august", "september", "october", "november", "december",
                "q1", "q2", "q3", "q4",
            ],
            "patterns": [
                r"^FY\d{2,4}$",
                r"^\d{4}-\d{2}$",  # YYYY-MM
                r"^P\d{2}$",
                r"^Q[1-4]$",
            ],
        },
        EntityType.CURRENCY: {
            "prefixes": [],
            "keywords": [
                "usd", "eur", "gbp", "jpy", "cny", "cad", "aud",
                "dollar", "euro", "pound", "yen", "yuan",
                "currency", "fx", "exchange",
            ],
            "patterns": [
                r"^[A-Z]{3}$",  # ISO currency codes
            ],
        },
    }

    # Column name patterns
    COLUMN_PATTERNS = {
        EntityType.ACCOUNT: [
            r"account[_\s]*(code|id|num|number|name)?",
            r"acct[_\s]*(code|id|num)?",
            r"gl[_\s]*(account|code)",
            r"chart[_\s]*of[_\s]*accounts",
        ],
        EntityType.COST_CENTER: [
            r"cost[_\s]*center",
            r"cc[_\s]*(code|id)",
            r"profit[_\s]*center",
        ],
        EntityType.DEPARTMENT: [
            r"department",
            r"dept[_\s]*(code|id|name)?",
            r"division",
            r"business[_\s]*unit",
        ],
        EntityType.ENTITY: [
            r"entity[_\s]*(code|id|name)?",
            r"legal[_\s]*entity",
            r"company[_\s]*(code|id)?",
        ],
        EntityType.PROJECT: [
            r"project[_\s]*(code|id|name)?",
            r"work[_\s]*order",
            r"wbs",
        ],
        EntityType.PRODUCT: [
            r"product[_\s]*(code|id|name)?",
            r"sku",
            r"item[_\s]*(code|id)?",
        ],
        EntityType.CUSTOMER: [
            r"customer[_\s]*(code|id|name)?",
            r"client[_\s]*(code|id)?",
        ],
        EntityType.VENDOR: [
            r"vendor[_\s]*(code|id|name)?",
            r"supplier",
        ],
        EntityType.EMPLOYEE: [
            r"employee[_\s]*(code|id|name)?",
            r"emp[_\s]*(code|id)?",
        ],
        EntityType.LOCATION: [
            r"location[_\s]*(code|id)?",
            r"site[_\s]*(code|id)?",
            r"facility",
            r"warehouse",
        ],
        EntityType.TIME_PERIOD: [
            r"period",
            r"fiscal[_\s]*(year|month|quarter)",
            r"date[_\s]*key",
        ],
        EntityType.CURRENCY: [
            r"currency[_\s]*(code|id)?",
            r"curr[_\s]*(code)?",
            r"fx",
        ],
    }

    def __init__(
        self,
        min_confidence: float = 0.5,
    ):
        """
        Initialize the detector.

        Args:
            min_confidence: Minimum confidence threshold for detection
        """
        self.min_confidence = min_confidence

    def detect(
        self,
        column_name: str | None = None,
        values: list[str] | None = None,
    ) -> EntityDetectionResult:
        """
        Detect entity type from column name and/or values.

        Args:
            column_name: Column name to analyze
            values: Sample values from the column

        Returns:
            EntityDetectionResult
        """
        scores: dict[EntityType, float] = defaultdict(float)
        evidence: dict[EntityType, list[str]] = defaultdict(list)

        # Score from column name
        if column_name:
            name_scores = self._score_from_column_name(column_name)
            for entity_type, score in name_scores.items():
                scores[entity_type] += score * 0.4
                if score > 0:
                    evidence[entity_type].append(f"Column name matches '{column_name}'")

        # Score from values
        if values:
            value_scores = self._score_from_values(values)
            for entity_type, (score, reasons) in value_scores.items():
                scores[entity_type] += score * 0.6
                evidence[entity_type].extend(reasons)

        # Find best match
        if not scores:
            return EntityDetectionResult(
                detected_type=EntityType.UNKNOWN,
                confidence=0.0,
                evidence=[],
                alternative_types=[],
            )

        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        best_type, best_score = sorted_scores[0]

        # Get alternatives
        alternatives = [
            (t, s) for t, s in sorted_scores[1:4]
            if s >= self.min_confidence
        ]

        if best_score < self.min_confidence:
            best_type = EntityType.UNKNOWN

        return EntityDetectionResult(
            detected_type=best_type,
            confidence=min(best_score, 1.0),
            evidence=evidence.get(best_type, []),
            alternative_types=alternatives,
        )

    def detect_from_values(self, values: list[str]) -> EntityDetectionResult:
        """Detect entity type from values only."""
        return self.detect(column_name=None, values=values)

    def detect_from_column_name(self, column_name: str) -> EntityDetectionResult:
        """Detect entity type from column name only."""
        return self.detect(column_name=column_name, values=None)

    def detect_from_dataframe(
        self,
        df: pd.DataFrame,
        columns: list[str] | None = None,
    ) -> MultiColumnDetectionResult:
        """
        Detect entity types across DataFrame columns.

        Args:
            df: DataFrame to analyze
            columns: Specific columns to check (defaults to all)

        Returns:
            MultiColumnDetectionResult
        """
        columns = columns if columns else df.columns.tolist()
        column_results: dict[str, EntityDetectionResult] = {}
        entity_coverage: dict[EntityType, list[str]] = defaultdict(list)

        for col in columns:
            if col not in df.columns:
                continue

            values = (
                df[col].dropna()
                .astype(str)
                .unique()[:100]
                .tolist()
            )

            result = self.detect(column_name=col, values=values)
            column_results[col] = result

            if result.detected_type != EntityType.UNKNOWN:
                entity_coverage[result.detected_type].append(col)

        # Determine primary entity
        primary_entity = None
        if entity_coverage:
            primary_entity = max(
                entity_coverage.keys(),
                key=lambda e: (
                    len(entity_coverage[e]),
                    max(column_results[c].confidence for c in entity_coverage[e])
                )
            )

        return MultiColumnDetectionResult(
            column_results=column_results,
            primary_entity=primary_entity,
            entity_coverage=dict(entity_coverage),
        )

    def _score_from_column_name(self, column_name: str) -> dict[EntityType, float]:
        """Score entity types based on column name."""
        scores: dict[EntityType, float] = {}
        col_lower = column_name.lower()

        for entity_type, patterns in self.COLUMN_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, col_lower, re.IGNORECASE):
                    scores[entity_type] = 1.0
                    break

        return scores

    def _score_from_values(
        self,
        values: list[str],
    ) -> dict[EntityType, tuple[float, list[str]]]:
        """Score entity types based on sample values."""
        results: dict[EntityType, tuple[float, list[str]]] = {}

        if not values:
            return results

        for entity_type, config in self.VALUE_PATTERNS.items():
            score = 0.0
            reasons: list[str] = []

            # Check prefixes
            prefixes = config.get("prefixes", [])
            if prefixes:
                prefix_matches = sum(
                    1 for v in values
                    if any(v.upper().startswith(p.upper()) for p in prefixes)
                )
                if prefix_matches > len(values) * 0.3:
                    score += 0.3
                    reasons.append(f"Prefix match ({prefix_matches}/{len(values)})")

            # Check keywords
            keywords = config.get("keywords", [])
            if keywords:
                keyword_matches = sum(
                    1 for v in values
                    if any(k.lower() in v.lower() for k in keywords)
                )
                if keyword_matches > 0:
                    keyword_score = min(keyword_matches / len(values), 0.5)
                    score += keyword_score
                    reasons.append(f"Keyword match ({keyword_matches} values)")

            # Check patterns
            patterns = config.get("patterns", [])
            if patterns:
                pattern_matches = sum(
                    1 for v in values
                    if any(re.match(p, v, re.IGNORECASE) for p in patterns)
                )
                if pattern_matches > len(values) * 0.5:
                    score += 0.4
                    reasons.append(f"Pattern match ({pattern_matches}/{len(values)})")

            if score > 0:
                results[entity_type] = (score, reasons)

        return results

    def get_entity_description(self, entity_type: EntityType) -> str:
        """Get a description of an entity type."""
        descriptions = {
            EntityType.ACCOUNT: "General ledger account codes for financial transactions",
            EntityType.COST_CENTER: "Organizational units for cost allocation and tracking",
            EntityType.DEPARTMENT: "Organizational departments, divisions, or business units",
            EntityType.ENTITY: "Legal entities, companies, or subsidiaries",
            EntityType.PROJECT: "Projects, work orders, or jobs for cost tracking",
            EntityType.PRODUCT: "Products, SKUs, or inventory items",
            EntityType.CUSTOMER: "Customer or client accounts",
            EntityType.VENDOR: "Vendor or supplier accounts",
            EntityType.EMPLOYEE: "Employee records and personnel",
            EntityType.LOCATION: "Physical locations, sites, or facilities",
            EntityType.TIME_PERIOD: "Time periods for reporting (fiscal periods, months, quarters)",
            EntityType.CURRENCY: "Currency codes and foreign exchange",
            EntityType.UNKNOWN: "Entity type could not be determined",
        }
        return descriptions.get(entity_type, "Unknown entity type")

    def get_all_entity_types(self) -> list[EntityType]:
        """Get list of all 12 standard entity types."""
        return [
            EntityType.ACCOUNT,
            EntityType.COST_CENTER,
            EntityType.DEPARTMENT,
            EntityType.ENTITY,
            EntityType.PROJECT,
            EntityType.PRODUCT,
            EntityType.CUSTOMER,
            EntityType.VENDOR,
            EntityType.EMPLOYEE,
            EntityType.LOCATION,
            EntityType.TIME_PERIOD,
            EntityType.CURRENCY,
        ]
