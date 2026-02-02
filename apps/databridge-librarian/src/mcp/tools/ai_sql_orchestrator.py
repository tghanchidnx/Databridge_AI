"""
AI-Enhanced SQL Hierarchy Analysis Orchestrator.

This module provides AI agent-powered SQL analysis with:
- Individual specialized agents for each analysis task
- An orchestrator agent that coordinates and adapts to client needs
- Client-specific customization through the knowledge base
- Intelligent CSV export based on orchestrator determination

Architecture:
    ┌─────────────────────────────────────────────────────────────┐
    │                    ORCHESTRATOR AGENT                        │
    │  (Context-aware coordinator with client knowledge)          │
    └─────────────────────┬───────────────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        │                 │                 │
        ▼                 ▼                 ▼
┌───────────────┐ ┌───────────────┐ ┌───────────────┐
│ CASE Extract  │ │ Entity Detect │ │ Pattern Detect│
│    Agent      │ │    Agent      │ │    Agent      │
└───────────────┘ └───────────────┘ └───────────────┘
        │                 │                 │
        └─────────────────┼─────────────────┘
                          ▼
                ┌───────────────────┐
                │ Confidence Scorer │
                │      Agent        │
                └───────────────────┘
                          │
                          ▼
                ┌───────────────────┐
                │   Export Agent    │
                │ (CSV Generation)  │
                └───────────────────┘
"""

from __future__ import annotations

import json
import os
import csv
import re
import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Callable
from collections import defaultdict

# Import base classes from sql_discovery
from .sql_discovery import (
    EntityType,
    ConditionOperator,
    CaseCondition,
    CaseWhen,
    ExtractedCase,
    SimpleCaseExtractor,
    ENTITY_PATTERNS,
    FINANCIAL_PATTERNS,
)


# ==============================================================================
# AI AGENT BASE CLASSES
# ==============================================================================

@dataclass
class AgentContext:
    """Context passed to AI agents for decision making."""
    client_id: Optional[str] = None
    client_name: Optional[str] = None
    industry: str = "general"
    erp_system: Optional[str] = None
    custom_prompts: List[Dict[str, str]] = field(default_factory=list)
    gl_patterns: Dict[str, str] = field(default_factory=dict)
    user_intent: str = ""
    sql_dialect: str = "snowflake"
    additional_context: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AgentResult:
    """Result from an AI agent."""
    success: bool
    data: Any
    reasoning: str
    confidence: float
    suggestions: List[str] = field(default_factory=list)
    modifications: Dict[str, Any] = field(default_factory=dict)


class BaseAgent(ABC):
    """Base class for all AI agents."""

    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
        self.execution_log: List[str] = []

    @abstractmethod
    def execute(self, input_data: Any, context: AgentContext) -> AgentResult:
        """Execute the agent's task."""
        pass

    def log(self, message: str) -> None:
        """Log agent activity."""
        timestamp = datetime.now().isoformat()
        self.execution_log.append(f"[{timestamp}] {self.name}: {message}")

    def get_client_specific_rules(self, context: AgentContext, rule_type: str) -> List[str]:
        """Get client-specific rules from context."""
        rules = []
        for prompt in context.custom_prompts:
            if prompt.get("category") == rule_type:
                rules.append(prompt.get("content", ""))
        return rules


# ==============================================================================
# SPECIALIZED AI AGENTS
# ==============================================================================

class CaseExtractionAgent(BaseAgent):
    """
    AI Agent for intelligent CASE statement extraction.

    Enhances regex-based extraction with:
    - Context-aware pattern recognition
    - Client-specific SQL dialect handling
    - Nested CASE detection
    - Complex condition understanding
    """

    def __init__(self):
        super().__init__(
            name="CaseExtractionAgent",
            description="Extracts and analyzes CASE statements from SQL"
        )
        self.base_extractor = SimpleCaseExtractor()

    def execute(self, input_data: Dict[str, Any], context: AgentContext) -> AgentResult:
        sql = input_data.get("sql", "")
        self.log(f"Starting CASE extraction for {context.industry} industry")

        # Get client-specific extraction rules
        client_rules = self.get_client_specific_rules(context, "sql_extraction")

        # Base extraction using regex
        cases = self.base_extractor.extract_from_sql(sql)

        # AI Enhancement: Analyze extraction quality and suggest improvements
        reasoning_parts = []
        suggestions = []
        modifications = {}

        # Check for nested CASE statements
        nested_count = sql.upper().count("CASE") - len(cases)
        if nested_count > 0:
            reasoning_parts.append(f"Detected {nested_count} potentially nested CASE statements")
            suggestions.append("Review nested CASE logic for additional hierarchy levels")

        # Check for client-specific patterns
        if context.industry == "oil_gas":
            # Oil & Gas specific enhancements
            og_patterns = self._detect_oil_gas_patterns(sql, cases)
            if og_patterns:
                modifications["oil_gas_enhancements"] = og_patterns
                reasoning_parts.append(f"Applied Oil & Gas specific patterns: {', '.join(og_patterns.keys())}")

        # Enhance cases with additional metadata
        enhanced_cases = []
        for case in cases:
            enhanced = self._enhance_case(case, context, client_rules)
            enhanced_cases.append(enhanced)

        confidence = self._calculate_extraction_confidence(enhanced_cases, sql)

        return AgentResult(
            success=len(enhanced_cases) > 0,
            data={"cases": enhanced_cases, "raw_cases": cases},
            reasoning=" | ".join(reasoning_parts) if reasoning_parts else "Standard extraction completed",
            confidence=confidence,
            suggestions=suggestions,
            modifications=modifications,
        )

    def _detect_oil_gas_patterns(self, sql: str, cases: List[ExtractedCase]) -> Dict[str, Any]:
        """Detect Oil & Gas specific patterns."""
        patterns = {}
        sql_lower = sql.lower()

        # LOE/LOS patterns
        if "loe" in sql_lower or "los" in sql_lower or "lease operating" in sql_lower:
            patterns["loe_hierarchy"] = True

        # AFE patterns
        if "afe" in sql_lower or "authorization for expenditure" in sql_lower:
            patterns["afe_tracking"] = True

        # JIB patterns
        if "jib" in sql_lower or "joint interest" in sql_lower:
            patterns["jib_allocation"] = True

        # Well/Field patterns
        if "well" in sql_lower or "field" in sql_lower or "basin" in sql_lower:
            patterns["asset_hierarchy"] = True

        return patterns

    def _enhance_case(
        self,
        case: ExtractedCase,
        context: AgentContext,
        client_rules: List[str]
    ) -> ExtractedCase:
        """Enhance a CASE statement with AI-derived metadata."""
        # Add industry-specific entity type refinement
        if context.industry == "oil_gas":
            case = self._refine_oil_gas_entity(case)
        elif context.industry == "manufacturing":
            case = self._refine_manufacturing_entity(case)

        return case

    def _refine_oil_gas_entity(self, case: ExtractedCase) -> ExtractedCase:
        """Refine entity type for Oil & Gas industry."""
        col_lower = case.input_column.lower()

        # Oil & Gas specific column patterns
        if any(p in col_lower for p in ["well", "api", "uwi"]):
            case.entity_type = EntityType.LOCATION
        elif any(p in col_lower for p in ["afe", "wbs"]):
            case.entity_type = EntityType.PROJECT
        elif any(p in col_lower for p in ["jib", "partner", "working_interest"]):
            case.entity_type = EntityType.ENTITY

        return case

    def _refine_manufacturing_entity(self, case: ExtractedCase) -> ExtractedCase:
        """Refine entity type for Manufacturing industry."""
        col_lower = case.input_column.lower()

        if any(p in col_lower for p in ["work_center", "workcenter", "machine"]):
            case.entity_type = EntityType.LOCATION
        elif any(p in col_lower for p in ["bom", "routing", "operation"]):
            case.entity_type = EntityType.PRODUCT

        return case

    def _calculate_extraction_confidence(self, cases: List[ExtractedCase], sql: str) -> float:
        """Calculate confidence in the extraction."""
        if not cases:
            return 0.0

        confidence = 0.6  # Base confidence

        # More cases = more confident the SQL is hierarchy-rich
        if len(cases) >= 5:
            confidence += 0.15
        elif len(cases) >= 3:
            confidence += 0.1

        # Check if all CASE statements have aliases
        aliased = sum(1 for c in cases if not c.source_column.startswith("case_column_"))
        if aliased == len(cases):
            confidence += 0.1

        # Check for consistent patterns
        pattern_types = set(c.pattern_type for c in cases if c.pattern_type)
        if len(pattern_types) <= 2:
            confidence += 0.1

        return min(confidence, 1.0)


class EntityDetectionAgent(BaseAgent):
    """
    AI Agent for intelligent entity type detection.

    Enhances rule-based detection with:
    - Client-specific entity mappings
    - Industry-aware classification
    - Context from surrounding SQL
    - Learning from previous classifications
    """

    def __init__(self):
        super().__init__(
            name="EntityDetectionAgent",
            description="Detects and classifies entity types from SQL patterns"
        )

    def execute(self, input_data: Dict[str, Any], context: AgentContext) -> AgentResult:
        cases = input_data.get("cases", [])
        self.log(f"Detecting entities for {len(cases)} CASE statements")

        # Get client-specific entity mappings
        client_gl_patterns = context.gl_patterns
        client_rules = self.get_client_specific_rules(context, "entity_mapping")

        enhanced_cases = []
        reasoning_parts = []
        entity_summary = defaultdict(int)

        for case in cases:
            # Apply AI-enhanced entity detection
            detected_type, detection_reasoning = self._detect_entity_with_context(
                case, context, client_gl_patterns
            )

            # Update case with detected entity
            if detected_type != EntityType.UNKNOWN:
                case.entity_type = detected_type
                reasoning_parts.append(f"{case.source_column}: {detected_type.value} ({detection_reasoning})")

            entity_summary[case.entity_type.value] += 1
            enhanced_cases.append(case)

        # Calculate overall confidence
        known_entities = sum(1 for c in enhanced_cases if c.entity_type != EntityType.UNKNOWN)
        confidence = known_entities / len(enhanced_cases) if enhanced_cases else 0.0

        suggestions = self._generate_entity_suggestions(enhanced_cases, context)

        return AgentResult(
            success=True,
            data={
                "cases": enhanced_cases,
                "entity_summary": dict(entity_summary),
            },
            reasoning=" | ".join(reasoning_parts[:5]) if reasoning_parts else "Entity detection completed",
            confidence=confidence,
            suggestions=suggestions,
        )

    def _detect_entity_with_context(
        self,
        case: ExtractedCase,
        context: AgentContext,
        client_gl_patterns: Dict[str, str],
    ) -> tuple[EntityType, str]:
        """Detect entity type using context-aware analysis."""
        column = case.input_column.lower()

        # Check client-specific GL patterns first
        for pattern, entity_type_str in client_gl_patterns.items():
            if re.search(pattern, column, re.IGNORECASE):
                try:
                    return EntityType(entity_type_str), "client GL pattern match"
                except ValueError:
                    pass

        # Industry-specific detection
        if context.industry == "oil_gas":
            detected = self._detect_oil_gas_entity(column, case)
            if detected != EntityType.UNKNOWN:
                return detected, "oil & gas industry pattern"

        elif context.industry == "manufacturing":
            detected = self._detect_manufacturing_entity(column, case)
            if detected != EntityType.UNKNOWN:
                return detected, "manufacturing industry pattern"

        elif context.industry == "saas":
            detected = self._detect_saas_entity(column, case)
            if detected != EntityType.UNKNOWN:
                return detected, "SaaS industry pattern"

        # Standard pattern matching
        for entity_type, patterns in ENTITY_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, column, re.IGNORECASE):
                    return entity_type, "standard pattern match"

        # Analyze result values for clues
        result_entity = self._detect_from_results(case)
        if result_entity != EntityType.UNKNOWN:
            return result_entity, "inferred from result values"

        return EntityType.UNKNOWN, "no pattern matched"

    def _detect_oil_gas_entity(self, column: str, case: ExtractedCase) -> EntityType:
        """Oil & Gas specific entity detection."""
        if any(p in column for p in ["well", "api", "uwi", "field", "basin", "area"]):
            return EntityType.LOCATION
        if any(p in column for p in ["afe", "wbs", "project", "drill"]):
            return EntityType.PROJECT
        if any(p in column for p in ["partner", "jib", "working_interest", "nri", "owner"]):
            return EntityType.ENTITY
        if any(p in column for p in ["billcat", "loe", "los", "opex"]):
            return EntityType.ACCOUNT
        return EntityType.UNKNOWN

    def _detect_manufacturing_entity(self, column: str, case: ExtractedCase) -> EntityType:
        """Manufacturing specific entity detection."""
        if any(p in column for p in ["plant", "line", "workcenter", "machine"]):
            return EntityType.LOCATION
        if any(p in column for p in ["bom", "sku", "item", "part"]):
            return EntityType.PRODUCT
        if any(p in column for p in ["work_order", "job", "batch"]):
            return EntityType.PROJECT
        return EntityType.UNKNOWN

    def _detect_saas_entity(self, column: str, case: ExtractedCase) -> EntityType:
        """SaaS specific entity detection."""
        if any(p in column for p in ["customer", "account", "tenant", "org"]):
            return EntityType.CUSTOMER
        if any(p in column for p in ["plan", "tier", "subscription", "product"]):
            return EntityType.PRODUCT
        if any(p in column for p in ["mrr", "arr", "revenue", "booking"]):
            return EntityType.ACCOUNT
        return EntityType.UNKNOWN

    def _detect_from_results(self, case: ExtractedCase) -> EntityType:
        """Infer entity type from result values."""
        results = [w.result_value.lower() for w in case.when_clauses]

        # Check for financial patterns
        financial_matches = sum(
            1 for r in results
            if any(p.lower() in r for p in FINANCIAL_PATTERNS)
        )
        if financial_matches >= 3:
            return EntityType.ACCOUNT

        # Check for geographic patterns
        geo_patterns = ["north", "south", "east", "west", "region", "state", "country"]
        geo_matches = sum(1 for r in results if any(p in r for p in geo_patterns))
        if geo_matches >= 2:
            return EntityType.LOCATION

        return EntityType.UNKNOWN

    def _generate_entity_suggestions(
        self,
        cases: List[ExtractedCase],
        context: AgentContext,
    ) -> List[str]:
        """Generate suggestions for entity detection improvement."""
        suggestions = []

        unknown_count = sum(1 for c in cases if c.entity_type == EntityType.UNKNOWN)
        if unknown_count > 0:
            suggestions.append(
                f"{unknown_count} columns have unknown entity types. "
                "Consider adding client-specific GL patterns."
            )

        if not context.gl_patterns:
            suggestions.append(
                "No client GL patterns configured. Add patterns to improve detection accuracy."
            )

        return suggestions


class PatternDetectionAgent(BaseAgent):
    """
    AI Agent for intelligent pattern detection.

    Analyzes SQL conditions to determine:
    - Pattern types (prefix, suffix, range, exact)
    - Hierarchy potential
    - Grouping strategies
    - Client-specific pattern recognition
    """

    def __init__(self):
        super().__init__(
            name="PatternDetectionAgent",
            description="Detects and analyzes condition patterns for hierarchy building"
        )

    def execute(self, input_data: Dict[str, Any], context: AgentContext) -> AgentResult:
        cases = input_data.get("cases", [])
        self.log(f"Analyzing patterns for {len(cases)} CASE statements")

        pattern_analysis = []
        suggestions = []

        for case in cases:
            analysis = self._analyze_case_patterns(case, context)
            pattern_analysis.append(analysis)

            # Update case pattern type if AI suggests different
            if analysis.get("suggested_pattern_type"):
                case.pattern_type = analysis["suggested_pattern_type"]

        # Generate hierarchy recommendations
        hierarchy_recommendations = self._generate_hierarchy_recommendations(
            cases, pattern_analysis, context
        )

        confidence = self._calculate_pattern_confidence(pattern_analysis)

        return AgentResult(
            success=True,
            data={
                "cases": cases,
                "pattern_analysis": pattern_analysis,
                "hierarchy_recommendations": hierarchy_recommendations,
            },
            reasoning=f"Analyzed {len(cases)} CASE patterns, found {len(hierarchy_recommendations)} hierarchy opportunities",
            confidence=confidence,
            suggestions=suggestions,
        )

    def _analyze_case_patterns(
        self,
        case: ExtractedCase,
        context: AgentContext,
    ) -> Dict[str, Any]:
        """Analyze patterns in a CASE statement."""
        analysis = {
            "source_column": case.source_column,
            "input_column": case.input_column,
            "original_pattern_type": case.pattern_type,
            "suggested_pattern_type": None,
            "pattern_breakdown": defaultdict(int),
            "hierarchy_potential": "low",
            "grouping_strategy": None,
            "notes": [],
        }

        # Analyze each condition
        for when in case.when_clauses:
            cond = when.condition
            pattern = self._classify_condition_pattern(cond)
            analysis["pattern_breakdown"][pattern] += 1

        # Determine dominant pattern
        if analysis["pattern_breakdown"]:
            dominant = max(analysis["pattern_breakdown"].items(), key=lambda x: x[1])
            analysis["suggested_pattern_type"] = dominant[0]

        # Assess hierarchy potential
        analysis["hierarchy_potential"] = self._assess_hierarchy_potential(case, analysis)

        # Suggest grouping strategy
        analysis["grouping_strategy"] = self._suggest_grouping(case, analysis, context)

        return analysis

    def _classify_condition_pattern(self, condition: CaseCondition) -> str:
        """Classify a condition into a pattern type."""
        if condition.operator in (ConditionOperator.LIKE, ConditionOperator.ILIKE):
            for value in condition.values:
                if value.endswith("%") and not value.startswith("%"):
                    return "prefix"
                elif value.startswith("%") and not value.endswith("%"):
                    return "suffix"
                elif value.startswith("%") and value.endswith("%"):
                    return "contains"
            return "exact_like"

        elif condition.operator == ConditionOperator.IN:
            return "exact_list"

        elif condition.operator == ConditionOperator.BETWEEN:
            return "range"

        elif condition.operator == ConditionOperator.EQUALS:
            return "exact"

        return "unknown"

    def _assess_hierarchy_potential(
        self,
        case: ExtractedCase,
        analysis: Dict[str, Any],
    ) -> str:
        """Assess how suitable this CASE is for hierarchy creation."""
        score = 0

        # More unique results = better hierarchy
        if len(case.unique_results) >= 10:
            score += 3
        elif len(case.unique_results) >= 5:
            score += 2
        elif len(case.unique_results) >= 3:
            score += 1

        # Prefix patterns are great for account hierarchies
        if analysis.get("suggested_pattern_type") == "prefix":
            score += 2

        # Known entity type
        if case.entity_type != EntityType.UNKNOWN:
            score += 2

        # Good rollup ratio (many conditions -> fewer results)
        if len(case.when_clauses) > len(case.unique_results) * 1.5:
            score += 1

        if score >= 6:
            return "high"
        elif score >= 3:
            return "medium"
        return "low"

    def _suggest_grouping(
        self,
        case: ExtractedCase,
        analysis: Dict[str, Any],
        context: AgentContext,
    ) -> Optional[str]:
        """Suggest a grouping strategy for hierarchy building."""
        if analysis["suggested_pattern_type"] == "prefix":
            return "prefix_rollup"  # Group by first N characters

        if analysis["suggested_pattern_type"] == "range":
            return "range_buckets"  # Group into range buckets

        if len(case.unique_results) > 20:
            return "category_grouping"  # Need parent categories

        return "flat"  # Simple flat list

    def _generate_hierarchy_recommendations(
        self,
        cases: List[ExtractedCase],
        analyses: List[Dict[str, Any]],
        context: AgentContext,
    ) -> List[Dict[str, Any]]:
        """Generate recommendations for hierarchy creation."""
        recommendations = []

        for case, analysis in zip(cases, analyses):
            if analysis["hierarchy_potential"] in ("high", "medium"):
                rec = {
                    "hierarchy_name": case.source_column,
                    "priority": "high" if analysis["hierarchy_potential"] == "high" else "medium",
                    "suggested_levels": self._suggest_level_count(case, analysis),
                    "grouping_strategy": analysis["grouping_strategy"],
                    "notes": [],
                }

                if context.industry == "oil_gas" and case.entity_type == EntityType.ACCOUNT:
                    rec["notes"].append("Consider aligning with LOS/LOE categories")

                recommendations.append(rec)

        return recommendations

    def _suggest_level_count(self, case: ExtractedCase, analysis: Dict[str, Any]) -> int:
        """Suggest number of hierarchy levels."""
        if analysis["grouping_strategy"] == "prefix_rollup":
            return 2  # Parent category + detail
        if len(case.unique_results) > 20:
            return 3  # Need intermediate grouping
        return 2  # Standard parent + child

    def _calculate_pattern_confidence(self, analyses: List[Dict[str, Any]]) -> float:
        """Calculate confidence in pattern detection."""
        if not analyses:
            return 0.0

        high_potential = sum(1 for a in analyses if a["hierarchy_potential"] == "high")
        med_potential = sum(1 for a in analyses if a["hierarchy_potential"] == "medium")

        return min((high_potential * 0.15 + med_potential * 0.1) + 0.5, 1.0)


class ConfidenceScoringAgent(BaseAgent):
    """
    AI Agent for intelligent confidence scoring.

    Calculates confidence scores based on:
    - Extraction quality
    - Entity detection accuracy
    - Pattern consistency
    - Client-specific factors
    - Historical accuracy (when available)
    """

    def __init__(self):
        super().__init__(
            name="ConfidenceScoringAgent",
            description="Calculates confidence scores for hierarchy recommendations"
        )

    def execute(self, input_data: Dict[str, Any], context: AgentContext) -> AgentResult:
        cases = input_data.get("cases", [])
        pattern_analysis = input_data.get("pattern_analysis", [])

        self.log(f"Scoring confidence for {len(cases)} hierarchies")

        scored_cases = []
        overall_scores = []

        for idx, case in enumerate(cases):
            analysis = pattern_analysis[idx] if idx < len(pattern_analysis) else {}

            scores = self._calculate_detailed_scores(case, analysis, context)
            overall_score = self._calculate_overall_score(scores)

            scored_cases.append({
                "hierarchy_name": case.source_column,
                "scores": scores,
                "overall_score": overall_score,
                "recommendation": self._get_recommendation(overall_score),
            })
            overall_scores.append(overall_score)

        avg_confidence = sum(overall_scores) / len(overall_scores) if overall_scores else 0.0

        return AgentResult(
            success=True,
            data={
                "cases": cases,
                "scored_cases": scored_cases,
                "average_confidence": avg_confidence,
            },
            reasoning=f"Average confidence: {avg_confidence:.0%}",
            confidence=avg_confidence,
            suggestions=self._generate_improvement_suggestions(scored_cases),
        )

    def _calculate_detailed_scores(
        self,
        case: ExtractedCase,
        analysis: Dict[str, Any],
        context: AgentContext,
    ) -> Dict[str, float]:
        """Calculate detailed confidence scores."""
        scores = {}

        # Extraction quality score
        scores["extraction"] = 0.7  # Base
        if not case.source_column.startswith("case_column_"):
            scores["extraction"] += 0.2
        if len(case.when_clauses) >= 5:
            scores["extraction"] += 0.1
        scores["extraction"] = min(scores["extraction"], 1.0)

        # Entity detection score
        scores["entity_detection"] = 0.3 if case.entity_type == EntityType.UNKNOWN else 0.9

        # Pattern consistency score
        if analysis.get("pattern_breakdown"):
            total = sum(analysis["pattern_breakdown"].values())
            dominant = max(analysis["pattern_breakdown"].values())
            scores["pattern_consistency"] = dominant / total if total > 0 else 0.5
        else:
            scores["pattern_consistency"] = 0.5

        # Hierarchy potential score
        potential_map = {"high": 0.95, "medium": 0.75, "low": 0.5}
        scores["hierarchy_potential"] = potential_map.get(
            analysis.get("hierarchy_potential", "low"), 0.5
        )

        # Client alignment score
        scores["client_alignment"] = self._calculate_client_alignment(case, context)

        return scores

    def _calculate_client_alignment(
        self,
        case: ExtractedCase,
        context: AgentContext,
    ) -> float:
        """Calculate how well this aligns with client needs."""
        score = 0.6  # Base

        # Industry alignment
        if context.industry != "general":
            if case.entity_type != EntityType.UNKNOWN:
                score += 0.2

        # GL pattern match
        if context.gl_patterns:
            for pattern in context.gl_patterns:
                if re.search(pattern, case.input_column, re.IGNORECASE):
                    score += 0.2
                    break

        return min(score, 1.0)

    def _calculate_overall_score(self, scores: Dict[str, float]) -> float:
        """Calculate weighted overall score."""
        weights = {
            "extraction": 0.15,
            "entity_detection": 0.25,
            "pattern_consistency": 0.20,
            "hierarchy_potential": 0.25,
            "client_alignment": 0.15,
        }

        total = sum(scores.get(k, 0.5) * w for k, w in weights.items())
        return min(total, 1.0)

    def _get_recommendation(self, score: float) -> str:
        """Get recommendation based on score."""
        if score >= 0.85:
            return "Highly recommended for hierarchy creation"
        elif score >= 0.70:
            return "Recommended with minor review"
        elif score >= 0.50:
            return "Consider with careful review"
        return "Not recommended without significant refinement"

    def _generate_improvement_suggestions(
        self,
        scored_cases: List[Dict[str, Any]],
    ) -> List[str]:
        """Generate suggestions to improve confidence."""
        suggestions = []

        low_entity = [c for c in scored_cases if c["scores"].get("entity_detection", 0) < 0.5]
        if low_entity:
            suggestions.append(
                f"{len(low_entity)} hierarchies have low entity detection. "
                "Consider adding column name patterns to client configuration."
            )

        low_pattern = [c for c in scored_cases if c["scores"].get("pattern_consistency", 0) < 0.6]
        if low_pattern:
            suggestions.append(
                f"{len(low_pattern)} hierarchies have inconsistent patterns. "
                "Review CASE logic for mixed condition types."
            )

        return suggestions


class ExportAgent(BaseAgent):
    """
    AI Agent for intelligent CSV export.

    Generates exports based on:
    - Orchestrator recommendations
    - Client-specific formatting
    - Industry templates
    - Confidence-based filtering
    """

    def __init__(self):
        super().__init__(
            name="ExportAgent",
            description="Generates intelligent CSV exports based on analysis"
        )

    def execute(self, input_data: Dict[str, Any], context: AgentContext) -> AgentResult:
        cases = input_data.get("cases", [])
        scored_cases = input_data.get("scored_cases", [])
        export_config = input_data.get("export_config", {})

        export_path = export_config.get("path", "./result_export")
        export_name = export_config.get("name", "")
        min_confidence = export_config.get("min_confidence", 0.0)

        self.log(f"Generating exports for {len(cases)} hierarchies")

        # Generate intelligent export name if not provided
        if not export_name:
            export_name = self._generate_export_name(cases, context)

        # Filter by confidence if specified
        if min_confidence > 0:
            filtered_indices = [
                i for i, sc in enumerate(scored_cases)
                if sc.get("overall_score", 0) >= min_confidence
            ]
            cases = [cases[i] for i in filtered_indices]
            scored_cases = [scored_cases[i] for i in filtered_indices]

        # Ensure export directory exists
        Path(export_path).mkdir(parents=True, exist_ok=True)

        # Generate CSV files
        exported_files = []

        # 1. Summary CSV
        summary_file = self._export_summary(cases, scored_cases, export_path, export_name, context)
        exported_files.append(summary_file)

        # 2. Hierarchy CSV
        hierarchy_file = self._export_hierarchy(cases, export_path, export_name, context)
        exported_files.append(hierarchy_file)

        # 3. Mapping CSV
        mapping_file = self._export_mapping(cases, export_path, export_name, context)
        exported_files.append(mapping_file)

        # 4. Client-specific exports
        if context.industry != "general":
            industry_file = self._export_industry_specific(cases, export_path, export_name, context)
            if industry_file:
                exported_files.append(industry_file)

        return AgentResult(
            success=True,
            data={
                "export_name": export_name,
                "export_path": export_path,
                "exported_files": exported_files,
                "hierarchy_count": len(cases),
            },
            reasoning=f"Exported {len(exported_files)} files with {len(cases)} hierarchies",
            confidence=1.0,
            suggestions=[],
        )

    def _generate_export_name(
        self,
        cases: List[ExtractedCase],
        context: AgentContext,
    ) -> str:
        """Generate intelligent export name based on content."""
        name_parts = []

        # Add client name if available
        if context.client_id:
            name_parts.append(context.client_id)

        # Add industry
        if context.industry != "general":
            name_parts.append(context.industry.replace("_", ""))

        # Analyze case columns for keywords
        case_columns = [c.source_column.lower() for c in cases]

        # Priority keywords
        keywords = {
            "segment": "segment",
            "fund": "fund",
            "gl": "gl",
            "state": "state",
            "loe": "loe",
            "los": "los",
            "alloc": "alloc",
            "stake": "stake",
        }

        for col in case_columns:
            for keyword, short in keywords.items():
                if keyword in col and short not in name_parts:
                    name_parts.append(short)
                    break

        # Fallback
        if len(name_parts) < 2:
            name_parts.append("hierarchy")

        name_parts.append("analysis")

        return "_".join(name_parts[:5])

    def _export_summary(
        self,
        cases: List[ExtractedCase],
        scored_cases: List[Dict[str, Any]],
        export_path: str,
        export_name: str,
        context: AgentContext,
    ) -> str:
        """Export summary CSV with AI insights."""
        rows = []

        for idx, case in enumerate(cases):
            scores = scored_cases[idx] if idx < len(scored_cases) else {}

            row = {
                "HIERARCHY_NAME": case.source_column,
                "SOURCE_COLUMN": case.input_column,
                "ENTITY_TYPE": case.entity_type.value,
                "PATTERN_TYPE": case.pattern_type or "mixed",
                "TOTAL_CONDITIONS": len(case.when_clauses),
                "UNIQUE_VALUES": len(case.unique_results),
                "HAS_ELSE": "Yes" if case.else_value else "No",
                "CONFIDENCE": f"{scores.get('overall_score', 0.5):.0%}",
                "RECOMMENDATION": scores.get("recommendation", ""),
                "CLIENT": context.client_name or context.client_id or "",
                "INDUSTRY": context.industry,
            }
            rows.append(row)

        file_path = os.path.join(export_path, f"{export_name}_SUMMARY.csv")
        self._write_csv(file_path, rows)
        return file_path

    def _export_hierarchy(
        self,
        cases: List[ExtractedCase],
        export_path: str,
        export_name: str,
        context: AgentContext,
    ) -> str:
        """Export hierarchy tree CSV."""
        rows = []

        for case in cases:
            hier_name = case.source_column
            sort_idx = 0

            # Parent node
            parent_id = hier_name.upper().replace(" ", "_")
            rows.append({
                "HIERARCHY_ID": parent_id,
                "HIERARCHY_NAME": hier_name,
                "PARENT_ID": "",
                "DESCRIPTION": f"Auto-extracted from SQL on {case.input_column}",
                "LEVEL_1": hier_name,
                "LEVEL_2": "",
                "SOURCE_COLUMN": case.input_column,
                "ENTITY_TYPE": case.entity_type.value,
                "IS_LEAF_NODE": "false",
                "SORT_ORDER": sort_idx,
            })
            sort_idx += 1

            # Group by result value
            result_groups = defaultdict(list)
            for when in case.when_clauses:
                result_groups[when.result_value].append(when)

            # Child nodes
            for result_value, whens in result_groups.items():
                child_id = f"{parent_id}_{result_value.upper().replace(' ', '_').replace('/', '_')[:25]}"

                rows.append({
                    "HIERARCHY_ID": child_id,
                    "HIERARCHY_NAME": result_value,
                    "PARENT_ID": parent_id,
                    "DESCRIPTION": f"Maps {case.input_column} to '{result_value}'",
                    "LEVEL_1": hier_name,
                    "LEVEL_2": result_value,
                    "SOURCE_COLUMN": case.input_column,
                    "ENTITY_TYPE": case.entity_type.value,
                    "IS_LEAF_NODE": "true",
                    "SORT_ORDER": sort_idx,
                })
                sort_idx += 1

            # ELSE node
            if case.else_value:
                else_id = f"{parent_id}_ELSE"
                rows.append({
                    "HIERARCHY_ID": else_id,
                    "HIERARCHY_NAME": str(case.else_value),
                    "PARENT_ID": parent_id,
                    "DESCRIPTION": "ELSE - default when no conditions match",
                    "LEVEL_1": hier_name,
                    "LEVEL_2": str(case.else_value),
                    "SOURCE_COLUMN": case.input_column,
                    "ENTITY_TYPE": case.entity_type.value,
                    "IS_LEAF_NODE": "true",
                    "SORT_ORDER": sort_idx,
                })

        file_path = os.path.join(export_path, f"{export_name}_HIERARCHY.csv")
        self._write_csv(file_path, rows)
        return file_path

    def _export_mapping(
        self,
        cases: List[ExtractedCase],
        export_path: str,
        export_name: str,
        context: AgentContext,
    ) -> str:
        """Export mapping CSV with all conditions."""
        rows = []

        for case in cases:
            hier_name = case.source_column
            parent_id = hier_name.upper().replace(" ", "_")

            # Group by result value
            result_groups = defaultdict(list)
            for when in case.when_clauses:
                result_groups[when.result_value].append(when)

            for result_value, whens in result_groups.items():
                child_id = f"{parent_id}_{result_value.upper().replace(' ', '_').replace('/', '_')[:25]}"

                for when in whens:
                    cond = when.condition
                    for value in cond.values:
                        rows.append({
                            "HIERARCHY_ID": child_id,
                            "HIERARCHY_NAME": result_value,
                            "PARENT_HIERARCHY": hier_name,
                            "SOURCE_COLUMN": case.input_column,
                            "CONDITION_TYPE": cond.operator.value,
                            "CONDITION_VALUE": value,
                            "MAPPED_VALUE": result_value,
                            "RAW_CONDITION": cond.raw_condition,
                        })

            # ELSE mapping
            if case.else_value:
                else_id = f"{parent_id}_ELSE"
                rows.append({
                    "HIERARCHY_ID": else_id,
                    "HIERARCHY_NAME": str(case.else_value),
                    "PARENT_HIERARCHY": hier_name,
                    "SOURCE_COLUMN": case.input_column,
                    "CONDITION_TYPE": "ELSE",
                    "CONDITION_VALUE": "*",
                    "MAPPED_VALUE": str(case.else_value),
                    "RAW_CONDITION": "ELSE",
                })

        file_path = os.path.join(export_path, f"{export_name}_MAPPING.csv")
        self._write_csv(file_path, rows)
        return file_path

    def _export_industry_specific(
        self,
        cases: List[ExtractedCase],
        export_path: str,
        export_name: str,
        context: AgentContext,
    ) -> Optional[str]:
        """Export industry-specific enhanced CSV."""
        if context.industry == "oil_gas":
            return self._export_oil_gas_enhanced(cases, export_path, export_name, context)
        return None

    def _export_oil_gas_enhanced(
        self,
        cases: List[ExtractedCase],
        export_path: str,
        export_name: str,
        context: AgentContext,
    ) -> str:
        """Export Oil & Gas enhanced analysis."""
        rows = []

        for case in cases:
            row = {
                "HIERARCHY_NAME": case.source_column,
                "SOURCE_COLUMN": case.input_column,
                "ENTITY_TYPE": case.entity_type.value,
                "IS_LOE_CATEGORY": "Yes" if any(
                    p in case.source_column.lower()
                    for p in ["loe", "los", "billcat", "opex"]
                ) else "No",
                "IS_AFE_RELATED": "Yes" if any(
                    p in case.input_column.lower()
                    for p in ["afe", "wbs", "project"]
                ) else "No",
                "IS_JIB_RELATED": "Yes" if any(
                    p in case.input_column.lower()
                    for p in ["jib", "partner", "owner", "interest"]
                ) else "No",
                "SUGGESTED_LOS_CATEGORY": self._suggest_los_category(case),
            }
            rows.append(row)

        file_path = os.path.join(export_path, f"{export_name}_OIL_GAS_ANALYSIS.csv")
        self._write_csv(file_path, rows)
        return file_path

    def _suggest_los_category(self, case: ExtractedCase) -> str:
        """Suggest LOS category mapping."""
        col = case.source_column.lower()

        if "sev" in col or "tax" in col:
            return "Severance Taxes"
        if "trans" in col or "gather" in col:
            return "Transportation & Gathering"
        if "compress" in col:
            return "Compression"
        if "treat" in col or "process" in col:
            return "Processing & Treating"
        if "labor" in col or "payroll" in col:
            return "Direct Labor"
        if "chem" in col:
            return "Chemicals"
        if "util" in col or "power" in col or "elec" in col:
            return "Utilities"

        return "Other Operating"

    def _write_csv(self, file_path: str, rows: List[Dict[str, Any]]) -> None:
        """Write rows to CSV file."""
        if not rows:
            return

        with open(file_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)


# ==============================================================================
# ORCHESTRATOR AGENT
# ==============================================================================

class SQLAnalysisOrchestrator:
    """
    AI Orchestrator that coordinates all specialized agents.

    Responsibilities:
    - Load client context and knowledge base
    - Coordinate agent execution pipeline
    - Make strategic decisions based on results
    - Adapt processing based on client needs
    - Generate final recommendations
    """

    def __init__(self):
        self.name = "SQLAnalysisOrchestrator"
        self.agents = {
            "extraction": CaseExtractionAgent(),
            "entity": EntityDetectionAgent(),
            "pattern": PatternDetectionAgent(),
            "confidence": ConfidenceScoringAgent(),
            "export": ExportAgent(),
        }
        self.execution_log: List[str] = []

    def log(self, message: str) -> None:
        """Log orchestrator activity."""
        timestamp = datetime.now().isoformat()
        self.execution_log.append(f"[{timestamp}] Orchestrator: {message}")

    def analyze(
        self,
        sql: str,
        client_id: Optional[str] = None,
        industry: str = "general",
        export_path: str = "./result_export",
        export_name: str = "",
        min_confidence: float = 0.0,
        user_intent: str = "",
        custom_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Orchestrate full SQL hierarchy analysis.

        Args:
            sql: SQL containing CASE statements
            client_id: Optional client ID for knowledge base lookup
            industry: Industry context (oil_gas, manufacturing, saas, etc.)
            export_path: Directory for CSV exports
            export_name: Base name for export files (auto-generated if empty)
            min_confidence: Minimum confidence threshold for exports
            user_intent: Description of user's goal
            custom_context: Additional context data

        Returns:
            Comprehensive analysis results with AI insights
        """
        self.log(f"Starting analysis for industry: {industry}")

        # Build context
        context = self._build_context(
            client_id=client_id,
            industry=industry,
            user_intent=user_intent,
            custom_context=custom_context,
        )

        # Pipeline execution
        results = {}

        # Step 1: CASE Extraction
        self.log("Step 1: Running CASE extraction agent")
        extraction_result = self.agents["extraction"].execute({"sql": sql}, context)
        results["extraction"] = extraction_result

        if not extraction_result.success:
            return self._build_error_response("No CASE statements found", results)

        cases = extraction_result.data.get("cases", [])
        self.log(f"Extracted {len(cases)} CASE statements")

        # Step 2: Entity Detection
        self.log("Step 2: Running entity detection agent")
        entity_result = self.agents["entity"].execute({"cases": cases}, context)
        results["entity"] = entity_result
        cases = entity_result.data.get("cases", cases)

        # Step 3: Pattern Detection
        self.log("Step 3: Running pattern detection agent")
        pattern_result = self.agents["pattern"].execute({"cases": cases}, context)
        results["pattern"] = pattern_result
        cases = pattern_result.data.get("cases", cases)
        pattern_analysis = pattern_result.data.get("pattern_analysis", [])

        # Step 4: Confidence Scoring
        self.log("Step 4: Running confidence scoring agent")
        confidence_result = self.agents["confidence"].execute({
            "cases": cases,
            "pattern_analysis": pattern_analysis,
        }, context)
        results["confidence"] = confidence_result
        scored_cases = confidence_result.data.get("scored_cases", [])

        # Step 5: Export Generation
        self.log("Step 5: Running export agent")
        export_result = self.agents["export"].execute({
            "cases": cases,
            "scored_cases": scored_cases,
            "export_config": {
                "path": export_path,
                "name": export_name,
                "min_confidence": min_confidence,
            },
        }, context)
        results["export"] = export_result

        # Build final response
        return self._build_success_response(results, context)

    def _build_context(
        self,
        client_id: Optional[str],
        industry: str,
        user_intent: str,
        custom_context: Optional[Dict[str, Any]],
    ) -> AgentContext:
        """Build agent context from inputs and knowledge base."""
        context = AgentContext(
            client_id=client_id,
            industry=industry,
            user_intent=user_intent,
        )

        # Load client knowledge base if available
        if client_id:
            client_kb = self._load_client_knowledge(client_id)
            if client_kb:
                context.client_name = client_kb.get("client_name")
                context.erp_system = client_kb.get("erp_system")
                context.custom_prompts = client_kb.get("custom_prompts", [])
                context.gl_patterns = client_kb.get("gl_patterns", {})

                # Override industry if specified in client KB
                if client_kb.get("industry"):
                    context.industry = client_kb["industry"]

        # Add custom context
        if custom_context:
            context.additional_context = custom_context

        return context

    def _load_client_knowledge(self, client_id: str) -> Optional[Dict[str, Any]]:
        """Load client knowledge base."""
        # Try to load from knowledge base
        kb_path = Path(f"./knowledge_base/clients/{client_id}/config.json")
        if kb_path.exists():
            try:
                with open(kb_path) as f:
                    return json.load(f)
            except Exception:
                pass
        return None

    def _build_error_response(
        self,
        error: str,
        results: Dict[str, AgentResult],
    ) -> Dict[str, Any]:
        """Build error response."""
        return {
            "success": False,
            "error": error,
            "agent_results": {
                name: {
                    "success": r.success,
                    "reasoning": r.reasoning,
                    "confidence": r.confidence,
                }
                for name, r in results.items()
            },
            "execution_log": self.execution_log,
        }

    def _build_success_response(
        self,
        results: Dict[str, AgentResult],
        context: AgentContext,
    ) -> Dict[str, Any]:
        """Build success response with all insights."""
        export_data = results["export"].data
        confidence_data = results["confidence"].data

        # Collect all suggestions
        all_suggestions = []
        for name, result in results.items():
            all_suggestions.extend(result.suggestions)

        return {
            "success": True,
            "summary": {
                "hierarchies_found": len(confidence_data.get("scored_cases", [])),
                "average_confidence": confidence_data.get("average_confidence", 0),
                "files_exported": len(export_data.get("exported_files", [])),
                "industry": context.industry,
                "client": context.client_id,
            },
            "export": {
                "name": export_data.get("export_name"),
                "path": export_data.get("export_path"),
                "files": export_data.get("exported_files", []),
            },
            "hierarchies": [
                {
                    "name": sc["hierarchy_name"],
                    "confidence": sc["overall_score"],
                    "recommendation": sc["recommendation"],
                    "scores": sc["scores"],
                }
                for sc in confidence_data.get("scored_cases", [])
            ],
            "agent_insights": {
                name: {
                    "reasoning": r.reasoning,
                    "confidence": r.confidence,
                    "suggestions": r.suggestions,
                }
                for name, r in results.items()
            },
            "suggestions": list(set(all_suggestions)),
            "execution_log": self.execution_log,
        }


# ==============================================================================
# MCP TOOL REGISTRATION
# ==============================================================================

def register_ai_sql_tools(mcp) -> None:
    """Register AI-enhanced SQL analysis tools with the MCP server."""

    @mcp.tool()
    def ai_analyze_sql_hierarchies(
        sql: str,
        client_id: str = "",
        industry: str = "general",
        export_path: str = "./result_export",
        export_name: str = "",
        min_confidence: float = 0.0,
        user_intent: str = "",
    ) -> Dict[str, Any]:
        """
        AI-powered SQL hierarchy analysis with intelligent agents.

        Uses specialized AI agents to analyze SQL CASE statements:
        - CaseExtractionAgent: Intelligent CASE statement extraction
        - EntityDetectionAgent: Context-aware entity type detection
        - PatternDetectionAgent: Smart pattern analysis for hierarchy building
        - ConfidenceScoringAgent: Multi-factor confidence calculation
        - ExportAgent: Intelligent CSV generation

        All agents are coordinated by an Orchestrator that:
        - Loads client-specific knowledge and preferences
        - Adapts analysis based on industry context
        - Makes strategic decisions about hierarchy creation
        - Generates tailored recommendations

        Args:
            sql: SQL containing CASE statements to analyze
            client_id: Client ID for knowledge base lookup (optional)
            industry: Industry context - oil_gas, manufacturing, saas, general, etc.
            export_path: Directory for CSV exports (default: ./result_export)
            export_name: Base name for files (auto-generated if empty)
            min_confidence: Minimum confidence to include in exports (0.0-1.0)
            user_intent: Description of what you're trying to achieve

        Returns:
            Comprehensive analysis with:
            - summary: Overview of findings
            - export: Exported file details
            - hierarchies: Detailed hierarchy analysis with confidence scores
            - agent_insights: Reasoning from each specialized agent
            - suggestions: Recommendations for improvement

        Example:
            >>> result = ai_analyze_sql_hierarchies(
            ...     sql="SELECT CASE WHEN account LIKE '4%' THEN 'Revenue' ...",
            ...     client_id="acme_oil",
            ...     industry="oil_gas",
            ...     user_intent="Build financial reporting hierarchy"
            ... )
            >>> print(result["summary"]["average_confidence"])
            0.85
        """
        orchestrator = SQLAnalysisOrchestrator()

        return orchestrator.analyze(
            sql=sql,
            client_id=client_id if client_id else None,
            industry=industry,
            export_path=export_path,
            export_name=export_name,
            min_confidence=min_confidence,
            user_intent=user_intent,
        )

    @mcp.tool()
    def configure_client_sql_patterns(
        client_id: str,
        gl_patterns: str = "",
        entity_mappings: str = "",
        custom_rules: str = "",
    ) -> Dict[str, Any]:
        """
        Configure client-specific patterns for AI SQL analysis.

        Allows customization of how the AI agents analyze SQL for a specific client.
        These patterns are used by the EntityDetectionAgent and PatternDetectionAgent
        to provide more accurate, client-specific results.

        Args:
            client_id: Client ID to configure
            gl_patterns: JSON string of column pattern to entity type mappings
                Example: {"well_.*": "location", "afe_.*": "project"}
            entity_mappings: JSON string of result value to category mappings
                Example: {"Revenue - Oil": "Oil Sales", "Revenue - Gas": "Gas Sales"}
            custom_rules: JSON string of custom processing rules
                Example: {"skip_columns": ["temp_*"], "force_entity": {"corp_code": "entity"}}

        Returns:
            Configuration status and applied settings

        Example:
            >>> configure_client_sql_patterns(
            ...     client_id="acme_oil",
            ...     gl_patterns='{"well_.*": "location", "afe_.*": "project"}',
            ...     entity_mappings='{"Revenue - Oil": "Oil Sales"}'
            ... )
        """
        # Parse inputs
        try:
            gl_dict = json.loads(gl_patterns) if gl_patterns else {}
        except json.JSONDecodeError:
            gl_dict = {}

        try:
            entity_dict = json.loads(entity_mappings) if entity_mappings else {}
        except json.JSONDecodeError:
            entity_dict = {}

        try:
            rules_dict = json.loads(custom_rules) if custom_rules else {}
        except json.JSONDecodeError:
            rules_dict = {}

        # Create client knowledge base directory
        kb_path = Path(f"./knowledge_base/clients/{client_id}")
        kb_path.mkdir(parents=True, exist_ok=True)

        # Load existing config or create new
        config_path = kb_path / "config.json"
        if config_path.exists():
            with open(config_path) as f:
                config = json.load(f)
        else:
            config = {
                "client_id": client_id,
                "client_name": client_id,
                "industry": "general",
            }

        # Update with new patterns
        config["gl_patterns"] = {**config.get("gl_patterns", {}), **gl_dict}
        config["entity_mappings"] = {**config.get("entity_mappings", {}), **entity_dict}
        config["custom_rules"] = {**config.get("custom_rules", {}), **rules_dict}
        config["updated_at"] = datetime.now().isoformat()

        # Save config
        with open(config_path, "w") as f:
            json.dump(config, f, indent=2)

        return {
            "success": True,
            "client_id": client_id,
            "config_path": str(config_path),
            "gl_patterns_count": len(config.get("gl_patterns", {})),
            "entity_mappings_count": len(config.get("entity_mappings", {})),
            "custom_rules_count": len(config.get("custom_rules", {})),
        }
