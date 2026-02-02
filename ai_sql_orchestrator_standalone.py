"""
AI-Enhanced SQL Hierarchy Analysis Orchestrator (Standalone Version).

This module provides AI agent-powered SQL analysis without MCP dependencies.

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

Usage:
    from ai_sql_orchestrator_standalone import SQLAnalysisOrchestrator

    orchestrator = SQLAnalysisOrchestrator()
    result = orchestrator.analyze(
        sql="SELECT CASE WHEN account LIKE '4%' THEN 'Revenue' ...",
        client_id="my_client",
        industry="oil_gas",
    )
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


# ==============================================================================
# ENUMS AND DATA CLASSES
# ==============================================================================

class EntityType(str, Enum):
    """Entity types that can be detected from SQL."""
    ACCOUNT = "account"
    COST_CENTER = "cost_center"
    DEPARTMENT = "department"
    ENTITY = "entity"
    PROJECT = "project"
    PRODUCT = "product"
    CUSTOMER = "customer"
    VENDOR = "vendor"
    EMPLOYEE = "employee"
    LOCATION = "location"
    TIME_PERIOD = "time_period"
    CURRENCY = "currency"
    UNKNOWN = "unknown"


class ConditionOperator(str, Enum):
    """SQL condition operators."""
    EQUALS = "="
    NOT_EQUALS = "<>"
    LIKE = "LIKE"
    ILIKE = "ILIKE"
    IN = "IN"
    BETWEEN = "BETWEEN"
    IS_NULL = "IS NULL"
    GREATER_THAN = ">"
    GREATER_EQUAL = ">="
    LESS_THAN = "<"
    LESS_EQUAL = "<="
    AND = "AND"
    OR = "OR"


# Patterns for entity type detection
ENTITY_PATTERNS = {
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
        r"corp[_\s]*(code|id)?",
    ],
    EntityType.PRODUCT: [
        r"product[_\s]*(code|id|name)?",
        r"sku",
        r"item[_\s]*(code|id)?",
    ],
    EntityType.LOCATION: [
        r"location[_\s]*(code|id)?",
        r"site[_\s]*(code|id)?",
        r"facility",
        r"warehouse",
    ],
}

# Financial hierarchy patterns based on result values
FINANCIAL_PATTERNS = [
    "Revenue", "Sales", "Income", "COGS", "Cost of Goods",
    "Gross Profit", "Operating Expenses", "SG&A", "R&D",
    "EBITDA", "Depreciation", "Interest", "Tax", "Net Income",
    "Cash", "Accounts Receivable", "Inventory", "Fixed Assets",
    "Accounts Payable", "Debt", "Equity", "Retained Earnings",
]


@dataclass
class CaseCondition:
    """A condition from a CASE WHEN clause."""
    column: str
    operator: ConditionOperator
    values: List[str]
    raw_condition: str
    is_negated: bool = False


@dataclass
class CaseWhen:
    """A WHEN clause from a CASE statement."""
    condition: CaseCondition
    result_value: str
    position: int
    raw_sql: str


@dataclass
class ExtractedCase:
    """An extracted CASE statement with metadata."""
    id: str
    source_column: str
    input_column: str
    input_table: Optional[str]
    when_clauses: List[CaseWhen]
    else_value: Optional[str]
    entity_type: EntityType
    pattern_type: Optional[str]
    unique_results: List[str]
    raw_sql: str = ""


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


# ==============================================================================
# BASE CASE EXTRACTOR (Regex-based)
# ==============================================================================

class SimpleCaseExtractor:
    """Simple CASE statement extractor using regex patterns."""

    def __init__(self, dialect: str = "snowflake"):
        self.dialect = dialect

    def extract_from_sql(self, sql: str) -> List[ExtractedCase]:
        """Extract CASE statements from SQL using regex."""
        cases = []
        case_pattern = r"CASE\s+(.*?)\s+END(?:\s+AS\s+(\w+))?"

        for idx, match in enumerate(re.finditer(case_pattern, sql, re.IGNORECASE | re.DOTALL)):
            case_body = match.group(1)
            alias = match.group(2) or f"case_column_{idx}"

            case_stmt = self._parse_case_body(case_body, alias, idx, match.group(0))
            if case_stmt:
                cases.append(case_stmt)

        return cases

    def _parse_case_body(
        self,
        case_body: str,
        alias: str,
        position: int,
        raw_sql: str,
    ) -> Optional[ExtractedCase]:
        """Parse the body of a CASE statement."""
        when_clauses = []
        else_value = None
        input_column = None

        # Extract WHEN clauses
        when_pattern = r"WHEN\s+(.*?)\s+THEN\s+['\"]?([^'\"]+?)['\"]?\s*(?=WHEN|ELSE|$)"

        for when_idx, when_match in enumerate(re.finditer(when_pattern, case_body, re.IGNORECASE | re.DOTALL)):
            condition_str = when_match.group(1).strip()
            result_value = when_match.group(2).strip().strip("'\"")

            condition = self._parse_condition(condition_str)
            if condition and not input_column:
                input_column = condition.column

            when_clauses.append(CaseWhen(
                condition=condition or CaseCondition(
                    column="unknown",
                    operator=ConditionOperator.EQUALS,
                    values=[],
                    raw_condition=condition_str,
                ),
                result_value=result_value,
                position=when_idx,
                raw_sql=when_match.group(0),
            ))

        # Extract ELSE value
        else_pattern = r"ELSE\s+['\"]?([^'\"]+?)['\"]?\s*$"
        else_match = re.search(else_pattern, case_body, re.IGNORECASE)
        if else_match:
            else_value = else_match.group(1).strip().strip("'\"")

        if not when_clauses:
            return None

        case_id = hashlib.md5(raw_sql.encode()).hexdigest()[:12]
        entity_type = self._detect_entity_type(input_column, when_clauses)
        pattern_type = self._detect_pattern_type(when_clauses)
        unique_results = list(set(w.result_value for w in when_clauses))
        if else_value:
            unique_results.append(else_value)

        return ExtractedCase(
            id=case_id,
            source_column=alias,
            input_column=input_column or "unknown",
            input_table=None,
            when_clauses=when_clauses,
            else_value=else_value,
            entity_type=entity_type,
            pattern_type=pattern_type,
            unique_results=unique_results,
            raw_sql=raw_sql,
        )

    def _parse_condition(self, condition_str: str) -> Optional[CaseCondition]:
        """Parse a condition string into CaseCondition."""
        condition_str = condition_str.strip()

        # ILIKE ANY pattern
        ilike_any_pattern = r"(\w+)\s+ILIKE\s+ANY\s*\(([^)]+)\)"
        ilike_any_match = re.match(ilike_any_pattern, condition_str, re.IGNORECASE)
        if ilike_any_match:
            values = [v.strip().strip("'\"") for v in ilike_any_match.group(2).split(",")]
            return CaseCondition(
                column=ilike_any_match.group(1),
                operator=ConditionOperator.ILIKE,
                values=values,
                raw_condition=condition_str,
            )

        # LIKE pattern
        like_pattern = r"(\w+)\s+(I?LIKE)\s+['\"]([^'\"]+)['\"]"
        like_match = re.match(like_pattern, condition_str, re.IGNORECASE)
        if like_match:
            return CaseCondition(
                column=like_match.group(1),
                operator=ConditionOperator.ILIKE if "ILIKE" in like_match.group(2).upper() else ConditionOperator.LIKE,
                values=[like_match.group(3)],
                raw_condition=condition_str,
            )

        # IN pattern
        in_pattern = r"(\w+)\s+IN\s*\(([^)]+)\)"
        in_match = re.match(in_pattern, condition_str, re.IGNORECASE)
        if in_match:
            values = [v.strip().strip("'\"") for v in in_match.group(2).split(",")]
            return CaseCondition(
                column=in_match.group(1),
                operator=ConditionOperator.IN,
                values=values,
                raw_condition=condition_str,
            )

        # BETWEEN pattern
        between_pattern = r"(\w+)\s+BETWEEN\s+['\"]?([^'\"]+)['\"]?\s+AND\s+['\"]?([^'\"]+)['\"]?"
        between_match = re.match(between_pattern, condition_str, re.IGNORECASE)
        if between_match:
            return CaseCondition(
                column=between_match.group(1),
                operator=ConditionOperator.BETWEEN,
                values=[between_match.group(2), between_match.group(3)],
                raw_condition=condition_str,
            )

        # Equals pattern
        eq_pattern = r"(\w+)\s*=\s*['\"]?([^'\"]+)['\"]?"
        eq_match = re.match(eq_pattern, condition_str, re.IGNORECASE)
        if eq_match:
            return CaseCondition(
                column=eq_match.group(1),
                operator=ConditionOperator.EQUALS,
                values=[eq_match.group(2).strip()],
                raw_condition=condition_str,
            )

        return None

    def _detect_entity_type(
        self,
        column_name: Optional[str],
        when_clauses: List[CaseWhen],
    ) -> EntityType:
        """Detect entity type from column name and patterns."""
        if not column_name:
            return EntityType.UNKNOWN

        column_lower = column_name.lower()

        for entity_type, patterns in ENTITY_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, column_lower, re.IGNORECASE):
                    return entity_type

        # Check result values for financial patterns
        result_values = [w.result_value for w in when_clauses]
        matches = sum(
            1 for v in result_values
            if any(p.lower() in v.lower() for p in FINANCIAL_PATTERNS)
        )
        if matches >= 3:
            return EntityType.ACCOUNT

        return EntityType.UNKNOWN

    def _detect_pattern_type(self, when_clauses: List[CaseWhen]) -> Optional[str]:
        """Detect the pattern type used in conditions."""
        if not when_clauses:
            return None

        pattern_counts: Dict[str, int] = defaultdict(int)

        for when in when_clauses:
            condition = when.condition
            if condition.operator in (ConditionOperator.LIKE, ConditionOperator.ILIKE):
                for value in condition.values:
                    if value.endswith("%") and not value.startswith("%"):
                        pattern_counts["prefix"] += 1
                    elif value.startswith("%") and not value.endswith("%"):
                        pattern_counts["suffix"] += 1
                    elif value.startswith("%") and value.endswith("%"):
                        pattern_counts["contains"] += 1
                    else:
                        pattern_counts["exact"] += 1
            elif condition.operator == ConditionOperator.IN:
                pattern_counts["exact_list"] += 1
            elif condition.operator == ConditionOperator.EQUALS:
                pattern_counts["exact"] += 1
            elif condition.operator == ConditionOperator.BETWEEN:
                pattern_counts["range"] += 1

        if not pattern_counts:
            return None

        return max(pattern_counts.items(), key=lambda x: x[1])[0]


# ==============================================================================
# AI AGENT BASE CLASS
# ==============================================================================

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
        timestamp = datetime.now().strftime("%H:%M:%S")
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
    """AI Agent for intelligent CASE statement extraction."""

    def __init__(self):
        super().__init__(
            name="CaseExtractionAgent",
            description="Extracts and analyzes CASE statements from SQL"
        )
        self.base_extractor = SimpleCaseExtractor()

    def execute(self, input_data: Dict[str, Any], context: AgentContext) -> AgentResult:
        sql = input_data.get("sql", "")
        self.log(f"Starting extraction for {context.industry} industry")

        # Get client-specific extraction rules
        client_rules = self.get_client_specific_rules(context, "sql_extraction")

        # Base extraction using regex
        cases = self.base_extractor.extract_from_sql(sql)

        # AI Enhancement: Analyze and enhance
        reasoning_parts = []
        suggestions = []
        modifications = {}

        # Check for nested CASE statements
        nested_count = sql.upper().count("CASE") - len(cases)
        if nested_count > 0:
            reasoning_parts.append(f"Detected {nested_count} nested CASE statements")
            suggestions.append("Review nested CASE logic for additional hierarchy levels")

        # Industry-specific enhancements
        if context.industry == "oil_gas":
            og_patterns = self._detect_oil_gas_patterns(sql, cases)
            if og_patterns:
                modifications["oil_gas_enhancements"] = og_patterns
                reasoning_parts.append(f"Applied Oil & Gas patterns: {list(og_patterns.keys())}")

        # Enhance cases
        enhanced_cases = []
        for case in cases:
            enhanced = self._enhance_case(case, context)
            enhanced_cases.append(enhanced)

        confidence = self._calculate_confidence(enhanced_cases, sql)

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

        if "loe" in sql_lower or "los" in sql_lower or "lease operating" in sql_lower:
            patterns["loe_hierarchy"] = True
        if "afe" in sql_lower or "authorization for expenditure" in sql_lower:
            patterns["afe_tracking"] = True
        if "jib" in sql_lower or "joint interest" in sql_lower:
            patterns["jib_allocation"] = True
        if "well" in sql_lower or "field" in sql_lower or "basin" in sql_lower:
            patterns["asset_hierarchy"] = True

        return patterns

    def _enhance_case(self, case: ExtractedCase, context: AgentContext) -> ExtractedCase:
        """Enhance a CASE statement with AI-derived metadata."""
        if context.industry == "oil_gas":
            case = self._refine_oil_gas_entity(case)
        elif context.industry == "manufacturing":
            case = self._refine_manufacturing_entity(case)
        return case

    def _refine_oil_gas_entity(self, case: ExtractedCase) -> ExtractedCase:
        """Refine entity type for Oil & Gas industry."""
        col_lower = case.input_column.lower()
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

    def _calculate_confidence(self, cases: List[ExtractedCase], sql: str) -> float:
        """Calculate confidence in the extraction."""
        if not cases:
            return 0.0

        confidence = 0.6
        if len(cases) >= 5:
            confidence += 0.15
        elif len(cases) >= 3:
            confidence += 0.1

        aliased = sum(1 for c in cases if not c.source_column.startswith("case_column_"))
        if aliased == len(cases):
            confidence += 0.1

        pattern_types = set(c.pattern_type for c in cases if c.pattern_type)
        if len(pattern_types) <= 2:
            confidence += 0.1

        return min(confidence, 1.0)


class EntityDetectionAgent(BaseAgent):
    """AI Agent for intelligent entity type detection."""

    def __init__(self):
        super().__init__(
            name="EntityDetectionAgent",
            description="Detects and classifies entity types from SQL patterns"
        )

    def execute(self, input_data: Dict[str, Any], context: AgentContext) -> AgentResult:
        cases = input_data.get("cases", [])
        self.log(f"Detecting entities for {len(cases)} CASE statements")

        client_gl_patterns = context.gl_patterns

        enhanced_cases = []
        reasoning_parts = []
        entity_summary = defaultdict(int)

        for case in cases:
            detected_type, detection_reasoning = self._detect_entity_with_context(
                case, context, client_gl_patterns
            )

            if detected_type != EntityType.UNKNOWN:
                case.entity_type = detected_type
                reasoning_parts.append(f"{case.source_column}: {detected_type.value}")

            entity_summary[case.entity_type.value] += 1
            enhanced_cases.append(case)

        known_entities = sum(1 for c in enhanced_cases if c.entity_type != EntityType.UNKNOWN)
        confidence = known_entities / len(enhanced_cases) if enhanced_cases else 0.0

        suggestions = self._generate_suggestions(enhanced_cases, context)

        return AgentResult(
            success=True,
            data={"cases": enhanced_cases, "entity_summary": dict(entity_summary)},
            reasoning=" | ".join(reasoning_parts[:5]) if reasoning_parts else "Entity detection completed",
            confidence=confidence,
            suggestions=suggestions,
        )

    def _detect_entity_with_context(
        self,
        case: ExtractedCase,
        context: AgentContext,
        client_gl_patterns: Dict[str, str],
    ) -> tuple:
        """Detect entity type using context-aware analysis."""
        column = case.input_column.lower()

        # Check client-specific patterns first
        for pattern, entity_type_str in client_gl_patterns.items():
            if re.search(pattern, column, re.IGNORECASE):
                try:
                    return EntityType(entity_type_str), "client GL pattern"
                except ValueError:
                    pass

        # Industry-specific detection
        if context.industry == "oil_gas":
            detected = self._detect_oil_gas_entity(column, case)
            if detected != EntityType.UNKNOWN:
                return detected, "oil & gas pattern"

        elif context.industry == "manufacturing":
            detected = self._detect_manufacturing_entity(column, case)
            if detected != EntityType.UNKNOWN:
                return detected, "manufacturing pattern"

        # Standard pattern matching
        for entity_type, patterns in ENTITY_PATTERNS.items():
            for pattern in patterns:
                if re.search(pattern, column, re.IGNORECASE):
                    return entity_type, "standard pattern"

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
        return EntityType.UNKNOWN

    def _generate_suggestions(self, cases: List[ExtractedCase], context: AgentContext) -> List[str]:
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
    """AI Agent for intelligent pattern detection."""

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

            if analysis.get("suggested_pattern_type"):
                case.pattern_type = analysis["suggested_pattern_type"]

        hierarchy_recommendations = self._generate_hierarchy_recommendations(cases, pattern_analysis, context)
        confidence = self._calculate_confidence(pattern_analysis)

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

    def _analyze_case_patterns(self, case: ExtractedCase, context: AgentContext) -> Dict[str, Any]:
        """Analyze patterns in a CASE statement."""
        analysis = {
            "source_column": case.source_column,
            "input_column": case.input_column,
            "original_pattern_type": case.pattern_type,
            "suggested_pattern_type": None,
            "pattern_breakdown": defaultdict(int),
            "hierarchy_potential": "low",
            "grouping_strategy": None,
        }

        for when in case.when_clauses:
            cond = when.condition
            pattern = self._classify_condition_pattern(cond)
            analysis["pattern_breakdown"][pattern] += 1

        if analysis["pattern_breakdown"]:
            dominant = max(analysis["pattern_breakdown"].items(), key=lambda x: x[1])
            analysis["suggested_pattern_type"] = dominant[0]

        analysis["hierarchy_potential"] = self._assess_hierarchy_potential(case, analysis)
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

    def _assess_hierarchy_potential(self, case: ExtractedCase, analysis: Dict[str, Any]) -> str:
        """Assess how suitable this CASE is for hierarchy creation."""
        score = 0

        if len(case.unique_results) >= 10:
            score += 3
        elif len(case.unique_results) >= 5:
            score += 2
        elif len(case.unique_results) >= 3:
            score += 1

        if analysis.get("suggested_pattern_type") == "prefix":
            score += 2

        if case.entity_type != EntityType.UNKNOWN:
            score += 2

        if len(case.when_clauses) > len(case.unique_results) * 1.5:
            score += 1

        if score >= 6:
            return "high"
        elif score >= 3:
            return "medium"
        return "low"

    def _suggest_grouping(self, case: ExtractedCase, analysis: Dict[str, Any], context: AgentContext) -> Optional[str]:
        """Suggest a grouping strategy for hierarchy building."""
        if analysis["suggested_pattern_type"] == "prefix":
            return "prefix_rollup"
        if analysis["suggested_pattern_type"] == "range":
            return "range_buckets"
        if len(case.unique_results) > 20:
            return "category_grouping"
        return "flat"

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
                    "suggested_levels": 2 if analysis["grouping_strategy"] == "prefix_rollup" else 2,
                    "grouping_strategy": analysis["grouping_strategy"],
                    "notes": [],
                }
                if context.industry == "oil_gas" and case.entity_type == EntityType.ACCOUNT:
                    rec["notes"].append("Consider aligning with LOS/LOE categories")
                recommendations.append(rec)

        return recommendations

    def _calculate_confidence(self, analyses: List[Dict[str, Any]]) -> float:
        """Calculate confidence in pattern detection."""
        if not analyses:
            return 0.0
        high_potential = sum(1 for a in analyses if a["hierarchy_potential"] == "high")
        med_potential = sum(1 for a in analyses if a["hierarchy_potential"] == "medium")
        return min((high_potential * 0.15 + med_potential * 0.1) + 0.5, 1.0)


class ConfidenceScoringAgent(BaseAgent):
    """AI Agent for intelligent confidence scoring."""

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
            data={"cases": cases, "scored_cases": scored_cases, "average_confidence": avg_confidence},
            reasoning=f"Average confidence: {avg_confidence:.0%}",
            confidence=avg_confidence,
            suggestions=self._generate_suggestions(scored_cases),
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
        scores["extraction"] = 0.7
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
        scores["hierarchy_potential"] = potential_map.get(analysis.get("hierarchy_potential", "low"), 0.5)

        # Client alignment score
        scores["client_alignment"] = self._calculate_client_alignment(case, context)

        return scores

    def _calculate_client_alignment(self, case: ExtractedCase, context: AgentContext) -> float:
        """Calculate how well this aligns with client needs."""
        score = 0.6
        if context.industry != "general" and case.entity_type != EntityType.UNKNOWN:
            score += 0.2
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

    def _generate_suggestions(self, scored_cases: List[Dict[str, Any]]) -> List[str]:
        """Generate suggestions to improve confidence."""
        suggestions = []
        low_entity = [c for c in scored_cases if c["scores"].get("entity_detection", 0) < 0.5]
        if low_entity:
            suggestions.append(f"{len(low_entity)} hierarchies have low entity detection.")
        low_pattern = [c for c in scored_cases if c["scores"].get("pattern_consistency", 0) < 0.6]
        if low_pattern:
            suggestions.append(f"{len(low_pattern)} hierarchies have inconsistent patterns.")
        return suggestions


class ExportAgent(BaseAgent):
    """AI Agent for intelligent CSV export."""

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

        summary_file = self._export_summary(cases, scored_cases, export_path, export_name, context)
        exported_files.append(summary_file)

        hierarchy_file = self._export_hierarchy(cases, export_path, export_name, context)
        exported_files.append(hierarchy_file)

        mapping_file = self._export_mapping(cases, export_path, export_name, context)
        exported_files.append(mapping_file)

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

    def _generate_export_name(self, cases: List[ExtractedCase], context: AgentContext) -> str:
        """Generate intelligent export name based on content."""
        name_parts = []

        if context.client_id:
            name_parts.append(context.client_id)

        if context.industry != "general":
            name_parts.append(context.industry.replace("_", ""))

        case_columns = [c.source_column.lower() for c in cases]

        keywords = {
            "segment": "segment", "fund": "fund", "gl": "gl", "state": "state",
            "loe": "loe", "los": "los", "alloc": "alloc", "stake": "stake",
        }

        for col in case_columns:
            for keyword, short in keywords.items():
                if keyword in col and short not in name_parts:
                    name_parts.append(short)
                    break

        if len(name_parts) < 2:
            name_parts.append("hierarchy")

        name_parts.append("analysis")
        return "_".join(name_parts[:5])

    def _export_summary(self, cases, scored_cases, export_path, export_name, context) -> str:
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

    def _export_hierarchy(self, cases, export_path, export_name, context) -> str:
        """Export hierarchy tree CSV."""
        rows = []

        for case in cases:
            hier_name = case.source_column
            sort_idx = 0

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

            result_groups = defaultdict(list)
            for when in case.when_clauses:
                result_groups[when.result_value].append(when)

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

    def _export_mapping(self, cases, export_path, export_name, context) -> str:
        """Export mapping CSV with all conditions."""
        rows = []

        for case in cases:
            hier_name = case.source_column
            parent_id = hier_name.upper().replace(" ", "_")

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

    def _export_industry_specific(self, cases, export_path, export_name, context) -> Optional[str]:
        """Export industry-specific enhanced CSV."""
        if context.industry == "oil_gas":
            return self._export_oil_gas_enhanced(cases, export_path, export_name, context)
        return None

    def _export_oil_gas_enhanced(self, cases, export_path, export_name, context) -> str:
        """Export Oil & Gas enhanced analysis."""
        rows = []
        for case in cases:
            row = {
                "HIERARCHY_NAME": case.source_column,
                "SOURCE_COLUMN": case.input_column,
                "ENTITY_TYPE": case.entity_type.value,
                "IS_LOE_CATEGORY": "Yes" if any(
                    p in case.source_column.lower() for p in ["loe", "los", "billcat", "opex"]
                ) else "No",
                "IS_AFE_RELATED": "Yes" if any(
                    p in case.input_column.lower() for p in ["afe", "wbs", "project"]
                ) else "No",
                "IS_JIB_RELATED": "Yes" if any(
                    p in case.input_column.lower() for p in ["jib", "partner", "owner", "interest"]
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
# ENRICHMENT CONFIGURATION AND AGENT
# ==============================================================================

@dataclass
class EnrichmentSourceConfig:
    """Configuration for a reference data source for enrichment."""
    source_id: str
    source_type: str  # 'csv' or 'database'
    source_path: str  # File path for CSV, connection string for DB
    table_name: str
    key_column: str  # Column used for matching (e.g., ACCOUNT_CODE)
    detail_columns: List[str] = field(default_factory=list)
    display_name: str = ""

    def to_dict(self) -> dict:
        return {
            'source_id': self.source_id,
            'source_type': self.source_type,
            'source_path': self.source_path,
            'table_name': self.table_name,
            'key_column': self.key_column,
            'detail_columns': self.detail_columns,
            'display_name': self.display_name or self.table_name,
        }


@dataclass
class EnrichmentConfig:
    """Configuration for mapping enrichment with reference data."""
    enabled: bool = False
    data_sources: List[EnrichmentSourceConfig] = field(default_factory=list)
    auto_detect_tables: bool = True
    prompt_for_columns: bool = True

    def to_dict(self) -> dict:
        return {
            'enabled': self.enabled,
            'data_sources': [ds.to_dict() for ds in self.data_sources],
            'auto_detect_tables': self.auto_detect_tables,
            'prompt_for_columns': self.prompt_for_columns,
        }


class EnrichmentAgent(BaseAgent):
    """
    AI Agent for post-hierarchy mapping enrichment.

    This agent:
    1. Detects which reference tables are used in mappings
    2. Generates prompts to ask users about data source preferences
    3. Expands mappings with reference data detail columns
    """

    # Table detection patterns
    TABLE_PATTERNS = {
        'DIM_ACCOUNT': {
            'keywords': ['account', 'acct', 'gl_code', 'account_code'],
            'key_column': 'ACCOUNT_CODE',
            'display_name': 'Chart of Accounts',
            'suggested_columns': ['ACCOUNT_ID', 'ACCOUNT_NAME', 'ACCOUNT_CLASS',
                                 'ACCOUNT_BILLING_CATEGORY_CODE', 'ACCOUNT_MAJOR', 'ACCOUNT_MINOR'],
        },
        'DIM_COST_CENTER': {
            'keywords': ['cost_center', 'cc', 'costcenter'],
            'key_column': 'COST_CENTER_CODE',
            'display_name': 'Cost Centers',
            'suggested_columns': ['COST_CENTER_ID', 'COST_CENTER_NAME', 'DEPARTMENT'],
        },
        'DIM_ENTITY': {
            'keywords': ['entity', 'company', 'corp', 'fund'],
            'key_column': 'ENTITY_CODE',
            'display_name': 'Entities',
            'suggested_columns': ['ENTITY_ID', 'ENTITY_NAME', 'ENTITY_TYPE'],
        },
        'DIM_PROJECT': {
            'keywords': ['project', 'job', 'work_order', 'afe'],
            'key_column': 'PROJECT_CODE',
            'display_name': 'Projects',
            'suggested_columns': ['PROJECT_ID', 'PROJECT_NAME', 'PROJECT_TYPE'],
        },
        'DIM_WELL': {
            'keywords': ['well', 'wellbore', 'api'],
            'key_column': 'WELL_CODE',
            'display_name': 'Wells',
            'suggested_columns': ['WELL_ID', 'WELL_NAME', 'WELL_STATUS', 'BASIN'],
        },
    }

    def __init__(self):
        super().__init__(
            name="EnrichmentAgent",
            description="Enriches mapping exports with reference data detail columns"
        )
        self.detected_tables: List[dict] = []
        self.reference_data: Dict[str, Dict[str, dict]] = {}

    def execute(self, input_data: Dict[str, Any], context: AgentContext) -> AgentResult:
        """Execute enrichment analysis and optionally enrich mappings."""
        cases = input_data.get("cases", [])
        export_result = input_data.get("export_result", {})
        enrichment_config = input_data.get("enrichment_config")

        # Step 1: Detect reference tables used in mappings
        self.detected_tables = self._detect_tables_from_cases(cases)
        self.log(f"Detected {len(self.detected_tables)} reference table(s) from mappings")

        # Step 2: Generate user prompt for enrichment
        enrichment_prompt = self._generate_enrichment_prompt(self.detected_tables, context)

        # Step 3: If enrichment config provided, perform enrichment
        enrichment_result = None
        if enrichment_config and enrichment_config.enabled:
            mapping_file = None
            for f in export_result.get("exported_files", []):
                if "_MAPPING.csv" in f:
                    mapping_file = f
                    break

            if mapping_file:
                enrichment_result = self._enrich_mapping_file(
                    mapping_file,
                    enrichment_config,
                    export_result.get("export_path", "./result_export"),
                    export_result.get("export_name", "analysis"),
                )
                self.log(f"Enrichment completed: {enrichment_result.get('expanded_rows', 0)} rows")

        return AgentResult(
            success=True,
            data={
                "detected_tables": self.detected_tables,
                "enrichment_prompt": enrichment_prompt,
                "enrichment_result": enrichment_result,
            },
            reasoning=f"Detected {len(self.detected_tables)} table(s). " +
                      ("Enrichment completed." if enrichment_result else "Ready for enrichment."),
            confidence=0.9,
            suggestions=self._generate_suggestions(self.detected_tables, enrichment_config),
        )

    def _detect_tables_from_cases(self, cases: List[ExtractedCase]) -> List[dict]:
        """Detect which reference tables are used based on column patterns."""
        tables = {}

        for case in cases:
            # Check both input_column and source_column, and also the when clause conditions
            columns_to_check = [
                case.input_column.lower() if case.input_column else "",
                case.source_column.lower() if case.source_column else "",
            ]

            # Also check individual condition columns
            for when in case.when_clauses:
                if when.condition.column:
                    columns_to_check.append(when.condition.column.lower())

            for table_name, config in self.TABLE_PATTERNS.items():
                found = False
                for col_lower in columns_to_check:
                    if found:
                        break
                    for keyword in config['keywords']:
                        if keyword in col_lower:
                            if table_name not in tables:
                                tables[table_name] = {
                                    'table_name': table_name,
                                    'key_column': config['key_column'],
                                    'display_name': config['display_name'],
                                    'suggested_columns': config['suggested_columns'],
                                    'matching_columns': [],
                                    'condition_count': 0,
                                    'sample_conditions': [],
                                }
                            tables[table_name]['matching_columns'].append(col_lower)
                            tables[table_name]['condition_count'] += len(case.when_clauses)
                            # Add sample conditions
                            for when in case.when_clauses[:3]:
                                if len(tables[table_name]['sample_conditions']) < 5:
                                    tables[table_name]['sample_conditions'].append(
                                        when.condition.values[0] if when.condition.values else ""
                                    )
                            found = True
                            break

        return list(tables.values())

    def _generate_enrichment_prompt(self, tables: List[dict], context: AgentContext) -> dict:
        """Generate structured prompt for user enrichment decisions."""
        if not tables:
            return {
                'has_tables': False,
                'message': 'No reference tables detected in mappings.',
            }

        prompt = {
            'has_tables': True,
            'message': f"I detected {len(tables)} reference table(s) in your mappings that can be enriched:",
            'tables': [],
            'questions': [
                {
                    'id': 'enable_enrichment',
                    'question': 'Would you like to add reference data to enrich your mapping exports?',
                    'type': 'choice',
                    'options': ['Yes', 'No'],
                    'default': 'Yes',
                }
            ],
        }

        for table in tables:
            table_info = {
                'table_name': table['table_name'],
                'display_name': table['display_name'],
                'key_column': table['key_column'],
                'condition_count': table['condition_count'],
                'sample_conditions': table['sample_conditions'][:3],
                'suggested_columns': table['suggested_columns'],
            }
            prompt['tables'].append(table_info)

            # Add questions for this table
            prompt['questions'].append({
                'id': f"source_{table['table_name']}",
                'question': f"How would you like to provide {table['display_name']} data?",
                'type': 'choice',
                'options': ['CSV File', 'Database Connection', 'Skip this table'],
                'default': 'CSV File',
                'depends_on': {'enable_enrichment': 'Yes'},
            })

            prompt['questions'].append({
                'id': f"path_{table['table_name']}",
                'question': f"Enter the path/connection for {table['display_name']}:",
                'type': 'text',
                'depends_on': {f"source_{table['table_name']}": ['CSV File', 'Database Connection']},
            })

            prompt['questions'].append({
                'id': f"columns_{table['table_name']}",
                'question': f"Which columns to include from {table['display_name']}?",
                'type': 'multi_select',
                'options': table['suggested_columns'],
                'default': table['suggested_columns'][:4],
                'depends_on': {f"source_{table['table_name']}": ['CSV File', 'Database Connection']},
            })

        return prompt

    def _enrich_mapping_file(self, mapping_path: str, config: EnrichmentConfig,
                            export_path: str, export_name: str) -> dict:
        """Enrich a mapping file with reference data."""
        stats = {
            'original_rows': 0,
            'expanded_rows': 0,
            'enriched_sources': [],
            'output_files': [],
        }

        current_path = mapping_path

        for source in config.data_sources:
            # Load reference data
            ref_data = self._load_reference_data(source)
            if not ref_data:
                continue

            self.reference_data[source.source_id] = ref_data

            # Process mapping file
            result = self._expand_with_reference(
                current_path,
                source,
                ref_data,
                export_path,
                export_name,
            )

            stats['original_rows'] = result['original_rows']
            stats['expanded_rows'] = result['expanded_rows']
            stats['enriched_sources'].append({
                'source': source.display_name,
                'records_loaded': len(ref_data),
                'columns_added': [f'EXPANDED_{c}' for c in source.detail_columns],
                'matched_rows': result['matched_rows'],
            })

            if result.get('output_path'):
                stats['output_files'].append(result['output_path'])
                current_path = result['output_path']

        return stats

    def _load_reference_data(self, source: EnrichmentSourceConfig) -> Dict[str, dict]:
        """Load reference data from CSV or database."""
        data = {}

        if source.source_type == 'csv':
            try:
                with open(source.source_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        key = row.get(source.key_column, '').strip()
                        if key:
                            data[key] = {k: v for k, v in row.items()}
            except Exception as e:
                self.log(f"Error loading CSV: {e}")

        elif source.source_type == 'database':
            try:
                from sqlalchemy import create_engine, text
                engine = create_engine(source.source_path)
                col_list = ', '.join([source.key_column] + source.detail_columns)
                query = f"SELECT {col_list} FROM {source.table_name}"
                with engine.connect() as conn:
                    result = conn.execute(text(query))
                    for row in result:
                        row_dict = dict(row._mapping)
                        key = str(row_dict.get(source.key_column, '')).strip()
                        if key:
                            data[key] = row_dict
            except Exception as e:
                self.log(f"Error loading database: {e}")

        return data

    def _expand_with_reference(self, mapping_path: str, source: EnrichmentSourceConfig,
                               ref_data: Dict[str, dict], export_path: str,
                               export_name: str) -> dict:
        """Expand mapping file with reference data."""
        stats = {'original_rows': 0, 'expanded_rows': 0, 'matched_rows': 0}
        expanded_rows = []
        original_fieldnames = []

        with open(mapping_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            original_fieldnames = list(reader.fieldnames or [])

            for row in reader:
                stats['original_rows'] += 1
                expanded = self._expand_row(row, source, ref_data)
                for exp_row in expanded:
                    if exp_row.get('MATCH_TYPE') == 'REFERENCE_MATCH':
                        stats['matched_rows'] += 1
                expanded_rows.extend(expanded)

        stats['expanded_rows'] = len(expanded_rows)

        # Build output fieldnames
        output_fieldnames = original_fieldnames.copy()
        for col in source.detail_columns:
            exp_col = f'EXPANDED_{col}'
            if exp_col not in output_fieldnames:
                output_fieldnames.append(exp_col)
        if 'MATCH_TYPE' not in output_fieldnames:
            output_fieldnames.append('MATCH_TYPE')

        # Write output
        enriched_dir = os.path.join(export_path, 'enriched')
        Path(enriched_dir).mkdir(parents=True, exist_ok=True)
        output_path = os.path.join(enriched_dir, f"{export_name}_MAPPING_ENRICHED.csv")

        with open(output_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=output_fieldnames)
            writer.writeheader()
            writer.writerows(expanded_rows)

        stats['output_path'] = output_path
        return stats

    def _expand_row(self, row: dict, source: EnrichmentSourceConfig,
                   ref_data: Dict[str, dict]) -> List[dict]:
        """Expand a single mapping row with reference data matches."""
        condition_type = row.get('CONDITION_TYPE', '').upper()
        condition_value = row.get('CONDITION_VALUE', '')

        keys = list(ref_data.keys())
        matches = []

        # Match based on condition type
        if condition_type in ('ILIKE', 'LIKE'):
            matches = self._match_ilike(condition_value, keys)
        elif condition_type == 'IN':
            values = [v.strip().strip("'\"") for v in condition_value.split(',')]
            matches = [k for k in keys if k in values]
        elif condition_type == '=':
            matches = [k for k in keys if k == condition_value]
        elif condition_type == 'BETWEEN':
            parts = condition_value.split(' AND ')
            if len(parts) == 2:
                matches = [k for k in keys if parts[0].strip() <= k <= parts[1].strip()]

        if not matches:
            # No matches - return original row
            expanded_row = row.copy()
            for col in source.detail_columns:
                expanded_row[f'EXPANDED_{col}'] = ''
            expanded_row['MATCH_TYPE'] = 'NO_MATCH'
            return [expanded_row]

        # Expand to one row per match
        expanded_rows = []
        for match_key in matches:
            expanded_row = row.copy()
            ref_record = ref_data[match_key]

            for col in source.detail_columns:
                value = ref_record.get(col, '')
                expanded_row[f'EXPANDED_{col}'] = value if value and str(value) != 'nan' else ''

            expanded_row['MATCH_TYPE'] = 'REFERENCE_MATCH'
            expanded_rows.append(expanded_row)

        return expanded_rows

    def _match_ilike(self, pattern: str, keys: List[str]) -> List[str]:
        """Match SQL ILIKE pattern."""
        pattern = pattern.strip()
        matches = []

        if pattern.endswith('%') and not pattern.startswith('%'):
            prefix = pattern[:-1].lower()
            matches = [k for k in keys if k.lower().startswith(prefix)]
        elif pattern.startswith('%') and not pattern.endswith('%'):
            suffix = pattern[1:].lower()
            matches = [k for k in keys if k.lower().endswith(suffix)]
        elif pattern.startswith('%') and pattern.endswith('%'):
            contains = pattern[1:-1].lower()
            matches = [k for k in keys if contains in k.lower()]
        else:
            matches = [k for k in keys if k.lower() == pattern.lower()]

        return matches

    def _generate_suggestions(self, tables: List[dict],
                             config: Optional[EnrichmentConfig]) -> List[str]:
        """Generate suggestions for enrichment."""
        suggestions = []

        if tables and (not config or not config.enabled):
            suggestions.append(
                f"Consider enabling enrichment to add detail columns from "
                f"{', '.join(t['display_name'] for t in tables)}"
            )

        for table in tables:
            suggestions.append(
                f"For {table['display_name']}, consider adding columns: "
                f"{', '.join(table['suggested_columns'][:3])}"
            )

        return suggestions


# ==============================================================================
# ORCHESTRATOR AGENT
# ==============================================================================

class SQLAnalysisOrchestrator:
    """AI Orchestrator that coordinates all specialized agents."""

    def __init__(self):
        self.name = "SQLAnalysisOrchestrator"
        self.agents = {
            "extraction": CaseExtractionAgent(),
            "entity": EntityDetectionAgent(),
            "pattern": PatternDetectionAgent(),
            "confidence": ConfidenceScoringAgent(),
            "export": ExportAgent(),
            "enrichment": EnrichmentAgent(),
        }
        self.execution_log: List[str] = []

    def log(self, message: str) -> None:
        """Log orchestrator activity."""
        timestamp = datetime.now().strftime("%H:%M:%S")
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
        enrichment_config: Optional[EnrichmentConfig] = None,
    ) -> Dict[str, Any]:
        """
        Orchestrate full SQL hierarchy analysis with optional enrichment.

        Args:
            sql: SQL containing CASE statements to analyze
            client_id: Client ID for knowledge base lookup
            industry: Industry context (oil_gas, manufacturing, saas, general, etc.)
            export_path: Directory for CSV exports
            export_name: Base name for files (auto-generated if empty)
            min_confidence: Minimum confidence to include in exports
            user_intent: Description of what you're trying to achieve
            custom_context: Additional context dictionary
            enrichment_config: Configuration for reference data enrichment

        Returns:
            Comprehensive analysis with hierarchies, confidence scores, and exported files
        """
        self.log(f"Starting analysis for industry: {industry}")

        # Build context
        context = self._build_context(client_id, industry, user_intent, custom_context)

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

        # Step 6: Enrichment Analysis (always run to detect tables)
        self.log("Step 6: Running enrichment agent")
        enrichment_result = self.agents["enrichment"].execute({
            "cases": cases,
            "export_result": export_result.data,
            "enrichment_config": enrichment_config,
        }, context)
        results["enrichment"] = enrichment_result

        return self._build_success_response(results, context)

    def _build_context(self, client_id, industry, user_intent, custom_context) -> AgentContext:
        """Build agent context from inputs."""
        context = AgentContext(
            client_id=client_id,
            industry=industry,
            user_intent=user_intent,
        )

        if client_id:
            client_kb = self._load_client_knowledge(client_id)
            if client_kb:
                context.client_name = client_kb.get("client_name")
                context.erp_system = client_kb.get("erp_system")
                context.custom_prompts = client_kb.get("custom_prompts", [])
                context.gl_patterns = client_kb.get("gl_patterns", {})
                if client_kb.get("industry"):
                    context.industry = client_kb["industry"]

        if custom_context:
            context.additional_context = custom_context

        return context

    def _load_client_knowledge(self, client_id: str) -> Optional[Dict[str, Any]]:
        """Load client knowledge base."""
        kb_path = Path(f"./knowledge_base/clients/{client_id}/config.json")
        if kb_path.exists():
            try:
                with open(kb_path) as f:
                    return json.load(f)
            except Exception:
                pass
        return None

    def _build_error_response(self, error: str, results: Dict[str, AgentResult]) -> Dict[str, Any]:
        """Build error response."""
        return {
            "success": False,
            "error": error,
            "agent_results": {
                name: {"success": r.success, "reasoning": r.reasoning, "confidence": r.confidence}
                for name, r in results.items()
            },
            "execution_log": self.execution_log,
        }

    def _build_success_response(self, results: Dict[str, AgentResult], context: AgentContext) -> Dict[str, Any]:
        """Build success response with all insights."""
        export_data = results["export"].data
        confidence_data = results["confidence"].data
        enrichment_result = results.get("enrichment")
        enrichment_data = enrichment_result.data if enrichment_result else {}

        all_suggestions = []
        for name, result in results.items():
            all_suggestions.extend(result.suggestions)

        # Combine export files with any enriched files
        all_files = export_data.get("exported_files", [])
        enrichment_result = enrichment_data.get("enrichment_result")
        if enrichment_result and enrichment_result.get("output_files"):
            all_files.extend(enrichment_result["output_files"])

        return {
            "success": True,
            "summary": {
                "hierarchies_found": len(confidence_data.get("scored_cases", [])),
                "average_confidence": confidence_data.get("average_confidence", 0),
                "files_exported": len(all_files),
                "industry": context.industry,
                "client": context.client_id,
            },
            "export": {
                "name": export_data.get("export_name"),
                "path": export_data.get("export_path"),
                "files": all_files,
            },
            "enrichment": {
                "detected_tables": enrichment_data.get("detected_tables", []),
                "prompt": enrichment_data.get("enrichment_prompt"),
                "result": enrichment_result,
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
# CONVENIENCE FUNCTION
# ==============================================================================

def ai_analyze_sql(
    sql: str,
    client_id: str = "",
    industry: str = "general",
    export_path: str = "./result_export",
    export_name: str = "",
    min_confidence: float = 0.0,
    user_intent: str = "",
    enrichment_config: Optional[EnrichmentConfig] = None,
) -> Dict[str, Any]:
    """
    AI-powered SQL hierarchy analysis with intelligent agents.

    Args:
        sql: SQL containing CASE statements to analyze
        client_id: Client ID for knowledge base lookup (optional)
        industry: Industry context - oil_gas, manufacturing, saas, general, etc.
        export_path: Directory for CSV exports (default: ./result_export)
        export_name: Base name for files (auto-generated if empty)
        min_confidence: Minimum confidence to include in exports (0.0-1.0)
        user_intent: Description of what you're trying to achieve
        enrichment_config: Configuration for reference data enrichment (optional)

    Returns:
        Comprehensive analysis with hierarchies, confidence scores, and exported files

    Example:
        >>> result = ai_analyze_sql(
        ...     sql="SELECT CASE WHEN account LIKE '4%' THEN 'Revenue' ...",
        ...     industry="oil_gas",
        ... )
        >>> print(result["summary"]["average_confidence"])
        0.85

    Example with enrichment:
        >>> config = EnrichmentConfig(
        ...     enabled=True,
        ...     data_sources=[
        ...         EnrichmentSourceConfig(
        ...             source_id="coa",
        ...             source_type="csv",
        ...             source_path="C:/data/DIM_ACCOUNT.csv",
        ...             table_name="DIM_ACCOUNT",
        ...             key_column="ACCOUNT_CODE",
        ...             detail_columns=["ACCOUNT_ID", "ACCOUNT_NAME", "ACCOUNT_BILLING_CATEGORY_CODE"],
        ...         )
        ...     ]
        ... )
        >>> result = ai_analyze_sql(sql, enrichment_config=config)
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
        enrichment_config=enrichment_config,
    )


def create_coa_enrichment(
    coa_path: str,
    detail_columns: List[str] = None,
) -> EnrichmentConfig:
    """
    Create an enrichment configuration for Chart of Accounts.

    This is a convenience function for the most common enrichment case.

    Args:
        coa_path: Path to the DIM_ACCOUNT.csv file
        detail_columns: Columns to include (defaults to common COA columns)

    Returns:
        EnrichmentConfig ready to use with ai_analyze_sql

    Example:
        >>> config = create_coa_enrichment("C:/data/DIM_ACCOUNT.csv")
        >>> result = ai_analyze_sql(sql, enrichment_config=config)
    """
    if detail_columns is None:
        detail_columns = [
            "ACCOUNT_ID",
            "ACCOUNT_NAME",
            "ACCOUNT_CLASS",
            "ACCOUNT_BILLING_CATEGORY_CODE",
            "ACCOUNT_MAJOR",
            "ACCOUNT_MINOR",
            "ACCOUNT_HOLDER",
        ]

    return EnrichmentConfig(
        enabled=True,
        data_sources=[
            EnrichmentSourceConfig(
                source_id="coa",
                source_type="csv",
                source_path=coa_path,
                table_name="DIM_ACCOUNT",
                key_column="ACCOUNT_CODE",
                detail_columns=detail_columns,
                display_name="Chart of Accounts",
            )
        ],
    )


def create_multi_source_enrichment(
    sources: List[Dict[str, Any]],
) -> EnrichmentConfig:
    """
    Create an enrichment configuration with multiple data sources.

    Args:
        sources: List of source configurations, each with:
            - source_id: Unique identifier
            - source_type: 'csv' or 'database'
            - source_path: File path or connection string
            - table_name: Name of the table
            - key_column: Column for matching
            - detail_columns: List of columns to include
            - display_name: Friendly name (optional)

    Returns:
        EnrichmentConfig ready to use

    Example:
        >>> config = create_multi_source_enrichment([
        ...     {
        ...         "source_id": "coa",
        ...         "source_type": "csv",
        ...         "source_path": "C:/data/DIM_ACCOUNT.csv",
        ...         "table_name": "DIM_ACCOUNT",
        ...         "key_column": "ACCOUNT_CODE",
        ...         "detail_columns": ["ACCOUNT_ID", "ACCOUNT_NAME"],
        ...     },
        ...     {
        ...         "source_id": "cost_centers",
        ...         "source_type": "csv",
        ...         "source_path": "C:/data/DIM_COST_CENTER.csv",
        ...         "table_name": "DIM_COST_CENTER",
        ...         "key_column": "COST_CENTER_CODE",
        ...         "detail_columns": ["COST_CENTER_ID", "COST_CENTER_NAME"],
        ...     },
        ... ])
    """
    data_sources = []
    for source in sources:
        ds = EnrichmentSourceConfig(
            source_id=source["source_id"],
            source_type=source["source_type"],
            source_path=source["source_path"],
            table_name=source["table_name"],
            key_column=source["key_column"],
            detail_columns=source.get("detail_columns", []),
            display_name=source.get("display_name", source["table_name"]),
        )
        data_sources.append(ds)

    return EnrichmentConfig(enabled=True, data_sources=data_sources)


# ==============================================================================
# INTERACTIVE ENRICHMENT HELPER
# ==============================================================================

def get_enrichment_questions(result: Dict[str, Any]) -> Optional[dict]:
    """
    Get the enrichment questions from analysis result.

    After running ai_analyze_sql(), use this to get the questions
    to present to the user about data enrichment.

    Args:
        result: Result from ai_analyze_sql()

    Returns:
        Enrichment prompt with questions, or None if no tables detected

    Example:
        >>> result = ai_analyze_sql(sql)
        >>> questions = get_enrichment_questions(result)
        >>> if questions and questions['has_tables']:
        ...     print(questions['message'])
        ...     for table in questions['tables']:
        ...         print(f"  - {table['display_name']}")
    """
    enrichment = result.get("enrichment", {})
    prompt = enrichment.get("prompt")
    return prompt if prompt and prompt.get("has_tables") else None
