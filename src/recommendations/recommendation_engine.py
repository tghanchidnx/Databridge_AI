"""
Core Recommendation Engine for DataBridge AI.

Analyzes CSV data and context to provide smart recommendations for:
- Skill selection (domain expertise)
- Template matching (industry hierarchies)
- Knowledge base lookups (client patterns)
- Import configuration
"""

import re
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Tuple
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class DataProfile:
    """Profile of analyzed CSV data."""
    columns: List[str] = field(default_factory=list)
    row_count: int = 0
    sample_values: Dict[str, List[str]] = field(default_factory=dict)
    detected_patterns: Dict[str, str] = field(default_factory=dict)
    column_types: Dict[str, str] = field(default_factory=dict)

    # Industry hints from data analysis
    industry_hints: List[str] = field(default_factory=list)
    domain_hints: List[str] = field(default_factory=list)
    hierarchy_type_hints: List[str] = field(default_factory=list)

    # Detected structures
    has_account_codes: bool = False
    has_hierarchy_levels: bool = False
    has_parent_child: bool = False
    has_amounts: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "columns": self.columns,
            "row_count": self.row_count,
            "sample_values": self.sample_values,
            "detected_patterns": self.detected_patterns,
            "column_types": self.column_types,
            "industry_hints": self.industry_hints,
            "domain_hints": self.domain_hints,
            "hierarchy_type_hints": self.hierarchy_type_hints,
            "has_account_codes": self.has_account_codes,
            "has_hierarchy_levels": self.has_hierarchy_levels,
            "has_parent_child": self.has_parent_child,
            "has_amounts": self.has_amounts,
        }


@dataclass
class Recommendation:
    """A single recommendation with reasoning."""
    type: str  # skill, template, import_tier, mapping, knowledge
    id: str
    name: str
    score: float  # 0-100 confidence score
    reason: str
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "id": self.id,
            "name": self.name,
            "score": self.score,
            "reason": self.reason,
            "details": self.details,
        }


@dataclass
class RecommendationContext:
    """Context for generating recommendations."""
    # User-provided context
    user_intent: Optional[str] = None
    client_id: Optional[str] = None
    industry: Optional[str] = None

    # Data context
    data_profile: Optional[DataProfile] = None
    file_path: Optional[str] = None

    # Constraint context
    target_database: Optional[str] = None
    target_schema: Optional[str] = None
    target_table: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "user_intent": self.user_intent,
            "client_id": self.client_id,
            "industry": self.industry,
            "data_profile": self.data_profile.to_dict() if self.data_profile else None,
            "file_path": self.file_path,
            "target_database": self.target_database,
            "target_schema": self.target_schema,
            "target_table": self.target_table,
        }


class RecommendationEngine:
    """
    Smart recommendation engine that combines:
    - CSV data profiling
    - Skill selection
    - Knowledge base queries
    - Template matching
    - Import tier detection
    """

    # Industry keywords for detection
    INDUSTRY_KEYWORDS = {
        "oil_gas": [
            "well", "field", "basin", "reservoir", "production", "drilling",
            "lease", "royalty", "working_interest", "nri", "afe", "jib",
            "loe", "dd&a", "dd_a", "depletion", "boe", "mcf", "bbl",
            "pipeline", "gathering", "midstream", "upstream", "downstream",
        ],
        "manufacturing": [
            "plant", "factory", "production_line", "work_center", "bom",
            "bill_of_materials", "inventory", "wip", "raw_material",
            "finished_goods", "variance", "standard_cost", "yield",
        ],
        "saas": [
            "arr", "mrr", "churn", "ltv", "cac", "customer_acquisition",
            "subscription", "renewal", "expansion", "contraction",
            "cohort", "retention", "nrr", "logo", "seat",
        ],
        "transportation": [
            "fleet", "route", "lane", "terminal", "driver", "trailer",
            "power_unit", "load", "deadhead", "linehaul", "operating_ratio",
            "fuel", "miles", "revenue_per_mile",
        ],
        "retail": [
            "store", "sku", "inventory", "pos", "margin", "shrinkage",
            "same_store", "comp", "traffic", "conversion", "basket",
        ],
    }

    # Domain keywords for detection
    DOMAIN_KEYWORDS = {
        "accounting": [
            "account", "gl", "general_ledger", "journal", "debit", "credit",
            "coa", "chart_of_accounts", "trial_balance", "posting",
            "income", "expense", "revenue", "asset", "liability", "equity",
        ],
        "finance": [
            "budget", "forecast", "variance", "cost_center", "profit_center",
            "actual", "plan", "fpa", "allocation", "driver",
        ],
        "operations": [
            "department", "region", "location", "site", "facility",
            "geography", "country", "state", "city", "division",
        ],
    }

    # Hierarchy type patterns
    HIERARCHY_PATTERNS = {
        "income_statement": [
            "revenue", "sales", "cogs", "cost_of_goods", "gross_margin",
            "operating", "ebitda", "ebit", "net_income", "expense",
        ],
        "balance_sheet": [
            "asset", "liability", "equity", "current", "non_current",
            "receivable", "payable", "inventory", "ppe", "cash",
        ],
        "cost_center": [
            "cost_center", "cc", "responsibility", "expense_center",
        ],
        "geographic": [
            "region", "country", "state", "city", "location", "site",
        ],
        "department": [
            "department", "dept", "function", "team", "division",
        ],
    }

    def __init__(self, template_service=None, hierarchy_service=None):
        """
        Initialize the recommendation engine.

        Args:
            template_service: Instance of TemplateService for templates/skills/KB
            hierarchy_service: Instance of HierarchyService for hierarchy operations
        """
        self._template_service = template_service
        self._hierarchy_service = hierarchy_service

    @property
    def template_service(self):
        """Lazy load template service."""
        if self._template_service is None:
            from src.templates.service import TemplateService
            self._template_service = TemplateService()
        return self._template_service

    @property
    def hierarchy_service(self):
        """Lazy load hierarchy service."""
        if self._hierarchy_service is None:
            from src.hierarchy.service import HierarchyService
            self._hierarchy_service = HierarchyService()
        return self._hierarchy_service

    def profile_csv(self, file_path: str = None, content: str = None,
                    sample_rows: int = 100) -> DataProfile:
        """
        Profile a CSV file to understand its structure and content.

        Args:
            file_path: Path to CSV file
            content: Raw CSV content (alternative to file_path)
            sample_rows: Number of rows to sample for analysis

        Returns:
            DataProfile with detected patterns and hints
        """
        import pandas as pd

        profile = DataProfile()

        try:
            # Load data
            if file_path:
                df = pd.read_csv(file_path, nrows=sample_rows)
                profile.row_count = sum(1 for _ in open(file_path)) - 1  # Minus header
            elif content:
                from io import StringIO
                df = pd.read_csv(StringIO(content), nrows=sample_rows)
                profile.row_count = len(content.strip().split('\n')) - 1
            else:
                return profile

            profile.columns = list(df.columns)

            # Analyze each column
            for col in df.columns:
                col_lower = col.lower().replace(' ', '_').replace('-', '_')

                # Get sample values (non-null, unique)
                sample_vals = df[col].dropna().astype(str).unique()[:5].tolist()
                profile.sample_values[col] = sample_vals

                # Detect column type
                if df[col].dtype in ['int64', 'float64']:
                    profile.column_types[col] = 'numeric'
                    if any(kw in col_lower for kw in ['amount', 'value', 'total', 'sum', 'balance']):
                        profile.has_amounts = True
                else:
                    profile.column_types[col] = 'text'

                # Detect patterns in column names
                if any(kw in col_lower for kw in ['account', 'gl', 'code', 'acct']):
                    profile.has_account_codes = True
                    profile.detected_patterns[col] = 'account_code'
                    profile.domain_hints.append('accounting')

                if any(kw in col_lower for kw in ['level', 'tier', 'l1', 'l2', 'l3', 'level_1', 'level_2']):
                    profile.has_hierarchy_levels = True
                    profile.detected_patterns[col] = 'hierarchy_level'

                if any(kw in col_lower for kw in ['parent', 'child', 'hierarchy_id', 'parent_id']):
                    profile.has_parent_child = True
                    profile.detected_patterns[col] = 'parent_child'

            # Detect industry from all column names and values
            all_text = ' '.join(profile.columns).lower()
            all_text += ' ' + ' '.join(str(v) for vals in profile.sample_values.values() for v in vals).lower()
            all_text = all_text.replace(' ', '_').replace('-', '_')

            for industry, keywords in self.INDUSTRY_KEYWORDS.items():
                matches = sum(1 for kw in keywords if kw in all_text)
                if matches >= 2:  # Need at least 2 keyword matches
                    profile.industry_hints.append(industry)

            # Detect domain
            for domain, keywords in self.DOMAIN_KEYWORDS.items():
                matches = sum(1 for kw in keywords if kw in all_text)
                if matches >= 2:
                    if domain not in profile.domain_hints:
                        profile.domain_hints.append(domain)

            # Detect hierarchy type
            for h_type, keywords in self.HIERARCHY_PATTERNS.items():
                matches = sum(1 for kw in keywords if kw in all_text)
                if matches >= 2:
                    profile.hierarchy_type_hints.append(h_type)

        except Exception as e:
            logger.error(f"Error profiling CSV: {e}")

        return profile

    def detect_import_tier(self, profile: DataProfile) -> Tuple[int, str]:
        """
        Detect the appropriate import tier based on data profile.

        Returns:
            Tuple of (tier_number, reason)
        """
        col_count = len(profile.columns)
        col_names_lower = [c.lower().replace(' ', '_') for c in profile.columns]

        # Tier 4: Enterprise format (28+ columns with all flags)
        tier4_indicators = [
            'hierarchy_id', 'parent_id', 'include_flag', 'exclude_flag',
            'level_1', 'level_2', 'formula_group', 'calculation_flag',
        ]
        tier4_matches = sum(1 for ind in tier4_indicators if any(ind in c for c in col_names_lower))
        if col_count >= 15 or tier4_matches >= 5:
            return 4, f"Enterprise format detected: {col_count} columns, {tier4_matches} tier-4 indicators"

        # Tier 3: Full control (10-12 columns with explicit IDs)
        tier3_indicators = ['hierarchy_id', 'parent_id', 'source_database', 'source_schema', 'source_table']
        tier3_matches = sum(1 for ind in tier3_indicators if any(ind in c for c in col_names_lower))
        if col_count >= 8 or tier3_matches >= 3:
            return 3, f"Full control format: {col_count} columns, explicit IDs/source info"

        # Tier 2: Basic with parents (5-7 columns)
        tier2_indicators = ['hierarchy_name', 'parent_name', 'sort_order', 'parent']
        tier2_matches = sum(1 for ind in tier2_indicators if any(ind in c for c in col_names_lower))
        if col_count >= 4 or tier2_matches >= 2:
            return 2, f"Basic hierarchy with parent-child: {col_count} columns"

        # Tier 1: Ultra-simple (2-3 columns)
        return 1, f"Ultra-simple grouping format: {col_count} columns (source_value, group_name)"

    def recommend_skills(self, context: RecommendationContext) -> List[Recommendation]:
        """
        Recommend skills based on context.

        Returns:
            List of skill recommendations sorted by score
        """
        recommendations = []

        # Get all skills
        skills = self.template_service.list_skills()

        profile = context.data_profile
        industry = context.industry or (profile.industry_hints[0] if profile and profile.industry_hints else None)
        domain = profile.domain_hints[0] if profile and profile.domain_hints else None

        for skill in skills:
            score = 50  # Base score
            reasons = []

            # Industry match
            if industry:
                if industry in skill.industries or 'all' in skill.industries:
                    score += 25
                    reasons.append(f"Matches {industry} industry")
                elif 'general' in skill.industries:
                    score += 10
                    reasons.append("General-purpose skill")

            # Domain match
            if domain and skill.domain.value == domain:
                score += 20
                reasons.append(f"Matches {domain} domain")

            # Hierarchy type match
            if profile and profile.hierarchy_type_hints:
                for h_type in profile.hierarchy_type_hints:
                    if h_type in skill.hierarchy_types:
                        score += 15
                        reasons.append(f"Handles {h_type} hierarchies")
                        break

            # User intent matching
            if context.user_intent:
                intent_lower = context.user_intent.lower()
                for cap in skill.capabilities:
                    if any(word in intent_lower for word in cap.lower().split('_')):
                        score += 10
                        reasons.append(f"Capability match: {cap}")
                        break

            if reasons:
                recommendations.append(Recommendation(
                    type="skill",
                    id=skill.id,
                    name=skill.name,
                    score=min(score, 100),
                    reason="; ".join(reasons),
                    details={
                        "domain": skill.domain.value,
                        "industries": skill.industries,
                        "capabilities": skill.capabilities[:5],
                    }
                ))

        # Sort by score
        recommendations.sort(key=lambda x: x.score, reverse=True)
        return recommendations[:3]  # Top 3

    def recommend_templates(self, context: RecommendationContext) -> List[Recommendation]:
        """
        Recommend templates based on context.

        Returns:
            List of template recommendations sorted by score
        """
        recommendations = []

        profile = context.data_profile
        industry = context.industry or (profile.industry_hints[0] if profile and profile.industry_hints else None)

        # Get templates filtered by industry
        templates = self.template_service.list_templates(industry=industry)
        if not templates:
            templates = self.template_service.list_templates()  # Fall back to all

        for template in templates:
            score = 50
            reasons = []

            # Industry match
            if industry:
                if template.industry == industry:
                    score += 30
                    reasons.append(f"Exact {industry} industry match")
                elif template.industry == "general":
                    score += 10
                    reasons.append("General-purpose template")

            # Domain match
            if profile and profile.domain_hints:
                if template.domain in profile.domain_hints:
                    score += 20
                    reasons.append(f"Matches {template.domain} domain")

            # Hierarchy type match
            if profile and profile.hierarchy_type_hints:
                if template.hierarchy_type in profile.hierarchy_type_hints:
                    score += 20
                    reasons.append(f"Matches {template.hierarchy_type} hierarchy type")

            # Complexity match (more hierarchies = more complete)
            if template.hierarchy_count > 10:
                score += 5
                reasons.append(f"Comprehensive: {template.hierarchy_count} nodes")

            if reasons:
                recommendations.append(Recommendation(
                    type="template",
                    id=template.id,
                    name=template.name,
                    score=min(score, 100),
                    reason="; ".join(reasons),
                    details={
                        "industry": template.industry,
                        "domain": template.domain,
                        "hierarchy_type": template.hierarchy_type,
                        "hierarchy_count": template.hierarchy_count,
                    }
                ))

        recommendations.sort(key=lambda x: x.score, reverse=True)
        return recommendations[:3]

    def check_knowledge_base(self, context: RecommendationContext) -> List[Recommendation]:
        """
        Check knowledge base for client-specific recommendations.

        Returns:
            List of knowledge-based recommendations
        """
        recommendations = []

        if not context.client_id:
            return recommendations

        client = self.template_service.get_client_knowledge(context.client_id)
        if not client:
            return recommendations

        profile = context.data_profile

        # Check for preferred template
        if client.preferred_template_id:
            recommendations.append(Recommendation(
                type="knowledge",
                id=client.preferred_template_id,
                name=f"Client preferred template",
                score=90,
                reason=f"Previously configured preference for {client.client_name}",
                details={"source": "client_preference", "field": "preferred_template_id"}
            ))

        # Check for preferred skill
        if client.preferred_skill_id:
            recommendations.append(Recommendation(
                type="knowledge",
                id=client.preferred_skill_id,
                name=f"Client preferred skill",
                score=90,
                reason=f"Previously configured skill for {client.client_name}",
                details={"source": "client_preference", "field": "preferred_skill_id"}
            ))

        # Check GL pattern matches
        if profile and profile.has_account_codes and client.gl_patterns:
            for pattern, hierarchy in client.gl_patterns.items():
                # Check if any sample values match this pattern
                for col, samples in profile.sample_values.items():
                    for sample in samples:
                        if re.match(pattern.replace('*', '.*'), str(sample)):
                            recommendations.append(Recommendation(
                                type="mapping",
                                id=f"gl_pattern_{pattern}",
                                name=f"GL Pattern: {pattern} → {hierarchy}",
                                score=85,
                                reason=f"Known GL pattern from {client.client_name} knowledge base",
                                details={
                                    "pattern": pattern,
                                    "hierarchy": hierarchy,
                                    "matched_column": col,
                                    "matched_value": sample,
                                }
                            ))
                            break

        # Check custom prompts for relevant context
        if context.user_intent:
            for prompt in client.custom_prompts:
                if any(word in context.user_intent.lower() for word in prompt.trigger.lower().split()):
                    recommendations.append(Recommendation(
                        type="knowledge",
                        id=prompt.id,
                        name=f"Custom prompt: {prompt.name}",
                        score=80,
                        reason=f"Triggered by: {prompt.trigger}",
                        details={"prompt_content": prompt.content[:200], "category": prompt.category}
                    ))

        return recommendations

    def get_recommendations(
        self,
        file_path: str = None,
        content: str = None,
        user_intent: str = None,
        client_id: str = None,
        industry: str = None,
        target_database: str = None,
        target_schema: str = None,
        target_table: str = None,
    ) -> Dict[str, Any]:
        """
        Get comprehensive recommendations for a CSV import.

        This is the main entry point that combines all recommendation sources.

        Args:
            file_path: Path to CSV file
            content: Raw CSV content
            user_intent: What the user wants to accomplish
            client_id: Client ID for knowledge base lookup
            industry: Known industry (or will be detected)
            target_database: Target database for deployment
            target_schema: Target schema for deployment
            target_table: Target table for deployment

        Returns:
            Dictionary containing:
            - data_profile: Analyzed data profile
            - import_tier: Recommended import tier
            - skills: Skill recommendations
            - templates: Template recommendations
            - knowledge: Knowledge base recommendations
            - summary: Human-readable summary
        """
        # Profile the data
        profile = self.profile_csv(file_path=file_path, content=content)

        # Build context
        context = RecommendationContext(
            user_intent=user_intent,
            client_id=client_id,
            industry=industry,
            data_profile=profile,
            file_path=file_path,
            target_database=target_database,
            target_schema=target_schema,
            target_table=target_table,
        )

        # Get import tier
        tier, tier_reason = self.detect_import_tier(profile)

        # Get all recommendations
        skill_recs = self.recommend_skills(context)
        template_recs = self.recommend_templates(context)
        kb_recs = self.check_knowledge_base(context)

        # Build summary
        summary_parts = []

        if profile.industry_hints:
            summary_parts.append(f"Detected industry: {', '.join(profile.industry_hints)}")
        if profile.domain_hints:
            summary_parts.append(f"Detected domain: {', '.join(profile.domain_hints)}")
        if profile.hierarchy_type_hints:
            summary_parts.append(f"Hierarchy type: {', '.join(profile.hierarchy_type_hints)}")

        summary_parts.append(f"Recommended import tier: {tier} ({tier_reason})")

        if skill_recs:
            summary_parts.append(f"Top skill: {skill_recs[0].name} (score: {skill_recs[0].score})")
        if template_recs:
            summary_parts.append(f"Top template: {template_recs[0].name} (score: {template_recs[0].score})")
        if kb_recs:
            summary_parts.append(f"Found {len(kb_recs)} knowledge base matches")

        return {
            "data_profile": profile.to_dict(),
            "import_tier": {
                "tier": tier,
                "reason": tier_reason,
            },
            "skills": [r.to_dict() for r in skill_recs],
            "templates": [r.to_dict() for r in template_recs],
            "knowledge": [r.to_dict() for r in kb_recs],
            "summary": "\n".join(summary_parts),
            "context": {
                "detected_industry": profile.industry_hints[0] if profile.industry_hints else None,
                "detected_domain": profile.domain_hints[0] if profile.domain_hints else None,
                "has_account_codes": profile.has_account_codes,
                "has_hierarchy_levels": profile.has_hierarchy_levels,
                "columns": profile.columns,
            }
        }

    def format_for_llm_validation(self, recommendations: Dict[str, Any]) -> str:
        """
        Format recommendations as a prompt for LLM validation.

        This creates a structured prompt that the LLM can use to validate
        and refine the recommendations based on user intent.

        Args:
            recommendations: Output from get_recommendations()

        Returns:
            Formatted prompt string for LLM
        """
        prompt_parts = [
            "## DataBridge AI Recommendation Analysis",
            "",
            "Based on the analyzed CSV data, here are my recommendations:",
            "",
            f"### Data Profile",
            f"- Columns: {', '.join(recommendations['context']['columns'][:10])}",
            f"- Detected Industry: {recommendations['context']['detected_industry'] or 'Unknown'}",
            f"- Detected Domain: {recommendations['context']['detected_domain'] or 'Unknown'}",
            f"- Has Account Codes: {recommendations['context']['has_account_codes']}",
            f"- Has Hierarchy Levels: {recommendations['context']['has_hierarchy_levels']}",
            "",
            f"### Import Tier Recommendation",
            f"- **Tier {recommendations['import_tier']['tier']}**: {recommendations['import_tier']['reason']}",
            "",
        ]

        if recommendations['skills']:
            prompt_parts.extend([
                "### Recommended Skills",
            ])
            for skill in recommendations['skills']:
                prompt_parts.append(f"- **{skill['name']}** (score: {skill['score']}): {skill['reason']}")
            prompt_parts.append("")

        if recommendations['templates']:
            prompt_parts.extend([
                "### Recommended Templates",
            ])
            for template in recommendations['templates']:
                prompt_parts.append(f"- **{template['name']}** (score: {template['score']}): {template['reason']}")
            prompt_parts.append("")

        if recommendations['knowledge']:
            prompt_parts.extend([
                "### Knowledge Base Matches",
            ])
            for kb in recommendations['knowledge']:
                prompt_parts.append(f"- **{kb['name']}** (score: {kb['score']}): {kb['reason']}")
            prompt_parts.append("")

        prompt_parts.extend([
            "### Summary",
            recommendations['summary'],
            "",
            "---",
            "Please review these recommendations and refine based on the user's specific needs.",
            "Consider whether the detected patterns align with the user's stated intent.",
        ])

        return "\n".join(prompt_parts)
