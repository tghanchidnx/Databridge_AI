"""
MCP Tools for the Smart Recommendation Engine.

Exposes recommendation capabilities through the MCP protocol.
"""

import json
import logging
from typing import Any, Optional

from .recommendation_engine import RecommendationEngine

logger = logging.getLogger(__name__)

# Singleton engine instance
_engine: Optional[RecommendationEngine] = None


def get_engine() -> RecommendationEngine:
    """Get or create the singleton RecommendationEngine."""
    global _engine
    if _engine is None:
        _engine = RecommendationEngine()
    return _engine


def register_recommendation_tools(mcp: Any) -> None:
    """
    Register all recommendation MCP tools.

    Args:
        mcp: The FastMCP instance
    """

    @mcp.tool()
    def get_smart_recommendations(
        file_path: str = "",
        content: str = "",
        user_intent: str = "",
        client_id: str = "",
        industry: str = "",
        target_database: str = "",
        target_schema: str = "",
        target_table: str = "",
    ) -> str:
        """
        Get smart recommendations for importing a CSV file.

        This tool analyzes your CSV data and provides context-aware recommendations
        by combining:
        - Data profiling (column analysis, pattern detection)
        - Skill selection (domain expertise like FP&A, Manufacturing, Oil & Gas)
        - Template matching (industry-specific hierarchies)
        - Knowledge base lookups (client-specific patterns and preferences)

        The recommendations help you:
        1. Choose the right import tier (1-4 based on complexity)
        2. Select appropriate domain expertise (skills)
        3. Find matching templates for your industry
        4. Apply known patterns from previous work

        Args:
            file_path: Path to CSV file to analyze
            content: Raw CSV content (alternative to file_path)
            user_intent: What you want to accomplish (e.g., "Build a P&L hierarchy for oil & gas")
            client_id: Client ID for knowledge base lookup (optional)
            industry: Known industry override (e.g., "oil_gas", "manufacturing", "saas")
            target_database: Target database for deployment hints
            target_schema: Target schema for deployment hints
            target_table: Target table for deployment hints

        Returns:
            JSON containing:
            - data_profile: Analyzed data structure and patterns
            - import_tier: Recommended tier (1-4) with reasoning
            - skills: Top 3 skill recommendations with scores
            - templates: Top 3 template recommendations with scores
            - knowledge: Knowledge base matches (if client_id provided)
            - summary: Human-readable summary of recommendations

        Example:
            get_smart_recommendations(
                file_path="C:/data/gl_accounts.csv",
                user_intent="Build a P&L hierarchy for upstream oil and gas",
                industry="oil_gas"
            )
        """
        engine = get_engine()

        recommendations = engine.get_recommendations(
            file_path=file_path if file_path else None,
            content=content if content else None,
            user_intent=user_intent if user_intent else None,
            client_id=client_id if client_id else None,
            industry=industry if industry else None,
            target_database=target_database if target_database else None,
            target_schema=target_schema if target_schema else None,
            target_table=target_table if target_table else None,
        )

        return json.dumps(recommendations, indent=2)

    @mcp.tool()
    def get_llm_validation_prompt(
        file_path: str = "",
        content: str = "",
        user_intent: str = "",
        client_id: str = "",
        industry: str = "",
    ) -> str:
        """
        Get a formatted prompt for LLM validation of recommendations.

        This tool generates recommendations and formats them as a structured
        prompt that you (the LLM) can use to validate and refine the
        suggestions based on the user's specific needs.

        Use this when you want to:
        1. Review DataBridge's automated recommendations
        2. Apply your knowledge to refine suggestions
        3. Explain the recommendations to the user
        4. Suggest modifications based on context

        Args:
            file_path: Path to CSV file to analyze
            content: Raw CSV content (alternative to file_path)
            user_intent: What the user wants to accomplish
            client_id: Client ID for knowledge base lookup
            industry: Known industry override

        Returns:
            Formatted markdown prompt with recommendations for LLM review

        Example:
            get_llm_validation_prompt(
                file_path="C:/data/chart_of_accounts.csv",
                user_intent="Create a standard P&L structure for manufacturing"
            )
        """
        engine = get_engine()

        recommendations = engine.get_recommendations(
            file_path=file_path if file_path else None,
            content=content if content else None,
            user_intent=user_intent if user_intent else None,
            client_id=client_id if client_id else None,
            industry=industry if industry else None,
        )

        return engine.format_for_llm_validation(recommendations)

    @mcp.tool()
    def suggest_enrichment_after_hierarchy(
        project_id: str,
        file_path: str = "",
        user_intent: str = "",
    ) -> str:
        """
        Suggest enrichment options after hierarchy import.

        Call this after importing a hierarchy to get recommendations for
        enriching the mapping file with additional detail columns from
        reference data (like Chart of Accounts).

        This helps complete the workflow:
        1. Import CSV → Hierarchy
        2. Suggest enrichment (this tool)
        3. Configure enrichment sources
        4. Enrich mapping file

        Args:
            project_id: The hierarchy project ID
            file_path: Original CSV file path (for context)
            user_intent: What the user wants to accomplish

        Returns:
            JSON with enrichment suggestions and next steps

        Example:
            suggest_enrichment_after_hierarchy(
                project_id="my-project",
                file_path="C:/data/gl_hierarchy.csv",
                user_intent="Add account names to the mapping export"
            )
        """
        engine = get_engine()

        # Get project context
        try:
            project = engine.hierarchy_service.get_project(project_id)
            hierarchies = engine.hierarchy_service.list_hierarchies(project_id)
            mappings = engine.hierarchy_service.get_all_mappings(project_id)
        except Exception as e:
            return json.dumps({
                "error": f"Could not load project: {e}",
                "project_id": project_id,
            })

        # Profile the original file if provided
        profile = None
        if file_path:
            profile = engine.profile_csv(file_path=file_path)

        # Build suggestions
        suggestions = {
            "project_id": project_id,
            "hierarchy_count": len(hierarchies) if hierarchies else 0,
            "mapping_count": len(mappings) if mappings else 0,
            "enrichment_suggestions": [],
            "next_steps": [],
        }

        # Suggest enrichment based on mappings
        if mappings:
            source_tables = set()
            source_columns = set()
            for m in mappings:
                if m.get("source_table"):
                    source_tables.add(m["source_table"])
                if m.get("source_column"):
                    source_columns.add(m["source_column"])

            if source_tables:
                suggestions["enrichment_suggestions"].append({
                    "type": "reference_data",
                    "description": f"Enrich with additional columns from source tables: {', '.join(source_tables)}",
                    "example_columns": ["ACCOUNT_NAME", "ACCOUNT_DESCRIPTION", "ACCOUNT_TYPE"],
                    "tool": "configure_mapping_enrichment",
                })

            if profile and profile.has_account_codes:
                suggestions["enrichment_suggestions"].append({
                    "type": "coa_lookup",
                    "description": "Add Chart of Accounts details (name, type, category)",
                    "example_columns": ["ACCOUNT_ID", "ACCOUNT_NAME", "ACCOUNT_BILLING_CATEGORY_CODE"],
                    "tool": "configure_mapping_enrichment",
                })

        # Suggest next steps
        suggestions["next_steps"] = [
            "1. Review the hierarchy structure with get_hierarchy_tree",
            "2. Configure enrichment source with configure_mapping_enrichment",
            "3. Export enriched mapping with export_mapping_csv",
            "4. Generate deployment scripts with generate_hierarchy_scripts",
        ]

        if user_intent:
            suggestions["user_intent"] = user_intent
            suggestions["next_steps"].insert(0, f"0. Validate: Does this hierarchy achieve '{user_intent}'?")

        return json.dumps(suggestions, indent=2)

    @mcp.tool()
    def smart_import_csv(
        file_path: str,
        project_name: str = "",
        user_intent: str = "",
        client_id: str = "",
        industry: str = "",
        use_recommendations: str = "true",
    ) -> str:
        """
        Smart CSV import with automatic recommendations.

        This is an intelligent wrapper around the flexible hierarchy import
        that automatically:
        1. Profiles the CSV data
        2. Gets recommendations (skill, template, tier)
        3. Configures the import based on detected patterns
        4. Imports the hierarchy
        5. Suggests next steps

        Use this for a streamlined import experience.

        Args:
            file_path: Path to CSV file to import
            project_name: Name for the new project (auto-generated if empty)
            user_intent: What you want to accomplish
            client_id: Client ID for knowledge base patterns
            industry: Industry override (detected if empty)
            use_recommendations: "true" to apply recommendations, "false" to just analyze

        Returns:
            JSON with import results and recommendations

        Example:
            smart_import_csv(
                file_path="C:/data/los_hierarchy.csv",
                project_name="Q4 LOS Hierarchy",
                user_intent="Build Lease Operating Statement hierarchy",
                industry="oil_gas"
            )
        """
        engine = get_engine()

        # Step 1: Get recommendations
        recommendations = engine.get_recommendations(
            file_path=file_path,
            user_intent=user_intent if user_intent else None,
            client_id=client_id if client_id else None,
            industry=industry if industry else None,
        )

        result = {
            "file_path": file_path,
            "recommendations": recommendations,
            "import_result": None,
            "next_steps": [],
        }

        # Step 2: If use_recommendations is false, just return analysis
        if use_recommendations.lower() != "true":
            result["status"] = "analysis_only"
            result["message"] = "Recommendations generated. Set use_recommendations='true' to import."
            return json.dumps(result, indent=2)

        # Step 3: Import using detected tier
        try:
            tier = recommendations["import_tier"]["tier"]
            detected_industry = recommendations["context"]["detected_industry"]

            # Auto-generate project name if not provided
            if not project_name:
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y%m%d_%H%M")
                industry_part = detected_industry or "general"
                project_name = f"{industry_part}_hierarchy_{timestamp}"

            # Create project
            project = engine.hierarchy_service.create_project(
                name=project_name,
                description=f"Smart import from {file_path}"
            )

            # Read file content
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # Import based on tier
            if tier <= 2:
                # Use flexible import for simple tiers
                from src.hierarchy.flexible_import import FlexibleHierarchyImporter
                importer = FlexibleHierarchyImporter(engine.hierarchy_service)

                # Configure defaults if we have recommendations
                defaults = {}
                if recommendations.get("knowledge"):
                    for kb in recommendations["knowledge"]:
                        if kb.get("details", {}).get("pattern"):
                            # Could configure source defaults here
                            pass

                import_result = importer.import_flexible_hierarchy(
                    project_id=project.id,
                    content=content,
                    source_defaults=defaults or None,
                )
                result["import_result"] = import_result
            else:
                # Use standard CSV import for complex tiers
                result["import_result"] = {
                    "status": "manual_required",
                    "tier": tier,
                    "message": f"Tier {tier} detected. Use import_hierarchy_csv for enterprise format.",
                    "project_id": project.id,
                }

            result["status"] = "success"
            result["project_id"] = project.id
            result["project_name"] = project_name

        except Exception as e:
            result["status"] = "error"
            result["error"] = str(e)
            logger.error(f"Smart import failed: {e}")

        # Step 4: Generate next steps
        result["next_steps"] = [
            f"1. Review hierarchy: get_hierarchy_tree(project_id='{result.get('project_id', 'PROJECT_ID')}')",
        ]

        if recommendations["skills"]:
            top_skill = recommendations["skills"][0]
            result["next_steps"].append(
                f"2. Load skill for expertise: get_skill_prompt(skill_id='{top_skill['id']}')"
            )

        if recommendations["templates"]:
            top_template = recommendations["templates"][0]
            result["next_steps"].append(
                f"3. Compare with template: get_template_details(template_id='{top_template['id']}')"
            )

        result["next_steps"].extend([
            "4. Add source mappings: add_source_mapping(...)",
            "5. Export for deployment: export_hierarchy_csv(...)",
            "6. Generate scripts: generate_hierarchy_scripts(...)",
        ])

        return json.dumps(result, indent=2)

    @mcp.tool()
    def get_recommendation_context(
        client_id: str = "",
        industry: str = "",
    ) -> str:
        """
        Get the full context available for recommendations.

        This tool shows what DataBridge knows that can be used for
        recommendations, including:
        - Available skills and their capabilities
        - Available templates for the industry
        - Client knowledge base (if client_id provided)

        Useful for understanding what recommendations are possible
        before importing data.

        Args:
            client_id: Client ID to show knowledge base
            industry: Industry to filter templates

        Returns:
            JSON with available context for recommendations

        Example:
            get_recommendation_context(industry="oil_gas")
        """
        engine = get_engine()

        context = {
            "skills": [],
            "templates": [],
            "knowledge_base": None,
        }

        # Get skills
        skills = engine.template_service.list_skills(industry=industry if industry else None)
        context["skills"] = [
            {
                "id": s.id,
                "name": s.name,
                "domain": s.domain.value,
                "industries": s.industries,
                "capabilities": s.capabilities[:5],
            }
            for s in skills
        ]

        # Get templates
        templates = engine.template_service.list_templates(industry=industry if industry else None)
        context["templates"] = [
            {
                "id": t.id,
                "name": t.name,
                "industry": t.industry,
                "domain": t.domain,
                "hierarchy_count": t.hierarchy_count,
            }
            for t in templates[:10]  # Limit to 10
        ]

        # Get knowledge base
        if client_id:
            client = engine.template_service.get_client_knowledge(client_id)
            if client:
                context["knowledge_base"] = {
                    "client_id": client.client_id,
                    "client_name": client.client_name,
                    "industry": client.industry,
                    "erp_system": client.erp_system,
                    "preferred_template": client.preferred_template_id,
                    "preferred_skill": client.preferred_skill_id,
                    "gl_pattern_count": len(client.gl_patterns),
                    "custom_prompt_count": len(client.custom_prompts),
                }

        return json.dumps(context, indent=2)

    logger.info("Registered 5 Recommendation Engine MCP tools")
