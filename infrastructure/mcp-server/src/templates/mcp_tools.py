"""MCP tools for Templates, Skills, and Knowledge Base."""
import json
import os
from pathlib import Path
from typing import Optional
from .service import TemplateService
from .types import CustomPrompt


# Documentation directory path (relative to project root)
DOCS_DIR = Path("current application/extracted_app/HIERARCHY_BUILDER_APP/dataamp-ai/docs")
UI_ROOT = Path("current application/extracted_app/HIERARCHY_BUILDER_APP/dataamp-ai")


def register_template_tools(mcp, templates_dir: str = "templates",
                           skills_dir: str = "skills",
                           kb_dir: str = "knowledge_base",
                           hierarchy_service=None):
    """Register all template, skill, and knowledge base tools with the MCP server.

    Args:
        mcp: FastMCP server instance
        templates_dir: Path to templates directory
        skills_dir: Path to skills directory
        kb_dir: Path to knowledge base directory
        hierarchy_service: Optional HierarchyService instance for project integration

    Returns:
        TemplateService instance
    """
    service = TemplateService(templates_dir, skills_dir, kb_dir)

    # =========================================================================
    # Template Tools (5)
    # =========================================================================

    @mcp.tool()
    def list_financial_templates(category: str = "", industry: str = "") -> str:
        """
        List available financial statement templates.

        Use this tool to recommend templates when users want to build hierarchies.
        Templates provide pre-defined structures for common financial statements.

        Args:
            category: Filter by category ('income_statement', 'balance_sheet', 'cash_flow', 'custom').
                     Leave empty to show all.
            industry: Filter by industry (e.g., 'oil_gas', 'manufacturing', 'general').
                     Leave empty to show all.

        Returns:
            JSON with list of available templates including name, category, industry, and description.
        """
        try:
            templates = service.list_templates(
                category=category if category else None,
                industry=industry if industry else None
            )

            result = {
                "total": len(templates),
                "templates": [t.model_dump() for t in templates]
            }

            if not templates:
                result["note"] = "No templates found. Create templates using save_project_as_template."

            return json.dumps(result, indent=2, default=str)

        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_template_details(template_id: str) -> str:
        """
        Get full details of a template including hierarchy structure.

        Use this to examine a template's structure before creating a project from it.

        Args:
            template_id: The unique identifier of the template.

        Returns:
            JSON with complete template details including all hierarchy nodes and their relationships.
        """
        try:
            template = service.get_template(template_id)

            if not template:
                return json.dumps({"error": f"Template '{template_id}' not found"})

            return json.dumps(template.model_dump(), indent=2, default=str)

        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def create_project_from_template(template_id: str, project_name: str) -> str:
        """
        Create a new hierarchy project pre-populated from a template.

        This creates a complete project with all hierarchies defined in the template,
        saving significant time compared to manual creation.

        Args:
            template_id: The template to use as the base.
            project_name: Name for the new hierarchy project.

        Returns:
            JSON with project details and list of created hierarchies.
        """
        try:
            if not hierarchy_service:
                return json.dumps({
                    "error": "Hierarchy service not available. Cannot create project."
                })

            result = service.create_project_from_template(
                template_id=template_id,
                project_name=project_name,
                hierarchy_service=hierarchy_service
            )

            if not result:
                return json.dumps({"error": f"Template '{template_id}' not found"})

            return json.dumps(result, indent=2, default=str)

        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def save_project_as_template(project_id: str, template_name: str,
                                  category: str, description: str,
                                  industry: str = "general") -> str:
        """
        Save an existing hierarchy project as a reusable template.

        Use this after building a hierarchy structure to make it reusable
        for future projects or other clients.

        Args:
            project_id: The project to convert to a template.
            template_name: Name for the new template.
            category: Template category ('income_statement', 'balance_sheet', 'cash_flow', 'custom').
            description: Description of what this template is for.
            industry: Target industry ('general', 'oil_gas', 'manufacturing', etc.).

        Returns:
            JSON with the created template details.
        """
        try:
            if not hierarchy_service:
                return json.dumps({
                    "error": "Hierarchy service not available. Cannot save template."
                })

            template = service.save_project_as_template(
                project_id=project_id,
                template_name=template_name,
                category=category,
                description=description,
                hierarchy_service=hierarchy_service,
                industry=industry
            )

            if not template:
                return json.dumps({"error": f"Project '{project_id}' not found"})

            return json.dumps({
                "status": "success",
                "message": f"Template '{template_name}' created successfully",
                "template": template.to_metadata().model_dump()
            }, indent=2, default=str)

        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_template_recommendations(industry: str = "", statement_type: str = "") -> str:
        """
        Get AI recommendations for which template to use based on context.

        Use this when a user describes their needs to suggest the best template.

        Args:
            industry: The user's industry (e.g., 'oil_gas', 'manufacturing', 'retail').
            statement_type: The type of statement needed ('pl', 'p&l', 'balance_sheet', 'cash_flow').

        Returns:
            JSON with ranked template recommendations and reasoning.
        """
        try:
            recommendations = service.get_template_recommendations(
                industry=industry if industry else None,
                statement_type=statement_type if statement_type else None
            )

            result = {
                "total_recommendations": len(recommendations),
                "recommendations": [r.model_dump() for r in recommendations]
            }

            if not recommendations:
                result["note"] = "No matching templates found. Consider creating a custom template."

            return json.dumps(result, indent=2, default=str)

        except Exception as e:
            return json.dumps({"error": str(e)})

    # =========================================================================
    # Skill Tools (3)
    # =========================================================================

    @mcp.tool()
    def list_available_skills() -> str:
        """
        List all AI expertise skills available for financial analysis.

        Skills provide specialized knowledge and prompts for different domains
        like general financial analysis or oil & gas FP&A.

        Returns:
            JSON with list of available skills including capabilities and target industries.
        """
        try:
            skills = service.list_skills()

            result = {
                "total": len(skills),
                "skills": [s.model_dump() for s in skills]
            }

            if not skills:
                result["note"] = "No skills found. Check skills/index.json configuration."

            return json.dumps(result, indent=2, default=str)

        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_skill_details(skill_id: str) -> str:
        """
        Get detailed information about a specific skill including capabilities.

        Use this to understand what a skill provides and when to use it.

        Args:
            skill_id: The unique identifier of the skill.

        Returns:
            JSON with skill details including capabilities, industries, and file references.
        """
        try:
            skill = service.get_skill(skill_id)

            if not skill:
                return json.dumps({"error": f"Skill '{skill_id}' not found"})

            # Include documentation preview
            doc_content = service.get_skill_documentation(skill_id)
            doc_preview = doc_content[:500] + "..." if doc_content and len(doc_content) > 500 else doc_content

            result = skill.model_dump()
            result["documentation_preview"] = doc_preview

            return json.dumps(result, indent=2, default=str)

        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_skill_prompt(skill_id: str) -> str:
        """
        Get the system prompt for a skill to adopt that expertise.

        Use this prompt to configure the AI to act as a specialist in the given domain.

        Args:
            skill_id: The unique identifier of the skill.

        Returns:
            The full system prompt content for the skill, or error if not found.
        """
        try:
            prompt = service.get_skill_prompt(skill_id)

            if not prompt:
                skill = service.get_skill(skill_id)
                if not skill:
                    return json.dumps({"error": f"Skill '{skill_id}' not found"})
                return json.dumps({"error": f"Prompt file not found for skill '{skill_id}'"})

            return json.dumps({
                "skill_id": skill_id,
                "prompt": prompt
            }, indent=2)

        except Exception as e:
            return json.dumps({"error": str(e)})

    # =========================================================================
    # Knowledge Base Tools (4)
    # =========================================================================

    @mcp.tool()
    def list_client_profiles() -> str:
        """
        List all client knowledge base profiles.

        Client profiles store client-specific information like COA patterns,
        custom prompts, and known GL mappings.

        Returns:
            JSON with list of client profiles including industry and ERP system.
        """
        try:
            clients = service.list_clients()

            result = {
                "total": len(clients),
                "clients": [c.model_dump() for c in clients]
            }

            if not clients:
                result["note"] = "No client profiles found. Create one using create_client_profile."

            return json.dumps(result, indent=2, default=str)

        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_client_knowledge(client_id: str) -> str:
        """
        Get client-specific knowledge including COA patterns, prompts, and notes.

        Use this to understand a client's specific requirements and configurations.

        Args:
            client_id: The unique identifier of the client.

        Returns:
            JSON with full client knowledge base including custom prompts and mappings.
        """
        try:
            knowledge = service.get_client_knowledge(client_id)

            if not knowledge:
                return json.dumps({"error": f"Client '{client_id}' not found"})

            return json.dumps(knowledge.model_dump(), indent=2, default=str)

        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def update_client_knowledge(client_id: str, field: str, value: str) -> str:
        """
        Update a specific field in client knowledge base.

        Supported fields: client_name, industry, erp_system, chart_of_accounts_pattern,
        preferred_template_id, preferred_skill_id, notes, gl_patterns

        Args:
            client_id: The unique identifier of the client.
            field: The field to update.
            value: The new value (string for most fields, JSON string for gl_patterns).

        Returns:
            JSON with updated client knowledge base.
        """
        try:
            # Parse value if it looks like JSON (for gl_patterns)
            parsed_value = value
            if field == "gl_patterns":
                try:
                    parsed_value = json.loads(value)
                except json.JSONDecodeError:
                    return json.dumps({
                        "error": "gl_patterns must be a valid JSON object (e.g., '{\"4000\": \"Revenue\"}')"
                    })

            knowledge = service.update_client_knowledge(client_id, field, parsed_value)

            if not knowledge:
                return json.dumps({"error": f"Client '{client_id}' not found"})

            return json.dumps({
                "status": "success",
                "message": f"Updated {field} for client '{client_id}'",
                "client": knowledge.model_dump()
            }, indent=2, default=str)

        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def create_client_profile(client_id: str, client_name: str,
                               industry: str = "general", erp_system: str = "") -> str:
        """
        Create a new client knowledge base profile.

        Client profiles help store and retrieve client-specific information
        for consistent handling across sessions.

        Args:
            client_id: Unique identifier for the client (e.g., 'acme', 'client_001').
            client_name: Display name for the client (e.g., 'ACME Corporation').
            industry: Client's industry ('general', 'oil_gas', 'manufacturing', etc.).
            erp_system: Client's ERP system ('SAP', 'Oracle', 'NetSuite', etc.).

        Returns:
            JSON with the created client profile.
        """
        try:
            # Check if client already exists
            existing = service.get_client_knowledge(client_id)
            if existing:
                return json.dumps({
                    "error": f"Client '{client_id}' already exists. Use update_client_knowledge to modify."
                })

            knowledge = service.create_client(
                client_id=client_id,
                client_name=client_name,
                industry=industry,
                erp_system=erp_system if erp_system else None
            )

            return json.dumps({
                "status": "success",
                "message": f"Client profile '{client_name}' created successfully",
                "client": knowledge.model_dump()
            }, indent=2, default=str)

        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def add_client_custom_prompt(client_id: str, name: str, trigger: str,
                                  content: str, category: str = "general") -> str:
        """
        Add a custom prompt to a client's knowledge base.

        Custom prompts help store client-specific instructions that can be
        used when working on that client's data.

        Args:
            client_id: The client to add the prompt to.
            name: Name for the prompt (e.g., 'Revenue Recognition Rules').
            trigger: When to use this prompt (e.g., 'When mapping revenue accounts').
            content: The actual prompt content/instructions.
            category: Prompt category for organization.

        Returns:
            JSON with the updated client profile including the new prompt.
        """
        try:
            prompt = CustomPrompt(
                id="",  # Will be generated
                name=name,
                trigger=trigger,
                content=content,
                category=category
            )

            knowledge = service.add_client_prompt(client_id, prompt)

            if not knowledge:
                return json.dumps({"error": f"Client '{client_id}' not found"})

            return json.dumps({
                "status": "success",
                "message": f"Custom prompt '{name}' added to client '{client_id}'",
                "prompt_count": len(knowledge.custom_prompts)
            }, indent=2, default=str)

        except Exception as e:
            return json.dumps({"error": str(e)})

    # =========================================================================
    # Documentation Tools (3)
    # =========================================================================

    @mcp.tool()
    def list_application_documentation() -> str:
        """
        List all available application documentation files.

        Use this to discover what documentation is available for the DataBridge AI
        UI application including user guides, API docs, and setup instructions.

        Returns:
            JSON with list of documentation files and their descriptions.
        """
        try:
            docs = []

            # Check UI docs directory
            if DOCS_DIR.exists():
                for doc_file in DOCS_DIR.glob("*.md"):
                    docs.append({
                        "file": doc_file.name,
                        "path": str(doc_file.relative_to(UI_ROOT)),
                        "type": "markdown",
                        "location": "docs"
                    })

            # Check root markdown files
            if UI_ROOT.exists():
                for doc_file in UI_ROOT.glob("*.md"):
                    # Skip node_modules
                    if "node_modules" not in str(doc_file):
                        docs.append({
                            "file": doc_file.name,
                            "path": str(doc_file.relative_to(UI_ROOT)),
                            "type": "markdown",
                            "location": "root"
                        })

            # Add known documentation
            known_docs = [
                {"file": "USER_GUIDE.md", "description": "Comprehensive user guide for all features"},
                {"file": "README.md", "description": "Quick start and overview"},
                {"file": "API_DOCUMENTATION.md", "description": "Complete API reference"},
                {"file": "SETUP.md", "description": "Installation and setup guide"},
                {"file": "SECURITY.md", "description": "Security best practices"},
                {"file": "PRD.md", "description": "Product requirements document"},
            ]

            return json.dumps({
                "total": len(docs),
                "documentation": docs,
                "known_docs": known_docs
            }, indent=2)

        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_application_documentation(doc_name: str) -> str:
        """
        Read the content of a specific application documentation file.

        Use this to read user guides, API documentation, or other app docs
        to help answer user questions about the application.

        Args:
            doc_name: Name of the documentation file (e.g., 'USER_GUIDE.md', 'README.md').

        Returns:
            The full content of the documentation file, or error if not found.
        """
        try:
            # Search in docs directory first
            doc_path = DOCS_DIR / doc_name
            if doc_path.exists():
                content = doc_path.read_text(encoding='utf-8')
                return json.dumps({
                    "file": doc_name,
                    "path": str(doc_path.relative_to(UI_ROOT)),
                    "content": content
                }, indent=2)

            # Search in root directory
            doc_path = UI_ROOT / doc_name
            if doc_path.exists():
                content = doc_path.read_text(encoding='utf-8')
                return json.dumps({
                    "file": doc_name,
                    "path": str(doc_path.relative_to(UI_ROOT)),
                    "content": content
                }, indent=2)

            # Try common variations
            variations = [
                doc_name,
                doc_name.upper(),
                doc_name.lower(),
                doc_name.replace('.md', '') + '.md',
            ]

            for variation in variations:
                for search_dir in [DOCS_DIR, UI_ROOT]:
                    doc_path = search_dir / variation
                    if doc_path.exists():
                        content = doc_path.read_text(encoding='utf-8')
                        return json.dumps({
                            "file": variation,
                            "path": str(doc_path.relative_to(UI_ROOT)),
                            "content": content
                        }, indent=2)

            return json.dumps({
                "error": f"Documentation file '{doc_name}' not found",
                "hint": "Use list_application_documentation to see available files"
            })

        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_user_guide_section(section: str = "") -> str:
        """
        Get a specific section from the user guide or the full guide.

        Use this to help users understand features and how to use the application.

        Args:
            section: Section to retrieve (e.g., 'Templates', 'Skills', 'Connections').
                    Leave empty to get the full guide.

        Returns:
            The requested section content or full user guide.
        """
        try:
            doc_path = DOCS_DIR / "USER_GUIDE.md"

            if not doc_path.exists():
                return json.dumps({
                    "error": "User guide not found",
                    "hint": "Check if USER_GUIDE.md exists in the docs directory"
                })

            content = doc_path.read_text(encoding='utf-8')

            if not section:
                return json.dumps({
                    "file": "USER_GUIDE.md",
                    "content": content
                }, indent=2)

            # Try to extract the section
            lines = content.split('\n')
            in_section = False
            section_content = []
            section_level = 0

            for line in lines:
                # Check for section headers
                if line.startswith('#'):
                    header_level = len(line.split()[0])
                    header_text = line.lstrip('#').strip()

                    if section.lower() in header_text.lower():
                        in_section = True
                        section_level = header_level
                        section_content.append(line)
                        continue

                    if in_section and header_level <= section_level:
                        # New section at same or higher level, stop
                        break

                if in_section:
                    section_content.append(line)

            if section_content:
                return json.dumps({
                    "section": section,
                    "content": '\n'.join(section_content)
                }, indent=2)

            return json.dumps({
                "error": f"Section '{section}' not found in user guide",
                "available_sections": [
                    "Getting Started",
                    "Schema Matcher",
                    "Report Matcher",
                    "Hierarchy Knowledge Base",
                    "AI Configuration",
                    "Templates",
                    "Skills",
                    "Knowledge Base",
                    "Connections",
                    "Settings",
                    "Tips & Best Practices"
                ]
            })

        except Exception as e:
            return json.dumps({"error": str(e)})

    return service
