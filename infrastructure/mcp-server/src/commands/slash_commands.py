"""
Slash Command Tools for DataBridge AI MCP Server.

These tools enable Claude to execute slash commands from the DataBridge AI frontend.
Commands map to common UI actions like navigation, templates, and AI features.
"""
import json
import logging
from typing import Optional, Dict, Any, List

logger = logging.getLogger("slash_commands")

# Available slash commands with their descriptions
SLASH_COMMANDS = {
    # Navigation Commands
    "/go dashboard": {
        "category": "navigation",
        "description": "Navigate to the main dashboard",
        "action": "navigate",
        "target": "dashboard",
    },
    "/go hierarchy": {
        "category": "navigation",
        "description": "Navigate to Hierarchy Knowledge Base",
        "action": "navigate",
        "target": "hierarchy-knowledge-base",
    },
    "/go connections": {
        "category": "navigation",
        "description": "Navigate to database connections",
        "action": "navigate",
        "target": "connections",
    },
    "/go ai-config": {
        "category": "navigation",
        "description": "Navigate to AI configuration",
        "action": "navigate",
        "target": "ai-config",
    },
    "/go settings": {
        "category": "navigation",
        "description": "Navigate to settings",
        "action": "navigate",
        "target": "settings",
    },
    "/go docs": {
        "category": "navigation",
        "description": "Navigate to documentation",
        "action": "navigate",
        "target": "docs",
    },

    # Template Commands
    "/templates": {
        "category": "templates",
        "description": "List all available templates",
        "action": "list_templates",
    },
    "/template pl": {
        "category": "templates",
        "description": "Use Standard P&L template",
        "action": "use_template",
        "template_id": "standard_pl",
    },
    "/template bs": {
        "category": "templates",
        "description": "Use Balance Sheet template",
        "action": "use_template",
        "template_id": "standard_bs",
    },
    "/template oil-gas": {
        "category": "templates",
        "description": "Use Oil & Gas LOS template",
        "action": "use_template",
        "template_id": "oil_gas_los",
    },
    "/template saas": {
        "category": "templates",
        "description": "Use SaaS P&L template",
        "action": "use_template",
        "template_id": "saas_pl",
    },

    # Action Commands
    "/new project": {
        "category": "actions",
        "description": "Create a new hierarchy project",
        "action": "create_project",
    },
    "/import": {
        "category": "actions",
        "description": "Import CSV or Excel file",
        "action": "import",
    },
    "/export": {
        "category": "actions",
        "description": "Export current project",
        "action": "export",
    },
    "/generate script": {
        "category": "actions",
        "description": "Generate SQL deployment scripts",
        "action": "generate_script",
    },
    "/formulas": {
        "category": "actions",
        "description": "Open formula manager",
        "action": "manage_formulas",
    },

    # AI Commands
    "/ai": {
        "category": "ai",
        "description": "List available AI features",
        "action": "list_ai_features",
    },
    "/ai suggest": {
        "category": "ai",
        "description": "Get AI mapping suggestions for selected hierarchy",
        "action": "ai_suggest_mappings",
    },
    "/ai build": {
        "category": "ai",
        "description": "Build hierarchy from natural language",
        "action": "ai_build_hierarchy",
    },
    "/ai chat": {
        "category": "ai",
        "description": "Open AI chat assistant",
        "action": "ai_chat",
    },
    "/ai analyze": {
        "category": "ai",
        "description": "Analyze current hierarchy structure",
        "action": "ai_analyze",
    },

    # Help Commands
    "/help": {
        "category": "help",
        "description": "Show help and available commands",
        "action": "show_help",
    },
    "/help templates": {
        "category": "help",
        "description": "Show help for template commands",
        "action": "show_help",
        "topic": "templates",
    },
    "/help ai": {
        "category": "help",
        "description": "Show help for AI commands",
        "action": "show_help",
        "topic": "ai",
    },
}

# Template definitions
TEMPLATES = {
    "standard_pl": {
        "id": "standard_pl",
        "name": "Standard P&L",
        "domain": "accounting",
        "hierarchy_type": "income_statement",
        "industry": "general",
        "description": "Standard income statement structure for most businesses",
        "hierarchy_count": 18,
    },
    "standard_bs": {
        "id": "standard_bs",
        "name": "Standard Balance Sheet",
        "domain": "accounting",
        "hierarchy_type": "balance_sheet",
        "industry": "general",
        "description": "Standard balance sheet with assets, liabilities, and equity",
        "hierarchy_count": 20,
    },
    "oil_gas_los": {
        "id": "oil_gas_los",
        "name": "Oil & Gas LOS",
        "domain": "accounting",
        "hierarchy_type": "income_statement",
        "industry": "oil_gas_upstream",
        "description": "Lease Operating Statement for upstream oil & gas operations",
        "hierarchy_count": 28,
    },
    "saas_pl": {
        "id": "saas_pl",
        "name": "SaaS P&L",
        "domain": "accounting",
        "hierarchy_type": "income_statement",
        "industry": "saas",
        "description": "SaaS-specific P&L with ARR/MRR tracking and unit economics",
        "hierarchy_count": 35,
    },
    "upstream_oil_gas_pl": {
        "id": "upstream_oil_gas_pl",
        "name": "Upstream Oil & Gas P&L",
        "domain": "accounting",
        "hierarchy_type": "income_statement",
        "industry": "oil_gas_upstream",
        "description": "E&P company income statement with LOE breakdown",
        "hierarchy_count": 32,
    },
    "manufacturing_pl": {
        "id": "manufacturing_pl",
        "name": "Manufacturing P&L",
        "domain": "accounting",
        "hierarchy_type": "income_statement",
        "industry": "manufacturing",
        "description": "Industrial manufacturing P&L with COGS breakdown",
        "hierarchy_count": 25,
    },
}


def register_slash_command_tools(mcp):
    """Register slash command MCP tools with the server."""

    @mcp.tool()
    def execute_slash_command(command: str, context: Optional[str] = None) -> str:
        """
        Execute a slash command from the DataBridge AI frontend.

        This tool processes slash commands that users type in the search bar.
        Commands start with '/' and perform various actions.

        Args:
            command: The slash command to execute (e.g., "/templates", "/ai suggest")
            context: Optional context like current project ID or hierarchy ID

        Returns:
            JSON with the command result and any data to display

        Examples:
            - execute_slash_command("/templates") -> Lists all templates
            - execute_slash_command("/ai suggest", "project_123") -> AI suggestions
            - execute_slash_command("/go dashboard") -> Navigate to dashboard
        """
        try:
            # Normalize command
            cmd = command.strip().lower()

            # Find matching command
            matched_cmd = None
            for key in SLASH_COMMANDS:
                if cmd == key or cmd.startswith(key + " "):
                    matched_cmd = SLASH_COMMANDS[key]
                    break

            if not matched_cmd:
                # Return suggestions for similar commands
                suggestions = [k for k in SLASH_COMMANDS.keys() if cmd[1:] in k]
                return json.dumps({
                    "status": "error",
                    "message": f"Unknown command: {command}",
                    "suggestions": suggestions[:5],
                    "hint": "Type /help to see all available commands",
                }, indent=2)

            # Execute based on action type
            action = matched_cmd.get("action")

            if action == "navigate":
                return json.dumps({
                    "status": "success",
                    "action": "navigate",
                    "target": matched_cmd.get("target"),
                    "message": f"Navigating to {matched_cmd.get('target')}",
                }, indent=2)

            elif action == "list_templates":
                return json.dumps({
                    "status": "success",
                    "action": "list_templates",
                    "templates": list(TEMPLATES.values()),
                    "message": f"Found {len(TEMPLATES)} templates",
                }, indent=2)

            elif action == "use_template":
                template_id = matched_cmd.get("template_id")
                template = TEMPLATES.get(template_id)
                if template:
                    return json.dumps({
                        "status": "success",
                        "action": "use_template",
                        "template": template,
                        "message": f"Ready to create project from '{template['name']}' template",
                        "next_step": "Call create_project_from_template with the template_id",
                    }, indent=2)
                return json.dumps({
                    "status": "error",
                    "message": f"Template '{template_id}' not found",
                }, indent=2)

            elif action == "list_ai_features":
                ai_features = [
                    {"id": "suggest_mappings", "name": "AI Mapping Suggestions", "description": "Get AI-powered suggestions for source mappings"},
                    {"id": "build_hierarchy", "name": "Natural Language Builder", "description": "Build hierarchies from text descriptions"},
                    {"id": "analyze_structure", "name": "Structure Analysis", "description": "Analyze hierarchy for issues and improvements"},
                    {"id": "chat_assistant", "name": "AI Chat", "description": "Chat with AI for help and guidance"},
                    {"id": "anomaly_detection", "name": "Anomaly Detection", "description": "Detect mapping inconsistencies"},
                ]
                return json.dumps({
                    "status": "success",
                    "action": "list_ai_features",
                    "features": ai_features,
                    "message": "Available AI features",
                    "recommended_provider": "Claude (Anthropic) - Best for complex financial analysis",
                }, indent=2)

            elif action == "ai_suggest_mappings":
                return json.dumps({
                    "status": "ready",
                    "action": "ai_suggest_mappings",
                    "message": "Ready to generate mapping suggestions",
                    "requires": ["project_id", "hierarchy_id"],
                    "hint": "Select a hierarchy first, then I can suggest appropriate source mappings",
                }, indent=2)

            elif action == "ai_build_hierarchy":
                return json.dumps({
                    "status": "ready",
                    "action": "ai_build_hierarchy",
                    "message": "Ready to build hierarchy from natural language",
                    "hint": "Describe the hierarchy structure you want to create",
                    "examples": [
                        "Create a P&L with Revenue, COGS, Gross Profit, OpEx, and Net Income",
                        "Build a balance sheet with Current Assets, Fixed Assets, Liabilities, and Equity",
                    ],
                }, indent=2)

            elif action == "ai_analyze":
                return json.dumps({
                    "status": "ready",
                    "action": "ai_analyze",
                    "message": "Ready to analyze hierarchy structure",
                    "requires": ["project_id"],
                    "analysis_types": [
                        "completeness - Check for missing mappings",
                        "consistency - Check naming and structure consistency",
                        "optimization - Suggest structure improvements",
                    ],
                }, indent=2)

            elif action == "show_help":
                topic = matched_cmd.get("topic")
                if topic:
                    cmds = {k: v for k, v in SLASH_COMMANDS.items() if v["category"] == topic}
                else:
                    cmds = SLASH_COMMANDS

                help_text = []
                by_category: Dict[str, List[Dict[str, Any]]] = {}
                for cmd_key, cmd_val in cmds.items():
                    cat = cmd_val["category"]
                    if cat not in by_category:
                        by_category[cat] = []
                    by_category[cat].append({"command": cmd_key, "description": cmd_val["description"]})

                return json.dumps({
                    "status": "success",
                    "action": "show_help",
                    "topic": topic or "all",
                    "commands_by_category": by_category,
                    "message": f"Found {len(cmds)} commands" + (f" in category '{topic}'" if topic else ""),
                }, indent=2)

            elif action in ["create_project", "import", "export", "generate_script", "manage_formulas", "ai_chat"]:
                return json.dumps({
                    "status": "ready",
                    "action": action,
                    "message": f"Ready to {action.replace('_', ' ')}",
                    "hint": f"This action can be performed in the UI. Navigate to hierarchy-knowledge-base.",
                }, indent=2)

            return json.dumps({
                "status": "success",
                "command": command,
                "matched": matched_cmd,
            }, indent=2)

        except Exception as e:
            logger.error(f"Error executing slash command: {e}")
            return json.dumps({
                "status": "error",
                "message": str(e),
            }, indent=2)

    @mcp.tool()
    def list_slash_commands(category: Optional[str] = None) -> str:
        """
        List all available slash commands.

        Args:
            category: Optional filter by category (navigation, templates, ai, actions, help)

        Returns:
            JSON with list of available commands
        """
        try:
            if category:
                cmds = {k: v for k, v in SLASH_COMMANDS.items() if v["category"] == category}
            else:
                cmds = SLASH_COMMANDS

            # Organize by category
            by_category: Dict[str, List[Dict[str, str]]] = {}
            for cmd_key, cmd_val in cmds.items():
                cat = cmd_val["category"]
                if cat not in by_category:
                    by_category[cat] = []
                by_category[cat].append({
                    "command": cmd_key,
                    "description": cmd_val["description"],
                })

            return json.dumps({
                "status": "success",
                "total_commands": len(cmds),
                "commands_by_category": by_category,
                "categories": list(by_category.keys()),
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def get_template_details(template_id: str) -> str:
        """
        Get detailed information about a specific template.

        Args:
            template_id: The template ID (e.g., "standard_pl", "oil_gas_los")

        Returns:
            JSON with template details including structure
        """
        try:
            template = TEMPLATES.get(template_id)
            if not template:
                return json.dumps({
                    "status": "error",
                    "message": f"Template '{template_id}' not found",
                    "available_templates": list(TEMPLATES.keys()),
                }, indent=2)

            return json.dumps({
                "status": "success",
                "template": template,
                "usage": f"To create a project from this template, use: create_project_from_template('{template_id}', 'Your Project Name')",
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    @mcp.tool()
    def suggest_command(user_input: str) -> str:
        """
        Suggest slash commands based on user input or intent.

        Args:
            user_input: What the user is trying to do

        Returns:
            JSON with suggested commands and their descriptions
        """
        try:
            input_lower = user_input.lower()

            suggestions = []

            # Match by keywords
            keyword_mappings = {
                ("template", "templates", "create from template"): ["/templates", "/template pl", "/template bs"],
                ("ai", "artificial", "intelligence", "suggest", "suggestion"): ["/ai", "/ai suggest", "/ai build"],
                ("navigate", "go", "open", "view"): ["/go dashboard", "/go hierarchy", "/go connections"],
                ("help", "how", "what"): ["/help", "/help templates", "/help ai"],
                ("project", "new", "create"): ["/new project"],
                ("import", "csv", "excel", "upload"): ["/import"],
                ("export", "download"): ["/export"],
                ("script", "sql", "generate", "deploy"): ["/generate script"],
                ("formula", "calculation"): ["/formulas"],
            }

            for keywords, cmds in keyword_mappings.items():
                if any(kw in input_lower for kw in keywords):
                    for cmd in cmds:
                        if cmd in SLASH_COMMANDS and cmd not in [s["command"] for s in suggestions]:
                            suggestions.append({
                                "command": cmd,
                                "description": SLASH_COMMANDS[cmd]["description"],
                                "category": SLASH_COMMANDS[cmd]["category"],
                            })

            if not suggestions:
                # Return general suggestions
                suggestions = [
                    {"command": "/help", "description": "Show all available commands", "category": "help"},
                    {"command": "/templates", "description": "Browse hierarchy templates", "category": "templates"},
                    {"command": "/ai", "description": "View AI features", "category": "ai"},
                ]

            return json.dumps({
                "status": "success",
                "input": user_input,
                "suggestions": suggestions[:5],
                "hint": "Type the suggested command to execute it",
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": str(e)})

    logger.info("Registered slash command tools")
