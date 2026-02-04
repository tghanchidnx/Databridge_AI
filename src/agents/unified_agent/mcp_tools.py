"""
MCP Tools for Unified AI Agent.

This module provides 10 new MCP tools for unified operations across
Book, Librarian, and Researcher systems:

Book ↔ Librarian Operations:
1. checkout_librarian_to_book - Convert Librarian project → Book for manipulation
2. promote_book_to_librarian - Create/update Librarian project from Book
3. sync_book_and_librarian - Bidirectional sync with conflict resolution
4. diff_book_and_librarian - Show differences between Book and project

Researcher Analytics:
5. analyze_book_with_researcher - Validate Book sources against database
6. compare_book_to_database - Compare hierarchy data to live data
7. profile_book_sources - Profile data for mapped columns

Unified Workflows:
8. create_unified_workflow - Plan workflow spanning all three systems
9. execute_unified_workflow - Execute planned workflow
10. get_unified_context - Get current context across all systems
"""

import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger("unified_agent.tools")

# Global MCP instance reference (set during registration)
_mcp = None


def register_unified_agent_tools(mcp):
    """
    Register all Unified Agent MCP tools.

    Args:
        mcp: FastMCP instance
    """
    global _mcp
    _mcp = mcp

    # Import dependencies
    from .context import get_context, UnifiedAgentContext
    from .bridges.librarian_bridge import LibrarianBridge
    from .bridges.researcher_bridge import ResearcherBridge

    # =========================================================================
    # Tool 1: Checkout Librarian to Book
    # =========================================================================

    @mcp.tool()
    def checkout_librarian_to_book(
        project_id: str,
        book_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Convert a Librarian project to a Book for in-memory manipulation.

        This "checks out" a Librarian project, creating a Book instance that can
        be manipulated using Book tools (add nodes, apply formulas, etc.).

        Args:
            project_id: Librarian project ID (UUID) to checkout
            book_name: Optional name for the Book (defaults to project name)

        Returns:
            Result with book info, hierarchy count, and any errors

        Example:
            checkout_librarian_to_book(project_id="abc-123")
        """
        ctx = get_context()
        config = ctx.librarian_config

        bridge = LibrarianBridge(
            base_url=config["base_url"],
            api_key=config["api_key"],
        )

        book, result = bridge.checkout_librarian_to_book(project_id, book_name)

        if book:
            # Register the book in context
            ctx.register_book(book, source_project_id=project_id)
            ctx.active_project_id = project_id

            ctx.record_operation(
                operation="checkout",
                source_system="librarian",
                target_system="book",
                details={
                    "project_id": project_id,
                    "book_name": book.name,
                    "hierarchy_count": result["hierarchy_count"],
                },
            )

            return {
                "success": True,
                "book_name": book.name,
                "hierarchy_count": result["hierarchy_count"],
                "root_node_count": len(book.root_nodes),
                "message": f"Successfully checked out project '{project_id}' to Book '{book.name}'",
            }
        else:
            ctx.record_operation(
                operation="checkout",
                source_system="librarian",
                target_system="book",
                details={"project_id": project_id},
                success=False,
                error_message="; ".join(result.get("errors", [])),
            )

            return {
                "success": False,
                "errors": result.get("errors", ["Unknown error"]),
            }

    # =========================================================================
    # Tool 2: Promote Book to Librarian
    # =========================================================================

    @mcp.tool()
    def promote_book_to_librarian(
        book_name: str,
        project_name: Optional[str] = None,
        project_description: str = "",
        existing_project_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create or update a Librarian project from a Book.

        This "promotes" a Book to Librarian, making it available in the web UI
        and for deployment to databases.

        Args:
            book_name: Name of the registered Book to promote
            project_name: Name for the Librarian project (defaults to book name)
            project_description: Description for the project
            existing_project_id: If provided, updates existing project instead of creating new

        Returns:
            Result with project_id, created/updated counts, and any errors

        Example:
            promote_book_to_librarian(book_name="My P&L", project_name="P&L Hierarchy")
        """
        ctx = get_context()

        # Get the book
        book = ctx.get_book(book_name)
        if not book:
            return {
                "success": False,
                "error": f"Book '{book_name}' not found in context. Use list_books() to see available books.",
            }

        config = ctx.librarian_config
        bridge = LibrarianBridge(
            base_url=config["base_url"],
            api_key=config["api_key"],
        )

        result = bridge.promote_book_to_librarian(
            book=book,
            project_name=project_name or book.name,
            project_description=project_description,
            existing_project_id=existing_project_id,
        )

        ctx.record_operation(
            operation="promote",
            source_system="book",
            target_system="librarian",
            details={
                "book_name": book_name,
                "project_id": result.get("project_id"),
                "created": result.get("created_hierarchies", 0),
                "updated": result.get("updated_hierarchies", 0),
            },
            success=result.get("success", False),
            error_message="; ".join(result.get("errors", [])) if result.get("errors") else None,
        )

        if result.get("success"):
            # Update context with new project ID
            ctx.active_project_id = result.get("project_id")

            return {
                "success": True,
                "project_id": result.get("project_id"),
                "created_hierarchies": result.get("created_hierarchies", 0),
                "updated_hierarchies": result.get("updated_hierarchies", 0),
                "message": f"Successfully promoted Book '{book_name}' to Librarian project",
            }
        else:
            return {
                "success": False,
                "errors": result.get("errors", ["Unknown error"]),
                "project_id": result.get("project_id"),
            }

    # =========================================================================
    # Tool 3: Sync Book and Librarian
    # =========================================================================

    @mcp.tool()
    def sync_book_and_librarian(
        book_name: str,
        project_id: str,
        direction: str = "bidirectional",
        conflict_resolution: str = "book_wins",
    ) -> Dict[str, Any]:
        """
        Synchronize a Book with a Librarian project.

        This performs a sync operation, detecting differences and applying
        changes based on the specified direction and conflict resolution.

        Args:
            book_name: Name of the registered Book
            project_id: Librarian project ID to sync with
            direction: Sync direction - "to_librarian", "from_librarian", or "bidirectional"
            conflict_resolution: How to resolve conflicts - "book_wins" or "librarian_wins"

        Returns:
            Sync result with pushed/pulled counts and any errors

        Example:
            sync_book_and_librarian(
                book_name="My P&L",
                project_id="abc-123",
                direction="bidirectional",
                conflict_resolution="book_wins"
            )
        """
        ctx = get_context()

        # Get the book
        book = ctx.get_book(book_name)
        if not book:
            return {
                "success": False,
                "error": f"Book '{book_name}' not found in context",
            }

        config = ctx.librarian_config
        bridge = LibrarianBridge(
            base_url=config["base_url"],
            api_key=config["api_key"],
        )

        result = bridge.sync_book_and_librarian(
            book=book,
            project_id=project_id,
            direction=direction,
            conflict_resolution=conflict_resolution,
        )

        ctx.record_operation(
            operation="sync",
            source_system="book",
            target_system="librarian",
            details={
                "book_name": book_name,
                "project_id": project_id,
                "direction": direction,
                "pushed": result.get("pushed", 0),
                "pulled": result.get("pulled", 0),
            },
            success=result.get("success", False),
        )

        return result

    # =========================================================================
    # Tool 4: Diff Book and Librarian
    # =========================================================================

    @mcp.tool()
    def diff_book_and_librarian(
        book_name: str,
        project_id: str,
    ) -> Dict[str, Any]:
        """
        Show differences between a Book and a Librarian project.

        This compares the current state of a Book with a Librarian project
        and shows what would change if synced.

        Args:
            book_name: Name of the registered Book
            project_id: Librarian project ID to compare with

        Returns:
            Diff result showing book-only, librarian-only, modified, and identical items

        Example:
            diff_book_and_librarian(book_name="My P&L", project_id="abc-123")
        """
        ctx = get_context()

        # Get the book
        book = ctx.get_book(book_name)
        if not book:
            return {
                "success": False,
                "error": f"Book '{book_name}' not found in context",
            }

        config = ctx.librarian_config
        bridge = LibrarianBridge(
            base_url=config["base_url"],
            api_key=config["api_key"],
        )

        diff = bridge.diff_book_project(book, project_id)

        return {
            "success": True,
            "book_name": book_name,
            "project_id": project_id,
            **diff.to_dict(),
        }

    # =========================================================================
    # Tool 5: Analyze Book with Researcher
    # =========================================================================

    @mcp.tool()
    def analyze_book_with_researcher(
        book_name: str,
        connection_id: str,
        analysis_type: str = "validate_sources",
    ) -> Dict[str, Any]:
        """
        Analyze a Book using Researcher capabilities.

        This validates source mappings and/or profiles source data using
        the Researcher's database analytics.

        Args:
            book_name: Name of the registered Book
            connection_id: Database connection ID for validation
            analysis_type: Type of analysis:
                - "validate_sources": Validate all source mappings exist
                - "profile_sources": Profile all source columns (counts, samples)
                - "full": Both validation and profiling

        Returns:
            Analysis results including validation errors and profile data

        Example:
            analyze_book_with_researcher(
                book_name="My P&L",
                connection_id="snowflake-prod",
                analysis_type="validate_sources"
            )
        """
        ctx = get_context()

        # Get the book
        book = ctx.get_book(book_name)
        if not book:
            return {
                "success": False,
                "error": f"Book '{book_name}' not found in context",
            }

        config = ctx.researcher_config
        bridge = ResearcherBridge(
            base_url=config["base_url"],
            api_key=config["api_key"],
        )

        result = bridge.analyze_book(
            book=book,
            connection_id=connection_id,
            analysis_type=analysis_type,
        )

        ctx.record_operation(
            operation="analyze",
            source_system="book",
            target_system="researcher",
            details={
                "book_name": book_name,
                "connection_id": connection_id,
                "analysis_type": analysis_type,
            },
        )

        return {
            "success": True,
            **result,
        }

    # =========================================================================
    # Tool 6: Compare Book to Database
    # =========================================================================

    @mcp.tool()
    def compare_book_to_database(
        book_name: str,
        connection_id: str,
        hierarchy_filter: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Compare Book hierarchy values with live database data.

        This extracts values from the Book's source mappings and compares
        them against the actual database values to find orphans and mismatches.

        Args:
            book_name: Name of the registered Book
            connection_id: Database connection ID
            hierarchy_filter: Optional hierarchy ID to compare (all if not specified)

        Returns:
            Comparison results showing matches, orphans, and statistics

        Example:
            compare_book_to_database(
                book_name="My P&L",
                connection_id="snowflake-prod"
            )
        """
        ctx = get_context()

        # Get the book
        book = ctx.get_book(book_name)
        if not book:
            return {
                "success": False,
                "error": f"Book '{book_name}' not found in context",
            }

        config = ctx.researcher_config
        bridge = ResearcherBridge(
            base_url=config["base_url"],
            api_key=config["api_key"],
        )

        # Extract sources from book
        sources = bridge.extract_sources_from_book(book)

        # Filter if specified
        if hierarchy_filter:
            sources = [s for s in sources if s.hierarchy_id == hierarchy_filter]

        if not sources:
            return {
                "success": True,
                "message": "No source mappings found to compare",
                "comparisons": [],
            }

        # Group by hierarchy and compare
        comparisons = []
        from collections import defaultdict
        hier_sources = defaultdict(list)
        for s in sources:
            hier_sources[s.hierarchy_id].append(s)

        for hier_id, hier_sources_list in hier_sources.items():
            for source in hier_sources_list:
                # Get hierarchy values (UIDs) from the source mapping
                # For now, use a simple comparison
                comparison = bridge.compare_hierarchy_data(
                    hierarchy_id=hier_id,
                    connection_id=connection_id,
                    hierarchy_values=[source.uid] if source.uid else [],
                    source_mapping=source,
                )
                comparisons.append(comparison.to_dict())

        ctx.record_operation(
            operation="compare_data",
            source_system="book",
            target_system="researcher",
            details={
                "book_name": book_name,
                "connection_id": connection_id,
                "comparisons": len(comparisons),
            },
        )

        return {
            "success": True,
            "book_name": book_name,
            "comparisons": comparisons,
            "summary": {
                "total_comparisons": len(comparisons),
                "total_matches": sum(c.get("matches", 0) for c in comparisons),
                "total_orphans_in_hierarchy": sum(c.get("orphans_in_hierarchy", 0) for c in comparisons),
                "total_orphans_in_database": sum(c.get("orphans_in_database", 0) for c in comparisons),
            },
        }

    # =========================================================================
    # Tool 7: Profile Book Sources
    # =========================================================================

    @mcp.tool()
    def profile_book_sources(
        book_name: str,
        connection_id: str,
    ) -> Dict[str, Any]:
        """
        Profile data for all source mappings in a Book.

        This analyzes each source column to get row counts, distinct values,
        null counts, and sample values.

        Args:
            book_name: Name of the registered Book
            connection_id: Database connection ID

        Returns:
            Profile data for each source mapping

        Example:
            profile_book_sources(book_name="My P&L", connection_id="snowflake-prod")
        """
        ctx = get_context()

        # Get the book
        book = ctx.get_book(book_name)
        if not book:
            return {
                "success": False,
                "error": f"Book '{book_name}' not found in context",
            }

        config = ctx.researcher_config
        bridge = ResearcherBridge(
            base_url=config["base_url"],
            api_key=config["api_key"],
        )

        sources = bridge.extract_sources_from_book(book)
        result = bridge.profile_sources(sources, connection_id)

        ctx.record_operation(
            operation="profile_sources",
            source_system="book",
            target_system="researcher",
            details={
                "book_name": book_name,
                "connection_id": connection_id,
                "source_count": len(sources),
            },
        )

        return {
            "success": True,
            "book_name": book_name,
            **result,
        }

    # =========================================================================
    # Tool 8: Create Unified Workflow
    # =========================================================================

    @mcp.tool()
    def create_unified_workflow(
        workflow_name: str,
        description: str,
        steps: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Create a workflow plan spanning Book, Librarian, and Researcher.

        This creates a workflow definition that can be executed to perform
        complex multi-system operations.

        Args:
            workflow_name: Name for the workflow
            description: Description of what the workflow does
            steps: List of workflow steps, each with:
                - system: "book", "librarian", or "researcher"
                - action: Action to perform
                - params: Parameters for the action

        Returns:
            Created workflow definition

        Example:
            create_unified_workflow(
                workflow_name="Import and Deploy",
                description="Import CSV to Book, clean up, deploy to Snowflake",
                steps=[
                    {"system": "book", "action": "create_from_csv", "params": {"path": "data.csv"}},
                    {"system": "book", "action": "apply_formula", "params": {"formula": "SUM"}},
                    {"system": "researcher", "action": "validate_sources", "params": {}},
                    {"system": "librarian", "action": "promote", "params": {}},
                    {"system": "librarian", "action": "push_to_snowflake", "params": {}}
                ]
            )
        """
        ctx = get_context()

        # Validate steps
        valid_systems = {"book", "librarian", "researcher"}
        for i, step in enumerate(steps):
            if step.get("system") not in valid_systems:
                return {
                    "success": False,
                    "error": f"Step {i}: Invalid system '{step.get('system')}'. Must be one of: {valid_systems}",
                }
            if not step.get("action"):
                return {
                    "success": False,
                    "error": f"Step {i}: Missing 'action' field",
                }

        # Create workflow definition
        workflow = {
            "name": workflow_name,
            "description": description,
            "steps": steps,
            "created_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
            "status": "created",
        }

        # Store in context (could also persist to file)
        if not hasattr(ctx, '_workflows'):
            ctx._workflows = {}
        ctx._workflows[workflow_name] = workflow

        ctx.record_operation(
            operation="create_workflow",
            source_system="unified",
            target_system=None,
            details={
                "workflow_name": workflow_name,
                "step_count": len(steps),
            },
        )

        return {
            "success": True,
            "workflow": workflow,
            "message": f"Created workflow '{workflow_name}' with {len(steps)} steps",
        }

    # =========================================================================
    # Tool 9: Execute Unified Workflow
    # =========================================================================

    @mcp.tool()
    def execute_unified_workflow(
        workflow_name: str,
        context_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Execute a previously created unified workflow.

        This runs through each step of the workflow, passing results
        between steps as needed.

        Args:
            workflow_name: Name of the workflow to execute
            context_params: Optional parameters to pass to the workflow

        Returns:
            Execution results for each step

        Example:
            execute_unified_workflow(
                workflow_name="Import and Deploy",
                context_params={"book_name": "My P&L", "connection_id": "snowflake-prod"}
            )
        """
        ctx = get_context()

        # Get workflow
        if not hasattr(ctx, '_workflows') or workflow_name not in ctx._workflows:
            return {
                "success": False,
                "error": f"Workflow '{workflow_name}' not found. Create it first with create_unified_workflow.",
            }

        workflow = ctx._workflows[workflow_name]
        results = []
        context = context_params or {}

        for i, step in enumerate(workflow["steps"]):
            step_result = {
                "step": i,
                "system": step["system"],
                "action": step["action"],
            }

            try:
                # Execute based on system
                if step["system"] == "book":
                    # Book operations would require Book tools
                    step_result["result"] = {"message": "Book action would execute here"}
                    step_result["status"] = "simulated"

                elif step["system"] == "librarian":
                    # Map to appropriate Librarian tool
                    if step["action"] == "promote":
                        if "book_name" in context:
                            result = promote_book_to_librarian(
                                book_name=context["book_name"],
                                **step.get("params", {}),
                            )
                            step_result["result"] = result
                            step_result["status"] = "completed" if result.get("success") else "failed"
                        else:
                            step_result["result"] = {"error": "book_name not in context"}
                            step_result["status"] = "failed"
                    else:
                        step_result["result"] = {"message": "Librarian action would execute here"}
                        step_result["status"] = "simulated"

                elif step["system"] == "researcher":
                    # Map to appropriate Researcher tool
                    if step["action"] == "validate_sources":
                        if "book_name" in context and "connection_id" in context:
                            result = analyze_book_with_researcher(
                                book_name=context["book_name"],
                                connection_id=context["connection_id"],
                                analysis_type="validate_sources",
                            )
                            step_result["result"] = result
                            step_result["status"] = "completed" if result.get("success") else "failed"
                        else:
                            step_result["result"] = {"error": "book_name or connection_id not in context"}
                            step_result["status"] = "failed"
                    else:
                        step_result["result"] = {"message": "Researcher action would execute here"}
                        step_result["status"] = "simulated"

            except Exception as e:
                step_result["status"] = "error"
                step_result["error"] = str(e)

            results.append(step_result)

            # Stop on failure unless continue_on_error is set
            if step_result.get("status") == "failed" and not step.get("continue_on_error"):
                break

        ctx.record_operation(
            operation="execute_workflow",
            source_system="unified",
            target_system=None,
            details={
                "workflow_name": workflow_name,
                "steps_executed": len(results),
                "success_count": sum(1 for r in results if r.get("status") == "completed"),
            },
        )

        return {
            "success": all(r.get("status") in ("completed", "simulated") for r in results),
            "workflow_name": workflow_name,
            "results": results,
            "summary": {
                "total_steps": len(workflow["steps"]),
                "executed": len(results),
                "completed": sum(1 for r in results if r.get("status") == "completed"),
                "simulated": sum(1 for r in results if r.get("status") == "simulated"),
                "failed": sum(1 for r in results if r.get("status") == "failed"),
            },
        }

    # =========================================================================
    # Tool 10: Get Unified Context
    # =========================================================================

    @mcp.tool()
    def get_unified_context() -> Dict[str, Any]:
        """
        Get the current unified context across all systems.

        This shows all registered Books, active Librarian project,
        database connections, and recent operations.

        Returns:
            Current context state across Book, Librarian, and Researcher

        Example:
            get_unified_context()
        """
        ctx = get_context()

        summary = ctx.get_context_summary()

        # Add workflow info if available
        if hasattr(ctx, '_workflows'):
            summary["workflows"] = list(ctx._workflows.keys())

        return {
            "success": True,
            **summary,
        }

    logger.info("Registered 10 Unified Agent MCP tools")
