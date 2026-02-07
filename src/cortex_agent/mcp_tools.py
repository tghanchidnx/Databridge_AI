"""
MCP Tools for CortexAgent.

Provides 12 MCP tools for Snowflake Cortex AI integration:

Core Cortex Functions (5):
- cortex_complete: Text generation via COMPLETE()
- cortex_summarize: Text summarization via SUMMARIZE()
- cortex_sentiment: Sentiment analysis via SENTIMENT()
- cortex_translate: Translation via TRANSLATE()
- cortex_extract_answer: QA extraction via EXTRACT_ANSWER()

Reasoning Loop (3):
- cortex_reason: Run full reasoning loop for complex goal
- cortex_analyze_data: AI-powered data analysis on table
- cortex_clean_data: Data cleaning with proposed changes

Configuration (2):
- configure_cortex_agent: Set connection and model config
- get_cortex_agent_status: Get agent status and connection

Console (2):
- get_cortex_console_log: Get console log entries
- get_cortex_conversation: Get full conversation with thinking
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from .console import CommunicationConsole, CLIOutput, FileOutput
from .context import CortexAgentContext, get_context
from .cortex_client import CortexClient
from .reasoning_loop import CortexReasoningLoop
from .types import CortexAgentConfig, MessageType

logger = logging.getLogger(__name__)

# Module-level state
_cortex_client: Optional[CortexClient] = None
_console: Optional[CommunicationConsole] = None
_reasoning_loop: Optional[CortexReasoningLoop] = None


def _get_query_func(settings):
    """Get the query function from connections API."""
    # Import here to avoid circular imports
    try:
        try:
            from src.connections_api import get_client
        except ImportError:
            from connections_api import get_client

        client = get_client(settings)

        def query_func(connection_id: str, query: str) -> List[Dict]:
            return client.execute_query(connection_id, query)

        return query_func
    except Exception as e:
        logger.warning(f"Failed to get connections API client: {e}")

        # Return a mock for testing
        def mock_query(connection_id: str, query: str) -> List[Dict]:
            logger.info(f"Mock query on {connection_id}: {query[:100]}...")
            return [{"RESULT": '{"message": "Mock response - configure real connection"}'}]

        return mock_query


def _ensure_initialized(settings) -> bool:
    """Ensure the agent is initialized."""
    global _cortex_client, _console, _reasoning_loop

    context = get_context()
    config = context.get_config()

    if not config:
        return False

    if _cortex_client is None:
        query_func = _get_query_func(settings)
        _cortex_client = CortexClient(
            connection_id=config.connection_id,
            query_func=query_func,
            default_model=config.cortex_model,
            temperature=config.temperature,
        )

    if _console is None:
        outputs = []
        if "cli" in config.console_outputs:
            outputs.append(CLIOutput(verbose=True))
        if "file" in config.console_outputs:
            log_path = Path("data/cortex_agent/console.jsonl")
            outputs.append(FileOutput(log_path))
        _console = CommunicationConsole(outputs=outputs if outputs else None)

    if _reasoning_loop is None:
        _reasoning_loop = CortexReasoningLoop(
            cortex=_cortex_client,
            console=_console,
            context=context,
            config=config,
        )

    return True


def register_cortex_agent_tools(mcp, settings):
    """Register all Cortex Agent MCP tools."""

    # =========================================================================
    # Configuration Tools (2)
    # =========================================================================

    @mcp.tool()
    def configure_cortex_agent(
        connection_id: str,
        cortex_model: str = "mistral-large",
        max_reasoning_steps: int = 10,
        temperature: float = 0.3,
        enable_console: bool = True,
        console_outputs: str = "cli,file",
    ) -> Dict[str, Any]:
        """
        Configure the Cortex Agent with a Snowflake connection.

        This must be called before using other Cortex tools. The connection_id
        should reference an existing Snowflake connection from list_backend_connections.

        Args:
            connection_id: ID of Snowflake connection (from list_backend_connections)
            cortex_model: Default model for COMPLETE (mistral-large, llama3-70b, etc.)
            max_reasoning_steps: Maximum steps in reasoning loop (1-50)
            temperature: Sampling temperature for COMPLETE (0.0-1.0)
            enable_console: Enable communication console
            console_outputs: Comma-separated outputs (cli, file, database)

        Returns:
            Configuration status

        Example:
            configure_cortex_agent(
                connection_id="snowflake-prod",
                cortex_model="mistral-large"
            )
        """
        global _cortex_client, _console, _reasoning_loop

        # Reset existing state
        _cortex_client = None
        _console = None
        _reasoning_loop = None

        # Parse console outputs
        outputs = [o.strip() for o in console_outputs.split(",") if o.strip()]

        config = CortexAgentConfig(
            connection_id=connection_id,
            cortex_model=cortex_model,
            max_reasoning_steps=max_reasoning_steps,
            temperature=temperature,
            enable_console=enable_console,
            console_outputs=outputs,
        )

        context = get_context()
        context.configure(config)

        # Initialize components
        if _ensure_initialized(settings):
            # Test connection
            test_result = _cortex_client.test_connection()

            return {
                "status": "configured",
                "connection_id": connection_id,
                "cortex_model": cortex_model,
                "max_reasoning_steps": max_reasoning_steps,
                "temperature": temperature,
                "console_outputs": outputs,
                "connection_test": test_result,
            }
        else:
            return {
                "status": "configured",
                "connection_id": connection_id,
                "warning": "Components not fully initialized",
            }

    @mcp.tool()
    def get_cortex_agent_status() -> Dict[str, Any]:
        """
        Get the current status of the Cortex Agent.

        Returns connection status, configuration, and statistics.

        Returns:
            Agent status including:
            - is_configured: Whether agent is configured
            - config: Current configuration
            - context_stats: Conversation statistics
            - console_status: Console output status
        """
        context = get_context()
        config = context.get_config()

        status = {
            "is_configured": context.is_configured(),
            "config": config.model_dump() if config else None,
            "context_stats": context.get_stats(),
        }

        if _console:
            status["console_status"] = _console.get_status()

        if _cortex_client:
            status["cortex_calls_made"] = _cortex_client.get_call_count()

        return status

    # =========================================================================
    # Core Cortex Functions (5)
    # =========================================================================

    @mcp.tool()
    def cortex_complete(
        prompt: str,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Generate text using Snowflake Cortex COMPLETE() function.

        Uses an LLM to generate text based on the prompt. All processing
        happens within Snowflake - data never leaves the cloud.

        Args:
            prompt: The text prompt for generation
            model: Model to use (default from config: mistral-large)
            temperature: Sampling temperature (0.0-1.0)
            max_tokens: Maximum tokens to generate

        Returns:
            Generated text and query details

        Example:
            cortex_complete(
                prompt="Explain data reconciliation in one sentence",
                model="mistral-large"
            )
        """
        if not _ensure_initialized(settings):
            return {"error": "Cortex Agent not configured. Call configure_cortex_agent first."}

        result = _cortex_client.complete(
            prompt=prompt,
            model=model,
            temperature=temperature,
            max_tokens=max_tokens,
        )

        return {
            "success": result.success,
            "result": result.result,
            "model": model or _cortex_client.default_model,
            "duration_ms": result.duration_ms,
            "error": result.error,
        }

    @mcp.tool()
    def cortex_summarize(text: str) -> Dict[str, Any]:
        """
        Summarize text using Snowflake Cortex SUMMARIZE() function.

        Generates a concise summary of the input text.

        Args:
            text: Text to summarize

        Returns:
            Summary and query details

        Example:
            cortex_summarize(
                text="Long document text here..."
            )
        """
        if not _ensure_initialized(settings):
            return {"error": "Cortex Agent not configured. Call configure_cortex_agent first."}

        result = _cortex_client.summarize(text)

        return {
            "success": result.success,
            "summary": result.result,
            "duration_ms": result.duration_ms,
            "error": result.error,
        }

    @mcp.tool()
    def cortex_sentiment(text: str) -> Dict[str, Any]:
        """
        Analyze sentiment using Snowflake Cortex SENTIMENT() function.

        Returns a sentiment score from -1 (negative) to 1 (positive).

        Args:
            text: Text to analyze

        Returns:
            Sentiment score and interpretation

        Example:
            cortex_sentiment(text="This product is amazing!")
            # Returns: {"sentiment": 0.85, "interpretation": "positive"}
        """
        if not _ensure_initialized(settings):
            return {"error": "Cortex Agent not configured. Call configure_cortex_agent first."}

        result = _cortex_client.sentiment(text)

        sentiment_value = result.result if result.success else None

        # Interpret sentiment
        interpretation = "neutral"
        if sentiment_value is not None:
            try:
                score = float(sentiment_value)
                if score > 0.3:
                    interpretation = "positive"
                elif score < -0.3:
                    interpretation = "negative"
            except (ValueError, TypeError):
                pass

        return {
            "success": result.success,
            "sentiment": sentiment_value,
            "interpretation": interpretation,
            "duration_ms": result.duration_ms,
            "error": result.error,
        }

    @mcp.tool()
    def cortex_translate(
        text: str,
        from_lang: str,
        to_lang: str,
    ) -> Dict[str, Any]:
        """
        Translate text using Snowflake Cortex TRANSLATE() function.

        Translates text between languages.

        Args:
            text: Text to translate
            from_lang: Source language code (en, es, fr, de, etc.)
            to_lang: Target language code

        Returns:
            Translated text

        Example:
            cortex_translate(
                text="Hello, world!",
                from_lang="en",
                to_lang="es"
            )
            # Returns: {"translation": "¡Hola, mundo!"}
        """
        if not _ensure_initialized(settings):
            return {"error": "Cortex Agent not configured. Call configure_cortex_agent first."}

        result = _cortex_client.translate(text, from_lang, to_lang)

        return {
            "success": result.success,
            "translation": result.result,
            "from_lang": from_lang,
            "to_lang": to_lang,
            "duration_ms": result.duration_ms,
            "error": result.error,
        }

    @mcp.tool()
    def cortex_extract_answer(
        context: str,
        question: str,
    ) -> Dict[str, Any]:
        """
        Extract answer from context using Snowflake Cortex EXTRACT_ANSWER().

        Finds and extracts the answer to a question from provided context.

        Args:
            context: Text context to search
            question: Question to answer

        Returns:
            Extracted answer

        Example:
            cortex_extract_answer(
                context="The company was founded in 2010 by John Smith.",
                question="When was the company founded?"
            )
            # Returns: {"answer": "2010"}
        """
        if not _ensure_initialized(settings):
            return {"error": "Cortex Agent not configured. Call configure_cortex_agent first."}

        result = _cortex_client.extract_answer(context, question)

        return {
            "success": result.success,
            "answer": result.result,
            "question": question,
            "duration_ms": result.duration_ms,
            "error": result.error,
        }

    # =========================================================================
    # Reasoning Loop Tools (3)
    # =========================================================================

    @mcp.tool()
    def cortex_reason(
        goal: str,
        context: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Run the full reasoning loop for a complex goal.

        Uses the Observe → Plan → Execute → Reflect pattern to break down
        complex tasks into steps and execute them via Cortex functions.

        Args:
            goal: The goal to achieve (natural language)
            context: Optional JSON context (e.g., table names, constraints)

        Returns:
            Complete response with thinking steps

        Example:
            cortex_reason(
                goal="Analyze the data quality in PRODUCTS table and suggest improvements",
                context='{"table": "ANALYTICS.PUBLIC.PRODUCTS"}'
            )
        """
        if not _ensure_initialized(settings):
            return {"error": "Cortex Agent not configured. Call configure_cortex_agent first."}

        # Parse context
        context_dict = None
        if context:
            try:
                context_dict = json.loads(context)
            except json.JSONDecodeError:
                context_dict = {"raw_context": context}

        # Run reasoning loop
        response = _reasoning_loop.run_sync(goal, context_dict)

        return {
            "success": response.success,
            "conversation_id": response.conversation_id,
            "goal": response.goal,
            "result": response.result,
            "thinking_steps": [s.to_dict() for s in response.thinking_steps],
            "total_cortex_calls": response.total_cortex_calls,
            "total_duration_ms": response.total_duration_ms,
            "plan": response.plan.to_dict() if response.plan else None,
            "error": response.error,
        }

    @mcp.tool()
    def cortex_analyze_data(
        table_name: str,
        analysis_type: str = "quality",
        sample_size: int = 100,
        focus_columns: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        AI-powered data analysis on a Snowflake table.

        Uses Cortex to analyze data quality, patterns, or statistics.

        Args:
            table_name: Fully qualified table name (DATABASE.SCHEMA.TABLE)
            analysis_type: Type of analysis (quality, patterns, statistics, anomalies)
            sample_size: Number of rows to sample for analysis
            focus_columns: Comma-separated column names to focus on (optional)

        Returns:
            Analysis results with AI insights

        Example:
            cortex_analyze_data(
                table_name="ANALYTICS.PUBLIC.CUSTOMERS",
                analysis_type="quality",
                sample_size=100
            )
        """
        if not _ensure_initialized(settings):
            return {"error": "Cortex Agent not configured. Call configure_cortex_agent first."}

        # Build context
        context = {
            "table": table_name,
            "analysis_type": analysis_type,
            "sample_size": sample_size,
        }
        if focus_columns:
            context["focus_columns"] = [c.strip() for c in focus_columns.split(",")]

        goal = f"Analyze {analysis_type} of data in {table_name}"
        if focus_columns:
            goal += f", focusing on columns: {focus_columns}"

        response = _reasoning_loop.run_sync(goal, context)

        return {
            "success": response.success,
            "conversation_id": response.conversation_id,
            "table": table_name,
            "analysis_type": analysis_type,
            "insights": response.result,
            "thinking_steps": len(response.thinking_steps),
            "duration_ms": response.total_duration_ms,
            "error": response.error,
        }

    @mcp.tool()
    def cortex_clean_data(
        table_name: str,
        column_name: str,
        cleaning_goal: str,
        preview_only: bool = True,
        limit: int = 10,
    ) -> Dict[str, Any]:
        """
        AI-powered data cleaning with proposed changes.

        Uses Cortex to analyze and propose data cleaning transformations.

        Args:
            table_name: Fully qualified table name
            column_name: Column to clean
            cleaning_goal: What to clean (e.g., "standardize product names")
            preview_only: If True, only preview changes without applying
            limit: Number of rows to preview

        Returns:
            Proposed cleaning transformations

        Example:
            cortex_clean_data(
                table_name="ANALYTICS.PUBLIC.PRODUCTS",
                column_name="PRODUCT_NAME",
                cleaning_goal="Standardize product names and fix typos",
                preview_only=True
            )
        """
        if not _ensure_initialized(settings):
            return {"error": "Cortex Agent not configured. Call configure_cortex_agent first."}

        context = {
            "table": table_name,
            "column": column_name,
            "cleaning_goal": cleaning_goal,
            "preview_only": preview_only,
            "limit": limit,
        }

        goal = f"Clean {column_name} in {table_name}: {cleaning_goal}"
        if preview_only:
            goal += " (preview only, do not apply changes)"

        response = _reasoning_loop.run_sync(goal, context)

        return {
            "success": response.success,
            "conversation_id": response.conversation_id,
            "table": table_name,
            "column": column_name,
            "cleaning_goal": cleaning_goal,
            "preview_only": preview_only,
            "proposed_changes": response.result,
            "thinking_steps": len(response.thinking_steps),
            "duration_ms": response.total_duration_ms,
            "error": response.error,
        }

    # =========================================================================
    # Console Tools (2)
    # =========================================================================

    @mcp.tool()
    def get_cortex_console_log(
        limit: int = 50,
        conversation_id: Optional[str] = None,
        message_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Get recent console log entries.

        The console captures all agent communication for observability.

        Args:
            limit: Maximum entries to return
            conversation_id: Filter by conversation ID
            message_type: Filter by type (request, response, thinking, plan, error)

        Returns:
            List of console log entries

        Example:
            get_cortex_console_log(limit=20, message_type="thinking")
        """
        if not _console:
            return {
                "entries": [],
                "message": "Console not initialized. Configure agent first.",
            }

        # Parse message type
        msg_type = None
        if message_type:
            try:
                msg_type = MessageType(message_type.lower())
            except ValueError:
                pass

        messages = _console.get_recent(limit=limit, message_type=msg_type)

        if conversation_id:
            messages = [m for m in messages if m.conversation_id == conversation_id]

        return {
            "entries": [m.to_dict() for m in messages],
            "count": len(messages),
            "console_status": _console.get_status(),
        }

    @mcp.tool()
    def get_cortex_conversation(conversation_id: str) -> Dict[str, Any]:
        """
        Get full conversation with all thinking steps.

        Retrieves the complete conversation history including observations,
        plans, executions, and reflections.

        Args:
            conversation_id: The conversation ID

        Returns:
            Full conversation with thinking steps

        Example:
            get_cortex_conversation(conversation_id="abc-123-...")
        """
        context = get_context()
        conversation = context.get_conversation(conversation_id)

        if not conversation:
            return {
                "error": f"Conversation not found: {conversation_id}",
                "available_conversations": context.list_conversations(limit=5),
            }

        return {
            "conversation": conversation.to_dict(),
            "scratchpad": context.get_scratchpad_context(conversation_id),
        }

    logger.info("Registered 12 Cortex Agent MCP tools")
    return {
        "tools_registered": 12,
        "categories": {
            "configuration": ["configure_cortex_agent", "get_cortex_agent_status"],
            "cortex_functions": ["cortex_complete", "cortex_summarize", "cortex_sentiment", "cortex_translate", "cortex_extract_answer"],
            "reasoning": ["cortex_reason", "cortex_analyze_data", "cortex_clean_data"],
            "console": ["get_cortex_console_log", "get_cortex_conversation"],
        },
    }
