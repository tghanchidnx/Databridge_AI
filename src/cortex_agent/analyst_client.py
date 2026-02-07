"""
Cortex Analyst REST API Client.

This client communicates with the Cortex Analyst API to translate
natural language questions into SQL using semantic models.

API Endpoint: POST /api/v2/cortex/analyst/message
"""

import json
import logging
import time
import uuid
from typing import Any, Callable, Dict, List, Optional

import requests

from .analyst_types import (
    AnalystConversation,
    AnalystMessage,
    AnalystQueryResult,
    AnalystResponse,
    MessageContent,
    MessageContentType,
    QueryResult,
)

logger = logging.getLogger(__name__)


class AnalystClient:
    """REST API client for Snowflake Cortex Analyst."""

    ENDPOINT = "/api/v2/cortex/analyst/message"

    def __init__(
        self,
        account: str,
        token_func: Callable[[], str],
        timeout: int = 60,
    ):
        """
        Initialize the Analyst client.

        Args:
            account: Snowflake account identifier (e.g., "xy12345.us-east-1")
            token_func: Function that returns a valid auth token
            timeout: Request timeout in seconds
        """
        self.account = account
        self.token_func = token_func
        self.timeout = timeout
        self.base_url = f"https://{account}.snowflakecomputing.com"
        self._conversations: Dict[str, AnalystConversation] = {}

    def ask(
        self,
        question: str,
        semantic_model_file: Optional[str] = None,
        semantic_view: Optional[str] = None,
        conversation_id: Optional[str] = None,
        stream: bool = False,
    ) -> AnalystResponse:
        """
        Send a natural language question to Cortex Analyst.

        Args:
            question: Natural language question
            semantic_model_file: Stage path to YAML (e.g., @db.schema.stage/model.yaml)
            semantic_view: Fully qualified semantic view name (alternative to file)
            conversation_id: ID of existing conversation for multi-turn
            stream: Enable streaming response (not yet implemented)

        Returns:
            AnalystResponse with SQL and explanation

        Raises:
            ValueError: If neither semantic_model_file nor semantic_view is provided
        """
        if not semantic_model_file and not semantic_view:
            raise ValueError("Either semantic_model_file or semantic_view must be provided")

        # Get or create conversation
        conversation = self._get_or_create_conversation(
            conversation_id, semantic_model_file or semantic_view
        )

        # Build request
        request_body = self._build_request(
            question=question,
            model_file=semantic_model_file,
            view=semantic_view,
            conversation=conversation,
        )

        # Execute request
        try:
            token = self.token_func()
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }

            url = f"{self.base_url}{self.ENDPOINT}"
            logger.debug(f"Sending request to {url}")

            response = requests.post(
                url,
                headers=headers,
                json=request_body,
                timeout=self.timeout,
            )

            response.raise_for_status()
            result = response.json()

            analyst_response = self._parse_response(result, conversation.id)

            # Update conversation history
            conversation.add_user_message(question)
            conversation.add_analyst_response(analyst_response)

            return analyst_response

        except requests.exceptions.RequestException as e:
            logger.error(f"Analyst API request failed: {e}")
            return AnalystResponse(
                request_id=str(uuid.uuid4()),
                message=AnalystMessage(role="analyst", content=[]),
                success=False,
                error=str(e),
            )

    def ask_with_execution(
        self,
        question: str,
        semantic_model_file: str,
        query_func: Callable[[str], List[Dict[str, Any]]],
        conversation_id: Optional[str] = None,
        limit: int = 100,
    ) -> AnalystQueryResult:
        """
        Ask a question and execute the resulting SQL.

        Args:
            question: Natural language question
            semantic_model_file: Stage path to semantic model YAML
            query_func: Function to execute SQL (connection_id, query) -> results
            conversation_id: Optional conversation ID for multi-turn
            limit: Maximum rows to return

        Returns:
            AnalystQueryResult with SQL, explanation, and query results
        """
        # Get SQL from Analyst
        analyst_response = self.ask(
            question=question,
            semantic_model_file=semantic_model_file,
            conversation_id=conversation_id,
        )

        if not analyst_response.success or not analyst_response.sql:
            return AnalystQueryResult(
                question=question,
                explanation=analyst_response.explanation,
                sql=analyst_response.sql or "",
                results=QueryResult(sql="", error=analyst_response.error),
                suggestions=analyst_response.suggestions,
                success=False,
                error=analyst_response.error or "No SQL generated",
            )

        # Execute the SQL
        sql = analyst_response.sql
        if limit and "LIMIT" not in sql.upper():
            sql = f"{sql.rstrip(';')} LIMIT {limit}"

        start_time = time.time()
        try:
            rows = query_func(sql)
            execution_time_ms = int((time.time() - start_time) * 1000)

            # Extract columns from first row
            columns = list(rows[0].keys()) if rows else []

            query_result = QueryResult(
                sql=sql,
                rows=rows[:limit],
                columns=columns,
                row_count=len(rows),
                execution_time_ms=execution_time_ms,
                truncated=len(rows) > limit,
            )

            return AnalystQueryResult(
                question=question,
                explanation=analyst_response.explanation,
                sql=sql,
                results=query_result,
                suggestions=analyst_response.suggestions,
                conversation_id=analyst_response.request_id,
                success=True,
            )

        except Exception as e:
            execution_time_ms = int((time.time() - start_time) * 1000)
            logger.error(f"SQL execution failed: {e}")

            return AnalystQueryResult(
                question=question,
                explanation=analyst_response.explanation,
                sql=sql,
                results=QueryResult(
                    sql=sql,
                    execution_time_ms=execution_time_ms,
                    error=str(e),
                ),
                suggestions=analyst_response.suggestions,
                success=False,
                error=f"SQL execution failed: {e}",
            )

    def get_conversation(self, conversation_id: str) -> Optional[AnalystConversation]:
        """Get a conversation by ID."""
        return self._conversations.get(conversation_id)

    def list_conversations(self) -> List[Dict[str, Any]]:
        """List all active conversations."""
        return [
            {
                "id": c.id,
                "semantic_model": c.semantic_model,
                "message_count": len(c.messages),
                "created_at": c.created_at.isoformat(),
            }
            for c in self._conversations.values()
        ]

    def clear_conversation(self, conversation_id: str) -> bool:
        """Clear a conversation from history."""
        if conversation_id in self._conversations:
            del self._conversations[conversation_id]
            return True
        return False

    def _get_or_create_conversation(
        self,
        conversation_id: Optional[str],
        semantic_model: str,
    ) -> AnalystConversation:
        """Get existing or create new conversation."""
        if conversation_id and conversation_id in self._conversations:
            return self._conversations[conversation_id]

        conversation = AnalystConversation(semantic_model=semantic_model)
        self._conversations[conversation.id] = conversation
        return conversation

    def _build_request(
        self,
        question: str,
        model_file: Optional[str],
        view: Optional[str],
        conversation: AnalystConversation,
    ) -> Dict[str, Any]:
        """Build the REST API request body."""
        # Build messages array
        messages = conversation.get_messages_for_api()

        # Add current question
        messages.append({
            "role": "user",
            "content": [{"type": "text", "text": question}]
        })

        request = {"messages": messages}

        # Add semantic model reference
        if model_file:
            request["semantic_model_file"] = model_file
        elif view:
            request["semantic_model"] = view

        return request

    def _parse_response(
        self,
        response: Dict[str, Any],
        conversation_id: str,
    ) -> AnalystResponse:
        """Parse the REST API response."""
        request_id = response.get("request_id", str(uuid.uuid4()))

        # Parse message content
        message_data = response.get("message", {})
        content_list = message_data.get("content", [])

        parsed_content = []
        sql = None
        explanation = None
        suggestions = []

        for item in content_list:
            content_type = item.get("type", "text")

            if content_type == "text":
                text = item.get("text", "")
                parsed_content.append(MessageContent(
                    type=MessageContentType.TEXT,
                    text=text,
                ))
                if not explanation:
                    explanation = text

            elif content_type == "sql":
                statement = item.get("statement", "")
                parsed_content.append(MessageContent(
                    type=MessageContentType.SQL,
                    statement=statement,
                ))
                sql = statement

            elif content_type == "suggestions":
                sugg_list = item.get("suggestions", [])
                parsed_content.append(MessageContent(
                    type=MessageContentType.SUGGESTIONS,
                    suggestions=sugg_list,
                ))
                suggestions.extend(sugg_list)

            elif content_type == "error":
                error_msg = item.get("message", item.get("text", "Unknown error"))
                parsed_content.append(MessageContent(
                    type=MessageContentType.ERROR,
                    error_message=error_msg,
                ))

        analyst_message = AnalystMessage(
            role=message_data.get("role", "analyst"),
            content=parsed_content,
        )

        return AnalystResponse(
            request_id=request_id,
            message=analyst_message,
            sql=sql,
            explanation=explanation,
            suggestions=suggestions,
            success=sql is not None or explanation is not None,
        )

    def _extract_sql(self, content: List[Dict[str, Any]]) -> Optional[str]:
        """Extract SQL statement from response content."""
        for item in content:
            if item.get("type") == "sql":
                return item.get("statement")
        return None
