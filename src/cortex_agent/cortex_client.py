"""
CortexClient - Execute Snowflake Cortex AI functions via SQL.

This client wraps Cortex LLM functions and executes them through
existing DataBridge connections. All data stays in Snowflake.

Supported functions:
- COMPLETE() - Text generation with LLM
- SUMMARIZE() - Text summarization
- SENTIMENT() - Sentiment analysis (-1 to 1)
- TRANSLATE() - Language translation
- EXTRACT_ANSWER() - Question answering from context
"""

import json
import time
from typing import Any, Dict, List, Optional
import logging

from .types import CortexFunction, CortexQueryResult

logger = logging.getLogger(__name__)


class CortexClient:
    """Execute Cortex LLM functions via SQL through existing connections."""

    def __init__(
        self,
        connection_id: str,
        query_func: callable,
        default_model: str = "mistral-large",
        temperature: float = 0.3,
    ):
        """
        Initialize CortexClient.

        Args:
            connection_id: ID of the Snowflake connection
            query_func: Function to execute SQL queries (from connections API)
            default_model: Default model for COMPLETE()
            temperature: Default temperature for COMPLETE()
        """
        self.connection_id = connection_id
        self.query_func = query_func
        self.default_model = default_model
        self.temperature = temperature
        self._call_count = 0

    def complete(
        self,
        prompt: str,
        model: Optional[str] = None,
        temperature: Optional[float] = None,
        max_tokens: Optional[int] = None,
    ) -> CortexQueryResult:
        """
        Generate text using Cortex COMPLETE().

        Args:
            prompt: The prompt for text generation
            model: Model to use (default: mistral-large)
            temperature: Sampling temperature
            max_tokens: Maximum tokens to generate

        Returns:
            CortexQueryResult with generated text
        """
        model = model or self.default_model
        temp = temperature if temperature is not None else self.temperature

        # Build options object
        options = {"temperature": temp}
        if max_tokens:
            options["max_tokens"] = max_tokens

        # Escape single quotes in prompt
        safe_prompt = prompt.replace("'", "''")
        options_json = json.dumps(options)

        sql = f"""
        SELECT SNOWFLAKE.CORTEX.COMPLETE(
            '{model}',
            '{safe_prompt}',
            {options_json}
        ) AS result
        """

        return self._execute_cortex(CortexFunction.COMPLETE, sql)

    def summarize(self, text: str) -> CortexQueryResult:
        """
        Summarize text using Cortex SUMMARIZE().

        Args:
            text: Text to summarize

        Returns:
            CortexQueryResult with summary
        """
        safe_text = text.replace("'", "''")

        sql = f"""
        SELECT SNOWFLAKE.CORTEX.SUMMARIZE('{safe_text}') AS result
        """

        return self._execute_cortex(CortexFunction.SUMMARIZE, sql)

    def sentiment(self, text: str) -> CortexQueryResult:
        """
        Analyze sentiment using Cortex SENTIMENT().

        Args:
            text: Text to analyze

        Returns:
            CortexQueryResult with sentiment score (-1 to 1)
        """
        safe_text = text.replace("'", "''")

        sql = f"""
        SELECT SNOWFLAKE.CORTEX.SENTIMENT('{safe_text}') AS result
        """

        return self._execute_cortex(CortexFunction.SENTIMENT, sql)

    def translate(
        self,
        text: str,
        from_lang: str,
        to_lang: str,
    ) -> CortexQueryResult:
        """
        Translate text using Cortex TRANSLATE().

        Args:
            text: Text to translate
            from_lang: Source language code (e.g., 'en', 'es', 'fr')
            to_lang: Target language code

        Returns:
            CortexQueryResult with translation
        """
        safe_text = text.replace("'", "''")

        sql = f"""
        SELECT SNOWFLAKE.CORTEX.TRANSLATE(
            '{safe_text}',
            '{from_lang}',
            '{to_lang}'
        ) AS result
        """

        return self._execute_cortex(CortexFunction.TRANSLATE, sql)

    def extract_answer(
        self,
        context: str,
        question: str,
    ) -> CortexQueryResult:
        """
        Extract answer from context using Cortex EXTRACT_ANSWER().

        Args:
            context: Context text to search
            question: Question to answer

        Returns:
            CortexQueryResult with extracted answer
        """
        safe_context = context.replace("'", "''")
        safe_question = question.replace("'", "''")

        sql = f"""
        SELECT SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
            '{safe_context}',
            '{safe_question}'
        ) AS result
        """

        return self._execute_cortex(CortexFunction.EXTRACT_ANSWER, sql)

    def complete_on_table(
        self,
        table: str,
        column: str,
        prompt_template: str,
        model: Optional[str] = None,
        limit: int = 10,
    ) -> CortexQueryResult:
        """
        Apply COMPLETE() to each row in a table column.

        Args:
            table: Fully qualified table name
            column: Column to process
            prompt_template: Prompt template with {value} placeholder
            model: Model to use
            limit: Maximum rows to process

        Returns:
            CortexQueryResult with array of results
        """
        model = model or self.default_model
        safe_template = prompt_template.replace("'", "''")

        sql = f"""
        SELECT
            {column} as original_value,
            SNOWFLAKE.CORTEX.COMPLETE(
                '{model}',
                REPLACE('{safe_template}', '{{value}}', {column})
            ) AS result
        FROM {table}
        LIMIT {limit}
        """

        return self._execute_cortex(CortexFunction.COMPLETE, sql)

    def sentiment_on_table(
        self,
        table: str,
        column: str,
        limit: int = 100,
    ) -> CortexQueryResult:
        """
        Apply SENTIMENT() to each row in a table column.

        Args:
            table: Fully qualified table name
            column: Column to analyze
            limit: Maximum rows to process

        Returns:
            CortexQueryResult with sentiment scores
        """
        sql = f"""
        SELECT
            {column} as text,
            SNOWFLAKE.CORTEX.SENTIMENT({column}) AS sentiment
        FROM {table}
        LIMIT {limit}
        """

        return self._execute_cortex(CortexFunction.SENTIMENT, sql)

    def execute_sql(self, sql: str) -> List[Dict[str, Any]]:
        """
        Execute arbitrary SQL via connection.

        Args:
            sql: SQL query to execute

        Returns:
            List of result rows as dictionaries
        """
        try:
            result = self.query_func(
                connection_id=self.connection_id,
                query=sql,
            )
            return result if isinstance(result, list) else [result]
        except Exception as e:
            logger.error(f"SQL execution failed: {e}")
            raise

    def _execute_cortex(
        self,
        function: CortexFunction,
        sql: str,
    ) -> CortexQueryResult:
        """
        Execute a Cortex SQL query and wrap result.

        Args:
            function: The Cortex function being called
            sql: The SQL query

        Returns:
            CortexQueryResult
        """
        start_time = time.time()
        self._call_count += 1

        try:
            result = self.query_func(
                connection_id=self.connection_id,
                query=sql,
            )

            duration_ms = int((time.time() - start_time) * 1000)

            # Extract the result value
            if isinstance(result, list) and len(result) > 0:
                raw_result = result[0].get("RESULT", result[0].get("result", result))
            else:
                raw_result = result

            # Parse JSON result if applicable
            parsed_result = raw_result
            if isinstance(raw_result, str):
                try:
                    parsed_result = json.loads(raw_result)
                except (json.JSONDecodeError, TypeError):
                    parsed_result = raw_result

            return CortexQueryResult(
                function=function,
                query=sql.strip(),
                result=parsed_result,
                duration_ms=duration_ms,
                success=True,
                raw_response=str(raw_result),
            )

        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            logger.error(f"Cortex {function.value} failed: {e}")

            return CortexQueryResult(
                function=function,
                query=sql.strip(),
                result=None,
                duration_ms=duration_ms,
                success=False,
                error=str(e),
            )

    def get_call_count(self) -> int:
        """Get total number of Cortex calls made."""
        return self._call_count

    def reset_call_count(self) -> None:
        """Reset the call counter."""
        self._call_count = 0

    def test_connection(self) -> Dict[str, Any]:
        """
        Test the Cortex connection with a simple query.

        Returns:
            Dict with connection status
        """
        try:
            result = self.complete(
                prompt="Say 'Hello from Cortex' in exactly 4 words.",
                max_tokens=20,
            )

            return {
                "success": result.success,
                "connection_id": self.connection_id,
                "model": self.default_model,
                "test_response": result.result if result.success else None,
                "error": result.error,
                "duration_ms": result.duration_ms,
            }

        except Exception as e:
            return {
                "success": False,
                "connection_id": self.connection_id,
                "model": self.default_model,
                "error": str(e),
            }
