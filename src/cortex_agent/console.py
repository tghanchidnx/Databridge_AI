"""
CommunicationConsole - Full observability for CortexAgent.

Provides multiple output targets for agent communication:
- CLIOutput: Color-coded terminal output
- FileOutput: JSON lines file logging
- DatabaseOutput: Snowflake table logging
- WebSocketOutput: Real-time streaming (future)

All outputs receive AgentMessage objects and format them appropriately.
"""

import asyncio
import json
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .types import AgentMessage, AgentState, MessageType

logger = logging.getLogger(__name__)


class ConsoleOutput(ABC):
    """Base class for console output targets."""

    @abstractmethod
    async def write(self, message: AgentMessage) -> None:
        """Write a message to this output."""
        pass

    @abstractmethod
    async def close(self) -> None:
        """Clean up resources."""
        pass


class CLIOutput(ConsoleOutput):
    """Color-coded terminal output for agent messages."""

    # ANSI color codes
    COLORS = {
        MessageType.REQUEST: "\033[94m",       # Blue
        MessageType.RESPONSE: "\033[92m",      # Green
        MessageType.THINKING: "\033[93m",      # Yellow
        MessageType.PLAN: "\033[95m",          # Magenta
        MessageType.OBSERVATION: "\033[96m",   # Cyan
        MessageType.EXECUTION: "\033[97m",     # White
        MessageType.REFLECTION: "\033[90m",    # Gray
        MessageType.ERROR: "\033[91m",         # Red
        MessageType.SYSTEM: "\033[90m",        # Gray
    }
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"

    # State indicators
    STATE_ICONS = {
        AgentState.IDLE: "⏸",
        AgentState.OBSERVING: "👁",
        AgentState.PLANNING: "📋",
        AgentState.EXECUTING: "⚡",
        AgentState.REFLECTING: "🤔",
        AgentState.SYNTHESIZING: "🔄",
        AgentState.COMPLETED: "✅",
        AgentState.ERROR: "❌",
    }

    def __init__(self, verbose: bool = True, show_queries: bool = False):
        """
        Initialize CLI output.

        Args:
            verbose: Show detailed output
            show_queries: Show full Cortex SQL queries
        """
        self.verbose = verbose
        self.show_queries = show_queries

    async def write(self, message: AgentMessage) -> None:
        """Write formatted message to terminal."""
        color = self.COLORS.get(message.message_type, "")
        timestamp = message.timestamp.strftime("%H:%M:%S")

        # Format header
        header = f"{self.DIM}[{timestamp}]{self.RESET} "
        header += f"{color}{self.BOLD}[{message.message_type.value.upper()}]{self.RESET} "

        # Format content
        content = message.content

        # Truncate long content in non-verbose mode
        if not self.verbose and len(content) > 200:
            content = content[:200] + "..."

        # Print main message
        print(f"{header}{color}{content}{self.RESET}")

        # Show thinking if present
        if message.thinking and self.verbose:
            thinking_lines = message.thinking.split("\n")
            for line in thinking_lines[:5]:  # Limit to 5 lines
                print(f"  {self.DIM}💭 {line}{self.RESET}")

        # Show Cortex query if enabled
        if message.cortex_query and self.show_queries:
            query_preview = message.cortex_query[:150].replace("\n", " ")
            print(f"  {self.DIM}📝 SQL: {query_preview}...{self.RESET}")

        # Show result preview for execution messages
        if message.cortex_result and message.message_type == MessageType.EXECUTION:
            result_preview = str(message.cortex_result)[:100]
            print(f"  {self.DIM}📤 Result: {result_preview}...{self.RESET}")

    async def close(self) -> None:
        """No cleanup needed for CLI."""
        pass


class FileOutput(ConsoleOutput):
    """JSON lines file logging for agent messages."""

    def __init__(self, log_path: Path):
        """
        Initialize file output.

        Args:
            log_path: Path to the log file
        """
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    async def write(self, message: AgentMessage) -> None:
        """Write message as JSON line to file."""
        try:
            with open(self.log_path, "a") as f:
                json_line = json.dumps(message.to_dict(), default=str)
                f.write(json_line + "\n")
        except Exception as e:
            logger.error(f"Failed to write to log file: {e}")

    async def close(self) -> None:
        """No cleanup needed for file."""
        pass

    def read_log(
        self,
        limit: int = 100,
        conversation_id: Optional[str] = None,
        message_type: Optional[MessageType] = None,
    ) -> List[Dict[str, Any]]:
        """
        Read messages from log file.

        Args:
            limit: Maximum messages to return
            conversation_id: Filter by conversation
            message_type: Filter by message type

        Returns:
            List of message dictionaries
        """
        if not self.log_path.exists():
            return []

        messages = []
        try:
            with open(self.log_path, "r") as f:
                for line in f:
                    if line.strip():
                        msg = json.loads(line)

                        # Apply filters
                        if conversation_id and msg.get("conversation_id") != conversation_id:
                            continue
                        if message_type and msg.get("message_type") != message_type.value:
                            continue

                        messages.append(msg)
        except Exception as e:
            logger.error(f"Failed to read log file: {e}")

        # Return most recent
        return messages[-limit:]


class DatabaseOutput(ConsoleOutput):
    """Snowflake table logging for agent messages."""

    TABLE_DDL = """
    CREATE TABLE IF NOT EXISTS DATABRIDGE_CORTEX_MESSAGES (
        ID VARCHAR(36) PRIMARY KEY,
        CONVERSATION_ID VARCHAR(36),
        TIMESTAMP TIMESTAMP_NTZ,
        FROM_AGENT VARCHAR(100),
        TO_AGENT VARCHAR(100),
        MESSAGE_TYPE VARCHAR(50),
        CONTENT TEXT,
        METADATA VARIANT,
        THINKING TEXT,
        CORTEX_QUERY TEXT,
        CORTEX_RESULT TEXT,
        CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
    )
    """

    def __init__(
        self,
        query_func: callable,
        connection_id: str,
        table_name: str = "DATABRIDGE_CORTEX_MESSAGES",
    ):
        """
        Initialize database output.

        Args:
            query_func: Function to execute SQL
            connection_id: Snowflake connection ID
            table_name: Table name for messages
        """
        self.query_func = query_func
        self.connection_id = connection_id
        self.table_name = table_name
        self._initialized = False

    async def _ensure_table(self) -> None:
        """Create table if not exists."""
        if self._initialized:
            return

        try:
            self.query_func(
                connection_id=self.connection_id,
                query=self.TABLE_DDL,
            )
            self._initialized = True
        except Exception as e:
            logger.warning(f"Failed to create messages table: {e}")

    async def write(self, message: AgentMessage) -> None:
        """Write message to Snowflake table."""
        await self._ensure_table()

        try:
            # Escape content for SQL
            def escape(s: str) -> str:
                if s is None:
                    return "NULL"
                return "'" + s.replace("'", "''") + "'"

            metadata_json = json.dumps(message.metadata) if message.metadata else "{}"

            sql = f"""
            INSERT INTO {self.table_name} (
                ID, CONVERSATION_ID, TIMESTAMP, FROM_AGENT, TO_AGENT,
                MESSAGE_TYPE, CONTENT, METADATA, THINKING, CORTEX_QUERY, CORTEX_RESULT
            ) VALUES (
                {escape(message.id)},
                {escape(message.conversation_id)},
                '{message.timestamp.isoformat()}',
                {escape(message.from_agent)},
                {escape(message.to_agent)},
                {escape(message.message_type.value)},
                {escape(message.content)},
                PARSE_JSON({escape(metadata_json)}),
                {escape(message.thinking)},
                {escape(message.cortex_query)},
                {escape(message.cortex_result)}
            )
            """

            self.query_func(
                connection_id=self.connection_id,
                query=sql,
            )

        except Exception as e:
            logger.error(f"Failed to write message to database: {e}")

    async def close(self) -> None:
        """No cleanup needed for database."""
        pass


class InMemoryOutput(ConsoleOutput):
    """In-memory storage for testing and quick access."""

    def __init__(self, max_messages: int = 1000):
        """
        Initialize in-memory output.

        Args:
            max_messages: Maximum messages to keep
        """
        self.max_messages = max_messages
        self.messages: List[AgentMessage] = []

    async def write(self, message: AgentMessage) -> None:
        """Store message in memory."""
        self.messages.append(message)

        # Trim if over limit
        if len(self.messages) > self.max_messages:
            self.messages = self.messages[-self.max_messages:]

    async def close(self) -> None:
        """Clear messages."""
        self.messages.clear()

    def get_messages(
        self,
        conversation_id: Optional[str] = None,
        message_type: Optional[MessageType] = None,
        limit: int = 100,
    ) -> List[AgentMessage]:
        """
        Get stored messages with optional filtering.

        Args:
            conversation_id: Filter by conversation
            message_type: Filter by type
            limit: Maximum to return

        Returns:
            List of messages
        """
        filtered = self.messages

        if conversation_id:
            filtered = [m for m in filtered if m.conversation_id == conversation_id]

        if message_type:
            filtered = [m for m in filtered if m.message_type == message_type]

        return filtered[-limit:]


class CommunicationConsole:
    """Central console distributing messages to all outputs."""

    def __init__(
        self,
        outputs: Optional[List[ConsoleOutput]] = None,
        enable_cli: bool = True,
        enable_file: bool = True,
        log_path: Optional[Path] = None,
    ):
        """
        Initialize communication console.

        Args:
            outputs: List of output targets (overrides defaults)
            enable_cli: Enable CLI output (if outputs not provided)
            enable_file: Enable file output (if outputs not provided)
            log_path: Path for file output
        """
        if outputs:
            self.outputs = outputs
        else:
            self.outputs = []
            if enable_cli:
                self.outputs.append(CLIOutput())
            if enable_file:
                path = log_path or Path("data/cortex_agent/console.jsonl")
                self.outputs.append(FileOutput(path))

        # Always keep in-memory store
        self._memory = InMemoryOutput()
        self.outputs.append(self._memory)

    async def log(self, message: AgentMessage) -> None:
        """
        Log message to all outputs.

        Args:
            message: The message to log
        """
        # Write to all outputs concurrently
        tasks = [output.write(message) for output in self.outputs]
        await asyncio.gather(*tasks, return_exceptions=True)

    def log_sync(self, message: AgentMessage) -> None:
        """
        Synchronous version of log for non-async contexts.

        Args:
            message: The message to log
        """
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Schedule in running loop
                asyncio.create_task(self.log(message))
            else:
                loop.run_until_complete(self.log(message))
        except RuntimeError:
            # No event loop, create one
            asyncio.run(self.log(message))

    def log_request(
        self,
        conversation_id: str,
        content: str,
        from_agent: str = "user",
        to_agent: str = "cortex_agent",
    ) -> AgentMessage:
        """Helper to log a request message."""
        message = AgentMessage.create(
            conversation_id=conversation_id,
            from_agent=from_agent,
            to_agent=to_agent,
            message_type=MessageType.REQUEST,
            content=content,
        )
        self.log_sync(message)
        return message

    def log_thinking(
        self,
        conversation_id: str,
        content: str,
        thinking: Optional[str] = None,
        cortex_query: Optional[str] = None,
    ) -> AgentMessage:
        """Helper to log a thinking message."""
        message = AgentMessage.create(
            conversation_id=conversation_id,
            from_agent="cortex_agent",
            to_agent="cortex_agent",
            message_type=MessageType.THINKING,
            content=content,
            thinking=thinking,
            cortex_query=cortex_query,
        )
        self.log_sync(message)
        return message

    def log_plan(
        self,
        conversation_id: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AgentMessage:
        """Helper to log a plan message."""
        message = AgentMessage.create(
            conversation_id=conversation_id,
            from_agent="cortex_agent",
            to_agent="user",
            message_type=MessageType.PLAN,
            content=content,
            metadata=metadata or {},
        )
        self.log_sync(message)
        return message

    def log_execution(
        self,
        conversation_id: str,
        content: str,
        cortex_query: Optional[str] = None,
        cortex_result: Optional[str] = None,
    ) -> AgentMessage:
        """Helper to log an execution message."""
        message = AgentMessage.create(
            conversation_id=conversation_id,
            from_agent="cortex_agent",
            to_agent="cortex",
            message_type=MessageType.EXECUTION,
            content=content,
            cortex_query=cortex_query,
            cortex_result=cortex_result,
        )
        self.log_sync(message)
        return message

    def log_response(
        self,
        conversation_id: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> AgentMessage:
        """Helper to log a response message."""
        message = AgentMessage.create(
            conversation_id=conversation_id,
            from_agent="cortex_agent",
            to_agent="user",
            message_type=MessageType.RESPONSE,
            content=content,
            metadata=metadata or {},
        )
        self.log_sync(message)
        return message

    def log_error(
        self,
        conversation_id: str,
        content: str,
        error: Optional[str] = None,
    ) -> AgentMessage:
        """Helper to log an error message."""
        message = AgentMessage.create(
            conversation_id=conversation_id,
            from_agent="cortex_agent",
            to_agent="user",
            message_type=MessageType.ERROR,
            content=content,
            metadata={"error": error} if error else {},
        )
        self.log_sync(message)
        return message

    def get_conversation(self, conversation_id: str) -> List[AgentMessage]:
        """
        Get all messages for a conversation.

        Args:
            conversation_id: The conversation ID

        Returns:
            List of messages in order
        """
        return self._memory.get_messages(conversation_id=conversation_id)

    def get_recent(
        self,
        limit: int = 50,
        message_type: Optional[MessageType] = None,
    ) -> List[AgentMessage]:
        """
        Get recent messages.

        Args:
            limit: Maximum to return
            message_type: Filter by type

        Returns:
            List of recent messages
        """
        return self._memory.get_messages(
            message_type=message_type,
            limit=limit,
        )

    def get_status(self) -> Dict[str, Any]:
        """Get console status."""
        return {
            "output_count": len(self.outputs),
            "output_types": [type(o).__name__ for o in self.outputs],
            "total_messages": len(self._memory.messages),
            "conversations": len(set(m.conversation_id for m in self._memory.messages)),
        }

    async def close(self) -> None:
        """Close all outputs."""
        tasks = [output.close() for output in self.outputs]
        await asyncio.gather(*tasks, return_exceptions=True)
