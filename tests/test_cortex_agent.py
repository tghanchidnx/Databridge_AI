"""
Unit tests for CortexAgent module.

Tests cover:
- Types and data models
- CortexClient SQL generation
- Context state management
- Console output handling
- Reasoning loop logic
"""

import json
import pytest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch, AsyncMock
import tempfile
import shutil

# Import types
from src.cortex_agent.types import (
    MessageType,
    AgentState,
    CortexFunction,
    AgentMessage,
    ThinkingStep,
    PlanStep,
    ExecutionPlan,
    StepResult,
    Conversation,
    AgentResponse,
    CortexAgentConfig,
    CortexQueryResult,
)

# Import core classes
from src.cortex_agent.cortex_client import CortexClient
from src.cortex_agent.context import CortexAgentContext, get_context, reset_context
from src.cortex_agent.console import (
    CLIOutput,
    FileOutput,
    InMemoryOutput,
    CommunicationConsole,
)


class TestTypes:
    """Test Pydantic models and dataclasses."""

    def test_message_type_enum(self):
        """Test MessageType enum values."""
        assert MessageType.REQUEST.value == "request"
        assert MessageType.THINKING.value == "thinking"
        assert MessageType.RESPONSE.value == "response"

    def test_agent_state_enum(self):
        """Test AgentState enum values."""
        assert AgentState.IDLE.value == "idle"
        assert AgentState.OBSERVING.value == "observing"
        assert AgentState.PLANNING.value == "planning"
        assert AgentState.EXECUTING.value == "executing"

    def test_cortex_function_enum(self):
        """Test CortexFunction enum values."""
        assert CortexFunction.COMPLETE.value == "COMPLETE"
        assert CortexFunction.SUMMARIZE.value == "SUMMARIZE"
        assert CortexFunction.SENTIMENT.value == "SENTIMENT"

    def test_agent_message_create(self):
        """Test AgentMessage factory method."""
        msg = AgentMessage.create(
            conversation_id="conv-123",
            from_agent="user",
            to_agent="cortex_agent",
            message_type=MessageType.REQUEST,
            content="Test message",
        )

        assert msg.conversation_id == "conv-123"
        assert msg.from_agent == "user"
        assert msg.message_type == MessageType.REQUEST
        assert msg.content == "Test message"
        assert msg.id is not None
        assert msg.timestamp is not None

    def test_agent_message_serialization(self):
        """Test AgentMessage to_dict and from_dict."""
        msg = AgentMessage.create(
            conversation_id="conv-123",
            from_agent="user",
            to_agent="cortex_agent",
            message_type=MessageType.REQUEST,
            content="Test message",
            metadata={"key": "value"},
        )

        msg_dict = msg.to_dict()
        assert msg_dict["conversation_id"] == "conv-123"
        assert msg_dict["message_type"] == "request"
        assert msg_dict["metadata"] == {"key": "value"}

        # Reconstruct from dict
        msg2 = AgentMessage.from_dict(msg_dict)
        assert msg2.conversation_id == msg.conversation_id
        assert msg2.message_type == msg.message_type

    def test_thinking_step(self):
        """Test ThinkingStep dataclass."""
        step = ThinkingStep(
            step_number=1,
            phase=AgentState.OBSERVING,
            content="Analyzing goal",
            cortex_function=CortexFunction.COMPLETE,
            cortex_query="SELECT ...",
            cortex_result="Analysis result",
            duration_ms=150,
        )

        step_dict = step.to_dict()
        assert step_dict["step_number"] == 1
        assert step_dict["phase"] == "observing"
        assert step_dict["cortex_function"] == "COMPLETE"

    def test_plan_step(self):
        """Test PlanStep dataclass."""
        step = PlanStep(
            step_number=1,
            action="Analyze data",
            description="Analyze the data quality",
            cortex_function=CortexFunction.COMPLETE,
            parameters={"prompt": "Analyze this"},
        )

        step_dict = step.to_dict()
        assert step_dict["action"] == "Analyze data"
        assert step_dict["cortex_function"] == "COMPLETE"

    def test_execution_plan(self):
        """Test ExecutionPlan dataclass."""
        steps = [
            PlanStep(step_number=1, action="Step 1", description="First step"),
            PlanStep(step_number=2, action="Step 2", description="Second step"),
        ]

        plan = ExecutionPlan(
            plan_id="plan-123",
            goal="Test goal",
            steps=steps,
            estimated_steps=2,
            confidence=0.85,
            reasoning="This is the reasoning",
        )

        plan_dict = plan.to_dict()
        assert plan_dict["plan_id"] == "plan-123"
        assert len(plan_dict["steps"]) == 2
        assert plan_dict["confidence"] == 0.85

    def test_cortex_agent_config(self):
        """Test CortexAgentConfig model."""
        config = CortexAgentConfig(
            connection_id="snowflake-prod",
            cortex_model="mistral-large",
            max_reasoning_steps=10,
            temperature=0.3,
        )

        assert config.connection_id == "snowflake-prod"
        assert config.cortex_model == "mistral-large"
        assert config.max_reasoning_steps == 10
        assert config.temperature == 0.3

    def test_cortex_query_result(self):
        """Test CortexQueryResult model."""
        result = CortexQueryResult(
            function=CortexFunction.COMPLETE,
            query="SELECT SNOWFLAKE.CORTEX.COMPLETE(...)",
            result="Generated text",
            duration_ms=250,
            success=True,
        )

        assert result.function == CortexFunction.COMPLETE
        assert result.success is True
        assert result.duration_ms == 250


class TestCortexClient:
    """Test CortexClient SQL generation."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_query = MagicMock(return_value=[{"RESULT": '"Test response"'}])
        self.client = CortexClient(
            connection_id="test-conn",
            query_func=self.mock_query,
            default_model="mistral-large",
        )

    def test_complete_basic(self):
        """Test basic COMPLETE call."""
        result = self.client.complete("Test prompt")

        assert result.function == CortexFunction.COMPLETE
        assert result.success is True
        self.mock_query.assert_called_once()

        # Check SQL contains COMPLETE function
        call_args = self.mock_query.call_args
        sql = call_args.kwargs.get("query", call_args.args[1] if len(call_args.args) > 1 else "")
        assert "SNOWFLAKE.CORTEX.COMPLETE" in sql
        assert "mistral-large" in sql

    def test_complete_with_options(self):
        """Test COMPLETE with temperature and max_tokens."""
        result = self.client.complete(
            "Test prompt",
            model="llama3-70b",
            temperature=0.5,
            max_tokens=100,
        )

        assert result.success is True
        call_args = self.mock_query.call_args
        sql = call_args.kwargs.get("query", call_args.args[1] if len(call_args.args) > 1 else "")
        assert "llama3-70b" in sql

    def test_summarize(self):
        """Test SUMMARIZE call."""
        self.mock_query.return_value = [{"RESULT": '"Summary of text"'}]
        result = self.client.summarize("Long text to summarize")

        assert result.function == CortexFunction.SUMMARIZE
        call_args = self.mock_query.call_args
        sql = call_args.kwargs.get("query", call_args.args[1] if len(call_args.args) > 1 else "")
        assert "SNOWFLAKE.CORTEX.SUMMARIZE" in sql

    def test_sentiment(self):
        """Test SENTIMENT call."""
        self.mock_query.return_value = [{"RESULT": "0.85"}]
        result = self.client.sentiment("This is great!")

        assert result.function == CortexFunction.SENTIMENT
        call_args = self.mock_query.call_args
        sql = call_args.kwargs.get("query", call_args.args[1] if len(call_args.args) > 1 else "")
        assert "SNOWFLAKE.CORTEX.SENTIMENT" in sql

    def test_translate(self):
        """Test TRANSLATE call."""
        self.mock_query.return_value = [{"RESULT": '"Hola mundo"'}]
        result = self.client.translate("Hello world", "en", "es")

        assert result.function == CortexFunction.TRANSLATE
        call_args = self.mock_query.call_args
        sql = call_args.kwargs.get("query", call_args.args[1] if len(call_args.args) > 1 else "")
        assert "SNOWFLAKE.CORTEX.TRANSLATE" in sql
        assert "'en'" in sql
        assert "'es'" in sql

    def test_extract_answer(self):
        """Test EXTRACT_ANSWER call."""
        self.mock_query.return_value = [{"RESULT": '"2010"'}]
        result = self.client.extract_answer(
            "The company was founded in 2010.",
            "When was the company founded?",
        )

        assert result.function == CortexFunction.EXTRACT_ANSWER
        call_args = self.mock_query.call_args
        sql = call_args.kwargs.get("query", call_args.args[1] if len(call_args.args) > 1 else "")
        assert "SNOWFLAKE.CORTEX.EXTRACT_ANSWER" in sql

    def test_escape_single_quotes(self):
        """Test that single quotes are escaped in SQL."""
        result = self.client.complete("It's a test with 'quotes'")

        call_args = self.mock_query.call_args
        sql = call_args.kwargs.get("query", call_args.args[1] if len(call_args.args) > 1 else "")
        assert "It''s a test with ''quotes''" in sql

    def test_call_count(self):
        """Test call counter."""
        assert self.client.get_call_count() == 0

        self.client.complete("Test 1")
        self.client.complete("Test 2")

        assert self.client.get_call_count() == 2

        self.client.reset_call_count()
        assert self.client.get_call_count() == 0

    def test_error_handling(self):
        """Test error handling in Cortex calls."""
        self.mock_query.side_effect = Exception("Connection failed")
        result = self.client.complete("Test")

        assert result.success is False
        assert result.error == "Connection failed"


class TestCortexAgentContext:
    """Test CortexAgentContext state management."""

    def setup_method(self):
        """Set up test fixtures with temp directory."""
        self.temp_dir = Path(tempfile.mkdtemp())
        reset_context()  # Reset singleton
        self.context = CortexAgentContext(data_dir=self.temp_dir)

    def teardown_method(self):
        """Clean up temp directory."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        reset_context()

    def test_configure(self):
        """Test configuration."""
        config = CortexAgentConfig(
            connection_id="test-conn",
            cortex_model="llama3-70b",
        )

        self.context.configure(config)

        assert self.context.is_configured()
        assert self.context.get_config().connection_id == "test-conn"
        assert self.context.get_config().cortex_model == "llama3-70b"

    def test_start_conversation(self):
        """Test starting a conversation."""
        conv_id = self.context.start_conversation("Test goal")

        assert conv_id is not None
        conv = self.context.get_conversation(conv_id)
        assert conv is not None
        assert conv.goal == "Test goal"
        assert conv.state == AgentState.IDLE

    def test_update_state(self):
        """Test updating conversation state."""
        conv_id = self.context.start_conversation("Test goal")

        self.context.update_state(conv_id, AgentState.OBSERVING)
        conv = self.context.get_conversation(conv_id)
        assert conv.state == AgentState.OBSERVING

        self.context.update_state(conv_id, AgentState.COMPLETED)
        conv = self.context.get_conversation(conv_id)
        assert conv.state == AgentState.COMPLETED
        assert conv.completed_at is not None

    def test_add_message(self):
        """Test adding messages."""
        conv_id = self.context.start_conversation("Test goal")

        msg = AgentMessage.create(
            conversation_id=conv_id,
            from_agent="user",
            to_agent="agent",
            message_type=MessageType.REQUEST,
            content="Test message",
        )
        self.context.add_message(conv_id, msg)

        conv = self.context.get_conversation(conv_id)
        assert len(conv.messages) == 1
        assert conv.messages[0].content == "Test message"

    def test_add_thinking_step(self):
        """Test adding thinking steps."""
        conv_id = self.context.start_conversation("Test goal")

        step = ThinkingStep(
            step_number=1,
            phase=AgentState.OBSERVING,
            content="Analyzing",
        )
        self.context.add_thinking_step(conv_id, step)

        conv = self.context.get_conversation(conv_id)
        assert len(conv.thinking_steps) == 1
        assert conv.thinking_steps[0].content == "Analyzing"

    def test_list_conversations(self):
        """Test listing conversations."""
        self.context.start_conversation("Goal 1")
        self.context.start_conversation("Goal 2")

        convs = self.context.list_conversations()
        assert len(convs) == 2

    def test_get_scratchpad_context(self):
        """Test scratchpad formatting."""
        conv_id = self.context.start_conversation("Test goal")

        step = ThinkingStep(
            step_number=1,
            phase=AgentState.OBSERVING,
            content="Analyzed the data",
            cortex_result="Found 10 issues",
        )
        self.context.add_thinking_step(conv_id, step)

        scratchpad = self.context.get_scratchpad_context(conv_id)
        assert "Test goal" in scratchpad
        assert "Analyzed the data" in scratchpad

    def test_persistence(self):
        """Test state persistence to disk."""
        config = CortexAgentConfig(connection_id="test-conn")
        self.context.configure(config)
        conv_id = self.context.start_conversation("Persistent goal")

        # Create new context instance with same directory
        context2 = CortexAgentContext(data_dir=self.temp_dir)

        assert context2.is_configured()
        assert context2.get_config().connection_id == "test-conn"

        conv = context2.get_conversation(conv_id)
        assert conv is not None
        assert conv.goal == "Persistent goal"

    def test_get_stats(self):
        """Test statistics gathering."""
        self.context.start_conversation("Goal 1")
        self.context.start_conversation("Goal 2")

        stats = self.context.get_stats()
        assert stats["total_conversations"] == 2
        assert "by_state" in stats

    def test_clear(self):
        """Test clearing conversations."""
        config = CortexAgentConfig(connection_id="test-conn")
        self.context.configure(config)
        self.context.start_conversation("Goal 1")

        self.context.clear()

        assert len(self.context.list_conversations()) == 0
        assert self.context.is_configured()  # Config preserved


class TestConsole:
    """Test Console output classes."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())

    def teardown_method(self):
        """Clean up temp directory."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_cli_output(self):
        """Test CLI output (just check it doesn't error)."""
        output = CLIOutput(verbose=True)
        msg = AgentMessage.create(
            conversation_id="conv-123",
            from_agent="user",
            to_agent="agent",
            message_type=MessageType.REQUEST,
            content="Test message",
        )

        # Should not raise
        await output.write(msg)
        await output.close()

    @pytest.mark.asyncio
    async def test_file_output(self):
        """Test file output."""
        log_path = self.temp_dir / "test.jsonl"
        output = FileOutput(log_path)

        msg = AgentMessage.create(
            conversation_id="conv-123",
            from_agent="user",
            to_agent="agent",
            message_type=MessageType.REQUEST,
            content="Test message",
        )

        await output.write(msg)

        # Check file was written
        assert log_path.exists()
        with open(log_path) as f:
            line = f.readline()
            data = json.loads(line)
            assert data["content"] == "Test message"

    @pytest.mark.asyncio
    async def test_file_output_read_log(self):
        """Test reading log entries."""
        log_path = self.temp_dir / "test.jsonl"
        output = FileOutput(log_path)

        # Write multiple messages
        for i in range(3):
            msg = AgentMessage.create(
                conversation_id=f"conv-{i}",
                from_agent="user",
                to_agent="agent",
                message_type=MessageType.REQUEST,
                content=f"Message {i}",
            )
            await output.write(msg)

        entries = output.read_log(limit=10)
        assert len(entries) == 3

        # Test filtering
        entries = output.read_log(conversation_id="conv-1")
        assert len(entries) == 1

    @pytest.mark.asyncio
    async def test_in_memory_output(self):
        """Test in-memory output."""
        output = InMemoryOutput(max_messages=5)

        for i in range(7):
            msg = AgentMessage.create(
                conversation_id="conv-123",
                from_agent="user",
                to_agent="agent",
                message_type=MessageType.REQUEST,
                content=f"Message {i}",
            )
            await output.write(msg)

        # Should only keep last 5
        messages = output.get_messages()
        assert len(messages) == 5

    @pytest.mark.asyncio
    async def test_communication_console(self):
        """Test CommunicationConsole with multiple outputs."""
        import asyncio

        log_path = self.temp_dir / "console.jsonl"
        console = CommunicationConsole(
            enable_cli=False,
            enable_file=True,
            log_path=log_path,
        )

        # Test async logging directly
        msg1 = AgentMessage.create(
            conversation_id="conv-123",
            from_agent="user",
            to_agent="agent",
            message_type=MessageType.REQUEST,
            content="Test request",
        )
        await console.log(msg1)

        msg2 = AgentMessage.create(
            conversation_id="conv-123",
            from_agent="agent",
            to_agent="agent",
            message_type=MessageType.THINKING,
            content="Thinking...",
        )
        await console.log(msg2)

        msg3 = AgentMessage.create(
            conversation_id="conv-123",
            from_agent="agent",
            to_agent="user",
            message_type=MessageType.RESPONSE,
            content="Final response",
        )
        await console.log(msg3)

        # Check conversation retrieval
        messages = console.get_conversation("conv-123")
        assert len(messages) == 3

        # Check status
        status = console.get_status()
        assert status["total_messages"] == 3


class TestReasoningLoopIntegration:
    """Integration tests for reasoning loop (mocked Cortex)."""

    def setup_method(self):
        """Set up test fixtures."""
        self.temp_dir = Path(tempfile.mkdtemp())
        reset_context()

        # Create mocked components
        self.mock_query = MagicMock(return_value=[{"RESULT": '"Test response"'}])
        self.client = CortexClient(
            connection_id="test-conn",
            query_func=self.mock_query,
        )

        self.console = CommunicationConsole(
            enable_cli=False,
            enable_file=True,
            log_path=self.temp_dir / "console.jsonl",
        )

        self.context = CortexAgentContext(data_dir=self.temp_dir)
        self.config = CortexAgentConfig(
            connection_id="test-conn",
            max_reasoning_steps=5,
        )
        self.context.configure(self.config)

    def teardown_method(self):
        """Clean up."""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
        reset_context()

    def test_reasoning_loop_initialization(self):
        """Test reasoning loop can be initialized."""
        from src.cortex_agent.reasoning_loop import CortexReasoningLoop

        loop = CortexReasoningLoop(
            cortex=self.client,
            console=self.console,
            context=self.context,
            config=self.config,
        )

        assert loop is not None

    def test_reasoning_loop_run_sync(self):
        """Test synchronous reasoning loop execution."""
        from src.cortex_agent.reasoning_loop import CortexReasoningLoop

        # Mock responses for different phases
        responses = [
            # Observe
            [{"RESULT": '"Observation: This is a simple test goal"'}],
            # Plan
            [{"RESULT": '[{"step": 1, "action": "Execute test", "function": "COMPLETE"}]'}],
            # Execute step
            [{"RESULT": '"Step 1 executed successfully"'}],
            # Reflect
            [{"RESULT": '{"complete": true, "summary": "Done"}'}],
            # Synthesize
            [{"RESULT": '"Final result: Test completed"'}],
        ]
        self.mock_query.side_effect = responses

        loop = CortexReasoningLoop(
            cortex=self.client,
            console=self.console,
            context=self.context,
            config=self.config,
        )

        response = loop.run_sync("Test goal")

        assert response.goal == "Test goal"
        assert response.conversation_id is not None
        # May or may not succeed depending on mock responses
        assert response.total_cortex_calls > 0


class TestMCPToolsRegistration:
    """Test MCP tools can be registered."""

    def test_register_tools(self):
        """Test tool registration doesn't error."""
        # Create mock MCP
        mock_mcp = MagicMock()
        mock_mcp.tool = MagicMock(return_value=lambda f: f)

        # Create mock settings
        mock_settings = MagicMock()

        from src.cortex_agent.mcp_tools import register_cortex_agent_tools

        result = register_cortex_agent_tools(mock_mcp, mock_settings)

        assert result["tools_registered"] == 12
        assert "configuration" in result["categories"]
        assert "cortex_functions" in result["categories"]
        assert "reasoning" in result["categories"]
        assert "console" in result["categories"]


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
