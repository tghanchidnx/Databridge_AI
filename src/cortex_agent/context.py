"""
CortexAgentContext - State management for Cortex Agent.

Manages conversation state since Cortex functions are stateless.
Follows the UnifiedAgentContext pattern from the Unified AI Agent.

State is persisted to data/cortex_agent/context.json.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import uuid

from .types import (
    AgentMessage,
    AgentState,
    Conversation,
    CortexAgentConfig,
    ExecutionPlan,
    MessageType,
    ThinkingStep,
)

logger = logging.getLogger(__name__)


class CortexAgentContext:
    """State management for Cortex Agent."""

    def __init__(self, data_dir: Optional[Path] = None):
        """
        Initialize context.

        Args:
            data_dir: Directory for persisting state
        """
        self.data_dir = data_dir or Path("data/cortex_agent")
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self._conversations: Dict[str, Conversation] = {}
        self._config: Optional[CortexAgentConfig] = None
        self._active_conversation_id: Optional[str] = None

        # Load existing state
        self._load()

    def configure(self, config: CortexAgentConfig) -> None:
        """
        Set agent configuration.

        Args:
            config: Agent configuration
        """
        self._config = config
        self._save()
        logger.info(f"Cortex Agent configured with connection: {config.connection_id}")

    def get_config(self) -> Optional[CortexAgentConfig]:
        """Get current configuration."""
        return self._config

    def is_configured(self) -> bool:
        """Check if agent is configured."""
        return self._config is not None

    def start_conversation(self, goal: str) -> str:
        """
        Start a new conversation.

        Args:
            goal: The goal for this conversation

        Returns:
            conversation_id
        """
        conversation_id = str(uuid.uuid4())

        conversation = Conversation(
            conversation_id=conversation_id,
            goal=goal,
            started_at=datetime.now(),
            state=AgentState.IDLE,
            messages=[],
            thinking_steps=[],
        )

        self._conversations[conversation_id] = conversation
        self._active_conversation_id = conversation_id
        self._save()

        logger.info(f"Started conversation {conversation_id}: {goal[:50]}...")
        return conversation_id

    def get_conversation(self, conversation_id: str) -> Optional[Conversation]:
        """
        Get a conversation by ID.

        Args:
            conversation_id: The conversation ID

        Returns:
            Conversation or None
        """
        return self._conversations.get(conversation_id)

    def get_active_conversation(self) -> Optional[Conversation]:
        """Get the currently active conversation."""
        if self._active_conversation_id:
            return self._conversations.get(self._active_conversation_id)
        return None

    def list_conversations(
        self,
        limit: int = 10,
        state: Optional[AgentState] = None,
    ) -> List[Dict[str, Any]]:
        """
        List recent conversations.

        Args:
            limit: Maximum number to return
            state: Filter by state

        Returns:
            List of conversation summaries
        """
        conversations = list(self._conversations.values())

        if state:
            conversations = [c for c in conversations if c.state == state]

        # Sort by started_at descending
        conversations.sort(key=lambda c: c.started_at, reverse=True)

        return [
            {
                "conversation_id": c.conversation_id,
                "goal": c.goal[:100],
                "state": c.state.value,
                "started_at": c.started_at.isoformat(),
                "completed_at": c.completed_at.isoformat() if c.completed_at else None,
                "message_count": len(c.messages),
                "step_count": len(c.thinking_steps),
            }
            for c in conversations[:limit]
        ]

    def update_state(
        self,
        conversation_id: str,
        state: AgentState,
    ) -> None:
        """
        Update conversation state.

        Args:
            conversation_id: The conversation ID
            state: New state
        """
        conversation = self._conversations.get(conversation_id)
        if conversation:
            conversation.state = state
            if state == AgentState.COMPLETED:
                conversation.completed_at = datetime.now()
            self._save()

    def add_message(
        self,
        conversation_id: str,
        message: AgentMessage,
    ) -> None:
        """
        Add a message to a conversation.

        Args:
            conversation_id: The conversation ID
            message: The message to add
        """
        conversation = self._conversations.get(conversation_id)
        if conversation:
            conversation.messages.append(message)
            self._save()

    def add_thinking_step(
        self,
        conversation_id: str,
        step: ThinkingStep,
    ) -> None:
        """
        Add a thinking step to a conversation.

        Args:
            conversation_id: The conversation ID
            step: The thinking step
        """
        conversation = self._conversations.get(conversation_id)
        if conversation:
            conversation.thinking_steps.append(step)
            self._save()

    def set_plan(
        self,
        conversation_id: str,
        plan: ExecutionPlan,
    ) -> None:
        """
        Set the execution plan for a conversation.

        Args:
            conversation_id: The conversation ID
            plan: The execution plan
        """
        conversation = self._conversations.get(conversation_id)
        if conversation:
            conversation.plan = plan
            self._save()

    def set_result(
        self,
        conversation_id: str,
        result: str,
    ) -> None:
        """
        Set the final result for a conversation.

        Args:
            conversation_id: The conversation ID
            result: The final result
        """
        conversation = self._conversations.get(conversation_id)
        if conversation:
            conversation.final_result = result
            self._save()

    def get_scratchpad_context(self, conversation_id: str) -> str:
        """
        Format the conversation's thinking steps as context for Cortex prompts.

        Args:
            conversation_id: The conversation ID

        Returns:
            Formatted scratchpad string
        """
        conversation = self._conversations.get(conversation_id)
        if not conversation:
            return ""

        lines = ["=== SCRATCHPAD ==="]
        lines.append(f"Goal: {conversation.goal}")
        lines.append(f"State: {conversation.state.value}")
        lines.append("")

        if conversation.plan:
            lines.append("Plan:")
            for step in conversation.plan.steps:
                lines.append(f"  {step.step_number}. {step.description}")
            lines.append("")

        if conversation.thinking_steps:
            lines.append("Progress:")
            for step in conversation.thinking_steps:
                status = "✓" if step.cortex_result else "→"
                lines.append(f"  {status} Step {step.step_number}: {step.content[:80]}")
                if step.cortex_result:
                    result_preview = str(step.cortex_result)[:100]
                    lines.append(f"      Result: {result_preview}...")
            lines.append("")

        return "\n".join(lines)

    def get_stats(self) -> Dict[str, Any]:
        """Get context statistics."""
        total = len(self._conversations)
        by_state = {}
        for conv in self._conversations.values():
            state = conv.state.value
            by_state[state] = by_state.get(state, 0) + 1

        total_messages = sum(len(c.messages) for c in self._conversations.values())
        total_steps = sum(len(c.thinking_steps) for c in self._conversations.values())

        return {
            "total_conversations": total,
            "by_state": by_state,
            "total_messages": total_messages,
            "total_thinking_steps": total_steps,
            "is_configured": self.is_configured(),
            "active_conversation": self._active_conversation_id,
            "data_dir": str(self.data_dir),
        }

    def clear(self) -> None:
        """Clear all conversations (keeps config)."""
        self._conversations.clear()
        self._active_conversation_id = None
        self._save()
        logger.info("Cleared all conversations")

    def _save(self) -> None:
        """Persist state to disk."""
        state = {
            "config": self._config.model_dump() if self._config else None,
            "active_conversation_id": self._active_conversation_id,
            "conversations": {
                cid: conv.to_dict()
                for cid, conv in self._conversations.items()
            },
        }

        state_file = self.data_dir / "context.json"
        with open(state_file, "w") as f:
            json.dump(state, f, indent=2, default=str)

    def _load(self) -> None:
        """Load state from disk."""
        state_file = self.data_dir / "context.json"

        if not state_file.exists():
            return

        try:
            with open(state_file, "r") as f:
                state = json.load(f)

            # Load config
            if state.get("config"):
                self._config = CortexAgentConfig(**state["config"])

            # Load active conversation
            self._active_conversation_id = state.get("active_conversation_id")

            # Load conversations (simplified - just keep recent ones)
            for cid, conv_data in state.get("conversations", {}).items():
                try:
                    # Reconstruct conversation
                    conv = Conversation(
                        conversation_id=conv_data["conversation_id"],
                        goal=conv_data["goal"],
                        started_at=datetime.fromisoformat(conv_data["started_at"]),
                        completed_at=datetime.fromisoformat(conv_data["completed_at"]) if conv_data.get("completed_at") else None,
                        state=AgentState(conv_data["state"]),
                        final_result=conv_data.get("final_result"),
                    )

                    # Reconstruct messages
                    for msg_data in conv_data.get("messages", []):
                        conv.messages.append(AgentMessage.from_dict(msg_data))

                    # Reconstruct thinking steps
                    for step_data in conv_data.get("thinking_steps", []):
                        conv.thinking_steps.append(ThinkingStep(
                            step_number=step_data["step_number"],
                            phase=AgentState(step_data["phase"]),
                            content=step_data["content"],
                            cortex_query=step_data.get("cortex_query"),
                            cortex_result=step_data.get("cortex_result"),
                            duration_ms=step_data.get("duration_ms"),
                        ))

                    self._conversations[cid] = conv

                except Exception as e:
                    logger.warning(f"Failed to load conversation {cid}: {e}")

            logger.info(f"Loaded {len(self._conversations)} conversations from disk")

        except Exception as e:
            logger.warning(f"Failed to load context: {e}")


# Singleton instance
_context: Optional[CortexAgentContext] = None


def get_context() -> CortexAgentContext:
    """Get the singleton CortexAgentContext instance."""
    global _context
    if _context is None:
        _context = CortexAgentContext()
    return _context


def reset_context() -> None:
    """Reset the singleton context (for testing)."""
    global _context
    _context = None
