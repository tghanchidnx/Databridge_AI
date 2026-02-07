"""
CortexReasoningLoop - Orchestrated reasoning loop for complex tasks.

Implements the Observe → Plan → Execute → Reflect pattern:
1. OBSERVE: Analyze goal and current state
2. PLAN: Use Cortex to create execution plan
3. EXECUTE: Run each step via Cortex SQL
4. UPDATE: Update internal scratchpad
5. REFLECT: Check if goal is complete, repeat if needed
6. SYNTHESIZE: Combine results into final response

This simulates a conversation with Cortex by maintaining state
and using the scratchpad as context for each Cortex call.
"""

import json
import logging
import time
import uuid
from typing import Any, Dict, List, Optional

from .console import CommunicationConsole
from .context import CortexAgentContext
from .cortex_client import CortexClient
from .types import (
    AgentResponse,
    AgentState,
    CortexAgentConfig,
    CortexFunction,
    ExecutionPlan,
    MessageType,
    PlanStep,
    StepResult,
    ThinkingStep,
)

logger = logging.getLogger(__name__)


class CortexReasoningLoop:
    """Orchestrated reasoning loop that simulates conversation with Cortex."""

    # Prompt templates for each phase
    OBSERVE_PROMPT = """You are analyzing a task to understand what needs to be done.

GOAL: {goal}

CONTEXT:
{context}

Analyze this goal and provide:
1. What is being asked
2. What information is available
3. What steps might be needed
4. Any potential challenges

Provide your analysis in a structured format."""

    PLAN_PROMPT = """You are creating an execution plan for a task.

GOAL: {goal}

OBSERVATIONS:
{observations}

AVAILABLE CORTEX FUNCTIONS:
- COMPLETE(prompt): Generate text, analyze data, make decisions
- SUMMARIZE(text): Summarize long text
- SENTIMENT(text): Analyze sentiment (-1 to 1)
- TRANSLATE(text, from, to): Translate between languages
- EXTRACT_ANSWER(context, question): Extract specific answers

Create a step-by-step plan. For each step specify:
1. Step number
2. Action description
3. Which Cortex function to use (if any)
4. What parameters to pass

Format as JSON array:
[
  {{"step": 1, "action": "description", "function": "COMPLETE", "parameters": {{"prompt": "..."}}}},
  ...
]

Keep the plan focused and efficient. Maximum 5 steps."""

    EXECUTE_PROMPT = """You are executing step {step_number} of a plan.

GOAL: {goal}

PLAN STEP: {step_description}

PREVIOUS RESULTS:
{previous_results}

Execute this step. Provide a clear, actionable result."""

    REFLECT_PROMPT = """You are evaluating if a goal has been achieved.

GOAL: {goal}

EXECUTION RESULTS:
{results}

Evaluate:
1. Has the goal been fully achieved? (yes/no)
2. What was accomplished?
3. Is anything still needed?

Respond with JSON: {{"complete": true/false, "summary": "...", "remaining": "..."}}"""

    SYNTHESIZE_PROMPT = """You are synthesizing the final response for a completed task.

GOAL: {goal}

EXECUTION SUMMARY:
{summary}

ALL RESULTS:
{all_results}

Provide a clear, comprehensive final response that:
1. Summarizes what was accomplished
2. Presents the key results
3. Notes any important observations

Keep it concise but complete."""

    def __init__(
        self,
        cortex: CortexClient,
        console: CommunicationConsole,
        context: CortexAgentContext,
        config: CortexAgentConfig,
    ):
        """
        Initialize the reasoning loop.

        Args:
            cortex: CortexClient for Cortex function calls
            console: CommunicationConsole for observability
            context: CortexAgentContext for state management
            config: Agent configuration
        """
        self.cortex = cortex
        self.console = console
        self.context = context
        self.config = config
        self._step_count = 0
        self._cortex_call_count = 0

    async def run(
        self,
        goal: str,
        initial_context: Optional[Dict[str, Any]] = None,
    ) -> AgentResponse:
        """
        Run the reasoning loop for a goal.

        Args:
            goal: The goal to achieve
            initial_context: Optional initial context data

        Returns:
            AgentResponse with results and thinking steps
        """
        start_time = time.time()
        self._step_count = 0
        self._cortex_call_count = 0

        # Start conversation
        conversation_id = self.context.start_conversation(goal)
        self.console.log_request(conversation_id, goal)

        try:
            # 1. OBSERVE
            self.context.update_state(conversation_id, AgentState.OBSERVING)
            observations = await self._observe(goal, initial_context, conversation_id)

            # 2. PLAN
            self.context.update_state(conversation_id, AgentState.PLANNING)
            plan = await self._create_plan(goal, observations, conversation_id)
            self.context.set_plan(conversation_id, plan)

            # 3-5. EXECUTE → UPDATE → REFLECT loop
            all_results: List[StepResult] = []
            for step in plan.steps:
                if self._step_count >= self.config.max_reasoning_steps:
                    logger.warning(f"Reached max reasoning steps ({self.config.max_reasoning_steps})")
                    break

                # EXECUTE
                self.context.update_state(conversation_id, AgentState.EXECUTING)
                result = await self._execute_step(step, goal, all_results, conversation_id)
                all_results.append(result)

                # UPDATE scratchpad
                self._update_scratchpad(step, result, conversation_id)

                # REFLECT
                self.context.update_state(conversation_id, AgentState.REFLECTING)
                if await self._is_goal_complete(goal, all_results, conversation_id):
                    break

            # 6. SYNTHESIZE
            self.context.update_state(conversation_id, AgentState.SYNTHESIZING)
            final_result = await self._synthesize(goal, all_results, conversation_id)

            # Complete
            self.context.update_state(conversation_id, AgentState.COMPLETED)
            self.context.set_result(conversation_id, final_result)

            total_duration = int((time.time() - start_time) * 1000)

            # Get thinking steps from context
            conversation = self.context.get_conversation(conversation_id)
            thinking_steps = conversation.thinking_steps if conversation else []

            response = AgentResponse(
                conversation_id=conversation_id,
                goal=goal,
                success=True,
                result=final_result,
                thinking_steps=thinking_steps,
                total_cortex_calls=self._cortex_call_count,
                total_duration_ms=total_duration,
                plan=plan,
            )

            self.console.log_response(
                conversation_id,
                final_result,
                metadata={"duration_ms": total_duration, "cortex_calls": self._cortex_call_count},
            )

            return response

        except Exception as e:
            logger.error(f"Reasoning loop failed: {e}")
            self.context.update_state(conversation_id, AgentState.ERROR)
            self.console.log_error(conversation_id, f"Reasoning loop failed: {str(e)}", str(e))

            total_duration = int((time.time() - start_time) * 1000)
            conversation = self.context.get_conversation(conversation_id)

            return AgentResponse(
                conversation_id=conversation_id,
                goal=goal,
                success=False,
                result="",
                thinking_steps=conversation.thinking_steps if conversation else [],
                total_cortex_calls=self._cortex_call_count,
                total_duration_ms=total_duration,
                error=str(e),
            )

    async def _observe(
        self,
        goal: str,
        context: Optional[Dict[str, Any]],
        conversation_id: str,
    ) -> str:
        """
        Observe phase: Analyze goal and context.

        Args:
            goal: The goal to analyze
            context: Initial context
            conversation_id: Current conversation

        Returns:
            Observation analysis
        """
        self._step_count += 1

        context_str = json.dumps(context, indent=2) if context else "No additional context provided."

        prompt = self.OBSERVE_PROMPT.format(
            goal=goal,
            context=context_str,
        )

        self.console.log_thinking(
            conversation_id,
            f"Observing: Analyzing goal and context",
            thinking=f"Goal: {goal}\nContext: {context_str[:200]}...",
        )

        result = self.cortex.complete(prompt)
        self._cortex_call_count += 1

        observations = result.result if result.success else "Failed to analyze goal."

        # Record thinking step
        step = ThinkingStep(
            step_number=self._step_count,
            phase=AgentState.OBSERVING,
            content=f"Analyzed goal: {goal[:50]}...",
            cortex_function=CortexFunction.COMPLETE,
            cortex_query=result.query,
            cortex_result=str(observations)[:500],
            duration_ms=result.duration_ms,
        )
        self.context.add_thinking_step(conversation_id, step)

        return str(observations)

    async def _create_plan(
        self,
        goal: str,
        observations: str,
        conversation_id: str,
    ) -> ExecutionPlan:
        """
        Plan phase: Create execution plan using Cortex.

        Args:
            goal: The goal
            observations: Observation analysis
            conversation_id: Current conversation

        Returns:
            ExecutionPlan
        """
        self._step_count += 1

        prompt = self.PLAN_PROMPT.format(
            goal=goal,
            observations=observations,
        )

        self.console.log_thinking(
            conversation_id,
            "Planning: Creating execution plan",
            thinking=f"Based on observations:\n{observations[:300]}...",
        )

        result = self.cortex.complete(prompt)
        self._cortex_call_count += 1

        # Parse plan from response
        plan_steps = self._parse_plan(result.result if result.success else "[]")

        plan = ExecutionPlan(
            plan_id=str(uuid.uuid4()),
            goal=goal,
            steps=plan_steps,
            estimated_steps=len(plan_steps),
            confidence=0.8 if plan_steps else 0.0,
            reasoning=str(result.result)[:500] if result.success else "Failed to create plan",
        )

        # Record thinking step
        step = ThinkingStep(
            step_number=self._step_count,
            phase=AgentState.PLANNING,
            content=f"Created plan with {len(plan_steps)} steps",
            cortex_function=CortexFunction.COMPLETE,
            cortex_query=result.query,
            cortex_result=json.dumps([s.to_dict() for s in plan_steps]),
            duration_ms=result.duration_ms,
        )
        self.context.add_thinking_step(conversation_id, step)

        # Log plan
        plan_summary = "\n".join([f"  {s.step_number}. {s.description}" for s in plan_steps])
        self.console.log_plan(
            conversation_id,
            f"Execution Plan:\n{plan_summary}",
            metadata={"steps": len(plan_steps)},
        )

        return plan

    def _parse_plan(self, plan_response: Any) -> List[PlanStep]:
        """Parse plan steps from Cortex response."""
        try:
            # Try to extract JSON from response
            response_str = str(plan_response)

            # Find JSON array in response
            start = response_str.find("[")
            end = response_str.rfind("]") + 1

            if start >= 0 and end > start:
                json_str = response_str[start:end]
                steps_data = json.loads(json_str)

                steps = []
                for i, step_data in enumerate(steps_data):
                    step_num = step_data.get("step", i + 1)
                    action = step_data.get("action", "Unknown action")
                    func_name = step_data.get("function", "")
                    params = step_data.get("parameters", {})

                    cortex_func = None
                    if func_name:
                        try:
                            cortex_func = CortexFunction(func_name.upper())
                        except ValueError:
                            pass

                    steps.append(PlanStep(
                        step_number=step_num,
                        action=action,
                        description=action,
                        cortex_function=cortex_func,
                        parameters=params,
                    ))

                return steps

        except (json.JSONDecodeError, ValueError) as e:
            logger.warning(f"Failed to parse plan: {e}")

        # Default single step plan
        return [PlanStep(
            step_number=1,
            action="Execute goal directly",
            description="Execute the goal using COMPLETE",
            cortex_function=CortexFunction.COMPLETE,
            parameters={"prompt": "Execute this goal"},
        )]

    async def _execute_step(
        self,
        step: PlanStep,
        goal: str,
        previous_results: List[StepResult],
        conversation_id: str,
    ) -> StepResult:
        """
        Execute a single plan step.

        Args:
            step: The step to execute
            goal: Original goal
            previous_results: Results from previous steps
            conversation_id: Current conversation

        Returns:
            StepResult
        """
        self._step_count += 1
        start_time = time.time()

        # Format previous results for context
        prev_results_str = ""
        for prev in previous_results:
            prev_results_str += f"Step {prev.step_number}: {str(prev.result)[:200]}\n"

        self.console.log_execution(
            conversation_id,
            f"Executing step {step.step_number}: {step.description}",
        )

        try:
            # Execute based on Cortex function
            if step.cortex_function == CortexFunction.COMPLETE:
                prompt = step.parameters.get("prompt", self.EXECUTE_PROMPT.format(
                    step_number=step.step_number,
                    goal=goal,
                    step_description=step.description,
                    previous_results=prev_results_str or "None yet",
                ))
                result = self.cortex.complete(prompt)

            elif step.cortex_function == CortexFunction.SUMMARIZE:
                text = step.parameters.get("text", prev_results_str)
                result = self.cortex.summarize(text)

            elif step.cortex_function == CortexFunction.SENTIMENT:
                text = step.parameters.get("text", "")
                result = self.cortex.sentiment(text)

            elif step.cortex_function == CortexFunction.TRANSLATE:
                text = step.parameters.get("text", "")
                from_lang = step.parameters.get("from", "en")
                to_lang = step.parameters.get("to", "es")
                result = self.cortex.translate(text, from_lang, to_lang)

            elif step.cortex_function == CortexFunction.EXTRACT_ANSWER:
                context = step.parameters.get("context", prev_results_str)
                question = step.parameters.get("question", goal)
                result = self.cortex.extract_answer(context, question)

            else:
                # Default to COMPLETE
                prompt = self.EXECUTE_PROMPT.format(
                    step_number=step.step_number,
                    goal=goal,
                    step_description=step.description,
                    previous_results=prev_results_str or "None yet",
                )
                result = self.cortex.complete(prompt)

            self._cortex_call_count += 1
            duration_ms = int((time.time() - start_time) * 1000)

            step_result = StepResult(
                step_number=step.step_number,
                success=result.success,
                result=result.result,
                error=result.error,
                cortex_query=result.query,
                cortex_raw_result=result.raw_response,
                duration_ms=duration_ms,
            )

            # Record thinking step
            thinking_step = ThinkingStep(
                step_number=self._step_count,
                phase=AgentState.EXECUTING,
                content=f"Executed: {step.description}",
                cortex_function=step.cortex_function or CortexFunction.COMPLETE,
                cortex_query=result.query,
                cortex_result=str(result.result)[:500] if result.result else None,
                duration_ms=duration_ms,
            )
            self.context.add_thinking_step(conversation_id, thinking_step)

            self.console.log_execution(
                conversation_id,
                f"Step {step.step_number} completed",
                cortex_query=result.query,
                cortex_result=str(result.result)[:200] if result.result else None,
            )

            return step_result

        except Exception as e:
            duration_ms = int((time.time() - start_time) * 1000)
            logger.error(f"Step {step.step_number} failed: {e}")

            return StepResult(
                step_number=step.step_number,
                success=False,
                result=None,
                error=str(e),
                duration_ms=duration_ms,
            )

    def _update_scratchpad(
        self,
        step: PlanStep,
        result: StepResult,
        conversation_id: str,
    ) -> None:
        """Update scratchpad with step result (already done via thinking steps)."""
        # The scratchpad is automatically maintained via context.add_thinking_step
        pass

    async def _is_goal_complete(
        self,
        goal: str,
        results: List[StepResult],
        conversation_id: str,
    ) -> bool:
        """
        Reflect phase: Check if goal is complete.

        Args:
            goal: The goal
            results: Results so far
            conversation_id: Current conversation

        Returns:
            True if goal is complete
        """
        self._step_count += 1

        # Format results
        results_str = ""
        for r in results:
            status = "✓" if r.success else "✗"
            results_str += f"Step {r.step_number} [{status}]: {str(r.result)[:300]}\n"

        prompt = self.REFLECT_PROMPT.format(
            goal=goal,
            results=results_str,
        )

        self.console.log_thinking(
            conversation_id,
            "Reflecting: Evaluating if goal is complete",
        )

        result = self.cortex.complete(prompt)
        self._cortex_call_count += 1

        # Parse reflection
        is_complete = False
        try:
            response_str = str(result.result)
            # Try to find JSON
            start = response_str.find("{")
            end = response_str.rfind("}") + 1
            if start >= 0 and end > start:
                reflection = json.loads(response_str[start:end])
                is_complete = reflection.get("complete", False)
        except (json.JSONDecodeError, ValueError):
            # If we can't parse, assume complete if all steps succeeded
            is_complete = all(r.success for r in results)

        # Record thinking step
        step = ThinkingStep(
            step_number=self._step_count,
            phase=AgentState.REFLECTING,
            content=f"Goal complete: {is_complete}",
            cortex_function=CortexFunction.COMPLETE,
            cortex_query=result.query,
            cortex_result=str(result.result)[:200] if result.result else None,
            duration_ms=result.duration_ms,
        )
        self.context.add_thinking_step(conversation_id, step)

        return is_complete

    async def _synthesize(
        self,
        goal: str,
        results: List[StepResult],
        conversation_id: str,
    ) -> str:
        """
        Synthesize phase: Combine results into final response.

        Args:
            goal: The goal
            results: All step results
            conversation_id: Current conversation

        Returns:
            Final synthesized response
        """
        self._step_count += 1

        # Create summary of results
        summary_parts = []
        for r in results:
            status = "completed successfully" if r.success else "failed"
            summary_parts.append(f"Step {r.step_number} {status}")

        summary = "; ".join(summary_parts)

        # Format all results
        all_results = ""
        for r in results:
            all_results += f"=== Step {r.step_number} ===\n{str(r.result)}\n\n"

        prompt = self.SYNTHESIZE_PROMPT.format(
            goal=goal,
            summary=summary,
            all_results=all_results,
        )

        self.console.log_thinking(
            conversation_id,
            "Synthesizing: Creating final response",
        )

        result = self.cortex.complete(prompt)
        self._cortex_call_count += 1

        final_result = str(result.result) if result.success else f"Task completed with {len(results)} steps. {summary}"

        # Record thinking step
        step = ThinkingStep(
            step_number=self._step_count,
            phase=AgentState.SYNTHESIZING,
            content="Synthesized final response",
            cortex_function=CortexFunction.COMPLETE,
            cortex_query=result.query,
            cortex_result=final_result[:500],
            duration_ms=result.duration_ms,
        )
        self.context.add_thinking_step(conversation_id, step)

        return final_result

    def run_sync(
        self,
        goal: str,
        initial_context: Optional[Dict[str, Any]] = None,
    ) -> AgentResponse:
        """
        Synchronous version of run for non-async contexts.

        Args:
            goal: The goal to achieve
            initial_context: Optional initial context

        Returns:
            AgentResponse
        """
        import asyncio

        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Create a new loop in a thread
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(
                        asyncio.run,
                        self.run(goal, initial_context)
                    )
                    return future.result()
            else:
                return loop.run_until_complete(self.run(goal, initial_context))
        except RuntimeError:
            return asyncio.run(self.run(goal, initial_context))
