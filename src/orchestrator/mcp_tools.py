"""MCP Tools for AI Orchestrator Integration.

These tools enable the MCP server to interact with the AI Orchestrator layer,
allowing for task management, agent registration, and agent-to-agent messaging.

The orchestrator enables:
- Task submission with priorities and dependencies
- Agent registration with capabilities
- Agent-to-agent messaging (AI-Link-Orchestrator)
- Workflow creation and execution
- Event Bus publishing
"""

import json
import logging
import requests
from typing import Optional, Dict, Any, List
from datetime import datetime

logger = logging.getLogger("orchestrator_tools")


class OrchestratorClient:
    """HTTP client for communicating with the NestJS Orchestrator service."""

    def __init__(self, base_url: str, api_key: str, timeout: int = 30):
        """
        Initialize the orchestrator client.

        Args:
            base_url: NestJS backend URL (e.g., 'http://localhost:8001/api')
            api_key: API key for authentication
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip('/')
        self.api_key = api_key
        self.timeout = timeout
        self.headers = {
            'X-API-Key': api_key,
            'Content-Type': 'application/json',
        }
        self._agent_id = "mcp-server"

    def _request(
        self,
        method: str,
        endpoint: str,
        data: Optional[Dict] = None,
        params: Optional[Dict] = None,
    ) -> Dict[str, Any]:
        """Make an HTTP request to the orchestrator."""
        url = f"{self.base_url}{endpoint}"

        try:
            response = requests.request(
                method=method,
                url=url,
                headers=self.headers,
                json=data,
                params=params,
                timeout=self.timeout,
            )

            if response.status_code >= 400:
                return {
                    "error": True,
                    "status_code": response.status_code,
                    "message": response.text,
                }

            return response.json() if response.text else {"success": True}

        except requests.exceptions.ConnectionError:
            return {"error": True, "message": "Orchestrator not reachable"}
        except requests.exceptions.Timeout:
            return {"error": True, "message": "Request timed out"}
        except Exception as e:
            return {"error": True, "message": str(e)}

    # =========================================================================
    # Task Management
    # =========================================================================

    def submit_task(
        self,
        task_type: str,
        payload: Dict[str, Any],
        priority: str = "normal",
        dependencies: Optional[List[str]] = None,
        callback_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Submit a task to the orchestrator queue."""
        return self._request("POST", "/orchestrator/tasks", {
            "type": task_type,
            "payload": payload,
            "priority": priority,
            "dependencies": dependencies or [],
            "callbackUrl": callback_url,
        })

    def get_task_status(self, task_id: str) -> Dict[str, Any]:
        """Get the status of a task."""
        return self._request("GET", f"/orchestrator/tasks/{task_id}")

    def cancel_task(self, task_id: str) -> Dict[str, Any]:
        """Cancel a task."""
        return self._request("DELETE", f"/orchestrator/tasks/{task_id}")

    def list_tasks(
        self,
        status: Optional[str] = None,
        task_type: Optional[str] = None,
        limit: int = 20,
    ) -> Dict[str, Any]:
        """List tasks with optional filters."""
        params = {"limit": limit}
        if status:
            params["status"] = status
        if task_type:
            params["type"] = task_type
        return self._request("GET", "/orchestrator/tasks", params=params)

    # =========================================================================
    # Agent Registration
    # =========================================================================

    def register_agent(
        self,
        agent_id: str,
        name: str,
        agent_type: str,
        capabilities: List[Dict[str, Any]],
        max_concurrent_tasks: int = 5,
        callback_url: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Register an agent with the orchestrator."""
        return self._request("POST", "/orchestrator/agents/register", {
            "id": agent_id,
            "name": name,
            "type": agent_type,
            "capabilities": capabilities,
            "maxConcurrentTasks": max_concurrent_tasks,
            "callbackUrl": callback_url,
        })

    def unregister_agent(self, agent_id: str) -> Dict[str, Any]:
        """Unregister an agent."""
        return self._request("DELETE", f"/orchestrator/agents/{agent_id}")

    def send_heartbeat(
        self,
        agent_id: str,
        current_load: Optional[int] = None,
        health_status: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Send agent heartbeat."""
        data = {}
        if current_load is not None:
            data["currentLoad"] = current_load
        if health_status:
            data["healthStatus"] = health_status
        return self._request("POST", f"/orchestrator/agents/{agent_id}/heartbeat", data)

    def list_agents(
        self,
        agent_type: Optional[str] = None,
        capability: Optional[str] = None,
        healthy_only: bool = False,
    ) -> Dict[str, Any]:
        """List registered agents."""
        params = {}
        if agent_type:
            params["type"] = agent_type
        if capability:
            params["capability"] = capability
        if healthy_only:
            params["healthyOnly"] = "true"
        return self._request("GET", "/orchestrator/agents", params=params)

    def get_agent(self, agent_id: str) -> Dict[str, Any]:
        """Get agent details."""
        return self._request("GET", f"/orchestrator/agents/{agent_id}")

    # =========================================================================
    # Agent Messaging (AI-Link-Orchestrator)
    # =========================================================================

    def send_message(
        self,
        to_agent: str,
        message_type: str,
        payload: Dict[str, Any],
        conversation_id: Optional[str] = None,
        requires_response: bool = False,
        response_timeout: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Send a message to another agent."""
        data = {
            "fromAgent": self._agent_id,
            "toAgent": to_agent,
            "messageType": message_type,
            "payload": payload,
            "requiresResponse": requires_response,
        }
        if conversation_id:
            data["conversationId"] = conversation_id
        if response_timeout:
            data["responseTimeout"] = response_timeout
        return self._request("POST", "/orchestrator/messages", data)

    def get_messages(self, agent_id: str, limit: int = 50) -> Dict[str, Any]:
        """Get messages for an agent."""
        return self._request("GET", f"/orchestrator/messages/{agent_id}", params={"limit": limit})

    def list_conversations(
        self,
        agent_id: str,
        active_only: bool = True,
    ) -> Dict[str, Any]:
        """List conversations for an agent."""
        params = {"participant": agent_id}
        if active_only:
            params["activeOnly"] = "true"
        return self._request("GET", "/orchestrator/conversations", params=params)

    # =========================================================================
    # Workflow Management
    # =========================================================================

    def create_workflow(
        self,
        name: str,
        steps: List[Dict[str, Any]],
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a new workflow."""
        data = {
            "name": name,
            "steps": steps,
        }
        if description:
            data["description"] = description
        return self._request("POST", "/orchestrator/workflows", data)

    def list_workflows(self) -> Dict[str, Any]:
        """List all workflows."""
        return self._request("GET", "/orchestrator/workflows")

    def execute_workflow(
        self,
        workflow_id: str,
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Start workflow execution."""
        return self._request("POST", f"/orchestrator/workflows/{workflow_id}/execute", {
            "context": context or {},
        })

    def get_execution_status(self, execution_id: str) -> Dict[str, Any]:
        """Get workflow execution status."""
        return self._request("GET", f"/orchestrator/executions/{execution_id}")

    # =========================================================================
    # Event Publishing
    # =========================================================================

    def publish_event(
        self,
        channel: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Publish an event to the Event Bus."""
        return self._request("POST", "/orchestrator/events/publish", {
            "channel": channel,
            "payload": payload,
            "source": self._agent_id,
        })

    def get_health(self) -> Dict[str, Any]:
        """Get orchestrator health status."""
        return self._request("GET", "/orchestrator/health")


# Global client instance (initialized by register_orchestrator_tools)
_orchestrator_client: Optional[OrchestratorClient] = None


def register_orchestrator_tools(mcp, backend_url: str, api_key: str) -> OrchestratorClient:
    """
    Register orchestrator tools with the MCP server.

    Args:
        mcp: FastMCP server instance
        backend_url: NestJS backend URL
        api_key: API key for authentication

    Returns:
        OrchestratorClient instance
    """
    global _orchestrator_client
    _orchestrator_client = OrchestratorClient(backend_url, api_key)

    # =========================================================================
    # Task Management Tools
    # =========================================================================

    @mcp.tool()
    def submit_orchestrated_task(
        task_type: str,
        payload: str,
        priority: str = "normal",
        dependencies: str = "[]",
        callback_url: str = "",
    ) -> str:
        """
        Submit a task to the AI Orchestrator for managed execution.

        The orchestrator will:
        1. Queue the task based on priority (critical > high > normal > low)
        2. Wait for dependencies to complete
        3. Assign to an appropriate agent
        4. Track progress and handle failures

        Task types:
        - hierarchy_build: Build or update hierarchy structures
        - data_reconciliation: Compare and reconcile datasets
        - sql_analysis: Analyze SQL for hierarchy extraction
        - mapping_suggestion: Suggest source mappings
        - report_generation: Generate reports
        - deployment: Deploy hierarchies to databases
        - agent_handoff: Hand off work to another agent
        - workflow_step: Execute as part of a workflow
        - custom: Custom task type

        Args:
            task_type: Type of task to execute
            payload: JSON string with task-specific parameters
            priority: Task priority (low, normal, high, critical)
            dependencies: JSON array of task IDs that must complete first
            callback_url: Webhook URL for completion notification

        Returns:
            JSON with task ID, status, and queue position
        """
        try:
            payload_dict = json.loads(payload)
            deps_list = json.loads(dependencies) if dependencies else []

            result = _orchestrator_client.submit_task(
                task_type=task_type,
                payload=payload_dict,
                priority=priority,
                dependencies=deps_list,
                callback_url=callback_url if callback_url else None,
            )
            return json.dumps(result, indent=2, default=str)
        except json.JSONDecodeError as e:
            return json.dumps({"error": True, "message": f"Invalid JSON: {e}"})
        except Exception as e:
            return json.dumps({"error": True, "message": str(e)})

    @mcp.tool()
    def get_task_status(task_id: str) -> str:
        """
        Get the current status of an orchestrated task.

        Returns progress percentage, assigned agent, checkpoints,
        and partial results if available.

        Args:
            task_id: The task ID to check

        Returns:
            JSON with task status, progress (0-100), and details
        """
        result = _orchestrator_client.get_task_status(task_id)
        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    def list_orchestrator_tasks(
        status: str = "",
        task_type: str = "",
        limit: int = 20,
    ) -> str:
        """
        List tasks in the orchestrator queue.

        Args:
            status: Filter by status (pending, queued, in_progress, completed, failed)
            task_type: Filter by task type
            limit: Maximum number of tasks to return (default: 20)

        Returns:
            JSON with task list and counts by status
        """
        result = _orchestrator_client.list_tasks(
            status=status if status else None,
            task_type=task_type if task_type else None,
            limit=limit,
        )
        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    def cancel_orchestrated_task(task_id: str) -> str:
        """
        Cancel a pending or running task.

        Args:
            task_id: The task ID to cancel

        Returns:
            JSON with cancellation result
        """
        result = _orchestrator_client.cancel_task(task_id)
        return json.dumps(result, indent=2, default=str)

    # =========================================================================
    # Agent Registration Tools
    # =========================================================================

    @mcp.tool()
    def register_agent(
        agent_id: str,
        name: str,
        agent_type: str,
        capabilities: str,
        max_concurrent_tasks: int = 5,
        callback_url: str = "",
    ) -> str:
        """
        Register an external agent with the orchestrator.

        Used by Excel plugins, Power BI connectors, and external AI agents
        to announce their presence and capabilities.

        Agent types:
        - mcp_native: Direct MCP tool access (Claude Code, etc.)
        - llm_agent: Claude/GPT with tool calling
        - specialized: Domain-specific agents (FP&A, DBA)
        - excel_plugin: Excel Add-in client
        - power_bi: Power BI connector
        - external: Third-party integrations

        Args:
            agent_id: Unique identifier for the agent
            name: Human-readable agent name
            agent_type: Type of agent
            capabilities: JSON array of capability objects [{tool, proficiency, constraints}]
            max_concurrent_tasks: Maximum concurrent tasks (default: 5)
            callback_url: Webhook URL for receiving messages

        Returns:
            JSON with registration confirmation and agent details
        """
        try:
            caps_list = json.loads(capabilities)
            result = _orchestrator_client.register_agent(
                agent_id=agent_id,
                name=name,
                agent_type=agent_type,
                capabilities=caps_list,
                max_concurrent_tasks=max_concurrent_tasks,
                callback_url=callback_url if callback_url else None,
            )
            return json.dumps(result, indent=2, default=str)
        except json.JSONDecodeError as e:
            return json.dumps({"error": True, "message": f"Invalid capabilities JSON: {e}"})
        except Exception as e:
            return json.dumps({"error": True, "message": str(e)})

    @mcp.tool()
    def list_registered_agents(
        agent_type: str = "",
        capability: str = "",
        healthy_only: bool = False,
    ) -> str:
        """
        List agents registered with the orchestrator.

        Args:
            agent_type: Filter by agent type
            capability: Filter by capability (tool name)
            healthy_only: Only return healthy agents

        Returns:
            JSON with agent list and health statistics
        """
        result = _orchestrator_client.list_agents(
            agent_type=agent_type if agent_type else None,
            capability=capability if capability else None,
            healthy_only=healthy_only,
        )
        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    def get_agent_details(agent_id: str) -> str:
        """
        Get details of a registered agent.

        Args:
            agent_id: The agent ID to look up

        Returns:
            JSON with agent details, capabilities, and health status
        """
        result = _orchestrator_client.get_agent(agent_id)
        return json.dumps(result, indent=2, default=str)

    # =========================================================================
    # Agent Messaging Tools (AI-Link-Orchestrator)
    # =========================================================================

    @mcp.tool()
    def send_agent_message(
        to_agent: str,
        message_type: str,
        payload: str,
        conversation_id: str = "",
        requires_response: bool = False,
        response_timeout: int = 30000,
    ) -> str:
        """
        Send a message to another agent via the AI-Link-Orchestrator.

        Use this for agent-to-agent communication including task handoffs,
        queries, status updates, and data sharing.

        Message types:
        - task_handoff: Pass task to another agent with context
        - query: Ask another agent for information
        - response: Reply to a query
        - status_update: Notify of progress
        - error: Escalate an error
        - approval_request: Request human approval
        - data_share: Share intermediate results

        Args:
            to_agent: Target agent ID or '*' for broadcast
            message_type: Type of message
            payload: JSON object with message content
            conversation_id: Existing conversation ID to continue (optional)
            requires_response: Whether to wait for response
            response_timeout: Response timeout in milliseconds (default: 30000)

        Returns:
            JSON with message ID, delivery status, and response if requested
        """
        try:
            payload_dict = json.loads(payload)
            result = _orchestrator_client.send_message(
                to_agent=to_agent,
                message_type=message_type,
                payload=payload_dict,
                conversation_id=conversation_id if conversation_id else None,
                requires_response=requires_response,
                response_timeout=response_timeout if requires_response else None,
            )
            return json.dumps(result, indent=2, default=str)
        except json.JSONDecodeError as e:
            return json.dumps({"error": True, "message": f"Invalid payload JSON: {e}"})
        except Exception as e:
            return json.dumps({"error": True, "message": str(e)})

    @mcp.tool()
    def get_agent_messages(agent_id: str, limit: int = 50) -> str:
        """
        Get messages sent to an agent.

        Args:
            agent_id: The agent ID to get messages for
            limit: Maximum messages to return (default: 50)

        Returns:
            JSON with message list
        """
        result = _orchestrator_client.get_messages(agent_id, limit)
        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    def list_agent_conversations(agent_id: str, active_only: bool = True) -> str:
        """
        List conversations an agent is participating in.

        Args:
            agent_id: The agent ID
            active_only: Only return active conversations (default: True)

        Returns:
            JSON with conversation list
        """
        result = _orchestrator_client.list_conversations(agent_id, active_only)
        return json.dumps(result, indent=2, default=str)

    # =========================================================================
    # Workflow Tools
    # =========================================================================

    @mcp.tool()
    def create_orchestrator_workflow(
        name: str,
        steps: str,
        description: str = "",
    ) -> str:
        """
        Create a multi-step workflow for the orchestrator to execute.

        Workflows can include:
        - Sequential steps with dependencies
        - Parallel execution branches
        - Conditional logic
        - Human approval gates
        - Automatic retries

        Step types:
        - task: Execute an orchestrator task
        - parallel: Run multiple tasks in parallel
        - conditional: Branch based on condition
        - approval: Wait for human approval
        - wait: Pause for specified duration

        Args:
            name: Workflow name
            steps: JSON array of workflow step definitions
            description: Optional workflow description

        Returns:
            JSON with workflow ID and validation results
        """
        try:
            steps_list = json.loads(steps)
            result = _orchestrator_client.create_workflow(
                name=name,
                steps=steps_list,
                description=description if description else None,
            )
            return json.dumps(result, indent=2, default=str)
        except json.JSONDecodeError as e:
            return json.dumps({"error": True, "message": f"Invalid steps JSON: {e}"})
        except Exception as e:
            return json.dumps({"error": True, "message": str(e)})

    @mcp.tool()
    def execute_orchestrator_workflow(
        workflow_id: str,
        context: str = "{}",
    ) -> str:
        """
        Start executing a workflow.

        Args:
            workflow_id: The workflow ID to execute
            context: JSON object with initial context variables

        Returns:
            JSON with execution ID and initial status
        """
        try:
            context_dict = json.loads(context) if context else {}
            result = _orchestrator_client.execute_workflow(workflow_id, context_dict)
            return json.dumps(result, indent=2, default=str)
        except json.JSONDecodeError as e:
            return json.dumps({"error": True, "message": f"Invalid context JSON: {e}"})
        except Exception as e:
            return json.dumps({"error": True, "message": str(e)})

    @mcp.tool()
    def list_orchestrator_workflows() -> str:
        """
        List all defined workflows.

        Returns:
            JSON with workflow list
        """
        result = _orchestrator_client.list_workflows()
        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    def get_workflow_execution_status(execution_id: str) -> str:
        """
        Get the status of a workflow execution.

        Args:
            execution_id: The execution ID to check

        Returns:
            JSON with execution status, current step, and results
        """
        result = _orchestrator_client.get_execution_status(execution_id)
        return json.dumps(result, indent=2, default=str)

    # =========================================================================
    # Event Bus Tools
    # =========================================================================

    @mcp.tool()
    def publish_orchestrator_event(channel: str, payload: str) -> str:
        """
        Publish an event to the orchestrator Event Bus.

        This allows notifying other agents and services of changes.

        Common channels:
        - hierarchy.updated: Hierarchy was modified
        - hierarchy.deployed: Hierarchy was deployed
        - task.completed: A task finished
        - sync.required: Synchronization needed

        Args:
            channel: Event channel name
            payload: JSON object with event data

        Returns:
            JSON with publication result
        """
        try:
            payload_dict = json.loads(payload)
            result = _orchestrator_client.publish_event(channel, payload_dict)
            return json.dumps(result, indent=2, default=str)
        except json.JSONDecodeError as e:
            return json.dumps({"error": True, "message": f"Invalid payload JSON: {e}"})
        except Exception as e:
            return json.dumps({"error": True, "message": str(e)})

    @mcp.tool()
    def get_orchestrator_health() -> str:
        """
        Get the health status of the orchestrator.

        Returns:
            JSON with orchestrator health, agent counts, and task statistics
        """
        result = _orchestrator_client.get_health()
        return json.dumps(result, indent=2, default=str)

    logger.info("Orchestrator tools registered successfully")
    return _orchestrator_client
