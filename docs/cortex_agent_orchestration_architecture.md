# Cortex AI Agent Orchestration Architecture

> **Purpose:** Design a multi-agent communication system where DataBridge agents communicate with Cortex AI back-and-forth, orchestrated by a central AI agent, with full observability through a communication console.

---

## Executive Summary

This document outlines the architecture for a sophisticated multi-agent orchestration system that:

1. **Enables agent-to-agent communication** between DataBridge and Cortex AI
2. **Orchestrates exchanges** through a central monitoring agent
3. **Implements chain-of-thought reasoning** similar to Claude/Gemini's thinking process
4. **Provides full observability** through a communication console
5. **Streams logs** to console, webpage, and database in real-time

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Agent Communication Protocol](#agent-communication-protocol)
3. [Orchestrator Agent Design](#orchestrator-agent-design)
4. [Communication Console](#communication-console)
5. [Implementation Plan](#implementation-plan)
6. [Code Examples](#code-examples)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           USER INTERFACE LAYER                                   │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────────────┐  │
│  │ CLI Console     │  │ Web Dashboard   │  │ REST API                        │  │
│  │ (Real-time)     │  │ (WebSocket)     │  │ (Polling/SSE)                   │  │
│  └────────┬────────┘  └────────┬────────┘  └───────────────┬─────────────────┘  │
│           │                    │                           │                     │
│           └────────────────────┼───────────────────────────┘                     │
│                                │                                                  │
│                                ▼                                                  │
│  ┌─────────────────────────────────────────────────────────────────────────────┐│
│  │                    COMMUNICATION CONSOLE                                     ││
│  │  ┌────────────────┐  ┌────────────────┐  ┌────────────────────────────────┐ ││
│  │  │ Log Stream     │  │ Status Monitor │  │ Conversation History           │ ││
│  │  │ (stdout/file)  │  │ (agent states) │  │ (full context)                 │ ││
│  │  └────────────────┘  └────────────────┘  └────────────────────────────────┘ ││
│  └─────────────────────────────────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                        ORCHESTRATOR AGENT LAYER                                   │
│                                                                                   │
│  ┌─────────────────────────────────────────────────────────────────────────────┐ │
│  │                      OrchestratorAgent                                       │ │
│  │                                                                              │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐│ │
│  │  │ Think()      │  │ Plan()       │  │ Clarify()    │  │ Synthesize()     ││ │
│  │  │ Chain-of-    │  │ Create step  │  │ Ask follow-  │  │ Combine results  ││ │
│  │  │ thought      │  │ by step plan │  │ up questions │  │ for user         ││ │
│  │  └──────────────┘  └──────────────┘  └──────────────┘  └──────────────────┘│ │
│  │                                                                              │ │
│  │  ┌──────────────────────────────────────────────────────────────────────┐   │ │
│  │  │                    Message Router                                     │   │ │
│  │  │  Routes messages between agents, logs all exchanges                   │   │ │
│  │  └──────────────────────────────────────────────────────────────────────┘   │ │
│  └─────────────────────────────────────────────────────────────────────────────┘ │
│                                        │                                          │
└────────────────────────────────────────┼──────────────────────────────────────────┘
                                         │
              ┌──────────────────────────┼──────────────────────────┐
              │                          │                          │
              ▼                          ▼                          ▼
┌─────────────────────────┐ ┌─────────────────────────┐ ┌─────────────────────────┐
│   DataBridge Agents     │ │   Cortex AI Agents      │ │   Specialist Agents     │
│                         │ │                         │ │                         │
│ ┌─────────────────────┐ │ │ ┌─────────────────────┐ │ │ ┌─────────────────────┐ │
│ │ HierarchyBuilder    │ │ │ │ CortexAnalyst       │ │ │ │ DataValidator       │ │
│ │ Agent               │ │ │ │ Agent               │ │ │ │ Agent               │ │
│ └─────────────────────┘ │ │ └─────────────────────┘ │ │ └─────────────────────┘ │
│ ┌─────────────────────┐ │ │ ┌─────────────────────┐ │ │ ┌─────────────────────┐ │
│ │ MappingExpert       │ │ │ │ CortexLLM           │ │ │ │ ReconciliationAgent │ │
│ │ Agent               │ │ │ │ Agent               │ │ │ │                     │ │
│ └─────────────────────┘ │ │ └─────────────────────┘ │ │ └─────────────────────┘ │
│ ┌─────────────────────┐ │ │ ┌─────────────────────┐ │ │ ┌─────────────────────┐ │
│ │ SQLTranslator       │ │ │ │ CortexSearch        │ │ │ │ RecommendationAgent │ │
│ │ Agent               │ │ │ │ Agent               │ │ │ │                     │ │
│ └─────────────────────┘ │ │ └─────────────────────┘ │ │ └─────────────────────┘ │
└─────────────────────────┘ └─────────────────────────┘ └─────────────────────────┘
              │                          │                          │
              └──────────────────────────┼──────────────────────────┘
                                         │
                                         ▼
┌──────────────────────────────────────────────────────────────────────────────────┐
│                           PERSISTENCE LAYER                                       │
│                                                                                   │
│  ┌─────────────────────┐  ┌─────────────────────┐  ┌──────────────────────────┐  │
│  │ Redis               │  │ SQLite/PostgreSQL   │  │ File System              │  │
│  │ (Real-time Pub/Sub) │  │ (Conversation DB)   │  │ (Log Files)              │  │
│  └─────────────────────┘  └─────────────────────┘  └──────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────────┘
```

---

## Agent Communication Protocol

### Message Format

Every agent communication follows a structured message format:

```python
@dataclass
class AgentMessage:
    """Standard message format for agent-to-agent communication."""
    id: str                      # Unique message ID (UUID)
    conversation_id: str         # Groups related messages
    timestamp: datetime          # When message was created
    from_agent: str              # Source agent ID
    to_agent: str                # Destination agent ID
    message_type: MessageType    # REQUEST, RESPONSE, THINKING, CLARIFICATION, etc.
    content: str                 # The actual message content
    metadata: Dict[str, Any]     # Additional context (tokens, latency, etc.)
    parent_id: Optional[str]     # For threading/reply chains
    thinking: Optional[str]      # Chain-of-thought reasoning (visible in console)
    status: MessageStatus        # PENDING, PROCESSING, COMPLETED, FAILED

class MessageType(Enum):
    REQUEST = "request"              # Initial request from user or agent
    RESPONSE = "response"            # Response to a request
    THINKING = "thinking"            # Chain-of-thought reasoning step
    PLAN = "plan"                    # Proposed plan of action
    CLARIFICATION = "clarification"  # Follow-up question
    OBSERVATION = "observation"      # Intermediate observation
    SYNTHESIS = "synthesis"          # Final synthesized answer
    ERROR = "error"                  # Error message
    STATUS = "status"                # Status update
```

### Communication Flow

```
User Query
    │
    ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATOR AGENT                            │
│                                                                  │
│  1. RECEIVE: Parse user query                                    │
│     └─► Log: [RECEIVED] "User query: ..."                       │
│                                                                  │
│  2. THINK: Chain-of-thought reasoning                           │
│     └─► Log: [THINKING] "I need to understand what the user..." │
│     └─► Log: [THINKING] "This requires data from Cortex..."     │
│     └─► Log: [THINKING] "I should also check the hierarchy..."  │
│                                                                  │
│  3. PLAN: Create step-by-step plan                              │
│     └─► Log: [PLAN] Step 1: Query Cortex Analyst for...         │
│     └─► Log: [PLAN] Step 2: Validate with HierarchyBuilder...   │
│     └─► Log: [PLAN] Step 3: Cross-reference mappings...         │
│                                                                  │
│  4. CLARIFY (if needed): Ask follow-up questions                │
│     └─► Log: [CLARIFICATION] "Do you want 2024 or 2025 data?"   │
│     └─► (Wait for user response)                                │
│                                                                  │
│  5. EXECUTE: Dispatch to specialist agents                      │
│     │                                                            │
│     ├─► [TO: CortexAnalyst] "Query revenue accounts..."         │
│     │   └─► [FROM: CortexAnalyst] "Found 47 accounts..."        │
│     │   └─► [THINKING] "Cortex returned 47 accounts, but..."    │
│     │                                                            │
│     ├─► [TO: HierarchyBuilder] "Validate against P&L..."        │
│     │   └─► [FROM: HierarchyBuilder] "3 unmapped accounts..."   │
│     │   └─► [THINKING] "There's a discrepancy, let me..."       │
│     │                                                            │
│     └─► [TO: CortexLLM] "Explain discrepancy..."                │
│         └─► [FROM: CortexLLM] "The accounts 4150, 4160..."      │
│                                                                  │
│  6. SYNTHESIZE: Combine results into coherent answer            │
│     └─► Log: [SYNTHESIS] "Based on my analysis..."              │
│                                                                  │
│  7. RESPOND: Present solution to user                           │
│     └─► Log: [RESPONSE] "Here's what I found..."                │
└─────────────────────────────────────────────────────────────────┘
```

---

## Orchestrator Agent Design

### Core Capabilities

```python
class OrchestratorAgent:
    """
    Central orchestrator that monitors and coordinates agent-to-agent
    communication, implements chain-of-thought reasoning, and provides
    full observability.
    """

    def __init__(self, config: OrchestratorConfig):
        self.agents: Dict[str, BaseAgent] = {}
        self.message_bus: MessageBus = MessageBus()
        self.console: CommunicationConsole = CommunicationConsole()
        self.conversation_store: ConversationStore = ConversationStore()

    async def process_request(self, user_query: str) -> AgentResponse:
        """Main entry point for processing user requests."""
        conversation_id = str(uuid.uuid4())

        # 1. Log the incoming request
        await self.console.log(AgentMessage(
            conversation_id=conversation_id,
            message_type=MessageType.REQUEST,
            content=user_query,
            from_agent="user",
            to_agent="orchestrator"
        ))

        # 2. Think through the problem (chain-of-thought)
        thinking_steps = await self._think(user_query, conversation_id)

        # 3. Create a plan of action
        plan = await self._create_plan(user_query, thinking_steps, conversation_id)

        # 4. Check if clarification is needed
        if plan.needs_clarification:
            clarifications = await self._request_clarification(plan, conversation_id)
            # Wait for user response and update plan

        # 5. Execute the plan by coordinating agents
        results = await self._execute_plan(plan, conversation_id)

        # 6. Synthesize results into a coherent response
        response = await self._synthesize(results, conversation_id)

        # 7. Return the final response
        return response

    async def _think(self, query: str, conversation_id: str) -> List[ThinkingStep]:
        """
        Implement chain-of-thought reasoning.
        Each step is logged to the console for observability.
        """
        thinking_steps = []

        # Use Cortex LLM to think through the problem
        prompt = f"""
        You are analyzing a user query. Think step by step about:
        1. What is the user really asking for?
        2. What data sources do I need?
        3. What agents should I involve?
        4. What are the potential challenges?
        5. What clarifications might I need?

        User Query: {query}

        Think through this step by step:
        """

        response = await self.cortex_llm.complete(prompt, stream=True)

        async for chunk in response:
            step = ThinkingStep(content=chunk, timestamp=datetime.now())
            thinking_steps.append(step)

            # Log each thinking step to console in real-time
            await self.console.log(AgentMessage(
                conversation_id=conversation_id,
                message_type=MessageType.THINKING,
                content=chunk,
                from_agent="orchestrator",
                to_agent="orchestrator"
            ))

        return thinking_steps

    async def _create_plan(
        self,
        query: str,
        thinking: List[ThinkingStep],
        conversation_id: str
    ) -> ExecutionPlan:
        """Create a step-by-step execution plan."""

        prompt = f"""
        Based on your analysis, create a step-by-step plan to answer this query.

        Query: {query}

        Your thinking:
        {[step.content for step in thinking]}

        Create a plan with:
        1. Specific steps to execute
        2. Which agent handles each step
        3. What information to pass between steps
        4. Any clarifications needed from the user

        Format as JSON:
        {{
            "steps": [
                {{"step": 1, "agent": "...", "action": "...", "input": "..."}},
                ...
            ],
            "clarifications_needed": ["question1", "question2"],
            "estimated_complexity": "low|medium|high"
        }}
        """

        plan_json = await self.cortex_llm.complete(prompt)
        plan = ExecutionPlan.from_json(plan_json)

        # Log the plan
        for step in plan.steps:
            await self.console.log(AgentMessage(
                conversation_id=conversation_id,
                message_type=MessageType.PLAN,
                content=f"Step {step.number}: [{step.agent}] {step.action}",
                from_agent="orchestrator",
                to_agent="orchestrator"
            ))

        return plan

    async def _execute_plan(
        self,
        plan: ExecutionPlan,
        conversation_id: str
    ) -> List[AgentResult]:
        """Execute the plan by coordinating agents."""
        results = []

        for step in plan.steps:
            agent = self.agents[step.agent]

            # Log the dispatch
            await self.console.log(AgentMessage(
                conversation_id=conversation_id,
                message_type=MessageType.REQUEST,
                content=step.action,
                from_agent="orchestrator",
                to_agent=step.agent
            ))

            # Execute the step
            result = await agent.execute(step.action, step.input)

            # Log the response
            await self.console.log(AgentMessage(
                conversation_id=conversation_id,
                message_type=MessageType.RESPONSE,
                content=result.content,
                from_agent=step.agent,
                to_agent="orchestrator",
                metadata={"tokens": result.tokens, "latency_ms": result.latency_ms}
            ))

            # Think about the result
            observation = await self._observe(result, conversation_id)
            results.append(AgentResult(step=step, result=result, observation=observation))

        return results
```

---

## Communication Console

### Console Architecture

```python
class CommunicationConsole:
    """
    Central console for observing agent communications.
    Supports multiple output targets: CLI, WebSocket, Database.
    """

    def __init__(self, config: ConsoleConfig):
        self.outputs: List[ConsoleOutput] = []
        self.message_queue: asyncio.Queue = asyncio.Queue()
        self.conversation_store: ConversationStore = ConversationStore()

        # Initialize outputs based on config
        if config.enable_cli:
            self.outputs.append(CLIOutput())
        if config.enable_websocket:
            self.outputs.append(WebSocketOutput(config.ws_port))
        if config.enable_database:
            self.outputs.append(DatabaseOutput(config.db_connection))
        if config.enable_file:
            self.outputs.append(FileOutput(config.log_path))

    async def log(self, message: AgentMessage):
        """Log a message to all configured outputs."""
        # Store in conversation history
        await self.conversation_store.save(message)

        # Dispatch to all outputs
        for output in self.outputs:
            await output.write(message)

    async def stream(self, conversation_id: str) -> AsyncIterator[AgentMessage]:
        """Stream messages for a specific conversation."""
        async for message in self.conversation_store.stream(conversation_id):
            yield message

    def get_conversation(self, conversation_id: str) -> Conversation:
        """Get full conversation history."""
        return self.conversation_store.get(conversation_id)

    def get_status(self) -> Dict[str, AgentStatus]:
        """Get current status of all agents."""
        return {
            agent_id: agent.get_status()
            for agent_id, agent in self.orchestrator.agents.items()
        }


class CLIOutput(ConsoleOutput):
    """Pretty-print messages to CLI with colors and formatting."""

    COLORS = {
        MessageType.REQUEST: "\033[94m",      # Blue
        MessageType.RESPONSE: "\033[92m",     # Green
        MessageType.THINKING: "\033[93m",     # Yellow
        MessageType.PLAN: "\033[95m",         # Magenta
        MessageType.CLARIFICATION: "\033[96m", # Cyan
        MessageType.ERROR: "\033[91m",        # Red
        MessageType.SYNTHESIS: "\033[97m",    # White
    }
    RESET = "\033[0m"

    async def write(self, message: AgentMessage):
        color = self.COLORS.get(message.message_type, "")
        timestamp = message.timestamp.strftime("%H:%M:%S.%f")[:-3]

        # Format the message
        prefix = f"[{timestamp}] [{message.message_type.value.upper():12}]"
        agent_info = f"[{message.from_agent} → {message.to_agent}]"

        print(f"{color}{prefix} {agent_info}{self.RESET}")
        print(f"  {message.content[:200]}{'...' if len(message.content) > 200 else ''}")
        print()


class WebSocketOutput(ConsoleOutput):
    """Stream messages to connected WebSocket clients."""

    def __init__(self, port: int):
        self.port = port
        self.clients: Set[WebSocket] = set()

    async def write(self, message: AgentMessage):
        # Broadcast to all connected clients
        data = message.to_json()
        await asyncio.gather(*[
            client.send(data)
            for client in self.clients
        ])


class DatabaseOutput(ConsoleOutput):
    """Persist messages to database for history and analysis."""

    async def write(self, message: AgentMessage):
        await self.db.execute("""
            INSERT INTO agent_messages
            (id, conversation_id, timestamp, from_agent, to_agent,
             message_type, content, metadata, thinking, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, message.to_tuple())
```

### CLI Console Example Output

```
$ python -m databridge.console --conversation abc123

╔══════════════════════════════════════════════════════════════════════════════╗
║  DataBridge Agent Communication Console                                       ║
║  Conversation: abc123 | Agents: 5 active | Status: Processing                ║
╠══════════════════════════════════════════════════════════════════════════════╣

[14:23:01.234] [REQUEST     ] [user → orchestrator]
  Show me all Revenue accounts with negative balances in 2024

[14:23:01.456] [THINKING    ] [orchestrator → orchestrator]
  I need to understand what the user is asking. They want:
  1. Revenue accounts (from the P&L hierarchy)
  2. That have negative balances
  3. For fiscal year 2024

[14:23:01.789] [THINKING    ] [orchestrator → orchestrator]
  This requires:
  - Querying Cortex Analyst with the semantic view
  - Cross-referencing with the hierarchy mappings
  - Filtering for negative amounts

[14:23:02.012] [PLAN        ] [orchestrator → orchestrator]
  Step 1: [CortexAnalyst] Query semantic view for Revenue accounts in 2024
  Step 2: [HierarchyBuilder] Validate accounts against P&L hierarchy
  Step 3: [CortexLLM] Explain any anomalies found

[14:23:02.234] [REQUEST     ] [orchestrator → cortex_analyst]
  Query: SELECT account_name, SUM(net_amount) as balance
         FROM pl_semantic_view
         WHERE category = 'Revenue' AND fiscal_year = 2024
         GROUP BY account_name
         HAVING SUM(net_amount) < 0

[14:23:03.567] [RESPONSE    ] [cortex_analyst → orchestrator]
  Found 3 accounts with negative balances:
  - Product Returns: -$45,230
  - Sales Discounts: -$12,100
  - Revenue Adjustments: -$8,450
  Tokens: 1,234 | Latency: 1,333ms

[14:23:03.890] [THINKING    ] [orchestrator → orchestrator]
  Cortex found 3 negative accounts. These could be:
  1. Normal contra-revenue accounts (expected)
  2. Data entry errors (needs investigation)
  Let me verify against the hierarchy...

[14:23:04.123] [REQUEST     ] [orchestrator → hierarchy_builder]
  Validate accounts: Product Returns, Sales Discounts, Revenue Adjustments
  Check if they are mapped as contra-revenue in the P&L hierarchy

[14:23:04.456] [RESPONSE    ] [hierarchy_builder → orchestrator]
  Validation complete:
  - Product Returns: ✓ Mapped as Contra-Revenue (expected negative)
  - Sales Discounts: ✓ Mapped as Contra-Revenue (expected negative)
  - Revenue Adjustments: ⚠ Mapped as Revenue (unexpected negative)
  Latency: 333ms

[14:23:04.789] [THINKING    ] [orchestrator → orchestrator]
  The first two accounts are correctly classified. However,
  "Revenue Adjustments" is mapped as regular Revenue but has a
  negative balance. This could indicate a mapping error or
  unusual business activity. I should explain this to the user.

[14:23:05.012] [SYNTHESIS   ] [orchestrator → orchestrator]
  Preparing final response for user with:
  1. Summary of findings
  2. Explanation of expected vs unexpected negatives
  3. Recommendation for Revenue Adjustments account

[14:23:05.234] [RESPONSE    ] [orchestrator → user]
  📊 Revenue Accounts with Negative Balances (2024)

  Found 3 accounts with negative balances:

  ✅ Expected (Contra-Revenue accounts):
  • Product Returns: -$45,230
  • Sales Discounts: -$12,100

  ⚠️ Needs Investigation:
  • Revenue Adjustments: -$8,450
    This account is mapped as regular Revenue but shows a
    negative balance. Consider:
    1. Remapping as Contra-Revenue, OR
    2. Investigating the journal entries for errors

  Would you like me to show the journal entries for
  Revenue Adjustments?

╚══════════════════════════════════════════════════════════════════════════════╝
```

---

## Implementation Plan

### Phase 1: Core Infrastructure (Week 1-2)

```
src/orchestrator/
├── __init__.py
├── agent_base.py          # Base class for all agents
├── message.py             # AgentMessage, MessageType, etc.
├── message_bus.py         # Async message routing
├── orchestrator.py        # OrchestratorAgent
└── console/
    ├── __init__.py
    ├── console.py         # CommunicationConsole
    ├── outputs/
    │   ├── cli.py         # CLI output with colors
    │   ├── websocket.py   # WebSocket streaming
    │   ├── database.py    # Database persistence
    │   └── file.py        # File logging
    └── conversation.py    # ConversationStore
```

**Deliverables:**
- [ ] `AgentMessage` dataclass with all fields
- [ ] `MessageBus` for async message routing
- [ ] `CommunicationConsole` with CLI output
- [ ] Basic `OrchestratorAgent` skeleton

### Phase 2: Agent Integration (Week 3-4)

```
src/orchestrator/agents/
├── __init__.py
├── cortex_analyst.py      # Cortex Analyst wrapper
├── cortex_llm.py          # Cortex LLM functions wrapper
├── cortex_search.py       # Cortex Search wrapper
├── hierarchy_builder.py   # DataBridge hierarchy agent
├── mapping_expert.py      # DataBridge mapping agent
└── sql_translator.py      # Semantic view agent
```

**Deliverables:**
- [ ] Cortex agent wrappers with message protocol
- [ ] DataBridge agent wrappers
- [ ] Agent registration with orchestrator

### Phase 3: Chain-of-Thought & Planning (Week 5-6)

**Deliverables:**
- [ ] `_think()` method with streaming
- [ ] `_create_plan()` with JSON plan format
- [ ] `_request_clarification()` for follow-up questions
- [ ] `_synthesize()` for combining results

### Phase 4: Console & Streaming (Week 7-8)

**Deliverables:**
- [ ] WebSocket output for web dashboard
- [ ] Database output for persistence
- [ ] Real-time status monitoring
- [ ] Conversation replay

### Phase 5: Web Dashboard (Week 9-10)

```
src/orchestrator/dashboard/
├── static/
│   ├── index.html
│   ├── console.js
│   └── styles.css
└── server.py              # FastAPI server
```

**Deliverables:**
- [ ] Web-based communication console
- [ ] Real-time WebSocket updates
- [ ] Conversation history viewer
- [ ] Agent status dashboard

---

## Code Examples

### Starting the Console

```python
# CLI Mode - Real-time monitoring
$ python -m databridge.console

# Web Dashboard Mode
$ python -m databridge.console --web --port 8080

# Database Logging Mode
$ python -m databridge.console --db "postgresql://..."

# All outputs
$ python -m databridge.console --cli --web --db "..." --file logs/agent.log
```

### Python API

```python
from databridge.orchestrator import OrchestratorAgent, CommunicationConsole

# Initialize orchestrator with console
console = CommunicationConsole(
    enable_cli=True,
    enable_websocket=True,
    ws_port=8765,
    enable_database=True,
    db_connection="postgresql://localhost/databridge"
)

orchestrator = OrchestratorAgent(
    console=console,
    cortex_config=CortexConfig(
        account="myaccount.snowflakecomputing.com",
        token="..."
    )
)

# Register agents
orchestrator.register_agent("cortex_analyst", CortexAnalystAgent())
orchestrator.register_agent("hierarchy_builder", HierarchyBuilderAgent())
orchestrator.register_agent("mapping_expert", MappingExpertAgent())

# Process a request (all communication logged to console)
response = await orchestrator.process_request(
    "Show me all Revenue accounts with negative balances in 2024"
)

# Stream conversation in real-time
async for message in console.stream(response.conversation_id):
    print(f"[{message.message_type}] {message.content}")

# Get full conversation history
conversation = console.get_conversation(response.conversation_id)
for message in conversation.messages:
    print(message)
```

### MCP Tool Integration

```python
@mcp.tool()
def ask_orchestrator(
    query: str,
    stream_to_console: bool = True,
    save_to_database: bool = True
) -> str:
    """
    Send a query to the orchestrator agent and get a response.
    All agent communication is logged to the console.

    Args:
        query: Natural language query
        stream_to_console: Whether to stream to CLI console
        save_to_database: Whether to save conversation to database

    Returns:
        JSON with response and conversation_id for replay
    """
    response = orchestrator.process_request(query)
    return json.dumps({
        "response": response.content,
        "conversation_id": response.conversation_id,
        "agents_involved": response.agents_involved,
        "total_tokens": response.total_tokens,
        "thinking_steps": len(response.thinking_steps),
        "console_url": f"http://localhost:8080/conversation/{response.conversation_id}"
    })

@mcp.tool()
def get_agent_conversation(conversation_id: str) -> str:
    """
    Retrieve the full conversation history for a previous request.

    Args:
        conversation_id: The conversation ID from a previous request

    Returns:
        JSON with full conversation including all agent messages
    """
    conversation = console.get_conversation(conversation_id)
    return json.dumps({
        "conversation_id": conversation_id,
        "messages": [msg.to_dict() for msg in conversation.messages],
        "duration_ms": conversation.duration_ms,
        "total_tokens": conversation.total_tokens
    })

@mcp.tool()
def get_agent_status() -> str:
    """
    Get the current status of all registered agents.

    Returns:
        JSON with status of each agent
    """
    return json.dumps(console.get_status())
```

---

## Database Schema

```sql
-- Conversations table
CREATE TABLE conversations (
    id UUID PRIMARY KEY,
    started_at TIMESTAMP NOT NULL,
    ended_at TIMESTAMP,
    user_query TEXT NOT NULL,
    final_response TEXT,
    status VARCHAR(20) NOT NULL,
    total_tokens INTEGER,
    total_latency_ms INTEGER
);

-- Messages table
CREATE TABLE agent_messages (
    id UUID PRIMARY KEY,
    conversation_id UUID REFERENCES conversations(id),
    timestamp TIMESTAMP NOT NULL,
    from_agent VARCHAR(50) NOT NULL,
    to_agent VARCHAR(50) NOT NULL,
    message_type VARCHAR(20) NOT NULL,
    content TEXT NOT NULL,
    metadata JSONB,
    thinking TEXT,
    parent_id UUID REFERENCES agent_messages(id),
    tokens INTEGER,
    latency_ms INTEGER
);

-- Indexes for fast retrieval
CREATE INDEX idx_messages_conversation ON agent_messages(conversation_id);
CREATE INDEX idx_messages_timestamp ON agent_messages(timestamp);
CREATE INDEX idx_messages_type ON agent_messages(message_type);

-- View for conversation summary
CREATE VIEW conversation_summary AS
SELECT
    c.id,
    c.started_at,
    c.user_query,
    c.status,
    COUNT(m.id) as message_count,
    SUM(m.tokens) as total_tokens,
    MAX(m.timestamp) - MIN(m.timestamp) as duration
FROM conversations c
LEFT JOIN agent_messages m ON c.id = m.conversation_id
GROUP BY c.id;
```

---

## Next Steps

1. **Review this architecture** with stakeholders
2. **Prototype Phase 1** (Core Infrastructure)
3. **Test with simple Cortex queries** before full integration
4. **Build web dashboard** for non-technical users
5. **Add metrics and monitoring** (Prometheus, Grafana)

---

## References

- [Snowflake Cortex Agents](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-agents)
- [MCP Protocol](https://modelcontextprotocol.io/)
- [LangChain Agent Framework](https://python.langchain.com/docs/modules/agents/)
- [AutoGen Multi-Agent](https://microsoft.github.io/autogen/)
