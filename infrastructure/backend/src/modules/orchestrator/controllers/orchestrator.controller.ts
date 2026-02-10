/**
 * Orchestrator Controller
 *
 * REST API endpoints for the AI orchestrator:
 * - Task management (submit, status, cancel)
 * - Agent registration and health
 * - Agent-to-agent messaging
 * - Workflow management
 */

import {
  Controller,
  Post,
  Get,
  Delete,
  Body,
  Param,
  Query,
  HttpCode,
  HttpStatus,
  NotFoundException,
  BadRequestException,
  Logger,
} from '@nestjs/common';
import {
  ApiTags,
  ApiOperation,
  ApiResponse,
  ApiParam,
  ApiBody,
} from '@nestjs/swagger';

// Services
import { TaskManagerService } from '../services/task-manager.service';
import { AgentRegistryService } from '../services/agent-registry.service';
import { AiLinkService } from '../services/ai-link.service';
import { WorkflowEngineService } from '../services/workflow-engine.service';
import { EventBusService } from '../services/event-bus.service';

// DTOs
import {
  CreateTaskRequestDto,
  TaskQueryDto,
  RegisterAgentRequestDto,
  AgentHeartbeatRequestDto,
  AgentQueryDto,
  SendMessageRequestDto,
  ConversationQueryDto,
} from '../dto/orchestrator.dto';

@ApiTags('orchestrator')
@Controller('orchestrator')
export class OrchestratorController {
  private readonly logger = new Logger(OrchestratorController.name);

  constructor(
    private readonly taskManager: TaskManagerService,
    private readonly agentRegistry: AgentRegistryService,
    private readonly aiLink: AiLinkService,
    private readonly workflowEngine: WorkflowEngineService,
    private readonly eventBus: EventBusService,
  ) {}

  // ===== TASK ENDPOINTS =====

  @Post('tasks')
  @ApiOperation({ summary: 'Submit a new task to the orchestrator' })
  @ApiResponse({ status: 201, description: 'Task created successfully' })
  @ApiResponse({ status: 400, description: 'Invalid request' })
  async submitTask(@Body() dto: CreateTaskRequestDto) {
    this.logger.log(`Submitting task of type: ${dto.type}`);
    const task = await this.taskManager.submitTask(dto);
    return {
      success: true,
      task,
      message: `Task ${task.id} created with status: ${task.status}`,
    };
  }

  @Get('tasks')
  @ApiOperation({ summary: 'List tasks with optional filters' })
  @ApiResponse({ status: 200, description: 'Task list retrieved' })
  async listTasks(@Query() query: TaskQueryDto) {
    const result = await this.taskManager.listTasks({
      status: query.status as any,
      type: query.type,
      assignedAgent: query.assignedAgent,
      limit: query.limit,
      offset: query.offset,
    });
    return result;
  }

  @Get('tasks/:taskId')
  @ApiOperation({ summary: 'Get task status and progress' })
  @ApiParam({ name: 'taskId', description: 'Task ID' })
  @ApiResponse({ status: 200, description: 'Task status retrieved' })
  @ApiResponse({ status: 404, description: 'Task not found' })
  async getTaskStatus(@Param('taskId') taskId: string) {
    const status = await this.taskManager.getTaskStatus(taskId);
    if (!status) {
      throw new NotFoundException(`Task not found: ${taskId}`);
    }
    return status;
  }

  @Delete('tasks/:taskId')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Cancel a task' })
  @ApiParam({ name: 'taskId', description: 'Task ID' })
  @ApiResponse({ status: 200, description: 'Task cancelled' })
  @ApiResponse({ status: 404, description: 'Task not found or cannot be cancelled' })
  async cancelTask(@Param('taskId') taskId: string) {
    const cancelled = await this.taskManager.cancelTask(taskId);
    if (!cancelled) {
      throw new NotFoundException(`Task not found or cannot be cancelled: ${taskId}`);
    }
    return {
      success: true,
      message: `Task ${taskId} cancelled`,
    };
  }

  // ===== AGENT ENDPOINTS =====

  @Post('agents/register')
  @ApiOperation({ summary: 'Register a new agent with the orchestrator' })
  @ApiResponse({ status: 201, description: 'Agent registered successfully' })
  @ApiResponse({ status: 400, description: 'Invalid request' })
  async registerAgent(@Body() dto: RegisterAgentRequestDto) {
    this.logger.log(`Registering agent: ${dto.id} (${dto.type})`);
    const agent = await this.agentRegistry.registerAgent(dto);
    return {
      success: true,
      agent,
      message: `Agent ${agent.id} registered successfully`,
    };
  }

  @Delete('agents/:agentId')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Unregister an agent' })
  @ApiParam({ name: 'agentId', description: 'Agent ID' })
  @ApiResponse({ status: 200, description: 'Agent unregistered' })
  @ApiResponse({ status: 404, description: 'Agent not found' })
  async unregisterAgent(@Param('agentId') agentId: string) {
    const unregistered = await this.agentRegistry.unregisterAgent(agentId);
    if (!unregistered) {
      throw new NotFoundException(`Agent not found: ${agentId}`);
    }
    return {
      success: true,
      message: `Agent ${agentId} unregistered`,
    };
  }

  @Get('agents')
  @ApiOperation({ summary: 'List registered agents' })
  @ApiResponse({ status: 200, description: 'Agent list retrieved' })
  async listAgents(@Query() query: AgentQueryDto) {
    const result = await this.agentRegistry.listAgents({
      type: query.type,
      tool: query.capability,
      healthyOnly: query.healthyOnly,
      limit: query.limit,
    });
    return result;
  }

  @Get('agents/:agentId')
  @ApiOperation({ summary: 'Get agent details' })
  @ApiParam({ name: 'agentId', description: 'Agent ID' })
  @ApiResponse({ status: 200, description: 'Agent details retrieved' })
  @ApiResponse({ status: 404, description: 'Agent not found' })
  async getAgent(@Param('agentId') agentId: string) {
    const agent = await this.agentRegistry.getAgent(agentId);
    if (!agent) {
      throw new NotFoundException(`Agent not found: ${agentId}`);
    }
    return agent;
  }

  @Post('agents/:agentId/heartbeat')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Send agent heartbeat' })
  @ApiParam({ name: 'agentId', description: 'Agent ID' })
  @ApiResponse({ status: 200, description: 'Heartbeat acknowledged' })
  @ApiResponse({ status: 404, description: 'Agent not found' })
  async agentHeartbeat(
    @Param('agentId') agentId: string,
    @Body() dto: AgentHeartbeatRequestDto,
  ) {
    const response = await this.agentRegistry.processHeartbeat(agentId, dto);
    if (!response) {
      throw new NotFoundException(`Agent not found: ${agentId}`);
    }
    return response;
  }

  // ===== MESSAGE ENDPOINTS =====

  @Post('messages')
  @ApiOperation({ summary: 'Send a message to another agent' })
  @ApiBody({ type: SendMessageRequestDto })
  @ApiResponse({ status: 201, description: 'Message sent' })
  @ApiResponse({ status: 400, description: 'Invalid request' })
  async sendMessage(
    @Body() dto: SendMessageRequestDto & { fromAgent: string },
  ) {
    if (!dto.fromAgent) {
      throw new BadRequestException('fromAgent is required');
    }

    const response = await this.aiLink.sendMessage(dto.fromAgent, dto);
    return response;
  }

  @Get('messages/:agentId')
  @ApiOperation({ summary: 'Get messages for an agent' })
  @ApiParam({ name: 'agentId', description: 'Agent ID' })
  @ApiResponse({ status: 200, description: 'Messages retrieved' })
  async getMessagesForAgent(
    @Param('agentId') agentId: string,
    @Query('limit') limit?: number,
  ) {
    const messages = await this.aiLink.getMessagesForAgent(agentId, limit);
    return { messages, count: messages.length };
  }

  @Post('messages/:messageId/read')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Mark message as read' })
  @ApiParam({ name: 'messageId', description: 'Message ID' })
  async markMessageRead(
    @Param('messageId') messageId: string,
    @Body('agentId') agentId: string,
  ) {
    const marked = await this.aiLink.markMessageRead(messageId, agentId);
    if (!marked) {
      throw new NotFoundException(`Message not found or not accessible: ${messageId}`);
    }
    return { success: true };
  }

  // ===== CONVERSATION ENDPOINTS =====

  @Get('conversations')
  @ApiOperation({ summary: 'List conversations for an agent' })
  @ApiResponse({ status: 200, description: 'Conversations retrieved' })
  async listConversations(@Query() query: ConversationQueryDto) {
    if (!query.participant) {
      throw new BadRequestException('participant query parameter is required');
    }
    const conversations = await this.aiLink.listConversations(query.participant, {
      activeOnly: query.activeOnly,
      limit: query.limit,
    });
    return { conversations, count: conversations.length };
  }

  @Get('conversations/:conversationId')
  @ApiOperation({ summary: 'Get conversation details' })
  @ApiParam({ name: 'conversationId', description: 'Conversation ID' })
  @ApiResponse({ status: 200, description: 'Conversation retrieved' })
  @ApiResponse({ status: 404, description: 'Conversation not found' })
  async getConversation(@Param('conversationId') conversationId: string) {
    const conversation = await this.aiLink.getConversation(conversationId);
    if (!conversation) {
      throw new NotFoundException(`Conversation not found: ${conversationId}`);
    }
    return conversation;
  }

  @Get('conversations/:conversationId/messages')
  @ApiOperation({ summary: 'Get messages in a conversation' })
  @ApiParam({ name: 'conversationId', description: 'Conversation ID' })
  async getConversationMessages(
    @Param('conversationId') conversationId: string,
    @Query('limit') limit?: number,
  ) {
    const messages = await this.aiLink.getConversationMessages(conversationId, limit);
    return { messages, count: messages.length };
  }

  @Post('conversations/:conversationId/close')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Close a conversation' })
  @ApiParam({ name: 'conversationId', description: 'Conversation ID' })
  async closeConversation(@Param('conversationId') conversationId: string) {
    const closed = await this.aiLink.closeConversation(conversationId);
    if (!closed) {
      throw new NotFoundException(`Conversation not found: ${conversationId}`);
    }
    return { success: true };
  }

  // ===== WORKFLOW ENDPOINTS =====

  @Post('workflows')
  @ApiOperation({ summary: 'Create a new workflow' })
  @ApiResponse({ status: 201, description: 'Workflow created' })
  async createWorkflow(
    @Body() dto: { name: string; steps: any[]; description?: string; triggers?: any[] },
  ) {
    const workflow = await this.workflowEngine.createWorkflow(dto.name, dto.steps, {
      description: dto.description,
      triggers: dto.triggers,
    });
    return { success: true, workflow };
  }

  @Get('workflows')
  @ApiOperation({ summary: 'List all workflows' })
  @ApiResponse({ status: 200, description: 'Workflows retrieved' })
  async listWorkflows() {
    const workflows = await this.workflowEngine.listWorkflows();
    return { workflows, count: workflows.length };
  }

  @Get('workflows/:workflowId')
  @ApiOperation({ summary: 'Get workflow details' })
  @ApiParam({ name: 'workflowId', description: 'Workflow ID' })
  async getWorkflow(@Param('workflowId') workflowId: string) {
    const workflow = await this.workflowEngine.getWorkflow(workflowId);
    if (!workflow) {
      throw new NotFoundException(`Workflow not found: ${workflowId}`);
    }
    return workflow;
  }

  @Post('workflows/:workflowId/execute')
  @ApiOperation({ summary: 'Start workflow execution' })
  @ApiParam({ name: 'workflowId', description: 'Workflow ID' })
  async executeWorkflow(
    @Param('workflowId') workflowId: string,
    @Body('context') context?: Record<string, any>,
  ) {
    const execution = await this.workflowEngine.executeWorkflow(workflowId, context);
    return { success: true, execution };
  }

  @Get('executions/:executionId')
  @ApiOperation({ summary: 'Get workflow execution status' })
  @ApiParam({ name: 'executionId', description: 'Execution ID' })
  async getExecution(@Param('executionId') executionId: string) {
    const execution = await this.workflowEngine.getExecution(executionId);
    if (!execution) {
      throw new NotFoundException(`Execution not found: ${executionId}`);
    }
    return execution;
  }

  @Post('executions/:executionId/pause')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Pause workflow execution' })
  @ApiParam({ name: 'executionId', description: 'Execution ID' })
  async pauseExecution(@Param('executionId') executionId: string) {
    const paused = await this.workflowEngine.pauseExecution(executionId);
    if (!paused) {
      throw new BadRequestException(`Cannot pause execution: ${executionId}`);
    }
    return { success: true };
  }

  @Post('executions/:executionId/resume')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Resume workflow execution' })
  @ApiParam({ name: 'executionId', description: 'Execution ID' })
  async resumeExecution(@Param('executionId') executionId: string) {
    const resumed = await this.workflowEngine.resumeExecution(executionId);
    if (!resumed) {
      throw new BadRequestException(`Cannot resume execution: ${executionId}`);
    }
    return { success: true };
  }

  @Delete('executions/:executionId')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Cancel workflow execution' })
  @ApiParam({ name: 'executionId', description: 'Execution ID' })
  async cancelExecution(@Param('executionId') executionId: string) {
    const cancelled = await this.workflowEngine.cancelExecution(executionId);
    if (!cancelled) {
      throw new BadRequestException(`Cannot cancel execution: ${executionId}`);
    }
    return { success: true };
  }

  // ===== EVENT PUBLISHING =====

  @Post('events/publish')
  @HttpCode(HttpStatus.OK)
  @ApiOperation({ summary: 'Publish an event to the Event Bus' })
  @ApiBody({
    schema: {
      type: 'object',
      properties: {
        channel: { type: 'string', description: 'Event channel (e.g., hierarchy.updated)' },
        payload: { type: 'object', description: 'Event payload' },
        source: { type: 'string', description: 'Source agent/service ID' },
      },
      required: ['channel', 'payload'],
    },
  })
  @ApiResponse({ status: 200, description: 'Event published' })
  async publishEvent(
    @Body() dto: { channel: string; payload: Record<string, any>; source?: string },
  ) {
    await this.eventBus.publish(
      dto.channel as any,
      dto.payload,
      dto.source || 'external',
    );
    return {
      success: true,
      channel: dto.channel,
      timestamp: new Date(),
    };
  }

  @Get('events/channels')
  @ApiOperation({ summary: 'List available event channels' })
  @ApiResponse({ status: 200, description: 'Available channels retrieved' })
  async listEventChannels() {
    return {
      channels: this.eventBus.getAvailableChannels(),
    };
  }

  // ===== HEALTH CHECK =====

  @Get('health')
  @ApiOperation({ summary: 'Orchestrator health check' })
  @ApiResponse({ status: 200, description: 'Orchestrator is healthy' })
  async healthCheck() {
    const agents = await this.agentRegistry.listAgents();
    const tasks = await this.taskManager.listTasks({ limit: 1 });

    return {
      status: 'healthy',
      timestamp: new Date(),
      agents: {
        total: agents.total,
        healthy: agents.healthy,
        degraded: agents.degraded,
        offline: agents.offline,
      },
      tasks: {
        total: tasks.total,
        pending: tasks.pending,
        inProgress: tasks.inProgress,
        completed: tasks.completed,
        failed: tasks.failed,
      },
    };
  }
}
