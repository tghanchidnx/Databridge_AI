/**
 * Orchestrator DTOs
 * Request/Response data transfer objects for the orchestrator API
 */

import { ApiProperty, ApiPropertyOptional } from '@nestjs/swagger';
import {
  IsString,
  IsEnum,
  IsOptional,
  IsArray,
  IsObject,
  IsNumber,
  IsBoolean,
  IsUrl,
  Min,
  Max,
  ValidateNested,
} from 'class-validator';
import { Type } from 'class-transformer';
import { TaskType, Priority } from '../types/task.types';
import { AgentType, HealthStatus, Capability } from '../types/agent.types';
import { MessageType } from '../types/message.types';

// ===== TASK DTOs =====

export class CreateTaskRequestDto {
  @ApiProperty({ enum: ['hierarchy_build', 'data_reconciliation', 'sql_analysis', 'mapping_suggestion', 'report_generation', 'deployment', 'agent_handoff', 'workflow_step', 'custom'] })
  @IsString()
  type: TaskType;

  @ApiProperty({ description: 'Task-specific parameters' })
  @IsObject()
  payload: Record<string, any>;

  @ApiPropertyOptional({ enum: ['low', 'normal', 'high', 'critical'], default: 'normal' })
  @IsOptional()
  @IsEnum(['low', 'normal', 'high', 'critical'])
  priority?: Priority;

  @ApiPropertyOptional({ description: 'Task IDs that must complete before this task starts' })
  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  dependencies?: string[];

  @ApiPropertyOptional({ description: 'Webhook URL for completion notification' })
  @IsOptional()
  @IsUrl()
  callbackUrl?: string;

  @ApiPropertyOptional({ description: 'Additional metadata' })
  @IsOptional()
  @IsObject()
  metadata?: Record<string, any>;
}

export class TaskQueryDto {
  @ApiPropertyOptional({ enum: ['pending', 'queued', 'in_progress', 'waiting_input', 'completed', 'failed', 'cancelled'] })
  @IsOptional()
  @IsString()
  status?: string;

  @ApiPropertyOptional({ enum: ['hierarchy_build', 'data_reconciliation', 'sql_analysis', 'mapping_suggestion', 'report_generation', 'deployment', 'agent_handoff', 'workflow_step', 'custom'] })
  @IsOptional()
  @IsString()
  type?: TaskType;

  @ApiPropertyOptional({ description: 'Filter by assigned agent' })
  @IsOptional()
  @IsString()
  assignedAgent?: string;

  @ApiPropertyOptional({ default: 20 })
  @IsOptional()
  @IsNumber()
  @Min(1)
  @Max(100)
  limit?: number;

  @ApiPropertyOptional({ default: 0 })
  @IsOptional()
  @IsNumber()
  @Min(0)
  offset?: number;
}

// ===== AGENT DTOs =====

export class CapabilityDto {
  @ApiProperty({ description: 'MCP tool name or capability identifier' })
  @IsString()
  tool: string;

  @ApiProperty({ description: 'Proficiency score 0-1' })
  @IsNumber()
  @Min(0)
  @Max(1)
  proficiency: number;

  @ApiPropertyOptional({ description: 'Any limitations or constraints' })
  @IsOptional()
  @IsArray()
  @IsString({ each: true })
  constraints?: string[];
}

export class RegisterAgentRequestDto {
  @ApiProperty({ description: 'Unique agent identifier' })
  @IsString()
  id: string;

  @ApiProperty({ description: 'Human-readable agent name' })
  @IsString()
  name: string;

  @ApiProperty({ enum: ['mcp_native', 'llm_agent', 'specialized', 'excel_plugin', 'power_bi', 'external'] })
  @IsEnum(['mcp_native', 'llm_agent', 'specialized', 'excel_plugin', 'power_bi', 'external'])
  type: AgentType;

  @ApiProperty({ type: [CapabilityDto], description: 'Agent capabilities' })
  @IsArray()
  @ValidateNested({ each: true })
  @Type(() => CapabilityDto)
  capabilities: CapabilityDto[];

  @ApiPropertyOptional({ default: 5 })
  @IsOptional()
  @IsNumber()
  @Min(1)
  @Max(100)
  maxConcurrentTasks?: number;

  @ApiPropertyOptional({ description: 'Webhook URL for receiving messages' })
  @IsOptional()
  @IsUrl()
  callbackUrl?: string;

  @ApiPropertyOptional({ description: 'Additional metadata' })
  @IsOptional()
  @IsObject()
  metadata?: Record<string, any>;
}

export class AgentHeartbeatRequestDto {
  @ApiPropertyOptional({ description: 'Current task load' })
  @IsOptional()
  @IsNumber()
  @Min(0)
  currentLoad?: number;

  @ApiPropertyOptional({ enum: ['healthy', 'degraded', 'unhealthy', 'offline'] })
  @IsOptional()
  @IsEnum(['healthy', 'degraded', 'unhealthy', 'offline'])
  healthStatus?: HealthStatus;

  @ApiPropertyOptional({ description: 'Additional metadata' })
  @IsOptional()
  @IsObject()
  metadata?: Record<string, any>;
}

export class AgentQueryDto {
  @ApiPropertyOptional({ enum: ['mcp_native', 'llm_agent', 'specialized', 'excel_plugin', 'power_bi', 'external'] })
  @IsOptional()
  @IsString()
  type?: AgentType;

  @ApiPropertyOptional({ description: 'Filter by capability tool' })
  @IsOptional()
  @IsString()
  capability?: string;

  @ApiPropertyOptional({ description: 'Only return healthy agents', default: false })
  @IsOptional()
  @IsBoolean()
  healthyOnly?: boolean;

  @ApiPropertyOptional({ default: 50 })
  @IsOptional()
  @IsNumber()
  @Min(1)
  @Max(100)
  limit?: number;
}

// ===== MESSAGE DTOs =====

export class SendMessageRequestDto {
  @ApiProperty({ description: 'Target agent ID or "*" for broadcast' })
  @IsString()
  toAgent: string;

  @ApiProperty({ enum: ['task_handoff', 'query', 'response', 'status_update', 'error', 'approval_request', 'approval_response', 'data_share', 'broadcast'] })
  @IsEnum(['task_handoff', 'query', 'response', 'status_update', 'error', 'approval_request', 'approval_response', 'data_share', 'broadcast'])
  messageType: MessageType;

  @ApiProperty({ description: 'Message content' })
  @IsObject()
  payload: Record<string, any>;

  @ApiPropertyOptional({ description: 'Existing conversation ID to continue' })
  @IsOptional()
  @IsString()
  conversationId?: string;

  @ApiPropertyOptional({ description: 'Context for the conversation' })
  @IsOptional()
  @IsObject()
  context?: Record<string, any>;

  @ApiPropertyOptional({ description: 'Whether to wait for response', default: false })
  @IsOptional()
  @IsBoolean()
  requiresResponse?: boolean;

  @ApiPropertyOptional({ description: 'Response timeout in milliseconds' })
  @IsOptional()
  @IsNumber()
  @Min(1000)
  @Max(300000)
  responseTimeout?: number;
}

export class ConversationQueryDto {
  @ApiPropertyOptional({ description: 'Filter by participant agent ID' })
  @IsOptional()
  @IsString()
  participant?: string;

  @ApiPropertyOptional({ description: 'Only return active conversations', default: true })
  @IsOptional()
  @IsBoolean()
  activeOnly?: boolean;

  @ApiPropertyOptional({ default: 20 })
  @IsOptional()
  @IsNumber()
  @Min(1)
  @Max(100)
  limit?: number;
}
