/**
 * AI-Link-Orchestrator Message Types
 * Defines message structures for agent-to-agent communication
 */

export type MessageType =
  | 'task_handoff'      // Pass task to another agent
  | 'query'             // Ask for information
  | 'response'          // Answer to query
  | 'status_update'     // Progress notification
  | 'error'             // Error escalation
  | 'approval_request'  // Human-in-the-loop
  | 'approval_response' // Human approval result
  | 'data_share'        // Share intermediate results
  | 'broadcast';        // Message to all agents

export interface SharedDataRef {
  key: string;
  location: 'redis' | 'database' | 'file';
  reference: string;
  expiresAt?: Date;
}

export interface Permission {
  action: string;
  resource: string;
  granted: boolean;
}

export interface ConversationContext {
  projectId?: string;
  hierarchyId?: string;
  taskId?: string;
  sessionState: Record<string, any>;
  sharedData: SharedDataRef[];
  permissions: Permission[];
}

export interface AgentMessage {
  id: string;
  conversationId: string;
  fromAgent: string;
  toAgent: string; // '*' for broadcast
  messageType: MessageType;
  payload: Record<string, any>;
  context: ConversationContext;
  requiresResponse: boolean;
  responseTimeout?: number; // milliseconds
  responseId?: string; // ID of message this responds to
  createdAt: Date;
  deliveredAt?: Date;
  readAt?: Date;
}

export interface SendMessageDto {
  toAgent: string;
  messageType: MessageType;
  payload: Record<string, any>;
  conversationId?: string; // existing conversation or new
  context?: Partial<ConversationContext>;
  requiresResponse?: boolean;
  responseTimeout?: number;
}

export interface MessageResponse {
  messageId: string;
  conversationId: string;
  delivered: boolean;
  deliveredAt?: Date;
  response?: AgentMessage; // if requiresResponse was true and response received
}

export interface Conversation {
  id: string;
  participants: string[]; // agent IDs
  context: ConversationContext;
  messageCount: number;
  lastMessageAt: Date;
  createdAt: Date;
  isActive: boolean;
}

export interface ConversationListResponse {
  conversations: Conversation[];
  total: number;
}

// Event Bus Channels
export const EVENT_CHANNELS = {
  TASK_CREATED: 'orchestrator.task.created',
  TASK_QUEUED: 'orchestrator.task.queued',
  TASK_STARTED: 'orchestrator.task.started',
  TASK_COMPLETED: 'orchestrator.task.completed',
  TASK_FAILED: 'orchestrator.task.failed',
  TASK_CANCELLED: 'orchestrator.task.cancelled',
  AGENT_REGISTERED: 'orchestrator.agent.registered',
  AGENT_UNREGISTERED: 'orchestrator.agent.unregistered',
  AGENT_HEARTBEAT: 'orchestrator.agent.heartbeat',
  AGENT_MESSAGE: 'orchestrator.agent.message',
  HIERARCHY_UPDATED: 'hierarchy.updated',
  HIERARCHY_DEPLOYED: 'hierarchy.deployed',
  SYNC_REQUIRED: 'sync.required',
} as const;

export type EventChannel = typeof EVENT_CHANNELS[keyof typeof EVENT_CHANNELS];

export interface OrchestratorEvent {
  channel: EventChannel;
  payload: Record<string, any>;
  timestamp: Date;
  source: string; // agent or service ID
}
