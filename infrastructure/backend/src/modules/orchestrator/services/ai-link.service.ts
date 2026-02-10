/**
 * AI-Link Service
 *
 * Manages agent-to-agent communication:
 * - Message routing and delivery
 * - Conversation management
 * - Shared data references
 * - Task handoff coordination
 */

import { Injectable, Logger } from '@nestjs/common';
import { v4 as uuidv4 } from 'uuid';
import {
  AgentMessage,
  SendMessageDto,
  MessageResponse,
  Conversation,
  ConversationContext,
  EVENT_CHANNELS,
} from '../types/message.types';
import { EventBusService } from './event-bus.service';
import { AgentRegistryService } from './agent-registry.service';

@Injectable()
export class AiLinkService {
  private readonly logger = new Logger(AiLinkService.name);

  // In-memory stores (will be replaced with Prisma in Phase 2)
  private messages: Map<string, AgentMessage> = new Map();
  private conversations: Map<string, Conversation> = new Map();

  constructor(
    private eventBus: EventBusService,
    private agentRegistry: AgentRegistryService,
  ) {}

  /**
   * Send a message to another agent
   */
  async sendMessage(fromAgent: string, dto: SendMessageDto): Promise<MessageResponse> {
    const messageId = uuidv4();
    const conversationId = dto.conversationId || uuidv4();

    // Get or create conversation
    let conversation = this.conversations.get(conversationId);
    if (!conversation) {
      conversation = this.createConversation(conversationId, fromAgent, dto.toAgent, dto.context);
    }

    // Create message
    const message: AgentMessage = {
      id: messageId,
      conversationId,
      fromAgent,
      toAgent: dto.toAgent,
      messageType: dto.messageType,
      payload: dto.payload,
      context: conversation.context,
      requiresResponse: dto.requiresResponse || false,
      responseTimeout: dto.responseTimeout,
      createdAt: new Date(),
    };

    // Store message
    this.messages.set(messageId, message);

    // Update conversation
    conversation.messageCount++;
    conversation.lastMessageAt = new Date();
    if (!conversation.participants.includes(fromAgent)) {
      conversation.participants.push(fromAgent);
    }
    if (dto.toAgent !== '*' && !conversation.participants.includes(dto.toAgent)) {
      conversation.participants.push(dto.toAgent);
    }
    this.conversations.set(conversationId, conversation);

    // Deliver message
    let delivered = false;
    let deliveredAt: Date | undefined;

    if (dto.toAgent === '*') {
      // Broadcast to all agents
      await this.eventBus.broadcast({
        messageId,
        message,
      }, fromAgent);
      delivered = true;
      deliveredAt = new Date();
    } else {
      // Send to specific agent
      const targetAgent = await this.agentRegistry.getAgent(dto.toAgent);
      if (targetAgent) {
        await this.eventBus.publish(EVENT_CHANNELS.AGENT_MESSAGE, {
          messageId,
          message,
          targetAgent: dto.toAgent,
        }, fromAgent);

        // Queue for heartbeat delivery
        await this.agentRegistry.queueMessageForAgent(dto.toAgent, messageId);

        delivered = true;
        deliveredAt = new Date();
        message.deliveredAt = deliveredAt;
        this.messages.set(messageId, message);
      }
    }

    const response: MessageResponse = {
      messageId,
      conversationId,
      delivered,
      deliveredAt,
    };

    // If requires response, wait for it
    if (dto.requiresResponse && delivered) {
      try {
        const timeout = dto.responseTimeout || 30000;
        const reply = await this.waitForResponse(messageId, timeout);
        response.response = reply;
      } catch (err) {
        this.logger.warn(`No response received for message ${messageId}`);
      }
    }

    this.logger.log(`Message ${messageId} sent from ${fromAgent} to ${dto.toAgent}`);
    return response;
  }

  /**
   * Get message by ID
   */
  async getMessage(messageId: string): Promise<AgentMessage | null> {
    return this.messages.get(messageId) || null;
  }

  /**
   * Get messages for an agent
   */
  async getMessagesForAgent(agentId: string, limit: number = 50): Promise<AgentMessage[]> {
    return Array.from(this.messages.values())
      .filter(m => m.toAgent === agentId || m.toAgent === '*')
      .sort((a, b) => b.createdAt.getTime() - a.createdAt.getTime())
      .slice(0, limit);
  }

  /**
   * Mark message as read
   */
  async markMessageRead(messageId: string, agentId: string): Promise<boolean> {
    const message = this.messages.get(messageId);
    if (!message) return false;
    if (message.toAgent !== agentId && message.toAgent !== '*') return false;

    message.readAt = new Date();
    this.messages.set(messageId, message);
    return true;
  }

  /**
   * Get conversation by ID
   */
  async getConversation(conversationId: string): Promise<Conversation | null> {
    return this.conversations.get(conversationId) || null;
  }

  /**
   * List conversations for an agent
   */
  async listConversations(
    agentId: string,
    options?: { activeOnly?: boolean; limit?: number },
  ): Promise<Conversation[]> {
    let conversations = Array.from(this.conversations.values())
      .filter(c => c.participants.includes(agentId));

    if (options?.activeOnly) {
      conversations = conversations.filter(c => c.isActive);
    }

    conversations.sort((a, b) => b.lastMessageAt.getTime() - a.lastMessageAt.getTime());

    const limit = options?.limit || 20;
    return conversations.slice(0, limit);
  }

  /**
   * Get messages in a conversation
   */
  async getConversationMessages(
    conversationId: string,
    limit: number = 100,
  ): Promise<AgentMessage[]> {
    return Array.from(this.messages.values())
      .filter(m => m.conversationId === conversationId)
      .sort((a, b) => a.createdAt.getTime() - b.createdAt.getTime())
      .slice(-limit);
  }

  /**
   * Close a conversation
   */
  async closeConversation(conversationId: string): Promise<boolean> {
    const conversation = this.conversations.get(conversationId);
    if (!conversation) return false;

    conversation.isActive = false;
    this.conversations.set(conversationId, conversation);
    return true;
  }

  /**
   * Create task handoff message
   */
  async createTaskHandoff(
    fromAgent: string,
    toAgent: string,
    taskId: string,
    context: Record<string, any>,
  ): Promise<MessageResponse> {
    return this.sendMessage(fromAgent, {
      toAgent,
      messageType: 'task_handoff',
      payload: {
        taskId,
        handoffContext: context,
        handoffTime: new Date(),
      },
      requiresResponse: true,
      responseTimeout: 60000, // 1 minute to acknowledge handoff
    });
  }

  /**
   * Create approval request (human-in-the-loop)
   */
  async requestApproval(
    fromAgent: string,
    approvalData: {
      title: string;
      description: string;
      options: string[];
      metadata?: Record<string, any>;
    },
  ): Promise<MessageResponse> {
    return this.sendMessage(fromAgent, {
      toAgent: '*', // Broadcast to all (including human operators)
      messageType: 'approval_request',
      payload: approvalData,
      requiresResponse: true,
      responseTimeout: 300000, // 5 minutes for human approval
    });
  }

  /**
   * Create a new conversation
   */
  private createConversation(
    id: string,
    fromAgent: string,
    toAgent: string,
    contextOverride?: Partial<ConversationContext>,
  ): Conversation {
    const context: ConversationContext = {
      sessionState: {},
      sharedData: [],
      permissions: [],
      ...contextOverride,
    };

    const conversation: Conversation = {
      id,
      participants: [fromAgent],
      context,
      messageCount: 0,
      lastMessageAt: new Date(),
      createdAt: new Date(),
      isActive: true,
    };

    if (toAgent !== '*') {
      conversation.participants.push(toAgent);
    }

    this.conversations.set(id, conversation);
    return conversation;
  }

  /**
   * Wait for response to a message
   */
  private async waitForResponse(messageId: string, timeoutMs: number): Promise<AgentMessage> {
    return new Promise((resolve, reject) => {
      const startTime = Date.now();

      const checkInterval = setInterval(() => {
        // Check for response message
        for (const msg of this.messages.values()) {
          if (msg.responseId === messageId) {
            clearInterval(checkInterval);
            resolve(msg);
            return;
          }
        }

        // Check timeout
        if (Date.now() - startTime > timeoutMs) {
          clearInterval(checkInterval);
          reject(new Error('Response timeout'));
        }
      }, 500);
    });
  }
}
