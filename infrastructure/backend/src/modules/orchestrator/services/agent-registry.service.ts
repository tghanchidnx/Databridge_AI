/**
 * Agent Registry Service
 *
 * Manages AI agent registration and health monitoring:
 * - Agent registration with capabilities
 * - Heartbeat monitoring
 * - Health status tracking
 * - Load balancing for task assignment
 * - Capability-based agent lookup
 */

import { Injectable, Logger } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';
import {
  RegisteredAgent,
  RegisterAgentDto,
  AgentHeartbeatDto,
  AgentHeartbeatResponse,
  AgentListResponse,
  AgentCapabilityQuery,
  HealthStatus,
  HEARTBEAT_TIMEOUT_MS,
  DEGRADED_THRESHOLD_MS,
} from '../types/agent.types';

@Injectable()
export class AgentRegistryService {
  private readonly logger = new Logger(AgentRegistryService.name);

  // In-memory agent store (will be replaced with Prisma in Phase 1.2)
  private agents: Map<string, RegisteredAgent> = new Map();

  // Pending messages per agent (for heartbeat response)
  private pendingMessages: Map<string, string[]> = new Map();

  // Assigned tasks per agent
  private assignedTasks: Map<string, string[]> = new Map();

  /**
   * Register a new agent
   */
  async registerAgent(dto: RegisterAgentDto): Promise<RegisteredAgent> {
    const now = new Date();

    const agent: RegisteredAgent = {
      id: dto.id,
      name: dto.name,
      type: dto.type,
      capabilities: dto.capabilities,
      maxConcurrentTasks: dto.maxConcurrentTasks || 5,
      currentLoad: 0,
      healthStatus: 'healthy',
      callbackUrl: dto.callbackUrl,
      lastHeartbeat: now,
      registeredAt: now,
      metadata: dto.metadata,
    };

    this.agents.set(dto.id, agent);
    this.pendingMessages.set(dto.id, []);
    this.assignedTasks.set(dto.id, []);

    this.logger.log(`Agent registered: ${dto.id} (${dto.type})`);
    return agent;
  }

  /**
   * Unregister an agent
   */
  async unregisterAgent(agentId: string): Promise<boolean> {
    const existed = this.agents.delete(agentId);
    this.pendingMessages.delete(agentId);
    this.assignedTasks.delete(agentId);

    if (existed) {
      this.logger.log(`Agent unregistered: ${agentId}`);
    }
    return existed;
  }

  /**
   * Process agent heartbeat
   */
  async processHeartbeat(
    agentId: string,
    dto: AgentHeartbeatDto,
  ): Promise<AgentHeartbeatResponse | null> {
    const agent = this.agents.get(agentId);
    if (!agent) return null;

    // Update agent state
    agent.lastHeartbeat = new Date();
    if (dto.currentLoad !== undefined) {
      agent.currentLoad = dto.currentLoad;
    }
    if (dto.healthStatus) {
      agent.healthStatus = dto.healthStatus;
    }
    if (dto.metadata) {
      agent.metadata = { ...agent.metadata, ...dto.metadata };
    }

    this.agents.set(agentId, agent);

    // Get pending messages and tasks
    const messages = this.pendingMessages.get(agentId) || [];
    const tasks = this.assignedTasks.get(agentId) || [];

    return {
      acknowledged: true,
      serverTime: new Date(),
      pendingMessages: messages.length,
      assignedTasks: tasks,
    };
  }

  /**
   * Get agent by ID
   */
  async getAgent(agentId: string): Promise<RegisteredAgent | null> {
    return this.agents.get(agentId) || null;
  }

  /**
   * List all agents with optional filters
   */
  async listAgents(query?: AgentCapabilityQuery & { limit?: number }): Promise<AgentListResponse> {
    let agents = Array.from(this.agents.values());

    // Apply filters
    if (query?.type) {
      agents = agents.filter(a => a.type === query.type);
    }
    if (query?.tool) {
      agents = agents.filter(a =>
        a.capabilities.some(c => c.tool === query.tool),
      );
    }
    if (query?.minProficiency !== undefined) {
      agents = agents.filter(a =>
        a.capabilities.some(c => c.proficiency >= query.minProficiency!),
      );
    }
    if (query?.healthyOnly) {
      agents = agents.filter(a => a.healthStatus === 'healthy');
    }

    // Calculate counts
    const allAgents = Array.from(this.agents.values());
    const counts = {
      healthy: allAgents.filter(a => a.healthStatus === 'healthy').length,
      degraded: allAgents.filter(a => a.healthStatus === 'degraded').length,
      offline: allAgents.filter(a => a.healthStatus === 'offline' || a.healthStatus === 'unhealthy').length,
    };

    // Limit results
    const limit = query?.limit || 50;
    agents = agents.slice(0, limit);

    return {
      agents,
      total: this.agents.size,
      ...counts,
    };
  }

  /**
   * Find best agent for a task based on capabilities
   */
  async findAgentForTask(
    requiredCapabilities: string[],
    preferredType?: string,
  ): Promise<RegisteredAgent | null> {
    let candidates = Array.from(this.agents.values()).filter(agent => {
      // Must be healthy and have capacity
      if (agent.healthStatus !== 'healthy') return false;
      if (agent.currentLoad >= agent.maxConcurrentTasks) return false;

      // Must have all required capabilities
      return requiredCapabilities.every(cap =>
        agent.capabilities.some(c => c.tool === cap),
      );
    });

    if (candidates.length === 0) return null;

    // Prefer agents of specified type
    if (preferredType) {
      const preferred = candidates.filter(a => a.type === preferredType);
      if (preferred.length > 0) {
        candidates = preferred;
      }
    }

    // Sort by load (prefer less loaded agents)
    candidates.sort((a, b) => a.currentLoad - b.currentLoad);

    return candidates[0];
  }

  /**
   * Assign task to agent
   */
  async assignTaskToAgent(agentId: string, taskId: string): Promise<boolean> {
    const agent = this.agents.get(agentId);
    if (!agent) return false;

    agent.currentLoad++;
    this.agents.set(agentId, agent);

    const tasks = this.assignedTasks.get(agentId) || [];
    tasks.push(taskId);
    this.assignedTasks.set(agentId, tasks);

    return true;
  }

  /**
   * Release task from agent
   */
  async releaseTaskFromAgent(agentId: string, taskId: string): Promise<boolean> {
    const agent = this.agents.get(agentId);
    if (!agent) return false;

    agent.currentLoad = Math.max(0, agent.currentLoad - 1);
    this.agents.set(agentId, agent);

    const tasks = this.assignedTasks.get(agentId) || [];
    const index = tasks.indexOf(taskId);
    if (index > -1) {
      tasks.splice(index, 1);
      this.assignedTasks.set(agentId, tasks);
    }

    return true;
  }

  /**
   * Queue message for agent
   */
  async queueMessageForAgent(agentId: string, messageId: string): Promise<boolean> {
    const messages = this.pendingMessages.get(agentId);
    if (!messages) return false;

    messages.push(messageId);
    this.pendingMessages.set(agentId, messages);
    return true;
  }

  /**
   * Periodic health check - runs every 10 seconds
   */
  @Cron(CronExpression.EVERY_10_SECONDS)
  async checkAgentHealth() {
    const now = Date.now();

    for (const [agentId, agent] of this.agents) {
      const timeSinceHeartbeat = now - agent.lastHeartbeat.getTime();

      let newStatus: HealthStatus = agent.healthStatus;

      if (timeSinceHeartbeat > HEARTBEAT_TIMEOUT_MS) {
        newStatus = 'offline';
      } else if (timeSinceHeartbeat > DEGRADED_THRESHOLD_MS) {
        newStatus = 'degraded';
      } else if (agent.healthStatus === 'degraded' || agent.healthStatus === 'offline') {
        newStatus = 'healthy';
      }

      if (newStatus !== agent.healthStatus) {
        this.logger.log(`Agent ${agentId} health changed: ${agent.healthStatus} -> ${newStatus}`);
        agent.healthStatus = newStatus;
        this.agents.set(agentId, agent);
      }
    }
  }
}
