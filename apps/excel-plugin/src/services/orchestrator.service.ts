/**
 * Orchestrator Service
 *
 * Handles agent registration and orchestrator communication.
 */

import { apiService } from './api.service';

interface AgentCapability {
  tool: string;
  proficiency: number;
  description?: string;
}

interface RegisterAgentRequest {
  id: string;
  name: string;
  type: 'excel_plugin';
  capabilities: AgentCapability[];
  callbackUrl?: string;
  maxConcurrent?: number;
}

interface RegisterAgentResponse {
  success: boolean;
  message: string;
}

interface HeartbeatResponse {
  success: boolean;
  tasks?: any[];
}

class OrchestratorService {
  private agentId: string | null = null;
  private heartbeatInterval: ReturnType<typeof setInterval> | null = null;

  /**
   * Generate a unique device ID for this Excel instance
   */
  private getDeviceId(): string {
    let deviceId = localStorage.getItem('excel_device_id');
    if (!deviceId) {
      deviceId = `excel-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
      localStorage.setItem('excel_device_id', deviceId);
    }
    return deviceId;
  }

  /**
   * Register this Excel plugin as an agent with the orchestrator
   */
  async registerAgent(): Promise<boolean> {
    const deviceId = this.getDeviceId();

    const request: RegisterAgentRequest = {
      id: deviceId,
      name: 'Excel Plugin',
      type: 'excel_plugin',
      capabilities: [
        { tool: 'display_data', proficiency: 1.0, description: 'Display data in Excel sheets' },
        { tool: 'user_input', proficiency: 1.0, description: 'Collect user input via dialogs' },
        { tool: 'sheet_reconcile', proficiency: 0.9, description: 'Reconcile and compare sheets' },
        { tool: 'hierarchy_mapping', proficiency: 0.9, description: 'Map data to hierarchies' },
        { tool: 'data_profiling', proficiency: 0.8, description: 'Profile data quality' },
      ],
      maxConcurrent: 3,
    };

    try {
      const response = await apiService.request<RegisterAgentResponse>(
        '/orchestrator/agents/register',
        {
          method: 'POST',
          body: JSON.stringify(request),
        }
      );

      if (response.success) {
        this.agentId = deviceId;
        this.startHeartbeat();
        console.log(`Agent registered: ${deviceId}`);
        return true;
      }

      return false;
    } catch (error) {
      console.error('Failed to register agent:', error);
      return false;
    }
  }

  /**
   * Unregister this agent from the orchestrator
   */
  async unregisterAgent(): Promise<void> {
    if (!this.agentId) return;

    this.stopHeartbeat();

    try {
      await apiService.request(`/orchestrator/agents/${this.agentId}`, {
        method: 'DELETE',
      });
      console.log(`Agent unregistered: ${this.agentId}`);
    } catch (error) {
      console.error('Failed to unregister agent:', error);
    }

    this.agentId = null;
  }

  /**
   * Start sending heartbeats to maintain agent health
   */
  private startHeartbeat(): void {
    if (this.heartbeatInterval) return;

    // Send heartbeat every 30 seconds
    this.heartbeatInterval = setInterval(async () => {
      await this.sendHeartbeat();
    }, 30000);

    // Send initial heartbeat
    this.sendHeartbeat();
  }

  /**
   * Stop the heartbeat interval
   */
  private stopHeartbeat(): void {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
  }

  /**
   * Send a heartbeat to the orchestrator
   */
  private async sendHeartbeat(): Promise<void> {
    if (!this.agentId) return;

    try {
      const response = await apiService.request<HeartbeatResponse>(
        `/orchestrator/agents/${this.agentId}/heartbeat`,
        { method: 'POST' }
      );

      // Handle any pending tasks assigned to this agent
      if (response.tasks && response.tasks.length > 0) {
        console.log(`Received ${response.tasks.length} pending tasks`);
        // TODO: Process assigned tasks
      }
    } catch (error) {
      console.warn('Heartbeat failed:', error);
    }
  }

  /**
   * Get messages sent to this agent
   */
  async getMessages(): Promise<any[]> {
    if (!this.agentId) return [];

    try {
      const response = await apiService.request<{ messages: any[] }>(
        `/orchestrator/messages/${this.agentId}`
      );
      return response.messages || [];
    } catch (error) {
      console.error('Failed to get messages:', error);
      return [];
    }
  }

  /**
   * Check if agent is registered
   */
  isRegistered(): boolean {
    return this.agentId !== null;
  }

  /**
   * Get the current agent ID
   */
  getAgentId(): string | null {
    return this.agentId;
  }
}

export const orchestratorService = new OrchestratorService();
