/**
 * Agent Registry Types
 * Defines agent structures for the AI orchestrator
 */

export type AgentType =
  | 'mcp_native'      // Direct MCP tool access (Claude Code, etc.)
  | 'llm_agent'       // Claude/GPT with tool calling
  | 'specialized'     // Domain-specific agents (FP&A, DBA)
  | 'excel_plugin'    // Excel Add-in client
  | 'power_bi'        // Power BI connector
  | 'external';       // Third-party integrations

export type HealthStatus =
  | 'healthy'
  | 'degraded'
  | 'unhealthy'
  | 'offline';

export interface Capability {
  tool: string;           // MCP tool name or capability ID
  proficiency: number;    // 0-1 confidence score
  constraints?: string[]; // Any limitations
}

export interface RegisteredAgent {
  id: string;
  name: string;
  type: AgentType;
  capabilities: Capability[];
  maxConcurrentTasks: number;
  currentLoad: number;
  healthStatus: HealthStatus;
  callbackUrl?: string;
  lastHeartbeat: Date;
  registeredAt: Date;
  metadata?: Record<string, any>;
}

export interface RegisterAgentDto {
  id: string;
  name: string;
  type: AgentType;
  capabilities: Capability[];
  maxConcurrentTasks?: number;
  callbackUrl?: string;
  metadata?: Record<string, any>;
}

export interface AgentHeartbeatDto {
  currentLoad?: number;
  healthStatus?: HealthStatus;
  metadata?: Record<string, any>;
}

export interface AgentHeartbeatResponse {
  acknowledged: boolean;
  serverTime: Date;
  pendingMessages: number;
  assignedTasks: string[];
}

export interface AgentListResponse {
  agents: RegisteredAgent[];
  total: number;
  healthy: number;
  degraded: number;
  offline: number;
}

export interface AgentCapabilityQuery {
  tool?: string;
  type?: AgentType;
  minProficiency?: number;
  healthyOnly?: boolean;
}

// Health check thresholds
export const HEARTBEAT_TIMEOUT_MS = 30000; // 30 seconds
export const DEGRADED_THRESHOLD_MS = 15000; // 15 seconds without heartbeat
