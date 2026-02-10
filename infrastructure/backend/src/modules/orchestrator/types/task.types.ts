/**
 * Orchestrator Task Types
 * Defines task structures for the AI orchestrator job queue
 */

export type TaskType =
  | 'hierarchy_build'
  | 'data_reconciliation'
  | 'sql_analysis'
  | 'mapping_suggestion'
  | 'report_generation'
  | 'deployment'
  | 'agent_handoff'
  | 'workflow_step'
  | 'custom';

export type TaskStatus =
  | 'pending'
  | 'queued'
  | 'in_progress'
  | 'waiting_input'
  | 'completed'
  | 'failed'
  | 'cancelled';

export type Priority = 'low' | 'normal' | 'high' | 'critical';

export interface Checkpoint {
  id: string;
  stepIndex: number;
  stepName: string;
  data: Record<string, any>;
  createdAt: Date;
}

export interface TaskResult {
  success: boolean;
  data?: any;
  error?: string;
  mcpToolsCalled: string[];
  duration: number;
}

export interface OrchestratorTask {
  id: string;
  type: TaskType;
  status: TaskStatus;
  priority: Priority;
  priorityValue: number; // numeric for sorting (critical=100, high=75, normal=50, low=25)
  payload: Record<string, any>;
  dependencies: string[]; // Task IDs that must complete first
  assignedAgent?: string;
  result?: TaskResult;
  checkpoints: Checkpoint[];
  callbackUrl?: string; // Webhook for completion notification
  createdAt: Date;
  startedAt?: Date;
  completedAt?: Date;
  metadata?: Record<string, any>;
}

export interface CreateTaskDto {
  type: TaskType;
  payload: Record<string, any>;
  priority?: Priority;
  dependencies?: string[];
  callbackUrl?: string;
  metadata?: Record<string, any>;
}

export interface TaskStatusResponse {
  task: OrchestratorTask;
  progress: number; // 0-100
  estimatedCompletion?: Date;
  position?: number; // queue position if pending
}

export interface TaskListResponse {
  tasks: OrchestratorTask[];
  total: number;
  pending: number;
  inProgress: number;
  completed: number;
  failed: number;
}

// Priority value mapping
export const PRIORITY_VALUES: Record<Priority, number> = {
  critical: 100,
  high: 75,
  normal: 50,
  low: 25,
};
