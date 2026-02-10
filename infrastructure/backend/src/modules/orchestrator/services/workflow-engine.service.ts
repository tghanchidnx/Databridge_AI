/**
 * Workflow Engine Service
 *
 * Executes multi-step workflows with:
 * - Step sequencing and parallel execution
 * - Checkpoint management for resumability
 * - Conditional branching
 * - Human approval gates
 */

import { Injectable, Logger } from '@nestjs/common';
import { v4 as uuidv4 } from 'uuid';
import { TaskManagerService } from './task-manager.service';
import { AiLinkService } from './ai-link.service';
import { EventBusService } from './event-bus.service';
import { EVENT_CHANNELS } from '../types/message.types';

export type StepType = 'task' | 'parallel' | 'conditional' | 'approval' | 'wait';

export interface WorkflowStep {
  id: string;
  name: string;
  type: StepType;
  config: Record<string, any>;
  onSuccess?: string; // next step ID
  onFailure?: string; // step ID on failure
  timeout?: number;   // milliseconds
}

export interface Workflow {
  id: string;
  name: string;
  description?: string;
  steps: WorkflowStep[];
  triggers?: WorkflowTrigger[];
  createdAt: Date;
  updatedAt: Date;
}

export interface WorkflowTrigger {
  type: 'event' | 'schedule' | 'manual';
  config: Record<string, any>;
}

export interface WorkflowExecution {
  id: string;
  workflowId: string;
  status: 'running' | 'paused' | 'completed' | 'failed' | 'cancelled';
  currentStepId: string;
  completedSteps: string[];
  stepResults: Map<string, any>;
  startedAt: Date;
  completedAt?: Date;
  error?: string;
}

@Injectable()
export class WorkflowEngineService {
  private readonly logger = new Logger(WorkflowEngineService.name);

  // In-memory stores
  private workflows: Map<string, Workflow> = new Map();
  private executions: Map<string, WorkflowExecution> = new Map();

  constructor(
    private taskManager: TaskManagerService,
    private aiLink: AiLinkService,
    private eventBus: EventBusService,
  ) {}

  /**
   * Create a new workflow
   */
  async createWorkflow(
    name: string,
    steps: WorkflowStep[],
    options?: {
      description?: string;
      triggers?: WorkflowTrigger[];
    },
  ): Promise<Workflow> {
    const workflow: Workflow = {
      id: uuidv4(),
      name,
      description: options?.description,
      steps,
      triggers: options?.triggers,
      createdAt: new Date(),
      updatedAt: new Date(),
    };

    this.workflows.set(workflow.id, workflow);
    this.logger.log(`Workflow created: ${workflow.id} (${name})`);
    return workflow;
  }

  /**
   * Get workflow by ID
   */
  async getWorkflow(workflowId: string): Promise<Workflow | null> {
    return this.workflows.get(workflowId) || null;
  }

  /**
   * List all workflows
   */
  async listWorkflows(): Promise<Workflow[]> {
    return Array.from(this.workflows.values());
  }

  /**
   * Start workflow execution
   */
  async executeWorkflow(
    workflowId: string,
    initialContext?: Record<string, any>,
  ): Promise<WorkflowExecution> {
    const workflow = this.workflows.get(workflowId);
    if (!workflow) {
      throw new Error(`Workflow not found: ${workflowId}`);
    }

    if (workflow.steps.length === 0) {
      throw new Error('Workflow has no steps');
    }

    const execution: WorkflowExecution = {
      id: uuidv4(),
      workflowId,
      status: 'running',
      currentStepId: workflow.steps[0].id,
      completedSteps: [],
      stepResults: new Map(),
      startedAt: new Date(),
    };

    if (initialContext) {
      execution.stepResults.set('_initial', initialContext);
    }

    this.executions.set(execution.id, execution);

    // Start execution asynchronously
    this.runExecution(execution.id).catch(err => {
      this.logger.error(`Workflow execution failed: ${err.message}`);
    });

    this.logger.log(`Workflow execution started: ${execution.id}`);
    return execution;
  }

  /**
   * Get execution status
   */
  async getExecution(executionId: string): Promise<WorkflowExecution | null> {
    return this.executions.get(executionId) || null;
  }

  /**
   * Pause execution
   */
  async pauseExecution(executionId: string): Promise<boolean> {
    const execution = this.executions.get(executionId);
    if (!execution || execution.status !== 'running') return false;

    execution.status = 'paused';
    this.executions.set(executionId, execution);
    return true;
  }

  /**
   * Resume execution
   */
  async resumeExecution(executionId: string): Promise<boolean> {
    const execution = this.executions.get(executionId);
    if (!execution || execution.status !== 'paused') return false;

    execution.status = 'running';
    this.executions.set(executionId, execution);

    // Continue execution
    this.runExecution(executionId).catch(err => {
      this.logger.error(`Workflow execution failed: ${err.message}`);
    });

    return true;
  }

  /**
   * Cancel execution
   */
  async cancelExecution(executionId: string): Promise<boolean> {
    const execution = this.executions.get(executionId);
    if (!execution) return false;
    if (execution.status === 'completed' || execution.status === 'failed') return false;

    execution.status = 'cancelled';
    execution.completedAt = new Date();
    this.executions.set(executionId, execution);
    return true;
  }

  /**
   * Run the workflow execution
   */
  private async runExecution(executionId: string): Promise<void> {
    const execution = this.executions.get(executionId);
    if (!execution) return;

    const workflow = this.workflows.get(execution.workflowId);
    if (!workflow) return;

    while (execution.status === 'running') {
      const currentStep = workflow.steps.find(s => s.id === execution.currentStepId);
      if (!currentStep) {
        execution.status = 'failed';
        execution.error = `Step not found: ${execution.currentStepId}`;
        break;
      }

      try {
        const result = await this.executeStep(currentStep, execution);
        execution.stepResults.set(currentStep.id, result);
        execution.completedSteps.push(currentStep.id);

        // Determine next step
        const nextStepId = currentStep.onSuccess;
        if (!nextStepId) {
          // No more steps, workflow complete
          execution.status = 'completed';
          execution.completedAt = new Date();
        } else {
          execution.currentStepId = nextStepId;
        }
      } catch (err) {
        this.logger.error(`Step ${currentStep.id} failed: ${err.message}`);

        if (currentStep.onFailure) {
          execution.currentStepId = currentStep.onFailure;
        } else {
          execution.status = 'failed';
          execution.error = err.message;
          execution.completedAt = new Date();
        }
      }

      this.executions.set(executionId, execution);
    }

    // Publish completion event
    await this.eventBus.publish(EVENT_CHANNELS.TASK_COMPLETED, {
      type: 'workflow',
      executionId,
      status: execution.status,
      workflowId: execution.workflowId,
    });
  }

  /**
   * Execute a single step
   */
  private async executeStep(
    step: WorkflowStep,
    execution: WorkflowExecution,
  ): Promise<any> {
    this.logger.log(`Executing step: ${step.id} (${step.type})`);

    switch (step.type) {
      case 'task':
        return this.executeTaskStep(step, execution);

      case 'parallel':
        return this.executeParallelStep(step, execution);

      case 'conditional':
        return this.executeConditionalStep(step, execution);

      case 'approval':
        return this.executeApprovalStep(step, execution);

      case 'wait':
        return this.executeWaitStep(step);

      default:
        throw new Error(`Unknown step type: ${step.type}`);
    }
  }

  private async executeTaskStep(step: WorkflowStep, execution: WorkflowExecution): Promise<any> {
    const task = await this.taskManager.submitTask({
      type: step.config.taskType || 'workflow_step',
      payload: {
        ...step.config.payload,
        _workflowId: execution.workflowId,
        _executionId: execution.id,
        _stepId: step.id,
        _previousResults: Object.fromEntries(execution.stepResults),
      },
      priority: step.config.priority || 'normal',
    });

    // Wait for task completion (polling)
    let status = await this.taskManager.getTaskStatus(task.id);
    const startTime = Date.now();
    const timeout = step.timeout || 300000; // 5 min default

    while (status && status.task.status !== 'completed' && status.task.status !== 'failed') {
      if (Date.now() - startTime > timeout) {
        await this.taskManager.cancelTask(task.id);
        throw new Error('Task timeout');
      }
      await new Promise(resolve => setTimeout(resolve, 1000));
      status = await this.taskManager.getTaskStatus(task.id);
    }

    if (status?.task.status === 'failed') {
      throw new Error(status.task.result?.error || 'Task failed');
    }

    return status?.task.result;
  }

  private async executeParallelStep(step: WorkflowStep, execution: WorkflowExecution): Promise<any> {
    const parallelTasks = step.config.tasks || [];
    const results = await Promise.all(
      parallelTasks.map((taskConfig: any) =>
        this.taskManager.submitTask({
          type: taskConfig.taskType || 'workflow_step',
          payload: taskConfig.payload,
          priority: taskConfig.priority || 'normal',
        }),
      ),
    );
    return results;
  }

  private async executeConditionalStep(step: WorkflowStep, execution: WorkflowExecution): Promise<any> {
    const condition = step.config.condition;
    const previousResults = Object.fromEntries(execution.stepResults);

    // Simple condition evaluation (can be extended)
    let result = false;
    if (typeof condition === 'function') {
      result = condition(previousResults);
    } else if (typeof condition === 'string') {
      // Evaluate simple expressions like "stepA.success === true"
      try {
        result = eval(`(${JSON.stringify(previousResults)}) => ${condition}`)(previousResults);
      } catch {
        result = false;
      }
    }

    return { conditionMet: result };
  }

  private async executeApprovalStep(step: WorkflowStep, execution: WorkflowExecution): Promise<any> {
    const response = await this.aiLink.requestApproval('workflow-engine', {
      title: step.config.title || 'Approval Required',
      description: step.config.description || '',
      options: step.config.options || ['Approve', 'Reject'],
      metadata: {
        workflowId: execution.workflowId,
        executionId: execution.id,
        stepId: step.id,
      },
    });

    return response;
  }

  private async executeWaitStep(step: WorkflowStep): Promise<any> {
    const duration = step.config.duration || 1000;
    await new Promise(resolve => setTimeout(resolve, duration));
    return { waited: duration };
  }
}
