/**
 * Task Manager Service
 *
 * Manages the orchestrator job queue with:
 * - Task submission with priorities
 * - Dependency resolution
 * - Task status tracking
 * - Checkpointing for resumability
 */

import { Injectable, Logger } from '@nestjs/common';
import { InjectQueue } from '@nestjs/bull';
import { Queue, Job } from 'bull';
import { v4 as uuidv4 } from 'uuid';
import {
  OrchestratorTask,
  CreateTaskDto,
  TaskStatus,
  TaskStatusResponse,
  TaskListResponse,
  PRIORITY_VALUES,
} from '../types/task.types';

@Injectable()
export class TaskManagerService {
  private readonly logger = new Logger(TaskManagerService.name);

  // In-memory task store (will be replaced with Prisma in Phase 1.2)
  private tasks: Map<string, OrchestratorTask> = new Map();

  constructor(
    @InjectQueue('orchestrator-tasks') private taskQueue: Queue,
  ) {
    this.initializeQueueHandlers();
  }

  private initializeQueueHandlers() {
    this.taskQueue.on('completed', (job: Job, result: any) => {
      this.logger.log(`Task ${job.id} completed`);
      this.updateTaskStatus(job.id as string, 'completed', result);
    });

    this.taskQueue.on('failed', (job: Job, err: Error) => {
      this.logger.error(`Task ${job.id} failed: ${err.message}`);
      this.updateTaskStatus(job.id as string, 'failed', { error: err.message });
    });
  }

  /**
   * Submit a new task to the orchestrator
   */
  async submitTask(dto: CreateTaskDto): Promise<OrchestratorTask> {
    const taskId = uuidv4();
    const priority = dto.priority || 'normal';
    const priorityValue = PRIORITY_VALUES[priority];

    const task: OrchestratorTask = {
      id: taskId,
      type: dto.type,
      status: 'pending',
      priority,
      priorityValue,
      payload: dto.payload,
      dependencies: dto.dependencies || [],
      callbackUrl: dto.callbackUrl,
      checkpoints: [],
      createdAt: new Date(),
      metadata: dto.metadata,
    };

    // Store task
    this.tasks.set(taskId, task);

    // Check dependencies
    const canExecute = await this.checkDependencies(task);

    if (canExecute) {
      // Add to queue
      await this.taskQueue.add(
        'process-task',
        { taskId, ...task },
        {
          jobId: taskId,
          priority: 100 - priorityValue, // Bull uses lower = higher priority
        },
      );
      task.status = 'queued';
      this.tasks.set(taskId, task);
    }

    this.logger.log(`Task ${taskId} created with status: ${task.status}`);
    return task;
  }

  /**
   * Get task status and progress
   */
  async getTaskStatus(taskId: string): Promise<TaskStatusResponse | null> {
    const task = this.tasks.get(taskId);
    if (!task) return null;

    // Calculate progress based on checkpoints and status
    let progress = 0;
    if (task.status === 'completed') progress = 100;
    else if (task.status === 'in_progress') progress = 50;
    else if (task.status === 'queued') progress = 10;

    // Get queue position if pending
    let position: number | undefined;
    if (task.status === 'queued') {
      const job = await this.taskQueue.getJob(taskId);
      if (job) {
        const waiting = await this.taskQueue.getWaiting();
        position = waiting.findIndex(j => j.id === taskId) + 1;
      }
    }

    return {
      task,
      progress,
      position,
    };
  }

  /**
   * Cancel a task
   */
  async cancelTask(taskId: string): Promise<boolean> {
    const task = this.tasks.get(taskId);
    if (!task) return false;

    if (task.status === 'completed' || task.status === 'failed') {
      return false; // Cannot cancel finished tasks
    }

    // Remove from queue if queued
    const job = await this.taskQueue.getJob(taskId);
    if (job) {
      await job.remove();
    }

    task.status = 'cancelled';
    task.completedAt = new Date();
    this.tasks.set(taskId, task);

    this.logger.log(`Task ${taskId} cancelled`);
    return true;
  }

  /**
   * List tasks with optional filters
   */
  async listTasks(filters?: {
    status?: TaskStatus;
    type?: string;
    assignedAgent?: string;
    limit?: number;
    offset?: number;
  }): Promise<TaskListResponse> {
    let tasks = Array.from(this.tasks.values());

    // Apply filters
    if (filters?.status) {
      tasks = tasks.filter(t => t.status === filters.status);
    }
    if (filters?.type) {
      tasks = tasks.filter(t => t.type === filters.type);
    }
    if (filters?.assignedAgent) {
      tasks = tasks.filter(t => t.assignedAgent === filters.assignedAgent);
    }

    // Sort by priority and creation time
    tasks.sort((a, b) => {
      if (a.priorityValue !== b.priorityValue) {
        return b.priorityValue - a.priorityValue;
      }
      return a.createdAt.getTime() - b.createdAt.getTime();
    });

    // Calculate counts
    const allTasks = Array.from(this.tasks.values());
    const counts = {
      pending: allTasks.filter(t => t.status === 'pending' || t.status === 'queued').length,
      inProgress: allTasks.filter(t => t.status === 'in_progress').length,
      completed: allTasks.filter(t => t.status === 'completed').length,
      failed: allTasks.filter(t => t.status === 'failed').length,
    };

    // Paginate
    const offset = filters?.offset || 0;
    const limit = filters?.limit || 20;
    const paginatedTasks = tasks.slice(offset, offset + limit);

    return {
      tasks: paginatedTasks,
      total: tasks.length,
      ...counts,
    };
  }

  /**
   * Check if all dependencies are completed
   */
  private async checkDependencies(task: OrchestratorTask): Promise<boolean> {
    if (!task.dependencies.length) return true;

    for (const depId of task.dependencies) {
      const depTask = this.tasks.get(depId);
      if (!depTask || depTask.status !== 'completed') {
        return false;
      }
    }
    return true;
  }

  /**
   * Update task status (called by queue handlers)
   */
  private updateTaskStatus(taskId: string, status: TaskStatus, result?: any) {
    const task = this.tasks.get(taskId);
    if (!task) return;

    task.status = status;
    if (status === 'completed' || status === 'failed') {
      task.completedAt = new Date();
      task.result = result;
    }
    if (status === 'in_progress') {
      task.startedAt = new Date();
    }

    this.tasks.set(taskId, task);

    // Re-check dependent tasks
    this.checkWaitingTasks();
  }

  /**
   * Check waiting tasks and queue those with satisfied dependencies
   */
  private async checkWaitingTasks() {
    for (const task of this.tasks.values()) {
      if (task.status === 'pending') {
        const canExecute = await this.checkDependencies(task);
        if (canExecute) {
          await this.taskQueue.add(
            'process-task',
            { taskId: task.id, ...task },
            {
              jobId: task.id,
              priority: 100 - task.priorityValue,
            },
          );
          task.status = 'queued';
          this.tasks.set(task.id, task);
        }
      }
    }
  }
}
