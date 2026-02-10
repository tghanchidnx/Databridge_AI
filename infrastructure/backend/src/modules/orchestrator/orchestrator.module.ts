/**
 * Orchestrator Module
 *
 * Provides AI orchestration capabilities including:
 * - Task Manager: Job queue with priorities and dependencies
 * - Agent Registry: Agent registration, health monitoring, capability matching
 * - AI-Link-Orchestrator: Agent-to-agent messaging and conversation management
 * - Event Bus: Redis Pub/Sub for real-time notifications
 * - Workflow Engine: Multi-step workflow execution with checkpoints
 */

import { Module } from '@nestjs/common';
import { BullModule } from '@nestjs/bull';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { DatabaseModule } from '../../database/database.module';

// Controllers
import { OrchestratorController } from './controllers/orchestrator.controller';

// Services
import { TaskManagerService } from './services/task-manager.service';
import { AgentRegistryService } from './services/agent-registry.service';
import { EventBusService } from './services/event-bus.service';
import { AiLinkService } from './services/ai-link.service';
import { WorkflowEngineService } from './services/workflow-engine.service';

@Module({
  imports: [
    DatabaseModule,
    // Bull queue for task management
    BullModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (configService: ConfigService) => ({
        redis: {
          host: configService.get('REDIS_HOST', 'localhost'),
          port: configService.get('REDIS_PORT', 6379),
        },
        defaultJobOptions: {
          removeOnComplete: 100, // Keep last 100 completed jobs
          removeOnFail: 50,      // Keep last 50 failed jobs
          attempts: 3,
          backoff: {
            type: 'exponential',
            delay: 1000,
          },
        },
      }),
    }),
    // Task queue
    BullModule.registerQueue({
      name: 'orchestrator-tasks',
    }),
  ],
  controllers: [OrchestratorController],
  providers: [
    TaskManagerService,
    AgentRegistryService,
    EventBusService,
    AiLinkService,
    WorkflowEngineService,
  ],
  exports: [
    TaskManagerService,
    AgentRegistryService,
    EventBusService,
    AiLinkService,
    WorkflowEngineService,
  ],
})
export class OrchestratorModule {}
