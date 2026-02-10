import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { ThrottlerModule } from '@nestjs/throttler';
import { ScheduleModule } from '@nestjs/schedule';
import configuration from './config/configuration';
import { DatabaseModule } from './database/database.module';
import { AuthModule } from './modules/auth/auth.module';
import { UsersModule } from './modules/users/users.module';
import { ConnectionsModule } from './modules/connections/connections.module';
import { SchemaMatcherModule } from './modules/schema-matcher/schema-matcher.module';
import { DataMatcherModule } from './modules/data-matcher/data-matcher.module';
import { ReportsModule } from './modules/reports/reports.module';
import { HealthModule } from './modules/health/health.module';
import { TestModule } from './modules/test/test.module';
import { AiModule } from './modules/ai/ai.module';
import { OrganizationsModule } from './modules/organizations/organizations.module';
import { SmartHierarchyModule } from './modules/smart-hierarchy/smart-hierarchy.module';
import { ExcelModule } from './modules/excel/excel.module';
import { TemplatesModule } from './modules/templates/templates.module';
import { OrchestratorModule } from './modules/orchestrator/orchestrator.module';
import { WinstonModule } from 'nest-winston';
import { winstonConfig } from './config/winston.config';

@Module({
  imports: [
    // Configuration
    ConfigModule.forRoot({
      isGlobal: true,
      load: [configuration],
      envFilePath: `.env`,
    }),

    // Logging
    WinstonModule.forRoot(winstonConfig),

    // Rate limiting
    ThrottlerModule.forRootAsync({
      imports: [ConfigModule],
      inject: [ConfigService],
      useFactory: (config: ConfigService) => [
        {
          ttl: config.get('THROTTLE_TTL', 60) * 1000,
          limit: config.get('THROTTLE_LIMIT', 100),
        },
      ],
    }),

    // Task scheduling
    ScheduleModule.forRoot(),

    // Database
    DatabaseModule,

    // Feature modules
    AuthModule,
    UsersModule,
    ConnectionsModule,
    SchemaMatcherModule,
    DataMatcherModule,
    ReportsModule,
    HealthModule,
    TestModule,
    AiModule,
    OrganizationsModule,
    SmartHierarchyModule,
    ExcelModule,
    TemplatesModule,
    OrchestratorModule,
  ],
})
export class AppModule {}
