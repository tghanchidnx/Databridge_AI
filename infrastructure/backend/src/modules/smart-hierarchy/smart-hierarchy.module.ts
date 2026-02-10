import { Module } from '@nestjs/common';
import { DatabaseModule } from '../../database/database.module';
import { ConnectionsModule } from '../connections/connections.module';
import { SmartHierarchyController } from './controllers/smart-hierarchy.controller';
import { SmartHierarchyService } from './services/smart-hierarchy.service';
import { ScriptGeneratorService } from './services/script-generator.service';
import { SnowflakeDeploymentService } from './services/snowflake-deployment.service';
import { CsvExportImportService } from './services/csv-export-import.service';
import { OldFormatMappingService } from './services/old-format-mapping.service';
import { OldFormatImportV2Service } from './services/old-format-import-v2.service';
import { QueryExecutorService } from './services/query-executor.service';
import { FactTableService } from './services/fact-table.service';
import { CsvAnalyzerService } from './services/csv-analyzer.service';
import { ChunkedUploadService } from './services/chunked-upload.service';
import { UploadLogService } from './services/upload-log.service';
import { ReferenceTableService } from './services/reference-table.service';

@Module({
  imports: [DatabaseModule, ConnectionsModule],
  controllers: [SmartHierarchyController],
  providers: [
    SmartHierarchyService,
    ScriptGeneratorService,
    SnowflakeDeploymentService,
    CsvExportImportService,
    OldFormatMappingService,
    OldFormatImportV2Service,
    QueryExecutorService,
    FactTableService,
    CsvAnalyzerService,
    ChunkedUploadService,
    UploadLogService,
    ReferenceTableService,
  ],
  exports: [
    SmartHierarchyService,
    ScriptGeneratorService,
    CsvExportImportService,
    OldFormatImportV2Service,
    QueryExecutorService,
    FactTableService,
    CsvAnalyzerService,
    ChunkedUploadService,
    UploadLogService,
    ReferenceTableService,
  ],
})
export class SmartHierarchyModule {}
