/**
 * Excel Module
 * Handles Excel import/export for hierarchy data
 */
import { Module } from '@nestjs/common';
import { ExcelController } from './excel.controller';
import { ExcelImportService } from './excel-import.service';
import { ExcelExportService } from './excel-export.service';

@Module({
  controllers: [ExcelController],
  providers: [ExcelImportService, ExcelExportService],
  exports: [ExcelImportService, ExcelExportService],
})
export class ExcelModule {}
