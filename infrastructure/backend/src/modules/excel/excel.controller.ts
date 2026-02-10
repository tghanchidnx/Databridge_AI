/**
 * Excel Controller
 * API endpoints for Excel import/export operations
 */
import {
  Controller,
  Post,
  Get,
  Body,
  Param,
  Res,
  UploadedFile,
  UseInterceptors,
  BadRequestException,
  Logger,
} from '@nestjs/common';
import { FileInterceptor } from '@nestjs/platform-express';
import { Response } from 'express';
import { ApiTags, ApiOperation, ApiBody, ApiConsumes, ApiResponse } from '@nestjs/swagger';
import { ExcelImportService, ExcelParseResult, ColumnMapping } from './excel-import.service';
import { ExcelExportService, ExcelExportOptions } from './excel-export.service';

class PreviewImportDto {
  projectId: string;
  sheetName?: string;
}

class ImportDto {
  projectId: string;
  sheetName: string;
  columnMappings: ColumnMapping[];
  conflictResolution?: 'merge' | 'replace' | 'skip';
}

class ExportDto {
  projectId: string;
  hierarchyIds?: string[];
  includeMappings?: boolean;
  includeFormulas?: boolean;
  includeVarianceConfig?: boolean;
}

@ApiTags('excel')
@Controller('excel')
export class ExcelController {
  private readonly logger = new Logger(ExcelController.name);

  constructor(
    private readonly importService: ExcelImportService,
    private readonly exportService: ExcelExportService,
  ) {}

  /**
   * Upload and preview Excel file
   */
  @Post('preview')
  @ApiOperation({ summary: 'Upload and preview Excel file structure' })
  @ApiConsumes('multipart/form-data')
  @ApiBody({
    schema: {
      type: 'object',
      properties: {
        file: { type: 'string', format: 'binary' },
      },
    },
  })
  @UseInterceptors(FileInterceptor('file'))
  async previewImport(
    @UploadedFile() file: Express.Multer.File,
  ): Promise<ExcelParseResult> {
    if (!file) {
      throw new BadRequestException('No file uploaded');
    }

    if (!file.originalname.match(/\.(xlsx|xls)$/i)) {
      throw new BadRequestException('Only Excel files (.xlsx, .xls) are allowed');
    }

    this.logger.log(`Previewing Excel file: ${file.originalname}`);
    return this.importService.parseExcelFile(file.buffer, file.originalname);
  }

  /**
   * Import Excel file to project
   */
  @Post('import')
  @ApiOperation({ summary: 'Import Excel file to project' })
  @ApiConsumes('multipart/form-data')
  @UseInterceptors(FileInterceptor('file'))
  async importExcel(
    @UploadedFile() file: Express.Multer.File,
    @Body() dto: ImportDto,
  ) {
    if (!file) {
      throw new BadRequestException('No file uploaded');
    }

    this.logger.log(`Importing Excel file to project: ${dto.projectId}`);

    // Parse the file
    const parseResult = await this.importService.parseExcelFile(file.buffer, file.originalname);

    // Import to project
    return this.importService.importExcelToProject(
      dto.projectId,
      parseResult,
      dto.sheetName,
      dto.columnMappings,
      dto.conflictResolution,
    );
  }

  /**
   * Export project to Excel
   */
  @Get('export/:projectId')
  @ApiOperation({ summary: 'Export project hierarchies to Excel' })
  @ApiResponse({
    status: 200,
    description: 'Excel file download',
    content: {
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': {},
    },
  })
  async exportToExcel(
    @Param('projectId') projectId: string,
    @Res() res: Response,
  ) {
    this.logger.log(`Exporting project to Excel: ${projectId}`);

    // In production, fetch hierarchies from database
    const sampleHierarchies = [
      {
        id: '1',
        hierarchyId: 'REVENUE_001',
        hierarchyName: 'Total Revenue',
        parentId: null,
        description: 'Total revenue hierarchy',
        hierarchyLevel: { level_1: 'Revenue' },
        sortOrder: 1,
        flags: { include_flag: true, exclude_flag: false, transform_flag: false, active_flag: true },
        mapping: [
          {
            mapping_index: 1,
            source_database: 'FINANCE_DB',
            source_schema: 'GL',
            source_table: 'FACT_REVENUE',
            source_column: 'AMOUNT',
            join_type: 'INNER',
            system_type: 'ACTUALS',
            dimension_role: 'PRIMARY',
            flags: { include_flag: true, exclude_flag: false, transform_flag: false, active_flag: true },
          },
        ],
        formulaConfig: null,
      },
    ];

    const options: ExcelExportOptions = {
      projectId,
      includeMappings: true,
      includeFormulas: true,
      includeVarianceConfig: false,
    };

    const buffer = await this.exportService.exportHierarchiesToExcel(sampleHierarchies, options);

    res.setHeader(
      'Content-Type',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    );
    res.setHeader(
      'Content-Disposition',
      `attachment; filename=hierarchy_export_${projectId}_${Date.now()}.xlsx`,
    );
    res.send(buffer);
  }

  /**
   * Download import template
   */
  @Get('template')
  @ApiOperation({ summary: 'Download Excel import template' })
  async downloadTemplate(@Res() res: Response) {
    this.logger.log('Generating Excel import template');

    const buffer = await this.exportService.generateImportTemplate();

    res.setHeader(
      'Content-Type',
      'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    );
    res.setHeader(
      'Content-Disposition',
      'attachment; filename=databridge_import_template.xlsx',
    );
    res.send(buffer);
  }

  /**
   * Validate import data
   */
  @Post('validate')
  @ApiOperation({ summary: 'Validate Excel import data' })
  @UseInterceptors(FileInterceptor('file'))
  async validateImport(@UploadedFile() file: Express.Multer.File) {
    if (!file) {
      throw new BadRequestException('No file uploaded');
    }

    const parseResult = await this.importService.parseExcelFile(file.buffer, file.originalname);

    // Perform validation checks
    const validationResult = {
      isValid: true,
      errors: [] as string[],
      warnings: [] as string[],
      statistics: {
        sheetsCount: parseResult.sheets.length,
        detectedFormat: parseResult.detectedFormat,
        totalRows: parseResult.sheets.reduce((sum, s) => sum + s.rowCount, 0),
      },
    };

    // Check for required columns
    if (!parseResult.suggestions.idColumn) {
      validationResult.warnings.push('No hierarchy ID column detected');
    }
    if (!parseResult.suggestions.nameColumn) {
      validationResult.warnings.push('No hierarchy name column detected');
    }
    if (parseResult.detectedFormat === 'unknown') {
      validationResult.warnings.push('Could not detect file format');
    }

    // Check for duplicate IDs (sample check)
    for (const sheet of parseResult.sheets) {
      const idColumn = parseResult.suggestions.idColumn;
      if (idColumn && sheet.headers.includes(idColumn)) {
        const ids = sheet.sampleRows.map((r) => r[idColumn]);
        const duplicates = ids.filter((id, i) => ids.indexOf(id) !== i);
        if (duplicates.length > 0) {
          validationResult.warnings.push(`Potential duplicate IDs detected: ${duplicates.join(', ')}`);
        }
      }
    }

    return validationResult;
  }
}
