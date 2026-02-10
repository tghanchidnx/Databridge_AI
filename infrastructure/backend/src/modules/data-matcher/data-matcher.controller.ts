import { Controller, Post, Body, UseGuards, Get, Query } from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiBearerAuth } from '@nestjs/swagger';
import { DataMatcherService } from './data-matcher.service';
import { JwtAuthGuard } from '../../common/guards/jwt-auth.guard';
import { CompareDataDto } from './dto/compare-data.dto';

@ApiTags('Data Matcher')
@ApiBearerAuth()
@UseGuards(JwtAuthGuard)
@Controller('data-matcher')
export class DataMatcherController {
  constructor(private readonly dataMatcherService: DataMatcherService) {}

  @Post('compare')
  @ApiOperation({
    summary: 'Compare data between two tables',
    description: 'Performs row-by-row comparison of data between source and target tables',
  })
  @ApiResponse({ status: 200, description: 'Data comparison completed successfully' })
  @ApiResponse({ status: 400, description: 'Invalid input parameters' })
  compareData(@Body() compareDataDto: CompareDataDto) {
    return this.dataMatcherService.compareData(compareDataDto);
  }

  @Get('table-statistics')
  @ApiOperation({
    summary: 'Get detailed statistics for a table',
    description:
      'Returns comprehensive table statistics including row count, column count, and column-level details with null counts and percentages. Useful for data profiling.',
  })
  @ApiResponse({
    status: 200,
    description: 'Table statistics retrieved successfully',
    schema: {
      example: {
        database: 'TEST_DB',
        schema: 'PUBLIC',
        table: 'CUSTOMERS',
        rowCount: 10000,
        columnCount: 8,
        columns: [
          {
            name: 'CUSTOMER_ID',
            type: 'NUMBER',
            nullable: false,
            nullCount: 0,
            nullPercentage: '0.00',
            notNullCount: 10000,
          },
        ],
      },
    },
  })
  getTableStatistics(
    @Query('connectionId') connectionId: string,
    @Query('database') database: string,
    @Query('schema') schema: string,
    @Query('table') table: string,
  ) {
    return this.dataMatcherService.getTableStatistics(connectionId, database, schema, table);
  }

  @Post('compare-paginated')
  @ApiOperation({
    summary: 'Compare data with pagination support',
    description:
      'Performs row-by-row data comparison with paginated results. Default page size is 100. Returns differences with pagination metadata (page, totalPages, hasNext, hasPrevious).',
  })
  @ApiResponse({
    status: 200,
    description: 'Paginated comparison completed',
    schema: {
      example: {
        status: 'COMPLETED',
        sourceRowCount: 5000,
        targetRowCount: 5000,
        matchPercentage: 95.5,
        mismatchedRows: 45,
        differences: [
          { type: 'MISSING_IN_TARGET', sourceRow: {} },
          { type: 'VALUE_MISMATCH', sourceRow: {}, targetRow: {}, columnDifferences: [] },
        ],
        pagination: {
          page: 1,
          pageSize: 100,
          totalRecords: 45,
          totalPages: 1,
          hasNext: false,
          hasPrevious: false,
        },
      },
    },
  })
  compareDataPaginated(
    @Body() compareDataDto: CompareDataDto & { page?: number; pageSize?: number },
  ) {
    return this.dataMatcherService.compareDataWithPagination(compareDataDto);
  }

  @Post('compare-summary')
  @ApiOperation({
    summary: 'Get comparison summary statistics',
    description:
      'Returns high-level summary of data comparison without detailed row differences. datacompy-style statistics including match percentage, row counts, and column-level mismatch breakdowns. Faster than full comparison.',
  })
  @ApiResponse({
    status: 200,
    description: 'Comparison summary retrieved',
    schema: {
      example: {
        summary: {
          totalRowsCompared: 1000,
          matchedRows: 950,
          mismatchedRows: 50,
          matchPercentage: 95.0,
          rowCountMatch: true,
          sourceRowCount: 5000,
          targetRowCount: 5000,
        },
        differences: {
          missingInTarget: 30,
          valueMismatches: 20,
          byColumn: {
            AMOUNT: 15,
            STATUS: 5,
          },
        },
        columnsCompared: ['ID', 'NAME', 'AMOUNT', 'STATUS'],
        duration: '2500ms',
      },
    },
  })
  getComparisonSummary(
    @Body('sourceConnectionId') sourceConnectionId: string,
    @Body('targetConnectionId') targetConnectionId: string,
    @Body('sourceTable') sourceTable: string,
    @Body('targetTable') targetTable: string,
    @Body('columns') columns?: string[],
  ) {
    return this.dataMatcherService.getComparisonSummary(
      sourceConnectionId,
      targetConnectionId,
      sourceTable,
      targetTable,
      columns,
    );
  }
}
