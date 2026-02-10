import {
  Controller,
  Get,
  Post,
  Body,
  Param,
  Query,
  UseGuards,
  UseInterceptors,
} from '@nestjs/common';
import { ApiTags, ApiOperation, ApiResponse, ApiBearerAuth } from '@nestjs/swagger';
import { CacheInterceptor } from '@nestjs/cache-manager';
import { SchemaMatcherService } from './schema-matcher.service';
import { ScriptGeneratorService } from './script-generator.service';
import { CompareSchemaDto } from './dto/compare-schema.dto';
import { SpecializedCompareDto } from './dto/specialized-compare.dto';
import { MergeTablesDto } from './dto/merge-tables.dto';
import { GetTablesDto } from './dto/get-tables.dto';
import { JwtAuthGuard } from '../../common/guards/jwt-auth.guard';
import { CurrentUser } from '../../common/decorators/current-user.decorator';

@ApiTags('Schema Matcher')
@ApiBearerAuth()
@UseGuards(JwtAuthGuard)
@UseInterceptors(CacheInterceptor)
@Controller('schema-matcher')
export class SchemaMatcherController {
  constructor(
    private readonly schemaMatcherService: SchemaMatcherService,
    private readonly scriptGeneratorService: ScriptGeneratorService,
  ) {}

  @Get('tables')
  @ApiOperation({ summary: 'Get all tables in a schema' })
  @ApiResponse({ status: 200, description: 'Tables retrieved successfully' })
  getTables(@Query() getTablesDto: GetTablesDto) {
    return this.schemaMatcherService.getTables(getTablesDto);
  }

  @Get('table/columns')
  @ApiOperation({ summary: 'Get columns for a specific table' })
  @ApiResponse({ status: 200, description: 'Columns retrieved successfully' })
  getTableColumns(
    @Query('connectionId') connectionId: string,
    @Query('database') database: string,
    @Query('schema') schema: string,
    @Query('table') table: string,
  ) {
    return this.schemaMatcherService.getTableColumns(connectionId, database, schema, table);
  }

  @Post('compare')
  @ApiOperation({ summary: 'Compare schemas between two connections' })
  @ApiResponse({
    status: 201,
    description: 'Schema comparison started',
  })
  compareSchemas(@CurrentUser('id') userId: string, @Body() compareDto: CompareSchemaDto) {
    return this.schemaMatcherService.compareSchemas(userId, compareDto);
  }

  @Post('compare-specialized')
  @ApiOperation({
    summary: 'Specialized schema comparison with type-specific handling',
    description:
      'Performs type-specific schema comparisons. Types: D2D (Database-to-Database), D2S (Database-to-Snowflake), S2D (Snowflake-to-Database), S2S (Snowflake-to-Snowflake). Results include detailed column-level comparison with status categorization (MATCH, MISMATCH, SOURCE_ONLY, TARGET_ONLY).',
  })
  @ApiResponse({
    status: 201,
    description: 'Specialized comparison started',
    schema: {
      example: {
        jobId: 'job-1732483200-D2D',
        status: 'PENDING',
        comparisonType: 'D2D',
        message: 'Comparison job started',
      },
    },
  })
  @ApiResponse({ status: 400, description: 'Invalid comparison type or missing required fields' })
  compareSpecialized(@CurrentUser('id') userId: string, @Body() compareDto: SpecializedCompareDto) {
    return this.schemaMatcherService.compareSchemaSpecialized(userId, compareDto);
  }

  @Get('jobs/:id')
  @ApiOperation({ summary: 'Get schema comparison job result' })
  @ApiResponse({ status: 200, description: 'Job result retrieved' })
  @ApiResponse({ status: 404, description: 'Job not found' })
  getComparisonResult(@Param('id') jobId: string, @CurrentUser('id') userId: string) {
    return this.schemaMatcherService.getComparisonResult(jobId, userId);
  }

  @Get('jobs-ids')
  @ApiOperation({
    summary: 'Get all comparison job IDs for current user',
    description:
      'Returns a lightweight list of job IDs with basic metadata. Use this endpoint for listing jobs without fetching full results.',
  })
  @ApiResponse({
    status: 200,
    description: 'Job IDs retrieved successfully',
    schema: {
      example: [
        {
          id: 'job-123',
          status: 'COMPLETED',
          sourceConnectionId: 'conn-abc',
          targetConnectionId: 'conn-xyz',
          createdAt: '2024-01-01T00:00:00Z',
        },
      ],
    },
  })
  getAllJobIds(@CurrentUser('id') userId: string) {
    return this.schemaMatcherService.getAllComparisonJobIds(userId);
  }

  @Get('jobs')
  @ApiOperation({ summary: 'Get all comparison jobs with full details' })
  @ApiResponse({ status: 200, description: 'Jobs retrieved successfully' })
  getAllJobs(@CurrentUser('id') userId: string) {
    return this.schemaMatcherService.getAllComparisonJobs(userId);
  }

  @Get('jobs/:id/graph')
  @ApiOperation({
    summary: 'Get comparison job with graph visualization data',
    description:
      'Returns job details with graph visualization data (nodes and edges) for rendering hierarchy diagrams. Nodes are color-coded: green=match, red=mismatch, blue=source only, purple=target only.',
  })
  @ApiResponse({
    status: 200,
    description: 'Job with graph data retrieved',
    schema: {
      example: {
        jobId: 'job-123',
        status: 'COMPLETED',
        graphData: {
          nodes: [
            { id: 'node1', label: 'TABLE1', color: 'green', data: {} },
            { id: 'node2', label: 'TABLE2', color: 'red', data: {} },
          ],
          edges: [{ from: 'node1', to: 'node2', color: 'orange' }],
        },
      },
    },
  })
  @ApiResponse({ status: 404, description: 'Job not found' })
  getJobWithGraph(@Param('id') jobId: string, @CurrentUser('id') userId: string) {
    return this.schemaMatcherService.getComparisonJobWithGraph(jobId, userId);
  }

  @Post('compare-tables')
  @ApiOperation({
    summary: 'Compare multiple tables in a schema',
    description:
      'Compares column structure across multiple tables. Optionally finds common columns across all tables using lodash intersection.',
  })
  @ApiResponse({
    status: 200,
    description: 'Tables compared successfully',
    schema: {
      example: {
        comparisons: [
          { table: 'TABLE1', columnCount: 10, status: 'success' },
          { table: 'TABLE2', columnCount: 8, status: 'success' },
        ],
        commonColumns: ['ID', 'NAME', 'CREATED_AT'],
        commonColumnCount: 3,
      },
    },
  })
  compareTables(
    @Body('connectionId') connectionId: string,
    @Body('database') database: string,
    @Body('schema') schema: string,
    @Body('tables') tables: string[],
    @Body('checkCommonOnly') checkCommonOnly?: boolean,
  ) {
    return this.schemaMatcherService.compareTables(
      connectionId,
      database,
      schema,
      tables,
      checkCommonOnly,
    );
  }

  @Post('jobs/:id/generate-script')
  @ApiOperation({ summary: 'Generate deployment script for comparison job' })
  @ApiResponse({ status: 200, description: 'Script generated successfully' })
  @ApiResponse({ status: 404, description: 'Job not found' })
  generateScript(
    @Param('id') jobId: string,
    @Body('commentsFilters') commentsFilters?: string[],
    @Body('resourceNameFilters') resourceNameFilters?: string[],
  ) {
    return this.scriptGeneratorService.generateDeploymentScript(jobId, {
      commentsFilters,
      resourceNameFilters,
    });
  }

  @Post('jobs/:id/generate-script-by-type')
  @ApiOperation({ summary: 'Generate script for specific resource types' })
  @ApiResponse({ status: 200, description: 'Script generated successfully' })
  generateScriptByType(
    @Param('id') jobId: string,
    @Body('resourceTypes') resourceTypes: string[],
    @Body('commentsFilters') commentsFilters?: string[],
    @Body('resourceNameFilters') resourceNameFilters?: string[],
  ) {
    return this.scriptGeneratorService.generateScriptByResourceType(jobId, resourceTypes, {
      commentsFilters,
      resourceNameFilters,
    });
  }

  @Post('merge-tables')
  @ApiOperation({
    summary: 'Generate MERGE SQL statement for two tables',
    description:
      'Generates a dynamic MERGE statement with UPDATE and INSERT clauses. Auto-detects common columns if not specified. Safe mode by default (returns SQL only). Set executeImmediately=true to run the merge.',
  })
  @ApiResponse({
    status: 200,
    description: 'MERGE statement generated or executed successfully',
    schema: {
      example: {
        success: true,
        message: 'MERGE statement generated (not executed)',
        sql: 'MERGE INTO DB1.PUBLIC.ORDERS AS target USING DB1.PUBLIC.ORDERS_STAGING AS source ON target.ORDER_ID = source.ORDER_ID ...',
        sourceTable: 'DB1.PUBLIC.ORDERS_STAGING',
        targetTable: 'DB1.PUBLIC.ORDERS',
        commonColumns: 8,
        mergeColumns: 8,
        joinKeys: ['ORDER_ID'],
        note: 'Set executeImmediately=true to run this SQL',
      },
    },
  })
  @ApiResponse({
    status: 400,
    description: 'Invalid table names, no common columns, or execution failed',
  })
  mergeTables(@Body() mergeDto: MergeTablesDto) {
    return this.schemaMatcherService.mergeTables(mergeDto);
  }

  // ==================== Resource Detail Endpoints ====================

  @Get('jobs/:jobId/resources')
  @ApiOperation({ summary: 'Get all resources for a comparison job' })
  @ApiResponse({ status: 200, description: 'Resources retrieved successfully' })
  getJobResources(
    @Param('jobId') jobId: string,
    @CurrentUser('id') userId: string,
    @Query('status') status?: string,
    @Query('type') type?: string,
  ) {
    return this.schemaMatcherService.getJobResources(jobId, userId, { status, type });
  }

  @Get('jobs/:jobId/resources/:resourceId')
  @ApiOperation({ summary: 'Get detailed information for a specific resource' })
  @ApiResponse({ status: 200, description: 'Resource details retrieved successfully' })
  getResourceDetails(
    @Param('jobId') jobId: string,
    @Param('resourceId') resourceId: string,
    @CurrentUser('id') userId: string,
  ) {
    return this.schemaMatcherService.getResourceDetails(jobId, resourceId, userId);
  }

  @Get('jobs/:jobId/resources/:resourceId/columns')
  @ApiOperation({ summary: 'Get column comparison for a specific resource' })
  @ApiResponse({ status: 200, description: 'Column comparison retrieved successfully' })
  getResourceColumns(
    @Param('jobId') jobId: string,
    @Param('resourceId') resourceId: string,
    @CurrentUser('id') userId: string,
  ) {
    return this.schemaMatcherService.getResourceColumns(jobId, resourceId, userId);
  }

  @Get('jobs/:jobId/resources/:resourceId/ddl')
  @ApiOperation({ summary: 'Get DDL comparison for a specific resource' })
  @ApiResponse({ status: 200, description: 'DDL comparison retrieved successfully' })
  getResourceDdl(
    @Param('jobId') jobId: string,
    @Param('resourceId') resourceId: string,
    @CurrentUser('id') userId: string,
  ) {
    return this.schemaMatcherService.getResourceDdl(jobId, resourceId, userId);
  }

  @Get('jobs/:jobId/resources/:resourceId/dependencies')
  @ApiOperation({ summary: 'Get dependencies for a specific resource' })
  @ApiResponse({ status: 200, description: 'Resource dependencies retrieved successfully' })
  getResourceDependencies(
    @Param('jobId') jobId: string,
    @Param('resourceId') resourceId: string,
    @CurrentUser('id') userId: string,
  ) {
    return this.schemaMatcherService.getResourceDependencies(jobId, resourceId, userId);
  }

  @Get('jobs/:jobId/dependency-graph')
  @ApiOperation({ summary: 'Get dependency graph for comparison job' })
  @ApiResponse({ status: 200, description: 'Dependency graph retrieved successfully' })
  getDependencyGraph(@Param('jobId') jobId: string, @CurrentUser('id') userId: string) {
    return this.schemaMatcherService.getDependencyGraph(jobId, userId);
  }
}
