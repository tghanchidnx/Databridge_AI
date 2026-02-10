import { Injectable, Logger, NotFoundException, BadRequestException } from '@nestjs/common';
import { PrismaService } from '../../database/prisma/prisma.service';
import { SnowflakeService } from '../../database/snowflake/snowflake.service';
import { SnowflakeService as SnowflakeAuthService } from '../connections/services/snowflake.service';
import { CompareSchemaDto } from './dto/compare-schema.dto';
import { GetTablesDto } from './dto/get-tables.dto';
import { SpecializedCompareDto, ComparisonType } from './dto/specialized-compare.dto';
import { MergeTablesDto } from './dto/merge-tables.dto';
import * as _ from 'lodash';

export interface NodeData {
  id: string;
  label: string;
  color: string;
  data: any;
}

export interface EdgeData {
  from: string;
  to: string;
  color: string;
}

@Injectable()
export class SchemaMatcherService {
  private readonly logger = new Logger(SchemaMatcherService.name);

  constructor(
    private prisma: PrismaService,
    private snowflakeService: SnowflakeService,
    private snowflakeAuthService: SnowflakeAuthService,
  ) {}

  async getTables(getTablesDto: GetTablesDto) {
    this.logger.log(`Getting tables for ${getTablesDto.database}.${getTablesDto.schema}`);

    // For now, return mock data - implement actual Snowflake connection later
    const tables = await this.snowflakeService.getTables(
      getTablesDto.connectionId,
      getTablesDto.database,
      getTablesDto.schema,
    );

    return tables;
  }

  async getTableColumns(connectionId: string, database: string, schema: string, table: string) {
    this.logger.log(`Getting columns for ${database}.${schema}.${table}`);

    const columns = await this.snowflakeService.getTableColumns(
      connectionId,
      database,
      schema,
      table,
    );

    return columns;
  }

  async compareSchemas(userId: string, compareDto: CompareSchemaDto) {
    this.logger.log('Starting schema comparison');

    // Create comparison job
    const job = await this.prisma.schemaComparisonJob.create({
      data: {
        userId,
        sourceConnectionId: compareDto.sourceConnectionId,
        sourceDatabase: compareDto.sourceDatabase,
        sourceSchema: compareDto.sourceSchema,
        targetConnectionId: compareDto.targetConnectionId,
        targetDatabase: compareDto.targetDatabase,
        targetSchema: compareDto.targetSchema,
        status: 'PENDING',
      },
    });

    // Start comparison in background
    this.performComparison(job.id, compareDto).catch((error) => {
      this.logger.error(`Comparison failed: ${error.message}`);
    });

    return {
      jobId: job.id,
      status: 'PENDING',
      message: 'Schema comparison started',
    };
  }

  async getComparisonResult(jobId: string, userId: string) {
    const job = await this.prisma.schemaComparisonJob.findFirst({
      where: { id: jobId, userId },
    });

    if (!job) {
      throw new NotFoundException(`Comparison job ${jobId} not found`);
    }

    return {
      jobId: job.id,
      status: job.status,
      result: job.result,
      createdAt: job.createdAt,
      completedAt: job.completedAt,
    };
  }

  private async performComparison(jobId: string, compareDto: CompareSchemaDto) {
    try {
      // Get source tables
      const sourceTables = await this.snowflakeService.getTables(
        compareDto.sourceConnectionId,
        compareDto.sourceDatabase,
        compareDto.sourceSchema,
      );

      // Get target tables
      const targetTables = await this.snowflakeService.getTables(
        compareDto.targetConnectionId,
        compareDto.targetDatabase,
        compareDto.targetSchema,
      );

      const comparison = {
        sourceTables: sourceTables.length,
        targetTables: targetTables.length,
        tablesOnlyInSource: [],
        tablesOnlyInTarget: [],
        commonTables: [],
      };

      // Compare tables (simplified logic)
      const sourceTableNames = sourceTables.map((t: any) => t.name);
      const targetTableNames = targetTables.map((t: any) => t.name);

      comparison.tablesOnlyInSource = sourceTableNames.filter(
        (name: string) => !targetTableNames.includes(name),
      );
      comparison.tablesOnlyInTarget = targetTableNames.filter(
        (name: string) => !sourceTableNames.includes(name),
      );
      comparison.commonTables = sourceTableNames.filter((name: string) =>
        targetTableNames.includes(name),
      );

      // Update job with results
      await this.prisma.schemaComparisonJob.update({
        where: { id: jobId },
        data: {
          status: 'COMPLETED',
          result: comparison,
          completedAt: new Date(),
        },
      });

      this.logger.log(`Comparison completed for job ${jobId}`);
    } catch (error) {
      this.logger.error(`Comparison failed: ${error.message}`);

      await this.prisma.schemaComparisonJob.update({
        where: { id: jobId },
        data: {
          status: 'FAILED',
          result: { error: error.message },
          completedAt: new Date(),
        },
      });
    }
  }

  /**
   * Get all comparison job IDs for a user
   * Migrated from Python: fetch_schema_comparision_job_id_only()
   */
  async getAllComparisonJobIds(userId: string) {
    const jobs = await this.prisma.schemaComparisonJob.findMany({
      where: { userId },
      select: {
        id: true,
        sourceConnectionId: true,
        targetConnectionId: true,
        sourceDatabase: true,
        sourceSchema: true,
        targetDatabase: true,
        targetSchema: true,
        status: true,
        createdAt: true,
      },
      orderBy: { createdAt: 'desc' },
    });

    return jobs;
  }

  /**
   * Get all comparison jobs with full details
   * Migrated from Python: fetch_all_schema_comparision_jobs()
   */
  async getAllComparisonJobs(userId: string) {
    const jobs = await this.prisma.schemaComparisonJob.findMany({
      where: { userId },
      orderBy: { createdAt: 'desc' },
    });

    return jobs;
  }

  /**
   * Get comparison job with graph visualization data
   * Migrated from Python: fetch_schema_comparision_jobs_by_job_id() with node/edge generation
   */
  async getComparisonJobWithGraph(jobId: string, userId: string) {
    this.logger.log(`Fetching comparison job with graph data: ${jobId} for user ${userId}`);

    const job = await this.prisma.schemaComparisonJob.findFirst({
      where: { id: jobId, userId },
    });

    if (!job) {
      this.logger.warn(`Comparison job not found: ${jobId} for user ${userId}`);
      throw new NotFoundException(
        `Comparison job ${jobId} not found or you don't have access to it`,
      );
    }

    // Generate graph visualization if result exists
    let graphData = null;
    if (job.result && typeof job.result === 'object' && 'details' in job.result) {
      try {
        graphData = this.generateGraphVisualization(job.result);
        this.logger.log(
          `Generated graph with ${graphData.nodes.length} nodes and ${graphData.edges.length} edges`,
        );
      } catch (error) {
        this.logger.error(`Failed to generate graph visualization: ${error.message}`, error.stack);
        // Continue without graph data rather than failing
      }
    } else {
      this.logger.warn(`Job ${jobId} has no valid result data for graph generation`);
    }

    return {
      jobId: job.id,
      status: job.status,
      result: job.result,
      graphData,
      sourceConnection: job.sourceConnectionId,
      targetConnection: job.targetConnectionId,
      createdAt: job.createdAt,
      completedAt: job.completedAt,
    };
  }

  /**
   * Generate node and edge data for graph visualization
   * Migrated from Python pandas DataFrame processing
   */
  private generateGraphVisualization(result: any) {
    const nodes: NodeData[] = [];
    const edges: EdgeData[] = [];
    const addedNodes: Set<string> = new Set();

    if (!result.details || !Array.isArray(result.details)) {
      return { nodes, edges };
    }

    // Process each comparison result to create nodes
    result.details.forEach((row: any) => {
      const nodeId = row.uniqueKey || `${row.database}.${row.schema}.${row.table}`;
      const nodeName = row.resourceNameSource || row.table;
      const nodeColor = this.getNodeColor(row.comments || row.status);

      if (!addedNodes.has(nodeId)) {
        nodes.push({
          id: nodeId,
          label: nodeName,
          color: nodeColor,
          data: row,
        });
        addedNodes.add(nodeId);
      }

      // Create edges if there's a parent relationship
      if (row.parentKey && addedNodes.has(row.parentKey)) {
        edges.push({
          from: row.parentKey,
          to: nodeId,
          color: 'orange',
        });
      }
    });

    return { nodes, edges };
  }

  /**
   * Compare multiple tables and return detailed differences
   * Migrated from Python: compare_tables()
   */
  async compareTables(
    connectionId: string,
    database: string,
    schema: string,
    tables: string[],
    checkCommonOnly: boolean = false,
  ) {
    this.logger.log(`Comparing ${tables.length} tables in ${database}.${schema}`);

    if (!tables || tables.length === 0) {
      throw new BadRequestException('At least one table must be provided for comparison');
    }

    const comparisons = [];

    for (const table of tables) {
      try {
        const columns = await this.snowflakeService.getTableColumns(
          connectionId,
          database,
          schema,
          table,
        );

        comparisons.push({
          table,
          columns,
          columnCount: columns.length,
          status: 'success',
        });
      } catch (error) {
        this.logger.error(`Failed to get columns for ${table}: ${error.message}`, error.stack);
        comparisons.push({
          table,
          columns: [],
          columnCount: 0,
          status: 'error',
          error: error.message,
        });
      }
    }

    // Find common columns across all tables if requested
    if (checkCommonOnly && comparisons.length > 1) {
      const successfulComparisons = comparisons.filter((c) => c.status === 'success');

      if (successfulComparisons.length === 0) {
        this.logger.warn('No successful table comparisons to find common columns');
        return { comparisons, commonColumns: [], commonColumnCount: 0 };
      }

      const allColumns = successfulComparisons.map((c) => c.columns.map((col: any) => col.name));

      if (allColumns.length > 0) {
        const commonColumns = _.intersection(...allColumns);
        this.logger.log(
          `Found ${commonColumns.length} common columns across ${successfulComparisons.length} tables`,
        );
        return {
          comparisons,
          commonColumns,
          commonColumnCount: commonColumns.length,
        };
      }
    }

    return { comparisons };
  }

  /**
   * Specialized comparison with type-specific handling
   * Migrated from Python: do_d2d_schema_comparision, do_d2s, do_s2d, do_s2s
   */
  async compareSchemaSpecialized(userId: string, compareDto: SpecializedCompareDto) {
    this.logger.log(
      `Starting specialized comparison: ${compareDto.comparisonType} - ${compareDto.sourceDatabase}.${compareDto.sourceSchema} vs ${compareDto.targetDatabase}.${compareDto.targetSchema} for user ${userId}`,
    );

    // Input validation
    if (!compareDto.comparisonType) {
      throw new BadRequestException('comparisonType is required');
    }

    if (!compareDto.sourceDatabase || !compareDto.sourceSchema) {
      throw new BadRequestException('sourceDatabase and sourceSchema are required');
    }

    if (!compareDto.targetDatabase || !compareDto.targetSchema) {
      throw new BadRequestException('targetDatabase and targetSchema are required');
    }

    if (
      compareDto.comparisonType === ComparisonType.DATABASE_TO_DATABASE ||
      compareDto.comparisonType === ComparisonType.DATABASE_TO_SNOWFLAKE
    ) {
      if (!compareDto.sourceConnectionId) {
        throw new BadRequestException(
          `sourceConnectionId is required for ${compareDto.comparisonType} comparison`,
        );
      }
    }

    if (
      compareDto.comparisonType === ComparisonType.SNOWFLAKE_TO_DATABASE ||
      compareDto.comparisonType === ComparisonType.SNOWFLAKE_TO_SNOWFLAKE
    ) {
      if (!compareDto.targetConnectionId) {
        throw new BadRequestException(
          `targetConnectionId is required for ${compareDto.comparisonType} comparison`,
        );
      }
    }

    const jobId = `job-${Date.now()}-${compareDto.comparisonType}`;
    const jobName = compareDto.jobName || `${compareDto.comparisonType} Comparison`;

    this.logger.log(`Creating comparison job: ${jobId} with name: ${jobName}`);

    // Create job record
    const job = await this.prisma.schemaComparisonJob.create({
      data: {
        id: jobId,
        userId,
        sourceConnectionId: compareDto.sourceConnectionId,
        targetConnectionId: compareDto.targetConnectionId,
        sourceDatabase: compareDto.sourceDatabase,
        sourceSchema: compareDto.sourceSchema,
        targetDatabase: compareDto.targetDatabase,
        targetSchema: compareDto.targetSchema,
        status: 'PENDING',
        result: {
          comparisonType: compareDto.comparisonType,
          jobName: jobName,
        },
      },
    });

    this.logger.log(`Job created: ${jobId}, starting background comparison`);

    // Execute comparison in background
    this.performSpecializedComparison(jobId, compareDto.comparisonType, compareDto).catch(
      (error) => {
        this.logger.error(
          `Specialized comparison failed for job ${jobId}: ${error.message}`,
          error.stack,
        );
      },
    );

    return {
      jobId: job.id,
      status: job.status,
      comparisonType: compareDto.comparisonType,
      message: 'Comparison job started',
    };
  }

  /**
   * Perform specialized comparison based on type
   */
  private async performSpecializedComparison(
    jobId: string,
    comparisonType: ComparisonType,
    compareDto: SpecializedCompareDto,
  ) {
    this.logger.log(`Performing ${comparisonType} comparison for job ${jobId}`);

    try {
      let result;

      switch (comparisonType) {
        case ComparisonType.DATABASE_TO_DATABASE:
          this.logger.log(
            `Starting D2D comparison: ${compareDto.sourceDatabase} → ${compareDto.targetDatabase}`,
          );
          result = await this.compareDatabaseToDatabase(jobId, compareDto);
          break;
        case ComparisonType.DATABASE_TO_SNOWFLAKE:
          this.logger.log(
            `Starting D2S comparison: ${compareDto.sourceDatabase} → ${compareDto.targetDatabase}`,
          );
          result = await this.compareDatabaseToSnowflake(jobId, compareDto);
          break;
        case ComparisonType.SNOWFLAKE_TO_DATABASE:
          this.logger.log(
            `Starting S2D comparison: ${compareDto.sourceDatabase} → ${compareDto.targetDatabase}`,
          );
          result = await this.compareSnowflakeToDatabase(jobId, compareDto);
          break;
        case ComparisonType.SNOWFLAKE_TO_SNOWFLAKE:
          this.logger.log(
            `Starting S2S comparison: ${compareDto.sourceDatabase} → ${compareDto.targetDatabase}`,
          );
          result = await this.compareSnowflakeToSnowflake(jobId, compareDto);
          break;
        default:
          const errorMsg = `Unknown comparison type: ${comparisonType}`;
          this.logger.error(errorMsg);
          throw new Error(errorMsg);
      }

      if (!result) {
        throw new Error('Comparison returned no results');
      }

      this.logger.log(`Comparison successful for job ${jobId}, updating job status`);

      await this.prisma.schemaComparisonJob.update({
        where: { id: jobId },
        data: {
          status: 'COMPLETED',
          result,
          completedAt: new Date(),
        },
      });

      this.logger.log(`Job ${jobId} marked as COMPLETED`);
    } catch (error) {
      this.logger.error(
        `Specialized comparison failed for job ${jobId}: ${error.message}`,
        error.stack,
      );

      await this.prisma.schemaComparisonJob.update({
        where: { id: jobId },
        data: {
          status: 'FAILED',
          result: { error: error.message, comparisonType: comparisonType },
          completedAt: new Date(),
        },
      });

      this.logger.error(`Job ${jobId} marked as FAILED`);
    }
  }

  /**
   * Database-to-Database comparison (D2D)
   * Both source and target are regular databases
   */
  private async compareDatabaseToDatabase(jobId: string, compareDto: SpecializedCompareDto) {
    this.logger.log(
      `Executing D2D comparison: ${compareDto.sourceDatabase}.${compareDto.sourceSchema} → ${compareDto.targetDatabase}.${compareDto.targetSchema}`,
    );

    // Fetch resources from source
    let sourceResources;
    try {
      sourceResources = await this.fetchResourcesWithColumns(
        compareDto.sourceConnectionId,
        compareDto.sourceDatabase,
        compareDto.sourceSchema,
      );
      this.logger.log(`Fetched ${sourceResources.length} source resources`);
    } catch (error) {
      this.logger.error(`Failed to fetch source resources: ${error.message}`, error.stack);
      throw new BadRequestException(`Failed to fetch source database resources: ${error.message}`);
    }

    // Fetch resources from target
    let targetResources;
    try {
      targetResources = await this.fetchResourcesWithColumns(
        compareDto.targetConnectionId,
        compareDto.targetDatabase,
        compareDto.targetSchema,
      );
      this.logger.log(`Fetched ${targetResources.length} target resources`);
    } catch (error) {
      this.logger.error(`Failed to fetch target resources: ${error.message}`, error.stack);
      throw new BadRequestException(`Failed to fetch target database resources: ${error.message}`);
    }

    // Compare resources
    const comparison = await this.compareResourceSets(
      jobId,
      sourceResources,
      targetResources,
      compareDto,
      'database',
      'database',
    );

    this.logger.log(`D2D comparison completed successfully`);

    return {
      comparisonType: ComparisonType.DATABASE_TO_DATABASE,
      ...comparison,
    };
  }

  /**
   * Database-to-Snowflake comparison (D2S)
   * Source is database, target is Snowflake
   */
  private async compareDatabaseToSnowflake(jobId: string, compareDto: SpecializedCompareDto) {
    this.logger.log(
      `Executing D2S comparison: ${compareDto.sourceDatabase}.${compareDto.sourceSchema} → Snowflake:${compareDto.targetDatabase}.${compareDto.targetSchema}`,
    );

    let sourceResources;
    try {
      sourceResources = await this.fetchResourcesWithColumns(
        compareDto.sourceConnectionId,
        compareDto.sourceDatabase,
        compareDto.sourceSchema,
      );
      this.logger.log(`Fetched ${sourceResources.length} source database resources`);
    } catch (error) {
      this.logger.error(`Failed to fetch source database: ${error.message}`, error.stack);
      throw new BadRequestException(`Failed to fetch source database resources: ${error.message}`);
    }

    let targetResources;
    try {
      targetResources = await this.fetchResourcesWithColumns(
        compareDto.targetConnectionId,
        compareDto.targetDatabase,
        compareDto.targetSchema,
      );
      this.logger.log(`Fetched ${targetResources.length} target Snowflake resources`);
    } catch (error) {
      this.logger.error(`Failed to fetch target Snowflake: ${error.message}`, error.stack);
      throw new BadRequestException(`Failed to fetch target Snowflake resources: ${error.message}`);
    }

    const comparison = await this.compareResourceSets(
      jobId,
      sourceResources,
      targetResources,
      compareDto,
      'database',
      'snowflake',
    );

    this.logger.log(`D2S comparison completed successfully`);

    return {
      comparisonType: ComparisonType.DATABASE_TO_SNOWFLAKE,
      ...comparison,
      note: 'Source is standard database, Target is Snowflake',
    };
  }

  /**
   * Snowflake-to-Database comparison (S2D)
   * Source is Snowflake, target is database
   */
  private async compareSnowflakeToDatabase(jobId: string, compareDto: SpecializedCompareDto) {
    this.logger.log(
      `Executing S2D comparison: Snowflake:${compareDto.sourceDatabase}.${compareDto.sourceSchema} → ${compareDto.targetDatabase}.${compareDto.targetSchema}`,
    );

    let sourceResources;
    try {
      sourceResources = await this.fetchResourcesWithColumns(
        compareDto.sourceConnectionId,
        compareDto.sourceDatabase,
        compareDto.sourceSchema,
      );
      this.logger.log(`Fetched ${sourceResources.length} source Snowflake resources`);
    } catch (error) {
      this.logger.error(`Failed to fetch source Snowflake: ${error.message}`, error.stack);
      throw new BadRequestException(`Failed to fetch source Snowflake resources: ${error.message}`);
    }

    let targetResources;
    try {
      targetResources = await this.fetchResourcesWithColumns(
        compareDto.targetConnectionId,
        compareDto.targetDatabase,
        compareDto.targetSchema,
      );
      this.logger.log(`Fetched ${targetResources.length} target database resources`);
    } catch (error) {
      this.logger.error(`Failed to fetch target database: ${error.message}`, error.stack);
      throw new BadRequestException(`Failed to fetch target database resources: ${error.message}`);
    }

    const comparison = await this.compareResourceSets(
      jobId,
      sourceResources,
      targetResources,
      compareDto,
      'snowflake',
      'database',
    );

    this.logger.log(`S2D comparison completed successfully`);

    return {
      comparisonType: ComparisonType.SNOWFLAKE_TO_DATABASE,
      ...comparison,
      note: 'Source is Snowflake, Target is standard database',
    };
  }

  /**
   * Snowflake-to-Snowflake comparison (S2S)
   * Both source and target are Snowflake
   */
  private async compareSnowflakeToSnowflake(jobId: string, compareDto: SpecializedCompareDto) {
    this.logger.log(
      `Executing S2S comparison: Snowflake:${compareDto.sourceDatabase}.${compareDto.sourceSchema} → Snowflake:${compareDto.targetDatabase}.${compareDto.targetSchema}`,
    );

    let sourceResources;
    try {
      sourceResources = await this.fetchResourcesWithColumns(
        compareDto.sourceConnectionId,
        compareDto.sourceDatabase,
        compareDto.sourceSchema,
      );
      this.logger.log(`Fetched ${sourceResources.length} source Snowflake resources`);
    } catch (error) {
      this.logger.error(`Failed to fetch source Snowflake: ${error.message}`, error.stack);
      throw new BadRequestException(`Failed to fetch source Snowflake resources: ${error.message}`);
    }

    let targetResources;
    try {
      targetResources = await this.fetchResourcesWithColumns(
        compareDto.targetConnectionId,
        compareDto.targetDatabase,
        compareDto.targetSchema,
      );
      this.logger.log(`Fetched ${targetResources.length} target Snowflake resources`);
    } catch (error) {
      this.logger.error(`Failed to fetch target Snowflake: ${error.message}`, error.stack);
      throw new BadRequestException(`Failed to fetch target Snowflake resources: ${error.message}`);
    }

    const comparison = await this.compareResourceSets(
      jobId,
      sourceResources,
      targetResources,
      compareDto,
      'snowflake',
      'snowflake',
    );

    this.logger.log(`S2S comparison completed successfully`);

    return {
      comparisonType: ComparisonType.SNOWFLAKE_TO_SNOWFLAKE,
      ...comparison,
      note: 'Both Source and Target are Snowflake',
    };
  }

  /**
   * Fetch all resources with columns from a schema
   */
  private async fetchResourcesWithColumns(connectionId: string, database: string, schema: string) {
    // Get connection to determine type
    const connection = await this.prisma.connection.findUnique({
      where: { id: connectionId },
    });

    if (!connection) {
      throw new NotFoundException(`Connection not found: ${connectionId}`);
    }

    const serverType = (connection.serverType || 'Snowflake').toLowerCase();
    this.logger.log(`Fetching resources for ${serverType} connection: ${database}.${schema}`);

    let tables: any[];
    const resourceMap = new Map();

    // Fetch tables based on connection type
    if (serverType === 'snowflake') {
      tables = await this.snowflakeService.getTables(connectionId, database, schema);
    } else if (serverType === 'mysql') {
      tables = await this.fetchMySQLTables(connection, database);
    } else if (serverType === 'postgresql') {
      tables = await this.fetchPostgreSQLTables(connection, database, schema);
    } else {
      throw new BadRequestException(`Unsupported database type: ${serverType}`);
    }

    // Fetch columns for each table
    for (const table of tables) {
      const resourceName = table.name;
      let columns: any[];

      if (serverType === 'snowflake') {
        columns = await this.snowflakeService.getTableColumns(
          connectionId,
          database,
          schema,
          resourceName,
        );
      } else if (serverType === 'mysql') {
        columns = await this.fetchMySQLColumns(connection, database, resourceName);
      } else if (serverType === 'postgresql') {
        columns = await this.fetchPostgreSQLColumns(connection, database, schema, resourceName);
      }

      resourceMap.set(resourceName, {
        resourceName,
        resourceType: table.type || 'TABLE',
        database,
        schema,
        columns: columns.map((col: any) => ({
          name: col.name,
          type: col.type,
          nullable: col.nullable,
        })),
      });
    }

    return resourceMap;
  }

  /**
   * Fetch MySQL tables
   */
  private async fetchMySQLTables(connection: any, database: string): Promise<any[]> {
    const mysql = require('mysql2/promise');
    const credentials = this.snowflakeAuthService.decryptCredentials(connection.credentials);

    this.logger.log(
      `Connecting to MySQL: ${connection.host}:${connection.port} as ${credentials.username}`,
    );

    const conn = await mysql.createConnection({
      host: connection.host,
      port: connection.port,
      user: credentials.username,
      password: credentials.password,
      database: database,
    });

    const [rows] = await conn.execute('SHOW TABLES');
    await conn.end();

    return rows.map((row: any) => ({
      name: Object.values(row)[0],
      type: 'TABLE',
    }));
  }

  /**
   * Fetch MySQL columns
   */
  private async fetchMySQLColumns(
    connection: any,
    database: string,
    table: string,
  ): Promise<any[]> {
    const mysql = require('mysql2/promise');
    const credentials = this.snowflakeAuthService.decryptCredentials(connection.credentials);

    const conn = await mysql.createConnection({
      host: connection.host,
      port: connection.port,
      user: credentials.username,
      password: credentials.password,
      database: database,
    });

    const [rows] = await conn.execute(`DESCRIBE ${table}`);
    await conn.end();

    return rows.map((row: any) => ({
      name: row.Field,
      type: row.Type,
      nullable: row.Null === 'YES',
    }));
  }

  /**
   * Fetch PostgreSQL tables
   */
  private async fetchPostgreSQLTables(
    connection: any,
    database: string,
    schema: string,
  ): Promise<any[]> {
    const { Client } = require('pg');
    const credentials = this.snowflakeAuthService.decryptCredentials(connection.credentials);

    const client = new Client({
      host: connection.host,
      port: connection.port,
      user: credentials.username,
      password: credentials.password,
      database: database,
    });

    await client.connect();
    const result = await client.query(
      `SELECT table_name as name, table_type as type 
       FROM information_schema.tables 
       WHERE table_schema = $1 
       AND table_type = 'BASE TABLE'`,
      [schema],
    );
    await client.end();

    return result.rows;
  }

  /**
   * Fetch PostgreSQL columns
   */
  private async fetchPostgreSQLColumns(
    connection: any,
    database: string,
    schema: string,
    table: string,
  ): Promise<any[]> {
    const { Client } = require('pg');
    const credentials = this.snowflakeAuthService.decryptCredentials(connection.credentials);

    const client = new Client({
      host: connection.host,
      port: connection.port,
      user: credentials.username,
      password: credentials.password,
      database: database,
    });

    await client.connect();
    const result = await client.query(
      `SELECT column_name as name, data_type as type, is_nullable 
       FROM information_schema.columns 
       WHERE table_schema = $1 AND table_name = $2
       ORDER BY ordinal_position`,
      [schema, table],
    );
    await client.end();

    return result.rows.map((row: any) => ({
      name: row.name,
      type: row.type,
      nullable: row.is_nullable === 'YES',
    }));
  }

  /**
   * Generate DDL for MySQL table
   */
  private async generateMySQLDdl(
    connection: any,
    database: string,
    table: string,
  ): Promise<string> {
    try {
      const mysql = require('mysql2/promise');
      const credentials = this.snowflakeAuthService.decryptCredentials(connection.credentials);

      const conn = await mysql.createConnection({
        host: connection.host,
        port: connection.port,
        user: credentials.username,
        password: credentials.password,
        database: database,
      });

      const [rows] = await conn.execute(`SHOW CREATE TABLE \`${table}\``);
      await conn.end();

      return rows[0]?.['Create Table'] || rows[0]?.['Create View'] || '';
    } catch (error) {
      this.logger.warn(`Failed to generate MySQL DDL for ${table}: ${error.message}`);
      return '';
    }
  }

  /**
   * Generate DDL for PostgreSQL table
   */
  private async generatePostgreSQLDdl(
    connection: any,
    database: string,
    schema: string,
    table: string,
  ): Promise<string> {
    try {
      const { Client } = require('pg');
      const credentials = this.snowflakeAuthService.decryptCredentials(connection.credentials);

      const client = new Client({
        host: connection.host,
        port: connection.port,
        user: credentials.username,
        password: credentials.password,
        database: database,
      });

      await client.connect();

      // Get columns with details
      const colResult = await client.query(
        `SELECT column_name, data_type, character_maximum_length, 
                is_nullable, column_default
         FROM information_schema.columns 
         WHERE table_schema = $1 AND table_name = $2
         ORDER BY ordinal_position`,
        [schema, table],
      );

      // Get primary keys
      const pkResult = await client.query(
        `SELECT kcu.column_name
         FROM information_schema.table_constraints tc
         JOIN information_schema.key_column_usage kcu 
           ON tc.constraint_name = kcu.constraint_name
           AND tc.table_schema = kcu.table_schema
         WHERE tc.table_schema = $1 AND tc.table_name = $2
         AND tc.constraint_type = 'PRIMARY KEY'
         ORDER BY kcu.ordinal_position`,
        [schema, table],
      );

      // Get foreign keys
      const fkResult = await client.query(
        `SELECT
           tc.constraint_name,
           kcu.column_name,
           ccu.table_schema AS foreign_table_schema,
           ccu.table_name AS foreign_table_name,
           ccu.column_name AS foreign_column_name
         FROM information_schema.table_constraints AS tc
         JOIN information_schema.key_column_usage AS kcu
           ON tc.constraint_name = kcu.constraint_name
           AND tc.table_schema = kcu.table_schema
         JOIN information_schema.constraint_column_usage AS ccu
           ON ccu.constraint_name = tc.constraint_name
           AND ccu.table_schema = tc.table_schema
         WHERE tc.table_schema = $1 AND tc.table_name = $2
         AND tc.constraint_type = 'FOREIGN KEY'`,
        [schema, table],
      );

      await client.end();

      // Build DDL
      let ddl = `CREATE TABLE ${schema}.${table} (\n`;

      const columns = colResult.rows.map((col: any) => {
        let colDef = `  ${col.column_name} ${col.data_type}`;
        if (col.character_maximum_length) {
          colDef += `(${col.character_maximum_length})`;
        }
        if (col.is_nullable === 'NO') {
          colDef += ' NOT NULL';
        }
        if (col.column_default) {
          colDef += ` DEFAULT ${col.column_default}`;
        }
        return colDef;
      });

      ddl += columns.join(',\n');

      if (pkResult.rows.length > 0) {
        const pkCols = pkResult.rows.map((r: any) => r.column_name).join(', ');
        ddl += `,\n  PRIMARY KEY (${pkCols})`;
      }

      // Add foreign keys
      const fkGroups = new Map<string, any[]>();
      for (const fk of fkResult.rows) {
        if (!fkGroups.has(fk.constraint_name)) {
          fkGroups.set(fk.constraint_name, []);
        }
        fkGroups.get(fk.constraint_name)!.push(fk);
      }

      for (const [constraintName, fks] of fkGroups) {
        const columns = fks.map((f) => f.column_name).join(', ');
        const refTable = `${fks[0].foreign_table_schema}.${fks[0].foreign_table_name}`;
        const refColumns = fks.map((f) => f.foreign_column_name).join(', ');
        ddl += `,\n  CONSTRAINT ${constraintName} FOREIGN KEY (${columns}) REFERENCES ${refTable}(${refColumns})`;
      }

      ddl += '\n);';

      return ddl;
    } catch (error) {
      this.logger.warn(`Failed to generate PostgreSQL DDL for ${table}: ${error.message}`);
      return '';
    }
  }

  /**
   * Generate DDL for Snowflake table
   */
  private async generateSnowflakeDdl(
    connectionId: string,
    database: string,
    schema: string,
    table: string,
  ): Promise<string> {
    try {
      const result = await this.snowflakeService.executeQuery(
        connectionId,
        `SELECT GET_DDL('TABLE', '${database}.${schema}.${table}') as ddl`,
      );
      return result[0]?.DDL || result[0]?.ddl || '';
    } catch (error) {
      this.logger.warn(`Failed to generate Snowflake DDL for ${table}: ${error.message}`);
      return '';
    }
  }

  /**
   * Compare two sets of resources and save to database
   */
  private async compareResourceSets(
    jobId: string,
    sourceResources: Map<string, any>,
    targetResources: Map<string, any>,
    compareDto: SpecializedCompareDto,
    sourceType: string,
    targetType: string,
  ) {
    const allResourceNames = new Set([...sourceResources.keys(), ...targetResources.keys()]);

    this.logger.log(`Comparing ${allResourceNames.size} resources for job ${jobId}`);

    const details = [];
    let matchCount = 0;
    let mismatchCount = 0;
    let sourceOnlyCount = 0;
    let targetOnlyCount = 0;
    let ddlNotFoundCount = 0;

    // Get connections for DDL generation
    const sourceConnection = compareDto.sourceConnectionId
      ? await this.prisma.connection.findUnique({ where: { id: compareDto.sourceConnectionId } })
      : null;
    const targetConnection = compareDto.targetConnectionId
      ? await this.prisma.connection.findUnique({ where: { id: compareDto.targetConnectionId } })
      : null;

    // Process each resource and save to database
    for (const resourceName of allResourceNames) {
      const sourceResource = sourceResources.get(resourceName);
      const targetResource = targetResources.get(resourceName);

      let status: string;
      let columnComparison: any = null;
      const differences: any = {};

      if (sourceResource && targetResource) {
        // Both exist - compare columns
        const sourceColumns = sourceResource.columns.map((c: any) => c.name);
        const targetColumns = targetResource.columns.map((c: any) => c.name);

        const commonColumns = _.intersection(sourceColumns, targetColumns);
        const sourceOnlyColumns = _.difference(sourceColumns, targetColumns);
        const targetOnlyColumns = _.difference(targetColumns, sourceColumns);

        if (sourceOnlyColumns.length === 0 && targetOnlyColumns.length === 0) {
          status = 'MATCHED';
          matchCount++;
        } else {
          status = 'MODIFIED';
          mismatchCount++;
        }

        columnComparison = {
          commonColumns: commonColumns.length,
          sourceOnlyColumns: sourceOnlyColumns.length,
          targetOnlyColumns: targetOnlyColumns.length,
          sourceOnlyColumnNames: sourceOnlyColumns,
          targetOnlyColumnNames: targetOnlyColumns,
        };

        differences.columnDifferences = columnComparison;
      } else if (sourceResource) {
        status = 'EXISTS_IN_SOURCE_ONLY';
        sourceOnlyCount++;
      } else {
        status = 'EXISTS_IN_TARGET_ONLY';
        targetOnlyCount++;
      }

      // Generate DDL for source and target
      let sourceDdl: string | null = null;
      let targetDdl: string | null = null;

      try {
        if (sourceResource && sourceConnection) {
          const serverType = (sourceConnection.serverType || 'Snowflake').toLowerCase();
          if (serverType === 'snowflake') {
            sourceDdl = await this.generateSnowflakeDdl(
              compareDto.sourceConnectionId,
              sourceResource.database,
              sourceResource.schema,
              resourceName,
            );
          } else if (serverType === 'mysql') {
            sourceDdl = await this.generateMySQLDdl(
              sourceConnection,
              sourceResource.database,
              resourceName,
            );
          } else if (serverType === 'postgresql') {
            sourceDdl = await this.generatePostgreSQLDdl(
              sourceConnection,
              sourceResource.database,
              sourceResource.schema,
              resourceName,
            );
          }
        }

        if (targetResource && targetConnection) {
          const serverType = (targetConnection.serverType || 'Snowflake').toLowerCase();
          if (serverType === 'snowflake') {
            targetDdl = await this.generateSnowflakeDdl(
              compareDto.targetConnectionId,
              targetResource.database,
              targetResource.schema,
              resourceName,
            );
          } else if (serverType === 'mysql') {
            targetDdl = await this.generateMySQLDdl(
              targetConnection,
              targetResource.database,
              resourceName,
            );
          } else if (serverType === 'postgresql') {
            targetDdl = await this.generatePostgreSQLDdl(
              targetConnection,
              targetResource.database,
              targetResource.schema,
              resourceName,
            );
          }
        }
      } catch (error) {
        this.logger.warn(`Failed to generate DDL for ${resourceName}: ${error.message}`);
      }

      if (!sourceDdl && !targetDdl) {
        ddlNotFoundCount++;
      }

      // Save ComparisonResource to database
      const comparisonResource = await this.prisma.comparisonResource.create({
        data: {
          jobId,
          resourceName,
          resourceType: sourceResource?.resourceType || targetResource?.resourceType || 'TABLE',
          database:
            sourceResource?.database || targetResource?.database || compareDto.sourceDatabase,
          schema: sourceResource?.schema || targetResource?.schema || compareDto.sourceSchema,
          status,
          sourceDdl,
          targetDdl,
          differences,
        },
      });

      // Save column-level comparisons if both resources exist
      if (sourceResource && targetResource && sourceResource.columns && targetResource.columns) {
        await this.saveColumnComparisons(comparisonResource.id, sourceResource, targetResource);
      }

      // Build summary detail for return
      details.push({
        resourceName,
        status,
        sourceResourceType: sourceResource?.resourceType,
        targetResourceType: targetResource?.resourceType,
        sourceDatabase: sourceResource?.database,
        sourceSchema: sourceResource?.schema,
        targetDatabase: targetResource?.database,
        targetSchema: targetResource?.schema,
        sourceColumnCount: sourceResource?.columns.length || 0,
        targetColumnCount: targetResource?.columns.length || 0,
        columnComparison,
      });
    }

    this.logger.log(`Saved ${allResourceNames.size} resources to database for job ${jobId}`);

    // Build dependency graph
    await this.buildDependencyGraph(jobId, sourceConnection, targetConnection, compareDto);

    return {
      summary: {
        totalResources: allResourceNames.size,
        matched: matchCount,
        modified: mismatchCount,
        sourceOnly: sourceOnlyCount,
        targetOnly: targetOnlyCount,
        ddlNotFound: ddlNotFoundCount,
        matchPercentage: ((matchCount / allResourceNames.size) * 100).toFixed(2),
        sourceType,
        targetType,
      },
      details: _.sortBy(details, ['status', 'resourceName']),
      sourceDatabase: compareDto.sourceDatabase,
      sourceSchema: compareDto.sourceSchema,
      targetDatabase: compareDto.targetDatabase,
      targetSchema: compareDto.targetSchema,
    };
  }

  /**
   * Save column-level comparisons
   */
  private async saveColumnComparisons(
    resourceId: string,
    sourceResource: any,
    targetResource: any,
  ) {
    const sourceColumnsMap = new Map(sourceResource.columns.map((c: any) => [c.name, c]));
    const targetColumnsMap = new Map(targetResource.columns.map((c: any) => [c.name, c]));

    const allColumnNames = new Set([...sourceColumnsMap.keys(), ...targetColumnsMap.keys()]);

    const columnData: any[] = [];

    for (const columnName of allColumnNames) {
      const sourceCol: any = sourceColumnsMap.get(columnName);
      const targetCol: any = targetColumnsMap.get(columnName);

      let status: string;
      const differences: any = {};

      if (sourceCol && targetCol) {
        // Compare types
        const typesMatch = sourceCol.type === targetCol.type;
        const nullableMatch = sourceCol.nullable === targetCol.nullable;
        const defaultMatch = sourceCol.default === targetCol.default;

        if (typesMatch && nullableMatch && defaultMatch) {
          status = 'MATCHED';
        } else {
          status = 'MODIFIED';
          if (!typesMatch) differences.type = { source: sourceCol.type, target: targetCol.type };
          if (!nullableMatch)
            differences.nullable = { source: sourceCol.nullable, target: targetCol.nullable };
          if (!defaultMatch)
            differences.default = { source: sourceCol.default, target: targetCol.default };
        }
      } else if (sourceCol) {
        status = 'SOURCE_ONLY';
      } else {
        status = 'TARGET_ONLY';
      }

      columnData.push({
        resourceId,
        columnName,
        status,
        sourceType: sourceCol?.type || null,
        targetType: targetCol?.type || null,
        sourceNullable: sourceCol?.nullable ?? null,
        targetNullable: targetCol?.nullable ?? null,
        sourceDefault: sourceCol?.default || null,
        targetDefault: targetCol?.default || null,
        differences,
      });
    }

    // Batch insert columns
    if (columnData.length > 0) {
      await this.prisma.comparisonColumn.createMany({
        data: columnData,
      });
    }
  }

  /**
   * Build dependency graph from foreign keys and constraints
   */
  private async buildDependencyGraph(
    jobId: string,
    sourceConnection: any,
    targetConnection: any,
    compareDto: SpecializedCompareDto,
  ) {
    this.logger.log(`Building dependency graph for job ${jobId}`);

    try {
      // Get all resources for this job
      const resources = await this.prisma.comparisonResource.findMany({
        where: { jobId },
      });

      const resourceMap = new Map(resources.map((r) => [r.resourceName.toLowerCase(), r]));
      const dependencies: any[] = [];

      // Helper function to extract FK dependencies from DDL
      const extractForeignKeys = (ddl: string, resourceName: string, resourceId: string) => {
        if (!ddl) return [];

        const fks: any[] = [];

        // Pattern 1: FOREIGN KEY (col) REFERENCES table(col)
        const fkPattern1 =
          /FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+([\w.`"\[\]]+)\s*\(([^)]+)\)/gi;
        let match;

        while ((match = fkPattern1.exec(ddl)) !== null) {
          const sourceColumns = match[1].replace(/[`"\[\]]/g, '').trim();
          let referencedTable = match[2].replace(/[`"\[\]]/g, '').trim();
          const referencedColumns = match[3].replace(/[`"\[\]]/g, '').trim();

          // Extract just table name if fully qualified (schema.table or db.schema.table)
          const parts = referencedTable.split('.');
          referencedTable = parts[parts.length - 1].toLowerCase();

          const targetResource = resourceMap.get(referencedTable);

          if (targetResource && targetResource.id !== resourceId) {
            fks.push({
              sourceResourceId: resourceId,
              targetResourceId: targetResource.id,
              constraintName: `FK_${resourceName}_${referencedTable}`,
              sourceColumns,
              targetColumns: referencedColumns,
              sourceTable: resourceName,
              targetTable: targetResource.resourceName,
            });
          }
        }

        // Pattern 2: CONSTRAINT name FOREIGN KEY
        const fkPattern2 =
          /CONSTRAINT\s+([\w`"\[\]]+)\s+FOREIGN\s+KEY\s*\(([^)]+)\)\s*REFERENCES\s+([\w.`"\[\]]+)\s*\(([^)]+)\)/gi;

        while ((match = fkPattern2.exec(ddl)) !== null) {
          const constraintName = match[1].replace(/[`"\[\]]/g, '').trim();
          const sourceColumns = match[2].replace(/[`"\[\]]/g, '').trim();
          let referencedTable = match[3].replace(/[`"\[\]]/g, '').trim();
          const referencedColumns = match[4].replace(/[`"\[\]]/g, '').trim();

          const parts = referencedTable.split('.');
          referencedTable = parts[parts.length - 1].toLowerCase();

          const targetResource = resourceMap.get(referencedTable);

          if (targetResource && targetResource.id !== resourceId) {
            // Check if we already added this FK
            const exists = fks.some(
              (fk) =>
                fk.sourceResourceId === resourceId &&
                fk.targetResourceId === targetResource.id &&
                fk.sourceColumns === sourceColumns,
            );

            if (!exists) {
              fks.push({
                sourceResourceId: resourceId,
                targetResourceId: targetResource.id,
                constraintName,
                sourceColumns,
                targetColumns: referencedColumns,
                sourceTable: resourceName,
                targetTable: targetResource.resourceName,
              });
            }
          }
        }

        return fks;
      };

      // Extract foreign keys from both source and target DDL
      for (const resource of resources) {
        // Process source DDL
        if (resource.sourceDdl) {
          const sourceFks = extractForeignKeys(
            resource.sourceDdl,
            resource.resourceName,
            resource.id,
          );

          for (const fk of sourceFks) {
            dependencies.push({
              jobId,
              sourceResourceId: fk.sourceResourceId,
              targetResourceId: fk.targetResourceId,
              dependencyType: 'FOREIGN_KEY',
              constraintName: fk.constraintName,
              metadata: {
                sourceColumns: fk.sourceColumns,
                targetColumns: fk.targetColumns,
                sourceTable: fk.sourceTable,
                targetTable: fk.targetTable,
                sourceType: resource.resourceType,
                side: 'source',
              },
            });
          }
        }

        // Process target DDL (if different from source)
        if (resource.targetDdl && resource.status !== 'matched') {
          const targetFks = extractForeignKeys(
            resource.targetDdl,
            resource.resourceName,
            resource.id,
          );

          for (const fk of targetFks) {
            // Check if this FK already exists from source
            const exists = dependencies.some(
              (dep) =>
                dep.sourceResourceId === fk.sourceResourceId &&
                dep.targetResourceId === fk.targetResourceId &&
                JSON.stringify(dep.metadata?.sourceColumns) === JSON.stringify(fk.sourceColumns),
            );

            if (!exists) {
              dependencies.push({
                jobId,
                sourceResourceId: fk.sourceResourceId,
                targetResourceId: fk.targetResourceId,
                dependencyType: 'FOREIGN_KEY',
                constraintName: fk.constraintName,
                metadata: {
                  sourceColumns: fk.sourceColumns,
                  targetColumns: fk.targetColumns,
                  sourceTable: fk.sourceTable,
                  targetTable: fk.targetTable,
                  sourceType: resource.resourceType,
                  side: 'target',
                },
              });
            }
          }
        }
      }

      // Remove duplicates
      const uniqueDeps = dependencies.filter(
        (dep, index, self) =>
          index ===
          self.findIndex(
            (d) =>
              d.sourceResourceId === dep.sourceResourceId &&
              d.targetResourceId === dep.targetResourceId &&
              d.constraintName === dep.constraintName,
          ),
      );

      // Save dependencies
      if (uniqueDeps.length > 0) {
        await this.prisma.comparisonDependency.createMany({
          data: uniqueDeps,
        });
        this.logger.log(`Saved ${uniqueDeps.length} dependencies for job ${jobId}`);
      } else {
        this.logger.log(`No dependencies found for job ${jobId}`);
      }
    } catch (error) {
      this.logger.error(`Failed to build dependency graph: ${error.message}`, error.stack);
    }
  }

  /**
   * Merge tables - Generate MERGE SQL statement
   * Migrated from Python: merge_tables()
   */
  async mergeTables(mergeDto: MergeTablesDto) {
    this.logger.log(
      `Generating MERGE statement: ${mergeDto.sourceTable} → ${mergeDto.targetTable}`,
    );

    // Parse table names
    const sourceTableParts = mergeDto.sourceTable.split('.');
    const targetTableParts = mergeDto.targetTable.split('.');

    if (sourceTableParts.length !== 3 || targetTableParts.length !== 3) {
      const errorMsg = 'Table names must be fully qualified (database.schema.table)';
      this.logger.error(`Invalid table format: ${errorMsg}`);
      throw new BadRequestException(errorMsg);
    }

    const [sourceDb, sourceSchema, sourceTableName] = sourceTableParts;
    const [targetDb, targetSchema, targetTableName] = targetTableParts;

    this.logger.log(`Parsed source: ${sourceDb}.${sourceSchema}.${sourceTableName}`);
    this.logger.log(`Parsed target: ${targetDb}.${targetSchema}.${targetTableName}`);

    // Get source columns
    let sourceColumns;
    try {
      sourceColumns = await this.snowflakeService.getTableColumns(
        mergeDto.connectionId,
        sourceDb,
        sourceSchema,
        sourceTableName,
      );
    } catch (error) {
      this.logger.error(`Failed to fetch source table columns: ${error.message}`, error.stack);
      throw new BadRequestException(
        `Source table ${mergeDto.sourceTable} not found or inaccessible: ${error.message}`,
      );
    }

    // Get target columns
    let targetColumns;
    try {
      targetColumns = await this.snowflakeService.getTableColumns(
        mergeDto.connectionId,
        targetDb,
        targetSchema,
        targetTableName,
      );
    } catch (error) {
      this.logger.error(`Failed to fetch target table columns: ${error.message}`, error.stack);
      throw new BadRequestException(
        `Target table ${mergeDto.targetTable} not found or inaccessible: ${error.message}`,
      );
    }

    if (!sourceColumns || sourceColumns.length === 0) {
      throw new BadRequestException(`Source table ${mergeDto.sourceTable} has no columns`);
    }

    if (!targetColumns || targetColumns.length === 0) {
      throw new BadRequestException(`Target table ${mergeDto.targetTable} has no columns`);
    }

    this.logger.log(
      `Source columns: ${sourceColumns.length}, Target columns: ${targetColumns.length}`,
    );

    // Find common columns
    const sourceColumnNames = sourceColumns.map((c: any) => c.name);
    const targetColumnNames = targetColumns.map((c: any) => c.name);
    const commonColumns: string[] = _.intersection(sourceColumnNames, targetColumnNames);

    if (commonColumns.length === 0) {
      const errorMsg = `No common columns found between source and target tables. Source: [${sourceColumnNames.join(', ')}], Target: [${targetColumnNames.join(', ')}]`;
      this.logger.error(errorMsg);
      throw new BadRequestException(errorMsg);
    }

    this.logger.log(`Found ${commonColumns.length} common columns: [${commonColumns.join(', ')}]`);

    // Determine merge columns with proper type safety
    let mergeColumns: string[];
    if (mergeDto.mergeColumns && Array.isArray(mergeDto.mergeColumns)) {
      mergeColumns = mergeDto.mergeColumns.map((col) => String(col));
    } else {
      mergeColumns = commonColumns;
    }

    let joinKeys: string[];
    if (mergeDto.joinKeys && Array.isArray(mergeDto.joinKeys)) {
      joinKeys = mergeDto.joinKeys.map((key) => String(key));
    } else {
      joinKeys = [commonColumns[0]]; // Use first common column as default key
    }

    this.logger.log(`Merge columns: ${mergeColumns.length}, Join keys: [${joinKeys.join(', ')}]`);

    // Validate join keys exist in common columns
    const invalidKeys = joinKeys.filter((key) => !commonColumns.includes(key));
    if (invalidKeys.length > 0) {
      const errorMsg = `Join keys not found in common columns: ${invalidKeys.join(', ')}`;
      this.logger.error(errorMsg);
      throw new BadRequestException(errorMsg);
    }

    // Generate MERGE statement
    const mergeSQL = this.generateMergeStatement(
      mergeDto.sourceTable,
      mergeDto.targetTable,
      mergeColumns,
      joinKeys,
    );

    this.logger.log(`Generated MERGE SQL (${mergeSQL.length} characters)`);

    // Execute if requested
    if (mergeDto.executeImmediately) {
      this.logger.warn(`Executing MERGE immediately for ${mergeDto.targetTable}`);

      try {
        const connection = await this.prisma.connection.findUnique({
          where: { id: mergeDto.connectionId },
        });

        if (!connection) {
          throw new BadRequestException(`Connection ${mergeDto.connectionId} not found`);
        }

        const credentials = JSON.parse(connection.credentials);
        await this.snowflakeService.executeQuery(credentials, mergeSQL);

        this.logger.log(`MERGE executed successfully for ${mergeDto.targetTable}`);

        return {
          success: true,
          message: 'MERGE executed successfully',
          sql: mergeSQL,
          sourceTable: mergeDto.sourceTable,
          targetTable: mergeDto.targetTable,
          commonColumns: commonColumns.length,
          mergeColumns: mergeColumns.length,
          joinKeys,
        };
      } catch (error) {
        this.logger.error(`MERGE execution failed: ${error.message}`, error.stack);
        throw new BadRequestException(
          `MERGE execution failed: ${error.message}. SQL: ${mergeSQL.substring(0, 200)}...`,
        );
      }
    }

    return {
      success: true,
      message: 'MERGE statement generated (not executed)',
      sql: mergeSQL,
      sourceTable: mergeDto.sourceTable,
      targetTable: mergeDto.targetTable,
      commonColumns: commonColumns.length,
      mergeColumns: mergeColumns.length,
      joinKeys,
      note: 'Set executeImmediately=true to run this SQL',
    };
  }

  /**
   * Generate MERGE SQL statement
   */
  private generateMergeStatement(
    sourceTable: string,
    targetTable: string,
    mergeColumns: string[],
    joinKeys: string[],
  ): string {
    // Build JOIN condition
    const joinCondition = joinKeys.map((key) => `target.${key} = source.${key}`).join(' AND ');

    // Build UPDATE SET clause
    const updateColumns = mergeColumns
      .filter((col) => !joinKeys.includes(col)) // Exclude join keys from update
      .map((col) => `target.${col} = source.${col}`)
      .join(',\n        ');

    // Build INSERT columns and values
    const insertColumns = mergeColumns.join(', ');
    const insertValues = mergeColumns.map((col) => `source.${col}`).join(', ');

    const mergeSQL = `
MERGE INTO ${targetTable} AS target
USING ${sourceTable} AS source
ON ${joinCondition}
WHEN MATCHED THEN
    UPDATE SET
        ${updateColumns}
WHEN NOT MATCHED THEN
    INSERT (${insertColumns})
    VALUES (${insertValues});
`.trim();

    return mergeSQL;
  }

  // ==================== Resource Detail Methods ====================

  /**
   * Get all resources for a comparison job with optional filtering
   */
  async getJobResources(
    jobId: string,
    userId: string,
    filters?: { status?: string; type?: string },
  ) {
    // Verify job belongs to user
    const job = await this.prisma.schemaComparisonJob.findFirst({
      where: { id: jobId, userId },
    });

    if (!job) {
      throw new NotFoundException(`Job ${jobId} not found`);
    }

    const whereClause: any = { jobId };
    if (filters?.status) {
      whereClause.status = filters.status;
    }
    if (filters?.type) {
      whereClause.resourceType = filters.type;
    }

    const resources = await this.prisma.comparisonResource.findMany({
      where: whereClause,
      select: {
        id: true,
        resourceName: true,
        resourceType: true,
        database: true,
        schema: true,
        status: true,
        createdAt: true,
        // Exclude large DDL fields for list view
      },
      orderBy: [{ resourceType: 'asc' }, { resourceName: 'asc' }],
    });

    return {
      jobId,
      resources,
      total: resources.length,
    };
  }

  /**
   * Get detailed information for a specific resource
   */
  async getResourceDetails(jobId: string, resourceId: string, userId: string) {
    // Verify job belongs to user
    const job = await this.prisma.schemaComparisonJob.findFirst({
      where: { id: jobId, userId },
    });

    if (!job) {
      throw new NotFoundException(`Job ${jobId} not found`);
    }

    const resource = await this.prisma.comparisonResource.findFirst({
      where: {
        id: resourceId,
        jobId,
      },
      include: {
        _count: {
          select: {
            columns: true,
            dependencies: true,
            dependedBy: true,
          },
        },
      },
    });

    if (!resource) {
      throw new NotFoundException(`Resource ${resourceId} not found`);
    }

    return resource;
  }

  /**
   * Get column comparison for a specific resource
   */
  async getResourceColumns(jobId: string, resourceId: string, userId: string) {
    // Verify job belongs to user
    const job = await this.prisma.schemaComparisonJob.findFirst({
      where: { id: jobId, userId },
    });

    if (!job) {
      throw new NotFoundException(`Job ${jobId} not found`);
    }

    const columns = await this.prisma.comparisonColumn.findMany({
      where: {
        resourceId,
      },
      orderBy: [{ status: 'asc' }, { columnName: 'asc' }],
    });

    // Group columns by status
    const grouped = {
      matched: columns.filter((c) => c.status === 'MATCHED'),
      modified: columns.filter((c) => c.status === 'MODIFIED'),
      sourceOnly: columns.filter((c) => c.status === 'SOURCE_ONLY'),
      targetOnly: columns.filter((c) => c.status === 'TARGET_ONLY'),
    };

    return {
      resourceId,
      total: columns.length,
      columns,
      grouped,
    };
  }

  /**
   * Get DDL comparison for a specific resource
   */
  async getResourceDdl(jobId: string, resourceId: string, userId: string) {
    // Verify job belongs to user
    const job = await this.prisma.schemaComparisonJob.findFirst({
      where: { id: jobId, userId },
    });

    if (!job) {
      throw new NotFoundException(`Job ${jobId} not found`);
    }

    const resource = await this.prisma.comparisonResource.findFirst({
      where: {
        id: resourceId,
        jobId,
      },
      select: {
        id: true,
        resourceName: true,
        resourceType: true,
        sourceDdl: true,
        targetDdl: true,
        status: true,
      },
    });

    if (!resource) {
      throw new NotFoundException(`Resource ${resourceId} not found`);
    }

    return resource;
  }

  /**
   * Get dependencies for a specific resource
   */
  async getResourceDependencies(jobId: string, resourceId: string, userId: string) {
    // Verify job belongs to user
    const job = await this.prisma.schemaComparisonJob.findFirst({
      where: { id: jobId, userId },
    });

    if (!job) {
      throw new NotFoundException(`Job ${jobId} not found`);
    }

    // Get dependencies where this resource is the source
    const outgoingDeps = await this.prisma.comparisonDependency.findMany({
      where: {
        jobId,
        sourceResourceId: resourceId,
      },
      include: {
        targetResource: {
          select: {
            id: true,
            resourceName: true,
            resourceType: true,
            status: true,
          },
        },
      },
    });

    // Get dependencies where this resource is the target
    const incomingDeps = await this.prisma.comparisonDependency.findMany({
      where: {
        jobId,
        targetResourceId: resourceId,
      },
      include: {
        sourceResource: {
          select: {
            id: true,
            resourceName: true,
            resourceType: true,
            status: true,
          },
        },
      },
    });

    return {
      resourceId,
      outgoing: outgoingDeps.map((d) => ({
        id: d.id,
        dependencyType: d.dependencyType,
        constraintName: d.constraintName,
        target: d.targetResource,
        metadata: d.metadata,
      })),
      incoming: incomingDeps.map((d) => ({
        id: d.id,
        dependencyType: d.dependencyType,
        constraintName: d.constraintName,
        source: d.sourceResource,
        metadata: d.metadata,
      })),
    };
  }

  /**
   * Get dependency graph for comparison job
   */
  async getDependencyGraph(jobId: string, userId: string) {
    // Verify job belongs to user
    const job = await this.prisma.schemaComparisonJob.findFirst({
      where: { id: jobId, userId },
    });

    if (!job) {
      throw new NotFoundException(`Job ${jobId} not found`);
    }

    const dependencies = await this.prisma.comparisonDependency.findMany({
      where: { jobId },
      include: {
        sourceResource: {
          select: {
            id: true,
            resourceName: true,
            resourceType: true,
            status: true,
          },
        },
        targetResource: {
          select: {
            id: true,
            resourceName: true,
            resourceType: true,
            status: true,
          },
        },
      },
    });

    // Build nodes and edges for visualization
    const nodes: NodeData[] = [];
    const edges: EdgeData[] = [];
    const nodeMap = new Map<string, boolean>();

    dependencies.forEach((dep) => {
      // Add source node
      if (!nodeMap.has(dep.sourceResource.id)) {
        nodes.push({
          id: dep.sourceResource.id,
          label: dep.sourceResource.resourceName,
          color: this.getNodeColor(dep.sourceResource.status),
          data: dep.sourceResource,
        });
        nodeMap.set(dep.sourceResource.id, true);
      }

      // Add target node
      if (!nodeMap.has(dep.targetResource.id)) {
        nodes.push({
          id: dep.targetResource.id,
          label: dep.targetResource.resourceName,
          color: this.getNodeColor(dep.targetResource.status),
          data: dep.targetResource,
        });
        nodeMap.set(dep.targetResource.id, true);
      }

      // Add edge
      edges.push({
        from: dep.sourceResource.id,
        to: dep.targetResource.id,
        color: this.getEdgeColor(dep.dependencyType),
      });
    });

    return {
      jobId,
      nodes,
      edges,
      dependencies,
    };
  }

  /**
   * Get node color based on status
   */
  private getNodeColor(status: string): string {
    const colorMap: Record<string, string> = {
      MATCHED: '#22c55e',
      MODIFIED: '#eab308',
      EXISTS_IN_SOURCE_ONLY: '#ef4444',
      EXISTS_IN_TARGET_ONLY: '#3b82f6',
      DDL_NOT_FOUND: '#6b7280',
    };
    return colorMap[status] || '#9ca3af';
  }

  /**
   * Get edge color based on dependency type
   */
  private getEdgeColor(dependencyType: string): string {
    const colorMap: Record<string, string> = {
      FOREIGN_KEY: '#3b82f6',
      VIEW_DEPENDENCY: '#8b5cf6',
      REFERENCE: '#ec4899',
    };
    return colorMap[dependencyType] || '#6b7280';
  }
}
