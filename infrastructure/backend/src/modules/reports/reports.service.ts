import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import { PrismaService } from '../../database/prisma/prisma.service';
import { SnowflakeService } from '../../database/snowflake/snowflake.service';

interface GenerateReportDto {
  connectionId: string;
  database: string;
  schema?: string;
  includeRowCounts?: boolean;
  includeColumnDetails?: boolean;
  includeSampleData?: boolean;
}

export interface HierarchyReport {
  generatedAt: Date;
  connection: {
    id: string;
    name: string;
    type: string;
  };
  database: string;
  schemas: SchemaInfo[];
  statistics: {
    totalSchemas: number;
    totalTables: number;
    totalColumns: number;
    totalRows?: number;
  };
}

interface SchemaInfo {
  name: string;
  tables: TableInfo[];
  tableCount: number;
}

interface TableInfo {
  name: string;
  fullyQualifiedName: string;
  rowCount?: number;
  columns: ColumnInfo[];
  sampleData?: any[];
}

interface ColumnInfo {
  name: string;
  dataType: string;
  nullable: string;
  defaultValue: string | null;
  comment: string | null;
  isPrimaryKey?: boolean;
  isForeignKey?: boolean;
}

@Injectable()
export class ReportsService {
  private readonly logger = new Logger(ReportsService.name);

  constructor(
    private prisma: PrismaService,
    private snowflakeService: SnowflakeService,
  ) {}

  async getHierarchyMapping(userId: string) {
    this.logger.log(`Fetching hierarchy mapping for user: ${userId}`);

    const aliases = await this.prisma.alias.findMany({
      where: { userId },
      orderBy: { createdAt: 'desc' },
    });

    const groupedBySource = aliases.reduce((acc, alias) => {
      if (!acc[alias.actualName]) {
        acc[alias.actualName] = [];
      }
      acc[alias.actualName].push({
        aliasName: alias.aliasName,
        aliasType: alias.aliasType,
        createdAt: alias.createdAt,
      });
      return acc;
    }, {});

    return {
      total: aliases.length,
      mappings: groupedBySource,
      aliases,
      summary: {
        uniqueSourceColumns: Object.keys(groupedBySource).length,
        totalMappings: aliases.length,
      },
    };
  }

  async generateReport(userId: string, reportDto: GenerateReportDto): Promise<HierarchyReport> {
    const startTime = Date.now();
    this.logger.log(`Generating hierarchy report for database: ${reportDto.database}`);

    try {
      // Get connection
      const connection = await this.prisma.connection.findUnique({
        where: { id: reportDto.connectionId },
      });

      if (!connection) {
        throw new NotFoundException('Connection not found');
      }

      const credentials = JSON.parse(connection.credentials);

      // Get schemas
      const schemasQuery = reportDto.schema
        ? `SELECT SCHEMA_NAME FROM ${reportDto.database}.INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '${reportDto.schema}'`
        : `SELECT SCHEMA_NAME FROM ${reportDto.database}.INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME`;

      const schemas = (await this.snowflakeService.executeQuery(
        credentials,
        schemasQuery,
      )) as unknown as any[];

      const schemaInfos: SchemaInfo[] = [];
      let totalTables = 0;
      let totalColumns = 0;
      let totalRows = 0;

      for (const schemaRow of schemas) {
        const schemaName = schemaRow.SCHEMA_NAME;

        // Get tables for this schema
        const tablesQuery = `
          SELECT TABLE_NAME, ROW_COUNT, BYTES
          FROM ${reportDto.database}.INFORMATION_SCHEMA.TABLES
          WHERE TABLE_SCHEMA = '${schemaName}'
          AND TABLE_TYPE = 'BASE TABLE'
          ORDER BY TABLE_NAME
        `;

        const tables = (await this.snowflakeService.executeQuery(
          credentials,
          tablesQuery,
        )) as unknown as any[];

        const tableInfos: TableInfo[] = [];

        for (const tableRow of tables) {
          const tableName = tableRow.TABLE_NAME;
          const fullyQualifiedName = `${reportDto.database}.${schemaName}.${tableName}`;

          // Get columns for this table
          const columnsQuery = `
            SELECT 
              COLUMN_NAME,
              DATA_TYPE,
              IS_NULLABLE,
              COLUMN_DEFAULT,
              COMMENT
            FROM ${reportDto.database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '${schemaName}'
            AND TABLE_NAME = '${tableName}'
            ORDER BY ORDINAL_POSITION
          `;

          const columns = (await this.snowflakeService.executeQuery(
            credentials,
            columnsQuery,
          )) as unknown as any[];

          const columnInfos: ColumnInfo[] = columns.map((col) => ({
            name: col.COLUMN_NAME,
            dataType: col.DATA_TYPE,
            nullable: col.IS_NULLABLE,
            defaultValue: col.COLUMN_DEFAULT,
            comment: col.COMMENT,
          }));

          let rowCount: number | undefined;
          let sampleData: any[] | undefined;

          // Get row count if requested
          if (reportDto.includeRowCounts) {
            try {
              const countQuery = `SELECT COUNT(*) as count FROM ${fullyQualifiedName}`;
              const countResult = await this.snowflakeService.executeQuery(credentials, countQuery);
              rowCount = countResult[0]?.COUNT || 0;
              totalRows += rowCount;
            } catch (error) {
              this.logger.warn(
                `Failed to get row count for ${fullyQualifiedName}: ${error.message}`,
              );
            }
          }

          // Get sample data if requested
          if (reportDto.includeSampleData) {
            try {
              const sampleQuery = `SELECT * FROM ${fullyQualifiedName} LIMIT 5`;
              sampleData = (await this.snowflakeService.executeQuery(
                credentials,
                sampleQuery,
              )) as unknown as any[];
            } catch (error) {
              this.logger.warn(
                `Failed to get sample data for ${fullyQualifiedName}: ${error.message}`,
              );
            }
          }

          totalColumns += columnInfos.length;

          tableInfos.push({
            name: tableName,
            fullyQualifiedName,
            rowCount,
            columns: columnInfos,
            sampleData,
          });
        }

        totalTables += tableInfos.length;

        schemaInfos.push({
          name: schemaName,
          tables: tableInfos,
          tableCount: tableInfos.length,
        });
      }

      const report: HierarchyReport = {
        generatedAt: new Date(),
        connection: {
          id: connection.id,
          name: connection.connectionName,
          type: connection.connectionType,
        },
        database: reportDto.database,
        schemas: schemaInfos,
        statistics: {
          totalSchemas: schemaInfos.length,
          totalTables,
          totalColumns,
          totalRows: reportDto.includeRowCounts ? totalRows : undefined,
        },
      };

      const durationMs = Date.now() - startTime;
      this.logger.log(
        `Report generated successfully in ${durationMs}ms: ${schemaInfos.length} schemas, ${totalTables} tables, ${totalColumns} columns`,
      );

      // Save report metadata to audit log
      await this.prisma.auditLog.create({
        data: {
          userId,
          action: 'GENERATE_REPORT',
          entity: 'REPORT',
          status: 'SUCCESS',
          changes: {
            connectionId: connection.id,
            database: reportDto.database,
            statistics: report.statistics,
            durationMs,
          },
        },
      });

      return report;
    } catch (error) {
      this.logger.error(`Report generation failed: ${error.message}`, error.stack);
      throw error;
    }
  }

  async exportReportAsJson(userId: string, reportDto: GenerateReportDto): Promise<string> {
    const report = await this.generateReport(userId, reportDto);
    return JSON.stringify(report, null, 2);
  }

  async exportReportAsCsv(userId: string, reportDto: GenerateReportDto): Promise<string> {
    const report = await this.generateReport(userId, reportDto);

    // Flatten the hierarchy to CSV format
    const rows: string[] = [];
    rows.push('Database,Schema,Table,Column,DataType,Nullable,RowCount');

    for (const schema of report.schemas) {
      for (const table of schema.tables) {
        for (const column of table.columns) {
          rows.push(
            [
              report.database,
              schema.name,
              table.name,
              column.name,
              column.dataType,
              column.nullable,
              table.rowCount || '',
            ].join(','),
          );
        }
      }
    }

    return rows.join('\n');
  }
}
