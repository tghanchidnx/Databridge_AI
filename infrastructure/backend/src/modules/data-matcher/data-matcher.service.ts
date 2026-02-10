import { Injectable, Logger, BadRequestException } from '@nestjs/common';
import * as _ from 'lodash';
import { PrismaService } from '../../database/prisma/prisma.service';
import { SnowflakeService } from '../../database/snowflake/snowflake.service';

interface DataComparisonDto {
  sourceConnectionId: string;
  targetConnectionId: string;
  sourceTable: string;
  targetTable: string;
  columns?: string[];
  sampleSize?: number;
  tolerance?: number;
}

export interface DataComparisonResult {
  status: 'COMPLETED' | 'FAILED' | 'PARTIAL';
  sourceRowCount: number;
  targetRowCount: number;
  rowCountMatch: boolean;
  columnsCompared: string[];
  mismatchedRows: number;
  matchPercentage: number;
  sampleChecked: number;
  differences: any[];
  startedAt: Date;
  completedAt: Date;
  durationMs: number;
}

@Injectable()
export class DataMatcherService {
  private readonly logger = new Logger(DataMatcherService.name);

  constructor(
    private prisma: PrismaService,
    private snowflakeService: SnowflakeService,
  ) {}

  async compareData(compareDto: DataComparisonDto): Promise<DataComparisonResult> {
    const startTime = Date.now();
    const startedAt = new Date();

    this.logger.log(
      `Starting data comparison: ${compareDto.sourceTable} vs ${compareDto.targetTable}`,
    );

    try {
      // Validate connections
      const sourceConnection = await this.prisma.connection.findUnique({
        where: { id: compareDto.sourceConnectionId },
      });

      const targetConnection = await this.prisma.connection.findUnique({
        where: { id: compareDto.targetConnectionId },
      });

      if (!sourceConnection || !targetConnection) {
        throw new BadRequestException('One or both connections not found');
      }

      // Get row counts
      const sourceRowCount = await this.getRowCount(sourceConnection, compareDto.sourceTable);

      const targetRowCount = await this.getRowCount(targetConnection, compareDto.targetTable);

      const rowCountMatch = sourceRowCount === targetRowCount;

      this.logger.log(
        `Row counts - Source: ${sourceRowCount}, Target: ${targetRowCount}, Match: ${rowCountMatch}`,
      );

      // Determine columns to compare
      let columnsToCompare = compareDto.columns;
      if (!columnsToCompare || columnsToCompare.length === 0) {
        columnsToCompare = await this.getCommonColumns(
          sourceConnection,
          targetConnection,
          compareDto.sourceTable,
          compareDto.targetTable,
        );
      }

      // Sample data comparison
      const sampleSize = compareDto.sampleSize || 1000;
      const tolerance = compareDto.tolerance || 0;

      const differences = await this.compareSampleData(
        sourceConnection,
        targetConnection,
        compareDto.sourceTable,
        compareDto.targetTable,
        columnsToCompare,
        sampleSize,
        tolerance,
      );

      const completedAt = new Date();
      const durationMs = Date.now() - startTime;

      const mismatchedRows = differences.length;
      const matchPercentage = ((sampleSize - mismatchedRows) / sampleSize) * 100;

      const result: DataComparisonResult = {
        status: differences.length === 0 ? 'COMPLETED' : 'PARTIAL',
        sourceRowCount,
        targetRowCount,
        rowCountMatch,
        columnsCompared: columnsToCompare,
        mismatchedRows,
        matchPercentage: Math.round(matchPercentage * 100) / 100,
        sampleChecked: Math.min(sampleSize, sourceRowCount),
        differences: differences.slice(0, 100), // Limit to first 100 differences
        startedAt,
        completedAt,
        durationMs,
      };

      this.logger.log(
        `Data comparison completed: ${matchPercentage}% match (${mismatchedRows} mismatches in ${sampleSize} samples)`,
      );

      return result;
    } catch (error) {
      const completedAt = new Date();
      const durationMs = Date.now() - startTime;

      this.logger.error(`Data comparison failed: ${error.message}`, error.stack);

      return {
        status: 'FAILED',
        sourceRowCount: 0,
        targetRowCount: 0,
        rowCountMatch: false,
        columnsCompared: [],
        mismatchedRows: 0,
        matchPercentage: 0,
        sampleChecked: 0,
        differences: [{ error: error.message }],
        startedAt,
        completedAt,
        durationMs,
      };
    }
  }

  private async getRowCount(connection: any, tableName: string): Promise<number> {
    const credentials = JSON.parse(connection.credentials);
    const [database, schema, table] = tableName.split('.');

    const query = `SELECT COUNT(*) as count FROM ${database}.${schema}.${table}`;

    const result = await this.snowflakeService.executeQuery(credentials, query);

    return result[0]?.COUNT || 0;
  }

  private async getCommonColumns(
    sourceConnection: any,
    targetConnection: any,
    sourceTable: string,
    targetTable: string,
  ): Promise<string[]> {
    const sourceCredentials = JSON.parse(sourceConnection.credentials);
    const targetCredentials = JSON.parse(targetConnection.credentials);

    const [sourceDb, sourceSchema, sourceTableName] = sourceTable.split('.');
    const [targetDb, targetSchema, targetTableName] = targetTable.split('.');

    // Get source columns
    const sourceColumnsQuery = `
      SELECT COLUMN_NAME 
      FROM ${sourceDb}.INFORMATION_SCHEMA.COLUMNS 
      WHERE TABLE_SCHEMA = '${sourceSchema}' 
      AND TABLE_NAME = '${sourceTableName}'
      ORDER BY ORDINAL_POSITION
    `;

    const sourceColumns = (await this.snowflakeService.executeQuery(
      sourceCredentials,
      sourceColumnsQuery,
    )) as unknown as any[];

    // Get target columns
    const targetColumnsQuery = `
      SELECT COLUMN_NAME 
      FROM ${targetDb}.INFORMATION_SCHEMA.COLUMNS 
      WHERE TABLE_SCHEMA = '${targetSchema}' 
      AND TABLE_NAME = '${targetTableName}'
      ORDER BY ORDINAL_POSITION
    `;

    const targetColumns = (await this.snowflakeService.executeQuery(
      targetCredentials,
      targetColumnsQuery,
    )) as unknown as any[];

    // Find common columns
    const sourceColumnNames = sourceColumns.map((c) => c.COLUMN_NAME);
    const targetColumnNames = targetColumns.map((c) => c.COLUMN_NAME);

    const commonColumns = sourceColumnNames.filter((col) => targetColumnNames.includes(col));

    this.logger.log(`Found ${commonColumns.length} common columns`);

    return commonColumns;
  }

  private async compareSampleData(
    sourceConnection: any,
    targetConnection: any,
    sourceTable: string,
    targetTable: string,
    columns: string[],
    sampleSize: number,
    tolerance: number,
  ): Promise<any[]> {
    const sourceCredentials = JSON.parse(sourceConnection.credentials);
    const targetCredentials = JSON.parse(targetConnection.credentials);

    const columnList = columns.join(', ');

    // Get sample from source
    const sourceQuery = `
      SELECT ${columnList} 
      FROM ${sourceTable} 
      SAMPLE (${sampleSize} ROWS)
    `;

    const sourceData = (await this.snowflakeService.executeQuery(
      sourceCredentials,
      sourceQuery,
    )) as unknown as any[];

    // For each source row, check if it exists in target
    const differences = [];

    for (const sourceRow of sourceData) {
      const whereClause = columns
        .map((col) => {
          const value = sourceRow[col];
          if (value === null) {
            return `${col} IS NULL`;
          } else if (typeof value === 'string') {
            return `${col} = '${value.replace(/'/g, "''")}'`;
          } else {
            return `${col} = ${value}`;
          }
        })
        .join(' AND ');

      const targetQuery = `
        SELECT ${columnList} 
        FROM ${targetTable} 
        WHERE ${whereClause}
        LIMIT 1
      `;

      try {
        const targetData = (await this.snowflakeService.executeQuery(
          targetCredentials,
          targetQuery,
        )) as unknown as any[];

        if (targetData.length === 0) {
          differences.push({
            type: 'MISSING_IN_TARGET',
            sourceRow,
          });
        } else if (tolerance > 0) {
          // Check for numeric differences within tolerance
          const targetRow = targetData[0];
          const columnDiffs = [];

          for (const col of columns) {
            const sourceVal = sourceRow[col];
            const targetVal = targetRow[col];

            if (typeof sourceVal === 'number' && typeof targetVal === 'number') {
              const diff = Math.abs(sourceVal - targetVal);
              if (diff > tolerance) {
                columnDiffs.push({
                  column: col,
                  sourceValue: sourceVal,
                  targetValue: targetVal,
                  difference: diff,
                });
              }
            } else if (sourceVal !== targetVal) {
              columnDiffs.push({
                column: col,
                sourceValue: sourceVal,
                targetValue: targetVal,
              });
            }
          }

          if (columnDiffs.length > 0) {
            differences.push({
              type: 'VALUE_MISMATCH',
              sourceRow,
              targetRow: targetData[0],
              columnDifferences: columnDiffs,
            });
          }
        }
      } catch (error) {
        this.logger.warn(`Error checking row in target: ${error.message}`);
      }

      // Limit differences to avoid memory issues
      if (differences.length >= 100) {
        break;
      }
    }

    return differences;
  }

  /**
   * Get detailed table statistics
   * Migrated from Python: Enhanced data profiling
   */
  async getTableStatistics(connectionId: string, database: string, schema: string, table: string) {
    const connection = await this.prisma.connection.findUnique({
      where: { id: connectionId },
    });

    if (!connection) {
      throw new BadRequestException('Connection not found');
    }

    const credentials = JSON.parse(connection.credentials);
    const fullTableName = `${database}.${schema}.${table}`;

    // Get row count
    const countQuery = `SELECT COUNT(*) as count FROM ${fullTableName}`;
    const countResult = await this.snowflakeService.executeQuery(credentials, countQuery);
    const rowCount = countResult[0]?.COUNT || 0;

    // Get column info with null counts
    const columnsQuery = `
      SELECT 
        COLUMN_NAME,
        DATA_TYPE,
        IS_NULLABLE,
        CHARACTER_MAXIMUM_LENGTH,
        NUMERIC_PRECISION,
        NUMERIC_SCALE
      FROM ${database}.INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = '${schema}'
        AND TABLE_NAME = '${table}'
      ORDER BY ORDINAL_POSITION
    `;

    const columns = await this.snowflakeService.executeQuery(credentials, columnsQuery);

    // Get null counts for each column
    const columnStats = [];
    const columnArray = Array.isArray(columns) ? columns : [columns];
    for (const col of columnArray) {
      const nullCountQuery = `
        SELECT COUNT(*) as null_count 
        FROM ${fullTableName} 
        WHERE ${col.COLUMN_NAME} IS NULL
      `;
      const nullResult = await this.snowflakeService.executeQuery(credentials, nullCountQuery);
      const nullCount = nullResult[0]?.NULL_COUNT || 0;

      columnStats.push({
        name: col.COLUMN_NAME,
        type: col.DATA_TYPE,
        nullable: col.IS_NULLABLE === 'YES',
        maxLength: col.CHARACTER_MAXIMUM_LENGTH,
        precision: col.NUMERIC_PRECISION,
        scale: col.NUMERIC_SCALE,
        nullCount,
        nullPercentage: rowCount > 0 ? ((nullCount / rowCount) * 100).toFixed(2) : 0,
        notNullCount: rowCount - nullCount,
      });
    }

    return {
      database,
      schema,
      table,
      rowCount,
      columnCount: columnStats.length,
      columns: columnStats,
    };
  }

  /**
   * Compare data with pagination support
   * Migrated from Python: Enhanced comparison with result pagination
   */
  async compareDataWithPagination(
    compareDto: DataComparisonDto & { page?: number; pageSize?: number },
  ) {
    const page = compareDto.page || 1;
    const pageSize = compareDto.pageSize || 100;
    const offset = (page - 1) * pageSize;

    const result = await this.compareData(compareDto);

    // Paginate differences
    const paginatedDifferences = result.differences.slice(offset, offset + pageSize);
    const totalDifferences = result.differences.length;
    const totalPages = Math.ceil(totalDifferences / pageSize);

    return {
      ...result,
      differences: paginatedDifferences,
      pagination: {
        page,
        pageSize,
        totalRecords: totalDifferences,
        totalPages,
        hasNext: page < totalPages,
        hasPrevious: page > 1,
      },
    };
  }

  /**
   * Get match and mismatch summaries
   * Migrated from Python: datacompy-style summary statistics
   */
  async getComparisonSummary(
    sourceConnectionId: string,
    targetConnectionId: string,
    sourceTable: string,
    targetTable: string,
    columns?: string[],
  ) {
    const result = await this.compareData({
      sourceConnectionId,
      targetConnectionId,
      sourceTable,
      targetTable,
      columns,
    });

    // Categorize differences
    const missingInTarget = result.differences.filter((d) => d.type === 'MISSING_IN_TARGET').length;
    const valueMismatches = result.differences.filter((d) => d.type === 'VALUE_MISMATCH').length;

    // Get column-level statistics
    const columnMismatches = {};
    result.differences.forEach((diff) => {
      if (diff.columnDifferences) {
        diff.columnDifferences.forEach((colDiff) => {
          if (!columnMismatches[colDiff.column]) {
            columnMismatches[colDiff.column] = 0;
          }
          columnMismatches[colDiff.column]++;
        });
      }
    });

    return {
      summary: {
        totalRowsCompared: result.sampleChecked,
        matchedRows: result.sampleChecked - result.mismatchedRows,
        mismatchedRows: result.mismatchedRows,
        matchPercentage: result.matchPercentage,
        rowCountMatch: result.rowCountMatch,
        sourceRowCount: result.sourceRowCount,
        targetRowCount: result.targetRowCount,
      },
      differences: {
        missingInTarget,
        valueMismatches,
        byColumn: columnMismatches,
      },
      columnsCompared: result.columnsCompared,
      duration: `${result.durationMs}ms`,
    };
  }
}
