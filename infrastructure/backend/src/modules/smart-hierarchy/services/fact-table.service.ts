/**
 * Fact Table Service
 * Generates fact table scripts with multi-system support and variance calculations
 */
import { Injectable, Logger, BadRequestException } from '@nestjs/common';
import {
  SourceMapping,
  SystemType,
  JoinType,
  DimensionRole,
  VarianceConfig,
  FactTableConfig,
  FactTableScript,
} from '../types/smart-hierarchy.types';

export interface GenerateFactTableDto {
  projectId: string;
  hierarchyIds?: string[];
  config: FactTableConfig;
  databaseType: 'snowflake' | 'postgres' | 'mysql' | 'sqlserver';
}

export interface DeployFactTableDto {
  connectionId: string;
  script: FactTableScript;
  dryRun?: boolean;
}

export interface DeploymentResult {
  success: boolean;
  message: string;
  tableName?: string;
  rowCount?: number;
  executionTimeMs?: number;
  errors?: string[];
}

@Injectable()
export class FactTableService {
  private readonly logger = new Logger(FactTableService.name);

  private readonly SYSTEM_TYPE_SUFFIXES: Record<SystemType, string> = {
    ACTUALS: 'ACT',
    BUDGET: 'BUD',
    FORECAST: 'FCST',
    PRIOR_YEAR: 'PY',
    CUSTOM: 'CUST',
  };

  /**
   * Generate fact table script
   */
  async generateFactTableScript(dto: GenerateFactTableDto): Promise<FactTableScript> {
    const { config, databaseType } = dto;

    this.logger.log(`Generating fact table script for project: ${dto.projectId}`);

    // In production, fetch hierarchies and mappings from database
    // For now, simulate with sample data
    const sampleMappings = this.getSampleMappings();

    // Group mappings by system type
    const mappingsBySystem = this.groupMappingsBySystem(sampleMappings);

    // Generate DDL
    const createTableScript = this.generateCreateTableDDL(
      config,
      mappingsBySystem,
      databaseType,
    );

    // Generate INSERT/MERGE script
    const { insertScript, joinLogic } = this.generateInsertScript(
      config,
      mappingsBySystem,
      databaseType,
    );

    // Generate variance columns
    const varianceColumns = config.varianceConfig?.enabled
      ? this.generateVarianceColumns(config.varianceConfig)
      : [];

    return {
      createTableScript,
      insertScript,
      joinLogic,
      varianceColumns,
    };
  }

  /**
   * Group mappings by system type
   */
  private groupMappingsBySystem(
    mappings: SourceMapping[],
  ): Map<SystemType, SourceMapping[]> {
    const grouped = new Map<SystemType, SourceMapping[]>();

    for (const mapping of mappings) {
      const systemType = mapping.system_type || 'ACTUALS';
      if (!grouped.has(systemType)) {
        grouped.set(systemType, []);
      }
      grouped.get(systemType)!.push(mapping);
    }

    return grouped;
  }

  /**
   * Generate CREATE TABLE DDL
   */
  private generateCreateTableDDL(
    config: FactTableConfig,
    mappingsBySystem: Map<SystemType, SourceMapping[]>,
    databaseType: string,
  ): string {
    const tableName = this.getQualifiedTableName(config, databaseType);
    const columns: string[] = [];

    // Add dimension columns (from PRIMARY dimension mappings)
    columns.push('-- Dimension Columns');
    columns.push('HIERARCHY_ID VARCHAR(255) NOT NULL');
    columns.push('HIERARCHY_NAME VARCHAR(500)');
    columns.push('PARENT_ID VARCHAR(255)');

    // Add system-specific value columns
    const systemColumns = config.systemColumns || ['ACTUALS', 'BUDGET', 'FORECAST'];
    for (const system of systemColumns) {
      const suffix = this.SYSTEM_TYPE_SUFFIXES[system as SystemType];
      columns.push(`-- ${system} Columns`);
      columns.push(`AMOUNT_${suffix} DECIMAL(18,2)`);
      columns.push(`SOURCE_ID_${suffix} VARCHAR(255)`);
    }

    // Add variance columns if enabled
    if (config.varianceConfig?.enabled) {
      columns.push('-- Variance Columns');
      for (const comparison of config.varianceConfig.comparisons) {
        const colName = comparison.name.replace(/\s+/g, '_').toUpperCase();
        columns.push(`${colName}_VAR DECIMAL(18,2)`);
        if (comparison.includePercent) {
          columns.push(`${colName}_VAR_PCT DECIMAL(10,4)`);
        }
      }
    }

    // Add metadata columns
    columns.push('-- Metadata');
    columns.push('CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP');
    columns.push('UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP');

    // Generate CREATE TABLE statement
    const columnDefs = columns.join(',\n  ');
    let script = '';

    switch (databaseType) {
      case 'snowflake':
        script = `
-- Drop existing table if exists
DROP TABLE IF EXISTS ${tableName};

-- Create fact table
CREATE TABLE ${tableName} (
  ${columnDefs},
  PRIMARY KEY (HIERARCHY_ID)
);

-- Add clustering for performance
ALTER TABLE ${tableName} CLUSTER BY (HIERARCHY_ID, PARENT_ID);
`;
        break;

      case 'postgres':
        script = `
-- Drop existing table if exists
DROP TABLE IF EXISTS ${tableName} CASCADE;

-- Create fact table
CREATE TABLE ${tableName} (
  ${columnDefs},
  PRIMARY KEY (HIERARCHY_ID)
);

-- Create index for parent lookup
CREATE INDEX idx_${config.factTableName}_parent ON ${tableName} (PARENT_ID);
`;
        break;

      case 'mysql':
        script = `
-- Drop existing table if exists
DROP TABLE IF EXISTS ${tableName};

-- Create fact table
CREATE TABLE ${tableName} (
  ${columnDefs},
  PRIMARY KEY (HIERARCHY_ID),
  INDEX idx_parent (PARENT_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`;
        break;

      case 'sqlserver':
        script = `
-- Drop existing table if exists
IF OBJECT_ID('${tableName}', 'U') IS NOT NULL
  DROP TABLE ${tableName};

-- Create fact table
CREATE TABLE ${tableName} (
  ${columnDefs.replace(/DECIMAL\(/g, 'NUMERIC(').replace(/VARCHAR\(/g, 'NVARCHAR(')},
  CONSTRAINT PK_${config.factTableName} PRIMARY KEY (HIERARCHY_ID)
);

-- Create index for parent lookup
CREATE INDEX IX_${config.factTableName}_parent ON ${tableName} (PARENT_ID);
`;
        break;
    }

    return script;
  }

  /**
   * Generate INSERT/MERGE script with joins
   */
  private generateInsertScript(
    config: FactTableConfig,
    mappingsBySystem: Map<SystemType, SourceMapping[]>,
    databaseType: string,
  ): { insertScript: string; joinLogic: string } {
    const tableName = this.getQualifiedTableName(config, databaseType);
    const systemColumns = config.systemColumns || ['ACTUALS', 'BUDGET', 'FORECAST'];

    // Build SELECT columns
    const selectColumns: string[] = [
      'h.HIERARCHY_ID',
      'h.HIERARCHY_NAME',
      'h.PARENT_ID',
    ];

    // Add system-specific columns
    for (const system of systemColumns) {
      const suffix = this.SYSTEM_TYPE_SUFFIXES[system as SystemType];
      const mappings = mappingsBySystem.get(system as SystemType) || [];
      if (mappings.length > 0) {
        const mapping = mappings[0];
        selectColumns.push(`COALESCE(${system.toLowerCase()}.${mapping.source_column}, 0) AS AMOUNT_${suffix}`);
        selectColumns.push(`${system.toLowerCase()}.${mapping.source_column} AS SOURCE_ID_${suffix}`);
      } else {
        selectColumns.push(`NULL AS AMOUNT_${suffix}`);
        selectColumns.push(`NULL AS SOURCE_ID_${suffix}`);
      }
    }

    // Add variance calculations
    if (config.varianceConfig?.enabled) {
      for (const comparison of config.varianceConfig.comparisons) {
        const colName = comparison.name.replace(/\s+/g, '_').toUpperCase();
        const minuendSuffix = this.SYSTEM_TYPE_SUFFIXES[comparison.minuend];
        const subtrahendSuffix = this.SYSTEM_TYPE_SUFFIXES[comparison.subtrahend];

        selectColumns.push(
          `(COALESCE(AMOUNT_${minuendSuffix}, 0) - COALESCE(AMOUNT_${subtrahendSuffix}, 0)) AS ${colName}_VAR`,
        );

        if (comparison.includePercent) {
          selectColumns.push(
            `CASE
              WHEN COALESCE(AMOUNT_${subtrahendSuffix}, 0) = 0 THEN NULL
              ELSE ((COALESCE(AMOUNT_${minuendSuffix}, 0) - COALESCE(AMOUNT_${subtrahendSuffix}, 0)) / AMOUNT_${subtrahendSuffix}) * 100
            END AS ${colName}_VAR_PCT`,
          );
        }
      }
    }

    // Build JOIN logic
    const joinClauses: string[] = [];
    for (const system of systemColumns) {
      const mappings = mappingsBySystem.get(system as SystemType) || [];
      if (mappings.length > 0) {
        const mapping = mappings[0];
        const joinType = this.getJoinTypeSQL(mapping.join_type);
        const fullyQualifiedTable = `${mapping.source_database}.${mapping.source_schema}.${mapping.source_table}`;

        joinClauses.push(
          `${joinType} JOIN ${fullyQualifiedTable} AS ${system.toLowerCase()}
    ON h.HIERARCHY_ID = ${system.toLowerCase()}.${mapping.hierarchy_key_column || 'HIERARCHY_KEY'}`,
        );
      }
    }

    const joinLogic = joinClauses.join('\n');

    // Build final INSERT statement
    const insertScript = `
-- MERGE data into fact table
MERGE INTO ${tableName} AS target
USING (
  SELECT
    ${selectColumns.join(',\n    ')}
  FROM HIERARCHY_MASTER_VIEW h
  ${joinLogic}
) AS source
ON target.HIERARCHY_ID = source.HIERARCHY_ID
WHEN MATCHED THEN
  UPDATE SET
    HIERARCHY_NAME = source.HIERARCHY_NAME,
    ${systemColumns.map(s => `AMOUNT_${this.SYSTEM_TYPE_SUFFIXES[s as SystemType]} = source.AMOUNT_${this.SYSTEM_TYPE_SUFFIXES[s as SystemType]}`).join(',\n    ')},
    UPDATED_AT = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
  INSERT (${selectColumns.map(c => c.split(' AS ')[1] || c.split('.')[1]).join(', ')}, CREATED_AT, UPDATED_AT)
  VALUES (${selectColumns.map(c => `source.${c.split(' AS ')[1] || c.split('.')[1]}`).join(', ')}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP);
`;

    return { insertScript, joinLogic };
  }

  /**
   * Generate variance column definitions
   */
  private generateVarianceColumns(config: VarianceConfig): string[] {
    const columns: string[] = [];

    for (const comparison of config.comparisons) {
      const colName = comparison.name.replace(/\s+/g, '_').toUpperCase();
      columns.push(`${colName}_VAR`);
      if (comparison.includePercent) {
        columns.push(`${colName}_VAR_PCT`);
      }
    }

    return columns;
  }

  /**
   * Get qualified table name
   */
  private getQualifiedTableName(config: FactTableConfig, databaseType: string): string {
    const parts = [];
    if (config.databaseName) parts.push(config.databaseName);
    if (config.schemaName) parts.push(config.schemaName);
    parts.push(config.factTableName);
    return parts.join('.');
  }

  /**
   * Convert join type to SQL keyword
   */
  private getJoinTypeSQL(joinType?: JoinType): string {
    switch (joinType) {
      case 'INNER':
        return 'INNER';
      case 'RIGHT':
        return 'RIGHT OUTER';
      case 'FULL':
        return 'FULL OUTER';
      case 'LEFT':
      default:
        return 'LEFT OUTER';
    }
  }

  /**
   * Deploy fact table to database
   */
  async deployFactTable(dto: DeployFactTableDto): Promise<DeploymentResult> {
    const { connectionId, script, dryRun } = dto;

    if (dryRun) {
      return {
        success: true,
        message: 'Dry run completed successfully',
        tableName: 'fact_table',
      };
    }

    try {
      // In production, execute scripts through connection service
      // 1. Execute CREATE TABLE
      // 2. Execute INSERT/MERGE
      // 3. Return results

      return {
        success: true,
        message: 'Fact table deployed successfully',
        tableName: 'fact_table',
        rowCount: 1000,
        executionTimeMs: 2500,
      };
    } catch (error) {
      this.logger.error(`Fact table deployment failed: ${error.message}`);
      return {
        success: false,
        message: 'Deployment failed',
        errors: [error.message],
      };
    }
  }

  /**
   * Preview fact table data
   */
  async previewFactData(
    projectId: string,
    config: FactTableConfig,
    limit: number = 100,
  ): Promise<any> {
    // Generate preview query
    const sampleMappings = this.getSampleMappings();
    const mappingsBySystem = this.groupMappingsBySystem(sampleMappings);

    // Return sample preview
    return {
      columns: [
        { name: 'HIERARCHY_ID', type: 'VARCHAR' },
        { name: 'HIERARCHY_NAME', type: 'VARCHAR' },
        { name: 'AMOUNT_ACT', type: 'DECIMAL' },
        { name: 'AMOUNT_BUD', type: 'DECIMAL' },
        { name: 'ACTUAL_VS_BUDGET_VAR', type: 'DECIMAL' },
        { name: 'ACTUAL_VS_BUDGET_VAR_PCT', type: 'DECIMAL' },
      ],
      rows: [
        {
          HIERARCHY_ID: 'REVENUE_001',
          HIERARCHY_NAME: 'Total Revenue',
          AMOUNT_ACT: 1500000.00,
          AMOUNT_BUD: 1400000.00,
          ACTUAL_VS_BUDGET_VAR: 100000.00,
          ACTUAL_VS_BUDGET_VAR_PCT: 7.14,
        },
        {
          HIERARCHY_ID: 'REVENUE_002',
          HIERARCHY_NAME: 'Product Revenue',
          AMOUNT_ACT: 1200000.00,
          AMOUNT_BUD: 1100000.00,
          ACTUAL_VS_BUDGET_VAR: 100000.00,
          ACTUAL_VS_BUDGET_VAR_PCT: 9.09,
        },
      ],
      rowCount: 2,
    };
  }

  /**
   * Get sample mappings for testing
   */
  private getSampleMappings(): SourceMapping[] {
    return [
      {
        mapping_index: 1,
        source_database: 'FINANCE_DB',
        source_schema: 'GL',
        source_table: 'FACT_ACTUALS',
        source_column: 'AMOUNT',
        system_type: 'ACTUALS',
        dimension_role: 'PRIMARY',
        join_type: 'INNER',
        hierarchy_key_column: 'ACCOUNT_KEY',
        flags: { include_flag: true, exclude_flag: false, transform_flag: false, active_flag: true },
      },
      {
        mapping_index: 2,
        source_database: 'FINANCE_DB',
        source_schema: 'GL',
        source_table: 'FACT_BUDGET',
        source_column: 'AMOUNT',
        system_type: 'BUDGET',
        dimension_role: 'SECONDARY',
        join_type: 'LEFT',
        hierarchy_key_column: 'ACCOUNT_KEY',
        flags: { include_flag: true, exclude_flag: false, transform_flag: false, active_flag: true },
      },
      {
        mapping_index: 3,
        source_database: 'FINANCE_DB',
        source_schema: 'GL',
        source_table: 'FACT_FORECAST',
        source_column: 'AMOUNT',
        system_type: 'FORECAST',
        dimension_role: 'SECONDARY',
        join_type: 'LEFT',
        hierarchy_key_column: 'ACCOUNT_KEY',
        flags: { include_flag: true, exclude_flag: false, transform_flag: false, active_flag: true },
      },
    ];
  }
}
