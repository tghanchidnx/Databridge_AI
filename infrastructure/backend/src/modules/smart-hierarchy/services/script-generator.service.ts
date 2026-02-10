import { Injectable, NotFoundException } from '@nestjs/common';
import { PrismaService } from '../../../database/prisma/prisma.service';
import { GenerateScriptDto } from '../dto/smart-hierarchy.dto';
import type { SmartHierarchyMaster, SourceMapping } from '../types/smart-hierarchy.types';

@Injectable()
export class ScriptGeneratorService {
  constructor(private readonly prisma: PrismaService) {}

  // ============================================================================
  // Main Script Generation
  // ============================================================================

  async generateScripts(dto: GenerateScriptDto): Promise<{ scripts: any[] }> {
    // Get hierarchies - support both single and multiple
    let hierarchies: SmartHierarchyMaster[];
    let hierarchiesToDeploy: SmartHierarchyMaster[];
    let requestedHierarchyIds: Set<string> | null = null;

    if (dto.hierarchyIds && dto.hierarchyIds.length > 0) {
      // Track which hierarchies were explicitly requested
      requestedHierarchyIds = new Set(dto.hierarchyIds);

      // Fetch ALL hierarchies in the project for complete ID mapping
      // (We need parent hierarchies to resolve parentId UUIDs)
      hierarchies = await this.getAllHierarchies(dto.projectId);

      // Filter to only requested hierarchies for script generation
      hierarchiesToDeploy = hierarchies.filter((h) => requestedHierarchyIds.has(h.hierarchyId));

      if (hierarchiesToDeploy.length === 0) {
        throw new NotFoundException('Requested hierarchies not found');
      }

      console.log(
        `\n📋 Generating scripts for ${hierarchiesToDeploy.length} hierarchies (using ${hierarchies.length} total for ID mapping)\n`,
      );
    } else {
      // All hierarchies in project
      hierarchies = await this.getAllHierarchies(dto.projectId);
      hierarchiesToDeploy = hierarchies;
    }

    if (hierarchies.length === 0) {
      throw new NotFoundException('No hierarchies found');
    }

    // Get project details for project name and deployment config
    const project: any = await this.prisma.hierarchyProject.findUnique({
      where: { id: dto.projectId },
    });

    if (!project) {
      throw new NotFoundException('Project not found');
    }

    // Use deployment config from project if available, otherwise use defaults or provided values
    // IMPORTANT: This is for DISPLAY ONLY in generated scripts (for users copying scripts)
    // During actual deployment, the deployment service passes the REAL database/schema from deployment DTO
    // Priority: dto.database/schema > project.deploymentConfig > defaults (TRANSFORMATION.CONFIGURATION)
    const deploymentConfig = project.deploymentConfig as any;
    const displayDatabase = dto.database || deploymentConfig?.database || 'TRANSFORMATION';
    const displaySchema = dto.schema || deploymentConfig?.schema || 'CONFIGURATION';

    console.log('[generateScripts] Using config for display in scripts:', {
      providedDatabase: dto.database,
      providedSchema: dto.schema,
      projectConfigDatabase: deploymentConfig?.database,
      projectConfigSchema: deploymentConfig?.schema,
      displayDatabase,
      displaySchema,
      note: 'During deployment, actual dto.database and dto.schema are used',
    });

    const scripts: any[] = [];
    const databaseTypes =
      dto.databaseType === 'all'
        ? ['snowflake', 'postgres', 'mysql', 'sqlserver']
        : [dto.databaseType];

    for (const dbType of databaseTypes) {
      // Generate master table MERGE scripts for SELECTED hierarchies only
      if (dto.scriptType === 'insert' || dto.scriptType === 'all') {
        scripts.push({
          hierarchyId:
            hierarchiesToDeploy.length === hierarchies.length
              ? 'ALL'
              : hierarchiesToDeploy.map((h) => h.hierarchyId).join(', '),
          hierarchyName:
            hierarchiesToDeploy.length === hierarchies.length
              ? 'Master Table Insert'
              : `Master Table Insert (${hierarchiesToDeploy.length} hierarchies)`,
          scriptType: 'insert',
          databaseType: dbType,
          script: this.generateMasterTableScript(
            hierarchiesToDeploy, // Use ONLY selected hierarchies
            project.name,
            dbType as any,
            requestedHierarchyIds,
            dto.deployedBy, // Pass user email
            displayDatabase, // Use display config (not for deployment)
            displaySchema, // Use display config (not for deployment)
          ),
        });
      }

      // Generate single unified hierarchical view for the entire project
      if (dto.scriptType === 'view' || dto.scriptType === 'all') {
        scripts.push({
          hierarchyId:
            hierarchiesToDeploy.length === hierarchies.length
              ? 'ALL'
              : hierarchiesToDeploy.map((h) => h.hierarchyId).join(', '),
          hierarchyName:
            hierarchiesToDeploy.length === hierarchies.length
              ? 'Project Hierarchy Master View'
              : `Hierarchy View (${hierarchiesToDeploy.length} hierarchies)`,
          scriptType: 'view',
          databaseType: dbType,
          script: this.generateUnifiedHierarchyViewScript(
            hierarchiesToDeploy, // Use ONLY selected hierarchies
            project.name,
            dbType as any,
            dto.deployedBy,
            displayDatabase, // Use display config (not for deployment)
            displaySchema, // Use display config (not for deployment)
          ),
        });
      }

      // Generate mapping expansion view for the entire project
      if (dto.scriptType === 'mapping' || dto.scriptType === 'all') {
        scripts.push({
          hierarchyId:
            hierarchiesToDeploy.length === hierarchies.length
              ? 'ALL'
              : hierarchiesToDeploy.map((h) => h.hierarchyId).join(', '),
          hierarchyName:
            hierarchiesToDeploy.length === hierarchies.length
              ? 'Project Mapping Expansion View'
              : `Mapping View (${hierarchiesToDeploy.length} hierarchies)`,
          scriptType: 'mapping',
          databaseType: dbType,
          script: this.generateMappingExpansionViewScript(
            hierarchiesToDeploy,
            project.name,
            dbType as any,
            displayDatabase, // Use display config (not for deployment)
            displaySchema, // Use display config (not for deployment)
          ),
        });
      }

      // Generate single unified dynamic table for the entire project
      if (dto.scriptType === 'dt' || dto.scriptType === 'all') {
        scripts.push({
          hierarchyId:
            hierarchiesToDeploy.length === hierarchies.length
              ? 'ALL'
              : hierarchiesToDeploy.map((h) => h.hierarchyId).join(', '),
          hierarchyName:
            hierarchiesToDeploy.length === hierarchies.length
              ? 'Project Dynamic Table'
              : `Dynamic Table (${hierarchiesToDeploy.length} hierarchies)`,
          scriptType: 'dt',
          databaseType: dbType,
          script: this.generateUnifiedDynamicTableScript(
            hierarchiesToDeploy,
            project.name,
            dbType as any,
            displayDatabase, // Use display config (not for deployment)
            displaySchema, // Use display config (not for deployment)
          ),
        });
      }
    }

    // Note: Scripts are NOT saved to database during generation
    // They are only saved when actually deployed via pushToSnowflake

    return { scripts };
  }

  // ============================================================================
  // Helper: Build UUID to HIERARCHY_ID mapping for parent resolution
  // ============================================================================

  /**
   * Build a map from database UUID (id field) to HIERARCHY_ID.
   * This is needed because parentId stores the database UUID, not HIERARCHY_ID.
   */
  private buildIdMapping(hierarchies: SmartHierarchyMaster[]): Map<string, string> {
    const idMap = new Map<string, string>();

    console.log('\n========== Building ID Mapping ==========');
    console.log(`Total hierarchies: ${hierarchies.length}`);

    for (const hierarchy of hierarchies) {
      console.log(`\nHierarchy: ${hierarchy.hierarchyName}`);
      console.log(`  - id (UUID): ${hierarchy.id || 'MISSING'}`);
      console.log(`  - hierarchyId: ${hierarchy.hierarchyId}`);
      console.log(`  - parentId: ${hierarchy.parentId || 'NULL'}`);
      console.log(`  - isRoot: ${hierarchy.isRoot}`);

      if (hierarchy.id) {
        idMap.set(hierarchy.id, hierarchy.hierarchyId);
      } else {
        console.warn(`  ⚠️ WARNING: Missing 'id' field for hierarchy ${hierarchy.hierarchyId}`);
      }
    }

    console.log(`\nID Map size: ${idMap.size}`);
    console.log('ID Map contents:');
    idMap.forEach((hierarchyId, uuid) => {
      console.log(`  ${uuid} → ${hierarchyId}`);
    });
    console.log('========================================\n');

    return idMap;
  }

  // ============================================================================
  // Helper: Get hierarchy node info (NO FLATTENING - each record is already a node)
  // ============================================================================

  /**
   * Each SmartHierarchyMaster record represents a SINGLE node in the tree.
   * Parent-child relationships are established via parentId field (UUID).
   * We resolve the parentId UUID to the parent's HIERARCHY_ID using idMap.
   */
  private getHierarchyNode(
    hierarchy: SmartHierarchyMaster,
    idMap: Map<string, string>,
  ): {
    nodeId: string;
    nodeName: string;
    parentNodeId: string | null;
    sortOrder: number;
    isRoot: boolean;
    hierarchyLevel: any;
    flags: any;
    mapping: any[];
    formulaConfig: any;
    filterConfig: any;
    pivotConfig: any;
    metadata: any;
    description: string;
  } {
    // Resolve parentId (UUID) to parent's HIERARCHY_ID
    let parentHierarchyId = null;
    if (hierarchy.parentId) {
      parentHierarchyId = idMap.get(hierarchy.parentId) || null;
      if (!parentHierarchyId) {
        console.error(`\n❌ ERROR: Cannot resolve parent!`);
        console.error(`  Hierarchy: ${hierarchy.hierarchyName} (${hierarchy.hierarchyId})`);
        console.error(`  ParentId (UUID): ${hierarchy.parentId}`);
        console.error(`  Available UUIDs in map: ${Array.from(idMap.keys()).join(', ')}`);
        console.error(`  This parent UUID is NOT in the mapping!\n`);
      } else {
        console.log(`✅ Resolved parent: ${hierarchy.parentId} → ${parentHierarchyId}`);
      }
    }

    return {
      nodeId: hierarchy.hierarchyId,
      nodeName: hierarchy.hierarchyName,
      parentNodeId: parentHierarchyId, // Now uses parent's HIERARCHY_ID, not UUID
      sortOrder: hierarchy.sortOrder || 0,
      isRoot: hierarchy.isRoot ?? !parentHierarchyId,
      hierarchyLevel: hierarchy.hierarchyLevel,
      flags: hierarchy.flags,
      mapping: hierarchy.mapping,
      formulaConfig: hierarchy.formulaConfig,
      filterConfig: hierarchy.filterConfig,
      pivotConfig: hierarchy.pivotConfig,
      metadata: hierarchy.metadata,
      description: hierarchy.description || '',
    };
  }

  // Legacy function kept for reference - NO LONGER USED
  private flattenHierarchyTree_DEPRECATED(hierarchy: SmartHierarchyMaster): any[] {
    const nodes: any[] = [];
    const levelData = hierarchy.hierarchyLevel;

    // Build a map of all unique paths
    const pathMap = new Map<string, any>();

    // Extract all level combinations from the hierarchyLevel object
    const levels = Object.keys(levelData)
      .filter((k) => k.startsWith('level_'))
      .sort();

    if (levels.length === 0) {
      // If no levels, create a single root node
      nodes.push({
        nodeId: hierarchy.hierarchyId,
        nodeName: hierarchy.hierarchyName,
        parentNodeId: null,
        level: 1,
        sortOrder: hierarchy.sortOrder || 0,
        isRoot: true,
        levelPath: [hierarchy.hierarchyName],
        flags: hierarchy.flags,
        mapping: hierarchy.mapping,
        formulaConfig: hierarchy.formulaConfig,
        filterConfig: hierarchy.filterConfig,
        pivotConfig: hierarchy.pivotConfig,
      });
      return nodes;
    }

    // Build path tree from level_X values - create a node for EACH level
    const pathParts: string[] = [];
    for (let i = 1; i <= 15; i++) {
      const levelKey = `level_${i}`;
      if (levelData[levelKey]) {
        const levelValue = levelData[levelKey];
        pathParts.push(levelValue);

        // Create a node for this level
        const path = pathParts.join('/');
        const parentPath = pathParts.length > 1 ? pathParts.slice(0, -1).join('/') : null;

        if (!pathMap.has(path)) {
          pathMap.set(path, {
            nodeName: levelValue,
            level: pathParts.length,
            parentPath: parentPath,
            sortOrder: i - 1,
          });
        }
      } else {
        // Stop when we hit the first missing level
        break;
      }
    }

    // Convert path map to node list with IDs
    let nodeIndex = 0;
    const pathToIdMap = new Map<string, string>();

    Array.from(pathMap.entries())
      .sort((a, b) => a[1].level - b[1].level || a[0].localeCompare(b[0]))
      .forEach(([path, data]) => {
        const nodeId = `${hierarchy.hierarchyId}_NODE_${nodeIndex++}`;
        pathToIdMap.set(path, nodeId);

        const parentNodeId = data.parentPath ? pathToIdMap.get(data.parentPath) || null : null;

        nodes.push({
          nodeId,
          nodeName: data.nodeName,
          parentNodeId,
          level: data.level,
          sortOrder: data.sortOrder,
          isRoot: data.level === 1,
          levelPath: path.split('/'),
          flags: hierarchy.flags,
          mapping: data.level === pathMap.size ? hierarchy.mapping : [], // Only leaf nodes get mappings
          formulaConfig: hierarchy.formulaConfig,
          filterConfig: hierarchy.filterConfig,
          pivotConfig: hierarchy.pivotConfig,
        });
      });

    return nodes;
  }

  // ============================================================================
  // Master Table Script Generation (HIERARCHY_MASTER for ALL projects)
  // ============================================================================

  private generateMasterTableScript(
    hierarchies: SmartHierarchyMaster[],
    projectName: string,
    dbType: 'snowflake' | 'postgres' | 'mysql' | 'sqlserver' = 'snowflake',
    requestedHierarchyIds: Set<string> | null = null,
    deployedBy?: string, // User email for UPDATED_BY field
    database?: string, // Target database for qualified names
    schema?: string, // Target schema for qualified names
  ): string {
    if (hierarchies.length === 0) return '';

    const projectId = hierarchies[0].projectId;

    // Filter to only requested hierarchies if specified
    const hierarchiesToGenerate = requestedHierarchyIds
      ? hierarchies.filter((h) => requestedHierarchyIds.has(h.hierarchyId))
      : hierarchies;

    let script = `-- MASTER TABLE MERGE script for hierarchies\n`;
    script += `-- Project: ${projectName}\n`;
    script += `-- Project ID: ${projectId}\n`;
    script += `-- Database Type: ${dbType.toUpperCase()}\n`;
    script += `-- Total Hierarchies in Project: ${hierarchies.length}\n`;
    script += `-- Generating Scripts For: ${hierarchiesToGenerate.length} ${requestedHierarchyIds ? '(filtered)' : '(all)'}\n`;
    script += `-- Generated at: ${new Date().toISOString()}\n\n`;

    // Fixed master table name (shared across all projects)
    const tableName = dbType === 'snowflake' ? 'HIERARCHY_MASTER' : 'hierarchy_master';

    // Build fully qualified table name if database and schema are provided
    const qualifiedTableName =
      database && schema
        ? dbType === 'snowflake'
          ? `${database}.${schema}.${tableName}`
          : `${database.toLowerCase()}.${schema.toLowerCase()}.${tableName}`
        : tableName;

    // Add commented table creation DDL at the top for reference
    script += `/*\n`;
    script += `============================================================================\n`;
    script += `TABLE CREATION DDL (for reference - uncomment if table doesn't exist)\n`;
    script += `============================================================================\n`;

    // Add CREATE TABLE IF NOT EXISTS statement
    if (dbType === 'snowflake') {
      script += `-- Create sequence for hierarchy IDs\n`;
      script += `CREATE SEQUENCE IF NOT EXISTS SEQ_HIERARCHY START = 1 INCREMENT = 1;\n\n`;
      script += `-- Create master table\n`;
      script += `CREATE TABLE IF NOT EXISTS ${qualifiedTableName} (\n`;
      script += `  PROJECT_ID VARCHAR(255) NOT NULL,\n`;
      script += `  PROJECT_NAME VARCHAR(500),\n`;
      script += `  HIERARCHY_ID VARCHAR(255) NOT NULL,\n`;
      script += `  HIERARCHY_NAME VARCHAR(500),\n`;
      script += `  PARENT_ID VARCHAR(255),\n`;
      script += `  IS_ROOT BOOLEAN DEFAULT FALSE,\n`;
      script += `  SORT_ORDER NUMBER(10,0) DEFAULT 0,\n`;
      script += `  HIERARCHY_LEVEL VARIANT,\n`;
      script += `  FLAGS VARIANT,\n`;
      script += `  MAPPING VARIANT,\n`;
      script += `  FORMULA_CONFIG VARIANT,\n`;
      script += `  FILTER_CONFIG VARIANT,\n`;
      script += `  PIVOT_CONFIG VARIANT,\n`;
      script += `  METADATA VARIANT,\n`;
      script += `  DESCRIPTION VARCHAR(1000),\n`;
      script += `  CREATED_BY VARCHAR(255),\n`;
      script += `  UPDATED_BY VARCHAR(255),\n`;
      script += `  CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),\n`;
      script += `  UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),\n`;
      script += `  PRIMARY KEY (PROJECT_ID, HIERARCHY_ID)\n`;
      script += `);\n`;
    }
    script += `*/\n\n`;
    script += `-- ============================================================================\n`;
    script += `-- INSERT/MERGE STATEMENTS\n`;
    script += `-- ============================================================================\n\n`;

    // Build mapping from database UUID to HIERARCHY_ID for parent resolution
    // Use ALL hierarchies for the mapping, not just the filtered ones
    const idMap = this.buildIdMapping(hierarchies);

    // Add a helpful comment about executing the script
    if (dbType === 'snowflake') {
      script += `-- NOTE: In Snowflake, you can execute all MERGE statements in a single script.\n`;
      script += `-- Each MERGE statement ends with a semicolon and executes independently.\n`;
      script += `-- Total statements to execute: ${hierarchiesToGenerate.length}\n\n`;
    }

    // Generate MERGE for each hierarchy - only for requested hierarchies
    for (const hierarchy of hierarchiesToGenerate) {
      const projectIdValue = this.escapeString(hierarchy.projectId, dbType);
      const hierarchyNameValue = this.escapeString(hierarchy.hierarchyName, dbType);

      // Get node info directly (no flattening - each hierarchy IS already a single node)
      // Pass idMap to resolve parentId UUID to parent's HIERARCHY_ID
      const node = this.getHierarchyNode(hierarchy, idMap);

      const nodeIdValue = this.escapeString(node.nodeId, dbType);
      const nodeNameValue = this.escapeString(node.nodeName, dbType);
      const parentNodeIdValue = node.parentNodeId
        ? `'${this.escapeString(node.parentNodeId, dbType)}'`
        : 'NULL';
      const descValue = this.escapeString(node.description || hierarchyNameValue, dbType);

      script += `-- Hierarchy Node: ${node.nodeName} (${node.isRoot ? 'ROOT' : 'CHILD of ' + node.parentNodeId})\n\n`;

      switch (dbType) {
        case 'snowflake':
          script += `MERGE INTO ${qualifiedTableName} AS target\n`;
          script += `USING (\n`;
          script += `  SELECT\n`;
          script += `    '${projectIdValue}' AS PROJECT_ID,\n`;
          script += `    '${this.escapeString(projectName, dbType)}' AS PROJECT_NAME,\n`;
          script += `    '${nodeIdValue}' AS HIERARCHY_ID,\n`;
          script += `    '${hierarchyNameValue}' AS HIERARCHY_NAME,\n`;
          script += `    ${parentNodeIdValue} AS PARENT_ID,\n`;
          script += `    ${node.isRoot ? 'TRUE' : 'FALSE'} AS IS_ROOT,\n`;
          script += `    ${node.sortOrder} AS SORT_ORDER,\n`;
          script += `    PARSE_JSON('${this.escapeJson(node.hierarchyLevel)}') AS HIERARCHY_LEVEL,\n`;
          script += `    PARSE_JSON('${this.escapeJson(node.flags)}') AS FLAGS,\n`;
          script += `    PARSE_JSON('${this.escapeJson(node.mapping)}') AS MAPPING,\n`;
          script += `    ${node.formulaConfig ? `PARSE_JSON('${this.escapeJson(node.formulaConfig)}')` : 'NULL'} AS FORMULA_CONFIG,\n`;
          script += `    ${node.filterConfig ? `PARSE_JSON('${this.escapeJson(node.filterConfig)}')` : 'NULL'} AS FILTER_CONFIG,\n`;
          script += `    ${node.pivotConfig ? `PARSE_JSON('${this.escapeJson(node.pivotConfig)}')` : 'NULL'} AS PIVOT_CONFIG,\n`;
          script += `    ${node.metadata ? `PARSE_JSON('${this.escapeJson(node.metadata)}')` : 'NULL'} AS METADATA,\n`;
          script += `    '${descValue}' AS DESCRIPTION,\n`;
          script += `    ${deployedBy ? `'${this.escapeString(deployedBy, dbType)}'` : 'NULL'} AS CREATED_BY,\n`;
          script += `    ${deployedBy ? `'${this.escapeString(deployedBy, dbType)}'` : 'NULL'} AS UPDATED_BY\n`;
          script += `) AS source\n`;
          script += `ON target.PROJECT_ID = source.PROJECT_ID AND target.HIERARCHY_ID = source.HIERARCHY_ID\n`;
          script += `WHEN MATCHED THEN\n`;
          script += `  UPDATE SET\n`;
          script += `    PROJECT_NAME = source.PROJECT_NAME,\n`;
          script += `    HIERARCHY_NAME = source.HIERARCHY_NAME,\n`;
          script += `    PARENT_ID = source.PARENT_ID,\n`;
          script += `    IS_ROOT = source.IS_ROOT,\n`;
          script += `    SORT_ORDER = source.SORT_ORDER,\n`;
          script += `    HIERARCHY_LEVEL = source.HIERARCHY_LEVEL,\n`;
          script += `    FLAGS = source.FLAGS,\n`;
          script += `    MAPPING = source.MAPPING,\n`;
          script += `    FORMULA_CONFIG = source.FORMULA_CONFIG,\n`;
          script += `    FILTER_CONFIG = source.FILTER_CONFIG,\n`;
          script += `    PIVOT_CONFIG = source.PIVOT_CONFIG,\n`;
          script += `    METADATA = source.METADATA,\n`;
          script += `    DESCRIPTION = source.DESCRIPTION,\n`;
          script += `    UPDATED_BY = source.UPDATED_BY,\n`;
          script += `    UPDATED_AT = CURRENT_TIMESTAMP()\n`;
          script += `WHEN NOT MATCHED THEN\n`;
          script += `  INSERT (PROJECT_ID, PROJECT_NAME, HIERARCHY_ID, HIERARCHY_NAME, PARENT_ID, IS_ROOT, SORT_ORDER, HIERARCHY_LEVEL, FLAGS, MAPPING,\n`;
          script += `          FORMULA_CONFIG, FILTER_CONFIG, PIVOT_CONFIG, METADATA, DESCRIPTION, CREATED_BY, UPDATED_BY, CREATED_AT, UPDATED_AT)\n`;
          script += `  VALUES (source.PROJECT_ID, source.PROJECT_NAME, source.HIERARCHY_ID, source.HIERARCHY_NAME, source.PARENT_ID, source.IS_ROOT, source.SORT_ORDER, source.HIERARCHY_LEVEL,\n`;
          script += `          source.FLAGS, source.MAPPING, source.FORMULA_CONFIG,\n`;
          script += `          source.FILTER_CONFIG, source.PIVOT_CONFIG, source.METADATA, source.DESCRIPTION, source.CREATED_BY, source.UPDATED_BY,\n`;
          script += `          CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());\n\n`;
          break;

        case 'postgres':
          // Add table DDL in the header if this is the first hierarchy
          if (hierarchiesToGenerate.indexOf(hierarchy) === 0) {
            let headerScript = `/*\n`;
            headerScript += `============================================================================\n`;
            headerScript += `TABLE CREATION DDL (for reference - uncomment if table doesn't exist)\n`;
            headerScript += `============================================================================\n`;
            headerScript += `-- Create sequence for hierarchy IDs\n`;
            headerScript += `CREATE SEQUENCE IF NOT EXISTS seq_hierarchy START 1 INCREMENT 1;\n\n`;
            headerScript += `-- Create master table\n`;
            headerScript += `CREATE TABLE IF NOT EXISTS ${tableName} (\n`;
            headerScript += `  project_id VARCHAR(255) NOT NULL,\n`;
            headerScript += `  project_name VARCHAR(500),\n`;
            headerScript += `  hierarchy_id VARCHAR(255) NOT NULL,\n`;
            headerScript += `  hierarchy_name VARCHAR(500),\n`;
            headerScript += `  parent_id VARCHAR(255),\n`;
            headerScript += `  is_root BOOLEAN DEFAULT FALSE,\n`;
            headerScript += `  sort_order INTEGER DEFAULT 0,\n`;
            headerScript += `  hierarchy_level JSONB,\n`;
            headerScript += `  flags JSONB,\n`;
            headerScript += `  mapping JSONB,\n`;
            headerScript += `  formula_config JSONB,\n`;
            headerScript += `  filter_config JSONB,\n`;
            headerScript += `  pivot_config JSONB,\n`;
            headerScript += `  metadata JSONB,\n`;
            headerScript += `  description VARCHAR(1000),\n`;
            headerScript += `  created_by VARCHAR(255),\n`;
            headerScript += `  updated_by VARCHAR(255),\n`;
            headerScript += `  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n`;
            headerScript += `  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n`;
            headerScript += `  PRIMARY KEY (project_id, hierarchy_id)\n`;
            headerScript += `);\n`;
            headerScript += `*/\n\n`;
            // Insert the DDL header at the top of the script
            const insertPos = script.indexOf(
              '-- ============================================================================\n-- INSERT/MERGE STATEMENTS',
            );
            if (insertPos > 0) {
              script = script.substring(0, insertPos) + headerScript + script.substring(insertPos);
            }
          }

          script += `INSERT INTO ${qualifiedTableName} (\n`;
          script += `  project_id, project_name, hierarchy_id, hierarchy_name, parent_id, is_root, sort_order, hierarchy_level, flags, mapping,\n`;
          script += `  formula_config, filter_config, pivot_config, metadata, description, created_by, updated_by, created_at, updated_at\n`;
          script += `) VALUES (\n`;
          script += `  '${projectIdValue}', '${this.escapeString(projectName, dbType)}', '${nodeIdValue}', '${hierarchyNameValue}', ${parentNodeIdValue}, ${node.isRoot ? 'TRUE' : 'FALSE'}, ${node.sortOrder},\n`;
          script += `  '${this.escapeJson(node.hierarchyLevel)}'::jsonb,\n`;
          script += `  '${this.escapeJson(node.flags)}'::jsonb,\n`;
          script += `  '${this.escapeJson(node.mapping)}'::jsonb,\n`;
          script += `  ${node.formulaConfig ? `'${this.escapeJson(node.formulaConfig)}'::jsonb` : 'NULL'},\n`;
          script += `  ${node.filterConfig ? `'${this.escapeJson(node.filterConfig)}'::jsonb` : 'NULL'},\n`;
          script += `  ${node.pivotConfig ? `'${this.escapeJson(node.pivotConfig)}'::jsonb` : 'NULL'},\n`;
          script += `  ${node.metadata ? `'${this.escapeJson(node.metadata)}'::jsonb` : 'NULL'},\n`;
          script += `  '${descValue}', ${deployedBy ? `'${this.escapeString(deployedBy, dbType)}'` : 'NULL'}, ${deployedBy ? `'${this.escapeString(deployedBy, dbType)}'` : 'NULL'}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP\n`;
          script += `)\n`;
          script += `ON CONFLICT (project_id, hierarchy_id) DO UPDATE SET\n`;
          script += `  project_name = EXCLUDED.project_name,\n`;
          script += `  hierarchy_name = EXCLUDED.hierarchy_name,\n`;
          script += `  parent_id = EXCLUDED.parent_id,\n`;
          script += `  is_root = EXCLUDED.is_root,\n`;
          script += `  sort_order = EXCLUDED.sort_order,\n`;
          script += `  hierarchy_level = EXCLUDED.hierarchy_level,\n`;
          script += `  flags = EXCLUDED.flags,\n`;
          script += `  mapping = EXCLUDED.mapping,\n`;
          script += `  formula_config = EXCLUDED.formula_config,\n`;
          script += `  filter_config = EXCLUDED.filter_config,\n`;
          script += `  pivot_config = EXCLUDED.pivot_config,\n`;
          script += `  metadata = EXCLUDED.metadata,\n`;
          script += `  description = EXCLUDED.description,\n`;
          script += `  updated_by = EXCLUDED.updated_by,\n`;
          script += `  updated_at = CURRENT_TIMESTAMP;\n\n`;
          break;

        case 'mysql':
          // Add table DDL in the header if this is the first hierarchy
          if (hierarchiesToGenerate.indexOf(hierarchy) === 0) {
            let headerScript = `/*\n`;
            headerScript += `============================================================================\n`;
            headerScript += `TABLE CREATION DDL (for reference - uncomment if table doesn't exist)\n`;
            headerScript += `============================================================================\n`;
            headerScript += `-- Create master table\n`;
            headerScript += `CREATE TABLE IF NOT EXISTS ${tableName} (\n`;
            headerScript += `  project_id VARCHAR(255) NOT NULL,\n`;
            headerScript += `  project_name VARCHAR(500),\n`;
            headerScript += `  hierarchy_id VARCHAR(255) NOT NULL,\n`;
            headerScript += `  hierarchy_name VARCHAR(500),\n`;
            headerScript += `  parent_id VARCHAR(255),\n`;
            headerScript += `  is_root BOOLEAN DEFAULT FALSE,\n`;
            headerScript += `  sort_order INT DEFAULT 0,\n`;
            headerScript += `  hierarchy_level JSON,\n`;
            headerScript += `  flags JSON,\n`;
            headerScript += `  mapping JSON,\n`;
            headerScript += `  formula_config JSON,\n`;
            headerScript += `  filter_config JSON,\n`;
            headerScript += `  pivot_config JSON,\n`;
            headerScript += `  description VARCHAR(1000),\n`;
            headerScript += `  created_by VARCHAR(255),\n`;
            headerScript += `  updated_by VARCHAR(255),\n`;
            headerScript += `  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n`;
            headerScript += `  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n`;
            headerScript += `  PRIMARY KEY (project_id, hierarchy_id)\n`;
            headerScript += `) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n`;
            headerScript += `*/\n\n`;
            // Insert the DDL header at the top of the script
            const insertPos = script.indexOf(
              '-- ============================================================================\n-- INSERT/MERGE STATEMENTS',
            );
            if (insertPos > 0) {
              script = script.substring(0, insertPos) + headerScript + script.substring(insertPos);
            }
          }

          script += `INSERT INTO ${qualifiedTableName} (\n`;
          script += `  project_id, project_name, hierarchy_id, hierarchy_name, parent_id, is_root, sort_order, hierarchy_level, flags, mapping,\n`;
          script += `  formula_config, filter_config, pivot_config, description, created_by, updated_by\n`;
          script += `) VALUES (\n`;
          script += `  '${projectIdValue}', '${this.escapeString(projectName, dbType)}', '${nodeIdValue}', '${hierarchyNameValue}', ${parentNodeIdValue}, ${node.isRoot ? 'TRUE' : 'FALSE'}, ${node.sortOrder},\n`;
          script += `  '${this.escapeJson(node.hierarchyLevel)}',\n`;
          script += `  '${this.escapeJson(node.flags)}',\n`;
          script += `  '${this.escapeJson(node.mapping)}',\n`;
          script += `  ${node.formulaConfig ? `'${this.escapeJson(node.formulaConfig)}'` : 'NULL'},\n`;
          script += `  ${node.filterConfig ? `'${this.escapeJson(node.filterConfig)}'` : 'NULL'},\n`;
          script += `  ${node.pivotConfig ? `'${this.escapeJson(node.pivotConfig)}'` : 'NULL'},\n`;
          script += `  '${descValue}', ${deployedBy ? `'${this.escapeString(deployedBy, dbType)}'` : 'NULL'}, ${deployedBy ? `'${this.escapeString(deployedBy, dbType)}'` : 'NULL'}\n`;
          script += `)\n`;
          script += `ON DUPLICATE KEY UPDATE\n`;
          script += `  project_name = VALUES(project_name),\n`;
          script += `  hierarchy_name = VALUES(hierarchy_name),\n`;
          script += `  parent_id = VALUES(parent_id),\n`;
          script += `  is_root = VALUES(is_root),\n`;
          script += `  sort_order = VALUES(sort_order),\n`;
          script += `  hierarchy_level = VALUES(hierarchy_level),\n`;
          script += `  flags = VALUES(flags),\n`;
          script += `  mapping = VALUES(mapping),\n`;
          script += `  formula_config = VALUES(formula_config),\n`;
          script += `  filter_config = VALUES(filter_config),\n`;
          script += `  pivot_config = VALUES(pivot_config),\n`;
          script += `  description = VALUES(description),\n`;
          script += `  updated_by = VALUES(updated_by);\n\n`;
          break;

        case 'sqlserver':
          // Add table DDL in the header if this is the first hierarchy
          if (hierarchiesToGenerate.indexOf(hierarchy) === 0) {
            let headerScript = `/*\n`;
            headerScript += `============================================================================\n`;
            headerScript += `TABLE CREATION DDL (for reference - uncomment if table doesn't exist)\n`;
            headerScript += `============================================================================\n`;
            headerScript += `-- Create sequence for hierarchy IDs\n`;
            headerScript += `IF NOT EXISTS (SELECT * FROM sys.sequences WHERE name = 'seq_hierarchy')\n`;
            headerScript += `  CREATE SEQUENCE seq_hierarchy START WITH 1 INCREMENT BY 1;\n\n`;
            headerScript += `-- Create master table\n`;
            headerScript += `IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'hierarchy_master')\n`;
            headerScript += `CREATE TABLE ${tableName} (\n`;
            headerScript += `  project_id VARCHAR(255) NOT NULL,\n`;
            headerScript += `  project_name VARCHAR(500),\n`;
            headerScript += `  hierarchy_id VARCHAR(255) NOT NULL,\n`;
            headerScript += `  hierarchy_name VARCHAR(500),\n`;
            headerScript += `  parent_id VARCHAR(255),\n`;
            headerScript += `  is_root BIT DEFAULT 0,\n`;
            headerScript += `  sort_order INT DEFAULT 0,\n`;
            headerScript += `  hierarchy_level NVARCHAR(MAX),\n`;
            headerScript += `  flags NVARCHAR(MAX),\n`;
            headerScript += `  mapping NVARCHAR(MAX),\n`;
            headerScript += `  formula_config NVARCHAR(MAX),\n`;
            headerScript += `  filter_config NVARCHAR(MAX),\n`;
            headerScript += `  pivot_config NVARCHAR(MAX),\n`;
            headerScript += `  description VARCHAR(1000),\n`;
            headerScript += `  created_by VARCHAR(255),\n`;
            headerScript += `  updated_by VARCHAR(255),\n`;
            headerScript += `  created_at DATETIME2 DEFAULT GETDATE(),\n`;
            headerScript += `  updated_at DATETIME2 DEFAULT GETDATE(),\n`;
            headerScript += `  PRIMARY KEY (project_id, hierarchy_id)\n`;
            headerScript += `);\n`;
            headerScript += `*/\n\n`;
            // Insert the DDL header at the top of the script
            const insertPos = script.indexOf(
              '-- ============================================================================\n-- INSERT/MERGE STATEMENTS',
            );
            if (insertPos > 0) {
              script = script.substring(0, insertPos) + headerScript + script.substring(insertPos);
            }
          }

          script += `MERGE INTO ${qualifiedTableName} AS target\n`;
          script += `USING (\n`;
          script += `  SELECT\n`;
          script += `    '${projectIdValue}' AS project_id,\n`;
          script += `    '${this.escapeString(projectName, dbType)}' AS project_name,\n`;
          script += `    '${nodeIdValue}' AS hierarchy_id,\n`;
          script += `    '${hierarchyNameValue}' AS hierarchy_name,\n`;
          script += `    ${parentNodeIdValue} AS parent_id,\n`;
          script += `    ${node.isRoot ? '1' : '0'} AS is_root,\n`;
          script += `    ${node.sortOrder} AS sort_order,\n`;
          script += `    '${this.escapeJson(node.hierarchyLevel)}' AS hierarchy_level,\n`;
          script += `    '${this.escapeJson(node.flags)}' AS flags,\n`;
          script += `    '${this.escapeJson(node.mapping)}' AS mapping,\n`;
          script += `    ${node.formulaConfig ? `'${this.escapeJson(node.formulaConfig)}'` : 'NULL'} AS formula_config,\n`;
          script += `    ${node.filterConfig ? `'${this.escapeJson(node.filterConfig)}'` : 'NULL'} AS filter_config,\n`;
          script += `    ${node.pivotConfig ? `'${this.escapeJson(node.pivotConfig)}'` : 'NULL'} AS pivot_config,\n`;
          script += `    '${descValue}' AS description,\n`;
          script += `    ${deployedBy ? `'${this.escapeString(deployedBy, dbType)}'` : 'NULL'} AS created_by,\n`;
          script += `    ${deployedBy ? `'${this.escapeString(deployedBy, dbType)}'` : 'NULL'} AS updated_by\n`;
          script += `) AS source\n`;
          script += `ON target.project_id = source.project_id AND target.hierarchy_id = source.hierarchy_id\n`;
          script += `WHEN MATCHED THEN\n`;
          script += `  UPDATE SET\n`;
          script += `    project_name = source.project_name,\n`;
          script += `    hierarchy_name = source.hierarchy_name,\n`;
          script += `    parent_id = source.parent_id,\n`;
          script += `    is_root = source.is_root,\n`;
          script += `    sort_order = source.sort_order,\n`;
          script += `    hierarchy_level = source.hierarchy_level,\n`;
          script += `    flags = source.flags,\n`;
          script += `    mapping = source.mapping,\n`;
          script += `    formula_config = source.formula_config,\n`;
          script += `    filter_config = source.filter_config,\n`;
          script += `    pivot_config = source.pivot_config,\n`;
          script += `    description = source.description,\n`;
          script += `    updated_by = source.updated_by,\n`;
          script += `    updated_at = GETDATE()\n`;
          script += `WHEN NOT MATCHED THEN\n`;
          script += `  INSERT (project_id, project_name, hierarchy_id, hierarchy_name, parent_id, is_root, sort_order, hierarchy_level, flags, mapping,\n`;
          script += `          formula_config, filter_config, pivot_config, description, created_by, updated_by, created_at, updated_at)\n`;
          script += `  VALUES (source.project_id, source.project_name, source.hierarchy_id, source.hierarchy_name, source.parent_id, source.is_root, source.sort_order, source.hierarchy_level,\n`;
          script += `          source.flags, source.mapping, source.formula_config,\n`;
          script += `          source.filter_config, source.pivot_config, source.description, source.created_by, source.updated_by,\n`;
          script += `          GETDATE(), GETDATE());\n\n`;
          break;

        default:
          // Fallback - should not reach here
          script += `-- Database type ${dbType} not fully implemented\n\n`;
          break;
      }
    }

    return script;
  }

  // ============================================================================
  // Project View Script Generation (VW_{PROJECT_NAME} filtering by PROJECT_ID)
  // ============================================================================

  private generateMasterViewScript(
    hierarchies: SmartHierarchyMaster[],
    projectName: string,
    dbType: 'snowflake' | 'postgres' | 'mysql' | 'sqlserver' = 'snowflake',
  ): string {
    if (!hierarchies || hierarchies.length === 0) {
      return '-- No hierarchies to generate view for\n';
    }

    const projectId = hierarchies[0].projectId;

    let script = `-- PROJECT VIEW filtering master table by PROJECT_ID\n`;
    script += `-- Project: ${projectName} (ID: ${projectId})\n`;
    script += `-- Database Type: ${dbType.toUpperCase()}\n`;
    script += `-- Generated at: ${new Date().toISOString()}\n\n`;

    const sanitizedProjectName = projectName.toUpperCase().replace(/[^A-Z0-9_]/g, '_');
    const projectIdValue = this.escapeString(projectId, dbType);

    switch (dbType) {
      case 'snowflake':
        const viewNameSF = `VW_${sanitizedProjectName}`;
        const masterTableSF = 'HIERARCHY_MASTER';

        script += `CREATE OR REPLACE VIEW ${viewNameSF} AS\n`;
        script += `SELECT\n`;
        script += `  PROJECT_ID,\n`;
        script += `  HIERARCHY_ID,\n`;
        script += `  HIERARCHY_NAME,\n`;
        script += `  HIERARCHY_LEVEL,\n`;
        script += `  FLAGS,\n`;
        script += `  MAPPING,\n`;
        script += `  FORMULA_CONFIG,\n`;
        script += `  FILTER_CONFIG,\n`;
        script += `  PIVOT_CONFIG,\n`;
        script += `  DESCRIPTION,\n`;
        script += `  CREATED_AT,\n`;
        script += `  UPDATED_AT\n`;
        script += `FROM ${masterTableSF}\n`;
        script += `WHERE PROJECT_ID = '${projectIdValue}';\n\n`;
        break;

      case 'postgres':
        const viewNamePG = `vw_${sanitizedProjectName.toLowerCase()}`;
        const masterTablePG = 'hierarchy_master';

        script += `CREATE OR REPLACE VIEW ${viewNamePG} AS\n`;
        script += `SELECT\n`;
        script += `  project_id,\n`;
        script += `  hierarchy_id,\n`;
        script += `  hierarchy_name,\n`;
        script += `  hierarchy_level,\n`;
        script += `  flags,\n`;
        script += `  mapping,\n`;
        script += `  formula_config,\n`;
        script += `  filter_config,\n`;
        script += `  pivot_config,\n`;
        script += `  description,\n`;
        script += `  created_at,\n`;
        script += `  updated_at\n`;
        script += `FROM ${masterTablePG}\n`;
        script += `WHERE project_id = '${projectIdValue}';\n\n`;
        break;

      case 'mysql':
        const viewNameMySQL = `vw_${sanitizedProjectName.toLowerCase()}`;
        const masterTableMySQL = 'hierarchy_master';

        script += `CREATE OR REPLACE VIEW ${viewNameMySQL} AS\n`;
        script += `SELECT\n`;
        script += `  project_id,\n`;
        script += `  hierarchy_id,\n`;
        script += `  hierarchy_name,\n`;
        script += `  hierarchy_level,\n`;
        script += `  flags,\n`;
        script += `  mapping,\n`;
        script += `  formula_config,\n`;
        script += `  filter_config,\n`;
        script += `  pivot_config,\n`;
        script += `  description,\n`;
        script += `  created_at,\n`;
        script += `  updated_at\n`;
        script += `FROM ${masterTableMySQL}\n`;
        script += `WHERE project_id = '${projectIdValue}';\n\n`;
        break;

      case 'sqlserver':
        const viewNameSS = `VW_${sanitizedProjectName}`;
        const masterTableSS = 'HIERARCHY_MASTER';

        script += `CREATE OR ALTER VIEW ${viewNameSS} AS\n`;
        script += `SELECT\n`;
        script += `  PROJECT_ID,\n`;
        script += `  HIERARCHY_ID,\n`;
        script += `  HIERARCHY_NAME,\n`;
        script += `  HIERARCHY_LEVEL,\n`;
        script += `  FLAGS,\n`;
        script += `  MAPPING,\n`;
        script += `  FORMULA_CONFIG,\n`;
        script += `  FILTER_CONFIG,\n`;
        script += `  PIVOT_CONFIG,\n`;
        script += `  DESCRIPTION,\n`;
        script += `  CREATED_AT,\n`;
        script += `  UPDATED_AT\n`;
        script += `FROM ${masterTableSS}\n`;
        script += `WHERE PROJECT_ID = '${projectIdValue}';\n`;
        script += `GO\n\n`;
        break;
    }

    return script;
  }

  // ============================================================================
  // INSERT Script Generation (deprecated - replaced by generateMasterTableScript)
  // ============================================================================

  private generateInsertScript(
    hierarchy: SmartHierarchyMaster,
    projectName: string,
    dbType: 'snowflake' | 'postgres' | 'mysql' | 'sqlserver' = 'snowflake',
  ): string {
    const levelColumns = this.getActiveLevelColumns(hierarchy.hierarchyLevel);
    const mappingJson = JSON.stringify(hierarchy.mapping, null, 2);
    const flagsJson = JSON.stringify(hierarchy.flags, null, 2);

    let script = `-- MERGE/UPSERT script for hierarchy: ${hierarchy.hierarchyName}\n`;
    script += `-- Project: ${projectName}\n`;
    script += `-- Database Type: ${dbType.toUpperCase()}\n`;
    script += `-- Generated at: ${new Date().toISOString()}\n\n`;

    const tableName = `TBL_${projectName.toUpperCase().replace(/[^A-Z0-9_]/g, '_')}_HIERARCHY_MASTER`;
    const projectIdValue = this.escapeString(hierarchy.projectId, dbType);
    const hierarchyIdValue = this.escapeString(hierarchy.hierarchyId, dbType);
    const descValue = this.escapeString(hierarchy.description || '', dbType);

    switch (dbType) {
      case 'snowflake':
        script += `MERGE INTO ${tableName} AS target\n`;
        script += `USING (\n`;
        script += `  SELECT\n`;
        script += `    '${projectIdValue}' AS PROJECT_NAME,\n`;
        script += `    '${hierarchyIdValue}' AS HIERARCHY_ID,\n`;
        script += `    PARSE_JSON('${this.escapeJson(hierarchy.hierarchyLevel)}') AS HIERARCHY_LEVEL,\n`;
        script += `    PARSE_JSON('${this.escapeJson(hierarchy.flags)}') AS FLAGS,\n`;
        script += `    PARSE_JSON('${this.escapeJson(hierarchy.mapping)}') AS MAPPING,\n`;
        script += `    ${hierarchy.formulaConfig ? `PARSE_JSON('${this.escapeJson(hierarchy.formulaConfig)}')` : 'NULL'} AS FORMULA_CONFIG,\n`;
        script += `    ${hierarchy.filterConfig ? `PARSE_JSON('${this.escapeJson(hierarchy.filterConfig)}')` : 'NULL'} AS FILTER_CONFIG,\n`;
        script += `    ${hierarchy.pivotConfig ? `PARSE_JSON('${this.escapeJson(hierarchy.pivotConfig)}')` : 'NULL'} AS PIVOT_CONFIG,\n`;
        script += `    '${descValue}' AS DESCRIPTION\n`;
        script += `) AS source\n`;
        script += `ON target.PROJECT_NAME = source.PROJECT_NAME AND target.HIERARCHY_ID = source.HIERARCHY_ID\n`;
        script += `WHEN MATCHED THEN\n`;
        script += `  UPDATE SET\n`;
        script += `    HIERARCHY_LEVEL = source.HIERARCHY_LEVEL,\n`;
        script += `    FLAGS = source.FLAGS,\n`;
        script += `    MAPPING = source.MAPPING,\n`;
        script += `    FORMULA_CONFIG = source.FORMULA_CONFIG,\n`;
        script += `    FILTER_CONFIG = source.FILTER_CONFIG,\n`;
        script += `    PIVOT_CONFIG = source.PIVOT_CONFIG,\n`;
        script += `    DESCRIPTION = source.DESCRIPTION\n`;
        script += `WHEN NOT MATCHED THEN\n`;
        script += `  INSERT (PROJECT_NAME, HIERARCHY_ID, HIERARCHY_LEVEL, FLAGS, MAPPING,\n`;
        script += `          FORMULA_CONFIG, FILTER_CONFIG, PIVOT_CONFIG, DESCRIPTION)\n`;
        script += `  VALUES (source.PROJECT_NAME, source.HIERARCHY_ID, source.HIERARCHY_LEVEL,\n`;
        script += `          source.FLAGS, source.MAPPING, source.FORMULA_CONFIG,\n`;
        script += `          source.FILTER_CONFIG, source.PIVOT_CONFIG, source.DESCRIPTION);\n`;
        break;

      case 'postgres':
        script += `INSERT INTO ${tableName.toLowerCase()} (\n`;
        script += `  project_name, hierarchy_id, hierarchy_level, flags, mapping,\n`;
        script += `  formula_config, filter_config, pivot_config, description\n`;
        script += `) VALUES (\n`;
        script += `  '${projectIdValue}',\n`;
        script += `  '${hierarchyIdValue}',\n`;
        script += `  '${this.escapeJson(hierarchy.hierarchyLevel)}'::jsonb,\n`;
        script += `  '${this.escapeJson(hierarchy.flags)}'::jsonb,\n`;
        script += `  '${this.escapeJson(hierarchy.mapping)}'::jsonb,\n`;
        script += `  ${hierarchy.formulaConfig ? `'${this.escapeJson(hierarchy.formulaConfig)}'::jsonb` : 'NULL'},\n`;
        script += `  ${hierarchy.filterConfig ? `'${this.escapeJson(hierarchy.filterConfig)}'::jsonb` : 'NULL'},\n`;
        script += `  ${hierarchy.pivotConfig ? `'${this.escapeJson(hierarchy.pivotConfig)}'::jsonb` : 'NULL'},\n`;
        script += `  '${descValue}'\n`;
        script += `)\n`;
        script += `ON CONFLICT (project_name, hierarchy_id)\n`;
        script += `DO UPDATE SET\n`;
        script += `  hierarchy_level = EXCLUDED.hierarchy_level,\n`;
        script += `  flags = EXCLUDED.flags,\n`;
        script += `  mapping = EXCLUDED.mapping,\n`;
        script += `  formula_config = EXCLUDED.formula_config,\n`;
        script += `  filter_config = EXCLUDED.filter_config,\n`;
        script += `  pivot_config = EXCLUDED.pivot_config,\n`;
        script += `  description = EXCLUDED.description;\n`;
        break;

      case 'mysql':
        script += `INSERT INTO ${tableName.toLowerCase()} (\n`;
        script += `  project_name, hierarchy_id, hierarchy_level, flags, mapping,\n`;
        script += `  formula_config, filter_config, pivot_config, description\n`;
        script += `) VALUES (\n`;
        script += `  '${projectIdValue}',\n`;
        script += `  '${hierarchyIdValue}',\n`;
        script += `  '${this.escapeJson(hierarchy.hierarchyLevel)}',\n`;
        script += `  '${this.escapeJson(hierarchy.flags)}',\n`;
        script += `  '${this.escapeJson(hierarchy.mapping)}',\n`;
        script += `  ${hierarchy.formulaConfig ? `'${this.escapeJson(hierarchy.formulaConfig)}'` : 'NULL'},\n`;
        script += `  ${hierarchy.filterConfig ? `'${this.escapeJson(hierarchy.filterConfig)}'` : 'NULL'},\n`;
        script += `  ${hierarchy.pivotConfig ? `'${this.escapeJson(hierarchy.pivotConfig)}'` : 'NULL'},\n`;
        script += `  '${descValue}'\n`;
        script += `)\n`;
        script += `ON DUPLICATE KEY UPDATE\n`;
        script += `  hierarchy_level = VALUES(hierarchy_level),\n`;
        script += `  flags = VALUES(flags),\n`;
        script += `  mapping = VALUES(mapping),\n`;
        script += `  formula_config = VALUES(formula_config),\n`;
        script += `  filter_config = VALUES(filter_config),\n`;
        script += `  pivot_config = VALUES(pivot_config),\n`;
        script += `  description = VALUES(description);\n`;
        break;

      case 'sqlserver':
        script += `MERGE INTO ${tableName} AS target\n`;
        script += `USING (\n`;
        script += `  SELECT\n`;
        script += `    '${projectIdValue}' AS PROJECT_NAME,\n`;
        script += `    '${hierarchyIdValue}' AS HIERARCHY_ID,\n`;
        script += `    '${this.escapeJson(hierarchy.hierarchyLevel)}' AS HIERARCHY_LEVEL,\n`;
        script += `    '${this.escapeJson(hierarchy.flags)}' AS FLAGS,\n`;
        script += `    '${this.escapeJson(hierarchy.mapping)}' AS MAPPING,\n`;
        script += `    ${hierarchy.formulaConfig ? `'${this.escapeJson(hierarchy.formulaConfig)}'` : 'NULL'} AS FORMULA_CONFIG,\n`;
        script += `    ${hierarchy.filterConfig ? `'${this.escapeJson(hierarchy.filterConfig)}'` : 'NULL'} AS FILTER_CONFIG,\n`;
        script += `    ${hierarchy.pivotConfig ? `'${this.escapeJson(hierarchy.pivotConfig)}'` : 'NULL'} AS PIVOT_CONFIG,\n`;
        script += `    '${descValue}' AS DESCRIPTION\n`;
        script += `) AS source\n`;
        script += `ON target.PROJECT_NAME = source.PROJECT_NAME AND target.HIERARCHY_ID = source.HIERARCHY_ID\n`;
        script += `WHEN MATCHED THEN\n`;
        script += `  UPDATE SET\n`;
        script += `    HIERARCHY_LEVEL = source.HIERARCHY_LEVEL,\n`;
        script += `    FLAGS = source.FLAGS,\n`;
        script += `    MAPPING = source.MAPPING,\n`;
        script += `    FORMULA_CONFIG = source.FORMULA_CONFIG,\n`;
        script += `    FILTER_CONFIG = source.FILTER_CONFIG,\n`;
        script += `    PIVOT_CONFIG = source.PIVOT_CONFIG,\n`;
        script += `    DESCRIPTION = source.DESCRIPTION\n`;
        script += `WHEN NOT MATCHED THEN\n`;
        script += `  INSERT (PROJECT_NAME, HIERARCHY_ID, HIERARCHY_LEVEL, FLAGS, MAPPING,\n`;
        script += `          FORMULA_CONFIG, FILTER_CONFIG, PIVOT_CONFIG, DESCRIPTION)\n`;
        script += `  VALUES (source.PROJECT_NAME, source.HIERARCHY_ID, source.HIERARCHY_LEVEL,\n`;
        script += `          source.FLAGS, source.MAPPING, source.FORMULA_CONFIG,\n`;
        script += `          source.FILTER_CONFIG, source.PIVOT_CONFIG, source.DESCRIPTION);\n`;
        break;
    }

    return script;
  }

  // ============================================================================
  // UNIFIED HIERARCHICAL VIEW - Single view for entire project with all hierarchies
  // ============================================================================

  private generateUnifiedHierarchyViewScript(
    hierarchies: SmartHierarchyMaster[],
    projectName: string,
    dbType: 'snowflake' | 'postgres' | 'mysql' | 'sqlserver' = 'snowflake',
    deployedBy?: string, // User email for UPDATED_BY field
    database?: string, // Target database for qualified names
    schema?: string, // Target schema for qualified names
  ): string {
    const sanitizedProjectName = projectName.toUpperCase().replace(/[^A-Z0-9_]/g, '_');
    const viewName = `VW_${sanitizedProjectName}_HIERARCHY_MASTER`;
    const masterTableName = dbType === 'snowflake' ? 'HIERARCHY_MASTER' : 'hierarchy_master';

    // Build fully qualified view name if database and schema are provided
    const qualifiedViewName = database && schema ? `${database}.${schema}.${viewName}` : viewName;
    const qualifiedTableName =
      database && schema ? `${database}.${schema}.${masterTableName}` : masterTableName;

    let script = `-- UNIFIED HIERARCHICAL VIEW for entire project: ${projectName}\n`;
    script += `-- Database Type: ${dbType.toUpperCase()}\n`;
    script += `-- This single view contains ALL hierarchies with complete unfolding\n`;
    script += `-- Includes recursive parent-child relationships and all configurations\n`;
    script += `-- Total Hierarchies: ${hierarchies.length}\n`;
    script += `-- PROJECT_ID Filter: '${hierarchies[0].projectId}'\n`;
    script += `-- Generated at: ${new Date().toISOString()}\n\n`;

    script += `-- TROUBLESHOOTING: If view returns no data, run these diagnostic queries:\n`;
    script += `-- 1. Check if table exists: SHOW TABLES LIKE '${masterTableName.toUpperCase()}' IN ${database || 'CURRENT'}.${schema || 'CURRENT'};\n`;
    script += `-- 2. Check PROJECT_IDs in table: SELECT DISTINCT PROJECT_ID FROM ${qualifiedTableName};\n`;
    script += `-- 3. Check record count: SELECT COUNT(*) FROM ${qualifiedTableName} WHERE PROJECT_ID = '${hierarchies[0].projectId}';\n`;
    script += `-- 4. Sample data: SELECT * FROM ${qualifiedTableName} WHERE PROJECT_ID = '${hierarchies[0].projectId}' LIMIT 5;\n\n`;

    // Generate appropriate SQL based on database type
    switch (dbType) {
      case 'snowflake':
        script += `CREATE OR REPLACE VIEW ${qualifiedViewName} AS\n`;
        script += `WITH RECURSIVE hierarchy_tree AS (\n`;
        script += `  -- Base: Root level nodes (Level 1) for ALL hierarchies in project\n`;
        script += `  SELECT\n`;
        script += `    h.PROJECT_ID,\n`;
        script += `    h.PROJECT_NAME,\n`;
        script += `    h.HIERARCHY_ID,\n`;
        script += `    h.HIERARCHY_NAME AS NODE_NAME,\n`;
        script += `    h.HIERARCHY_NAME AS HIERARCHY_NAME,\n`;
        script += `    h.DESCRIPTION,\n`;
        script += `    h.PARENT_ID,\n`;
        script += `    h.IS_ROOT,\n`;
        script += `    h.SORT_ORDER,\n`;
        script += `    \n`;
        script += `    -- Level indicators\n`;
        script += `    1 AS HIERARCHY_LEVEL,\n`;
        script += `    h.HIERARCHY_LEVEL:level_1::VARCHAR AS LEVEL_1_NAME,\n`;
        script += `    h.HIERARCHY_LEVEL:level_1_sort::NUMBER AS LEVEL_1_SORT,\n`;
        script += `    h.HIERARCHY_LEVEL:level_2::VARCHAR AS LEVEL_2_NAME,\n`;
        script += `    h.HIERARCHY_LEVEL:level_2_sort::NUMBER AS LEVEL_2_SORT,\n`;
        script += `    h.HIERARCHY_LEVEL:level_3::VARCHAR AS LEVEL_3_NAME,\n`;
        script += `    h.HIERARCHY_LEVEL:level_3_sort::NUMBER AS LEVEL_3_SORT,\n`;
        script += `    h.HIERARCHY_LEVEL:level_4::VARCHAR AS LEVEL_4_NAME,\n`;
        script += `    h.HIERARCHY_LEVEL:level_4_sort::NUMBER AS LEVEL_4_SORT,\n`;
        script += `    h.HIERARCHY_LEVEL:level_5::VARCHAR AS LEVEL_5_NAME,\n`;
        script += `    h.HIERARCHY_LEVEL:level_5_sort::NUMBER AS LEVEL_5_SORT,\n`;
        script += `    h.HIERARCHY_LEVEL:level_6::VARCHAR AS LEVEL_6_NAME,\n`;
        script += `    h.HIERARCHY_LEVEL:level_6_sort::NUMBER AS LEVEL_6_SORT,\n`;
        script += `    h.HIERARCHY_LEVEL:level_7::VARCHAR AS LEVEL_7_NAME,\n`;
        script += `    h.HIERARCHY_LEVEL:level_7_sort::NUMBER AS LEVEL_7_SORT,\n`;
        script += `    h.HIERARCHY_LEVEL:level_8::VARCHAR AS LEVEL_8_NAME,\n`;
        script += `    h.HIERARCHY_LEVEL:level_8_sort::NUMBER AS LEVEL_8_SORT,\n`;
        script += `    h.HIERARCHY_LEVEL:level_9::VARCHAR AS LEVEL_9_NAME,\n`;
        script += `    h.HIERARCHY_LEVEL:level_9_sort::NUMBER AS LEVEL_9_SORT,\n`;
        script += `    \n`;
        script += `    -- Hierarchy path for sorting\n`;
        script += `    h.HIERARCHY_ID || '/' || LPAD(h.SORT_ORDER::VARCHAR, 5, '0') AS HIERARCHY_PATH,\n`;
        script += `    \n`;
        script += `    -- Flags\n`;
        script += `    h.FLAGS:include_flag::BOOLEAN AS INCLUDE_FLAG,\n`;
        script += `    h.FLAGS:exclude_flag::BOOLEAN AS EXCLUDE_FLAG,\n`;
        script += `    h.FLAGS:transform_flag::BOOLEAN AS TRANSFORM_FLAG,\n`;
        script += `    h.FLAGS:calculation_flag::BOOLEAN AS CALCULATION_FLAG,\n`;
        script += `    h.FLAGS:active_flag::BOOLEAN AS ACTIVE_FLAG,\n`;
        script += `    h.FLAGS:is_leaf_node::BOOLEAN AS IS_LEAF_NODE,\n`;
        script += `    h.FLAGS:is_root::BOOLEAN AS IS_ROOT_FLAG,\n`;
        script += `    \n`;
        script += `    -- Formula Configuration (Priority: METADATA > FORMULA_CONFIG > FILTER_CONFIG)\n`;
        script += `    h.FORMULA_CONFIG:formula_type::VARCHAR AS FORMULA_TYPE,\n`;
        script += `    h.FORMULA_CONFIG:formula_text::VARCHAR AS FORMULA_TEXT,\n`;
        script += `    COALESCE(\n`;
        script += `      h.METADATA:formula_group::VARCHAR,\n`;
        script += `      h.FORMULA_CONFIG:formula_group:mainHierarchyName::VARCHAR,\n`;
        script += `      h.FILTER_CONFIG:total_formula:mainHierarchyName::VARCHAR,\n`;
        script += `      h.FORMULA_CONFIG:formula_group_ref:formulaGroupName::VARCHAR\n`;
        script += `    ) AS FORMULA_GROUP,\n`;
        script += `    COALESCE(\n`;
        script += `      h.METADATA:formula_precedence::NUMBER,\n`;
        script += `      h.FORMULA_CONFIG:formula_group:precedence::NUMBER,\n`;
        script += `      h.FORMULA_CONFIG:formula_group_ref:FORMULA_PRECEDENCE::NUMBER,\n`;
        script += `      h.FORMULA_CONFIG:formula_precedence::NUMBER,\n`;
        script += `      1\n`;
        script += `    ) AS FORMULA_PRECEDENCE,\n`;
        script += `    COALESCE(\n`;
        script += `      h.METADATA:arithmetic_logic::VARCHAR,\n`;
        script += `      h.FILTER_CONFIG:total_formula:aggregation::VARCHAR,\n`;
        script += `      h.FORMULA_CONFIG:formula_group_ref:role::VARCHAR,\n`;
        script += `      h.FORMULA_CONFIG:arithmetic_logic::VARCHAR\n`;
        script += `    ) AS ARITHMETIC_LOGIC,\n`;
        script += `    COALESCE(\n`;
        script += `      h.METADATA:formula_param_ref::VARCHAR,\n`;
        script += `      h.FILTER_CONFIG:total_formula:mainHierarchyName::VARCHAR,\n`;
        script += `      h.FORMULA_CONFIG:formula_group_ref:FORMULA_PARAM_REF::VARCHAR,\n`;
        script += `      h.FORMULA_CONFIG:formula_param_ref::VARCHAR\n`;
        script += `    ) AS FORMULA_PARAM_REF,\n`;
        script += `    COALESCE(\n`;
        script += `      h.METADATA:formula_ref_source::VARCHAR,\n`;
        script += `      h.FORMULA_CONFIG:formula_group_ref:FORMULA_REF_SOURCE::VARCHAR,\n`;
        script += `      h.FORMULA_CONFIG:formula_ref_source::VARCHAR\n`;
        script += `    ) AS FORMULA_REF_SOURCE,\n`;
        script += `    COALESCE(\n`;
        script += `      h.METADATA:formula_ref_table::VARCHAR,\n`;
        script += `      h.FORMULA_CONFIG:formula_group_ref:FORMULA_REF_TABLE::VARCHAR,\n`;
        script += `      h.FORMULA_CONFIG:formula_ref_table::VARCHAR\n`;
        script += `    ) AS FORMULA_REF_TABLE,\n`;
        script += `    COALESCE(\n`;
        script += `      h.METADATA:formula_param2_const_number::NUMBER,\n`;
        script += `      h.FORMULA_CONFIG:formula_group_ref:FORMULA_PARAM2_CONST_NUMBER::NUMBER,\n`;
        script += `      h.FORMULA_CONFIG:formula_param2_const_number::NUMBER\n`;
        script += `    ) AS FORMULA_PARAM2_CONST_NUMBER,\n`;
        script += `    h.FORMULA_CONFIG:sign_change_flag::BOOLEAN AS SIGN_CHANGE_FLAG,\n`;
        script += `    \n`;
        script += `    -- Filter Configuration\n`;
        script += `    h.FILTER_CONFIG:filter_group_1::VARCHAR AS FILTER_GROUP_1,\n`;
        script += `    h.FILTER_CONFIG:filter_group_1_type::VARCHAR AS FILTER_GROUP_1_TYPE,\n`;
        script += `    h.FILTER_CONFIG:filter_group_2::VARCHAR AS FILTER_GROUP_2,\n`;
        script += `    h.FILTER_CONFIG:filter_group_2_type::VARCHAR AS FILTER_GROUP_2_TYPE,\n`;
        script += `    h.FILTER_CONFIG:filter_group_3::VARCHAR AS FILTER_GROUP_3,\n`;
        script += `    h.FILTER_CONFIG:filter_group_3_type::VARCHAR AS FILTER_GROUP_3_TYPE,\n`;
        script += `    h.FILTER_CONFIG:filter_group_4::VARCHAR AS FILTER_GROUP_4,\n`;
        script += `    h.FILTER_CONFIG:filter_group_4_type::VARCHAR AS FILTER_GROUP_4_TYPE,\n`;
        script += `    h.FILTER_CONFIG:custom_sql::VARCHAR AS FILTER_CUSTOM_SQL,\n`;
        script += `    \n`;
        script += `    -- Mapping (JSON for later expansion)\n`;
        script += `    h.MAPPING AS MAPPING_JSON,\n`;
        script += `    \n`;
        script += `    -- Raw JSON columns for expansion CTEs\n`;
        script += `    h.FORMULA_CONFIG,\n`;
        script += `    h.FILTER_CONFIG,\n`;
        script += `    h.PIVOT_CONFIG,\n`;
        script += `    h.FLAGS,\n`;
        script += `    h.METADATA,\n`;
        script += `    \n`;
        script += `    -- Metadata\n`;
        script += `    h.CREATED_AT,\n`;
        script += `    h.UPDATED_AT\n`;
        script += `  FROM ${qualifiedTableName} h\n`;
        script += `  WHERE h.PROJECT_ID = '${hierarchies[0].projectId}'\n`;
        script += `    AND h.PARENT_ID IS NULL\n`;
        script += `  -- Note: Filtering by PROJECT_ID = '${hierarchies[0].projectId}'\n`;
        script += `  -- If no data is returned, verify this PROJECT_ID exists in ${qualifiedTableName}\n`;
        script += `  -- Debug query: SELECT DISTINCT PROJECT_ID FROM ${qualifiedTableName};\n`;
        script += `  \n`;
        script += `  UNION ALL\n`;
        script += `  \n`;
        script += `  -- Recursive: Child nodes at each subsequent level\n`;
        script += `  SELECT\n`;
        script += `    child.PROJECT_ID,\n`;
        script += `    parent.PROJECT_NAME,\n`;
        script += `    child.HIERARCHY_ID,\n`;
        script += `    child.HIERARCHY_NAME AS NODE_NAME,\n`;
        script += `    parent.HIERARCHY_NAME,\n`;
        script += `    child.DESCRIPTION,\n`;
        script += `    child.PARENT_ID,\n`;
        script += `    child.IS_ROOT,\n`;
        script += `    child.SORT_ORDER,\n`;
        script += `    \n`;
        script += `    -- Increment level and extract all levels from child's HIERARCHY_LEVEL JSON (already contains complete path)\n`;
        script += `    parent.HIERARCHY_LEVEL + 1 AS HIERARCHY_LEVEL,\n`;
        script += `    child.HIERARCHY_LEVEL:level_1::VARCHAR AS LEVEL_1_NAME,\n`;
        script += `    child.HIERARCHY_LEVEL:level_1_sort::NUMBER AS LEVEL_1_SORT,\n`;
        script += `    child.HIERARCHY_LEVEL:level_2::VARCHAR AS LEVEL_2_NAME,\n`;
        script += `    child.HIERARCHY_LEVEL:level_2_sort::NUMBER AS LEVEL_2_SORT,\n`;
        script += `    child.HIERARCHY_LEVEL:level_3::VARCHAR AS LEVEL_3_NAME,\n`;
        script += `    child.HIERARCHY_LEVEL:level_3_sort::NUMBER AS LEVEL_3_SORT,\n`;
        script += `    child.HIERARCHY_LEVEL:level_4::VARCHAR AS LEVEL_4_NAME,\n`;
        script += `    child.HIERARCHY_LEVEL:level_4_sort::NUMBER AS LEVEL_4_SORT,\n`;
        script += `    child.HIERARCHY_LEVEL:level_5::VARCHAR AS LEVEL_5_NAME,\n`;
        script += `    child.HIERARCHY_LEVEL:level_5_sort::NUMBER AS LEVEL_5_SORT,\n`;
        script += `    child.HIERARCHY_LEVEL:level_6::VARCHAR AS LEVEL_6_NAME,\n`;
        script += `    child.HIERARCHY_LEVEL:level_6_sort::NUMBER AS LEVEL_6_SORT,\n`;
        script += `    child.HIERARCHY_LEVEL:level_7::VARCHAR AS LEVEL_7_NAME,\n`;
        script += `    child.HIERARCHY_LEVEL:level_7_sort::NUMBER AS LEVEL_7_SORT,\n`;
        script += `    child.HIERARCHY_LEVEL:level_8::VARCHAR AS LEVEL_8_NAME,\n`;
        script += `    child.HIERARCHY_LEVEL:level_8_sort::NUMBER AS LEVEL_8_SORT,\n`;
        script += `    child.HIERARCHY_LEVEL:level_9::VARCHAR AS LEVEL_9_NAME,\n`;
        script += `    child.HIERARCHY_LEVEL:level_9_sort::NUMBER AS LEVEL_9_SORT,\n`;
        script += `    \n`;
        script += `    -- Build hierarchy path\n`;
        script += `    parent.HIERARCHY_PATH || '/' || LPAD(child.SORT_ORDER::VARCHAR, 5, '0') AS HIERARCHY_PATH,\n`;
        script += `    \n`;
        script += `    -- Child's flags and configuration\n`;
        script += `    child.FLAGS:include_flag::BOOLEAN AS INCLUDE_FLAG,\n`;
        script += `    child.FLAGS:exclude_flag::BOOLEAN AS EXCLUDE_FLAG,\n`;
        script += `    child.FLAGS:transform_flag::BOOLEAN AS TRANSFORM_FLAG,\n`;
        script += `    child.FLAGS:calculation_flag::BOOLEAN AS CALCULATION_FLAG,\n`;
        script += `    child.FLAGS:active_flag::BOOLEAN AS ACTIVE_FLAG,\n`;
        script += `    child.FLAGS:is_leaf_node::BOOLEAN AS IS_LEAF_NODE,\n`;
        script += `    child.FLAGS:is_root::BOOLEAN AS IS_ROOT_FLAG,\n`;
        script += `    \n`;
        script += `    child.FORMULA_CONFIG:formula_type::VARCHAR AS FORMULA_TYPE,\n`;
        script += `    child.FORMULA_CONFIG:formula_text::VARCHAR AS FORMULA_TEXT,\n`;
        script += `    COALESCE(\n`;
        script += `      child.METADATA:formula_group::VARCHAR,\n`;
        script += `      child.FORMULA_CONFIG:formula_group:mainHierarchyName::VARCHAR,\n`;
        script += `      child.FILTER_CONFIG:total_formula:mainHierarchyName::VARCHAR,\n`;
        script += `      child.FORMULA_CONFIG:formula_group_ref:formulaGroupName::VARCHAR\n`;
        script += `    ) AS FORMULA_GROUP,\n`;
        script += `    COALESCE(\n`;
        script += `      child.METADATA:formula_precedence::NUMBER,\n`;
        script += `      child.FORMULA_CONFIG:formula_group:precedence::NUMBER,\n`;
        script += `      child.FORMULA_CONFIG:formula_group_ref:FORMULA_PRECEDENCE::NUMBER,\n`;
        script += `      child.FORMULA_CONFIG:formula_precedence::NUMBER,\n`;
        script += `      1\n`;
        script += `    ) AS FORMULA_PRECEDENCE,\n`;
        script += `    COALESCE(\n`;
        script += `      child.METADATA:arithmetic_logic::VARCHAR,\n`;
        script += `      child.FILTER_CONFIG:total_formula:aggregation::VARCHAR,\n`;
        script += `      child.FORMULA_CONFIG:formula_group_ref:role::VARCHAR,\n`;
        script += `      child.FORMULA_CONFIG:arithmetic_logic::VARCHAR\n`;
        script += `    ) AS ARITHMETIC_LOGIC,\n`;
        script += `    COALESCE(\n`;
        script += `      child.METADATA:formula_param_ref::VARCHAR,\n`;
        script += `      child.FILTER_CONFIG:total_formula:mainHierarchyName::VARCHAR,\n`;
        script += `      child.FORMULA_CONFIG:formula_group_ref:FORMULA_PARAM_REF::VARCHAR,\n`;
        script += `      child.FORMULA_CONFIG:formula_param_ref::VARCHAR\n`;
        script += `    ) AS FORMULA_PARAM_REF,\n`;
        script += `    COALESCE(\n`;
        script += `      child.METADATA:formula_ref_source::VARCHAR,\n`;
        script += `      child.FORMULA_CONFIG:formula_group_ref:FORMULA_REF_SOURCE::VARCHAR,\n`;
        script += `      child.FORMULA_CONFIG:formula_ref_source::VARCHAR\n`;
        script += `    ) AS FORMULA_REF_SOURCE,\n`;
        script += `    COALESCE(\n`;
        script += `      child.METADATA:formula_ref_table::VARCHAR,\n`;
        script += `      child.FORMULA_CONFIG:formula_group_ref:FORMULA_REF_TABLE::VARCHAR,\n`;
        script += `      child.FORMULA_CONFIG:formula_ref_table::VARCHAR\n`;
        script += `    ) AS FORMULA_REF_TABLE,\n`;
        script += `    COALESCE(\n`;
        script += `      child.METADATA:formula_param2_const_number::NUMBER,\n`;
        script += `      child.FORMULA_CONFIG:formula_group_ref:FORMULA_PARAM2_CONST_NUMBER::NUMBER,\n`;
        script += `      child.FORMULA_CONFIG:formula_param2_const_number::NUMBER\n`;
        script += `    ) AS FORMULA_PARAM2_CONST_NUMBER,\n`;
        script += `    child.FORMULA_CONFIG:sign_change_flag::BOOLEAN AS SIGN_CHANGE_FLAG,\n`;
        script += `    \n`;
        script += `    child.FILTER_CONFIG:filter_group_1::VARCHAR AS FILTER_GROUP_1,\n`;
        script += `    child.FILTER_CONFIG:filter_group_1_type::VARCHAR AS FILTER_GROUP_1_TYPE,\n`;
        script += `    child.FILTER_CONFIG:filter_group_2::VARCHAR AS FILTER_GROUP_2,\n`;
        script += `    child.FILTER_CONFIG:filter_group_2_type::VARCHAR AS FILTER_GROUP_2_TYPE,\n`;
        script += `    child.FILTER_CONFIG:filter_group_3::VARCHAR AS FILTER_GROUP_3,\n`;
        script += `    child.FILTER_CONFIG:filter_group_3_type::VARCHAR AS FILTER_GROUP_3_TYPE,\n`;
        script += `    child.FILTER_CONFIG:filter_group_4::VARCHAR AS FILTER_GROUP_4,\n`;
        script += `    child.FILTER_CONFIG:filter_group_4_type::VARCHAR AS FILTER_GROUP_4_TYPE,\n`;
        script += `    child.FILTER_CONFIG:custom_sql::VARCHAR AS FILTER_CUSTOM_SQL,\n`;
        script += `    \n`;
        script += `    child.MAPPING AS MAPPING_JSON,\n`;
        script += `    \n`;
        script += `    -- Raw JSON columns for expansion CTEs\n`;
        script += `    child.FORMULA_CONFIG,\n`;
        script += `    child.FILTER_CONFIG,\n`;
        script += `    child.PIVOT_CONFIG,\n`;
        script += `    child.FLAGS,\n`;
        script += `    child.METADATA,\n`;
        script += `    \n`;
        script += `    child.CREATED_AT,\n`;
        script += `    child.UPDATED_AT\n`;
        script += `  FROM hierarchy_tree parent\n`;
        script += `  INNER JOIN ${masterTableName} child\n`;
        script += `    ON child.PARENT_ID = parent.HIERARCHY_ID\n`;
        script += `    AND child.PROJECT_ID = parent.PROJECT_ID\n`;
        script += `  WHERE parent.HIERARCHY_LEVEL < 9\n`;
        script += `),\n`;
        script += `-- Formula Group Expansion: Additional rows for formula rules DIFFERENT from main row\n`;
        script += `formula_expansion AS (\n`;
        script += `  SELECT\n`;
        script += `    parent.PROJECT_ID,\n`;
        script += `    parent.PROJECT_NAME,\n`;
        script += `    parent.HIERARCHY_ID,\n`;
        script += `    parent.NODE_NAME,\n`;
        script += `    parent.HIERARCHY_NAME,\n`;
        script += `    parent.DESCRIPTION,\n`;
        script += `    parent.PARENT_ID,\n`;
        script += `    FALSE AS IS_ROOT,\n`;
        script += `    parent.SORT_ORDER + 1000 + rule.index AS SORT_ORDER,\n`;
        script += `    parent.HIERARCHY_LEVEL AS HIERARCHY_LEVEL,\n`;
        script += `    parent.LEVEL_1_NAME,\n`;
        script += `    parent.LEVEL_1_SORT,\n`;
        script += `    parent.LEVEL_2_NAME,\n`;
        script += `    parent.LEVEL_2_SORT,\n`;
        script += `    parent.LEVEL_3_NAME,\n`;
        script += `    parent.LEVEL_3_SORT,\n`;
        script += `    parent.LEVEL_4_NAME,\n`;
        script += `    parent.LEVEL_4_SORT,\n`;
        script += `    parent.LEVEL_5_NAME,\n`;
        script += `    parent.LEVEL_5_SORT,\n`;
        script += `    parent.LEVEL_6_NAME,\n`;
        script += `    parent.LEVEL_6_SORT,\n`;
        script += `    parent.LEVEL_7_NAME,\n`;
        script += `    parent.LEVEL_7_SORT,\n`;
        script += `    parent.LEVEL_8_NAME,\n`;
        script += `    parent.LEVEL_8_SORT,\n`;
        script += `    parent.LEVEL_9_NAME,\n`;
        script += `    parent.LEVEL_9_SORT,\n`;
        script += `    parent.HIERARCHY_PATH || '/F' || LPAD(rule.index::VARCHAR, 4, '0') AS HIERARCHY_PATH,\n`;
        script += `    parent.INCLUDE_FLAG,\n`;
        script += `    parent.EXCLUDE_FLAG,\n`;
        script += `    parent.TRANSFORM_FLAG,\n`;
        script += `    parent.CALCULATION_FLAG,\n`;
        script += `    parent.ACTIVE_FLAG,\n`;
        script += `    parent.IS_LEAF_NODE,\n`;
        script += `    parent.IS_ROOT_FLAG,\n`;
        script += `    parent.FORMULA_TYPE,\n`;
        script += `    parent.FORMULA_TEXT,\n`;
        script += `    parent.FORMULA_GROUP,\n`;
        script += `    rule.value:precedence::NUMBER AS FORMULA_PRECEDENCE,\n`;
        script += `    rule.value:operation::STRING AS ARITHMETIC_LOGIC,\n`;
        script += `    rule.value:parameterReference::STRING AS FORMULA_PARAM_REF,\n`;
        script += `    CASE WHEN rule.value:isTableReference::BOOLEAN THEN rule.value:parameterReference::STRING ELSE NULL END AS FORMULA_REF_SOURCE,\n`;
        script += `    CASE WHEN rule.value:isTableReference::BOOLEAN THEN rule.value:parameterReference::STRING ELSE NULL END AS FORMULA_REF_TABLE,\n`;
        script += `    NULL AS FORMULA_PARAM2_CONST_NUMBER,\n`;
        script += `    parent.SIGN_CHANGE_FLAG,\n`;
        script += `    parent.FILTER_GROUP_1,\n`;
        script += `    parent.FILTER_GROUP_1_TYPE,\n`;
        script += `    parent.FILTER_GROUP_2,\n`;
        script += `    parent.FILTER_GROUP_2_TYPE,\n`;
        script += `    parent.FILTER_GROUP_3,\n`;
        script += `    parent.FILTER_GROUP_3_TYPE,\n`;
        script += `    parent.FILTER_GROUP_4,\n`;
        script += `    parent.FILTER_GROUP_4_TYPE,\n`;
        script += `    parent.FILTER_CUSTOM_SQL,\n`;
        script += `    parent.MAPPING_JSON,\n`;
        script += `    parent.FORMULA_CONFIG,\n`;
        script += `    parent.FILTER_CONFIG,\n`;
        script += `    parent.PIVOT_CONFIG,\n`;
        script += `    parent.FLAGS,\n`;
        script += `    parent.METADATA,\n`;
        script += `    parent.CREATED_AT,\n`;
        script += `    parent.UPDATED_AT\n`;
        script += `  FROM hierarchy_tree parent,\n`;
        script += `  LATERAL FLATTEN(input => parent.FORMULA_CONFIG:formula_group.rules) rule\n`;
        script += `  WHERE parent.FORMULA_CONFIG IS NOT NULL\n`;
        script += `    AND parent.FORMULA_CONFIG:formula_group.rules IS NOT NULL\n`;
        script += `    AND (\n`;
        script += `      rule.value:operation::STRING != parent.METADATA:arithmetic_logic::STRING\n`;
        script += `      OR rule.value:parameterReference::STRING != parent.METADATA:formula_param_ref::STRING\n`;
        script += `    )\n`;
        script += `),\n`;
        script += `-- Combine actual hierarchies with expanded formula rows\n`;
        script += `combined_hierarchy AS (\n`;
        script += `  SELECT * FROM hierarchy_tree\n`;
        script += `  UNION ALL\n`;
        script += `  SELECT * FROM formula_expansion\n`;
        script += `)\n`;
        script += `SELECT\n`;
        script += `  ROW_NUMBER() OVER (ORDER BY HIERARCHY_ID, SORT_ORDER) AS XREF_HIERARCHY_KEY,\n`;
        script += `  HIERARCHY_NAME AS HIERARCHY_GROUP_NAME,\n`;
        script += `  COALESCE(LEVEL_1_SORT, 0) AS LEVEL_1_SORT,\n`;
        script += `  COALESCE(LEVEL_1_NAME, '') AS LEVEL_1,\n`;
        script += `  COALESCE(LEVEL_2_SORT, 0) AS LEVEL_2_SORT,\n`;
        script += `  COALESCE(LEVEL_2_NAME, '') AS LEVEL_2,\n`;
        script += `  COALESCE(LEVEL_3_SORT, 0) AS LEVEL_3_SORT,\n`;
        script += `  COALESCE(LEVEL_3_NAME, '') AS LEVEL_3,\n`;
        script += `  COALESCE(LEVEL_4_SORT, 0) AS LEVEL_4_SORT,\n`;
        script += `  COALESCE(LEVEL_4_NAME, '') AS LEVEL_4,\n`;
        script += `  COALESCE(LEVEL_5_SORT, 0) AS LEVEL_5_SORT,\n`;
        script += `  COALESCE(LEVEL_5_NAME, '') AS LEVEL_5,\n`;
        script += `  COALESCE(LEVEL_6_SORT, 0) AS LEVEL_6_SORT,\n`;
        script += `  COALESCE(LEVEL_6_NAME, '') AS LEVEL_6,\n`;
        script += `  COALESCE(LEVEL_7_SORT, 0) AS LEVEL_7_SORT,\n`;
        script += `  COALESCE(LEVEL_7_NAME, '') AS LEVEL_7,\n`;
        script += `  COALESCE(LEVEL_8_SORT, 0) AS LEVEL_8_SORT,\n`;
        script += `  COALESCE(LEVEL_8_NAME, '') AS LEVEL_8,\n`;
        script += `  COALESCE(LEVEL_9_SORT, 0) AS LEVEL_9_SORT,\n`;
        script += `  COALESCE(LEVEL_9_NAME, '') AS LEVEL_9,\n`;
        script += `  FORMULA_PRECEDENCE AS GROUP_FILTER_PRECEDENCE,\n`;
        script += `  NODE_NAME AS HIERARCHY_NAME,\n`;
        script += `  PARENT_ID AS PARENT_XREF_KEY,\n`;
        script += `  SORT_ORDER,\n`;
        script += `  IS_ROOT,\n`;
        script += `  COALESCE(FLAGS:customFlags.has_multiple_tables::BOOLEAN, FALSE) AS HAS_MULTIPLE_TABLES,\n`;
        script += `  COALESCE(FLAGS:customFlags.do_not_expand_flag::BOOLEAN, FALSE) AS DO_NOT_EXPAND_FLAG,\n`;
        script += `  COALESCE(FLAGS:customFlags.is_secured_flag::BOOLEAN, FALSE) AS IS_SECURED_FLAG,\n`;
        script += `  COALESCE(FLAGS:customFlags.split_active_flag::BOOLEAN, FALSE) AS SPLIT_ACTIVE_FLAG,\n`;
        script += `  COALESCE(EXCLUDE_FLAG, FALSE) AS EXCLUSION_FLAG,\n`;
        script += `  COALESCE(CALCULATION_FLAG, FALSE) AS CALCULATION_FLAG,\n`;
        script += `  COALESCE(ACTIVE_FLAG, FALSE) AS ACTIVE_FLAG,\n`;
        script += `  COALESCE(FLAGS:customFlags.volume_flag::BOOLEAN, FALSE) AS VOLUME_FLAG,\n`;
        script += `  COALESCE(FLAGS:customFlags.id_unpivot_flag::BOOLEAN, FALSE) AS ID_UNPIVOT_FLAG,\n`;
        script += `  COALESCE(FLAGS:customFlags.id_row_flag::BOOLEAN, FALSE) AS ID_ROW_FLAG,\n`;
        script += `  COALESCE(FLAGS:customFlags.remove_from_totals::BOOLEAN, FALSE) AS REMOVE_FROM_TOTALS,\n`;
        script += `  FILTER_GROUP_1,\n`;
        script += `  FILTER_GROUP_1_TYPE,\n`;
        script += `  FILTER_GROUP_2,\n`;
        script += `  FILTER_GROUP_2_TYPE,\n`;
        script += `  FILTER_GROUP_3,\n`;
        script += `  FILTER_GROUP_3_TYPE,\n`;
        script += `  FILTER_GROUP_4,\n`;
        script += `  FILTER_GROUP_4_TYPE,\n`;
        script += `  FORMULA_GROUP,\n`;
        script += `  COALESCE(SIGN_CHANGE_FLAG, FALSE) AS SIGN_CHANGE_FLAG,\n`;
        script += `  FORMULA_PRECEDENCE,\n`;
        script += `  FORMULA_PARAM_REF,\n`;
        script += `  ARITHMETIC_LOGIC,\n`;
        script += `  FORMULA_REF_SOURCE,\n`;
        script += `  FORMULA_REF_TABLE,\n`;
        script += `  FORMULA_PARAM2_CONST_NUMBER,\n`;
        script += `  COALESCE(FLAGS:customFlags.create_new_column::BOOLEAN, FALSE) AS CREATE_NEW_COLUMN,\n`;
        script += `  ` + (deployedBy ? `'${deployedBy}'` : 'NULL') + ` AS UPDATED_BY,\n`;
        script += `  UPDATED_AT,\n`;
        script += `  FLAGS:customFlags AS CUSTOM_FLAGS,\n`;
        script += `  MAPPING_JSON\n`;
        script += `FROM combined_hierarchy\n`;
        script += `WHERE NOT (HIERARCHY_ID LIKE 'HIER_AUTO_%' AND MAPPING_JSON IS NULL)\n`;
        script += `ORDER BY HIERARCHY_ID, SORT_ORDER;\n\n`;

        // Add diagnostic queries
        script += `-- ========================================\n`;
        script += `-- DIAGNOSTIC QUERIES (Run these to verify data)\n`;
        script += `-- ========================================\n\n`;

        script += `-- 1. Check all records in master table for this project:\n`;
        script += `-- SELECT PROJECT_ID, HIERARCHY_ID, HIERARCHY_NAME, PARENT_ID, IS_ROOT, SORT_ORDER \n`;
        script += `-- FROM ${masterTableName} \n`;
        script += `-- WHERE PROJECT_ID = '${hierarchies[0].projectId}'\n`;
        script += `-- ORDER BY PARENT_ID NULLS FIRST, SORT_ORDER;\n\n`;

        script += `-- 2. Check root records (should return ${hierarchies.length} hierarchies):\n`;
        script += `-- SELECT COUNT(*) as root_count FROM ${masterTableName} \n`;
        script += `-- WHERE PROJECT_ID = '${hierarchies[0].projectId}' AND (PARENT_ID IS NULL OR IS_ROOT = TRUE);\n\n`;

        script += `-- 3. Check parent-child relationships:\n`;
        script += `-- SELECT \n`;
        script += `--   p.HIERARCHY_ID as parent_id,\n`;
        script += `--   p.HIERARCHY_NAME as parent_name,\n`;
        script += `--   c.HIERARCHY_ID as child_id,\n`;
        script += `--   c.HIERARCHY_NAME as child_name,\n`;
        script += `--   c.PARENT_ID as child_parent_id\n`;
        script += `-- FROM ${masterTableName} p\n`;
        script += `-- LEFT JOIN ${masterTableName} c ON c.PARENT_ID = p.HIERARCHY_ID AND c.PROJECT_ID = p.PROJECT_ID\n`;
        script += `-- WHERE p.PROJECT_ID = '${hierarchies[0].projectId}'\n`;
        script += `-- ORDER BY p.SORT_ORDER, c.SORT_ORDER;\n\n`;

        script += `-- 4. Test the view:\n`;
        script += `-- SELECT HIERARCHY_LEVEL, COUNT(*) as record_count \n`;
        script += `-- FROM ${qualifiedViewName} \n`;
        script += `-- GROUP BY HIERARCHY_LEVEL \n`;
        script += `-- ORDER BY HIERARCHY_LEVEL;\n\n`;

        // Add comment about usage
        script += `-- ========================================\n`;
        script += `-- USAGE EXAMPLES\n`;
        script += `-- ========================================\n`;
        script += `-- 1. Get all hierarchies: SELECT * FROM ${qualifiedViewName};\n`;
        script += `-- 2. Get specific hierarchy: SELECT * FROM ${qualifiedViewName} WHERE HIERARCHY_ID = 'your_id';\n`;
        script += `-- 3. Get by level: SELECT * FROM ${qualifiedViewName} WHERE HIERARCHY_LEVEL = 2;\n`;
        script += `-- 4. Get descendants: SELECT * FROM ${qualifiedViewName} WHERE LEVEL_1_NAME = 'Root Name';\n`;
        break;

      case 'postgres':
        const pgViewName = viewName.toLowerCase();
        const pgTableName = masterTableName.toLowerCase();
        // Build qualified names for Postgres (lowercase)
        const qualifiedPgViewName =
          database && schema
            ? `${database.toLowerCase()}.${schema.toLowerCase()}.${pgViewName}`
            : pgViewName;
        const qualifiedPgTableName =
          database && schema
            ? `${database.toLowerCase()}.${schema.toLowerCase()}.${pgTableName}`
            : pgTableName;

        script += `CREATE OR REPLACE VIEW ${qualifiedPgViewName} AS\n`;
        script += `WITH RECURSIVE hierarchy_tree AS (\n`;
        script += `  -- Base: Root level nodes\n`;
        script += `  SELECT\n`;
        script += `    h.project_id,\n`;
        script += `    h.hierarchy_id,\n`;
        script += `    h.hierarchy_name AS node_name,\n`;
        script += `    h.hierarchy_name,\n`;
        script += `    h.description,\n`;
        script += `    h.parent_id,\n`;
        script += `    h.is_root,\n`;
        script += `    h.sort_order,\n`;
        script += `    1 AS hierarchy_level,\n`;
        script += `    h.hierarchy_name AS level_1_name,\n`;
        script += `    h.sort_order AS level_1_sort,\n`;
        script += `    NULL::VARCHAR AS level_2_name, NULL::INTEGER AS level_2_sort,\n`;
        script += `    NULL::VARCHAR AS level_3_name, NULL::INTEGER AS level_3_sort,\n`;
        script += `    NULL::VARCHAR AS level_4_name, NULL::INTEGER AS level_4_sort,\n`;
        script += `    NULL::VARCHAR AS level_5_name, NULL::INTEGER AS level_5_sort,\n`;
        script += `    NULL::VARCHAR AS level_6_name, NULL::INTEGER AS level_6_sort,\n`;
        script += `    NULL::VARCHAR AS level_7_name, NULL::INTEGER AS level_7_sort,\n`;
        script += `    NULL::VARCHAR AS level_8_name, NULL::INTEGER AS level_8_sort,\n`;
        script += `    NULL::VARCHAR AS level_9_name, NULL::INTEGER AS level_9_sort,\n`;
        script += `    LPAD(h.sort_order::VARCHAR, 5, '0') AS hierarchy_path,\n`;
        script += `    (h.flags->>'include_flag')::BOOLEAN AS include_flag,\n`;
        script += `    (h.flags->>'exclude_flag')::BOOLEAN AS exclude_flag,\n`;
        script += `    (h.flags->>'transform_flag')::BOOLEAN AS transform_flag,\n`;
        script += `    (h.flags->>'calculation_flag')::BOOLEAN AS calculation_flag,\n`;
        script += `    (h.flags->>'active_flag')::BOOLEAN AS active_flag,\n`;
        script += `    (h.flags->>'is_leaf_node')::BOOLEAN AS is_leaf_node,\n`;
        script += `    (h.flags->>'is_root')::BOOLEAN AS is_root_flag,\n`;
        script += `    h.formula_config->>'formula_type' AS formula_type,\n`;
        script += `    h.formula_config->>'formula_text' AS formula_text,\n`;
        script += `    h.filter_config->>'filter_group_1' AS filter_group_1,\n`;
        script += `    h.pivot_config->>'do_not_expand_flag' AS do_not_expand_flag,\n`;
        script += `    h.mapping,\n`;
        script += `    h.created_at,\n`;
        script += `    h.updated_at\n`;
        script += `  FROM ${qualifiedPgTableName} h\n`;
        script += `  WHERE h.parent_id IS NULL AND h.is_root = TRUE\n`;
        script += `  \n`;
        script += `  UNION ALL\n`;
        script += `  \n`;
        script += `  -- Recursive: Child nodes\n`;
        script += `  SELECT\n`;
        script += `    child.project_id,\n`;
        script += `    child.hierarchy_id,\n`;
        script += `    child.hierarchy_name AS node_name,\n`;
        script += `    child.hierarchy_name,\n`;
        script += `    child.description,\n`;
        script += `    child.parent_id,\n`;
        script += `    child.is_root,\n`;
        script += `    child.sort_order,\n`;
        script += `    parent.hierarchy_level + 1,\n`;
        script += `    CASE WHEN parent.hierarchy_level = 1 THEN parent.level_1_name ELSE parent.level_1_name END,\n`;
        script += `    parent.level_1_sort,\n`;
        script += `    CASE WHEN parent.hierarchy_level = 1 THEN child.hierarchy_name ELSE parent.level_2_name END,\n`;
        script += `    CASE WHEN parent.hierarchy_level = 1 THEN child.sort_order ELSE parent.level_2_sort END,\n`;
        script += `    CASE WHEN parent.hierarchy_level = 2 THEN child.hierarchy_name ELSE parent.level_3_name END,\n`;
        script += `    CASE WHEN parent.hierarchy_level = 2 THEN child.sort_order ELSE parent.level_3_sort END,\n`;
        script += `    CASE WHEN parent.hierarchy_level = 3 THEN child.hierarchy_name ELSE parent.level_4_name END,\n`;
        script += `    CASE WHEN parent.hierarchy_level = 3 THEN child.sort_order ELSE parent.level_4_sort END,\n`;
        script += `    CASE WHEN parent.hierarchy_level = 4 THEN child.hierarchy_name ELSE parent.level_5_name END,\n`;
        script += `    CASE WHEN parent.hierarchy_level = 4 THEN child.sort_order ELSE parent.level_5_sort END,\n`;
        script += `    CASE WHEN parent.hierarchy_level = 5 THEN child.hierarchy_name ELSE parent.level_6_name END,\n`;
        script += `    CASE WHEN parent.hierarchy_level = 5 THEN child.sort_order ELSE parent.level_6_sort END,\n`;
        script += `    CASE WHEN parent.hierarchy_level = 6 THEN child.hierarchy_name ELSE parent.level_7_name END,\n`;
        script += `    CASE WHEN parent.hierarchy_level = 6 THEN child.sort_order ELSE parent.level_7_sort END,\n`;
        script += `    CASE WHEN parent.hierarchy_level = 7 THEN child.hierarchy_name ELSE parent.level_8_name END,\n`;
        script += `    CASE WHEN parent.hierarchy_level = 7 THEN child.sort_order ELSE parent.level_8_sort END,\n`;
        script += `    CASE WHEN parent.hierarchy_level = 8 THEN child.hierarchy_name ELSE parent.level_9_name END,\n`;
        script += `    CASE WHEN parent.hierarchy_level = 8 THEN child.sort_order ELSE parent.level_9_sort END,\n`;
        script += `    parent.hierarchy_path || '/' || LPAD(child.sort_order::VARCHAR, 5, '0'),\n`;
        script += `    (child.flags->>'include_flag')::BOOLEAN,\n`;
        script += `    (child.flags->>'exclude_flag')::BOOLEAN,\n`;
        script += `    (child.flags->>'transform_flag')::BOOLEAN,\n`;
        script += `    (child.flags->>'calculation_flag')::BOOLEAN,\n`;
        script += `    (child.flags->>'active_flag')::BOOLEAN,\n`;
        script += `    (child.flags->>'is_leaf_node')::BOOLEAN,\n`;
        script += `    (child.flags->>'is_root')::BOOLEAN,\n`;
        script += `    child.formula_config->>'formula_type',\n`;
        script += `    child.formula_config->>'formula_text',\n`;
        script += `    child.filter_config->>'filter_group_1',\n`;
        script += `    child.pivot_config->>'do_not_expand_flag',\n`;
        script += `    child.mapping,\n`;
        script += `    child.created_at,\n`;
        script += `    child.updated_at\n`;
        script += `  FROM hierarchy_tree parent\n`;
        script += `  INNER JOIN ${qualifiedPgTableName} child ON child.parent_id = parent.hierarchy_id\n`;
        script += `    AND child.project_id = parent.project_id\n`;
        script += `  WHERE parent.hierarchy_level < 9\n`;
        script += `)\n`;
        script += `SELECT * FROM hierarchy_tree ORDER BY hierarchy_path;\n`;
        break;

      case 'mysql':
        const mysqlViewName = viewName.toLowerCase();
        const mysqlTableName = masterTableName.toLowerCase();
        // Build qualified names for MySQL (lowercase)
        const qualifiedMysqlViewName =
          database && schema
            ? `${database.toLowerCase()}.${schema.toLowerCase()}.${mysqlViewName}`
            : mysqlViewName;
        const qualifiedMysqlTableName =
          database && schema
            ? `${database.toLowerCase()}.${schema.toLowerCase()}.${mysqlTableName}`
            : mysqlTableName;

        script += `CREATE OR REPLACE VIEW ${qualifiedMysqlViewName} AS\n`;
        script += `WITH RECURSIVE hierarchy_tree AS (\n`;
        script += `  SELECT\n`;
        script += `    h.project_id,\n`;
        script += `    h.hierarchy_id,\n`;
        script += `    h.hierarchy_name AS node_name,\n`;
        script += `    h.hierarchy_name,\n`;
        script += `    h.description,\n`;
        script += `    h.parent_id,\n`;
        script += `    h.is_root,\n`;
        script += `    h.sort_order,\n`;
        script += `    1 AS hierarchy_level,\n`;
        script += `    h.hierarchy_name AS level_1_name,\n`;
        script += `    h.sort_order AS level_1_sort,\n`;
        script += `    NULL AS level_2_name, NULL AS level_2_sort,\n`;
        script += `    NULL AS level_3_name, NULL AS level_3_sort,\n`;
        script += `    NULL AS level_4_name, NULL AS level_4_sort,\n`;
        script += `    NULL AS level_5_name, NULL AS level_5_sort,\n`;
        script += `    NULL AS level_6_name, NULL AS level_6_sort,\n`;
        script += `    NULL AS level_7_name, NULL AS level_7_sort,\n`;
        script += `    NULL AS level_8_name, NULL AS level_8_sort,\n`;
        script += `    NULL AS level_9_name, NULL AS level_9_sort,\n`;
        script += `    LPAD(h.sort_order, 5, '0') AS hierarchy_path,\n`;
        script += `    JSON_UNQUOTE(JSON_EXTRACT(h.flags, '$.include_flag')) AS include_flag,\n`;
        script += `    JSON_UNQUOTE(JSON_EXTRACT(h.flags, '$.exclude_flag')) AS exclude_flag,\n`;
        script += `    JSON_UNQUOTE(JSON_EXTRACT(h.flags, '$.active_flag')) AS active_flag,\n`;
        script += `    JSON_UNQUOTE(JSON_EXTRACT(h.flags, '$.is_leaf_node')) AS is_leaf_node,\n`;
        script += `    JSON_UNQUOTE(JSON_EXTRACT(h.flags, '$.transform_flag')) AS transform_flag,\n`;
        script += `    JSON_UNQUOTE(JSON_EXTRACT(h.flags, '$.calculation_flag')) AS calculation_flag,\n`;
        script += `    h.mapping,\n`;
        script += `    h.created_at,\n`;
        script += `    h.updated_at\n`;
        script += `  FROM ${qualifiedMysqlTableName} h\n`;
        script += `  WHERE h.parent_id IS NULL AND h.is_root = 1\n`;
        script += `  \n`;
        script += `  UNION ALL\n`;
        script += `  \n`;
        script += `  SELECT\n`;
        script += `    child.project_id,\n`;
        script += `    child.hierarchy_id,\n`;
        script += `    child.hierarchy_name,\n`;
        script += `    child.hierarchy_name,\n`;
        script += `    child.description,\n`;
        script += `    child.parent_id,\n`;
        script += `    child.is_root,\n`;
        script += `    child.sort_order,\n`;
        script += `    parent.hierarchy_level + 1,\n`;
        script += `    parent.level_1_name, parent.level_1_sort,\n`;
        script += `    IF(parent.hierarchy_level = 1, child.hierarchy_name, parent.level_2_name),\n`;
        script += `    IF(parent.hierarchy_level = 1, child.sort_order, parent.level_2_sort),\n`;
        script += `    IF(parent.hierarchy_level = 2, child.hierarchy_name, parent.level_3_name),\n`;
        script += `    IF(parent.hierarchy_level = 2, child.sort_order, parent.level_3_sort),\n`;
        script += `    IF(parent.hierarchy_level = 3, child.hierarchy_name, parent.level_4_name),\n`;
        script += `    IF(parent.hierarchy_level = 3, child.sort_order, parent.level_4_sort),\n`;
        script += `    IF(parent.hierarchy_level = 4, child.hierarchy_name, parent.level_5_name),\n`;
        script += `    IF(parent.hierarchy_level = 4, child.sort_order, parent.level_5_sort),\n`;
        script += `    IF(parent.hierarchy_level = 5, child.hierarchy_name, parent.level_6_name),\n`;
        script += `    IF(parent.hierarchy_level = 5, child.sort_order, parent.level_6_sort),\n`;
        script += `    IF(parent.hierarchy_level = 6, child.hierarchy_name, parent.level_7_name),\n`;
        script += `    IF(parent.hierarchy_level = 6, child.sort_order, parent.level_7_sort),\n`;
        script += `    IF(parent.hierarchy_level = 7, child.hierarchy_name, parent.level_8_name),\n`;
        script += `    IF(parent.hierarchy_level = 7, child.sort_order, parent.level_8_sort),\n`;
        script += `    IF(parent.hierarchy_level = 8, child.hierarchy_name, parent.level_9_name),\n`;
        script += `    IF(parent.hierarchy_level = 8, child.sort_order, parent.level_9_sort),\n`;
        script += `    CONCAT(parent.hierarchy_path, '/', LPAD(child.sort_order, 5, '0')),\n`;
        script += `    JSON_UNQUOTE(JSON_EXTRACT(child.flags, '$.include_flag')),\n`;
        script += `    JSON_UNQUOTE(JSON_EXTRACT(child.flags, '$.exclude_flag')),\n`;
        script += `    JSON_UNQUOTE(JSON_EXTRACT(child.flags, '$.active_flag')),\n`;
        script += `    JSON_UNQUOTE(JSON_EXTRACT(child.flags, '$.is_leaf_node')),\n`;
        script += `    JSON_UNQUOTE(JSON_EXTRACT(child.flags, '$.transform_flag')),\n`;
        script += `    JSON_UNQUOTE(JSON_EXTRACT(child.flags, '$.calculation_flag')),\n`;
        script += `    child.mapping,\n`;
        script += `    child.created_at,\n`;
        script += `    child.updated_at\n`;
        script += `  FROM hierarchy_tree parent\n`;
        script += `  INNER JOIN ${qualifiedMysqlTableName} child ON child.parent_id = parent.hierarchy_id\n`;
        script += `    AND child.project_id = parent.project_id\n`;
        script += `  WHERE parent.hierarchy_level < 9\n`;
        script += `)\n`;
        script += `SELECT * FROM hierarchy_tree ORDER BY hierarchy_path;\n`;
        break;

      case 'sqlserver':
        // SQL Server qualified names (database.schema.object format)
        const qualifiedSqlServerViewName =
          database && schema ? `${database}.${schema}.${viewName}` : viewName;
        const qualifiedSqlServerTableName =
          database && schema ? `${database}.${schema}.${masterTableName}` : masterTableName;

        script += `CREATE OR ALTER VIEW ${qualifiedSqlServerViewName} AS\n`;
        script += `WITH hierarchy_tree AS (\n`;
        script += `  SELECT\n`;
        script += `    h.PROJECT_ID,\n`;
        script += `    h.HIERARCHY_ID,\n`;
        script += `    h.HIERARCHY_NAME AS NODE_NAME,\n`;
        script += `    h.HIERARCHY_NAME,\n`;
        script += `    h.DESCRIPTION,\n`;
        script += `    h.PARENT_ID,\n`;
        script += `    h.IS_ROOT,\n`;
        script += `    h.SORT_ORDER,\n`;
        script += `    1 AS HIERARCHY_LEVEL,\n`;
        script += `    h.HIERARCHY_NAME AS LEVEL_1_NAME,\n`;
        script += `    h.SORT_ORDER AS LEVEL_1_SORT,\n`;
        script += `    CAST(NULL AS NVARCHAR(500)) AS LEVEL_2_NAME, CAST(NULL AS INT) AS LEVEL_2_SORT,\n`;
        script += `    CAST(NULL AS NVARCHAR(500)) AS LEVEL_3_NAME, CAST(NULL AS INT) AS LEVEL_3_SORT,\n`;
        script += `    CAST(NULL AS NVARCHAR(500)) AS LEVEL_4_NAME, CAST(NULL AS INT) AS LEVEL_4_SORT,\n`;
        script += `    CAST(NULL AS NVARCHAR(500)) AS LEVEL_5_NAME, CAST(NULL AS INT) AS LEVEL_5_SORT,\n`;
        script += `    CAST(NULL AS NVARCHAR(500)) AS LEVEL_6_NAME, CAST(NULL AS INT) AS LEVEL_6_SORT,\n`;
        script += `    CAST(NULL AS NVARCHAR(500)) AS LEVEL_7_NAME, CAST(NULL AS INT) AS LEVEL_7_SORT,\n`;
        script += `    CAST(NULL AS NVARCHAR(500)) AS LEVEL_8_NAME, CAST(NULL AS INT) AS LEVEL_8_SORT,\n`;
        script += `    CAST(NULL AS NVARCHAR(500)) AS LEVEL_9_NAME, CAST(NULL AS INT) AS LEVEL_9_SORT,\n`;
        script += `    RIGHT('00000' + CAST(h.SORT_ORDER AS NVARCHAR), 5) AS HIERARCHY_PATH,\n`;
        script += `    JSON_VALUE(h.FLAGS, '$.include_flag') AS INCLUDE_FLAG,\n`;
        script += `    JSON_VALUE(h.FLAGS, '$.exclude_flag') AS EXCLUDE_FLAG,\n`;
        script += `    JSON_VALUE(h.FLAGS, '$.active_flag') AS ACTIVE_FLAG,\n`;
        script += `    JSON_VALUE(h.FLAGS, '$.transform_flag') AS TRANSFORM_FLAG,\n`;
        script += `    JSON_VALUE(h.FLAGS, '$.calculation_flag') AS CALCULATION_FLAG,\n`;
        script += `    h.MAPPING,\n`;
        script += `    h.CREATED_AT,\n`;
        script += `    h.UPDATED_AT\n`;
        script += `  FROM ${qualifiedSqlServerTableName} h\n`;
        script += `  WHERE h.PARENT_ID IS NULL AND h.IS_ROOT = 1\n`;
        script += `  \n`;
        script += `  UNION ALL\n`;
        script += `  \n`;
        script += `  SELECT\n`;
        script += `    child.PROJECT_ID,\n`;
        script += `    child.HIERARCHY_ID,\n`;
        script += `    child.HIERARCHY_NAME,\n`;
        script += `    child.HIERARCHY_NAME,\n`;
        script += `    child.DESCRIPTION,\n`;
        script += `    child.PARENT_ID,\n`;
        script += `    child.IS_ROOT,\n`;
        script += `    child.SORT_ORDER,\n`;
        script += `    parent.HIERARCHY_LEVEL + 1,\n`;
        script += `    parent.LEVEL_1_NAME, parent.LEVEL_1_SORT,\n`;
        script += `    CASE WHEN parent.HIERARCHY_LEVEL = 1 THEN child.HIERARCHY_NAME ELSE parent.LEVEL_2_NAME END,\n`;
        script += `    CASE WHEN parent.HIERARCHY_LEVEL = 1 THEN child.SORT_ORDER ELSE parent.LEVEL_2_SORT END,\n`;
        script += `    CASE WHEN parent.HIERARCHY_LEVEL = 2 THEN child.HIERARCHY_NAME ELSE parent.LEVEL_3_NAME END,\n`;
        script += `    CASE WHEN parent.HIERARCHY_LEVEL = 2 THEN child.SORT_ORDER ELSE parent.LEVEL_3_SORT END,\n`;
        script += `    CASE WHEN parent.HIERARCHY_LEVEL = 3 THEN child.HIERARCHY_NAME ELSE parent.LEVEL_4_NAME END,\n`;
        script += `    CASE WHEN parent.HIERARCHY_LEVEL = 3 THEN child.SORT_ORDER ELSE parent.LEVEL_4_SORT END,\n`;
        script += `    CASE WHEN parent.HIERARCHY_LEVEL = 4 THEN child.HIERARCHY_NAME ELSE parent.LEVEL_5_NAME END,\n`;
        script += `    CASE WHEN parent.HIERARCHY_LEVEL = 4 THEN child.SORT_ORDER ELSE parent.LEVEL_5_SORT END,\n`;
        script += `    CASE WHEN parent.HIERARCHY_LEVEL = 5 THEN child.HIERARCHY_NAME ELSE parent.LEVEL_6_NAME END,\n`;
        script += `    CASE WHEN parent.HIERARCHY_LEVEL = 5 THEN child.SORT_ORDER ELSE parent.LEVEL_6_SORT END,\n`;
        script += `    CASE WHEN parent.HIERARCHY_LEVEL = 6 THEN child.HIERARCHY_NAME ELSE parent.LEVEL_7_NAME END,\n`;
        script += `    CASE WHEN parent.HIERARCHY_LEVEL = 6 THEN child.SORT_ORDER ELSE parent.LEVEL_7_SORT END,\n`;
        script += `    CASE WHEN parent.HIERARCHY_LEVEL = 7 THEN child.HIERARCHY_NAME ELSE parent.LEVEL_8_NAME END,\n`;
        script += `    CASE WHEN parent.HIERARCHY_LEVEL = 7 THEN child.SORT_ORDER ELSE parent.LEVEL_8_SORT END,\n`;
        script += `    CASE WHEN parent.HIERARCHY_LEVEL = 8 THEN child.HIERARCHY_NAME ELSE parent.LEVEL_9_NAME END,\n`;
        script += `    CASE WHEN parent.HIERARCHY_LEVEL = 8 THEN child.SORT_ORDER ELSE parent.LEVEL_9_SORT END,\n`;
        script += `    parent.HIERARCHY_PATH + '/' + RIGHT('00000' + CAST(child.SORT_ORDER AS NVARCHAR), 5),\n`;
        script += `    JSON_VALUE(child.FLAGS, '$.include_flag'),\n`;
        script += `    JSON_VALUE(child.FLAGS, '$.exclude_flag'),\n`;
        script += `    JSON_VALUE(child.FLAGS, '$.active_flag'),\n`;
        script += `    JSON_VALUE(child.FLAGS, '$.transform_flag'),\n`;
        script += `    JSON_VALUE(child.FLAGS, '$.calculation_flag'),\n`;
        script += `    child.MAPPING,\n`;
        script += `    child.CREATED_AT,\n`;
        script += `    child.UPDATED_AT\n`;
        script += `  FROM hierarchy_tree parent\n`;
        script += `  INNER JOIN ${qualifiedSqlServerTableName} child ON child.PARENT_ID = parent.HIERARCHY_ID\n`;
        script += `    AND child.PROJECT_ID = parent.PROJECT_ID\n`;
        script += `  WHERE parent.HIERARCHY_LEVEL < 9\n`;
        script += `)\n`;
        script += `SELECT * FROM hierarchy_tree ORDER BY HIERARCHY_PATH;\n`;
        break;
    }

    return script;
  }

  // ============================================================================
  // MAPPING EXPANSION VIEW - Unfold all source mappings for project
  // ============================================================================

  private generateMappingExpansionViewScript(
    hierarchies: SmartHierarchyMaster[],
    projectName: string,
    dbType: 'snowflake' | 'postgres' | 'mysql' | 'sqlserver' = 'snowflake',
    database?: string, // Target database for qualified names
    schema?: string, // Target schema for qualified names
  ): string {
    const sanitizedProjectName = projectName.toUpperCase().replace(/[^A-Z0-9_]/g, '_');
    const viewName = `VW_${sanitizedProjectName}_MAPPING_EXPANSION`;
    const hierarchyViewName = `VW_${sanitizedProjectName}_HIERARCHY_MASTER`;

    // Build fully qualified view names if database and schema are provided
    const qualifiedViewName = database && schema ? `${database}.${schema}.${viewName}` : viewName;
    const qualifiedHierarchyViewName =
      database && schema ? `${database}.${schema}.${hierarchyViewName}` : hierarchyViewName;

    let script = `-- MAPPING EXPANSION VIEW for project: ${projectName}\n`;
    script += `-- Database Type: ${dbType.toUpperCase()}\n`;
    script += `-- This view expands all source mappings from the MAPPING JSON field\n`;
    script += `-- Links mappings to their parent hierarchies for easy querying\n`;
    script += `-- Total Hierarchies: ${hierarchies.length}\n`;
    script += `-- Generated at: ${new Date().toISOString()}\n\n`;

    switch (dbType) {
      case 'snowflake':
        script += `CREATE OR REPLACE VIEW ${qualifiedViewName} AS\n`;
        script += `WITH MAPPING_EXPANDED AS (\n`;
        script += `  SELECT\n`;
        script += `    h.XREF_HIERARCHY_KEY,\n`;
        script += `    h.HIERARCHY_GROUP_NAME,\n`;
        script += `    h.HIERARCHY_NAME,\n`;
        script += `    h.LEVEL_1,\n`;
        script += `    h.LEVEL_2,\n`;
        script += `    h.LEVEL_3,\n`;
        script += `    h.LEVEL_4,\n`;
        script += `    h.LEVEL_5,\n`;
        script += `    h.PARENT_XREF_KEY,\n`;
        script += `    h.IS_ROOT,\n`;
        script += `    h.SORT_ORDER,\n`;
        script += `    \n`;
        script += `    -- Expand mapping array using LATERAL FLATTEN\n`;
        script += `    ROW_NUMBER() OVER (\n`;
        script += `      PARTITION BY h.XREF_HIERARCHY_KEY \n`;
        script += `      ORDER BY mapping.VALUE:mapping_index::NUMBER\n`;
        script += `    ) AS MAPPING_SEQUENCE,\n`;
        script += `    mapping.VALUE:mapping_index::NUMBER AS MAPPING_INDEX,\n`;
        script += `    mapping.VALUE:source_database::VARCHAR AS SOURCE_DATABASE,\n`;
        script += `    mapping.VALUE:source_schema::VARCHAR AS SOURCE_SCHEMA,\n`;
        script += `    mapping.VALUE:source_table::VARCHAR AS SOURCE_TABLE,\n`;
        script += `    mapping.VALUE:source_column::VARCHAR AS SOURCE_COLUMN,\n`;
        script += `    mapping.VALUE:source_uid::VARCHAR AS SOURCE_UID,\n`;
        script += `    mapping.VALUE:precedence_group::VARCHAR AS PRECEDENCE_GROUP,\n`;
        script += `    \n`;
        script += `    -- Static Mapping Flags (from mapping.flags)\n`;
        script += `    mapping.VALUE:flags.active_flag::BOOLEAN AS ACTIVE_FLAG,\n`;
        script += `    mapping.VALUE:flags.include_flag::BOOLEAN AS INCLUDE_FLAG,\n`;
        script += `    mapping.VALUE:flags.exclude_flag::BOOLEAN AS EXCLUDE_FLAG,\n`;
        script += `    mapping.VALUE:flags.transform_flag::BOOLEAN AS TRANSFORM_FLAG,\n`;
        script += `    \n`;
        script += `    -- Raw custom flags JSON (contains all dynamic custom flags)\n`;
        script += `    mapping.VALUE:flags.customFlags::VARIANT AS CUSTOM_FLAGS,\n`;
        script += `    \n`;
        script += `    -- Build full qualified table name based on connection config\n`;
        script += `    CASE \n`;
        script += `      WHEN mapping.VALUE:source_database::VARCHAR IS NOT NULL \n`;
        script += `        AND mapping.VALUE:source_schema::VARCHAR IS NOT NULL \n`;
        script += `      THEN mapping.VALUE:source_database::VARCHAR || '.' || \n`;
        script += `           mapping.VALUE:source_schema::VARCHAR || '.' || \n`;
        script += `           mapping.VALUE:source_table::VARCHAR\n`;
        script += `      ELSE mapping.VALUE:source_table::VARCHAR\n`;
        script += `    END AS FULL_TABLE_NAME,\n`;
        script += `    \n`;
        script += `    -- Hierarchy configuration for reference\n`;
        script += `    h.ACTIVE_FLAG AS HIERARCHY_ACTIVE,\n`;
        script += `    h.INCLUDE_FLAG AS HIERARCHY_INCLUDE,\n`;
        script += `    h.EXCLUSION_FLAG AS HIERARCHY_EXCLUDE,\n`;
        script += `    h.CALCULATION_FLAG AS HIERARCHY_CALCULATION,\n`;
        script += `    h.TRANSFORM_FLAG AS HIERARCHY_TRANSFORM,\n`;
        script += `    h.IS_LEAF_NODE AS HIERARCHY_IS_LEAF,\n`;
        script += `    h.UPDATED_BY,\n`;
        script += `    h.UPDATED_AT\n`;
        script += `  FROM ${qualifiedHierarchyViewName} h\n`;
        script += `  , LATERAL FLATTEN(input => h.MAPPING_JSON, OUTER => TRUE) mapping\n`;
        script += `  WHERE h.MAPPING_JSON IS NOT NULL\n`;
        script += `    AND ARRAY_SIZE(h.MAPPING_JSON) > 0\n`;
        script += `)\n`;
        script += `SELECT \n`;
        script += `  XREF_HIERARCHY_KEY,\n`;
        script += `  HIERARCHY_GROUP_NAME,\n`;
        script += `  HIERARCHY_NAME,\n`;
        script += `  LEVEL_1,\n`;
        script += `  LEVEL_2,\n`;
        script += `  LEVEL_3,\n`;
        script += `  LEVEL_4,\n`;
        script += `  LEVEL_5,\n`;
        script += `  PARENT_XREF_KEY,\n`;
        script += `  IS_ROOT,\n`;
        script += `  SORT_ORDER,\n`;
        script += `  MAPPING_SEQUENCE,\n`;
        script += `  MAPPING_INDEX,\n`;
        script += `  SOURCE_DATABASE,\n`;
        script += `  SOURCE_SCHEMA,\n`;
        script += `  SOURCE_TABLE,\n`;
        script += `  SOURCE_COLUMN,\n`;
        script += `  SOURCE_UID,\n`;
        script += `  PRECEDENCE_GROUP,\n`;
        script += `  ACTIVE_FLAG,\n`;
        script += `  INCLUDE_FLAG,\n`;
        script += `  EXCLUDE_FLAG,\n`;
        script += `  TRANSFORM_FLAG,\n`;
        script += `  CUSTOM_FLAGS,\n`;
        script += `  FULL_TABLE_NAME,\n`;
        script += `  HIERARCHY_ACTIVE,\n`;
        script += `  HIERARCHY_INCLUDE,\n`;
        script += `  HIERARCHY_EXCLUDE,\n`;
        script += `  HIERARCHY_CALCULATION,\n`;
        script += `  HIERARCHY_TRANSFORM,\n`;
        script += `  HIERARCHY_IS_LEAF,\n`;
        script += `  UPDATED_BY,\n`;
        script += `  UPDATED_AT\n`;
        script += `FROM MAPPING_EXPANDED\n`;
        script += `WHERE ACTIVE_FLAG = TRUE  -- Only include active mappings\n`;
        script += `  AND HIERARCHY_ACTIVE = TRUE  -- Only include active hierarchies\n`;
        script += `ORDER BY XREF_HIERARCHY_KEY, MAPPING_SEQUENCE;\n\n`;

        // Add usage examples
        script += `-- ========================================\n`;
        script += `-- USAGE EXAMPLES\n`;
        script += `-- ========================================\n`;
        script += `-- 1. Get all mappings for a specific hierarchy:\n`;
        script += `-- SELECT * FROM ${qualifiedViewName} WHERE XREF_HIERARCHY_KEY = 'your_hierarchy_key';\n\n`;
        script += `-- 2. Get all mappings for a specific source table:\n`;
        script += `-- SELECT * FROM ${qualifiedViewName} WHERE SOURCE_TABLE = 'DIM_ACCOUNT';\n\n`;
        script += `-- 3. Count mappings by source database:\n`;
        script += `-- SELECT SOURCE_DATABASE, COUNT(*) as mapping_count \n`;
        script += `-- FROM ${qualifiedViewName} GROUP BY SOURCE_DATABASE;\n\n`;
        script += `-- 4. Find hierarchies using a specific column:\n`;
        script += `-- SELECT DISTINCT HIERARCHY_NAME, XREF_HIERARCHY_KEY \n`;
        script += `-- FROM ${qualifiedViewName} WHERE SOURCE_COLUMN = 'ACCOUNT_CODE';\n\n`;
        script += `-- 5. Get mapping precedence groups:\n`;
        script += `-- SELECT HIERARCHY_NAME, PRECEDENCE_GROUP, COUNT(*) as mapping_count\n`;
        script += `-- FROM ${qualifiedViewName} \n`;
        script += `-- GROUP BY HIERARCHY_NAME, PRECEDENCE_GROUP\n`;
        script += `-- ORDER BY HIERARCHY_NAME, PRECEDENCE_GROUP;\n\n`;
        script += `-- 6. Query custom flags dynamically (works with any custom flag):\n`;
        script += `-- SELECT HIERARCHY_NAME, SOURCE_TABLE, \n`;
        script += `--   CUSTOM_FLAGS:new_test_flag::BOOLEAN AS NEW_TEST_FLAG,\n`;
        script += `--   CUSTOM_FLAGS:any_other_flag::BOOLEAN AS ANY_OTHER_FLAG\n`;
        script += `-- FROM ${qualifiedViewName};\n\n`;
        script += `-- 7. Find mappings where a specific custom flag is true:\n`;
        script += `-- SELECT * FROM ${qualifiedViewName} \n`;
        script += `-- WHERE CUSTOM_FLAGS:new_test_flag::BOOLEAN = TRUE;\n\n`;
        script += `-- 8. List all custom flag keys in the data:\n`;
        script += `-- SELECT DISTINCT f.key AS custom_flag_name\n`;
        script += `-- FROM ${qualifiedViewName}, LATERAL FLATTEN(input => CUSTOM_FLAGS) f;\n`;
        break;

      case 'postgres':
        const pgViewName = viewName.toLowerCase();
        const pgHierViewName = hierarchyViewName.toLowerCase();
        script += `CREATE OR REPLACE VIEW ${pgViewName} AS\n`;
        script += `WITH mapping_expanded AS (\n`;
        script += `  SELECT\n`;
        script += `    h.project_id,\n`;
        script += `    h.hierarchy_id,\n`;
        script += `    h.hierarchy_name,\n`;
        script += `    h.node_name,\n`;
        script += `    h.hierarchy_level,\n`;
        script += `    h.hierarchy_path,\n`;
        script += `    h.parent_id,\n`;
        script += `    h.is_root,\n`;
        script += `    h.sort_order,\n`;
        script += `    ROW_NUMBER() OVER (\n`;
        script += `      PARTITION BY h.hierarchy_id \n`;
        script += `      ORDER BY (mapping->>'mapping_index')::INTEGER\n`;
        script += `    ) AS mapping_sequence,\n`;
        script += `    (mapping->>'mapping_index')::INTEGER AS mapping_index,\n`;
        script += `    mapping->>'source_database' AS source_database,\n`;
        script += `    mapping->>'source_schema' AS source_schema,\n`;
        script += `    mapping->>'source_table' AS source_table,\n`;
        script += `    mapping->>'source_column' AS source_column,\n`;
        script += `    mapping->>'source_uid' AS source_uid,\n`;
        script += `    mapping->>'precedence_group' AS precedence_group,\n`;
        script += `    (mapping->'flags'->>'active_flag')::BOOLEAN AS active_flag,\n`;
        script += `    (mapping->'flags'->>'include_flag')::BOOLEAN AS include_flag,\n`;
        script += `    (mapping->'flags'->>'exclude_flag')::BOOLEAN AS exclude_flag,\n`;
        script += `    (mapping->'flags'->>'transform_flag')::BOOLEAN AS transform_flag,\n`;
        script += `    mapping->'flags'->'customFlags' AS custom_flags,\n`;
        script += `    mapping->>'source_database' || '.' || \n`;
        script += `    mapping->>'source_schema' || '.' || \n`;
        script += `    mapping->>'source_table' AS full_table_name,\n`;
        script += `    h.active_flag AS hierarchy_active,\n`;
        script += `    h.include_flag AS hierarchy_include,\n`;
        script += `    h.exclusion_flag AS hierarchy_exclude,\n`;
        script += `    h.calculation_flag AS hierarchy_calculation,\n`;
        script += `    h.transform_flag AS hierarchy_transform,\n`;
        script += `    h.is_leaf_node AS hierarchy_is_leaf,\n`;
        script += `    h.formula_type,\n`;
        script += `    h.formula_text,\n`;
        script += `    h.formula_group,\n`;
        script += `    h.arithmetic_logic,\n`;
        script += `    h.formula_param_ref,\n`;
        script += `    h.filter_group_1,\n`;
        script += `    h.filter_group_2,\n`;
        script += `    h.filter_group_3,\n`;
        script += `    h.filter_group_4\n`;
        script += `  FROM ${pgHierViewName} h\n`;
        script += `  CROSS JOIN LATERAL jsonb_array_elements(h.mapping_json) AS mapping\n`;
        script += `  WHERE h.mapping_json IS NOT NULL\n`;
        script += `    AND jsonb_array_length(h.mapping_json) > 0\n`;
        script += `)\n`;
        script += `SELECT * FROM mapping_expanded\n`;
        script += `WHERE active_flag = TRUE AND hierarchy_active = TRUE\n`;
        script += `ORDER BY hierarchy_path, mapping_sequence;\n`;
        break;

      case 'mysql':
        const mysqlViewName = viewName.toLowerCase();
        const mysqlHierViewName = hierarchyViewName.toLowerCase();
        script += `CREATE OR REPLACE VIEW ${mysqlViewName} AS\n`;
        script += `SELECT\n`;
        script += `  h.project_id,\n`;
        script += `  h.hierarchy_id,\n`;
        script += `  h.hierarchy_name,\n`;
        script += `  h.node_name,\n`;
        script += `  h.hierarchy_level,\n`;
        script += `  h.hierarchy_path,\n`;
        script += `  idx.seq AS mapping_sequence,\n`;
        script += `  JSON_UNQUOTE(JSON_EXTRACT(h.mapping_json, CONCAT('$[', idx.seq, '].mapping_index'))) AS mapping_index,\n`;
        script += `  JSON_UNQUOTE(JSON_EXTRACT(h.mapping_json, CONCAT('$[', idx.seq, '].source_database'))) AS source_database,\n`;
        script += `  JSON_UNQUOTE(JSON_EXTRACT(h.mapping_json, CONCAT('$[', idx.seq, '].source_schema'))) AS source_schema,\n`;
        script += `  JSON_UNQUOTE(JSON_EXTRACT(h.mapping_json, CONCAT('$[', idx.seq, '].source_table'))) AS source_table,\n`;
        script += `  JSON_UNQUOTE(JSON_EXTRACT(h.mapping_json, CONCAT('$[', idx.seq, '].source_column'))) AS source_column,\n`;
        script += `  JSON_UNQUOTE(JSON_EXTRACT(h.mapping_json, CONCAT('$[', idx.seq, '].source_uid'))) AS source_uid,\n`;
        script += `  JSON_UNQUOTE(JSON_EXTRACT(h.mapping_json, CONCAT('$[', idx.seq, '].precedence_group'))) AS precedence_group,\n`;
        script += `  JSON_UNQUOTE(JSON_EXTRACT(h.mapping_json, CONCAT('$[', idx.seq, '].flags.active_flag'))) = 'true' AS active_flag,\n`;
        script += `  JSON_UNQUOTE(JSON_EXTRACT(h.mapping_json, CONCAT('$[', idx.seq, '].flags.include_flag'))) = 'true' AS include_flag,\n`;
        script += `  JSON_UNQUOTE(JSON_EXTRACT(h.mapping_json, CONCAT('$[', idx.seq, '].flags.exclude_flag'))) = 'true' AS exclude_flag,\n`;
        script += `  JSON_UNQUOTE(JSON_EXTRACT(h.mapping_json, CONCAT('$[', idx.seq, '].flags.transform_flag'))) = 'true' AS transform_flag,\n`;
        script += `  h.active_flag AS hierarchy_active,\n`;
        script += `  h.include_flag AS hierarchy_include,\n`;
        script += `  h.exclusion_flag AS hierarchy_exclude,\n`;
        script += `  h.calculation_flag AS hierarchy_calculation,\n`;
        script += `  h.transform_flag AS hierarchy_transform,\n`;
        script += `  h.is_leaf_node AS hierarchy_is_leaf,\n`;
        script += `  h.formula_type,\n`;
        script += `  h.formula_text,\n`;
        script += `  h.formula_group,\n`;
        script += `  h.arithmetic_logic,\n`;
        script += `  h.filter_group_1,\n`;
        script += `  h.filter_group_2\n`;
        script += `FROM ${mysqlHierViewName} h\n`;
        script += `CROSS JOIN (\n`;
        script += `  SELECT 0 AS seq UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL\n`;
        script += `  SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL\n`;
        script += `  SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10 UNION ALL SELECT 11\n`;
        script += `) idx\n`;
        script += `WHERE JSON_EXTRACT(h.mapping_json, CONCAT('$[', idx.seq, ']')) IS NOT NULL\n`;
        script += `  AND JSON_UNQUOTE(JSON_EXTRACT(h.mapping_json, CONCAT('$[', idx.seq, '].flags.active_flag'))) = 'true'\n`;
        script += `ORDER BY h.hierarchy_path, idx.seq;\n`;
        break;

      case 'sqlserver':
        // SQL Server qualified names (database.schema.object format)
        const qualifiedSqlServerViewName2 =
          database && schema ? `${database}.${schema}.${viewName}` : viewName;
        const qualifiedSqlServerHierarchyViewName =
          database && schema ? `${database}.${schema}.${hierarchyViewName}` : hierarchyViewName;

        script += `CREATE OR ALTER VIEW ${qualifiedSqlServerViewName2} AS\n`;
        script += `SELECT\n`;
        script += `  h.PROJECT_ID,\n`;
        script += `  h.HIERARCHY_ID,\n`;
        script += `  h.HIERARCHY_NAME,\n`;
        script += `  h.NODE_NAME,\n`;
        script += `  h.HIERARCHY_LEVEL,\n`;
        script += `  h.HIERARCHY_PATH,\n`;
        script += `  ROW_NUMBER() OVER (\n`;
        script += `    PARTITION BY h.HIERARCHY_ID \n`;
        script += `    ORDER BY JSON_VALUE(mapping.value, '$.mapping_index')\n`;
        script += `  ) AS MAPPING_SEQUENCE,\n`;
        script += `  CAST(JSON_VALUE(mapping.value, '$.mapping_index') AS INT) AS MAPPING_INDEX,\n`;
        script += `  JSON_VALUE(mapping.value, '$.source_database') AS SOURCE_DATABASE,\n`;
        script += `  JSON_VALUE(mapping.value, '$.source_schema') AS SOURCE_SCHEMA,\n`;
        script += `  JSON_VALUE(mapping.value, '$.source_table') AS SOURCE_TABLE,\n`;
        script += `  JSON_VALUE(mapping.value, '$.source_column') AS SOURCE_COLUMN,\n`;
        script += `  JSON_VALUE(mapping.value, '$.source_uid') AS SOURCE_UID,\n`;
        script += `  JSON_VALUE(mapping.value, '$.precedence_group') AS PRECEDENCE_GROUP,\n`;
        script += `  CAST(JSON_VALUE(mapping.value, '$.flags.active_flag') AS BIT) AS ACTIVE_FLAG,\n`;
        script += `  CAST(JSON_VALUE(mapping.value, '$.flags.include_flag') AS BIT) AS INCLUDE_FLAG,\n`;
        script += `  CAST(JSON_VALUE(mapping.value, '$.flags.exclude_flag') AS BIT) AS EXCLUDE_FLAG,\n`;
        script += `  CAST(JSON_VALUE(mapping.value, '$.flags.transform_flag') AS BIT) AS TRANSFORM_FLAG,\n`;
        script += `  h.ACTIVE_FLAG AS HIERARCHY_ACTIVE,\n`;
        script += `  h.INCLUDE_FLAG AS HIERARCHY_INCLUDE,\n`;
        script += `  h.EXCLUSION_FLAG AS HIERARCHY_EXCLUDE,\n`;
        script += `  h.CALCULATION_FLAG AS HIERARCHY_CALCULATION,\n`;
        script += `  h.TRANSFORM_FLAG AS HIERARCHY_TRANSFORM,\n`;
        script += `  h.IS_LEAF_NODE AS HIERARCHY_IS_LEAF,\n`;
        script += `  h.FORMULA_TYPE,\n`;
        script += `  h.FORMULA_TEXT,\n`;
        script += `  h.FORMULA_GROUP,\n`;
        script += `  h.ARITHMETIC_LOGIC,\n`;
        script += `  h.FORMULA_PARAM_REF,\n`;
        script += `  h.FILTER_GROUP_1,\n`;
        script += `  h.FILTER_GROUP_2,\n`;
        script += `  h.FILTER_GROUP_3,\n`;
        script += `  h.FILTER_GROUP_4\n`;
        script += `FROM ${qualifiedSqlServerHierarchyViewName} h\n`;
        script += `CROSS APPLY OPENJSON(h.MAPPING_JSON) AS mapping\n`;
        script += `WHERE CAST(JSON_VALUE(mapping.value, '$.flags.active_flag') AS BIT) = 1\n`;
        script += `  AND h.ACTIVE_FLAG = 1;\n`;
        script += `GO\n`;
        break;
    }

    return script;
  }

  // ============================================================================
  // UNIFIED DYNAMIC TABLE - Single dynamic table for entire project
  // ============================================================================

  private generateUnifiedDynamicTableScript(
    hierarchies: SmartHierarchyMaster[],
    projectName: string,
    dbType: 'snowflake' | 'postgres' | 'mysql' | 'sqlserver' = 'snowflake',
    database?: string, // Target database for qualified names
    schema?: string, // Target schema for qualified names
  ): string {
    const sanitizedProjectName = projectName.toUpperCase().replace(/[^A-Z0-9_]/g, '_');
    const dtName = `DT_${sanitizedProjectName}_HIERARCHY_EXPANSION`;
    const viewName = `VW_${sanitizedProjectName}_HIERARCHY_MASTER`;

    // Build qualified view name for FROM clause
    const qualifiedViewName = database && schema ? `${database}.${schema}.${viewName}` : viewName;

    let script = `-- UNIFIED DYNAMIC TABLE for entire project: ${projectName}\n`;
    script += `-- Database Type: ${dbType.toUpperCase()}\n`;
    script += `-- Source View: ${viewName}\n`;
    script += `-- This dynamic table automatically expands ALL hierarchies with their source mappings\n`;
    script += `-- Total Hierarchies: ${hierarchies.length}\n`;
    script += `-- Approach: Uses dynamic CASE-based join logic to handle any mapping configuration\n`;
    script += `-- Generated at: ${new Date().toISOString()}\n\n`;

    if (dbType === 'snowflake') {
      script += `CREATE OR REPLACE DYNAMIC TABLE ${dtName}(\n`;
      script += `  XREF_HIERARCHY_KEY,\n`;
      script += `  PROJECT_ID,\n`;
      script += `  HIERARCHY_ID,\n`;
      script += `  HIERARCHY_NAME,\n`;
      script += `  NODE_NAME,\n`;
      script += `  HIERARCHY_LEVEL,\n`;
      script += `  LEVEL_1,\n`;
      script += `  LEVEL_2,\n`;
      script += `  LEVEL_3,\n`;
      script += `  LEVEL_4,\n`;
      script += `  LEVEL_5,\n`;
      script += `  LEVEL_6,\n`;
      script += `  LEVEL_7,\n`;
      script += `  LEVEL_8,\n`;
      script += `  LEVEL_9,\n`;
      script += `  HIERARCHY_PATH,\n`;
      script += `  PARENT_ID,\n`;
      script += `  IS_ROOT,\n`;
      script += `  VOLUME_FLAG,\n`;
      script += `  SOURCE_UID,\n`;
      script += `  LEVEL_1_SORT,\n`;
      script += `  LEVEL_2_SORT,\n`;
      script += `  LEVEL_3_SORT,\n`;
      script += `  LEVEL_4_SORT,\n`;
      script += `  LEVEL_5_SORT,\n`;
      script += `  LEVEL_6_SORT,\n`;
      script += `  LEVEL_7_SORT,\n`;
      script += `  LEVEL_8_SORT,\n`;
      script += `  LEVEL_9_SORT,\n`;
      script += `  ACTIVE_FLAG,\n`;
      script += `  INCLUDE_FLAG,\n`;
      script += `  EXCLUDE_FLAG,\n`;
      script += `  TRANSFORM_FLAG,\n`;
      script += `  IS_LEAF_NODE,\n`;
      script += `  MAPPING_INDEX,\n`;
      script += `  SOURCE_DATABASE,\n`;
      script += `  SOURCE_SCHEMA,\n`;
      script += `  SOURCE_TABLE,\n`;
      script += `  SOURCE_COLUMN,\n`;
      script += `  SOURCE_VALUE,\n`;
      script += `  FORMULA_TYPE,\n`;
      script += `  FORMULA_TEXT,\n`;
      script += `  SIGN_CHANGE_FLAG\n`;
      script += `) TARGET_LAG = '6 hours' REFRESH_MODE = AUTO INITIALIZE = ON_CREATE WAREHOUSE = <YOUR_WAREHOUSE>\n`;
      script += `AS\n`;
      script += `/********************************************************************************\n`;
      script += `  Generated Dynamic Table for Project: ${projectName}\n`;
      script += `  Total Hierarchies: ${hierarchies.length}\n`;
      script += `  \n`;
      script += `  This table automatically expands ALL hierarchies with their source mappings.\n`;
      script += `  It uses the MAPPING JSON from the view to dynamically join to source tables.\n`;
      script += `  \n`;
      script += `  Key Features:\n`;
      script += `  - Automatically handles thousands of hierarchies and mappings\n`;
      script += `  - Uses LATERAL FLATTEN to expand mapping arrays\n`;
      script += `  - Dynamic CASE-based joins using source_database, source_schema, source_table\n`;
      script += `  - Preserves all hierarchy levels and sort orders\n`;
      script += `  - Fills missing levels with source values as fallback\n`;
      script += `  \n`;
      script += `  Generated at: ${new Date().toISOString()}\n`;
      script += `********************************************************************************/\n`;
    } else {
      script += `-- Note: ${dbType.toUpperCase()} does not support Dynamic Tables.\n`;
      script += `-- Using Materialized View instead:\n\n`;
      switch (dbType) {
        case 'postgres':
          script += `CREATE MATERIALIZED VIEW ${dtName.toLowerCase()} AS\n`;
          break;
        case 'mysql':
          script += `CREATE OR REPLACE VIEW ${dtName.toLowerCase()} AS\n`;
          break;
        case 'sqlserver':
          script += `CREATE VIEW ${dtName} WITH SCHEMABINDING AS\n`;
          break;
      }
    }

    // Get project ID from first hierarchy
    const projectId = hierarchies.length > 0 ? hierarchies[0].projectId : 'PROJECT_ID';

    // Outer SELECT with transformations
    script += `SELECT \n`;
    script += `  ROW_NUMBER() OVER (\n`;
    script += `    ORDER BY LEVEL_1, LEVEL_2, LEVEL_3, LEVEL_4, LEVEL_5, LEVEL_6, \n`;
    script += `             VOLUME_FLAG, MAPPING_INDEX, SOURCE_UID\n`;
    script += `  ) AS XREF_HIERARCHY_KEY,\n`;
    script += `  PROJECT_ID,\n`;
    script += `  HIERARCHY_ID,\n`;
    script += `  HIERARCHY_NAME,\n`;
    script += `  NODE_NAME,\n`;
    script += `  HIERARCHY_LEVEL,\n`;
    script += `  LEVEL_1,\n`;
    script += `  LEVEL_2,\n`;
    script += `  -- Fill missing levels with source value as fallback\n`;
    script += `  IFNULL(LEVEL_3, IFF(LEVEL_2 IS NULL, SOURCE_VALUE, SOURCE_VALUE)) AS LEVEL_3,\n`;
    script += `  IFNULL(LEVEL_4, IFF(LEVEL_3 IS NULL, SOURCE_VALUE, SOURCE_VALUE)) AS LEVEL_4,\n`;
    script += `  IFNULL(LEVEL_5, IFF(LEVEL_4 IS NULL, SOURCE_VALUE, SOURCE_VALUE)) AS LEVEL_5,\n`;
    script += `  IFNULL(LEVEL_6, IFF(LEVEL_5 IS NULL, SOURCE_VALUE, SOURCE_VALUE)) AS LEVEL_6,\n`;
    script += `  IFNULL(LEVEL_7, IFF(LEVEL_6 IS NULL, SOURCE_VALUE, SOURCE_VALUE)) AS LEVEL_7,\n`;
    script += `  IFNULL(LEVEL_8, IFF(LEVEL_7 IS NULL, SOURCE_VALUE, SOURCE_VALUE)) AS LEVEL_8,\n`;
    script += `  IFNULL(LEVEL_9, IFF(LEVEL_8 IS NULL, SOURCE_VALUE, SOURCE_VALUE)) AS LEVEL_9,\n`;
    script += `  HIERARCHY_PATH,\n`;
    script += `  PARENT_ID,\n`;
    script += `  IS_ROOT,\n`;
    script += `  VOLUME_FLAG,\n`;
    script += `  SOURCE_UID,\n`;
    script += `  -- Generate cumulative sort orders\n`;
    script += `  DENSE_RANK() OVER (ORDER BY LEVEL_1_SORT) AS LEVEL_1_SORT,\n`;
    script += `  DENSE_RANK() OVER (ORDER BY LEVEL_2_SORT) AS LEVEL_2_SORT,\n`;
    script += `  DENSE_RANK() OVER (ORDER BY LEVEL_3_SORT) AS LEVEL_3_SORT,\n`;
    script += `  DENSE_RANK() OVER (ORDER BY LEVEL_4_SORT) AS LEVEL_4_SORT,\n`;
    script += `  DENSE_RANK() OVER (ORDER BY LEVEL_1_SORT, LEVEL_2_SORT, LEVEL_3_SORT, LEVEL_4_SORT, LEVEL_5_SORT) AS LEVEL_5_SORT,\n`;
    script += `  DENSE_RANK() OVER (ORDER BY LEVEL_1_SORT, LEVEL_2_SORT, LEVEL_3_SORT, LEVEL_4_SORT, LEVEL_5_SORT, LEVEL_6_SORT) AS LEVEL_6_SORT,\n`;
    script += `  DENSE_RANK() OVER (ORDER BY LEVEL_1_SORT, LEVEL_2_SORT, LEVEL_3_SORT, LEVEL_4_SORT, LEVEL_5_SORT, LEVEL_6_SORT, LEVEL_7_SORT) AS LEVEL_7_SORT,\n`;
    script += `  DENSE_RANK() OVER (ORDER BY LEVEL_1_SORT, LEVEL_2_SORT, LEVEL_3_SORT, LEVEL_4_SORT, LEVEL_5_SORT, LEVEL_6_SORT, LEVEL_7_SORT, LEVEL_8_SORT) AS LEVEL_8_SORT,\n`;
    script += `  DENSE_RANK() OVER (ORDER BY LEVEL_1_SORT, LEVEL_2_SORT, LEVEL_3_SORT, LEVEL_4_SORT, LEVEL_5_SORT, LEVEL_6_SORT, LEVEL_7_SORT, LEVEL_8_SORT, LEVEL_9_SORT) AS LEVEL_9_SORT,\n`;
    script += `  ACTIVE_FLAG,\n`;
    script += `  INCLUDE_FLAG,\n`;
    script += `  EXCLUDE_FLAG,\n`;
    script += `  TRANSFORM_FLAG,\n`;
    script += `  IS_LEAF_NODE,\n`;
    script += `  MAPPING_INDEX,\n`;
    script += `  SOURCE_DATABASE,\n`;
    script += `  SOURCE_SCHEMA,\n`;
    script += `  SOURCE_TABLE,\n`;
    script += `  SOURCE_COLUMN,\n`;
    script += `  SOURCE_VALUE,\n`;
    script += `  FORMULA_TYPE,\n`;
    script += `  FORMULA_TEXT,\n`;
    script += `  SIGN_CHANGE_FLAG\n`;
    script += `FROM (\n`;
    script += `  SELECT DISTINCT\n`;
    script += `    h.PROJECT_ID,\n`;
    script += `    h.HIERARCHY_ID,\n`;
    script += `    h.HIERARCHY_NAME,\n`;
    script += `    h.NODE_NAME,\n`;
    script += `    h.HIERARCHY_LEVEL,\n`;
    script += `    h.LEVEL_1_NAME AS LEVEL_1,\n`;
    script += `    h.LEVEL_2_NAME AS LEVEL_2,\n`;
    script += `    h.LEVEL_3_NAME AS LEVEL_3,\n`;
    script += `    h.LEVEL_4_NAME AS LEVEL_4,\n`;
    script += `    h.LEVEL_5_NAME AS LEVEL_5,\n`;
    script += `    h.LEVEL_6_NAME AS LEVEL_6,\n`;
    script += `    h.LEVEL_7_NAME AS LEVEL_7,\n`;
    script += `    h.LEVEL_8_NAME AS LEVEL_8,\n`;
    script += `    h.LEVEL_9_NAME AS LEVEL_9,\n`;
    script += `    h.HIERARCHY_PATH,\n`;
    script += `    h.PARENT_ID,\n`;
    script += `    h.IS_ROOT,\n`;
    script += `    h.VOLUME_FLAG,\n`;
    script += `    h.LEVEL_1_SORT,\n`;
    script += `    h.LEVEL_2_SORT,\n`;
    script += `    h.LEVEL_3_SORT,\n`;
    script += `    h.LEVEL_4_SORT,\n`;
    script += `    h.LEVEL_5_SORT,\n`;
    script += `    h.LEVEL_6_SORT,\n`;
    script += `    h.LEVEL_7_SORT,\n`;
    script += `    h.LEVEL_8_SORT,\n`;
    script += `    h.LEVEL_9_SORT,\n`;
    script += `    h.ACTIVE_FLAG,\n`;
    script += `    h.INCLUDE_FLAG,\n`;
    script += `    h.EXCLUDE_FLAG,\n`;
    script += `    h.TRANSFORM_FLAG,\n`;
    script += `    h.IS_LEAF_NODE,\n`;
    script += `    -- Expand mapping array using LATERAL FLATTEN\n`;
    script += `    mapping.VALUE:mapping_index::NUMBER AS MAPPING_INDEX,\n`;
    script += `    mapping.VALUE:source_database::VARCHAR AS SOURCE_DATABASE,\n`;
    script += `    mapping.VALUE:source_schema::VARCHAR AS SOURCE_SCHEMA,\n`;
    script += `    mapping.VALUE:source_table::VARCHAR AS SOURCE_TABLE,\n`;
    script += `    mapping.VALUE:source_column::VARCHAR AS SOURCE_COLUMN,\n`;
    script += `    mapping.VALUE:source_uid::VARCHAR AS SOURCE_UID_COLUMN,\n`;
    script += `    mapping.VALUE:flags.active_flag::BOOLEAN AS MAPPING_ACTIVE,\n`;
    script += `    -- Source table data (using dynamic SQL approach - see note below)\n`;
    script += `    NULL AS SOURCE_UID,  -- Will be populated by join\n`;
    script += `    NULL AS SOURCE_VALUE,  -- Will be populated by join\n`;
    script += `    h.FORMULA_TYPE,\n`;
    script += `    h.FORMULA_TEXT,\n`;
    script += `    h.SIGN_CHANGE_FLAG\n`;
    script += `  FROM ${qualifiedViewName} h\n`;
    script += `  , LATERAL FLATTEN(input => h.MAPPING_JSON) mapping\n`;
    script += `  WHERE h.PROJECT_ID = '${projectId}'\n`;
    script += `    AND h.ACTIVE_FLAG = TRUE\n`;
    script += `    AND h.INCLUDE_FLAG = TRUE\n`;
    script += `    AND h.EXCLUDE_FLAG = FALSE\n`;
    script += `    AND mapping.VALUE:flags.active_flag::BOOLEAN = TRUE\n`;
    script += `) AS HIERARCHY_EXPANSION\n`;
    script += `;\n\n`;

    script += `/****************************************************************************\n`;
    script += ` * IMPORTANT NOTE: Dynamic Source Table Joins\n`;
    script += ` * \n`;
    script += ` * The above query uses LATERAL FLATTEN to expand all mappings dynamically.\n`;
    script += ` * However, Snowflake does not support truly dynamic table names in joins.\n`;
    script += ` * \n`;
    script += ` * To complete the expansion with actual source data, you have TWO options:\n`;
    script += ` * \n`;
    script += ` * OPTION 1: Use Snowflake's IDENTIFIER() function (requires stored procedure)\n`;
    script += ` * - Create a stored procedure that generates the full SQL dynamically\n`;
    script += ` * - Loop through each unique source table combination\n`;
    script += ` * - Execute dynamic SQL with IDENTIFIER() for table names\n`;
    script += ` * \n`;
    script += ` * OPTION 2: Generate explicit UNION ALL for each source table (recommended)\n`;
    script += ` * - Use the script generator to create explicit joins for each mapping\n`;
    script += ` * - This is more performant and easier to maintain\n`;
    script += ` * - See below for the generated approach:\n`;
    script += ` ****************************************************************************/\n\n`;

    // Generate explicit UNION ALL approach for actual implementation
    script += `-- RECOMMENDED IMPLEMENTATION: Explicit UNION ALL for each source table\n`;
    script += `-- This approach generates a UNION ALL for each unique source table referenced in mappings\n\n`;

    // Collect all unique source tables across all hierarchies
    const sourceTablesMap = new Map<string, any>();
    hierarchies.forEach((hierarchy) => {
      const activeMappings = hierarchy.mapping.filter((m) => m.flags.active_flag);
      activeMappings.forEach((mapping) => {
        const key = `${mapping.source_database}.${mapping.source_schema}.${mapping.source_table}`;
        if (!sourceTablesMap.has(key)) {
          sourceTablesMap.set(key, {
            database: mapping.source_database,
            schema: mapping.source_schema,
            table: mapping.source_table,
            mappings: [],
          });
        }
        sourceTablesMap.get(key).mappings.push({
          hierarchyId: hierarchy.hierarchyId,
          mappingIndex: mapping.mapping_index,
          sourceColumn: mapping.source_column,
          sourceUid: mapping.source_uid || 'id',
          hierarchyKeyColumn: mapping.hierarchy_key_column,
        });
      });
    });

    if (sourceTablesMap.size === 0) {
      script += `-- No active mappings found across all hierarchies\n`;
      script += `-- Please configure source mappings in your hierarchies\n`;
      return script;
    }

    script += `-- Found ${sourceTablesMap.size} unique source tables across all hierarchies\n\n`;
    script += `/*\n`;
    script += `CREATE OR REPLACE DYNAMIC TABLE ${dtName}_FULL(\n`;
    script += `  -- Same columns as above\n`;
    script += `) TARGET_LAG = '6 hours' REFRESH_MODE = AUTO INITIALIZE = ON_CREATE WAREHOUSE = <YOUR_WAREHOUSE>\n`;
    script += `AS\n`;
    script += `SELECT \n`;
    script += `  -- Apply same transformations as outer SELECT above\n`;
    script += `  ROW_NUMBER() OVER (ORDER BY LEVEL_1, LEVEL_2, LEVEL_3, ...) AS XREF_HIERARCHY_KEY,\n`;
    script += `  -- ... all columns with transformations ...\n`;
    script += `FROM (\n`;

    let tableIdx = 0;
    sourceTablesMap.forEach((tableInfo, tableKey) => {
      if (tableIdx > 0) {
        script += `\n  UNION ALL\n\n`;
      }

      script += `  -- Source Table: ${tableKey} (${tableInfo.mappings.length} mappings)\n`;
      script += `  SELECT DISTINCT\n`;
      script += `    h.PROJECT_ID,\n`;
      script += `    h.HIERARCHY_ID,\n`;
      script += `    -- ... all hierarchy columns ...\n`;
      script += `    mapping.VALUE:mapping_index::NUMBER AS MAPPING_INDEX,\n`;
      const firstMapping = tableInfo.mappings[0];
      if (firstMapping.sourceUid) {
        script += `    src.${firstMapping.sourceUid} AS SOURCE_UID,\n`;
      } else {
        script += `    NULL AS SOURCE_UID, -- Configure source_uid in mapping\n`;
      }
      if (firstMapping.sourceColumn) {
        script += `    src.${firstMapping.sourceColumn} AS SOURCE_VALUE\n`;
      } else {
        script += `    NULL AS SOURCE_VALUE -- Configure source_column in mapping\n`;
      }
      script += `  FROM ${qualifiedViewName} h\n`;
      script += `  , LATERAL FLATTEN(input => h.MAPPING_JSON) mapping\n`;
      script += `  INNER JOIN ${tableInfo.database}.${tableInfo.schema}.${tableInfo.table} src\n`;
      script += `    ON mapping.VALUE:source_database::VARCHAR = '${tableInfo.database}'\n`;
      script += `    AND mapping.VALUE:source_schema::VARCHAR = '${tableInfo.schema}'\n`;
      script += `    AND mapping.VALUE:source_table::VARCHAR = '${tableInfo.table}'\n`;
      if (firstMapping.hierarchyKeyColumn && firstMapping.sourceUid) {
        script += `    AND src.${firstMapping.sourceUid} = h.${firstMapping.hierarchyKeyColumn}\n`;
      } else if (firstMapping.sourceUid) {
        script += `    -- Note: hierarchy_key_column not configured, using default XREF_HIERARCHY_KEY\n`;
        script += `    AND src.${firstMapping.sourceUid} = h.XREF_HIERARCHY_KEY\n`;
      } else {
        script += `    -- WARNING: source_uid not configured - add join condition manually\n`;
        script += `    -- Example: AND src.<your_key_column> = h.XREF_HIERARCHY_KEY\n`;
      }
      script += `  WHERE h.PROJECT_ID = '${projectId}'\n`;
      script += `    AND h.ACTIVE_FLAG = TRUE\n`;
      script += `    AND mapping.VALUE:flags.active_flag::BOOLEAN = TRUE`;

      tableIdx++;
    });

    script += `\n) AS HIERARCHY_EXPANSION\n`;
    script += `;\n`;
    script += `*/\n\n`;

    script += `-- Note: The UNION ALL approach above is commented out as a template.\n`;
    script += `-- Uncomment and adjust the join conditions (ON clauses) based on your data model.\n`;
    script += `-- Each UNION ALL section handles one source table with all its mappings.\n`;

    return script;
  }

  // ============================================================================
  // Dynamic Table Script Generation (Project-based, built on individual views)
  // ============================================================================

  private generateDynamicTableScript(
    hierarchy: SmartHierarchyMaster,
    projectName: string,
    dbType: 'snowflake' | 'postgres' | 'mysql' | 'sqlserver' = 'snowflake',
  ): string {
    const levelColumns = this.getActiveLevelColumns(hierarchy.hierarchyLevel);
    const sanitizedProjectName = projectName.toUpperCase().replace(/[^A-Z0-9_]/g, '_');
    const dtName = `DT_${sanitizedProjectName}_${hierarchy.hierarchyId.toUpperCase()}_EXPANSION`;
    const individualViewName = `VW_${sanitizedProjectName}_${hierarchy.hierarchyId.toUpperCase()}`;

    let script = `-- Dynamic Table script for hierarchy: ${hierarchy.hierarchyName}\n`;
    script += `-- Project: ${projectName}\n`;
    script += `-- Hierarchy ID: ${hierarchy.hierarchyId}\n`;
    script += `-- Database Type: ${dbType.toUpperCase()}\n`;
    script += `-- Source View: ${individualViewName}\n`;
    script += `-- Generated at: ${new Date().toISOString()}\n\n`;

    // Get active mappings
    const mappings = hierarchy.mapping.filter((m) => m.flags.active_flag);

    if (mappings.length === 0) {
      script += `-- No active mappings found for this hierarchy\n`;
      script += `-- Dynamic table cannot be created without mappings\n`;
      return script;
    }

    // Only Snowflake supports Dynamic Tables natively
    if (dbType === 'snowflake') {
      script += `CREATE OR REPLACE DYNAMIC TABLE ${dtName}\n`;
      script += `TARGET_LAG = '1 minute'\n`;
      script += `WAREHOUSE = <YOUR_WAREHOUSE>\n`;
      script += `REFRESH_MODE = AUTO\n`;
      script += `AS\n`;
    } else {
      script += `-- Note: ${dbType.toUpperCase()} does not support Dynamic Tables.\n`;
      script += `-- Using Materialized View instead:\n\n`;

      switch (dbType) {
        case 'postgres':
          script += `CREATE MATERIALIZED VIEW ${dtName.toLowerCase()} AS\n`;
          break;
        case 'mysql':
          script += `-- MySQL does not support materialized views natively.\n`;
          script += `-- Creating regular view instead:\n`;
          script += `CREATE OR REPLACE VIEW ${dtName.toLowerCase()} AS\n`;
          break;
        case 'sqlserver':
          script += `CREATE VIEW ${dtName} WITH SCHEMABINDING AS\n`;
          break;
      }
    }

    // Generate UNION ALL for each active mapping
    mappings.forEach((mapping, idx) => {
      if (idx > 0) {
        script += `\nUNION ALL\n\n`;
      }

      script += `-- Mapping ${mapping.mapping_index}: ${mapping.source_database}.${mapping.source_schema}.${mapping.source_table}\n`;
      script += `SELECT\n`;

      // Add level columns from view (already flattened)
      levelColumns.forEach((level, levelIdx) => {
        const columnName =
          dbType === 'snowflake' || dbType === 'sqlserver'
            ? level.toUpperCase()
            : level.toLowerCase();
        script += `  v.${columnName}${levelIdx < levelColumns.length - 1 || mapping.source_column ? ',' : ''}\n`;
      });

      // Add source data columns
      if (mapping.source_column) {
        script += `  src.${mapping.source_column} AS ACCOUNT_ID,\n`;
        script += `  src.ACCOUNT_NAME,\n`;
        script += `  src.ACCOUNT_TYPE,\n`;
      }

      // Add metadata columns
      script += `  v.PROJECT_ID,\n`;
      script += `  v.HIERARCHY_ID,\n`;
      script += `  v.SOURCE_DATABASE,\n`;
      script += `  v.SOURCE_SCHEMA,\n`;
      script += `  v.SOURCE_TABLE,\n`;
      script += `  v.SOURCE_UID,\n`;
      script += `  v.MAPPING_INDEX\n`;

      // Build source table reference
      const sourceTable =
        dbType === 'mysql'
          ? `\`${mapping.source_database}\`.\`${mapping.source_schema}\`.\`${mapping.source_table}\``
          : `${mapping.source_database}.${mapping.source_schema}.${mapping.source_table}`;

      // FROM clause with individual view
      const viewNameInQuery =
        dbType === 'postgres' || dbType === 'mysql'
          ? individualViewName.toLowerCase()
          : individualViewName;

      script += `FROM ${viewNameInQuery} v\n`;

      // JOIN with source table based on mapping
      if (mapping.source_column) {
        const sourceUidColumn =
          dbType === 'postgres' || dbType === 'mysql' ? 'source_uid' : 'SOURCE_UID';
        script += `INNER JOIN ${sourceTable} src ON v.${sourceUidColumn} = src.${mapping.source_column}\n`;
      }

      // Apply filters if configured
      const whereConditions: string[] = [];

      // Filter by mapping index from view
      const mappingIndexColumn =
        dbType === 'postgres' || dbType === 'mysql' ? 'mapping_index' : 'MAPPING_INDEX';
      whereConditions.push(`v.${mappingIndexColumn} = ${mapping.mapping_index}`);

      // Add custom filter conditions
      if (hierarchy.filterConfig && hierarchy.filterConfig.filter_conditions.length > 0) {
        hierarchy.filterConfig.filter_conditions.forEach((cond) => {
          const escapedValue = this.escapeString(cond.value, dbType);
          whereConditions.push(`src.${cond.column} ${cond.operator} '${escapedValue}'`);
        });
      }

      if (whereConditions.length > 0) {
        script += `WHERE ${whereConditions.join(`\n  AND `)}\n`;
      }
    });

    // Add post-creation notes
    script += `;\n\n`;

    // Add post-creation notes
    script += `;\n\n`;

    // Database-specific refresh instructions
    script += `-- Refresh Instructions:\n`;
    if (dbType === 'snowflake') {
      script += `-- Dynamic table will auto-refresh based on TARGET_LAG setting\n`;
      script += `-- To manually refresh:\n`;
      script += `-- ALTER DYNAMIC TABLE ${dtName} REFRESH;\n\n`;
      script += `-- To monitor refresh status:\n`;
      script += `-- SELECT * FROM TABLE(INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY('${dtName}'));\n`;
    } else if (dbType === 'postgres') {
      script += `-- To refresh the materialized view:\n`;
      script += `-- REFRESH MATERIALIZED VIEW ${dtName.toLowerCase()};\n\n`;
      script += `-- To refresh concurrently (non-blocking):\n`;
      script += `-- REFRESH MATERIALIZED VIEW CONCURRENTLY ${dtName.toLowerCase()};\n`;
    } else if (dbType === 'mysql') {
      script += `-- MySQL views are automatically updated when queried\n`;
      script += `-- No manual refresh needed\n`;
    } else if (dbType === 'sqlserver') {
      script += `-- To create indexed view for auto-refresh:\n`;
      script += `-- CREATE UNIQUE CLUSTERED INDEX IDX_${dtName} ON ${dtName} (PROJECT_ID, HIERARCHY_ID, MAPPING_INDEX);\n\n`;
      script += `-- Indexed views are automatically maintained by SQL Server\n`;
    }

    return script;
  }

  // ============================================================================
  // Helper Methods
  // ============================================================================

  private async getHierarchy(projectId: string, hierarchyId: string): Promise<any> {
    const hierarchy = await this.prisma.smartHierarchyMaster.findUnique({
      where: {
        projectId_hierarchyId: {
          projectId,
          hierarchyId,
        },
      },
    });

    if (!hierarchy) {
      throw new NotFoundException(`Hierarchy '${hierarchyId}' not found`);
    }

    return hierarchy;
  }

  private async getAllHierarchies(projectId: string): Promise<any[]> {
    return await this.prisma.smartHierarchyMaster.findMany({
      where: { projectId },
    });
  }

  private getActiveLevelColumns(hierarchyLevel: any): string[] {
    const columns: string[] = [];
    for (let i = 1; i <= 15; i++) {
      const level = hierarchyLevel[`level_${i}`];
      if (level) {
        columns.push(level.toUpperCase().replace(/\s+/g, '_'));
      }
    }
    return columns;
  }

  private extractActiveLevels(hierarchyLevel: any): string[] {
    const levels: string[] = [];
    for (let i = 1; i <= 15; i++) {
      if (hierarchyLevel && hierarchyLevel[`level_${i}`]) {
        levels.push(hierarchyLevel[`level_${i}`]);
      }
    }
    return levels;
  }

  // Database-specific escape methods
  private escapeString(value: string, dbType: string): string {
    if (!value) return '';
    return value.replace(/'/g, "''");
  }

  private escapeJson(obj: any): string {
    try {
      const jsonStr = JSON.stringify(obj);
      // For Snowflake PARSE_JSON, we only need to escape single quotes
      // JSON.stringify already handles internal escaping (backslashes, quotes, etc.)
      // Double-escaping backslashes breaks the JSON parsing
      return jsonStr.replace(/'/g, "''");
    } catch (error) {
      console.error('Error stringifying JSON:', error);
      return '{}';
    }
  }
}
