import { Injectable, Logger, NotFoundException } from '@nestjs/common';
import * as _ from 'lodash';
import { PrismaService } from '../../database/prisma/prisma.service';

interface DependencyNode {
  uniqueKey: string;
  resourceName: string;
  database: string;
  schema: string;
  resourceType: string;
  ddl: string;
  children: string[];
  dependencyLength: number;
  comments: string;
  parentKey?: string;
}

interface ScriptGenerationOptions {
  commentsFilters?: string[];
  resourceNameFilters?: string[];
}

@Injectable()
export class ScriptGeneratorService {
  private readonly logger = new Logger(ScriptGeneratorService.name);

  constructor(private prisma: PrismaService) {}

  /**
   * Generate deployment scripts for a schema comparison job
   * Migrated from Python: generate_script_target_deployment()
   */
  async generateDeploymentScript(jobId: string, options: ScriptGenerationOptions = {}) {
    this.logger.log(`Generating deployment script for job ${jobId}`);

    // Fetch the comparison job
    const job = await this.prisma.schemaComparisonJob.findUnique({
      where: { id: jobId },
    });

    if (!job || !job.result) {
      throw new NotFoundException(`Comparison job ${jobId} not found or incomplete`);
    }

    // Extract comparison details
    const details = (job.result as any).details || [];
    if (details.length === 0) {
      return { message: 'Nothing to compare after filters', scripts: [] };
    }

    // Build dependency hierarchy
    const dependencyMap = this.buildDependencyHierarchy(details);

    // Apply filters
    let filteredNodes = this.applyFilters(
      dependencyMap,
      options.commentsFilters,
      options.resourceNameFilters,
    );

    // Sort by dependency depth (resources with no dependencies first)
    const sortedNodes = _.sortBy(filteredNodes, ['dependencyLength']);

    // Generate DDL scripts
    const sourceScripts = this.generateDDLScripts(sortedNodes, 'source');
    const targetScripts = this.generateDDLScripts(sortedNodes, 'target');

    return {
      jobId,
      totalResources: sortedNodes.length,
      sourceDatabase: job.sourceDatabase,
      sourceSchema: job.sourceSchema,
      targetDatabase: job.targetDatabase,
      targetSchema: job.targetSchema,
      sourceScript: sourceScripts.join('\n\n'),
      targetScript: targetScripts.join('\n\n'),
      resources: sortedNodes.map((node) => ({
        name: node.resourceName,
        type: node.resourceType,
        dependencies: node.children,
        comments: node.comments,
      })),
    };
  }

  /**
   * Build dependency hierarchy from comparison details
   */
  private buildDependencyHierarchy(details: any[]): Map<string, DependencyNode> {
    const nodeMap = new Map<string, DependencyNode>();

    // First pass: Create all nodes
    details.forEach((row) => {
      const uniqueKey = row.uniqueKey || `${row.database}.${row.schema}.${row.table}`;

      nodeMap.set(uniqueKey, {
        uniqueKey,
        resourceName: row.resourceNameSource || row.table,
        database: row.databaseSource || row.database,
        schema: row.schemaSource || row.schema,
        resourceType: row.resourceTypeSource || row.type || 'TABLE',
        ddl: row.ddlSource || '',
        children: [],
        dependencyLength: 0,
        comments: row.comments || row.status || '',
        parentKey: row.parentKey,
      });
    });

    // Second pass: Build parent-child relationships
    nodeMap.forEach((node) => {
      if (node.parentKey && nodeMap.has(node.parentKey)) {
        const parent = nodeMap.get(node.parentKey)!;
        if (!parent.children.includes(node.uniqueKey)) {
          parent.children.push(node.uniqueKey);
        }
      }
    });

    // Third pass: Calculate transitive dependencies (children of children)
    nodeMap.forEach((node) => {
      const allChildren = this.getAllDependencies(node.uniqueKey, nodeMap);
      node.children = allChildren;
      node.dependencyLength = allChildren.length;
    });

    return nodeMap;
  }

  /**
   * Get all transitive dependencies of a node
   */
  private getAllDependencies(nodeKey: string, nodeMap: Map<string, DependencyNode>): string[] {
    const visited = new Set<string>();
    const stack = [nodeKey];
    const dependencies: string[] = [];

    while (stack.length > 0) {
      const currentKey = stack.pop()!;

      if (visited.has(currentKey)) {
        continue;
      }

      visited.add(currentKey);
      const node = nodeMap.get(currentKey);

      if (!node) {
        continue;
      }

      // Find all nodes that reference this node as parent
      nodeMap.forEach((childNode) => {
        if (childNode.parentKey === currentKey && !visited.has(childNode.uniqueKey)) {
          dependencies.push(childNode.uniqueKey);
          stack.push(childNode.uniqueKey);
        }
      });
    }

    return _.uniq(dependencies);
  }

  /**
   * Apply comment and resource name filters
   */
  private applyFilters(
    nodeMap: Map<string, DependencyNode>,
    commentsFilters?: string[],
    resourceNameFilters?: string[],
  ): DependencyNode[] {
    let filtered: DependencyNode[] = Array.from(nodeMap.values());

    // If resource name filters provided, include all dependencies
    if (resourceNameFilters && resourceNameFilters.length > 0) {
      const requiredKeys = new Set<string>();

      // Find keys matching resource names
      filtered.forEach((node) => {
        if (resourceNameFilters.includes(node.resourceName)) {
          requiredKeys.add(node.uniqueKey);
          // Add all dependencies
          node.children.forEach((childKey) => requiredKeys.add(childKey));
        }
      });

      // Also add parents of required resources
      filtered.forEach((node) => {
        if (requiredKeys.has(node.uniqueKey)) {
          let parent = node.parentKey;
          while (parent && nodeMap.has(parent)) {
            requiredKeys.add(parent);
            parent = nodeMap.get(parent)?.parentKey;
          }
        }
      });

      filtered = filtered.filter((node) => requiredKeys.has(node.uniqueKey));
    }

    // Apply comments filter
    if (commentsFilters && commentsFilters.length > 0) {
      filtered = filtered.filter((node) => commentsFilters.includes(node.comments));
    }

    return filtered;
  }

  /**
   * Generate DDL scripts for source or target
   */
  private generateDDLScripts(nodes: DependencyNode[], mode: 'source' | 'target'): string[] {
    const scripts: string[] = [];
    const currentDate = new Date().toISOString().split('T')[0];

    scripts.push(`-- Generated on ${currentDate}`);
    scripts.push(`-- Total resources: ${nodes.length}`);
    scripts.push(`-- Mode: ${mode.toUpperCase()}`);
    scripts.push('');

    nodes.forEach((node, index) => {
      scripts.push(
        `-- [${index + 1}/${nodes.length}] ${node.resourceType}: ${node.database}.${node.schema}.${node.resourceName}`,
      );
      scripts.push(`-- Dependencies: ${node.dependencyLength}`);
      scripts.push(`-- Status: ${node.comments}`);

      if (node.ddl) {
        scripts.push(node.ddl.trim());
      } else {
        scripts.push(`-- DDL not available for ${node.resourceName}`);
      }

      scripts.push('');
    });

    return scripts;
  }

  /**
   * Generate script for specific resource types only
   */
  async generateScriptByResourceType(
    jobId: string,
    resourceTypes: string[],
    options: ScriptGenerationOptions = {},
  ) {
    const result = await this.generateDeploymentScript(jobId, options);

    // Filter resources by type
    const filteredResources = result.resources.filter((r) =>
      resourceTypes.includes(r.type.toUpperCase()),
    );

    return {
      ...result,
      resources: filteredResources,
      totalResources: filteredResources.length,
    };
  }
}
