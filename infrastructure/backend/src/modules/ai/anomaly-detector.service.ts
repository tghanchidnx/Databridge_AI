/**
 * Anomaly Detection Service
 * Detects mapping and hierarchy anomalies using pattern analysis
 */
import { Injectable, Logger } from '@nestjs/common';

export interface Anomaly {
  id: string;
  type: AnomalyType;
  severity: 'error' | 'warning' | 'info';
  hierarchyId?: string;
  hierarchyName?: string;
  message: string;
  details: string;
  suggestion?: string;
  autoFixable: boolean;
  fixAction?: {
    type: string;
    params: Record<string, any>;
  };
}

export type AnomalyType =
  | 'missing_mapping'
  | 'type_mismatch'
  | 'inconsistent_pattern'
  | 'orphan_node'
  | 'circular_reference'
  | 'duplicate_mapping'
  | 'missing_formula'
  | 'formula_error'
  | 'naming_convention'
  | 'level_inconsistency';

export interface HierarchyNode {
  id: string;
  hierarchyId: string;
  name: string;
  parentId?: string;
  level: number;
  isLeaf: boolean;
  mappings: Array<{
    sourceDatabase: string;
    sourceTable: string;
    sourceColumn: string;
    dataType?: string;
  }>;
  formula?: string;
  formulaType?: string;
  children?: HierarchyNode[];
}

export interface AnomalyDetectionConfig {
  checkMissingMappings: boolean;
  checkTypeMismatches: boolean;
  checkPatternConsistency: boolean;
  checkCircularReferences: boolean;
  checkDuplicates: boolean;
  checkFormulas: boolean;
  checkNamingConventions: boolean;
  namingPattern?: RegExp;
}

const DEFAULT_CONFIG: AnomalyDetectionConfig = {
  checkMissingMappings: true,
  checkTypeMismatches: true,
  checkPatternConsistency: true,
  checkCircularReferences: true,
  checkDuplicates: true,
  checkFormulas: true,
  checkNamingConventions: true,
};

@Injectable()
export class AnomalyDetectorService {
  private readonly logger = new Logger(AnomalyDetectorService.name);

  /**
   * Run full anomaly detection on a hierarchy tree
   */
  detectAnomalies(
    nodes: HierarchyNode[],
    config: Partial<AnomalyDetectionConfig> = {},
  ): Anomaly[] {
    const fullConfig = { ...DEFAULT_CONFIG, ...config };
    const anomalies: Anomaly[] = [];
    let anomalyId = 1;

    const flatNodes = this.flattenNodes(nodes);
    const nodeMap = new Map(flatNodes.map(n => [n.id, n]));

    // Check for missing mappings on leaf nodes
    if (fullConfig.checkMissingMappings) {
      const missingMappings = this.checkMissingMappings(flatNodes);
      anomalies.push(...missingMappings.map(a => ({ ...a, id: `anomaly-${anomalyId++}` })));
    }

    // Check for type mismatches in mappings
    if (fullConfig.checkTypeMismatches) {
      const typeMismatches = this.checkTypeMismatches(flatNodes);
      anomalies.push(...typeMismatches.map(a => ({ ...a, id: `anomaly-${anomalyId++}` })));
    }

    // Check for inconsistent mapping patterns among siblings
    if (fullConfig.checkPatternConsistency) {
      const patternIssues = this.checkPatternConsistency(flatNodes, nodeMap);
      anomalies.push(...patternIssues.map(a => ({ ...a, id: `anomaly-${anomalyId++}` })));
    }

    // Check for circular references
    if (fullConfig.checkCircularReferences) {
      const circularRefs = this.checkCircularReferences(flatNodes, nodeMap);
      anomalies.push(...circularRefs.map(a => ({ ...a, id: `anomaly-${anomalyId++}` })));
    }

    // Check for duplicate mappings
    if (fullConfig.checkDuplicates) {
      const duplicates = this.checkDuplicateMappings(flatNodes);
      anomalies.push(...duplicates.map(a => ({ ...a, id: `anomaly-${anomalyId++}` })));
    }

    // Check formula issues
    if (fullConfig.checkFormulas) {
      const formulaIssues = this.checkFormulaIssues(flatNodes, nodeMap);
      anomalies.push(...formulaIssues.map(a => ({ ...a, id: `anomaly-${anomalyId++}` })));
    }

    // Check naming conventions
    if (fullConfig.checkNamingConventions) {
      const namingIssues = this.checkNamingConventions(flatNodes, fullConfig.namingPattern);
      anomalies.push(...namingIssues.map(a => ({ ...a, id: `anomaly-${anomalyId++}` })));
    }

    // Sort by severity
    const severityOrder = { error: 0, warning: 1, info: 2 };
    return anomalies.sort((a, b) => severityOrder[a.severity] - severityOrder[b.severity]);
  }

  /**
   * Check for real-time anomalies on a single mapping change
   */
  checkMappingChange(
    node: HierarchyNode,
    newMapping: HierarchyNode['mappings'][0],
    allNodes: HierarchyNode[],
  ): Anomaly[] {
    const anomalies: Anomaly[] = [];
    let anomalyId = 1;

    // Check for duplicate across project
    const existingMapping = allNodes.find(n =>
      n.id !== node.id &&
      n.mappings.some(m =>
        m.sourceDatabase === newMapping.sourceDatabase &&
        m.sourceTable === newMapping.sourceTable &&
        m.sourceColumn === newMapping.sourceColumn
      )
    );

    if (existingMapping) {
      anomalies.push({
        id: `rt-anomaly-${anomalyId++}`,
        type: 'duplicate_mapping',
        severity: 'warning',
        hierarchyId: node.id,
        hierarchyName: node.name,
        message: 'Duplicate mapping detected',
        details: `This column is already mapped to "${existingMapping.name}"`,
        suggestion: 'Consider if this is intentional or if you should use a different source',
        autoFixable: false,
      });
    }

    // Check type compatibility (if data types are available)
    if (newMapping.dataType) {
      const typeIssue = this.checkSingleTypeMatch(node, newMapping);
      if (typeIssue) {
        anomalies.push({ ...typeIssue, id: `rt-anomaly-${anomalyId++}` });
      }
    }

    return anomalies;
  }

  /**
   * Flatten nested nodes into array
   */
  private flattenNodes(nodes: HierarchyNode[]): HierarchyNode[] {
    const result: HierarchyNode[] = [];

    const traverse = (nodeList: HierarchyNode[]) => {
      for (const node of nodeList) {
        result.push(node);
        if (node.children?.length) {
          traverse(node.children);
        }
      }
    };

    traverse(nodes);
    return result;
  }

  /**
   * Check for missing mappings on leaf nodes
   */
  private checkMissingMappings(nodes: HierarchyNode[]): Omit<Anomaly, 'id'>[] {
    return nodes
      .filter(node => node.isLeaf && (!node.mappings || node.mappings.length === 0) && !node.formula)
      .map(node => ({
        type: 'missing_mapping' as AnomalyType,
        severity: 'warning' as const,
        hierarchyId: node.id,
        hierarchyName: node.name,
        message: 'Leaf node without mapping or formula',
        details: `"${node.name}" is a leaf node but has no source mapping or calculation formula`,
        suggestion: 'Add a source mapping or define a formula for this node',
        autoFixable: false,
      }));
  }

  /**
   * Check for type mismatches
   */
  private checkTypeMismatches(nodes: HierarchyNode[]): Omit<Anomaly, 'id'>[] {
    const anomalies: Omit<Anomaly, 'id'>[] = [];

    for (const node of nodes) {
      for (const mapping of node.mappings) {
        const issue = this.checkSingleTypeMatch(node, mapping);
        if (issue) {
          anomalies.push(issue);
        }
      }
    }

    return anomalies;
  }

  /**
   * Check single type match
   */
  private checkSingleTypeMatch(
    node: HierarchyNode,
    mapping: HierarchyNode['mappings'][0],
  ): Omit<Anomaly, 'id'> | null {
    if (!mapping.dataType) return null;

    // Financial hierarchies typically expect numeric types
    const numericTypes = ['number', 'decimal', 'float', 'double', 'int', 'integer', 'numeric', 'money', 'currency'];
    const isNumeric = numericTypes.some(t => mapping.dataType?.toLowerCase().includes(t));

    // Check if this looks like an amount/value node
    const isValueNode = /amount|value|total|sum|balance|price|cost|revenue|expense|profit/i.test(node.name);

    if (isValueNode && !isNumeric) {
      return {
        type: 'type_mismatch',
        severity: 'warning',
        hierarchyId: node.id,
        hierarchyName: node.name,
        message: 'Potential type mismatch',
        details: `"${node.name}" appears to be a value field but is mapped to non-numeric column (${mapping.dataType})`,
        suggestion: 'Verify the source column type or add a type conversion',
        autoFixable: false,
      };
    }

    return null;
  }

  /**
   * Check for inconsistent patterns among siblings
   */
  private checkPatternConsistency(
    nodes: HierarchyNode[],
    nodeMap: Map<string, HierarchyNode>,
  ): Omit<Anomaly, 'id'>[] {
    const anomalies: Omit<Anomaly, 'id'>[] = [];

    // Group nodes by parent
    const nodesByParent = new Map<string, HierarchyNode[]>();
    for (const node of nodes) {
      const parentId = node.parentId || 'root';
      if (!nodesByParent.has(parentId)) {
        nodesByParent.set(parentId, []);
      }
      nodesByParent.get(parentId)!.push(node);
    }

    // Check siblings for pattern inconsistencies
    for (const [parentId, siblings] of nodesByParent) {
      if (siblings.length < 2) continue;

      // Check mapping source consistency
      const mappedSiblings = siblings.filter(s => s.mappings.length > 0);
      if (mappedSiblings.length >= 2) {
        const tables = new Set(mappedSiblings.flatMap(s => s.mappings.map(m => m.sourceTable)));

        // If most siblings use the same table but one doesn't, flag it
        const tableCounts = new Map<string, number>();
        for (const sibling of mappedSiblings) {
          for (const mapping of sibling.mappings) {
            tableCounts.set(mapping.sourceTable, (tableCounts.get(mapping.sourceTable) || 0) + 1);
          }
        }

        const [mostCommonTable, count] = [...tableCounts.entries()]
          .sort((a, b) => b[1] - a[1])[0] || [null, 0];

        if (mostCommonTable && count >= mappedSiblings.length * 0.7) {
          const outliers = mappedSiblings.filter(s =>
            !s.mappings.some(m => m.sourceTable === mostCommonTable)
          );

          for (const outlier of outliers) {
            anomalies.push({
              type: 'inconsistent_pattern',
              severity: 'info',
              hierarchyId: outlier.id,
              hierarchyName: outlier.name,
              message: 'Inconsistent mapping source',
              details: `Siblings mostly map to "${mostCommonTable}", but "${outlier.name}" uses different source(s)`,
              suggestion: 'Verify if this is intentional or should match sibling pattern',
              autoFixable: false,
            });
          }
        }
      }
    }

    return anomalies;
  }

  /**
   * Check for circular references
   */
  private checkCircularReferences(
    nodes: HierarchyNode[],
    nodeMap: Map<string, HierarchyNode>,
  ): Omit<Anomaly, 'id'>[] {
    const anomalies: Omit<Anomaly, 'id'>[] = [];
    const visited = new Set<string>();
    const recursionStack = new Set<string>();

    const detectCycle = (nodeId: string, path: string[]): boolean => {
      if (recursionStack.has(nodeId)) {
        return true;
      }
      if (visited.has(nodeId)) {
        return false;
      }

      visited.add(nodeId);
      recursionStack.add(nodeId);

      const node = nodeMap.get(nodeId);
      if (node?.children) {
        for (const child of node.children) {
          if (detectCycle(child.id, [...path, nodeId])) {
            return true;
          }
        }
      }

      recursionStack.delete(nodeId);
      return false;
    };

    for (const node of nodes) {
      if (!visited.has(node.id)) {
        if (detectCycle(node.id, [])) {
          anomalies.push({
            type: 'circular_reference',
            severity: 'error',
            hierarchyId: node.id,
            hierarchyName: node.name,
            message: 'Circular reference detected',
            details: `Node "${node.name}" is part of a circular parent-child relationship`,
            suggestion: 'Review and fix the parent-child relationships',
            autoFixable: false,
          });
        }
      }
    }

    return anomalies;
  }

  /**
   * Check for duplicate mappings
   */
  private checkDuplicateMappings(nodes: HierarchyNode[]): Omit<Anomaly, 'id'>[] {
    const anomalies: Omit<Anomaly, 'id'>[] = [];
    const mappingMap = new Map<string, HierarchyNode[]>();

    for (const node of nodes) {
      for (const mapping of node.mappings) {
        const key = `${mapping.sourceDatabase}:${mapping.sourceTable}:${mapping.sourceColumn}`;
        if (!mappingMap.has(key)) {
          mappingMap.set(key, []);
        }
        mappingMap.get(key)!.push(node);
      }
    }

    for (const [key, nodesWithMapping] of mappingMap) {
      if (nodesWithMapping.length > 1) {
        const [db, table, column] = key.split(':');
        anomalies.push({
          type: 'duplicate_mapping',
          severity: 'warning',
          message: 'Duplicate mapping across nodes',
          details: `Column "${table}.${column}" is mapped to multiple hierarchies: ${nodesWithMapping.map(n => n.name).join(', ')}`,
          suggestion: 'Review if this duplication is intentional',
          autoFixable: false,
        });
      }
    }

    return anomalies;
  }

  /**
   * Check formula issues
   */
  private checkFormulaIssues(
    nodes: HierarchyNode[],
    nodeMap: Map<string, HierarchyNode>,
  ): Omit<Anomaly, 'id'>[] {
    const anomalies: Omit<Anomaly, 'id'>[] = [];

    for (const node of nodes) {
      // Check if parent node should have a formula but doesn't
      if (!node.isLeaf && !node.formula && node.children && node.children.length > 0) {
        const childrenHaveMappings = node.children.some(c => c.mappings.length > 0 || c.formula);

        if (childrenHaveMappings) {
          anomalies.push({
            type: 'missing_formula',
            severity: 'info',
            hierarchyId: node.id,
            hierarchyName: node.name,
            message: 'Parent node may need a formula',
            details: `"${node.name}" has children with mappings but no aggregation formula defined`,
            suggestion: 'Consider adding a SUM or other aggregation formula',
            autoFixable: true,
            fixAction: {
              type: 'add_formula',
              params: {
                hierarchyId: node.id,
                formulaType: 'SUM',
                formulaText: `SUM(${node.children.map(c => c.name).join(', ')})`,
              },
            },
          });
        }
      }

      // Check for formula syntax issues
      if (node.formula) {
        const syntaxIssue = this.checkFormulaSyntax(node.formula);
        if (syntaxIssue) {
          anomalies.push({
            type: 'formula_error',
            severity: 'error',
            hierarchyId: node.id,
            hierarchyName: node.name,
            message: 'Formula syntax error',
            details: syntaxIssue,
            autoFixable: false,
          });
        }
      }
    }

    return anomalies;
  }

  /**
   * Check formula syntax
   */
  private checkFormulaSyntax(formula: string): string | null {
    // Check for balanced parentheses
    let depth = 0;
    for (const char of formula) {
      if (char === '(') depth++;
      if (char === ')') depth--;
      if (depth < 0) return 'Unbalanced parentheses - extra closing parenthesis';
    }
    if (depth !== 0) return 'Unbalanced parentheses - missing closing parenthesis';

    // Check for division by zero
    if (/\/\s*0(?!\d)/.test(formula)) {
      return 'Potential division by zero';
    }

    // Check for empty variable references
    if (/\{\s*\}/.test(formula)) {
      return 'Empty variable reference found';
    }

    return null;
  }

  /**
   * Check naming conventions
   */
  private checkNamingConventions(
    nodes: HierarchyNode[],
    customPattern?: RegExp,
  ): Omit<Anomaly, 'id'>[] {
    const anomalies: Omit<Anomaly, 'id'>[] = [];

    // Default: check for common issues
    for (const node of nodes) {
      // Check for leading/trailing whitespace
      if (node.name !== node.name.trim()) {
        anomalies.push({
          type: 'naming_convention',
          severity: 'info',
          hierarchyId: node.id,
          hierarchyName: node.name,
          message: 'Name has leading/trailing whitespace',
          details: `"${node.name}" contains extra whitespace`,
          suggestion: 'Trim whitespace from hierarchy name',
          autoFixable: true,
          fixAction: {
            type: 'rename',
            params: {
              hierarchyId: node.id,
              newName: node.name.trim(),
            },
          },
        });
      }

      // Check for special characters that might cause issues
      if (/[<>:"|?*\\]/.test(node.name)) {
        anomalies.push({
          type: 'naming_convention',
          severity: 'warning',
          hierarchyId: node.id,
          hierarchyName: node.name,
          message: 'Name contains special characters',
          details: `"${node.name}" contains characters that may cause issues in exports`,
          suggestion: 'Consider removing special characters',
          autoFixable: false,
        });
      }

      // Check custom pattern if provided
      if (customPattern && !customPattern.test(node.name)) {
        anomalies.push({
          type: 'naming_convention',
          severity: 'info',
          hierarchyId: node.id,
          hierarchyName: node.name,
          message: 'Name does not match naming convention',
          details: `"${node.name}" doesn't match the expected pattern`,
          suggestion: 'Update name to match project naming convention',
          autoFixable: false,
        });
      }
    }

    return anomalies;
  }

  /**
   * Get summary statistics for anomalies
   */
  getAnomalySummary(anomalies: Anomaly[]): {
    total: number;
    byType: Record<AnomalyType, number>;
    bySeverity: Record<'error' | 'warning' | 'info', number>;
    autoFixableCount: number;
  } {
    const byType: Record<string, number> = {};
    const bySeverity: Record<string, number> = { error: 0, warning: 0, info: 0 };
    let autoFixableCount = 0;

    for (const anomaly of anomalies) {
      byType[anomaly.type] = (byType[anomaly.type] || 0) + 1;
      bySeverity[anomaly.severity]++;
      if (anomaly.autoFixable) autoFixableCount++;
    }

    return {
      total: anomalies.length,
      byType: byType as Record<AnomalyType, number>,
      bySeverity: bySeverity as Record<'error' | 'warning' | 'info', number>,
      autoFixableCount,
    };
  }
}
