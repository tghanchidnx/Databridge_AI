import React, { useState, useMemo } from "react";
import type { SmartHierarchyMaster, SourceMapping } from "@/services/api/hierarchy";
import { Badge } from "@/components/ui/badge";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import {
  ChevronDown,
  ChevronRight,
  Database,
  GitBranch,
  Layers,
  MapPin,
  FolderTree,
} from "lucide-react";
import { HelpTooltip } from "@/components/ui/help-tooltip";

interface InheritedMappingsViewProps {
  hierarchy: SmartHierarchyMaster;
  allHierarchies: SmartHierarchyMaster[];
}

interface MappingWithPath {
  mapping: SourceMapping;
  sourcePath: string[]; // Full path from current hierarchy down to where mapping is attached
  sourceHierarchyId: string;
  sourceHierarchyName: string;
  depth: number;
}

interface HierarchyTreeNode {
  id: string;
  hierarchyId: string;
  hierarchyName: string;
  ownMappings: SourceMapping[];
  ownCount: number;
  descendantCount: number;
  totalCount: number;
  children: HierarchyTreeNode[];
  path: string[]; // Path from root of current view
  depth: number;
}

interface PrecedenceGroup {
  group: string;
  mappings: MappingWithPath[];
}

export const InheritedMappingsView: React.FC<InheritedMappingsViewProps> = ({
  hierarchy,
  allHierarchies,
}) => {
  const [expandedNodes, setExpandedNodes] = useState<Set<string>>(new Set());
  const [expandedPrecedence, setExpandedPrecedence] = useState<Set<string>>(new Set(["1"])); // Default group 1 expanded

  // Build a map of id -> hierarchy for quick lookup
  const hierarchyMap = useMemo(() => {
    const map = new Map<string, SmartHierarchyMaster>();
    allHierarchies.forEach((h) => {
      if (h.id) map.set(h.id, h);
    });
    return map;
  }, [allHierarchies]);

  // Find direct children of a hierarchy
  // Note: parentId in the data is the parent's hierarchyId (slug), not UUID
  const getDirectChildren = (parentHierarchyId: string | undefined): SmartHierarchyMaster[] => {
    if (!parentHierarchyId) return [];
    return allHierarchies.filter((h) => h.parentId === parentHierarchyId)
      .sort((a, b) => (a.sortOrder || 0) - (b.sortOrder || 0));
  };

  // Build the full tree structure with mapping counts
  const buildTree = (
    hier: SmartHierarchyMaster,
    currentPath: string[] = [],
    depth: number = 0
  ): HierarchyTreeNode => {
    const newPath = [...currentPath, hier.hierarchyName];
    // Use hierarchyId to find children since that's what parentId references
    const children = getDirectChildren(hier.hierarchyId);

    // Recursively build child trees
    const childNodes = children.map(child =>
      buildTree(child, newPath, depth + 1)
    );

    // Calculate descendant mapping count (sum of all children's total counts)
    const descendantCount = childNodes.reduce(
      (sum, child) => sum + child.totalCount,
      0
    );

    const ownCount = hier.mapping?.length || 0;

    return {
      id: hier.id || "",
      hierarchyId: hier.hierarchyId,
      hierarchyName: hier.hierarchyName,
      ownMappings: hier.mapping || [],
      ownCount,
      descendantCount,
      totalCount: ownCount + descendantCount,
      children: childNodes,
      path: newPath,
      depth,
    };
  };

  // Collect all mappings with their full paths
  const collectMappingsWithPaths = (
    node: HierarchyTreeNode,
    basePath: string[] = []
  ): MappingWithPath[] => {
    const result: MappingWithPath[] = [];

    // Add this node's own mappings
    node.ownMappings.forEach(mapping => {
      result.push({
        mapping,
        sourcePath: node.path,
        sourceHierarchyId: node.hierarchyId,
        sourceHierarchyName: node.hierarchyName,
        depth: node.depth,
      });
    });

    // Recursively collect from children
    node.children.forEach(child => {
      result.push(...collectMappingsWithPaths(child, node.path));
    });

    return result;
  };

  // Compute all data
  const computedData = useMemo(() => {
    // Debug logging - use hierarchyId for matching since that's what parentId contains
    console.log('[InheritedMappingsView] Current hierarchy:', hierarchy.hierarchyName, 'hierarchyId:', hierarchy.hierarchyId);
    console.log('[InheritedMappingsView] All hierarchies count:', allHierarchies.length);
    console.log('[InheritedMappingsView] Children found (parentId === hierarchyId):',
      allHierarchies.filter(h => h.parentId === hierarchy.hierarchyId).map(h => h.hierarchyName)
    );

    // Build tree starting from current hierarchy's children
    // Pass hierarchyId because that's what children's parentId references
    const childTrees = getDirectChildren(hierarchy.hierarchyId).map(child =>
      buildTree(child, [hierarchy.hierarchyName], 1)
    );

    console.log('[InheritedMappingsView] Child trees:', childTrees.length, childTrees.map(t => ({name: t.hierarchyName, total: t.totalCount})));

    const ownMappings = hierarchy.mapping || [];

    // Total inherited from all descendants
    const totalInheritedCount = childTrees.reduce(
      (sum, tree) => sum + tree.totalCount,
      0
    );

    // Collect all mappings with paths
    const allInheritedMappings: MappingWithPath[] = [];
    childTrees.forEach(tree => {
      allInheritedMappings.push(...collectMappingsWithPaths(tree));
    });

    // Add own mappings
    const ownMappingsWithPath: MappingWithPath[] = ownMappings.map(m => ({
      mapping: m,
      sourcePath: [hierarchy.hierarchyName],
      sourceHierarchyId: hierarchy.hierarchyId,
      sourceHierarchyName: hierarchy.hierarchyName,
      depth: 0,
    }));

    // Group all mappings by precedence
    const byPrecedence: Record<string, MappingWithPath[]> = {};
    [...ownMappingsWithPath, ...allInheritedMappings].forEach(m => {
      const group = m.mapping.precedence_group || "1";
      if (!byPrecedence[group]) {
        byPrecedence[group] = [];
      }
      byPrecedence[group].push(m);
    });

    const precedenceGroups: PrecedenceGroup[] = Object.keys(byPrecedence)
      .sort()
      .map(group => ({
        group,
        mappings: byPrecedence[group],
      }));

    return {
      ownCount: ownMappings.length,
      inheritedCount: totalInheritedCount,
      totalCount: ownMappings.length + totalInheritedCount,
      childTrees,
      precedenceGroups,
      allInheritedMappings,
    };
  }, [hierarchy, allHierarchies]);

  const toggleNode = (id: string) => {
    setExpandedNodes(prev => {
      const next = new Set(prev);
      if (next.has(id)) {
        next.delete(id);
      } else {
        next.add(id);
      }
      return next;
    });
  };

  const togglePrecedence = (group: string) => {
    setExpandedPrecedence(prev => {
      const next = new Set(prev);
      if (next.has(group)) {
        next.delete(group);
      } else {
        next.add(group);
      }
      return next;
    });
  };

  // Render a tree node recursively
  const renderTreeNode = (node: HierarchyTreeNode, isRoot: boolean = false): React.ReactNode => {
    const isExpanded = expandedNodes.has(node.id);
    const hasContent = node.totalCount > 0;
    const hasChildren = node.children.length > 0;

    if (!hasContent) return null;

    return (
      <div key={node.id} className={`${isRoot ? "" : "ml-4 border-l border-border/50 pl-3"}`}>
        <Collapsible open={isExpanded} onOpenChange={() => toggleNode(node.id)}>
          <CollapsibleTrigger asChild>
            <div
              className={`flex items-center gap-2 py-1.5 px-2 rounded hover:bg-accent/50 cursor-pointer transition-colors`}
            >
              {(hasChildren || node.ownCount > 0) ? (
                isExpanded ? (
                  <ChevronDown className="w-4 h-4 text-muted-foreground flex-shrink-0" />
                ) : (
                  <ChevronRight className="w-4 h-4 text-muted-foreground flex-shrink-0" />
                )
              ) : (
                <div className="w-4 h-4 flex-shrink-0" />
              )}
              <GitBranch className="w-4 h-4 text-blue-500 flex-shrink-0" />
              <span className="text-sm font-medium flex-1 truncate">{node.hierarchyName}</span>
              <div className="flex items-center gap-1.5 flex-shrink-0">
                {node.ownCount > 0 && (
                  <Badge variant="default" className="text-xs px-1.5 py-0 h-5">
                    {node.ownCount} direct
                  </Badge>
                )}
                {node.descendantCount > 0 && (
                  <Badge variant="secondary" className="text-xs px-1.5 py-0 h-5">
                    +{node.descendantCount} below
                  </Badge>
                )}
              </div>
            </div>
          </CollapsibleTrigger>
          <CollapsibleContent>
            <div className="mt-1 space-y-1">
              {/* Show own mappings with path indicator */}
              {node.ownMappings.length > 0 && (
                <div className="ml-6 bg-blue-50 dark:bg-blue-950/30 rounded-md p-2 border border-blue-200 dark:border-blue-900">
                  <div className="flex items-center gap-1.5 text-xs text-blue-600 dark:text-blue-400 mb-1.5">
                    <MapPin className="w-3 h-3" />
                    <span className="font-medium">Mappings attached here:</span>
                  </div>
                  <div className="space-y-1">
                    {node.ownMappings.map((m, idx) => (
                      <div key={idx} className="text-xs flex items-center gap-2 py-1 px-2 bg-white dark:bg-gray-900 rounded">
                        <Database className="w-3 h-3 text-muted-foreground flex-shrink-0" />
                        <span className="font-mono truncate">
                          {m.source_database}.{m.source_schema}.{m.source_table}
                          {m.source_column && `.${m.source_column}`}
                        </span>
                        {m.source_uid && (
                          <span className="text-muted-foreground">= {m.source_uid}</span>
                        )}
                        {m.precedence_group && m.precedence_group !== "1" && (
                          <Badge variant="outline" className="text-[10px] px-1 py-0 h-4">
                            P{m.precedence_group}
                          </Badge>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}
              {/* Render children recursively */}
              {node.children
                .filter(child => child.totalCount > 0)
                .map(child => renderTreeNode(child))}
            </div>
          </CollapsibleContent>
        </Collapsible>
      </div>
    );
  };

  // Render hierarchy path breadcrumb
  const renderPath = (path: string[]) => {
    return (
      <div className="flex items-center gap-1 text-[10px] text-muted-foreground">
        {path.map((segment, idx) => (
          <React.Fragment key={idx}>
            {idx > 0 && <span className="text-muted-foreground/50">/</span>}
            <span className={idx === path.length - 1 ? "text-foreground font-medium" : ""}>
              {segment}
            </span>
          </React.Fragment>
        ))}
      </div>
    );
  };

  if (computedData.totalCount === 0 && computedData.childTrees.length === 0) {
    return null;
  }

  return (
    <div className="space-y-4">
      {/* Summary Header */}
      <div className="bg-card rounded-lg p-4 ring-1 ring-border/50">
        <div className="flex items-center justify-between mb-3">
          <div className="flex items-center gap-2">
            <Layers className="w-5 h-5 text-primary" />
            <HelpTooltip topicId="mappingSummary">
              <span className="font-medium">Mapping Summary</span>
            </HelpTooltip>
          </div>
          <div className="flex items-center gap-2">
            <Badge variant="default" className="text-xs">
              {computedData.ownCount} own
            </Badge>
            <Badge variant="secondary" className="text-xs">
              +{computedData.inheritedCount} inherited
            </Badge>
            <Badge variant="outline" className="text-xs font-semibold">
              {computedData.totalCount} total
            </Badge>
          </div>
        </div>

        {/* Hierarchy Tree View */}
        {computedData.childTrees.length > 0 && computedData.inheritedCount > 0 && (
          <div className="border-t pt-3 mt-3">
            <div className="flex items-center gap-2 text-sm font-medium mb-3 text-muted-foreground">
              <FolderTree className="w-4 h-4" />
              <HelpTooltip topicId="mappingInheritance">
                <span>Child Hierarchy Mappings</span>
              </HelpTooltip>
            </div>
            <div className="space-y-1 max-h-[400px] overflow-y-auto">
              {computedData.childTrees
                .filter(tree => tree.totalCount > 0)
                .map(tree => renderTreeNode(tree, true))}
            </div>
          </div>
        )}
      </div>

      {/* Precedence Groups Section - All mappings grouped by precedence with paths */}
      {computedData.precedenceGroups.length > 0 && computedData.totalCount > 0 && (
        <div className="bg-card rounded-lg ring-1 ring-border/50">
          <div className="p-4 border-b">
            <div className="flex items-center gap-2">
              <Layers className="w-5 h-5 text-primary" />
              <HelpTooltip topicId="precedenceGroups">
                <span className="font-medium">All Mappings by Precedence Group</span>
              </HelpTooltip>
            </div>
            <p className="text-xs text-muted-foreground mt-1">
              Shows where each mapping is attached in the hierarchy
            </p>
          </div>
          <div className="divide-y">
            {computedData.precedenceGroups.map((pg) => {
              const isExpanded = expandedPrecedence.has(pg.group);
              return (
                <Collapsible
                  key={pg.group}
                  open={isExpanded}
                  onOpenChange={() => togglePrecedence(pg.group)}
                >
                  <CollapsibleTrigger asChild>
                    <div className="flex items-center gap-3 p-3 hover:bg-accent/50 cursor-pointer transition-colors">
                      {isExpanded ? (
                        <ChevronDown className="w-4 h-4 text-muted-foreground" />
                      ) : (
                        <ChevronRight className="w-4 h-4 text-muted-foreground" />
                      )}
                      <Badge variant="default" className="min-w-[70px] justify-center">
                        Group {pg.group}
                      </Badge>
                      <span className="text-sm text-muted-foreground flex-1">
                        {pg.mappings.length} mapping{pg.mappings.length !== 1 ? "s" : ""}
                      </span>
                    </div>
                  </CollapsibleTrigger>
                  <CollapsibleContent>
                    <div className="px-4 pb-3 space-y-2">
                      {pg.mappings.map((m, idx) => (
                        <div
                          key={idx}
                          className="flex flex-col gap-1 text-sm py-2 px-3 bg-muted/30 rounded-md"
                        >
                          {/* Hierarchy Path */}
                          <div className="flex items-center gap-2">
                            <FolderTree className="w-3 h-3 text-muted-foreground flex-shrink-0" />
                            {renderPath(m.sourcePath)}
                          </div>
                          {/* Mapping Details */}
                          <div className="flex items-center gap-2 ml-5">
                            <Database className="w-3.5 h-3.5 text-blue-500 flex-shrink-0" />
                            <span className="font-mono text-xs">
                              {m.mapping.source_database}.{m.mapping.source_schema}.{m.mapping.source_table}
                              {m.mapping.source_column && `.${m.mapping.source_column}`}
                            </span>
                            {m.mapping.source_uid && (
                              <span className="text-muted-foreground text-xs">
                                = {m.mapping.source_uid}
                              </span>
                            )}
                          </div>
                        </div>
                      ))}
                    </div>
                  </CollapsibleContent>
                </Collapsible>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
};
