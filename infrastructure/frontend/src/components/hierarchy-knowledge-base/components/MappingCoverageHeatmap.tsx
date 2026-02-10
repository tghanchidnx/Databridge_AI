/**
 * Mapping Coverage Heatmap
 * Visual representation of mapping completeness across hierarchy
 */
import { useMemo, useState } from "react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import {
  ChevronRight,
  ChevronDown,
  Database,
  AlertCircle,
  CheckCircle2,
  MinusCircle,
  Filter,
} from "lucide-react";

export interface HierarchyNode {
  id: string;
  name: string;
  hierarchyId: string;
  parentId: string | null;
  children?: HierarchyNode[];
  mappings?: Array<{
    sourceDatabase?: string;
    sourceTable?: string;
    sourceColumn?: string;
  }>;
  formulaConfig?: {
    formulaType?: string;
  };
}

interface MappingCoverageStats {
  totalNodes: number;
  mappedNodes: number;
  unmappedLeafNodes: number;
  partiallyMappedNodes: number;
  formulaNodes: number;
  coveragePercentage: number;
}

interface NodeCoverageInfo {
  node: HierarchyNode;
  status: "complete" | "partial" | "none" | "formula";
  mappingCount: number;
  childCoverage: number;
  issues: string[];
}

// Calculate coverage for a single node
function getNodeCoverage(node: HierarchyNode): NodeCoverageInfo {
  const mappingCount = node.mappings?.length || 0;
  const hasFormula = !!node.formulaConfig?.formulaType;
  const isLeaf = !node.children || node.children.length === 0;

  let status: NodeCoverageInfo["status"] = "none";
  const issues: string[] = [];

  if (hasFormula) {
    status = "formula";
  } else if (mappingCount > 0) {
    status = "complete";
  } else if (isLeaf) {
    status = "none";
    issues.push("Leaf node without mappings");
  }

  // Calculate child coverage
  let childCoverage = 100;
  if (node.children && node.children.length > 0) {
    const childStats = node.children.map((c) => getNodeCoverage(c));
    const coveredChildren = childStats.filter(
      (s) => s.status === "complete" || s.status === "formula"
    ).length;
    childCoverage = Math.round((coveredChildren / node.children.length) * 100);

    if (childCoverage > 0 && childCoverage < 100) {
      status = "partial";
    }
  }

  return {
    node,
    status,
    mappingCount,
    childCoverage,
    issues,
  };
}

// Calculate overall stats
function calculateStats(nodes: HierarchyNode[]): MappingCoverageStats {
  let totalNodes = 0;
  let mappedNodes = 0;
  let unmappedLeafNodes = 0;
  let partiallyMappedNodes = 0;
  let formulaNodes = 0;

  function traverse(node: HierarchyNode) {
    totalNodes++;
    const coverage = getNodeCoverage(node);

    if (coverage.status === "complete") mappedNodes++;
    else if (coverage.status === "formula") formulaNodes++;
    else if (coverage.status === "partial") partiallyMappedNodes++;
    else if (!node.children?.length) unmappedLeafNodes++;

    node.children?.forEach(traverse);
  }

  nodes.forEach(traverse);

  const coveragePercentage =
    totalNodes > 0
      ? Math.round(((mappedNodes + formulaNodes) / totalNodes) * 100)
      : 0;

  return {
    totalNodes,
    mappedNodes,
    unmappedLeafNodes,
    partiallyMappedNodes,
    formulaNodes,
    coveragePercentage,
  };
}

// Get color based on coverage percentage
function getCoverageColor(percentage: number): string {
  if (percentage >= 90) return "bg-green-500";
  if (percentage >= 70) return "bg-yellow-500";
  if (percentage >= 50) return "bg-orange-500";
  return "bg-red-500";
}

function getStatusColor(status: NodeCoverageInfo["status"]): string {
  switch (status) {
    case "complete":
      return "text-green-600 bg-green-50 border-green-200";
    case "formula":
      return "text-blue-600 bg-blue-50 border-blue-200";
    case "partial":
      return "text-yellow-600 bg-yellow-50 border-yellow-200";
    case "none":
      return "text-red-600 bg-red-50 border-red-200";
  }
}

function getStatusIcon(status: NodeCoverageInfo["status"]) {
  switch (status) {
    case "complete":
      return <CheckCircle2 className="h-4 w-4 text-green-600" />;
    case "formula":
      return <Database className="h-4 w-4 text-blue-600" />;
    case "partial":
      return <MinusCircle className="h-4 w-4 text-yellow-600" />;
    case "none":
      return <AlertCircle className="h-4 w-4 text-red-600" />;
  }
}

interface HeatmapNodeProps {
  node: HierarchyNode;
  depth: number;
  showUnmappedOnly: boolean;
  onSelect?: (node: HierarchyNode) => void;
}

function HeatmapNode({
  node,
  depth,
  showUnmappedOnly,
  onSelect,
}: HeatmapNodeProps) {
  const [expanded, setExpanded] = useState(depth < 2);
  const coverage = getNodeCoverage(node);
  const hasChildren = node.children && node.children.length > 0;

  // Filter logic for showUnmappedOnly
  if (showUnmappedOnly && coverage.status === "complete") {
    // Check if any descendant is unmapped
    const hasUnmappedDescendant = (n: HierarchyNode): boolean => {
      const c = getNodeCoverage(n);
      if (c.status === "none" || c.status === "partial") return true;
      return n.children?.some(hasUnmappedDescendant) || false;
    };
    if (!hasUnmappedDescendant(node)) return null;
  }

  return (
    <div className="select-none">
      <div
        className={cn(
          "flex items-center gap-2 py-1.5 px-2 rounded-md hover:bg-muted/50 cursor-pointer transition-colors",
          depth === 0 && "font-medium"
        )}
        style={{ paddingLeft: `${depth * 16 + 8}px` }}
        onClick={() => onSelect?.(node)}
      >
        {/* Expand/collapse button */}
        {hasChildren ? (
          <button
            onClick={(e) => {
              e.stopPropagation();
              setExpanded(!expanded);
            }}
            className="p-0.5 hover:bg-muted rounded"
          >
            {expanded ? (
              <ChevronDown className="h-4 w-4 text-muted-foreground" />
            ) : (
              <ChevronRight className="h-4 w-4 text-muted-foreground" />
            )}
          </button>
        ) : (
          <span className="w-5" />
        )}

        {/* Status icon */}
        <TooltipProvider>
          <Tooltip>
            <TooltipTrigger asChild>
              <div className="flex-shrink-0">{getStatusIcon(coverage.status)}</div>
            </TooltipTrigger>
            <TooltipContent>
              {coverage.status === "complete" && `${coverage.mappingCount} mapping(s)`}
              {coverage.status === "formula" && "Has formula"}
              {coverage.status === "partial" && `${coverage.childCoverage}% child coverage`}
              {coverage.status === "none" && "No mappings"}
            </TooltipContent>
          </Tooltip>
        </TooltipProvider>

        {/* Node name */}
        <span className="flex-1 truncate text-sm">{node.name}</span>

        {/* Coverage indicator */}
        {hasChildren && (
          <div className="flex items-center gap-2">
            <div className="w-12 h-1.5 bg-muted rounded-full overflow-hidden">
              <div
                className={cn("h-full transition-all", getCoverageColor(coverage.childCoverage))}
                style={{ width: `${coverage.childCoverage}%` }}
              />
            </div>
            <span className="text-xs text-muted-foreground w-8">
              {coverage.childCoverage}%
            </span>
          </div>
        )}

        {/* Mapping count badge */}
        {coverage.mappingCount > 0 && (
          <Badge variant="secondary" className="text-xs">
            {coverage.mappingCount}
          </Badge>
        )}
      </div>

      {/* Children */}
      {hasChildren && expanded && (
        <div>
          {node.children!.map((child) => (
            <HeatmapNode
              key={child.id}
              node={child}
              depth={depth + 1}
              showUnmappedOnly={showUnmappedOnly}
              onSelect={onSelect}
            />
          ))}
        </div>
      )}
    </div>
  );
}

interface MappingCoverageHeatmapProps {
  hierarchies: HierarchyNode[];
  onSelectNode?: (node: HierarchyNode) => void;
  className?: string;
}

export function MappingCoverageHeatmap({
  hierarchies,
  onSelectNode,
  className,
}: MappingCoverageHeatmapProps) {
  const [showUnmappedOnly, setShowUnmappedOnly] = useState(false);

  const stats = useMemo(() => calculateStats(hierarchies), [hierarchies]);

  return (
    <div className={cn("flex flex-col h-full", className)}>
      {/* Header with stats */}
      <div className="p-4 border-b space-y-4">
        <div className="flex items-center justify-between">
          <h3 className="font-semibold">Mapping Coverage</h3>
          <Badge
            variant="outline"
            className={cn(
              "text-sm font-medium",
              stats.coveragePercentage >= 90
                ? "border-green-500 text-green-700"
                : stats.coveragePercentage >= 70
                ? "border-yellow-500 text-yellow-700"
                : "border-red-500 text-red-700"
            )}
          >
            {stats.coveragePercentage}%
          </Badge>
        </div>

        {/* Progress bar */}
        <div className="space-y-2">
          <Progress value={stats.coveragePercentage} className="h-2" />
          <div className="flex justify-between text-xs text-muted-foreground">
            <span>{stats.mappedNodes + stats.formulaNodes} mapped</span>
            <span>{stats.totalNodes} total</span>
          </div>
        </div>

        {/* Quick stats */}
        <div className="grid grid-cols-2 gap-2">
          <div className="flex items-center gap-2 text-sm">
            <CheckCircle2 className="h-4 w-4 text-green-600" />
            <span className="text-muted-foreground">Mapped:</span>
            <span className="font-medium">{stats.mappedNodes}</span>
          </div>
          <div className="flex items-center gap-2 text-sm">
            <Database className="h-4 w-4 text-blue-600" />
            <span className="text-muted-foreground">Formulas:</span>
            <span className="font-medium">{stats.formulaNodes}</span>
          </div>
          <div className="flex items-center gap-2 text-sm">
            <MinusCircle className="h-4 w-4 text-yellow-600" />
            <span className="text-muted-foreground">Partial:</span>
            <span className="font-medium">{stats.partiallyMappedNodes}</span>
          </div>
          <div className="flex items-center gap-2 text-sm">
            <AlertCircle className="h-4 w-4 text-red-600" />
            <span className="text-muted-foreground">Unmapped:</span>
            <span className="font-medium">{stats.unmappedLeafNodes}</span>
          </div>
        </div>

        {/* Filter toggle */}
        <div className="flex items-center justify-between pt-2 border-t">
          <Label htmlFor="unmapped-filter" className="flex items-center gap-2 text-sm">
            <Filter className="h-4 w-4" />
            Show unmapped only
          </Label>
          <Switch
            id="unmapped-filter"
            checked={showUnmappedOnly}
            onCheckedChange={setShowUnmappedOnly}
          />
        </div>
      </div>

      {/* Heatmap tree */}
      <ScrollArea className="flex-1">
        <div className="p-2">
          {hierarchies.length > 0 ? (
            hierarchies.map((node) => (
              <HeatmapNode
                key={node.id}
                node={node}
                depth={0}
                showUnmappedOnly={showUnmappedOnly}
                onSelect={onSelectNode}
              />
            ))
          ) : (
            <div className="text-center py-8 text-muted-foreground">
              No hierarchies to display
            </div>
          )}
        </div>
      </ScrollArea>
    </div>
  );
}
