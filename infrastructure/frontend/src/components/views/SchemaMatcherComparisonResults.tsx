import { useState, useEffect, useMemo, useCallback } from "react";
import {
  MagnifyingGlass,
  CaretDown,
  CaretRight,
  CheckCircle,
  WarningCircle,
  MinusCircle,
  PlusCircle,
  FileCode,
  Database,
  Table,
  Eye,
  FunnelSimple,
  TreeStructure,
  CircleNotch,
  GitBranch,
  ArrowRight,
} from "@phosphor-icons/react";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { apiService } from "@/lib/api-service";
import { toast } from "sonner";
import type { SchemaComparison, Difference } from "@/types";
import ReactFlow, {
  Node,
  Edge,
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  MarkerType,
  Position,
} from "reactflow";
import "reactflow/dist/style.css";
import dagre from "dagre";

interface ComparisonResultsProps {
  comparison: SchemaComparison;
  onGenerateScript: () => void;
}

interface ResourceNode {
  id?: string;
  name: string;
  type: string;
  status: string;
  children?: ResourceNode[];
  details?: any;
}

export function ComparisonResults({
  comparison,
  onGenerateScript,
}: ComparisonResultsProps) {
  const [searchQuery, setSearchQuery] = useState("");
  const [filterStatus, setFilterStatus] = useState<string>("all");
  const [filterType, setFilterType] = useState<string>("all");
  const [expandedNodes, setExpandedNodes] = useState<Set<string>>(new Set());
  const [selectedResource, setSelectedResource] = useState<any>(null);
  const [viewMode, setViewMode] = useState<"tree" | "list">("tree");
  const [resources, setResources] = useState<any[]>([]);
  const [loadingResources, setLoadingResources] = useState(false);
  const [loadingDetails, setLoadingDetails] = useState(false);
  const [resourceColumns, setResourceColumns] = useState<any>(null);
  const [resourceDdl, setResourceDdl] = useState<any>(null);
  const [resourceDependencies, setResourceDependencies] = useState<any>(null);
  const [dependencyViewMode, setDependencyViewMode] = useState<
    "list" | "graph"
  >("graph");
  const [detailsPanelHeight, setDetailsPanelHeight] = useState(350);
  const [clickedDependency, setClickedDependency] = useState<any>(null);

  // React Flow states for dependency graph
  const [depNodes, setDepNodes, onDepNodesChange] = useNodesState([]);
  const [depEdges, setDepEdges, onDepEdgesChange] = useEdgesState([]);

  const results = comparison.results;
  const jobId = comparison.id || comparison.jobId;

  // Load resources on mount
  useEffect(() => {
    if (jobId) {
      loadResources();
    }
  }, [jobId]);

  const loadResources = async () => {
    setLoadingResources(true);
    try {
      const data = await apiService.getJobResources(jobId, {
        status: filterStatus !== "all" ? filterStatus : undefined,
        type: filterType !== "all" ? filterType : undefined,
      });
      console.log("Loaded resources data:", data);
      setResources(data.resources || []);
    } catch (error: any) {
      console.error("Failed to load resources:", error);
      toast.error("Failed to load resources", {
        description: error.message,
      });
    } finally {
      setLoadingResources(false);
    }
  };

  // Reload when filters change
  useEffect(() => {
    if (jobId) {
      loadResources();
    }
  }, [jobId, filterStatus, filterType]);

  const loadResourceDetails = async (resourceId: string) => {
    if (!resourceId) {
      console.error("No resourceId provided");
      return;
    }

    console.log("Loading resource details for:", resourceId);
    setLoadingDetails(true);
    setClickedDependency(null); // Clear clicked dependency

    try {
      const [details, columns, ddl, dependenciesResponse] = await Promise.all([
        apiService.getResourceDetails(jobId, resourceId),
        apiService.getResourceColumns(jobId, resourceId),
        apiService.getResourceDdl(jobId, resourceId),
        apiService.getResourceDependencies(jobId, resourceId),
      ]);

      console.log("Raw dependencies response:", dependenciesResponse);

      // Handle new API format (edges/nodes/dependencies) or old format (outgoing/incoming)
      let dependencies = dependenciesResponse;
      if (dependenciesResponse.edges && dependenciesResponse.dependencies) {
        // New format: convert to expected format
        const currentResourceDeps = dependenciesResponse.dependencies || [];
        dependencies = {
          resourceId: resourceId,
          outgoing: currentResourceDeps
            .filter((dep: any) => dep.sourceResourceId === resourceId)
            .map((dep: any) => ({
              id: dep.id,
              dependencyType: dep.dependencyType,
              constraintName: dep.constraintName,
              target: dep.targetResource,
              metadata: dep.metadata,
            })),
          incoming: currentResourceDeps
            .filter((dep: any) => dep.targetResourceId === resourceId)
            .map((dep: any) => ({
              id: dep.id,
              dependencyType: dep.dependencyType,
              constraintName: dep.constraintName,
              source: dep.sourceResource,
              metadata: dep.metadata,
            })),
        };
        console.log("Converted dependencies:", dependencies);
      }

      console.log("Loaded dependencies:", dependencies);

      setSelectedResource(details);
      setResourceColumns(columns);
      setResourceDdl(ddl);
      setResourceDependencies(dependencies);

      // Build dependency graph
      if (
        dependencies &&
        (dependencies.outgoing?.length > 0 || dependencies.incoming?.length > 0)
      ) {
        buildDependencyGraph(details, dependencies);
      } else {
        console.log("No dependencies found");
        setDepNodes([]);
        setDepEdges([]);
      }
    } catch (error: any) {
      console.error("Failed to load resource details:", error);
      toast.error("Failed to load resource details", {
        description: error.message,
      });
    } finally {
      setLoadingDetails(false);
    }
  };

  // Handle dependency node click
  const handleDependencyNodeClick = useCallback(
    (nodeData: any) => {
      if (!nodeData || !nodeData.id) return;

      console.log("Dependency node clicked:", nodeData);

      // If center node, do nothing
      if (nodeData.id === selectedResource?.id) {
        return;
      }

      // Toggle clicked dependency
      if (clickedDependency?.id === nodeData.id) {
        setClickedDependency(null);
      } else {
        setClickedDependency(nodeData);
      }
    },
    [clickedDependency, selectedResource]
  );

  // Custom node component for dependency graph
  const DependencyNode = useCallback(
    ({ data }: any) => {
      const getIcon = (type: string) => {
        const iconMap: Record<string, any> = {
          TABLE: Table,
          VIEW: Eye,
          FUNCTION: FileCode,
          PROCEDURE: FileCode,
        };
        return iconMap[type] || Table;
      };

      const Icon = getIcon(data.resourceType);
      const isCenter = data.id === selectedResource?.id;
      const isClicked = data.id === clickedDependency?.id;

      return (
        <div
          className={`px-4 py-3 rounded-lg border-2 bg-card shadow-lg cursor-pointer hover:shadow-xl hover:scale-105 transition-all min-w-40 ${
            isCenter
              ? "border-primary ring-2 ring-primary/30 bg-primary/10 scale-110"
              : isClicked
              ? "border-yellow-500 ring-2 ring-yellow-500/30 bg-yellow-500/10"
              : "border-border hover:border-primary/50"
          }`}
          onClick={(e) => {
            e.stopPropagation();
            if (data.id) {
              console.log("Node clicked:", data.resourceName, data.id);
              handleDependencyNodeClick(data);
            }
          }}
        >
          <div className="flex items-center gap-2 mb-1">
            <Icon
              className={`h-4 w-4 ${
                isCenter
                  ? "text-primary"
                  : isClicked
                  ? "text-yellow-600"
                  : "text-muted-foreground"
              }`}
              weight="fill"
            />
            <span
              className={`font-semibold text-sm ${
                isCenter
                  ? "text-primary"
                  : isClicked
                  ? "text-yellow-600"
                  : "text-foreground"
              }`}
            >
              {data.resourceName}
            </span>
          </div>
          <div className="text-xs text-muted-foreground">
            {data.resourceType}
          </div>
          {!isCenter && (
            <div className="text-xs text-muted-foreground mt-1 opacity-70">
              {isClicked ? "Selected" : "Click to view"}
            </div>
          )}
          {isCenter && (
            <div className="text-xs text-primary mt-1 font-medium">Current</div>
          )}
        </div>
      );
    },
    [selectedResource, clickedDependency, handleDependencyNodeClick]
  );

  const nodeTypes = useMemo(
    () => ({
      dependencyNode: DependencyNode,
    }),
    [DependencyNode]
  );

  // Build dependency graph visualization
  const buildDependencyGraph = useCallback(
    (currentResource: any, dependencies: any) => {
      console.log(
        "Building dependency graph for:",
        currentResource.resourceName,
        "ID:",
        currentResource.id,
        dependencies
      );

      // Validate current resource
      if (!currentResource || !currentResource.id) {
        console.error("Invalid current resource:", currentResource);
        setDepNodes([]);
        setDepEdges([]);
        return;
      }

      if (
        !dependencies ||
        (!dependencies.outgoing?.length && !dependencies.incoming?.length)
      ) {
        console.log("No dependencies found for resource");
        setDepNodes([]);
        setDepEdges([]);
        return;
      }

      console.log("Outgoing dependencies:", dependencies.outgoing?.length || 0);
      console.log("Incoming dependencies:", dependencies.incoming?.length || 0);

      const nodes: Node[] = [];
      const edges: Edge[] = [];
      const nodeMap = new Map<string, any>();

      // Add center node (current resource)
      nodeMap.set(currentResource.id, {
        id: currentResource.id,
        ...currentResource,
        level: 0,
      });

      // Add outgoing dependencies (what this depends on) - level -1
      dependencies.outgoing?.forEach((dep: any, idx: number) => {
        const targetId = dep.target?.id || dep.targetResource?.id;
        const targetData = dep.target || dep.targetResource;

        if (targetId && targetData && !nodeMap.has(targetId)) {
          nodeMap.set(targetId, {
            id: targetId,
            ...targetData,
            level: -1,
          });
        }

        // Only add edge if both source and target nodes exist
        if (targetId && currentResource.id && nodeMap.has(targetId)) {
          edges.push({
            id: `edge-out-${idx}`,
            source: currentResource.id,
            target: targetId,
            type: "smoothstep",
            animated: true,
            style: { stroke: "#3b82f6", strokeWidth: 2 },
            markerEnd: {
              type: MarkerType.ArrowClosed,
              color: "#3b82f6",
            },
            label: dep.constraintName?.substring(0, 20),
            labelStyle: { fontSize: 9, fill: "#6b7280", fontWeight: 500 },
            labelBgStyle: { fill: "#ffffff", fillOpacity: 0.8 },
          });
        } else {
          console.warn(
            `Skipping edge creation - missing node. Source: ${currentResource.id}, Target: ${targetId}`
          );
        }
      });

      // Add incoming dependencies (what depends on this) - level 1
      dependencies.incoming?.forEach((dep: any, idx: number) => {
        const sourceId = dep.source?.id || dep.sourceResource?.id;
        const sourceData = dep.source || dep.sourceResource;

        if (sourceId && sourceData && !nodeMap.has(sourceId)) {
          nodeMap.set(sourceId, {
            id: sourceId,
            ...sourceData,
            level: 1,
          });
        }

        // Only add edge if both source and target nodes exist
        if (sourceId && currentResource.id && nodeMap.has(sourceId)) {
          edges.push({
            id: `edge-in-${idx}`,
            source: sourceId,
            target: currentResource.id,
            type: "smoothstep",
            animated: true,
            style: { stroke: "#22c55e", strokeWidth: 2 },
            markerEnd: {
              type: MarkerType.ArrowClosed,
              color: "#22c55e",
            },
            label: dep.constraintName?.substring(0, 20),
            labelStyle: { fontSize: 9, fill: "#6b7280", fontWeight: 500 },
            labelBgStyle: { fill: "#ffffff", fillOpacity: 0.8 },
          });
        } else {
          console.warn(
            `Skipping edge creation - missing node. Source: ${sourceId}, Target: ${currentResource.id}`
          );
        }
      });

      console.log("Created nodes:", nodeMap.size, "edges:", edges.length);
      console.log("Node IDs in map:", Array.from(nodeMap.keys()));
      console.log(
        "Edges created:",
        edges.map((e) => ({ id: e.id, source: e.source, target: e.target }))
      );

      if (nodeMap.size === 0) {
        console.warn("No nodes to display in dependency graph");
        setDepNodes([]);
        setDepEdges([]);
        return;
      }

      // Use dagre for layout
      const dagreGraph = new dagre.graphlib.Graph();
      dagreGraph.setDefaultEdgeLabel(() => ({}));
      dagreGraph.setGraph({
        rankdir: "TB",
        nodesep: 120,
        ranksep: 150,
        marginx: 50,
        marginy: 50,
      });

      // Add nodes to dagre
      Array.from(nodeMap.values()).forEach((node) => {
        dagreGraph.setNode(node.id, { width: 200, height: 80 });
      });

      // Add edges to dagre
      edges.forEach((edge) => {
        dagreGraph.setEdge(edge.source, edge.target);
      });

      // Calculate layout
      dagre.layout(dagreGraph);

      // Create React Flow nodes with positions
      const flowNodes: Node[] = Array.from(nodeMap.values()).map((node) => {
        const nodeWithPosition = dagreGraph.node(node.id);

        return {
          id: node.id,
          type: "dependencyNode",
          position: {
            x: nodeWithPosition.x - nodeWithPosition.width / 2,
            y: nodeWithPosition.y - nodeWithPosition.height / 2,
          },
          data: node,
          sourcePosition: Position.Bottom,
          targetPosition: Position.Top,
        };
      });

      console.log("Final flow nodes:", flowNodes.length);
      setDepNodes(flowNodes);
      setDepEdges(edges);
    },
    [selectedResource, setDepNodes, setDepEdges]
  );

  // Build hierarchical tree structure from loaded resources
  const buildHierarchy = (): ResourceNode[] => {
    const hierarchy: ResourceNode[] = [];
    const resourcesByType: Record<string, any[]> = {};

    // Group by resource type
    resources.forEach((resource: any) => {
      const type = resource.resourceType || "TABLE";
      if (!resourcesByType[type]) {
        resourcesByType[type] = [];
      }
      resourcesByType[type].push(resource);
    });

    // Create tree nodes
    Object.entries(resourcesByType).forEach(([type, items]) => {
      hierarchy.push({
        name: type,
        type: "category",
        status: "category",
        children: items.map((r) => ({
          id: r.id,
          name: r.resourceName,
          type: type,
          status: r.status,
          details: r,
        })),
      });
    });

    return hierarchy;
  };

  const hierarchyData = buildHierarchy();

  const toggleNode = (path: string) => {
    const newExpanded = new Set(expandedNodes);
    if (newExpanded.has(path)) {
      newExpanded.delete(path);
    } else {
      newExpanded.add(path);
    }
    setExpandedNodes(newExpanded);
  };

  const getStatusIcon = (status: string) => {
    switch (status) {
      case "MATCHED":
      case "matched":
        return <CheckCircle className="h-4 w-4 text-green-500" weight="fill" />;
      case "MODIFIED":
      case "modified":
      case "SCHEMA_MODIFIED":
        return (
          <WarningCircle className="h-4 w-4 text-yellow-500" weight="fill" />
        );
      case "EXISTS_IN_SOURCE_ONLY":
      case "source_only":
        return <MinusCircle className="h-4 w-4 text-red-500" weight="fill" />;
      case "EXISTS_IN_TARGET_ONLY":
      case "target_only":
        return <PlusCircle className="h-4 w-4 text-blue-500" weight="fill" />;
      case "DDL_NOT_FOUND":
        return (
          <WarningCircle className="h-4 w-4 text-gray-500" weight="fill" />
        );
      default:
        return <Table className="h-4 w-4 text-muted-foreground" />;
    }
  };

  const getStatusBadge = (status: string) => {
    const statusMap: Record<string, { color: string; label: string }> = {
      MATCHED: {
        color: "bg-green-500/10 text-green-500 border-green-500/20",
        label: "Match",
      },
      MODIFIED: {
        color: "bg-yellow-500/10 text-yellow-500 border-yellow-500/20",
        label: "Modified",
      },
      SCHEMA_MODIFIED: {
        color: "bg-yellow-500/10 text-yellow-500 border-yellow-500/20",
        label: "Schema Modified",
      },
      EXISTS_IN_SOURCE_ONLY: {
        color: "bg-red-500/10 text-red-500 border-red-500/20",
        label: "Source Only",
      },
      EXISTS_IN_TARGET_ONLY: {
        color: "bg-blue-500/10 text-blue-500 border-blue-500/20",
        label: "Target Only",
      },
      DDL_NOT_FOUND: {
        color: "bg-gray-500/10 text-gray-500 border-gray-500/20",
        label: "DDL Not Found",
      },
    };

    const config = statusMap[status] || {
      color: "bg-gray-500/10 text-gray-500 border-gray-500/20",
      label: status,
    };
    return (
      <Badge variant="outline" className={config.color}>
        {config.label}
      </Badge>
    );
  };

  // Render diff view with lazy-loaded DDL
  const renderDiffView = () => {
    if (!resourceDdl) {
      return (
        <div className="text-center text-muted-foreground py-8">
          Select a resource to view DDL comparison
        </div>
      );
    }

    const source = resourceDdl.sourceDdl || "";
    const target = resourceDdl.targetDdl || "";
    const sourceLines = source.split("\n");
    const targetLines = target.split("\n");

    return (
      <div className="grid grid-cols-2 gap-0 border rounded-lg overflow-hidden font-mono text-xs">
        {/* Source Schema */}
        <div className="bg-red-500/5 border-r">
          <div className="bg-red-500/20 text-red-700 dark:text-red-300 px-3 py-2 font-semibold border-b sticky top-0">
            Source Schema
          </div>
          <ScrollArea className="h-[500px]">
            <div className="p-0">
              {sourceLines.map((line, idx) => (
                <div key={`source-${idx}`} className="flex hover:bg-red-500/10">
                  <div className="w-12 text-center text-muted-foreground bg-red-500/5 border-r py-1 select-none shrink-0">
                    {idx + 1}
                  </div>
                  <pre className="flex-1 px-3 py-1 overflow-x-auto whitespace-pre">
                    {line || " "}
                  </pre>
                </div>
              ))}
              {sourceLines.length === 0 && (
                <div className="text-center text-muted-foreground py-8 italic">
                  No source definition
                </div>
              )}
            </div>
          </ScrollArea>
        </div>

        {/* Target Schema */}
        <div className="bg-green-500/5">
          <div className="bg-green-500/20 text-green-700 dark:text-green-300 px-3 py-2 font-semibold border-b sticky top-0">
            Target Schema
          </div>
          <ScrollArea className="h-[500px]">
            <div className="p-0">
              {targetLines.map((line, idx) => (
                <div
                  key={`target-${idx}`}
                  className="flex hover:bg-green-500/10"
                >
                  <div className="w-12 text-center text-muted-foreground bg-green-500/5 border-r py-1 select-none shrink-0">
                    {idx + 1}
                  </div>
                  <pre className="flex-1 px-3 py-1 overflow-x-auto whitespace-pre">
                    {line || " "}
                  </pre>
                </div>
              ))}
              {targetLines.length === 0 && (
                <div className="text-center text-muted-foreground py-8 italic">
                  No target definition
                </div>
              )}
            </div>
          </ScrollArea>
        </div>
      </div>
    );
  };

  // Render tree node with lazy loading on click
  const renderTreeNode = (
    node: ResourceNode,
    path: string,
    level: number = 0
  ) => {
    const nodePath = `${path}/${node.name}`;
    const isExpanded = expandedNodes.has(nodePath);
    const isCategory = node.type === "category";

    const matchesFilter =
      filterStatus === "all" ||
      node.status?.toLowerCase() === filterStatus.toLowerCase();
    const matchesType = filterType === "all" || node.type === filterType;
    const matchesSearch =
      searchQuery === "" ||
      node.name.toLowerCase().includes(searchQuery.toLowerCase());

    if (!isCategory && (!matchesFilter || !matchesType || !matchesSearch)) {
      return null;
    }

    return (
      <div key={nodePath}>
        <div
          className={`flex items-center gap-2 px-3 py-2 hover:bg-accent rounded-md cursor-pointer transition-colors ${
            selectedResource?.id === node.id ? "bg-accent" : ""
          }`}
          style={{ paddingLeft: `${level * 1.5 + 0.75}rem` }}
          onClick={() => {
            if (isCategory) {
              toggleNode(nodePath);
            } else {
              // Lazy load resource details
              if (node.id) {
                loadResourceDetails(node.id);
              }
              if (!isExpanded) toggleNode(nodePath);
            }
          }}
        >
          {node.children &&
            node.children.length > 0 &&
            (isExpanded ? (
              <CaretDown className="h-4 w-4 text-muted-foreground shrink-0" />
            ) : (
              <CaretRight className="h-4 w-4 text-muted-foreground shrink-0" />
            ))}

          {!isCategory && getStatusIcon(node.status)}

          {isCategory ? (
            <Database className="h-4 w-4 text-primary shrink-0" weight="fill" />
          ) : (
            <Table className="h-4 w-4 text-muted-foreground shrink-0" />
          )}

          <span
            className={`flex-1 truncate ${
              isCategory ? "font-semibold text-sm" : "text-sm"
            }`}
          >
            {node.name}
          </span>

          {!isCategory && (
            <div className="shrink-0">{getStatusBadge(node.status)}</div>
          )}

          {isCategory && node.children && (
            <Badge variant="secondary" className="shrink-0 text-xs">
              {node.children.length}
            </Badge>
          )}
        </div>

        {isExpanded && node.children && (
          <div className="space-y-0.5">
            {node.children.map((child) =>
              renderTreeNode(child, nodePath, level + 1)
            )}
          </div>
        )}
      </div>
    );
  };

  if (!results) return null;

  return (
    <div className="flex flex-col h-full gap-4 overflow-hidden">
      {/* Summary Cards */}
      <div className="grid grid-cols-2 md:grid-cols-6 gap-3 shrink-0">
        <Card className="p-3 text-center">
          <div className="text-xl font-bold text-foreground">
            {results.summary?.totalResources || resources.length || 0}
          </div>
          <div className="text-xs text-muted-foreground mt-1">Total</div>
        </Card>

        <Card className="p-3 text-center bg-green-500/5">
          <div className="text-xl font-bold text-green-500">
            {results.summary?.matched || 0}
          </div>
          <div className="text-xs text-muted-foreground mt-1">Matched</div>
        </Card>

        <Card className="p-3 text-center bg-yellow-500/5">
          <div className="text-xl font-bold text-yellow-500">
            {results.summary?.modified || 0}
          </div>
          <div className="text-xs text-muted-foreground mt-1">Modified</div>
        </Card>

        <Card className="p-3 text-center bg-red-500/5">
          <div className="text-xl font-bold text-red-500">
            {results.summary?.sourceOnly || 0}
          </div>
          <div className="text-xs text-muted-foreground mt-1">Source Only</div>
        </Card>

        <Card className="p-3 text-center bg-blue-500/5">
          <div className="text-xl font-bold text-blue-500">
            {results.summary?.targetOnly || 0}
          </div>
          <div className="text-xs text-muted-foreground mt-1">Target Only</div>
        </Card>

        <Card className="p-3 text-center bg-purple-500/5">
          <div className="text-xl font-bold text-purple-500">
            {results.summary?.ddlNotFound || 0}
          </div>
          <div className="text-xs text-muted-foreground mt-1">DDL Missing</div>
        </Card>
      </div>

      {/* Toolbar */}
      <Card className="p-3 shrink-0">
        <div className="flex items-center gap-3 flex-wrap">
          <div className="relative flex-1 min-w-[200px]">
            <MagnifyingGlass className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              placeholder="Search resources..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-10 h-9"
            />
          </div>

          <Select value={filterStatus} onValueChange={setFilterStatus}>
            <SelectTrigger className="w-36 h-9">
              <FunnelSimple className="h-4 w-4 mr-2" />
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Status</SelectItem>
              <SelectItem value="matched">Matched</SelectItem>
              <SelectItem value="modified">Modified</SelectItem>
              <SelectItem value="exists_in_source_only">Source Only</SelectItem>
              <SelectItem value="exists_in_target_only">Target Only</SelectItem>
            </SelectContent>
          </Select>

          <Select value={filterType} onValueChange={setFilterType}>
            <SelectTrigger className="w-32 h-9">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Types</SelectItem>
              <SelectItem value="TABLE">Tables</SelectItem>
              <SelectItem value="VIEW">Views</SelectItem>
              <SelectItem value="PROCEDURE">Procedures</SelectItem>
              <SelectItem value="FUNCTION">Functions</SelectItem>
            </SelectContent>
          </Select>

          <div className="flex gap-1 border rounded-md p-1">
            <Button
              variant={viewMode === "tree" ? "secondary" : "ghost"}
              size="sm"
              className="h-7 px-2"
              onClick={() => setViewMode("tree")}
            >
              <TreeStructure className="h-4 w-4" />
            </Button>
            <Button
              variant={viewMode === "list" ? "secondary" : "ghost"}
              size="sm"
              className="h-7 px-2"
              onClick={() => setViewMode("list")}
            >
              <Eye className="h-4 w-4" />
            </Button>
          </div>

          <Button onClick={onGenerateScript} size="sm" className="h-9">
            <FileCode className="h-4 w-4 mr-2" />
            Generate Script
          </Button>
        </div>
      </Card>

      {/* Main Content */}
      <div className="flex-1 flex gap-4 min-h-0">
        {/* Left Panel - Tree/List View - Fixed Width */}
        <Card className="w-80 shrink-0 p-4 flex flex-col overflow-hidden">
          <h3 className="font-semibold mb-3 flex items-center gap-2">
            <Database className="h-5 w-5 text-primary" weight="fill" />
            Resources
            <Badge variant="secondary" className="ml-auto">
              {resources.length || 0}
            </Badge>
          </h3>
          <ScrollArea className="flex-1 -mr-4 pr-4">
            {loadingResources ? (
              <div className="flex items-center justify-center py-12">
                <div className="text-center">
                  <CircleNotch className="h-6 w-6 animate-spin text-primary mx-auto mb-2" />
                  <p className="text-xs text-muted-foreground">
                    Loading resources...
                  </p>
                </div>
              </div>
            ) : (
              <div className="space-y-1">
                {hierarchyData.map((node) => renderTreeNode(node, "", 0))}
                {hierarchyData.length === 0 && (
                  <div className="text-center py-12 text-muted-foreground text-sm">
                    No resources found
                  </div>
                )}
              </div>
            )}
          </ScrollArea>
        </Card>

        {/* Right Panel - Detail View - Flexible Width */}
        <Card className="flex-1 p-4 flex flex-col overflow-hidden">
          {loadingDetails ? (
            <div className="flex-1 flex items-center justify-center">
              <div className="text-center">
                <CircleNotch className="h-8 w-8 animate-spin text-primary mx-auto mb-2" />
                <p className="text-sm text-muted-foreground">
                  Loading details...
                </p>
              </div>
            </div>
          ) : selectedResource ? (
            <>
              <div className="flex items-center justify-between mb-4 pb-3 border-b">
                <div className="flex items-center gap-3">
                  <Table className="h-5 w-5 text-primary" weight="fill" />
                  <div>
                    <h3 className="font-semibold">
                      {selectedResource.resourceName}
                    </h3>
                    <p className="text-xs text-muted-foreground capitalize">
                      {selectedResource.resourceType} •{" "}
                      {selectedResource.database}.{selectedResource.schema}
                    </p>
                  </div>
                </div>
                {getStatusBadge(selectedResource.status)}
              </div>

              <Tabs
                defaultValue="diff"
                className="flex-1 flex flex-col min-h-0"
              >
                <TabsList className="mb-3">
                  <TabsTrigger value="diff">Schema Diff</TabsTrigger>
                  <TabsTrigger value="columns">Columns</TabsTrigger>
                  <TabsTrigger value="dependencies">Dependencies</TabsTrigger>
                  <TabsTrigger value="details">Details</TabsTrigger>
                </TabsList>

                <TabsContent value="diff" className="flex-1 mt-0 min-h-0">
                  {renderDiffView()}
                </TabsContent>

                <TabsContent value="columns" className="flex-1 mt-0">
                  <ScrollArea className="h-[500px]">
                    {resourceColumns?.grouped ? (
                      <div className="space-y-3">
                        {/* Matched Columns */}
                        {resourceColumns.grouped.matched?.length > 0 && (
                          <div>
                            <div className="text-sm font-medium mb-2 flex items-center gap-2">
                              <CheckCircle
                                className="h-4 w-4 text-green-500"
                                weight="fill"
                              />
                              Matched Columns (
                              {resourceColumns.grouped.matched.length})
                            </div>
                            <div className="space-y-1">
                              {resourceColumns.grouped.matched.map(
                                (col: any) => (
                                  <div
                                    key={col.columnName}
                                    className="text-sm bg-green-500/5 border border-green-500/20 rounded px-3 py-2"
                                  >
                                    <div className="font-mono">
                                      {col.columnName}
                                    </div>
                                    <div className="text-xs text-muted-foreground mt-1">
                                      {col.sourceType || col.targetType}
                                    </div>
                                  </div>
                                )
                              )}
                            </div>
                          </div>
                        )}

                        {/* Modified Columns */}
                        {resourceColumns.grouped.modified?.length > 0 && (
                          <div>
                            <div className="text-sm font-medium mb-2 flex items-center gap-2">
                              <WarningCircle
                                className="h-4 w-4 text-yellow-500"
                                weight="fill"
                              />
                              Modified Columns (
                              {resourceColumns.grouped.modified.length})
                            </div>
                            <div className="space-y-1">
                              {resourceColumns.grouped.modified.map(
                                (col: any) => (
                                  <div
                                    key={col.columnName}
                                    className="text-sm bg-yellow-500/5 border border-yellow-500/20 rounded px-3 py-2"
                                  >
                                    <div className="font-mono">
                                      {col.columnName}
                                    </div>
                                    <div className="text-xs text-muted-foreground mt-1 grid grid-cols-2 gap-2">
                                      <div>Source: {col.sourceType}</div>
                                      <div>Target: {col.targetType}</div>
                                    </div>
                                  </div>
                                )
                              )}
                            </div>
                          </div>
                        )}

                        {/* Source Only */}
                        {resourceColumns.grouped.sourceOnly?.length > 0 && (
                          <div>
                            <div className="text-sm font-medium mb-2 flex items-center gap-2">
                              <MinusCircle
                                className="h-4 w-4 text-red-500"
                                weight="fill"
                              />
                              Source Only (
                              {resourceColumns.grouped.sourceOnly.length})
                            </div>
                            <div className="space-y-1">
                              {resourceColumns.grouped.sourceOnly.map(
                                (col: any) => (
                                  <div
                                    key={col.columnName}
                                    className="text-sm bg-red-500/5 border border-red-500/20 rounded px-3 py-2"
                                  >
                                    <div className="font-mono">
                                      {col.columnName}
                                    </div>
                                    <div className="text-xs text-muted-foreground mt-1">
                                      {col.sourceType}
                                    </div>
                                  </div>
                                )
                              )}
                            </div>
                          </div>
                        )}

                        {/* Target Only */}
                        {resourceColumns.grouped.targetOnly?.length > 0 && (
                          <div>
                            <div className="text-sm font-medium mb-2 flex items-center gap-2">
                              <PlusCircle
                                className="h-4 w-4 text-blue-500"
                                weight="fill"
                              />
                              Target Only (
                              {resourceColumns.grouped.targetOnly.length})
                            </div>
                            <div className="space-y-1">
                              {resourceColumns.grouped.targetOnly.map(
                                (col: any) => (
                                  <div
                                    key={col.columnName}
                                    className="text-sm bg-blue-500/5 border border-blue-500/20 rounded px-3 py-2"
                                  >
                                    <div className="font-mono">
                                      {col.columnName}
                                    </div>
                                    <div className="text-xs text-muted-foreground mt-1">
                                      {col.targetType}
                                    </div>
                                  </div>
                                )
                              )}
                            </div>
                          </div>
                        )}
                      </div>
                    ) : (
                      <div className="text-center py-12 text-muted-foreground text-sm">
                        No column comparison data available
                      </div>
                    )}
                  </ScrollArea>
                </TabsContent>

                <TabsContent
                  value="dependencies"
                  className="flex-1 mt-0 flex flex-col"
                >
                  <div className="flex items-center justify-between mb-3">
                    <div className="text-sm font-medium">
                      Dependency Lineage
                    </div>
                    <div className="flex gap-1 border rounded-md p-1">
                      <Button
                        variant={
                          dependencyViewMode === "graph" ? "secondary" : "ghost"
                        }
                        size="sm"
                        className="h-7 px-2"
                        onClick={() => setDependencyViewMode("graph")}
                      >
                        <GitBranch className="h-4 w-4" />
                      </Button>
                      <Button
                        variant={
                          dependencyViewMode === "list" ? "secondary" : "ghost"
                        }
                        size="sm"
                        className="h-7 px-2"
                        onClick={() => setDependencyViewMode("list")}
                      >
                        <TreeStructure className="h-4 w-4" />
                      </Button>
                    </div>
                  </div>

                  {dependencyViewMode === "graph" ? (
                    <div
                      className="flex-1 border rounded-lg overflow-hidden bg-muted/20"
                      style={{ height: "450px" }}
                    >
                      {loadingDetails ? (
                        <div className="flex items-center justify-center h-full">
                          <CircleNotch className="h-6 w-6 animate-spin text-primary" />
                        </div>
                      ) : depNodes.length > 0 ? (
                        <ReactFlow
                          nodes={depNodes}
                          edges={depEdges}
                          onNodesChange={onDepNodesChange}
                          onEdgesChange={onDepEdgesChange}
                          nodeTypes={nodeTypes}
                          fitView
                          minZoom={0.5}
                          maxZoom={1.5}
                          attributionPosition="bottom-right"
                        >
                          <Background />
                          <Controls showInteractive={false} />
                          <MiniMap
                            nodeColor={(node) => {
                              return node.id === selectedResource?.id
                                ? "#8b5cf6"
                                : "#9ca3af";
                            }}
                            style={{ height: 80 }}
                          />
                        </ReactFlow>
                      ) : (
                        <div className="flex items-center justify-center h-full">
                          <div className="text-center">
                            <GitBranch className="h-8 w-8 text-muted-foreground mx-auto mb-2 opacity-50" />
                            <p className="text-sm text-muted-foreground">
                              No dependencies found
                            </p>
                          </div>
                        </div>
                      )}
                    </div>
                  ) : (
                    <ScrollArea className="flex-1">
                      {loadingDetails ? (
                        <div className="flex items-center justify-center py-12">
                          <CircleNotch className="h-6 w-6 animate-spin text-primary" />
                        </div>
                      ) : resourceDependencies ? (
                        <div className="space-y-4">
                          {/* Outgoing Dependencies - What this resource depends on */}
                          <div>
                            <div className="text-sm font-medium mb-2 flex items-center gap-2">
                              <ArrowRight
                                className="h-4 w-4 text-blue-500"
                                weight="bold"
                              />
                              Depends On (
                              {resourceDependencies.outgoing?.length || 0})
                            </div>
                            {resourceDependencies.outgoing?.length > 0 ? (
                              <div className="space-y-2">
                                {resourceDependencies.outgoing.map(
                                  (dep: any) => (
                                    <div
                                      key={dep.id}
                                      className="bg-card border rounded-lg p-3 cursor-pointer hover:bg-accent transition-colors"
                                      onClick={() =>
                                        loadResourceDetails(dep.target.id)
                                      }
                                    >
                                      <div className="flex items-start justify-between">
                                        <div className="flex-1">
                                          <div className="flex items-center gap-2">
                                            <GitBranch className="h-4 w-4 text-blue-500" />
                                            <span className="font-medium text-sm">
                                              {dep.target.resourceName}
                                            </span>
                                            {getStatusBadge(dep.target.status)}
                                          </div>
                                          <p className="text-xs text-muted-foreground mt-1">
                                            {dep.target.database}.
                                            {dep.target.schema} •{" "}
                                            {dep.target.resourceType}
                                          </p>
                                          {dep.constraintName && (
                                            <p className="text-xs text-muted-foreground mt-1 font-mono">
                                              FK: {dep.constraintName}
                                            </p>
                                          )}
                                          {dep.metadata?.sourceColumns && (
                                            <p className="text-xs text-muted-foreground mt-1">
                                              {dep.metadata.sourceColumns} →{" "}
                                              {dep.metadata.targetColumns}
                                            </p>
                                          )}
                                        </div>
                                        <ArrowRight className="h-4 w-4 text-muted-foreground shrink-0 mt-1" />
                                      </div>
                                    </div>
                                  )
                                )}
                              </div>
                            ) : (
                              <p className="text-xs text-muted-foreground py-3 text-center bg-muted/30 rounded">
                                No outgoing dependencies
                              </p>
                            )}
                          </div>

                          {/* Incoming Dependencies - What depends on this resource */}
                          <div>
                            <div className="text-sm font-medium mb-2 flex items-center gap-2">
                              <ArrowRight
                                className="h-4 w-4 text-green-500 rotate-180"
                                weight="bold"
                              />
                              Depended By (
                              {resourceDependencies.incoming?.length || 0})
                            </div>
                            {resourceDependencies.incoming?.length > 0 ? (
                              <div className="space-y-2">
                                {resourceDependencies.incoming.map(
                                  (dep: any) => (
                                    <div
                                      key={dep.id}
                                      className="bg-card border rounded-lg p-3 cursor-pointer hover:bg-accent transition-colors"
                                      onClick={() =>
                                        loadResourceDetails(dep.source.id)
                                      }
                                    >
                                      <div className="flex items-start justify-between">
                                        <div className="flex-1">
                                          <div className="flex items-center gap-2">
                                            <GitBranch className="h-4 w-4 text-green-500" />
                                            <span className="font-medium text-sm">
                                              {dep.source.resourceName}
                                            </span>
                                            {getStatusBadge(dep.source.status)}
                                          </div>
                                          <p className="text-xs text-muted-foreground mt-1">
                                            {dep.source.database}.
                                            {dep.source.schema} •{" "}
                                            {dep.source.resourceType}
                                          </p>
                                          {dep.constraintName && (
                                            <p className="text-xs text-muted-foreground mt-1 font-mono">
                                              FK: {dep.constraintName}
                                            </p>
                                          )}
                                          {dep.metadata?.sourceColumns && (
                                            <p className="text-xs text-muted-foreground mt-1">
                                              {dep.metadata.sourceColumns} →{" "}
                                              {dep.metadata.targetColumns}
                                            </p>
                                          )}
                                        </div>
                                        <ArrowRight className="h-4 w-4 text-muted-foreground shrink-0 mt-1 rotate-180" />
                                      </div>
                                    </div>
                                  )
                                )}
                              </div>
                            ) : (
                              <p className="text-xs text-muted-foreground py-3 text-center bg-muted/30 rounded">
                                No incoming dependencies
                              </p>
                            )}
                          </div>
                        </div>
                      ) : (
                        <p className="text-xs text-muted-foreground text-center py-12">
                          No dependency data available
                        </p>
                      )}
                    </ScrollArea>
                  )}

                  {/* Bottom Panel - Clicked Dependency Details */}
                  {clickedDependency && dependencyViewMode === "graph" && (
                    <Card className="mt-3 p-4 border-yellow-500/50 bg-yellow-500/5">
                      <div className="flex items-start justify-between mb-3">
                        <div>
                          <h4 className="font-semibold text-sm flex items-center gap-2">
                            <Database
                              className="h-4 w-4 text-yellow-600"
                              weight="fill"
                            />
                            {clickedDependency.resourceName}
                          </h4>
                          <p className="text-xs text-muted-foreground mt-1">
                            {clickedDependency.resourceType}
                          </p>
                        </div>
                        <div className="flex items-center gap-2">
                          <Button
                            size="sm"
                            variant="outline"
                            onClick={() =>
                              loadResourceDetails(clickedDependency.id)
                            }
                          >
                            View Full Details
                          </Button>
                          <Button
                            size="sm"
                            variant="ghost"
                            onClick={() => setClickedDependency(null)}
                          >
                            ×
                          </Button>
                        </div>
                      </div>

                      <div className="grid grid-cols-2 gap-4 text-sm">
                        <div>
                          <div className="text-xs text-muted-foreground mb-1">
                            Resource ID
                          </div>
                          <div className="font-mono text-xs bg-muted px-2 py-1 rounded">
                            {clickedDependency.id}
                          </div>
                        </div>
                        <div>
                          <div className="text-xs text-muted-foreground mb-1">
                            Status
                          </div>
                          <div>{getStatusBadge(clickedDependency.status)}</div>
                        </div>
                      </div>

                      {/* Show relationship info */}
                      <div className="mt-3 pt-3 border-t">
                        <div className="text-xs font-medium text-muted-foreground mb-2">
                          Relationship with {selectedResource?.resourceName}:
                        </div>
                        <div className="text-xs">
                          {resourceDependencies?.outgoing?.find(
                            (d: any) => d.target?.id === clickedDependency.id
                          ) && (
                            <div className="flex items-center gap-2 text-blue-600">
                              <ArrowRight className="h-3 w-3" weight="bold" />
                              <span>
                                {selectedResource?.resourceName} depends on this
                                table
                              </span>
                            </div>
                          )}
                          {resourceDependencies?.incoming?.find(
                            (d: any) => d.source?.id === clickedDependency.id
                          ) && (
                            <div className="flex items-center gap-2 text-green-600">
                              <ArrowRight
                                className="h-3 w-3 rotate-180"
                                weight="bold"
                              />
                              <span>
                                This table depends on{" "}
                                {selectedResource?.resourceName}
                              </span>
                            </div>
                          )}
                        </div>
                      </div>
                    </Card>
                  )}
                </TabsContent>

                <TabsContent value="details" className="flex-1 mt-0">
                  <ScrollArea className="h-[500px]">
                    <div className="space-y-3">
                      <div>
                        <div className="text-sm font-medium text-muted-foreground mb-1">
                          Database
                        </div>
                        <div className="text-sm bg-muted px-3 py-2 rounded">
                          {selectedResource.database}
                        </div>
                      </div>
                      <div>
                        <div className="text-sm font-medium text-muted-foreground mb-1">
                          Schema
                        </div>
                        <div className="text-sm bg-muted px-3 py-2 rounded">
                          {selectedResource.schema}
                        </div>
                      </div>
                      <div>
                        <div className="text-sm font-medium text-muted-foreground mb-1">
                          Resource Type
                        </div>
                        <div className="text-sm bg-muted px-3 py-2 rounded capitalize">
                          {selectedResource.resourceType}
                        </div>
                      </div>
                      <div>
                        <div className="text-sm font-medium text-muted-foreground mb-1">
                          Status
                        </div>
                        <div className="flex gap-2">
                          {getStatusBadge(selectedResource.status)}
                        </div>
                      </div>
                      {selectedResource.differences && (
                        <div>
                          <div className="text-sm font-medium text-muted-foreground mb-1">
                            Differences
                          </div>
                          <div className="text-sm bg-muted px-3 py-2 rounded whitespace-pre-wrap">
                            {JSON.stringify(
                              selectedResource.differences,
                              null,
                              2
                            )}
                          </div>
                        </div>
                      )}
                    </div>
                  </ScrollArea>
                </TabsContent>
              </Tabs>
            </>
          ) : (
            <div className="flex-1 flex items-center justify-center text-muted-foreground">
              <div className="text-center">
                <Eye className="h-12 w-12 mx-auto mb-3 opacity-50" />
                <p className="text-sm">Select a resource to view details</p>
              </div>
            </div>
          )}
        </Card>
      </div>
    </div>
  );
}
