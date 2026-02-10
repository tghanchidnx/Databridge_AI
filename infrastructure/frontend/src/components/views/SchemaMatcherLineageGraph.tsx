import { useEffect, useState, useCallback } from "react";
import {
  GitBranch,
  MagnifyingGlass,
  FunnelSimple,
  ArrowsOut,
  ArrowsIn,
  Download,
  CircleNotch,
  Table as TableIcon,
  Eye,
  Function as FunctionIcon,
  FileCode,
  Database,
  ArrowRight,
  ArrowLeft,
  CheckCircle,
  WarningCircle,
  MinusCircle,
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
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { toast } from "sonner";
import { apiService } from "@/lib/api-service";
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

interface LineageGraphProps {
  jobId: string;
}

interface DependencyData {
  id: string;
  sourceResource: any;
  targetResource: any;
  constraintName: string;
  metadata: any;
}

const getStatusBadge = (status: string) => {
  const variants: Record<string, { icon: any; color: string; label: string }> =
    {
      matched: { icon: CheckCircle, color: "text-green-500", label: "Matched" },
      modified: {
        icon: WarningCircle,
        color: "text-yellow-500",
        label: "Modified",
      },
      exists_in_source_only: {
        icon: MinusCircle,
        color: "text-red-500",
        label: "Source Only",
      },
      exists_in_target_only: {
        icon: MinusCircle,
        color: "text-blue-500",
        label: "Target Only",
      },
    };

  const variant = variants[status] || variants.matched;
  const Icon = variant.icon;

  return (
    <Badge variant="secondary" className="gap-1">
      <Icon className={`h-3 w-3 ${variant.color}`} weight="fill" />
      <span className="text-xs">{variant.label}</span>
    </Badge>
  );
};

const getResourceIcon = (type: string) => {
  const iconMap: Record<string, any> = {
    TABLE: TableIcon,
    VIEW: Eye,
    FUNCTION: FunctionIcon,
    PROCEDURE: FileCode,
  };
  return iconMap[type] || TableIcon;
};

// Custom node component
const CustomNode = ({ data }: any) => {
  const Icon = getResourceIcon(data.resourceType);

  return (
    <div
      className={`px-4 py-3 rounded-lg border-2 bg-card shadow-lg cursor-pointer hover:shadow-xl transition-all ${
        data.selected
          ? "border-primary ring-2 ring-primary/20"
          : "border-border"
      }`}
      onClick={() => data.onClick?.(data)}
    >
      <div className="flex items-center gap-2 mb-1">
        <Icon className="h-4 w-4 text-primary" weight="fill" />
        <span className="font-semibold text-sm">{data.resourceName}</span>
      </div>
      <div className="flex items-center gap-2">
        <span className="text-xs text-muted-foreground">
          {data.resourceType}
        </span>
        {getStatusBadge(data.status)}
      </div>
      {data.dependencyCount !== undefined && (
        <div className="mt-2 pt-2 border-t text-xs text-muted-foreground">
          {data.dependencyCount}{" "}
          {data.dependencyCount === 1 ? "dependency" : "dependencies"}
        </div>
      )}
    </div>
  );
};

const nodeTypes = {
  custom: CustomNode,
};

export function SchemaMatcherLineageGraph({ jobId }: LineageGraphProps) {
  const [loading, setLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [filterType, setFilterType] = useState("all");
  const [filterStatus, setFilterStatus] = useState("all");
  const [layout, setLayout] = useState<"TB" | "LR">("TB");

  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);

  const [dependencies, setDependencies] = useState<DependencyData[]>([]);
  const [selectedNode, setSelectedNode] = useState<any>(null);
  const [selectedDependency, setSelectedDependency] =
    useState<DependencyData | null>(null);

  useEffect(() => {
    if (jobId) {
      loadDependencyGraph();
    }
  }, [jobId]);

  const loadDependencyGraph = async () => {
    setLoading(true);
    try {
      const data = await apiService.getDependencyGraph(jobId);
      console.log("Loaded dependency graph:", data);

      // Handle new API format with nodes/edges/dependencies
      const deps = data.dependencies || [];
      setDependencies(deps);

      // Build nodes and edges
      buildGraphLayout(deps);
    } catch (error: any) {
      console.error("Failed to load dependency graph:", error);
      toast.error("Failed to load dependency graph", {
        description: error.message,
      });
    } finally {
      setLoading(false);
    }
  };

  const buildGraphLayout = useCallback(
    (deps: DependencyData[]) => {
      if (!deps || deps.length === 0) {
        setNodes([]);
        setEdges([]);
        return;
      }

      // Create a directed graph
      const dagreGraph = new dagre.graphlib.Graph();
      dagreGraph.setDefaultEdgeLabel(() => ({}));
      dagreGraph.setGraph({
        rankdir: layout,
        nodesep: 100,
        ranksep: 150,
        marginx: 50,
        marginy: 50,
      });

      // Collect all unique nodes
      const nodeMap = new Map<string, any>();
      const nodeDependencyCount = new Map<string, number>();

      deps.forEach((dep) => {
        // Validate dependency has required data
        if (!dep.sourceResource?.id || !dep.targetResource?.id) {
          console.warn("Skipping invalid dependency:", dep);
          return;
        }

        if (!nodeMap.has(dep.sourceResource.id)) {
          nodeMap.set(dep.sourceResource.id, dep.sourceResource);
          nodeDependencyCount.set(dep.sourceResource.id, 0);
        }
        if (!nodeMap.has(dep.targetResource.id)) {
          nodeMap.set(dep.targetResource.id, dep.targetResource);
          nodeDependencyCount.set(dep.targetResource.id, 0);
        }

        // Count dependencies
        nodeDependencyCount.set(
          dep.sourceResource.id,
          (nodeDependencyCount.get(dep.sourceResource.id) || 0) + 1
        );
      });

      // Filter nodes based on search and filters
      let filteredNodes = Array.from(nodeMap.values());

      if (searchQuery) {
        filteredNodes = filteredNodes.filter((node) =>
          node.resourceName.toLowerCase().includes(searchQuery.toLowerCase())
        );
      }

      if (filterType !== "all") {
        filteredNodes = filteredNodes.filter(
          (node) => node.resourceType === filterType
        );
      }

      if (filterStatus !== "all") {
        filteredNodes = filteredNodes.filter(
          (node) => node.status === filterStatus
        );
      }

      const filteredNodeIds = new Set(filteredNodes.map((n) => n.id));

      // Filter dependencies to only include filtered nodes
      const filteredDeps = deps.filter(
        (dep) =>
          filteredNodeIds.has(dep.sourceResource?.id) &&
          filteredNodeIds.has(dep.targetResource?.id)
      );

      console.log(
        `Building graph: ${filteredNodes.length} nodes, ${filteredDeps.length} dependencies`
      );

      // Add nodes to dagre first
      filteredNodes.forEach((node) => {
        if (!node.id) {
          console.warn("Node without ID:", node);
          return;
        }
        dagreGraph.setNode(node.id, { width: 250, height: 100 });
      });

      // Then add edges - only between nodes that exist in dagre
      const validEdges: DependencyData[] = [];
      filteredDeps.forEach((dep) => {
        const sourceId = dep.sourceResource?.id;
        const targetId = dep.targetResource?.id;

        if (
          sourceId &&
          targetId &&
          dagreGraph.hasNode(sourceId) &&
          dagreGraph.hasNode(targetId)
        ) {
          try {
            dagreGraph.setEdge(sourceId, targetId);
            validEdges.push(dep);
          } catch (err) {
            console.warn(`Failed to set edge: ${sourceId} -> ${targetId}`, err);
          }
        } else {
          console.warn(
            `Skipping edge - node not in graph. Source: ${sourceId} (${dagreGraph.hasNode(
              sourceId
            )}), Target: ${targetId} (${dagreGraph.hasNode(targetId)})`
          );
        }
      });

      // Calculate layout
      dagre.layout(dagreGraph);

      // Create React Flow nodes
      const flowNodes: Node[] = filteredNodes.map((node) => {
        const nodeWithPosition = dagreGraph.node(node.id);

        return {
          id: node.id,
          type: "custom",
          position: {
            x: nodeWithPosition.x - nodeWithPosition.width / 2,
            y: nodeWithPosition.y - nodeWithPosition.height / 2,
          },
          data: {
            ...node,
            dependencyCount: nodeDependencyCount.get(node.id) || 0,
            selected: selectedNode?.id === node.id,
            onClick: handleNodeClick,
          },
          sourcePosition: layout === "LR" ? Position.Right : Position.Bottom,
          targetPosition: layout === "LR" ? Position.Left : Position.Top,
        };
      });

      // Create React Flow edges - only for validated edges with verified IDs
      const flowEdges: Edge[] = validEdges
        .filter((dep) => dep.sourceResource?.id && dep.targetResource?.id)
        .map((dep, idx) => ({
          id: `edge-${dep.id || idx}`,
          source: dep.sourceResource!.id,
          target: dep.targetResource!.id,
          type: "smoothstep",
          animated: true,
          style: { stroke: "#8b5cf6", strokeWidth: 2 },
          markerEnd: {
            type: MarkerType.ArrowClosed,
            color: "#8b5cf6",
          },
          label: dep.constraintName?.substring(0, 15) || undefined,
          labelStyle: { fontSize: 9, fill: "#6b7280", fontWeight: 500 },
          labelBgStyle: { fill: "#ffffff", fillOpacity: 0.9 },
          data: dep,
        }));

      console.log(
        `Created ${flowNodes.length} nodes and ${flowEdges.length} valid edges`
      );
      setNodes(flowNodes);
      setEdges(flowEdges);
    },
    [layout, searchQuery, filterType, filterStatus, selectedNode]
  );

  useEffect(() => {
    if (dependencies.length > 0) {
      buildGraphLayout(dependencies);
    }
  }, [buildGraphLayout, dependencies]);

  const handleNodeClick = (nodeData: any) => {
    setSelectedNode(nodeData);

    // Find all dependencies related to this node
    const relatedDeps = dependencies.filter(
      (dep) =>
        dep.sourceResource.id === nodeData.id ||
        dep.targetResource.id === nodeData.id
    );

    console.log("Selected node dependencies:", relatedDeps);
  };

  const handleEdgeClick = (event: any, edge: any) => {
    const dep = dependencies.find(
      (d) =>
        d.sourceResource.id === edge.source &&
        d.targetResource.id === edge.target
    );
    if (dep) {
      setSelectedDependency(dep);
    }
  };

  const exportGraph = () => {
    const graphData = {
      nodes: nodes.map((n) => ({
        id: n.id,
        label: n.data.resourceName,
        type: n.data.resourceType,
      })),
      edges: edges.map((e) => ({ from: e.source, to: e.target })),
    };

    const blob = new Blob([JSON.stringify(graphData, null, 2)], {
      type: "application/json",
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `dependency-graph-${jobId}.json`;
    a.click();
    URL.revokeObjectURL(url);

    toast.success("Graph exported successfully");
  };

  return (
    <div className="flex flex-col h-full gap-4 overflow-hidden">
      {/* Header Controls */}
      <Card className="p-4 shrink-0">
        <div className="flex items-center gap-3">
          <div className="relative flex-1">
            <MagnifyingGlass className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Search resources..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-10"
            />
          </div>

          <Select value={filterStatus} onValueChange={setFilterStatus}>
            <SelectTrigger className="w-36">
              <FunnelSimple className="h-4 w-4 mr-2" />
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Status</SelectItem>
              <SelectItem value="MATCHED">Matched</SelectItem>
              <SelectItem value="MODIFIED">Modified</SelectItem>
              <SelectItem value="EXISTS_IN_SOURCE_ONLY">Source Only</SelectItem>
              <SelectItem value="EXISTS_IN_TARGET_ONLY">Target Only</SelectItem>
            </SelectContent>
          </Select>

          <Select value={filterType} onValueChange={setFilterType}>
            <SelectTrigger className="w-32">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="all">All Types</SelectItem>
              <SelectItem value="TABLE">Tables</SelectItem>
              <SelectItem value="VIEW">Views</SelectItem>
              <SelectItem value="FUNCTION">Functions</SelectItem>
              <SelectItem value="PROCEDURE">Procedures</SelectItem>
            </SelectContent>
          </Select>

          <Select
            value={layout}
            onValueChange={(val) => setLayout(val as "TB" | "LR")}
          >
            <SelectTrigger className="w-32">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="TB">Top to Bottom</SelectItem>
              <SelectItem value="LR">Left to Right</SelectItem>
            </SelectContent>
          </Select>

          <Button onClick={exportGraph} size="sm" variant="outline">
            <Download className="h-4 w-4 mr-2" />
            Export
          </Button>
        </div>
      </Card>

      {/* Main Content - Fixed Height */}
      <div className="flex-1 flex gap-4 min-h-0 overflow-hidden">
        {/* Graph Visualization */}
        <div
          className={`${
            selectedNode ? "flex-1" : "w-full"
          } relative overflow-hidden`}
        >
          <Card className="h-full p-0 relative overflow-hidden">
            {loading ? (
              <div className="flex items-center justify-center h-full">
                <div className="text-center">
                  <CircleNotch className="h-8 w-8 animate-spin text-primary mx-auto mb-2" />
                  <p className="text-sm text-muted-foreground">
                    Loading dependency graph...
                  </p>
                </div>
              </div>
            ) : nodes.length === 0 ? (
              <div className="flex items-center justify-center h-full">
                <div className="text-center">
                  <GitBranch className="h-12 w-12 text-muted-foreground mx-auto mb-3 opacity-50" />
                  <p className="text-sm text-muted-foreground">
                    No dependencies found
                  </p>
                  <p className="text-xs text-muted-foreground mt-1">
                    Tables with foreign key relationships will appear here
                  </p>
                </div>
              </div>
            ) : (
              <div style={{ width: "100%", height: "100%" }}>
                <ReactFlow
                  nodes={nodes}
                  edges={edges}
                  onNodesChange={onNodesChange}
                  onEdgesChange={onEdgesChange}
                  onEdgeClick={handleEdgeClick}
                  nodeTypes={nodeTypes}
                  fitView
                  minZoom={0.3}
                  maxZoom={2}
                  attributionPosition="bottom-left"
                >
                  <Background />
                  <Controls />
                  <MiniMap
                    nodeColor={(node) => {
                      const statusColors: Record<string, string> = {
                        MATCHED: "#22c55e",
                        MODIFIED: "#eab308",
                        EXISTS_IN_SOURCE_ONLY: "#ef4444",
                        EXISTS_IN_TARGET_ONLY: "#3b82f6",
                      };
                      return statusColors[node.data.status] || "#9ca3af";
                    }}
                    style={{ height: 100 }}
                  />
                </ReactFlow>
              </div>
            )}
          </Card>
        </div>

        {/* Details Side Panel */}
        {selectedNode && (
          <Card className="w-96 shrink-0 flex flex-col overflow-hidden">
            <div className="p-4 border-b shrink-0">
              <div className="flex items-start justify-between mb-2">
                <div className="flex items-center gap-2">
                  {(() => {
                    const Icon = getResourceIcon(selectedNode.resourceType);
                    return (
                      <Icon className="h-5 w-5 text-primary" weight="fill" />
                    );
                  })()}
                  <div>
                    <h3 className="font-semibold">
                      {selectedNode.resourceName}
                    </h3>
                    <p className="text-xs text-muted-foreground">
                      {selectedNode.resourceType}
                    </p>
                  </div>
                </div>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => setSelectedNode(null)}
                  className="h-6 w-6 p-0"
                >
                  ×
                </Button>
              </div>
              {getStatusBadge(selectedNode.status)}
            </div>

            <Tabs
              defaultValue="dependencies"
              className="flex-1 flex flex-col min-h-0"
            >
              <TabsList className="mx-4 mt-3 shrink-0">
                <TabsTrigger value="dependencies">Dependencies</TabsTrigger>
                <TabsTrigger value="details">Details</TabsTrigger>
              </TabsList>

              <TabsContent
                value="dependencies"
                className="flex-1 mt-3 overflow-hidden"
              >
                <ScrollArea className="h-full px-4 pb-4">
                  <div className="space-y-4 pr-4">
                    {/* Outgoing Dependencies */}
                    <div>
                      <div className="text-sm font-medium mb-2 flex items-center gap-2">
                        <ArrowRight
                          className="h-4 w-4 text-blue-500"
                          weight="bold"
                        />
                        Depends On (
                        {
                          dependencies.filter(
                            (d) => d.sourceResource?.id === selectedNode.id
                          ).length
                        }
                        )
                      </div>
                      {dependencies
                        .filter((d) => d.sourceResource?.id === selectedNode.id)
                        .map((dep, idx) => (
                          <Card
                            key={idx}
                            className="p-3 mb-2 cursor-pointer hover:bg-accent transition-colors"
                            onClick={() => handleNodeClick(dep.targetResource)}
                          >
                            <div className="flex items-start justify-between">
                              <div className="flex-1">
                                <div className="font-medium text-sm">
                                  {dep.targetResource.resourceName}
                                </div>
                                <div className="text-xs text-muted-foreground mt-1">
                                  {dep.targetResource.resourceType}
                                </div>
                                {dep.constraintName && (
                                  <div className="text-xs text-muted-foreground mt-1 font-mono">
                                    FK: {dep.constraintName}
                                  </div>
                                )}
                                {dep.metadata?.sourceColumns && (
                                  <div className="text-xs text-muted-foreground mt-1">
                                    {dep.metadata.sourceColumns} →{" "}
                                    {dep.metadata.targetColumns}
                                  </div>
                                )}
                              </div>
                              {getStatusBadge(dep.targetResource.status)}
                            </div>
                          </Card>
                        ))}
                      {dependencies.filter(
                        (d) => d.sourceResource?.id === selectedNode.id
                      ).length === 0 && (
                        <p className="text-xs text-muted-foreground text-center py-4 bg-muted/30 rounded">
                          No outgoing dependencies
                        </p>
                      )}
                    </div>

                    {/* Incoming Dependencies */}
                    <div>
                      <div className="text-sm font-medium mb-2 flex items-center gap-2">
                        <ArrowLeft
                          className="h-4 w-4 text-green-500"
                          weight="bold"
                        />
                        Referenced By (
                        {
                          dependencies.filter(
                            (d) => d.targetResource?.id === selectedNode.id
                          ).length
                        }
                        )
                      </div>
                      {dependencies
                        .filter((d) => d.targetResource?.id === selectedNode.id)
                        .map((dep, idx) => (
                          <Card
                            key={idx}
                            className="p-3 mb-2 cursor-pointer hover:bg-accent transition-colors"
                            onClick={() => handleNodeClick(dep.sourceResource)}
                          >
                            <div className="flex items-start justify-between">
                              <div className="flex-1">
                                <div className="font-medium text-sm">
                                  {dep.sourceResource.resourceName}
                                </div>
                                <div className="text-xs text-muted-foreground mt-1">
                                  {dep.sourceResource.resourceType}
                                </div>
                                {dep.constraintName && (
                                  <div className="text-xs text-muted-foreground mt-1 font-mono">
                                    FK: {dep.constraintName}
                                  </div>
                                )}
                                {dep.metadata?.sourceColumns && (
                                  <div className="text-xs text-muted-foreground mt-1">
                                    {dep.metadata.sourceColumns} →{" "}
                                    {dep.metadata.targetColumns}
                                  </div>
                                )}
                              </div>
                              {getStatusBadge(dep.sourceResource.status)}
                            </div>
                          </Card>
                        ))}
                      {dependencies.filter(
                        (d) => d.targetResource?.id === selectedNode.id
                      ).length === 0 && (
                        <p className="text-xs text-muted-foreground text-center py-4 bg-muted/30 rounded">
                          No incoming dependencies
                        </p>
                      )}
                    </div>
                  </div>
                </ScrollArea>
              </TabsContent>

              <TabsContent
                value="details"
                className="flex-1 mt-3 overflow-hidden"
              >
                <ScrollArea className="h-full px-4 pb-4">
                  <div className="space-y-3 pr-4">
                    <div>
                      <div className="text-xs font-medium text-muted-foreground mb-1">
                        Database
                      </div>
                      <div className="text-sm font-mono bg-muted/30 p-2 rounded">
                        {selectedNode.database || "N/A"}
                      </div>
                    </div>
                    <div>
                      <div className="text-xs font-medium text-muted-foreground mb-1">
                        Schema
                      </div>
                      <div className="text-sm font-mono bg-muted/30 p-2 rounded">
                        {selectedNode.schema || "N/A"}
                      </div>
                    </div>
                    <div>
                      <div className="text-xs font-medium text-muted-foreground mb-1">
                        Status
                      </div>
                      <div className="text-sm">
                        {getStatusBadge(selectedNode.status)}
                      </div>
                    </div>
                    <div>
                      <div className="text-xs font-medium text-muted-foreground mb-1">
                        Resource ID
                      </div>
                      <div className="text-xs font-mono bg-muted/30 p-2 rounded break-all">
                        {selectedNode.id}
                      </div>
                    </div>
                    {selectedNode.dependencyCount > 0 && (
                      <div>
                        <div className="text-xs font-medium text-muted-foreground mb-1">
                          Dependency Count
                        </div>
                        <div className="text-sm font-semibold">
                          {selectedNode.dependencyCount}
                        </div>
                      </div>
                    )}
                  </div>
                </ScrollArea>
              </TabsContent>
            </Tabs>
          </Card>
        )}
      </div>
    </div>
  );
}
