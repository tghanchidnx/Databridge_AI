import React, { useMemo, useCallback } from "react";
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
  Handle,
} from "reactflow";
import "reactflow/dist/style.css";
import dagre from "dagre";
import type { SmartHierarchyMaster } from "@/services/api/hierarchy";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import {
  CheckCircle,
  XCircle,
  Database,
  Layers,
  ChevronDown,
  ChevronRight,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";

interface EnhancedGraphViewProps {
  hierarchies: SmartHierarchyMaster[];
  selectedId: string | null;
  selectedForFormula: Set<string>;
  onSelect: (id: string) => void;
  onToggleFormulaSelection: (id: string) => void;
}

// Custom Node Component
const HierarchyNode = ({ data }: any) => {
  const [showDetails, setShowDetails] = React.useState(false);
  const { hierarchy, isSelected, isChecked, onClick, onToggleCheck } = data;

  // Handle both minimal and full hierarchy data formats
  const levelCount = (hierarchy as any).levelsCount || 0;
  const sourcesCount = (hierarchy as any).sourcesCount || 0;
  const childrenCount = (hierarchy as any).childrenCount || 0;

  return (
    <Card
      className={cn(
        "min-w-[200px] max-w-60 cursor-pointer transition-all p-0 rounded-full border border-muted-foreground/10",
        isSelected ? "ring-2 ring-primary shadow-lg" : "hover:shadow-md"
      )}
      onClick={onClick}
    >
      <Handle type="target" position={Position.Left} className="bg-primary!" />

      <div className="p-2 space-y-1.5 ">
        {/* Header */}
        <div className="flex items-start gap-1.5">
          {/* <Checkbox
            checked={isChecked}
            onCheckedChange={(checked) => {
              onToggleCheck();
            }}
            onClick={(e) => e.stopPropagation()}
            className="mt-0.5 shrink-0"
          /> */}
          <div className="flex-1 min-w-0 ml-3">
            <div className="flex items-center gap-1.5 mb-0.5">
              <h4 className="font-semibold text-xs truncate">
                {hierarchy.hierarchyName}
              </h4>
              {hierarchy.isRoot && (
                <Badge variant="outline" className="text-[10px] py-0 px-1 h-4">
                  Root
                </Badge>
              )}
            </div>
          </div>
          {hierarchy.flags?.active_flag ? (
            <CheckCircle className="w-3 h-3 mt-0.5 text-green-500 shrink-0" />
          ) : (
            <XCircle className="w-3 h-3 mt-0.5 text-gray-400 shrink-0" />
          )}
          <Button
            variant="ghost"
            size="sm"
            className="h-4 w-4 p-0 shrink-0"
            onClick={(e) => {
              e.stopPropagation();
              setShowDetails(!showDetails);
            }}
          >
            {showDetails ? (
              <ChevronDown className="w-3 h-3" />
            ) : (
              <ChevronRight className="w-3 h-3" />
            )}
          </Button>
        </div>

        {/* Expanded Details */}
        {showDetails && hierarchy.description && (
          <>
            {/* Compact Stats */}
            <div className="flex items-center gap-2 text-[10px] text-muted-foreground">
              {/* <span className="flex items-center gap-0.5">
                <Layers className="w-2.5 h-2.5" />
                {levelCount}
              </span> */}
              <span className="flex items-center gap-0.5">
                <Database className="w-2.5 h-2.5" />
                {sourcesCount}
              </span>
              {childrenCount > 0 && (
                <span className="flex items-center gap-0.5">
                  <ChevronDown className="w-2.5 h-2.5" />
                  {childrenCount}
                </span>
              )}
            </div>

            <div className="pt-1.5 border-t text-[10px]">
              <p className="text-muted-foreground line-clamp-2">
                {hierarchy.description}
              </p>
            </div>
          </>
        )}
      </div>

      <Handle type="source" position={Position.Right} className="bg-primary!" />
    </Card>
  );
};

const nodeTypes = {
  hierarchyNode: HierarchyNode,
};

export const EnhancedGraphView: React.FC<EnhancedGraphViewProps> = ({
  hierarchies,
  selectedId,
  selectedForFormula,
  onSelect,
  onToggleFormulaSelection,
}) => {
  // Build tree structure with dagre layout for proper hierarchical positioning
  const { nodes: initialNodes, edges: initialEdges } = useMemo(() => {
    const nodes: Node[] = [];
    const edges: Edge[] = [];

    // Node dimensions
    const NODE_WIDTH = 240;
    const NODE_HEIGHT = 100;

    // Debug: Log hierarchy data
    console.log("EnhancedGraphView - Hierarchies:", hierarchies);
    console.log("EnhancedGraphView - Total hierarchies:", hierarchies.length);

    // Create nodes
    hierarchies.forEach((hierarchy) => {
      console.log(`Creating node for: ${hierarchy.hierarchyName}`, {
        hierarchyId: hierarchy.hierarchyId,
        parentId: hierarchy.parentId,
        isRoot: hierarchy.isRoot,
      });

      nodes.push({
        id: hierarchy.hierarchyId,
        type: "hierarchyNode",
        position: { x: 0, y: 0 }, // Will be set by dagre
        data: {
          hierarchy,
          isSelected: hierarchy.hierarchyId === selectedId,
          isChecked: selectedForFormula.has(hierarchy.hierarchyId),
          onClick: () => onSelect(hierarchy.hierarchyId),
          onToggleCheck: () => onToggleFormulaSelection(hierarchy.hierarchyId),
        },
      });
    });

    console.log("EnhancedGraphView - Created nodes:", nodes.length);

    // Create edges based on parent-child relationships
    hierarchies.forEach((hierarchy) => {
      if (!hierarchy.isRoot && hierarchy.parentId) {
        console.log(
          `Creating edge: ${hierarchy.parentId} -> ${hierarchy.hierarchyId}`
        );

        const isHighlighted = selectedForFormula.has(hierarchy.hierarchyId);
        edges.push({
          id: `${hierarchy.parentId}-${hierarchy.hierarchyId}`,
          source: hierarchy.parentId,
          target: hierarchy.hierarchyId,
          type: "smoothstep",
          animated: isHighlighted,
          style: {
            stroke: isHighlighted ? "hsl(var(--primary))" : "#94a3b8", // Use solid color instead of HSL with opacity
            strokeWidth: isHighlighted ? 3 : 2,
          },
          markerEnd: {
            type: MarkerType.ArrowClosed,
            width: 20,
            height: 20,
            color: isHighlighted ? "hsl(var(--primary))" : "#94a3b8",
          },
        });
      } else {
        console.log(`Skipping edge for ${hierarchy.hierarchyName}:`, {
          isRoot: hierarchy.isRoot,
          hasParentId: !!hierarchy.parentId,
          parentId: hierarchy.parentId,
        });
      }
    });

    console.log("EnhancedGraphView - Created edges:", edges.length, edges);

    // Use dagre for automatic hierarchical layout
    const dagreGraph = new dagre.graphlib.Graph();
    dagreGraph.setDefaultEdgeLabel(() => ({}));
    dagreGraph.setGraph({
      rankdir: "LR", // Left to Right
      nodesep: 0, // Horizontal spacing between nodes at same level
      ranksep: 50, // Vertical spacing between levels
      edgesep: 0, // Spacing between edges
      marginx: 0,
      marginy: 0,
    });

    // Add nodes to dagre graph
    nodes.forEach((node) => {
      dagreGraph.setNode(node.id, { width: NODE_WIDTH, height: NODE_HEIGHT });
    });

    // Add edges to dagre graph
    edges.forEach((edge) => {
      dagreGraph.setEdge(edge.source, edge.target);
    });

    // Calculate layout
    dagre.layout(dagreGraph);

    // Apply calculated positions to nodes
    nodes.forEach((node) => {
      const nodeWithPosition = dagreGraph.node(node.id);
      node.position = {
        x: nodeWithPosition.x - NODE_WIDTH / 2,
        y: nodeWithPosition.y - NODE_HEIGHT / 2,
      };
    });

    return { nodes, edges };
  }, [
    hierarchies,
    selectedId,
    selectedForFormula,
    onSelect,
    onToggleFormulaSelection,
  ]);

  const [nodes, setNodes, onNodesChange] = useNodesState(initialNodes);
  const [edges, setEdges, onEdgesChange] = useEdgesState(initialEdges);

  // Update nodes when dependencies change
  React.useEffect(() => {
    setNodes(initialNodes);
    setEdges(initialEdges);
  }, [initialNodes, initialEdges, setNodes, setEdges]);

  if (hierarchies.length === 0) {
    return (
      <div className="h-full flex items-center justify-center text-muted-foreground">
        <div className="text-center">
          <p className="text-lg mb-2">No hierarchies to display</p>
          <p className="text-sm">
            Create hierarchies to see them in the graph view
          </p>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full w-full">
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        fitView
        minZoom={0.1}
        maxZoom={1.5}
        defaultViewport={{ x: 0, y: 0, zoom: 0.8 }}
      >
        <Background />
        <Controls />
      </ReactFlow>
    </div>
  );
};
