/**
 * Canvas Builder View
 * Visual node-based hierarchy editor with drag-drop connections
 * Note: In production, this would integrate with React Flow or similar library
 */
import { useState, useCallback, useRef, useEffect, useMemo } from "react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import {
  ContextMenu,
  ContextMenuContent,
  ContextMenuItem,
  ContextMenuSeparator,
  ContextMenuTrigger,
} from "@/components/ui/context-menu";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import {
  GitBranch,
  Plus,
  Minus,
  ZoomIn,
  ZoomOut,
  Maximize,
  Move,
  GripVertical,
  Database,
  Calculator,
  Edit3,
  Trash2,
  Copy,
  Link,
  Unlink,
  ChevronDown,
  ChevronRight,
  Settings,
  Save,
  Download,
  ArrowRight,
  CheckCircle,
  AlertTriangle,
  Layers,
  Grid3X3,
  Hand,
  MousePointer,
} from "lucide-react";

// Node types
export type CanvasNodeType = "regular" | "calculated" | "mapped" | "group";

export interface CanvasNode {
  id: string;
  hierarchyId: string;
  name: string;
  type: CanvasNodeType;
  position: { x: number; y: number };
  size?: { width: number; height: number };
  parentId?: string;
  children?: string[];
  data?: {
    formula?: string;
    formulaType?: string;
    mappingCount?: number;
    level?: number;
    isValid?: boolean;
  };
  collapsed?: boolean;
  selected?: boolean;
}

export interface CanvasConnection {
  id: string;
  source: string;
  target: string;
  type: "parent-child" | "reference";
}

interface CanvasBuilderViewProps {
  nodes: CanvasNode[];
  connections: CanvasConnection[];
  onNodeSelect: (nodeId: string | null) => void;
  onNodeUpdate: (nodeId: string, updates: Partial<CanvasNode>) => void;
  onNodeDelete: (nodeId: string) => void;
  onNodeCreate: (parentId: string | null, position: { x: number; y: number }) => void;
  onConnectionCreate: (sourceId: string, targetId: string) => void;
  onConnectionDelete: (connectionId: string) => void;
  selectedNodeId?: string | null;
  className?: string;
}

function getNodeColor(type: CanvasNodeType, isValid?: boolean): string {
  if (!isValid && isValid !== undefined) {
    return "border-red-500 bg-red-50";
  }
  switch (type) {
    case "calculated":
      return "border-purple-500 bg-purple-50";
    case "mapped":
      return "border-green-500 bg-green-50";
    case "group":
      return "border-blue-500 bg-blue-50";
    default:
      return "border-gray-300 bg-white";
  }
}

function getNodeIcon(type: CanvasNodeType) {
  switch (type) {
    case "calculated":
      return <Calculator className="h-4 w-4 text-purple-600" />;
    case "mapped":
      return <Database className="h-4 w-4 text-green-600" />;
    case "group":
      return <Layers className="h-4 w-4 text-blue-600" />;
    default:
      return <GitBranch className="h-4 w-4 text-gray-600" />;
  }
}

function CanvasNodeComponent({
  node,
  isSelected,
  onSelect,
  onDragStart,
  onDragEnd,
  onDelete,
  onDuplicate,
  onToggleCollapse,
  onEdit,
}: {
  node: CanvasNode;
  isSelected: boolean;
  onSelect: () => void;
  onDragStart: (e: React.MouseEvent) => void;
  onDragEnd: () => void;
  onDelete: () => void;
  onDuplicate: () => void;
  onToggleCollapse: () => void;
  onEdit: () => void;
}) {
  const [isDragging, setIsDragging] = useState(false);

  return (
    <ContextMenu>
      <ContextMenuTrigger>
        <div
          className={cn(
            "absolute cursor-move select-none transition-shadow",
            isDragging && "z-50 shadow-xl"
          )}
          style={{
            left: node.position.x,
            top: node.position.y,
            width: node.size?.width || 180,
          }}
          onClick={(e) => {
            e.stopPropagation();
            onSelect();
          }}
          onMouseDown={(e) => {
            if (e.button === 0) {
              setIsDragging(true);
              onDragStart(e);
            }
          }}
          onMouseUp={() => {
            setIsDragging(false);
            onDragEnd();
          }}
        >
          <Card
            className={cn(
              "border-2 transition-all",
              getNodeColor(node.type, node.data?.isValid),
              isSelected && "ring-2 ring-primary ring-offset-2",
              isDragging && "scale-105"
            )}
          >
            <CardContent className="p-3">
              {/* Header */}
              <div className="flex items-center gap-2">
                <GripVertical className="h-4 w-4 text-muted-foreground cursor-grab" />
                {getNodeIcon(node.type)}
                <span className="font-medium text-sm truncate flex-1">{node.name}</span>

                {node.children && node.children.length > 0 && (
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      onToggleCollapse();
                    }}
                    className="p-1 hover:bg-muted rounded"
                  >
                    {node.collapsed ? (
                      <ChevronRight className="h-4 w-4" />
                    ) : (
                      <ChevronDown className="h-4 w-4" />
                    )}
                  </button>
                )}
              </div>

              {/* Info badges */}
              <div className="flex flex-wrap gap-1 mt-2">
                {node.data?.level !== undefined && (
                  <Badge variant="outline" className="text-xs">
                    L{node.data.level}
                  </Badge>
                )}
                {node.data?.mappingCount !== undefined && node.data.mappingCount > 0 && (
                  <Badge variant="secondary" className="text-xs gap-1">
                    <Database className="h-3 w-3" />
                    {node.data.mappingCount}
                  </Badge>
                )}
                {node.data?.formulaType && (
                  <Badge variant="secondary" className="text-xs gap-1">
                    <Calculator className="h-3 w-3" />
                    {node.data.formulaType}
                  </Badge>
                )}
                {node.data?.isValid === false && (
                  <Badge variant="destructive" className="text-xs gap-1">
                    <AlertTriangle className="h-3 w-3" />
                    Invalid
                  </Badge>
                )}
              </div>

              {/* Connection points */}
              {!node.collapsed && (
                <>
                  <div className="absolute -top-2 left-1/2 -translate-x-1/2 w-4 h-4 bg-white border-2 border-gray-400 rounded-full hover:border-primary hover:bg-primary/10 transition-colors" />
                  <div className="absolute -bottom-2 left-1/2 -translate-x-1/2 w-4 h-4 bg-white border-2 border-gray-400 rounded-full hover:border-primary hover:bg-primary/10 transition-colors" />
                </>
              )}
            </CardContent>
          </Card>
        </div>
      </ContextMenuTrigger>
      <ContextMenuContent>
        <ContextMenuItem onClick={onEdit}>
          <Edit3 className="h-4 w-4 mr-2" />
          Edit Node
        </ContextMenuItem>
        <ContextMenuItem onClick={onDuplicate}>
          <Copy className="h-4 w-4 mr-2" />
          Duplicate
        </ContextMenuItem>
        <ContextMenuSeparator />
        <ContextMenuItem onClick={onDelete} className="text-red-600">
          <Trash2 className="h-4 w-4 mr-2" />
          Delete
        </ContextMenuItem>
      </ContextMenuContent>
    </ContextMenu>
  );
}

function CanvasConnectionLine({
  connection,
  sourceNode,
  targetNode,
  isSelected,
  onSelect,
}: {
  connection: CanvasConnection;
  sourceNode: CanvasNode;
  targetNode: CanvasNode;
  isSelected: boolean;
  onSelect: () => void;
}) {
  const sourceX = sourceNode.position.x + (sourceNode.size?.width || 180) / 2;
  const sourceY = sourceNode.position.y + 80;
  const targetX = targetNode.position.x + (targetNode.size?.width || 180) / 2;
  const targetY = targetNode.position.y;

  // Calculate control points for bezier curve
  const midY = (sourceY + targetY) / 2;

  const path = `M ${sourceX} ${sourceY} C ${sourceX} ${midY}, ${targetX} ${midY}, ${targetX} ${targetY}`;

  return (
    <g onClick={onSelect} className="cursor-pointer">
      {/* Invisible wider path for easier clicking */}
      <path d={path} stroke="transparent" strokeWidth={20} fill="none" />
      {/* Visible path */}
      <path
        d={path}
        stroke={isSelected ? "#3b82f6" : "#94a3b8"}
        strokeWidth={isSelected ? 3 : 2}
        fill="none"
        className="transition-colors"
        markerEnd="url(#arrowhead)"
      />
    </g>
  );
}

function CanvasToolbar({
  zoom,
  onZoomIn,
  onZoomOut,
  onZoomReset,
  onFitView,
  tool,
  onToolChange,
  onSave,
  onExport,
}: {
  zoom: number;
  onZoomIn: () => void;
  onZoomOut: () => void;
  onZoomReset: () => void;
  onFitView: () => void;
  tool: "select" | "pan" | "connect";
  onToolChange: (tool: "select" | "pan" | "connect") => void;
  onSave: () => void;
  onExport: () => void;
}) {
  return (
    <div className="absolute top-4 left-4 flex items-center gap-2 z-10">
      <Card>
        <CardContent className="p-1 flex items-center gap-1">
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  size="icon"
                  variant={tool === "select" ? "secondary" : "ghost"}
                  className="h-8 w-8"
                  onClick={() => onToolChange("select")}
                >
                  <MousePointer className="h-4 w-4" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>Select (V)</TooltipContent>
            </Tooltip>

            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  size="icon"
                  variant={tool === "pan" ? "secondary" : "ghost"}
                  className="h-8 w-8"
                  onClick={() => onToolChange("pan")}
                >
                  <Hand className="h-4 w-4" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>Pan (Space)</TooltipContent>
            </Tooltip>

            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  size="icon"
                  variant={tool === "connect" ? "secondary" : "ghost"}
                  className="h-8 w-8"
                  onClick={() => onToolChange("connect")}
                >
                  <Link className="h-4 w-4" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>Connect (C)</TooltipContent>
            </Tooltip>
          </TooltipProvider>

          <Separator orientation="vertical" className="h-6 mx-1" />

          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <Button size="icon" variant="ghost" className="h-8 w-8" onClick={onZoomOut}>
                  <ZoomOut className="h-4 w-4" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>Zoom Out (-)</TooltipContent>
            </Tooltip>

            <Button
              variant="ghost"
              size="sm"
              className="h-8 px-2 font-mono text-xs"
              onClick={onZoomReset}
            >
              {Math.round(zoom * 100)}%
            </Button>

            <Tooltip>
              <TooltipTrigger asChild>
                <Button size="icon" variant="ghost" className="h-8 w-8" onClick={onZoomIn}>
                  <ZoomIn className="h-4 w-4" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>Zoom In (+)</TooltipContent>
            </Tooltip>

            <Tooltip>
              <TooltipTrigger asChild>
                <Button size="icon" variant="ghost" className="h-8 w-8" onClick={onFitView}>
                  <Maximize className="h-4 w-4" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>Fit View (F)</TooltipContent>
            </Tooltip>
          </TooltipProvider>

          <Separator orientation="vertical" className="h-6 mx-1" />

          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <Button size="icon" variant="ghost" className="h-8 w-8" onClick={onSave}>
                  <Save className="h-4 w-4" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>Save (Ctrl+S)</TooltipContent>
            </Tooltip>

            <Tooltip>
              <TooltipTrigger asChild>
                <Button size="icon" variant="ghost" className="h-8 w-8" onClick={onExport}>
                  <Download className="h-4 w-4" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>Export</TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </CardContent>
      </Card>
    </div>
  );
}

function NodePropertiesPanel({
  node,
  open,
  onClose,
  onUpdate,
}: {
  node: CanvasNode | null;
  open: boolean;
  onClose: () => void;
  onUpdate: (updates: Partial<CanvasNode>) => void;
}) {
  const [name, setName] = useState(node?.name || "");

  useEffect(() => {
    setName(node?.name || "");
  }, [node]);

  if (!node) return null;

  return (
    <Sheet open={open} onOpenChange={onClose}>
      <SheetContent className="w-[400px]">
        <SheetHeader>
          <SheetTitle className="flex items-center gap-2">
            {getNodeIcon(node.type)}
            Node Properties
          </SheetTitle>
          <SheetDescription>Edit node settings and properties</SheetDescription>
        </SheetHeader>

        <div className="space-y-4 mt-6">
          <div className="space-y-2">
            <Label>Name</Label>
            <Input
              value={name}
              onChange={(e) => setName(e.target.value)}
              onBlur={() => onUpdate({ name })}
            />
          </div>

          <div className="space-y-2">
            <Label>Hierarchy ID</Label>
            <Input value={node.hierarchyId} disabled className="font-mono text-sm" />
          </div>

          <Separator />

          <div className="space-y-2">
            <Label>Node Type</Label>
            <div className="flex flex-wrap gap-2">
              {(["regular", "calculated", "mapped", "group"] as CanvasNodeType[]).map((type) => (
                <Button
                  key={type}
                  variant={node.type === type ? "secondary" : "outline"}
                  size="sm"
                  onClick={() => onUpdate({ type })}
                  className="capitalize"
                >
                  {getNodeIcon(type)}
                  <span className="ml-1">{type}</span>
                </Button>
              ))}
            </div>
          </div>

          {node.data?.formula && (
            <>
              <Separator />
              <div className="space-y-2">
                <Label>Formula</Label>
                <div className="p-2 bg-muted rounded font-mono text-sm">{node.data.formula}</div>
              </div>
            </>
          )}

          <Separator />

          <div className="space-y-2">
            <Label>Position</Label>
            <div className="grid grid-cols-2 gap-2">
              <div>
                <Label className="text-xs text-muted-foreground">X</Label>
                <Input
                  type="number"
                  value={Math.round(node.position.x)}
                  onChange={(e) =>
                    onUpdate({ position: { ...node.position, x: parseInt(e.target.value) || 0 } })
                  }
                />
              </div>
              <div>
                <Label className="text-xs text-muted-foreground">Y</Label>
                <Input
                  type="number"
                  value={Math.round(node.position.y)}
                  onChange={(e) =>
                    onUpdate({ position: { ...node.position, y: parseInt(e.target.value) || 0 } })
                  }
                />
              </div>
            </div>
          </div>
        </div>
      </SheetContent>
    </Sheet>
  );
}

export function CanvasBuilderView({
  nodes,
  connections,
  onNodeSelect,
  onNodeUpdate,
  onNodeDelete,
  onNodeCreate,
  onConnectionCreate,
  onConnectionDelete,
  selectedNodeId,
  className,
}: CanvasBuilderViewProps) {
  const canvasRef = useRef<HTMLDivElement>(null);
  const [zoom, setZoom] = useState(1);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const [tool, setTool] = useState<"select" | "pan" | "connect">("select");
  const [isPanning, setIsPanning] = useState(false);
  const [panStart, setPanStart] = useState({ x: 0, y: 0 });
  const [draggingNodeId, setDraggingNodeId] = useState<string | null>(null);
  const [dragOffset, setDragOffset] = useState({ x: 0, y: 0 });
  const [showPropertiesPanel, setShowPropertiesPanel] = useState(false);

  const selectedNode = useMemo(
    () => nodes.find((n) => n.id === selectedNodeId) || null,
    [nodes, selectedNodeId]
  );

  const nodeMap = useMemo(() => new Map(nodes.map((n) => [n.id, n])), [nodes]);

  // Zoom handlers
  const handleZoomIn = useCallback(() => setZoom((z) => Math.min(z * 1.2, 3)), []);
  const handleZoomOut = useCallback(() => setZoom((z) => Math.max(z / 1.2, 0.25)), []);
  const handleZoomReset = useCallback(() => setZoom(1), []);
  const handleFitView = useCallback(() => {
    setZoom(1);
    setPan({ x: 0, y: 0 });
  }, []);

  // Mouse handlers
  const handleMouseDown = useCallback(
    (e: React.MouseEvent) => {
      if (tool === "pan" || e.button === 1) {
        setIsPanning(true);
        setPanStart({ x: e.clientX - pan.x, y: e.clientY - pan.y });
      }
    },
    [tool, pan]
  );

  const handleMouseMove = useCallback(
    (e: React.MouseEvent) => {
      if (isPanning) {
        setPan({
          x: e.clientX - panStart.x,
          y: e.clientY - panStart.y,
        });
      } else if (draggingNodeId) {
        const node = nodeMap.get(draggingNodeId);
        if (node) {
          const rect = canvasRef.current?.getBoundingClientRect();
          if (rect) {
            const x = (e.clientX - rect.left - pan.x) / zoom - dragOffset.x;
            const y = (e.clientY - rect.top - pan.y) / zoom - dragOffset.y;
            onNodeUpdate(draggingNodeId, { position: { x, y } });
          }
        }
      }
    },
    [isPanning, panStart, draggingNodeId, dragOffset, pan, zoom, nodeMap, onNodeUpdate]
  );

  const handleMouseUp = useCallback(() => {
    setIsPanning(false);
    setDraggingNodeId(null);
  }, []);

  const handleNodeDragStart = useCallback(
    (nodeId: string, e: React.MouseEvent) => {
      const node = nodeMap.get(nodeId);
      if (node) {
        const rect = canvasRef.current?.getBoundingClientRect();
        if (rect) {
          const mouseX = (e.clientX - rect.left - pan.x) / zoom;
          const mouseY = (e.clientY - rect.top - pan.y) / zoom;
          setDragOffset({
            x: mouseX - node.position.x,
            y: mouseY - node.position.y,
          });
          setDraggingNodeId(nodeId);
        }
      }
    },
    [nodeMap, pan, zoom]
  );

  const handleCanvasClick = useCallback(
    (e: React.MouseEvent) => {
      if (e.target === e.currentTarget) {
        onNodeSelect(null);
      }
    },
    [onNodeSelect]
  );

  const handleCanvasDoubleClick = useCallback(
    (e: React.MouseEvent) => {
      if (e.target === e.currentTarget) {
        const rect = canvasRef.current?.getBoundingClientRect();
        if (rect) {
          const x = (e.clientX - rect.left - pan.x) / zoom;
          const y = (e.clientY - rect.top - pan.y) / zoom;
          onNodeCreate(null, { x, y });
        }
      }
    },
    [pan, zoom, onNodeCreate]
  );

  // Keyboard shortcuts
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.target instanceof HTMLInputElement) return;

      switch (e.key.toLowerCase()) {
        case "v":
          setTool("select");
          break;
        case " ":
          setTool("pan");
          e.preventDefault();
          break;
        case "c":
          setTool("connect");
          break;
        case "f":
          handleFitView();
          break;
        case "=":
        case "+":
          handleZoomIn();
          break;
        case "-":
          handleZoomOut();
          break;
        case "delete":
        case "backspace":
          if (selectedNodeId) {
            onNodeDelete(selectedNodeId);
          }
          break;
      }
    };

    const handleKeyUp = (e: KeyboardEvent) => {
      if (e.key === " ") {
        setTool("select");
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    window.addEventListener("keyup", handleKeyUp);
    return () => {
      window.removeEventListener("keydown", handleKeyDown);
      window.removeEventListener("keyup", handleKeyUp);
    };
  }, [selectedNodeId, onNodeDelete, handleFitView, handleZoomIn, handleZoomOut]);

  return (
    <div className={cn("relative h-full overflow-hidden bg-muted/30", className)}>
      {/* Grid background */}
      <div
        className="absolute inset-0 opacity-30"
        style={{
          backgroundImage: `
            linear-gradient(to right, #e5e7eb 1px, transparent 1px),
            linear-gradient(to bottom, #e5e7eb 1px, transparent 1px)
          `,
          backgroundSize: `${20 * zoom}px ${20 * zoom}px`,
          backgroundPosition: `${pan.x}px ${pan.y}px`,
        }}
      />

      {/* Toolbar */}
      <CanvasToolbar
        zoom={zoom}
        onZoomIn={handleZoomIn}
        onZoomOut={handleZoomOut}
        onZoomReset={handleZoomReset}
        onFitView={handleFitView}
        tool={tool}
        onToolChange={setTool}
        onSave={() => console.log("Save")}
        onExport={() => console.log("Export")}
      />

      {/* Canvas */}
      <div
        ref={canvasRef}
        className={cn(
          "absolute inset-0",
          tool === "pan" && "cursor-grab",
          isPanning && "cursor-grabbing"
        )}
        onMouseDown={handleMouseDown}
        onMouseMove={handleMouseMove}
        onMouseUp={handleMouseUp}
        onMouseLeave={handleMouseUp}
        onClick={handleCanvasClick}
        onDoubleClick={handleCanvasDoubleClick}
      >
        <div
          className="absolute"
          style={{
            transform: `translate(${pan.x}px, ${pan.y}px) scale(${zoom})`,
            transformOrigin: "0 0",
          }}
        >
          {/* SVG for connections */}
          <svg className="absolute top-0 left-0 overflow-visible" style={{ zIndex: 0 }}>
            <defs>
              <marker
                id="arrowhead"
                markerWidth="10"
                markerHeight="7"
                refX="9"
                refY="3.5"
                orient="auto"
              >
                <polygon points="0 0, 10 3.5, 0 7" fill="#94a3b8" />
              </marker>
            </defs>
            {connections.map((connection) => {
              const sourceNode = nodeMap.get(connection.source);
              const targetNode = nodeMap.get(connection.target);
              if (!sourceNode || !targetNode) return null;
              return (
                <CanvasConnectionLine
                  key={connection.id}
                  connection={connection}
                  sourceNode={sourceNode}
                  targetNode={targetNode}
                  isSelected={false}
                  onSelect={() => {}}
                />
              );
            })}
          </svg>

          {/* Nodes */}
          {nodes.map((node) => (
            <CanvasNodeComponent
              key={node.id}
              node={node}
              isSelected={selectedNodeId === node.id}
              onSelect={() => onNodeSelect(node.id)}
              onDragStart={(e) => handleNodeDragStart(node.id, e)}
              onDragEnd={() => setDraggingNodeId(null)}
              onDelete={() => onNodeDelete(node.id)}
              onDuplicate={() => {
                const newPosition = {
                  x: node.position.x + 50,
                  y: node.position.y + 50,
                };
                onNodeCreate(node.parentId || null, newPosition);
              }}
              onToggleCollapse={() => onNodeUpdate(node.id, { collapsed: !node.collapsed })}
              onEdit={() => setShowPropertiesPanel(true)}
            />
          ))}
        </div>
      </div>

      {/* Properties panel */}
      <NodePropertiesPanel
        node={selectedNode}
        open={showPropertiesPanel}
        onClose={() => setShowPropertiesPanel(false)}
        onUpdate={(updates) => {
          if (selectedNodeId) {
            onNodeUpdate(selectedNodeId, updates);
          }
        }}
      />

      {/* Mini map (bottom right) */}
      <Card className="absolute bottom-4 right-4 w-48 h-32 overflow-hidden">
        <CardContent className="p-1 h-full relative bg-muted/50">
          <div className="absolute inset-1 flex items-center justify-center text-xs text-muted-foreground">
            <Grid3X3 className="h-4 w-4 mr-1" />
            Mini Map
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
