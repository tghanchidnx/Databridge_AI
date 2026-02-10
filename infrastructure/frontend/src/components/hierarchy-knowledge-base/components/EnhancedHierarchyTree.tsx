import React, { useState, useMemo } from "react";
import {
  DndContext,
  closestCenter,
  KeyboardSensor,
  PointerSensor,
  useSensor,
  useSensors,
  DragEndEvent,
  DragOverlay,
  DragStartEvent,
  DragOverEvent,
  pointerWithin,
  rectIntersection,
  getFirstCollision,
  UniqueIdentifier,
} from "@dnd-kit/core";
import {
  arrayMove,
  SortableContext,
  sortableKeyboardCoordinates,
  verticalListSortingStrategy,
  useSortable,
} from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import type { SmartHierarchyMaster } from "@/services/api/hierarchy";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import {
  ChevronRight,
  ChevronDown,
  Layers,
  Database,
  GripVertical,
  Info,
  CheckCircle,
  XCircle,
  Calculator,
  Sigma,
  Plus,
  Minus,
  X,
  Divide,
} from "lucide-react";
import { cn } from "@/lib/utils";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";

interface EnhancedHierarchyTreeProps {
  hierarchies: SmartHierarchyMaster[];
  selectedId: string | null;
  selectedForFormula: Set<string>;
  selectedForScript?: Set<string>;
  onSelect: (id: string) => void;
  onToggleFormulaSelection: (id: string) => void;
  onToggleScriptSelection?: (id: string) => void;
  onReorder: (hierarchies: SmartHierarchyMaster[]) => void;
  expandedNodes?: Set<string>;
  expandedDetails?: Set<string>;
  onExpandedNodesChange?: (expanded: Set<string>) => void;
  onExpandedDetailsChange?: (expanded: Set<string>) => void;
}

interface TreeNode extends SmartHierarchyMaster {
  children: TreeNode[];
  level: number;
}

export const EnhancedHierarchyTree: React.FC<EnhancedHierarchyTreeProps> = ({
  hierarchies,
  selectedId,
  selectedForFormula,
  selectedForScript,
  onSelect,
  onToggleFormulaSelection,
  onToggleScriptSelection,
  onReorder,
  expandedNodes: externalExpandedNodes,
  expandedDetails: externalExpandedDetails,
  onExpandedNodesChange,
  onExpandedDetailsChange,
}) => {
  const [internalExpandedNodes, setInternalExpandedNodes] = useState<
    Set<string>
  >(new Set());
  const [internalExpandedDetails, setInternalExpandedDetails] = useState<
    Set<string>
  >(new Set());

  // Use external state if provided, otherwise use internal state
  const expandedNodes = externalExpandedNodes ?? internalExpandedNodes;
  const expandedDetails = externalExpandedDetails ?? internalExpandedDetails;
  const setExpandedNodes = onExpandedNodesChange ?? setInternalExpandedNodes;
  const setExpandedDetails =
    onExpandedDetailsChange ?? setInternalExpandedDetails;

  const [activeId, setActiveId] = useState<UniqueIdentifier | null>(null);
  const [overId, setOverId] = useState<UniqueIdentifier | null>(null);
  const [dropAsChild, setDropAsChild] = useState<boolean>(false);

  // Helper function to find node by ID
  const findNode = (nodes: TreeNode[], id: string): TreeNode | null => {
    for (const node of nodes) {
      if (node.hierarchyId === id) return node;
      const found = findNode(node.children, id);
      if (found) return found;
    }
    return null;
  };

  // Helper function to get all descendant IDs (including the node itself)
  const getAllDescendantIds = (node: TreeNode): string[] => {
    const ids: string[] = [node.hierarchyId];
    const traverse = (n: TreeNode) => {
      n.children.forEach((child) => {
        ids.push(child.hierarchyId);
        traverse(child);
      });
    };
    traverse(node);
    return ids;
  };

  // Check if draggedNode is ancestor of targetNode (prevent circular references)
  const isDescendant = (draggedNode: TreeNode, targetId: string): boolean => {
    const descendantIds = getAllDescendantIds(draggedNode);
    return descendantIds.includes(targetId);
  };

  // Handler for checkbox with hierarchical selection
  const handleCheckboxToggle = (node: TreeNode) => {
    const isCurrentlyChecked = selectedForFormula.has(node.hierarchyId);
    const descendantIds = getAllDescendantIds(node).filter(
      (id) => id !== node.hierarchyId
    );

    if (isCurrentlyChecked) {
      // Uncheck: remove this node and all descendants
      onToggleFormulaSelection(node.hierarchyId);
      descendantIds.forEach((id) => {
        if (selectedForFormula.has(id)) {
          onToggleFormulaSelection(id);
        }
      });

      // Also handle script selection if available
      if (onToggleScriptSelection) {
        if (selectedForScript?.has(node.hierarchyId)) {
          onToggleScriptSelection(node.hierarchyId);
        }
        descendantIds.forEach((id) => {
          if (selectedForScript?.has(id)) {
            onToggleScriptSelection(id);
          }
        });
      }
    } else {
      // Check: add this node and all descendants
      onToggleFormulaSelection(node.hierarchyId);
      descendantIds.forEach((id) => {
        if (!selectedForFormula.has(id)) {
          onToggleFormulaSelection(id);
        }
      });

      // Also handle script selection if available
      if (onToggleScriptSelection) {
        if (!selectedForScript?.has(node.hierarchyId)) {
          onToggleScriptSelection(node.hierarchyId);
        }
        descendantIds.forEach((id) => {
          if (!selectedForScript?.has(id)) {
            onToggleScriptSelection(id);
          }
        });
      }
    }
  };

  // Build tree structure
  const treeData = useMemo(() => {
    const buildTree = (items: SmartHierarchyMaster[]): TreeNode[] => {
      const roots: TreeNode[] = [];
      const childMap = new Map<string, SmartHierarchyMaster[]>();

      items.forEach((item) => {
        if (item.isRoot || !item.parentId) {
          roots.push({ ...item, children: [], level: 0 });
        } else {
          if (!childMap.has(item.parentId)) {
            childMap.set(item.parentId, []);
          }
          childMap.get(item.parentId)!.push(item);
        }
      });

      const addChildren = (node: TreeNode, level: number) => {
        const children = childMap.get(node.hierarchyId) || [];
        node.children = children
          .sort((a, b) => (a.sortOrder || 0) - (b.sortOrder || 0))
          .map((child) => {
            const childNode: TreeNode = {
              ...child,
              children: [],
              level: level + 1,
            };
            addChildren(childNode, level + 1);
            return childNode;
          });
      };

      roots.forEach((root) => addChildren(root, 0));
      return roots.sort((a, b) => (a.sortOrder || 0) - (b.sortOrder || 0));
    };

    return buildTree(hierarchies);
  }, [hierarchies]);

  const sensors = useSensors(
    useSensor(PointerSensor),
    useSensor(KeyboardSensor, {
      coordinateGetter: sortableKeyboardCoordinates,
    })
  );

  const toggleExpand = (id: string) => {
    setExpandedNodes((prev) => {
      const newSet = new Set(prev);
      if (newSet.has(id)) {
        newSet.delete(id);
      } else {
        newSet.add(id);
      }
      return newSet;
    });
  };

  const toggleDetails = (id: string) => {
    setExpandedDetails((prev) => {
      const newSet = new Set(prev);
      if (newSet.has(id)) {
        newSet.delete(id);
      } else {
        newSet.add(id);
      }
      return newSet;
    });
  };

  const handleDragEnd = (event: DragEndEvent) => {
    const { active, over } = event;

    setActiveId(null);
    setOverId(null);
    const shouldDropAsChild = dropAsChild;
    setDropAsChild(false);

    if (!over || active.id === over.id) return;

    const draggedId = active.id as string;
    const targetId = over.id as string;

    // Find the dragged node and target node
    const draggedNode = findNode(treeData, draggedId);
    const targetNode = findNode(treeData, targetId);

    if (!draggedNode || !targetNode) return;

    // Check if we're dragging multiple selected nodes
    const isDraggingMultiple =
      selectedForFormula.has(draggedId) && selectedForFormula.size > 1;

    // Get all nodes to move (either just the dragged one, or all selected)
    let nodesToMove: string[] = [];

    if (isDraggingMultiple) {
      // Get all top-level selected nodes (exclude children of selected nodes)
      const selectedNodes = hierarchies.filter((h) =>
        selectedForFormula.has(h.hierarchyId)
      );
      const selectedNodeIds = new Set(selectedNodes.map((h) => h.hierarchyId));

      // Filter to get only top-level selected nodes (not children of other selected nodes)
      const topLevelSelected = selectedNodes.filter((node) => {
        // Check if any parent is also selected
        let currentParentId = node.parentId;
        while (currentParentId) {
          if (selectedNodeIds.has(currentParentId)) {
            return false; // This node has a selected parent, skip it
          }
          const parent = hierarchies.find(
            (h) => h.hierarchyId === currentParentId
          );
          currentParentId = parent?.parentId || null;
        }
        return true;
      });

      nodesToMove = topLevelSelected.map((n) => n.hierarchyId);
    } else {
      nodesToMove = [draggedId];
    }

    // Prevent dropping any node into its own descendants
    for (const nodeId of nodesToMove) {
      const node = findNode(treeData, nodeId);
      if (node && isDescendant(node, targetId)) {
        console.warn("Cannot move a node into its own descendant");
        return;
      }
    }

    // Also prevent dropping into any of the nodes being moved
    if (nodesToMove.includes(targetId)) {
      console.warn("Cannot drop onto a node that is being moved");
      return;
    }

    // Clone hierarchies array
    const updatedHierarchies = [...hierarchies];

    // Find the target hierarchy
    const targetHierarchy = updatedHierarchies.find(
      (h) => h.hierarchyId === targetId
    );

    if (!targetHierarchy) return;

    // Determine if we should drop as child or sibling
    let newParentId: string | null;
    let isNewRoot: boolean;

    if (shouldDropAsChild) {
      // Drop as CHILD of the target
      newParentId = targetId;
      isNewRoot = false;
    } else {
      // Drop as SIBLING of the target (same parent)
      newParentId = targetHierarchy.parentId || null;
      isNewRoot = targetHierarchy.isRoot || false;
    }

    // Get all hierarchies to move
    const hierarchiesToMove = updatedHierarchies.filter((h) =>
      nodesToMove.includes(h.hierarchyId)
    );

    // Get all siblings at the target's level (excluding the ones being moved)
    const siblings = updatedHierarchies.filter((h) => {
      // Exclude nodes being moved
      if (nodesToMove.includes(h.hierarchyId)) return false;

      // Filter by parent level
      if (shouldDropAsChild) {
        // If dropping as child, get existing children of target
        return h.parentId === targetId;
      } else if (isNewRoot) {
        return h.isRoot;
      } else {
        return h.parentId === newParentId;
      }
    });

    // Find target position in siblings
    let targetIndex: number;
    if (shouldDropAsChild) {
      // When dropping as child, add at the end of children
      targetIndex = siblings.length - 1;
    } else {
      // When dropping as sibling, add after target
      targetIndex = siblings.findIndex((h) => h.hierarchyId === targetId);
    }

    // Insert moved items after target (or at end of children if dropping as child)
    siblings.splice(targetIndex + 1, 0, ...hierarchiesToMove);

    // Update the moved hierarchies' parent and root status
    hierarchiesToMove.forEach((h) => {
      h.parentId = newParentId;
      h.isRoot = isNewRoot;
    });

    // If dropping as child, auto-expand the target node to show new children
    if (shouldDropAsChild && targetId) {
      setExpandedNodes((prev) => {
        const newSet = new Set(prev);
        newSet.add(targetId);
        return newSet;
      });
    }

    // Update sort orders for all siblings (including newly moved items)
    const updatedSiblings = siblings.map((item, idx) => ({
      ...item,
      sortOrder: idx,
    }));

    // Merge back: keep all non-siblings unchanged, update siblings
    const finalHierarchies = updatedHierarchies.map((h) => {
      const updated = updatedSiblings.find(
        (s) => s.hierarchyId === h.hierarchyId
      );
      return updated || h;
    });

    onReorder(finalHierarchies);
  };

  const handleDragStart = (event: DragStartEvent) => {
    setActiveId(event.active.id);
  };

  const handleDragOver = (event: DragOverEvent) => {
    setOverId(event.over?.id || null);

    // Check if Shift key is pressed to drop as child
    const isShiftPressed = (event as any).activatorEvent?.shiftKey || false;
    setDropAsChild(isShiftPressed);
  };

  const handleDragCancel = () => {
    setActiveId(null);
    setOverId(null);
  };

  if (treeData.length === 0) {
    return (
      <div className="text-center text-muted-foreground text-sm py-8">
        No hierarchies found. Create one to get started.
      </div>
    );
  }

  return (
    <TooltipProvider>
      <DndContext
        sensors={sensors}
        collisionDetection={closestCenter}
        onDragStart={handleDragStart}
        onDragOver={handleDragOver}
        onDragEnd={handleDragEnd}
        onDragCancel={handleDragCancel}
      >
        <div className="space-y-1 p-2">
          <SortableContext
            items={treeData.map((node) => node.hierarchyId)}
            strategy={verticalListSortingStrategy}
          >
            {treeData.map((node) => (
              <TreeNodeComponent
                key={node.hierarchyId}
                node={node}
                selectedId={selectedId}
                selectedForFormula={selectedForFormula}
                selectedForScript={selectedForScript}
                expandedNodes={expandedNodes}
                expandedDetails={expandedDetails}
                onSelect={onSelect}
                onCheckboxToggle={handleCheckboxToggle}
                onToggleExpand={toggleExpand}
                onToggleDetails={toggleDetails}
                activeId={activeId}
                overId={overId}
                dropAsChild={dropAsChild}
              />
            ))}
          </SortableContext>
        </div>
        <DragOverlay>
          {activeId ? (
            <div className="bg-card border-2 border-primary rounded-lg p-2 shadow-lg opacity-90">
              <div className="flex items-center gap-2">
                <GripVertical className="w-4 h-4 text-muted-foreground" />
                <span className="font-medium text-sm">
                  {findNode(treeData, activeId as string)?.hierarchyName ||
                    "Moving..."}
                </span>
                {selectedForFormula.has(activeId as string) &&
                  selectedForFormula.size > 1 && (
                    <Badge variant="secondary" className="text-xs">
                      +{selectedForFormula.size - 1} more
                    </Badge>
                  )}
                {dropAsChild && (
                  <Badge variant="default" className="text-xs bg-blue-500">
                    → as child
                  </Badge>
                )}
              </div>
            </div>
          ) : null}
        </DragOverlay>
      </DndContext>
    </TooltipProvider>
  );
};

// Individual Tree Node Component
interface TreeNodeComponentProps {
  node: TreeNode;
  selectedId: string | null;
  selectedForFormula: Set<string>;
  selectedForScript?: Set<string>;
  expandedNodes: Set<string>;
  expandedDetails: Set<string>;
  onSelect: (id: string) => void;
  onCheckboxToggle: (node: TreeNode) => void;
  onToggleExpand: (id: string) => void;
  onToggleDetails: (id: string) => void;
  activeId: UniqueIdentifier | null;
  overId: UniqueIdentifier | null;
  dropAsChild: boolean;
}

const TreeNodeComponent: React.FC<TreeNodeComponentProps> = ({
  node,
  selectedId,
  selectedForFormula,
  selectedForScript,
  expandedNodes,
  expandedDetails,
  onSelect,
  onCheckboxToggle,
  onToggleExpand,
  onToggleDetails,
  activeId,
  overId,
  dropAsChild,
}) => {
  const {
    attributes,
    listeners,
    setNodeRef,
    transform,
    transition,
    isDragging,
  } = useSortable({
    id: node.hierarchyId,
  });

  const style = {
    transform: CSS.Transform.toString(transform),
    transition,
    opacity: isDragging ? 0.5 : 1,
  };

  const hasChildren = node.children.length > 0;
  const isExpanded = expandedNodes.has(node.hierarchyId);
  const isDetailsExpanded = expandedDetails.has(node.hierarchyId);
  const isSelected = node.hierarchyId === selectedId;
  const isChecked = selectedForFormula.has(node.hierarchyId);
  const isOver = overId === node.hierarchyId;
  const isActive = activeId === node.hierarchyId;
  const isDropTarget = isOver && !isActive;
  const willBeChild = isDropTarget && dropAsChild;

  // Check if this node is part of a multi-select drag (checked and another checked node is being dragged)
  const isPartOfMultiDrag =
    isChecked &&
    activeId &&
    activeId !== node.hierarchyId &&
    selectedForFormula.has(activeId as string) &&
    selectedForFormula.size > 1;

  // Handle both minimal and full hierarchy data formats
  const levelCount =
    (node as any).levelsCount ||
    Object.values(node.hierarchyLevel || {}).filter(Boolean).length;
  const mappingCount = (node as any).sourcesCount || node.mapping?.length || 0;
  const levels = (node as any).levels || [];
  const sources = (node as any).sources || [];

  // Formula configuration
  const formulaConfig = node.formulaConfig as any;
  const isFormulaGroup = formulaConfig?.isFormulaGroup || false;
  const isCalculated = formulaConfig?.isCalculated || false;
  const formulaOperation = formulaConfig?.formulaOperation || "";
  const formulaGroupName = formulaConfig?.formulaGroupName || "";
  const formulaMembers = formulaConfig?.formulaMembers || [];

  // Operation icon helper
  const getOperationIcon = (operation: string) => {
    switch (operation?.toUpperCase()) {
      case "ADD":
        return Plus;
      case "SUBTRACT":
        return Minus;
      case "MULTIPLY":
        return X;
      case "DIVIDE":
        return Divide;
      case "SUM":
        return Sigma;
      case "AVG":
        return Sigma;
      default:
        return null;
    }
  };

  const OperationIcon = getOperationIcon(formulaOperation);

  return (
    <div
      ref={setNodeRef}
      style={style}
      className={cn("", node.level > 0 && "ml-6")}
    >
      <div
        className={cn(
          "group rounded-lg border overflow-hidden transition-all",
          isSelected
            ? "bg-primary/10 border-primary shadow-sm"
            : "bg-card hover:bg-accent/50 border-border",
          isDropTarget &&
            !willBeChild &&
            "border-primary border-2 bg-primary/5",
          willBeChild && "border-blue-500 border-2 bg-blue-500/10",
          isActive && "opacity-50",
          isPartOfMultiDrag && "opacity-60 border-primary/50"
        )}
      >
        {/* Main Row */}
        <div className="flex items-center gap-2 px-2 py-2">
          {/* Drag Handle */}
          <Tooltip>
            <TooltipTrigger asChild>
              <div
                {...attributes}
                {...listeners}
                className="cursor-grab active:cursor-grabbing shrink-0"
              >
                <GripVertical className="w-4 h-4 text-muted-foreground" />
              </div>
            </TooltipTrigger>
            <TooltipContent>
              <p>
                Drag to reorder
                {isChecked && selectedForFormula.size > 1
                  ? ` (${selectedForFormula.size} selected)`
                  : ""}
              </p>
              <p className="text-xs text-muted mt-1">
                Hold Shift to drop as child
              </p>
            </TooltipContent>
          </Tooltip>

          {/* Checkbox for Formula Selection */}
          <Tooltip>
            <TooltipTrigger asChild>
              <Checkbox
                checked={isChecked}
                onCheckedChange={() => onCheckboxToggle(node)}
                className="shrink-0"
              />
            </TooltipTrigger>
            <TooltipContent>
              <p>Select for multi-actions (formula, drag & drop)</p>
            </TooltipContent>
          </Tooltip>

          {/* Checkbox for Script Generation
              {onToggleScriptSelection && (
                <Checkbox
                  checked={selectedForScript?.has(node.hierarchyId)}
                  onCheckedChange={() =>
                    onToggleScriptSelection(node.hierarchyId)
                  }
                  className="shrink-0"
                  title="Select for script generation"
                />
              )} */}

          {/* Children Expand/Collapse */}
          {hasChildren ? (
            <Button
              variant="ghost"
              size="sm"
              className="h-5 w-5 p-0 shrink-0"
              onClick={() => onToggleExpand(node.hierarchyId)}
            >
              {isExpanded ? (
                <ChevronDown className="w-3.5 h-3.5" />
              ) : (
                <ChevronRight className="w-3.5 h-3.5" />
              )}
            </Button>
          ) : (
            <div className="w-5" />
          )}

          {/* Name and Level Badge */}
          <div
            className="flex-1 min-w-0 flex items-center gap-2 cursor-pointer"
            onClick={() => onSelect(node.hierarchyId)}
          >
            <span className="font-medium text-sm truncate">
              {node.hierarchyName}
            </span>

            {/* Drop as Child Indicator */}
            {willBeChild && (
              <Badge
                variant="default"
                className="text-xs bg-blue-500 animate-pulse"
              >
                ↓ Drop as child
              </Badge>
            )}

            {/* Formula Group Indicator */}
            {isFormulaGroup && (
              <Tooltip>
                <TooltipTrigger>
                  <Badge
                    variant="default"
                    className="text-xs shrink-0 bg-purple-500 hover:bg-purple-600"
                  >
                    <Calculator className="w-3 h-3 mr-1" />
                    Formula Group
                  </Badge>
                </TooltipTrigger>
                <TooltipContent>
                  <p>Formula Group: {formulaGroupName || node.hierarchyName}</p>
                  <p className="text-xs">{formulaMembers.length} members</p>
                </TooltipContent>
              </Tooltip>
            )}

            {/* Calculated Field Indicator */}
            {isCalculated && !isFormulaGroup && (
              <Tooltip>
                <TooltipTrigger>
                  <Badge variant="secondary" className="text-xs shrink-0">
                    {OperationIcon && (
                      <OperationIcon className="w-3 h-3 mr-1" />
                    )}
                    {formulaOperation}
                  </Badge>
                </TooltipTrigger>
                <TooltipContent>
                  <p>Calculated: {formulaOperation}</p>
                  {formulaConfig?.formulaParamRef && (
                    <p className="text-xs">
                      Ref: {formulaConfig.formulaParamRef}
                    </p>
                  )}
                </TooltipContent>
              </Tooltip>
            )}

            {node.isRoot && (
              <Badge variant="outline" className="text-xs shrink-0">
                Root
              </Badge>
            )}
            {!node.isRoot && node.level > 0 && (
              <Badge variant="secondary" className="text-xs shrink-0">
                L{node.level}
              </Badge>
            )}
          </div>

          {/* Compact Stats */}
          <div className="flex items-center gap-2 shrink-0">
            {/* <Tooltip>
                  <TooltipTrigger>
                    <span className="text-xs text-muted-foreground flex items-center gap-1">
                      <Layers className="w-3 h-3" />
                      {levelCount}
                    </span>
                  </TooltipTrigger>
                  <TooltipContent>
                    <p>{levelCount} hierarchy levels</p>
                  </TooltipContent>
                </Tooltip> */}

            <Tooltip>
              <TooltipTrigger>
                <span className="text-xs text-muted-foreground flex items-center gap-1">
                  <Database className="w-3 h-3" />
                  {mappingCount}
                </span>
              </TooltipTrigger>
              <TooltipContent>
                <p>{mappingCount} source mappings</p>
              </TooltipContent>
            </Tooltip>

            {node.flags?.active_flag ? (
              <CheckCircle className="w-3.5 h-3.5 text-green-500" />
            ) : (
              <XCircle className="w-3.5 h-3.5 text-gray-400" />
            )}
          </div>

          {/* Details Expand */}
          <Button
            variant="ghost"
            size="sm"
            className="h-5 w-5 p-0 shrink-0"
            onClick={() => onToggleDetails(node.hierarchyId)}
          >
            <Info className="w-3.5 h-3.5" />
          </Button>
        </div>

        {/* Expanded Details */}
        {isDetailsExpanded && (
          <div className="px-2 pb-2 pt-1 space-y-2 border-t bg-muted/30 text-xs">
            {/* Description */}
            {node.description && (
              <div>
                <span className="font-medium text-muted-foreground">
                  Description:{" "}
                </span>
                <span className="text-foreground">{node.description}</span>
              </div>
            )}

            {/* Formula Group Members */}
            {isFormulaGroup && formulaMembers.length > 0 && (
              <div>
                <span className="font-medium text-muted-foreground">
                  Formula Members ({formulaMembers.length}):{" "}
                </span>
                <div className="space-y-1 mt-1">
                  {formulaMembers.map((member: any, idx: number) => {
                    const MemberIcon = getOperationIcon(member.operation);
                    return (
                      <div
                        key={idx}
                        className="flex items-center gap-2 text-xs bg-muted/50 rounded px-2 py-1"
                      >
                        {MemberIcon && (
                          <MemberIcon className="w-3 h-3 text-primary" />
                        )}
                        <span className="font-medium">{member.operation}</span>
                        <span className="text-muted-foreground">→</span>
                        <span>{member.hierarchyName}</span>
                        {member.constantValue && (
                          <Badge variant="outline" className="text-xs">
                            × {member.constantValue}
                          </Badge>
                        )}
                        {member.precedence && (
                          <Badge variant="secondary" className="text-xs">
                            #{member.precedence}
                          </Badge>
                        )}
                      </div>
                    );
                  })}
                </div>
              </div>
            )}

            {/* Formula Configuration */}
            {isCalculated && !isFormulaGroup && (
              <div>
                <span className="font-medium text-muted-foreground">
                  Formula:{" "}
                </span>
                <div className="text-xs bg-muted/50 rounded px-2 py-1 mt-1 font-mono">
                  {formulaOperation}
                  {formulaConfig?.formulaParamRef &&
                    ` (${formulaConfig.formulaParamRef})`}
                  {formulaConfig?.formulaParam2Const &&
                    ` × ${formulaConfig.formulaParam2Const}`}
                </div>
              </div>
            )}

            {/* Levels */}
            {levelCount > 0 && (
              <div>
                <span className="font-medium text-muted-foreground">
                  Levels ({levelCount}):{" "}
                </span>
                <div className="flex flex-wrap gap-1 mt-1">
                  {levels.length > 0 ? (
                    <>
                      {levels.map((level: string, idx: number) => (
                        <Badge
                          key={idx}
                          variant="secondary"
                          className="text-xs"
                        >
                          {level}
                        </Badge>
                      ))}
                      {levelCount > levels.length && (
                        <Badge variant="outline" className="text-xs">
                          +{levelCount - levels.length} more
                        </Badge>
                      )}
                    </>
                  ) : (
                    // Fallback to full data if available
                    Object.entries(node.hierarchyLevel || {})
                      .filter(([_, value]) => value)
                      .slice(0, 3)
                      .map(([key, value]) => (
                        <Badge
                          key={key}
                          variant="secondary"
                          className="text-xs"
                        >
                          {value}
                        </Badge>
                      ))
                  )}
                </div>
              </div>
            )}

            {/* Mappings/Sources */}
            {mappingCount > 0 && (
              <div>
                <span className="font-medium text-muted-foreground">
                  Sources ({mappingCount}):{" "}
                </span>
                <div className="space-y-1 mt-1">
                  {sources.length > 0 ? (
                    <>
                      {sources.map((source: string, i: number) => (
                        <div key={i} className="text-xs font-mono">
                          {source}
                        </div>
                      ))}
                      {mappingCount > sources.length && (
                        <div className="text-xs text-muted-foreground">
                          +{mappingCount - sources.length} more sources
                        </div>
                      )}
                    </>
                  ) : (
                    // Fallback to full mapping data if available
                    <>
                      {node.mapping?.slice(0, 3).map((m, i) => (
                        <div key={i} className="text-xs font-mono">
                          {m.source_database}.{m.source_schema}.{m.source_table}
                        </div>
                      ))}
                      {mappingCount > 3 && (
                        <div className="text-xs text-muted-foreground">
                          +{mappingCount - 3} more sources
                        </div>
                      )}
                    </>
                  )}
                </div>
              </div>
            )}

            {/* Flags */}
            <div>
              <span className="font-medium text-muted-foreground">Flags: </span>
              <div className="flex flex-wrap gap-1 mt-1">
                {node.flags?.include_flag && (
                  <Badge variant="outline" className="text-xs">
                    Include
                  </Badge>
                )}
                {node.flags?.transform_flag && (
                  <Badge variant="outline" className="text-xs">
                    Transform
                  </Badge>
                )}
                {node.flags?.is_leaf_node && (
                  <Badge variant="outline" className="text-xs">
                    Leaf
                  </Badge>
                )}
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Children */}
      {hasChildren && isExpanded && (
        <div className="mt-1 space-y-1 pl-3 border-l-2 border-muted ml-2">
          <SortableContext
            items={node.children.map((c) => c.hierarchyId)}
            strategy={verticalListSortingStrategy}
          >
            {node.children.map((child) => (
              <TreeNodeComponent
                key={child.hierarchyId}
                node={child}
                selectedId={selectedId}
                selectedForFormula={selectedForFormula}
                selectedForScript={selectedForScript}
                expandedNodes={expandedNodes}
                expandedDetails={expandedDetails}
                onSelect={onSelect}
                onCheckboxToggle={onCheckboxToggle}
                onToggleExpand={onToggleExpand}
                onToggleDetails={onToggleDetails}
                activeId={activeId}
                overId={overId}
                dropAsChild={dropAsChild}
              />
            ))}
          </SortableContext>
        </div>
      )}
    </div>
  );
};
