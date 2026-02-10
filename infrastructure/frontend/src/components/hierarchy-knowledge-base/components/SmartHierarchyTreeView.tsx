import React, { useMemo } from "react";
import type { SmartHierarchyMaster } from "@/services/api/hierarchy";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Trash2,
  CheckCircle,
  XCircle,
  ChevronRight,
  ChevronDown,
  Layers,
  Database,
} from "lucide-react";
import { cn } from "@/lib/utils";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";

interface SmartHierarchyTreeViewProps {
  hierarchies: SmartHierarchyMaster[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  onDelete: (id: string) => void;
}

interface TreeNode extends SmartHierarchyMaster {
  children: TreeNode[];
  level: number;
}

export const SmartHierarchyTreeView: React.FC<SmartHierarchyTreeViewProps> = ({
  hierarchies,
  selectedId,
  onSelect,
  onDelete,
}) => {
  // Build tree structure
  const treeData = useMemo(() => {
    const buildTree = (items: SmartHierarchyMaster[]): TreeNode[] => {
      const roots: TreeNode[] = [];
      const childMap = new Map<string, SmartHierarchyMaster[]>();

      // Group by parent
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

      // Recursive function to build children
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

      // Build tree for all roots
      roots.forEach((root) => addChildren(root, 0));

      // Sort roots by sortOrder
      return roots.sort((a, b) => (a.sortOrder || 0) - (b.sortOrder || 0));
    };

    return buildTree(hierarchies);
  }, [hierarchies]);

  if (treeData.length === 0) {
    return (
      <div className="text-center text-muted-foreground text-sm py-8">
        No hierarchies found. Create one to get started.
      </div>
    );
  }

  return (
    <div className="space-y-2">
      {treeData.map((node) => (
        <TreeNodeComponent
          key={node.hierarchyId}
          node={node}
          selectedId={selectedId}
          onSelect={onSelect}
          onDelete={onDelete}
        />
      ))}
    </div>
  );
};

// Tree Node Component
const TreeNodeComponent: React.FC<{
  node: TreeNode;
  selectedId: string | null;
  onSelect: (id: string) => void;
  onDelete: (id: string) => void;
}> = ({ node, selectedId, onSelect, onDelete }) => {
  const [isChildrenExpanded, setIsChildrenExpanded] = React.useState(false);
  const [isDetailsExpanded, setIsDetailsExpanded] = React.useState(false);
  const hasChildren = node.children && node.children.length > 0;
  const isSelected = node.hierarchyId === selectedId;

  const levelCount = Object.values(node.hierarchyLevel || {}).filter(
    Boolean
  ).length;
  const mappingCount = Array.isArray(node.mapping) ? node.mapping.length : 0;

  return (
    <div className={cn("space-y-1", node.level > 0 && "ml-6")}>
      <div
        className={cn(
          "group rounded-lg border transition-all overflow-hidden",
          isSelected
            ? "bg-primary/10 border-primary shadow-sm"
            : "bg-card hover:bg-accent/50 border-border"
        )}
      >
        {/* Main Row - Always Visible */}
        <div
          className="flex items-center gap-2 px-3 py-2 cursor-pointer"
          onClick={() => onSelect(node.hierarchyId)}
        >
          {/* Child Expand/Collapse Icon */}
          {hasChildren ? (
            <Button
              variant="ghost"
              size="sm"
              className="h-5 w-5 p-0 shrink-0"
              onClick={(e) => {
                e.stopPropagation();
                setIsChildrenExpanded(!isChildrenExpanded);
              }}
            >
              {isChildrenExpanded ? (
                <ChevronDown className="w-3.5 h-3.5" />
              ) : (
                <ChevronRight className="w-3.5 h-3.5" />
              )}
            </Button>
          ) : (
            <div className="w-5" />
          )}

          {/* Name */}
          <h4 className="font-medium text-sm truncate flex-1 min-w-0">
            {node.hierarchyName}
          </h4>

          {/* Compact Stats */}
          <div className="flex items-center gap-2 shrink-0">
            <span className="text-xs text-muted-foreground flex items-center gap-1">
              <Layers className="w-3 h-3" />
              {levelCount}
            </span>
            <span className="text-xs text-muted-foreground flex items-center gap-1">
              <Database className="w-3 h-3" />
              {mappingCount}
            </span>
            {node.flags?.active_flag ? (
              <CheckCircle className="w-3.5 h-3.5 text-green-500" />
            ) : (
              <XCircle className="w-3.5 h-3.5 text-gray-400" />
            )}
          </div>

          {/* Details Expand/Collapse */}
          <Button
            variant="ghost"
            size="sm"
            className="h-5 w-5 p-0 shrink-0"
            onClick={(e) => {
              e.stopPropagation();
              setIsDetailsExpanded(!isDetailsExpanded);
            }}
          >
            {isDetailsExpanded ? (
              <ChevronDown className="w-3.5 h-3.5" />
            ) : (
              <ChevronRight className="w-3.5 h-3.5" />
            )}
          </Button>

          {/* Delete Button */}
          <Button
            variant="ghost"
            size="sm"
            className="shrink-0 opacity-0 group-hover:opacity-100 h-5 w-5 p-0"
            onClick={(e) => {
              e.stopPropagation();
              onDelete(node.hierarchyId);
            }}
          >
            <Trash2 className="w-3 h-3 text-destructive" />
          </Button>
        </div>

        {/* Expanded Details - Hidden by Default */}
        {isDetailsExpanded && (
          <div className="px-3 pb-2 pt-1 space-y-2 border-t bg-muted/30">
            {/* Description */}
            {node.description && (
              <div>
                <p className="text-xs font-medium text-muted-foreground mb-1">
                  Description:
                </p>
                <p className="text-xs text-foreground">{node.description}</p>
              </div>
            )}

            {/* Levels */}
            {levelCount > 0 && (
              <div>
                <p className="text-xs font-medium text-muted-foreground mb-1">
                  Levels ({levelCount}):
                </p>
                <div className="flex flex-wrap gap-1">
                  {Object.entries(node.hierarchyLevel || {})
                    .filter(([_, value]) => value)
                    .map(([key, value]) => (
                      <Badge key={key} variant="secondary" className="text-xs">
                        {value}
                      </Badge>
                    ))}
                </div>
              </div>
            )}

            {/* Sources */}
            {mappingCount > 0 && (
              <div>
                <p className="text-xs font-medium text-muted-foreground mb-1">
                  Sources ({mappingCount}):
                </p>
                <div className="space-y-1">
                  {node.mapping?.slice(0, 3).map((m, i) => (
                    <div key={i} className="text-xs text-foreground">
                      {m.source_database}.{m.source_schema}.{m.source_table}
                    </div>
                  ))}
                  {mappingCount > 3 && (
                    <div className="text-xs text-muted-foreground">
                      +{mappingCount - 3} more...
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* Meta Info */}
            <div className="flex items-center gap-3 text-xs text-muted-foreground pt-1">
              {node.isRoot && (
                <Badge variant="outline" className="text-xs">
                  Root
                </Badge>
              )}
              {hasChildren && (
                <span className="text-primary">
                  {node.children.length} child
                  {node.children.length !== 1 ? "ren" : ""}
                </span>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Children Hierarchies */}
      {hasChildren && isChildrenExpanded && (
        <div className="space-y-1 pl-3 border-l-2 border-muted ml-2">
          {node.children.map((child) => (
            <TreeNodeComponent
              key={child.hierarchyId}
              node={child}
              selectedId={selectedId}
              onSelect={onSelect}
              onDelete={onDelete}
            />
          ))}
        </div>
      )}
    </div>
  );
};
