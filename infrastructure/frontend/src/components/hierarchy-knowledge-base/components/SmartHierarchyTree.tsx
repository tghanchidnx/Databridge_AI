import React from "react";
import type { HierarchyTree } from "@/services/api/hierarchy";
import { Button } from "@/components/ui/button";
import { Trash2, CheckCircle, XCircle } from "lucide-react";

interface SmartHierarchyTreeProps {
  tree: HierarchyTree[];
  selectedId: string | null;
  onSelect: (id: string) => void;
  onDelete: (id: string) => void;
}

export const SmartHierarchyTree: React.FC<SmartHierarchyTreeProps> = ({
  tree,
  selectedId,
  onSelect,
  onDelete,
}) => {
  if (tree.length === 0) {
    return (
      <div className="text-center text-muted-foreground text-sm py-8">
        No hierarchies found. Create one to get started.
      </div>
    );
  }

  return (
    <div className="space-y-2">
      {tree.map((item) => (
        <div
          key={item.id}
          className={`
            p-3 rounded-lg border cursor-pointer transition-all
            ${
              selectedId === item.id
                ? "bg-primary/10 border-primary"
                : "bg-background hover:bg-accent/50 border-border"
            }
          `}
          onClick={() => onSelect(item.id)}
        >
          <div className="flex items-start justify-between gap-2">
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-2">
                <h4 className="font-medium text-sm truncate">{item.name}</h4>
                {item.flags.active_flag ? (
                  <CheckCircle className="w-3 h-3 text-green-500 shrink-0" />
                ) : (
                  <XCircle className="w-3 h-3 text-gray-400 shrink-0" />
                )}
              </div>
              {item.description && (
                <p className="text-xs text-muted-foreground mt-1 line-clamp-2">
                  {item.description}
                </p>
              )}
              <div className="flex items-center gap-3 mt-2 text-xs text-muted-foreground">
                <span>{item.levels.length} levels</span>
                <span>{item.mappingCount} mappings</span>
                {item.hasFormula && (
                  <span className="text-blue-500">Formula</span>
                )}
                {item.hasFilter && (
                  <span className="text-purple-500">Filter</span>
                )}
              </div>
            </div>
            <Button
              variant="ghost"
              size="sm"
              className="shrink-0"
              onClick={(e) => {
                e.stopPropagation();
                onDelete(item.id);
              }}
            >
              <Trash2 className="w-4 h-4" />
            </Button>
          </div>
        </div>
      ))}
    </div>
  );
};
