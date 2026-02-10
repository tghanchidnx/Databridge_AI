/**
 * Import Diff View
 * Split-screen comparison of current vs incoming data with selective acceptance
 */
import { useState, useMemo, useCallback } from "react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import {
  Plus,
  Minus,
  Edit3,
  ChevronDown,
  ChevronRight,
  Check,
  X,
  ArrowRight,
  RotateCcw,
  CheckSquare,
  Square,
  Filter,
} from "lucide-react";

export interface DiffChange {
  id: string;
  type: "add" | "remove" | "modify";
  path: string[];
  field?: string;
  currentValue?: string | number | boolean | null;
  incomingValue?: string | number | boolean | null;
  children?: DiffChange[];
}

export interface DiffItem {
  id: string;
  hierarchyId: string;
  name: string;
  changeType: "add" | "remove" | "modify" | "unchanged";
  changes: DiffChange[];
  selected: boolean;
}

interface ImportDiffViewProps {
  items: DiffItem[];
  onSelectionChange: (itemId: string, selected: boolean) => void;
  onSelectAll: (selected: boolean) => void;
  onApply: (selectedIds: string[]) => void;
  onCancel: () => void;
  className?: string;
}

function getChangeIcon(type: DiffItem["changeType"]) {
  switch (type) {
    case "add":
      return <Plus className="h-4 w-4 text-green-600" />;
    case "remove":
      return <Minus className="h-4 w-4 text-red-600" />;
    case "modify":
      return <Edit3 className="h-4 w-4 text-yellow-600" />;
    default:
      return null;
  }
}

function getChangeBgColor(type: DiffItem["changeType"]) {
  switch (type) {
    case "add":
      return "bg-green-50 border-green-200";
    case "remove":
      return "bg-red-50 border-red-200";
    case "modify":
      return "bg-yellow-50 border-yellow-200";
    default:
      return "bg-background";
  }
}

function DiffValue({
  value,
  type,
}: {
  value: string | number | boolean | null | undefined;
  type: "current" | "incoming";
}) {
  const displayValue = value === null || value === undefined ? "(empty)" : String(value);
  const isEmpty = value === null || value === undefined || value === "";

  return (
    <span
      className={cn(
        "font-mono text-sm px-2 py-0.5 rounded",
        type === "current" && "bg-red-100 text-red-800 line-through",
        type === "incoming" && "bg-green-100 text-green-800",
        isEmpty && "text-muted-foreground italic"
      )}
    >
      {displayValue}
    </span>
  );
}

function ChangeRow({ change }: { change: DiffChange }) {
  const [expanded, setExpanded] = useState(true);
  const hasChildren = change.children && change.children.length > 0;

  return (
    <div className="space-y-1">
      <div className="flex items-center gap-2 py-1 hover:bg-muted/50 rounded px-2">
        {hasChildren ? (
          <button onClick={() => setExpanded(!expanded)} className="p-0.5">
            {expanded ? (
              <ChevronDown className="h-4 w-4 text-muted-foreground" />
            ) : (
              <ChevronRight className="h-4 w-4 text-muted-foreground" />
            )}
          </button>
        ) : (
          <span className="w-5" />
        )}

        <span className="flex-shrink-0">{getChangeIcon({ changeType: change.type } as any)}</span>

        <span className="text-sm font-medium">
          {change.field || change.path.join(" > ")}
        </span>

        {change.type === "modify" && (
          <div className="flex items-center gap-2 ml-auto">
            <DiffValue value={change.currentValue} type="current" />
            <ArrowRight className="h-4 w-4 text-muted-foreground" />
            <DiffValue value={change.incomingValue} type="incoming" />
          </div>
        )}

        {change.type === "add" && (
          <div className="ml-auto">
            <DiffValue value={change.incomingValue} type="incoming" />
          </div>
        )}

        {change.type === "remove" && (
          <div className="ml-auto">
            <DiffValue value={change.currentValue} type="current" />
          </div>
        )}
      </div>

      {hasChildren && expanded && (
        <div className="ml-6 border-l pl-2 space-y-1">
          {change.children!.map((child) => (
            <ChangeRow key={child.id} change={child} />
          ))}
        </div>
      )}
    </div>
  );
}

function DiffItemRow({
  item,
  onSelectionChange,
}: {
  item: DiffItem;
  onSelectionChange: (selected: boolean) => void;
}) {
  const [expanded, setExpanded] = useState(item.changeType !== "unchanged");

  return (
    <Collapsible open={expanded} onOpenChange={setExpanded}>
      <div
        className={cn(
          "border rounded-lg overflow-hidden",
          getChangeBgColor(item.changeType),
          item.selected && "ring-2 ring-primary"
        )}
      >
        {/* Header */}
        <div className="flex items-center gap-3 p-3">
          <Checkbox
            checked={item.selected}
            onCheckedChange={onSelectionChange}
            disabled={item.changeType === "unchanged"}
          />

          <CollapsibleTrigger asChild>
            <button className="flex items-center gap-2 flex-1 text-left">
              {expanded ? (
                <ChevronDown className="h-4 w-4 text-muted-foreground" />
              ) : (
                <ChevronRight className="h-4 w-4 text-muted-foreground" />
              )}

              {getChangeIcon(item.changeType)}

              <span className="font-medium">{item.name}</span>

              <Badge variant="outline" className="ml-2 text-xs font-mono">
                {item.hierarchyId}
              </Badge>

              {item.changes.length > 0 && (
                <Badge variant="secondary" className="ml-auto">
                  {item.changes.length} changes
                </Badge>
              )}
            </button>
          </CollapsibleTrigger>
        </div>

        {/* Changes */}
        <CollapsibleContent>
          {item.changes.length > 0 && (
            <div className="border-t bg-background/50 p-3 space-y-1">
              {item.changes.map((change) => (
                <ChangeRow key={change.id} change={change} />
              ))}
            </div>
          )}
        </CollapsibleContent>
      </div>
    </Collapsible>
  );
}

export function ImportDiffView({
  items,
  onSelectionChange,
  onSelectAll,
  onApply,
  onCancel,
  className,
}: ImportDiffViewProps) {
  const [filter, setFilter] = useState<"all" | "add" | "modify" | "remove">("all");

  const stats = useMemo(() => {
    return {
      total: items.length,
      added: items.filter((i) => i.changeType === "add").length,
      modified: items.filter((i) => i.changeType === "modify").length,
      removed: items.filter((i) => i.changeType === "remove").length,
      unchanged: items.filter((i) => i.changeType === "unchanged").length,
      selected: items.filter((i) => i.selected).length,
    };
  }, [items]);

  const filteredItems = useMemo(() => {
    if (filter === "all") return items.filter((i) => i.changeType !== "unchanged");
    return items.filter((i) => i.changeType === filter);
  }, [items, filter]);

  const allSelected = filteredItems.every((i) => i.selected || i.changeType === "unchanged");
  const someSelected = filteredItems.some((i) => i.selected);

  const handleSelectAllVisible = useCallback(() => {
    const newSelected = !allSelected;
    filteredItems.forEach((item) => {
      if (item.changeType !== "unchanged") {
        onSelectionChange(item.id, newSelected);
      }
    });
  }, [filteredItems, allSelected, onSelectionChange]);

  const selectedIds = items.filter((i) => i.selected).map((i) => i.id);

  return (
    <div className={cn("flex flex-col h-full", className)}>
      {/* Header with stats */}
      <div className="p-4 border-b space-y-4">
        <div className="flex items-center justify-between">
          <h3 className="font-semibold">Import Preview</h3>
          <div className="flex items-center gap-2">
            <Badge variant="outline" className="gap-1">
              <CheckSquare className="h-3 w-3" />
              {stats.selected} selected
            </Badge>
          </div>
        </div>

        {/* Filter buttons */}
        <div className="flex items-center gap-2">
          <Button
            variant={filter === "all" ? "secondary" : "ghost"}
            size="sm"
            onClick={() => setFilter("all")}
          >
            All Changes
            <Badge variant="outline" className="ml-1">
              {stats.added + stats.modified + stats.removed}
            </Badge>
          </Button>
          <Button
            variant={filter === "add" ? "secondary" : "ghost"}
            size="sm"
            onClick={() => setFilter("add")}
            className="text-green-600"
          >
            <Plus className="h-3 w-3 mr-1" />
            Added
            <Badge variant="outline" className="ml-1">
              {stats.added}
            </Badge>
          </Button>
          <Button
            variant={filter === "modify" ? "secondary" : "ghost"}
            size="sm"
            onClick={() => setFilter("modify")}
            className="text-yellow-600"
          >
            <Edit3 className="h-3 w-3 mr-1" />
            Modified
            <Badge variant="outline" className="ml-1">
              {stats.modified}
            </Badge>
          </Button>
          <Button
            variant={filter === "remove" ? "secondary" : "ghost"}
            size="sm"
            onClick={() => setFilter("remove")}
            className="text-red-600"
          >
            <Minus className="h-3 w-3 mr-1" />
            Removed
            <Badge variant="outline" className="ml-1">
              {stats.removed}
            </Badge>
          </Button>
        </div>

        {/* Select all */}
        <div className="flex items-center gap-2">
          <Button
            variant="ghost"
            size="sm"
            onClick={handleSelectAllVisible}
            className="gap-2"
          >
            {allSelected ? (
              <CheckSquare className="h-4 w-4" />
            ) : (
              <Square className="h-4 w-4" />
            )}
            {allSelected ? "Deselect All Visible" : "Select All Visible"}
          </Button>
        </div>
      </div>

      {/* Diff list */}
      <ScrollArea className="flex-1">
        <div className="p-4 space-y-3">
          {filteredItems.length === 0 ? (
            <div className="text-center py-8 text-muted-foreground">
              No changes to display
            </div>
          ) : (
            filteredItems.map((item) => (
              <DiffItemRow
                key={item.id}
                item={item}
                onSelectionChange={(selected) => onSelectionChange(item.id, selected)}
              />
            ))
          )}
        </div>
      </ScrollArea>

      {/* Footer actions */}
      <div className="p-4 border-t flex items-center justify-between">
        <div className="text-sm text-muted-foreground">
          {stats.selected} of {stats.added + stats.modified + stats.removed} changes selected
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" onClick={onCancel}>
            <X className="h-4 w-4 mr-2" />
            Cancel
          </Button>
          <Button
            onClick={() => onApply(selectedIds)}
            disabled={stats.selected === 0}
          >
            <Check className="h-4 w-4 mr-2" />
            Apply {stats.selected} Changes
          </Button>
        </div>
      </div>
    </div>
  );
}

// Helper to generate diff between two hierarchy structures
export function generateDiff(
  current: Array<{ id: string; hierarchyId: string; name: string; [key: string]: any }>,
  incoming: Array<{ id: string; hierarchyId: string; name: string; [key: string]: any }>
): DiffItem[] {
  const currentMap = new Map(current.map((h) => [h.hierarchyId, h]));
  const incomingMap = new Map(incoming.map((h) => [h.hierarchyId, h]));
  const diffItems: DiffItem[] = [];

  // Find additions and modifications
  incoming.forEach((inc) => {
    const curr = currentMap.get(inc.hierarchyId);

    if (!curr) {
      // New item
      diffItems.push({
        id: inc.id,
        hierarchyId: inc.hierarchyId,
        name: inc.name,
        changeType: "add",
        changes: Object.entries(inc)
          .filter(([key]) => !["id"].includes(key))
          .map(([key, value]) => ({
            id: `${inc.id}-${key}`,
            type: "add" as const,
            path: [key],
            field: key,
            incomingValue: value,
          })),
        selected: true,
      });
    } else {
      // Check for modifications
      const changes: DiffChange[] = [];
      Object.keys(inc).forEach((key) => {
        if (key === "id") return;
        if (JSON.stringify(curr[key]) !== JSON.stringify(inc[key])) {
          changes.push({
            id: `${inc.id}-${key}`,
            type: "modify",
            path: [key],
            field: key,
            currentValue: curr[key],
            incomingValue: inc[key],
          });
        }
      });

      if (changes.length > 0) {
        diffItems.push({
          id: inc.id,
          hierarchyId: inc.hierarchyId,
          name: inc.name,
          changeType: "modify",
          changes,
          selected: true,
        });
      }
    }
  });

  // Find removals
  current.forEach((curr) => {
    if (!incomingMap.has(curr.hierarchyId)) {
      diffItems.push({
        id: curr.id,
        hierarchyId: curr.hierarchyId,
        name: curr.name,
        changeType: "remove",
        changes: [],
        selected: false, // Don't select removals by default
      });
    }
  });

  return diffItems;
}
