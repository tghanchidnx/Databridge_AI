/**
 * Floating Toolbar Component
 * Shows contextual quick actions when items are selected
 */
import { cn } from "@/lib/utils";
import { Button } from "./button";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "./tooltip";
import { Separator } from "./separator";
import {
  Edit,
  Copy,
  Trash2,
  Plus,
  ArrowUp,
  ArrowDown,
  ChevronRight,
  Database,
  Calculator,
  MoreHorizontal,
  X,
  type LucideIcon,
} from "lucide-react";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "./dropdown-menu";
import { motion, AnimatePresence } from "framer-motion";
import { formatShortcut } from "@/hooks/use-keyboard-shortcuts";

export interface ToolbarAction {
  id: string;
  icon: LucideIcon;
  label: string;
  onClick: () => void;
  shortcut?: string[];
  disabled?: boolean;
  variant?: "default" | "destructive";
}

export interface ToolbarGroup {
  id: string;
  actions: ToolbarAction[];
}

interface FloatingToolbarProps {
  visible: boolean;
  onClose?: () => void;
  selectedCount?: number;
  selectedLabel?: string;
  groups?: ToolbarGroup[];
  position?: "top" | "bottom";
  className?: string;
}

// Default hierarchy actions
export const defaultHierarchyActions: ToolbarGroup[] = [
  {
    id: "edit",
    actions: [
      {
        id: "edit",
        icon: Edit,
        label: "Edit",
        onClick: () => {},
        shortcut: ["Enter"],
      },
      {
        id: "duplicate",
        icon: Copy,
        label: "Duplicate",
        onClick: () => {},
        shortcut: ["Ctrl", "D"],
      },
    ],
  },
  {
    id: "hierarchy",
    actions: [
      {
        id: "add-child",
        icon: Plus,
        label: "Add Child",
        onClick: () => {},
        shortcut: ["Ctrl", "Enter"],
      },
      {
        id: "add-sibling",
        icon: ChevronRight,
        label: "Add Sibling",
        onClick: () => {},
      },
    ],
  },
  {
    id: "move",
    actions: [
      {
        id: "move-up",
        icon: ArrowUp,
        label: "Move Up",
        onClick: () => {},
        shortcut: ["Alt", "↑"],
      },
      {
        id: "move-down",
        icon: ArrowDown,
        label: "Move Down",
        onClick: () => {},
        shortcut: ["Alt", "↓"],
      },
    ],
  },
  {
    id: "data",
    actions: [
      {
        id: "add-mapping",
        icon: Database,
        label: "Add Mapping",
        onClick: () => {},
      },
      {
        id: "add-formula",
        icon: Calculator,
        label: "Add Formula",
        onClick: () => {},
      },
    ],
  },
  {
    id: "delete",
    actions: [
      {
        id: "delete",
        icon: Trash2,
        label: "Delete",
        onClick: () => {},
        shortcut: ["Delete"],
        variant: "destructive",
      },
    ],
  },
];

export function FloatingToolbar({
  visible,
  onClose,
  selectedCount = 1,
  selectedLabel,
  groups = defaultHierarchyActions,
  position = "bottom",
  className,
}: FloatingToolbarProps) {
  // Flatten actions for mobile dropdown
  const allActions = groups.flatMap((g) => g.actions);
  const primaryActions = allActions.slice(0, 4);
  const moreActions = allActions.slice(4);

  return (
    <AnimatePresence>
      {visible && (
        <motion.div
          initial={{ opacity: 0, y: position === "bottom" ? 20 : -20, scale: 0.95 }}
          animate={{ opacity: 1, y: 0, scale: 1 }}
          exit={{ opacity: 0, y: position === "bottom" ? 20 : -20, scale: 0.95 }}
          transition={{ duration: 0.15, ease: "easeOut" }}
          className={cn(
            "fixed left-1/2 -translate-x-1/2 z-50",
            position === "bottom" ? "bottom-6" : "top-20",
            className
          )}
        >
          <TooltipProvider delayDuration={300}>
            <div className="flex items-center gap-1 px-2 py-1.5 bg-background/95 backdrop-blur-sm border border-border rounded-lg shadow-lg">
              {/* Selection info */}
              {selectedLabel && (
                <>
                  <div className="px-3 py-1.5 text-sm font-medium text-muted-foreground">
                    {selectedCount > 1 ? `${selectedCount} selected` : selectedLabel}
                  </div>
                  <Separator orientation="vertical" className="h-6 mx-1" />
                </>
              )}

              {/* Desktop: Show all action groups */}
              <div className="hidden md:flex items-center gap-1">
                {groups.map((group, groupIndex) => (
                  <div key={group.id} className="flex items-center">
                    {groupIndex > 0 && (
                      <Separator orientation="vertical" className="h-6 mx-1" />
                    )}
                    {group.actions.map((action) => (
                      <Tooltip key={action.id}>
                        <TooltipTrigger asChild>
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={action.onClick}
                            disabled={action.disabled}
                            className={cn(
                              "h-8 w-8 p-0",
                              action.variant === "destructive" &&
                                "text-destructive hover:text-destructive hover:bg-destructive/10"
                            )}
                          >
                            <action.icon className="h-4 w-4" />
                          </Button>
                        </TooltipTrigger>
                        <TooltipContent side="top" className="flex items-center gap-2">
                          <span>{action.label}</span>
                          {action.shortcut && (
                            <kbd className="ml-2 px-1.5 py-0.5 text-xs bg-muted rounded">
                              {formatShortcut(action.shortcut)}
                            </kbd>
                          )}
                        </TooltipContent>
                      </Tooltip>
                    ))}
                  </div>
                ))}
              </div>

              {/* Mobile: Show primary actions + more menu */}
              <div className="flex md:hidden items-center gap-1">
                {primaryActions.map((action) => (
                  <Tooltip key={action.id}>
                    <TooltipTrigger asChild>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={action.onClick}
                        disabled={action.disabled}
                        className={cn(
                          "h-8 w-8 p-0",
                          action.variant === "destructive" &&
                            "text-destructive hover:text-destructive hover:bg-destructive/10"
                        )}
                      >
                        <action.icon className="h-4 w-4" />
                      </Button>
                    </TooltipTrigger>
                    <TooltipContent side="top">{action.label}</TooltipContent>
                  </Tooltip>
                ))}

                {moreActions.length > 0 && (
                  <DropdownMenu>
                    <DropdownMenuTrigger asChild>
                      <Button variant="ghost" size="sm" className="h-8 w-8 p-0">
                        <MoreHorizontal className="h-4 w-4" />
                      </Button>
                    </DropdownMenuTrigger>
                    <DropdownMenuContent align="end">
                      {moreActions.map((action, index) => (
                        <div key={action.id}>
                          {index > 0 && action.variant === "destructive" && (
                            <DropdownMenuSeparator />
                          )}
                          <DropdownMenuItem
                            onClick={action.onClick}
                            disabled={action.disabled}
                            className={cn(
                              action.variant === "destructive" && "text-destructive"
                            )}
                          >
                            <action.icon className="h-4 w-4 mr-2" />
                            {action.label}
                            {action.shortcut && (
                              <kbd className="ml-auto text-xs text-muted-foreground">
                                {formatShortcut(action.shortcut)}
                              </kbd>
                            )}
                          </DropdownMenuItem>
                        </div>
                      ))}
                    </DropdownMenuContent>
                  </DropdownMenu>
                )}
              </div>

              {/* Close button */}
              {onClose && (
                <>
                  <Separator orientation="vertical" className="h-6 mx-1" />
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={onClose}
                        className="h-8 w-8 p-0"
                      >
                        <X className="h-4 w-4" />
                      </Button>
                    </TooltipTrigger>
                    <TooltipContent side="top">
                      Close <kbd className="ml-1 text-xs">Esc</kbd>
                    </TooltipContent>
                  </Tooltip>
                </>
              )}
            </div>
          </TooltipProvider>
        </motion.div>
      )}
    </AnimatePresence>
  );
}

// Hook to create toolbar actions with handlers
export function useFloatingToolbar(handlers: {
  onEdit?: () => void;
  onDuplicate?: () => void;
  onAddChild?: () => void;
  onAddSibling?: () => void;
  onMoveUp?: () => void;
  onMoveDown?: () => void;
  onAddMapping?: () => void;
  onAddFormula?: () => void;
  onDelete?: () => void;
}) {
  const groups: ToolbarGroup[] = [
    {
      id: "edit",
      actions: [
        {
          id: "edit",
          icon: Edit,
          label: "Edit",
          onClick: handlers.onEdit || (() => {}),
          shortcut: ["Enter"],
          disabled: !handlers.onEdit,
        },
        {
          id: "duplicate",
          icon: Copy,
          label: "Duplicate",
          onClick: handlers.onDuplicate || (() => {}),
          shortcut: ["Ctrl", "D"],
          disabled: !handlers.onDuplicate,
        },
      ],
    },
    {
      id: "hierarchy",
      actions: [
        {
          id: "add-child",
          icon: Plus,
          label: "Add Child",
          onClick: handlers.onAddChild || (() => {}),
          shortcut: ["Ctrl", "Enter"],
          disabled: !handlers.onAddChild,
        },
        {
          id: "add-sibling",
          icon: ChevronRight,
          label: "Add Sibling",
          onClick: handlers.onAddSibling || (() => {}),
          disabled: !handlers.onAddSibling,
        },
      ],
    },
    {
      id: "move",
      actions: [
        {
          id: "move-up",
          icon: ArrowUp,
          label: "Move Up",
          onClick: handlers.onMoveUp || (() => {}),
          shortcut: ["Alt", "↑"],
          disabled: !handlers.onMoveUp,
        },
        {
          id: "move-down",
          icon: ArrowDown,
          label: "Move Down",
          onClick: handlers.onMoveDown || (() => {}),
          shortcut: ["Alt", "↓"],
          disabled: !handlers.onMoveDown,
        },
      ],
    },
    {
      id: "data",
      actions: [
        {
          id: "add-mapping",
          icon: Database,
          label: "Add Mapping",
          onClick: handlers.onAddMapping || (() => {}),
          disabled: !handlers.onAddMapping,
        },
        {
          id: "add-formula",
          icon: Calculator,
          label: "Add Formula",
          onClick: handlers.onAddFormula || (() => {}),
          disabled: !handlers.onAddFormula,
        },
      ],
    },
    {
      id: "delete",
      actions: [
        {
          id: "delete",
          icon: Trash2,
          label: "Delete",
          onClick: handlers.onDelete || (() => {}),
          shortcut: ["Delete"],
          variant: "destructive",
          disabled: !handlers.onDelete,
        },
      ],
    },
  ];

  return groups.filter((g) => g.actions.some((a) => !a.disabled));
}
