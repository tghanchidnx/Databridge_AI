/**
 * Keyboard Shortcuts Modal
 * Displays all available keyboard shortcuts organized by category
 */
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "./dialog";
import { ScrollArea } from "./scroll-area";
import { Badge } from "./badge";
import { Keyboard } from "lucide-react";
import { formatShortcut, type KeyboardShortcut } from "@/hooks/use-keyboard-shortcuts";
import { cn } from "@/lib/utils";

interface KeyboardShortcutsModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  shortcuts?: KeyboardShortcut[];
}

// Default shortcuts to display
const defaultShortcuts: KeyboardShortcut[] = [
  // General
  { id: "save", keys: ["ctrl", "s"], description: "Save changes", category: "General", action: () => {} },
  { id: "undo", keys: ["ctrl", "z"], description: "Undo last action", category: "General", action: () => {} },
  { id: "redo", keys: ["ctrl", "shift", "z"], description: "Redo last action", category: "General", action: () => {} },
  { id: "search", keys: ["ctrl", "k"], description: "Open search", category: "General", action: () => {} },
  { id: "help", keys: ["shift", "/"], description: "Show keyboard shortcuts", category: "General", action: () => {} },
  { id: "escape", keys: ["escape"], description: "Close dialog / Cancel", category: "General", action: () => {} },

  // Hierarchy
  { id: "delete", keys: ["delete"], description: "Delete selected item", category: "Hierarchy", action: () => {} },
  { id: "add-child", keys: ["ctrl", "enter"], description: "Add child hierarchy", category: "Hierarchy", action: () => {} },
  { id: "add-sibling", keys: ["enter"], description: "Add sibling hierarchy", category: "Hierarchy", action: () => {} },
  { id: "move-up", keys: ["alt", "arrowup"], description: "Move item up", category: "Hierarchy", action: () => {} },
  { id: "move-down", keys: ["alt", "arrowdown"], description: "Move item down", category: "Hierarchy", action: () => {} },
  { id: "duplicate", keys: ["ctrl", "d"], description: "Duplicate selected", category: "Hierarchy", action: () => {} },

  // Navigation
  { id: "nav-up", keys: ["arrowup"], description: "Move selection up", category: "Navigation", action: () => {} },
  { id: "nav-down", keys: ["arrowdown"], description: "Move selection down", category: "Navigation", action: () => {} },
  { id: "expand", keys: ["arrowright"], description: "Expand selected item", category: "Navigation", action: () => {} },
  { id: "collapse", keys: ["arrowleft"], description: "Collapse selected item", category: "Navigation", action: () => {} },
  { id: "home", keys: ["home"], description: "Go to first item", category: "Navigation", action: () => {} },
  { id: "end", keys: ["end"], description: "Go to last item", category: "Navigation", action: () => {} },

  // View
  { id: "toggle-sidebar", keys: ["ctrl", "b"], description: "Toggle sidebar", category: "View", action: () => {} },
  { id: "toggle-panel", keys: ["ctrl", "\\"], description: "Toggle details panel", category: "View", action: () => {} },
  { id: "zoom-in", keys: ["ctrl", "="], description: "Zoom in", category: "View", action: () => {} },
  { id: "zoom-out", keys: ["ctrl", "-"], description: "Zoom out", category: "View", action: () => {} },
];

function ShortcutKey({ children }: { children: React.ReactNode }) {
  return (
    <kbd className="px-2 py-1 text-xs font-semibold text-muted-foreground bg-muted border border-border rounded shadow-sm">
      {children}
    </kbd>
  );
}

export function KeyboardShortcutsModal({
  open,
  onOpenChange,
  shortcuts = defaultShortcuts,
}: KeyboardShortcutsModalProps) {
  // Group shortcuts by category
  const groupedShortcuts = shortcuts.reduce((acc, shortcut) => {
    const category = shortcut.category || "General";
    if (!acc[category]) {
      acc[category] = [];
    }
    acc[category].push(shortcut);
    return acc;
  }, {} as Record<string, KeyboardShortcut[]>);

  const categoryOrder = ["General", "Hierarchy", "Navigation", "View"];
  const sortedCategories = Object.keys(groupedShortcuts).sort((a, b) => {
    const indexA = categoryOrder.indexOf(a);
    const indexB = categoryOrder.indexOf(b);
    if (indexA === -1 && indexB === -1) return a.localeCompare(b);
    if (indexA === -1) return 1;
    if (indexB === -1) return -1;
    return indexA - indexB;
  });

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl max-h-[80vh]">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Keyboard className="h-5 w-5" />
            Keyboard Shortcuts
          </DialogTitle>
          <DialogDescription>
            Use these shortcuts to navigate and perform actions quickly
          </DialogDescription>
        </DialogHeader>

        <ScrollArea className="h-[50vh] pr-4">
          <div className="space-y-6">
            {sortedCategories.map((category) => (
              <div key={category}>
                <div className="flex items-center gap-2 mb-3">
                  <h3 className="text-sm font-semibold text-foreground">
                    {category}
                  </h3>
                  <Badge variant="secondary" className="text-xs">
                    {groupedShortcuts[category].length}
                  </Badge>
                </div>
                <div className="space-y-2">
                  {groupedShortcuts[category].map((shortcut) => (
                    <div
                      key={shortcut.id}
                      className="flex items-center justify-between py-2 px-3 rounded-lg hover:bg-muted/50 transition-colors"
                    >
                      <span className="text-sm text-muted-foreground">
                        {shortcut.description}
                      </span>
                      <div className="flex items-center gap-1">
                        {shortcut.keys.map((key, index) => (
                          <span key={index} className="flex items-center">
                            {index > 0 && (
                              <span className="text-muted-foreground/50 mx-1">+</span>
                            )}
                            <ShortcutKey>
                              {formatShortcut([key])}
                            </ShortcutKey>
                          </span>
                        ))}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </ScrollArea>

        <div className="pt-4 border-t">
          <p className="text-xs text-muted-foreground text-center">
            Press <ShortcutKey>?</ShortcutKey> anytime to show this dialog
          </p>
        </div>
      </DialogContent>
    </Dialog>
  );
}
