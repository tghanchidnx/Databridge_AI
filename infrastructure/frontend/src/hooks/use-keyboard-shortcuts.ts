/**
 * Keyboard Shortcuts System
 * Global keyboard shortcut manager with customizable bindings
 */
import { useEffect, useCallback, useRef, useState } from "react";

export interface KeyboardShortcut {
  id: string;
  keys: string[]; // e.g., ["ctrl", "s"] or ["shift", "?"]
  description: string;
  category: string;
  action: () => void;
  enabled?: boolean;
  preventDefault?: boolean;
}

type ModifierKey = "ctrl" | "alt" | "shift" | "meta" | "cmd";

interface ShortcutMatch {
  modifiers: Set<ModifierKey>;
  key: string;
}

function parseShortcut(keys: string[]): ShortcutMatch {
  const modifiers = new Set<ModifierKey>();
  let key = "";

  for (const k of keys) {
    const lower = k.toLowerCase();
    if (lower === "ctrl" || lower === "control") {
      modifiers.add("ctrl");
    } else if (lower === "alt" || lower === "option") {
      modifiers.add("alt");
    } else if (lower === "shift") {
      modifiers.add("shift");
    } else if (lower === "meta" || lower === "cmd" || lower === "command" || lower === "win") {
      modifiers.add("meta");
    } else {
      key = lower;
    }
  }

  return { modifiers, key };
}

function matchesShortcut(event: KeyboardEvent, shortcut: ShortcutMatch): boolean {
  const eventModifiers = new Set<ModifierKey>();
  if (event.ctrlKey) eventModifiers.add("ctrl");
  if (event.altKey) eventModifiers.add("alt");
  if (event.shiftKey) eventModifiers.add("shift");
  if (event.metaKey) eventModifiers.add("meta");

  // Check modifiers match exactly
  if (eventModifiers.size !== shortcut.modifiers.size) return false;
  for (const mod of shortcut.modifiers) {
    if (!eventModifiers.has(mod)) return false;
  }

  // Check key matches
  const eventKey = event.key.toLowerCase();
  return eventKey === shortcut.key || event.code.toLowerCase() === `key${shortcut.key}`;
}

// Format shortcut for display
export function formatShortcut(keys: string[]): string {
  const isMac = typeof navigator !== "undefined" && /Mac|iPod|iPhone|iPad/.test(navigator.platform);

  return keys
    .map((key) => {
      const lower = key.toLowerCase();
      if (lower === "ctrl" || lower === "control") return isMac ? "^" : "Ctrl";
      if (lower === "alt" || lower === "option") return isMac ? "⌥" : "Alt";
      if (lower === "shift") return isMac ? "⇧" : "Shift";
      if (lower === "meta" || lower === "cmd" || lower === "command") return isMac ? "⌘" : "Win";
      if (lower === "enter" || lower === "return") return "↵";
      if (lower === "escape" || lower === "esc") return "Esc";
      if (lower === "backspace") return "⌫";
      if (lower === "delete") return "Del";
      if (lower === "arrowup") return "↑";
      if (lower === "arrowdown") return "↓";
      if (lower === "arrowleft") return "←";
      if (lower === "arrowright") return "→";
      if (lower === "space") return "Space";
      return key.toUpperCase();
    })
    .join(isMac ? "" : "+");
}

// Global shortcuts registry
const globalShortcuts = new Map<string, KeyboardShortcut>();

export function useKeyboardShortcuts(shortcuts: KeyboardShortcut[]) {
  const shortcutsRef = useRef<KeyboardShortcut[]>(shortcuts);
  shortcutsRef.current = shortcuts;

  useEffect(() => {
    // Register shortcuts
    shortcuts.forEach((shortcut) => {
      globalShortcuts.set(shortcut.id, shortcut);
    });

    const handleKeyDown = (event: KeyboardEvent) => {
      // Skip if user is typing in an input
      const target = event.target as HTMLElement;
      const isInputFocused =
        target.tagName === "INPUT" ||
        target.tagName === "TEXTAREA" ||
        target.isContentEditable;

      for (const shortcut of shortcutsRef.current) {
        if (shortcut.enabled === false) continue;

        const parsed = parseShortcut(shortcut.keys);

        // Allow shortcuts in inputs only if they have modifiers
        if (isInputFocused && parsed.modifiers.size === 0) continue;

        if (matchesShortcut(event, parsed)) {
          if (shortcut.preventDefault !== false) {
            event.preventDefault();
          }
          shortcut.action();
          return;
        }
      }
    };

    window.addEventListener("keydown", handleKeyDown);

    return () => {
      window.removeEventListener("keydown", handleKeyDown);
      shortcuts.forEach((shortcut) => {
        globalShortcuts.delete(shortcut.id);
      });
    };
  }, [shortcuts]);
}

// Hook for a single shortcut
export function useShortcut(
  keys: string[],
  action: () => void,
  options?: { enabled?: boolean; preventDefault?: boolean }
) {
  useEffect(() => {
    if (options?.enabled === false) return;

    const parsed = parseShortcut(keys);

    const handleKeyDown = (event: KeyboardEvent) => {
      const target = event.target as HTMLElement;
      const isInputFocused =
        target.tagName === "INPUT" ||
        target.tagName === "TEXTAREA" ||
        target.isContentEditable;

      if (isInputFocused && parsed.modifiers.size === 0) return;

      if (matchesShortcut(event, parsed)) {
        if (options?.preventDefault !== false) {
          event.preventDefault();
        }
        action();
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [keys, action, options?.enabled, options?.preventDefault]);
}

// Get all registered shortcuts grouped by category
export function useShortcutRegistry() {
  const [shortcuts, setShortcuts] = useState<Map<string, KeyboardShortcut[]>>(new Map());

  useEffect(() => {
    const grouped = new Map<string, KeyboardShortcut[]>();

    globalShortcuts.forEach((shortcut) => {
      const category = shortcut.category || "General";
      if (!grouped.has(category)) {
        grouped.set(category, []);
      }
      grouped.get(category)!.push(shortcut);
    });

    setShortcuts(grouped);
  }, []);

  return shortcuts;
}

// Default shortcuts for hierarchy builder
export function useHierarchyShortcuts({
  onSave,
  onUndo,
  onRedo,
  onDelete,
  onAddChild,
  onAddSibling,
  onExpand,
  onCollapse,
  onMoveUp,
  onMoveDown,
  onSearch,
  onHelp,
  enabled = true,
}: {
  onSave?: () => void;
  onUndo?: () => void;
  onRedo?: () => void;
  onDelete?: () => void;
  onAddChild?: () => void;
  onAddSibling?: () => void;
  onExpand?: () => void;
  onCollapse?: () => void;
  onMoveUp?: () => void;
  onMoveDown?: () => void;
  onSearch?: () => void;
  onHelp?: () => void;
  enabled?: boolean;
}) {
  const shortcuts: KeyboardShortcut[] = [
    {
      id: "save",
      keys: ["ctrl", "s"],
      description: "Save changes",
      category: "General",
      action: onSave || (() => {}),
      enabled: enabled && !!onSave,
    },
    {
      id: "undo",
      keys: ["ctrl", "z"],
      description: "Undo last action",
      category: "General",
      action: onUndo || (() => {}),
      enabled: enabled && !!onUndo,
    },
    {
      id: "redo",
      keys: ["ctrl", "shift", "z"],
      description: "Redo last action",
      category: "General",
      action: onRedo || (() => {}),
      enabled: enabled && !!onRedo,
    },
    {
      id: "search",
      keys: ["ctrl", "k"],
      description: "Open search",
      category: "General",
      action: onSearch || (() => {}),
      enabled: enabled && !!onSearch,
    },
    {
      id: "help",
      keys: ["shift", "/"],
      description: "Show keyboard shortcuts",
      category: "General",
      action: onHelp || (() => {}),
      enabled: enabled && !!onHelp,
    },
    {
      id: "delete",
      keys: ["delete"],
      description: "Delete selected item",
      category: "Hierarchy",
      action: onDelete || (() => {}),
      enabled: enabled && !!onDelete,
    },
    {
      id: "add-child",
      keys: ["ctrl", "enter"],
      description: "Add child hierarchy",
      category: "Hierarchy",
      action: onAddChild || (() => {}),
      enabled: enabled && !!onAddChild,
    },
    {
      id: "add-sibling",
      keys: ["enter"],
      description: "Add sibling hierarchy",
      category: "Hierarchy",
      action: onAddSibling || (() => {}),
      enabled: enabled && !!onAddSibling,
      preventDefault: false,
    },
    {
      id: "expand",
      keys: ["arrowright"],
      description: "Expand selected item",
      category: "Navigation",
      action: onExpand || (() => {}),
      enabled: enabled && !!onExpand,
    },
    {
      id: "collapse",
      keys: ["arrowleft"],
      description: "Collapse selected item",
      category: "Navigation",
      action: onCollapse || (() => {}),
      enabled: enabled && !!onCollapse,
    },
    {
      id: "move-up",
      keys: ["alt", "arrowup"],
      description: "Move item up",
      category: "Hierarchy",
      action: onMoveUp || (() => {}),
      enabled: enabled && !!onMoveUp,
    },
    {
      id: "move-down",
      keys: ["alt", "arrowdown"],
      description: "Move item down",
      category: "Hierarchy",
      action: onMoveDown || (() => {}),
      enabled: enabled && !!onMoveDown,
    },
  ].filter((s) => s.enabled);

  useKeyboardShortcuts(shortcuts);

  return shortcuts;
}
