import { useState, useEffect, useCallback } from "react";
import type { GraphNode, GraphTree } from "@/types/hierarchybuilder";

export interface HistoryState {
  nodes: GraphNode[];
  timestamp: number;
  action: string;
}

export const useUndoRedo = (
  initialNodes: GraphNode[],
  onRestore: (nodes: GraphNode[]) => Promise<void>
) => {
  const [past, setPast] = useState<HistoryState[]>([]);
  const [future, setFuture] = useState<HistoryState[]>([]);
  const [currentNodes, setCurrentNodes] = useState<GraphNode[]>(initialNodes);

  // Update current nodes when initial changes
  useEffect(() => {
    setCurrentNodes(initialNodes);
  }, [initialNodes]);

  // Save state to history
  const saveState = useCallback(
    (nodes: GraphNode[], action: string) => {
      const newState: HistoryState = {
        nodes: JSON.parse(JSON.stringify(currentNodes)), // Deep clone previous state
        timestamp: Date.now(),
        action,
      };

      setPast((prev) => [...prev, newState]);
      setFuture([]); // Clear future when new action is performed
      setCurrentNodes(nodes);
    },
    [currentNodes]
  );

  // Undo operation
  const undo = useCallback(async () => {
    if (past.length === 0) {
      console.log("Nothing to undo");
      return false;
    }

    const previousState = past[past.length - 1];
    const newPast = past.slice(0, -1);

    // Save current state to future
    setFuture((prev) => [
      ...prev,
      {
        nodes: JSON.parse(JSON.stringify(currentNodes)),
        timestamp: Date.now(),
        action: "redo-point",
      },
    ]);

    setPast(newPast);
    setCurrentNodes(previousState.nodes);

    await onRestore(previousState.nodes);
    return true;
  }, [past, currentNodes, onRestore]);

  // Redo operation
  const redo = useCallback(async () => {
    if (future.length === 0) {
      console.log("Nothing to redo");
      return false;
    }

    const nextState = future[future.length - 1];
    const newFuture = future.slice(0, -1);

    // Save current state to past
    setPast((prev) => [
      ...prev,
      {
        nodes: JSON.parse(JSON.stringify(currentNodes)),
        timestamp: Date.now(),
        action: "undo-point",
      },
    ]);

    setFuture(newFuture);
    setCurrentNodes(nextState.nodes);

    await onRestore(nextState.nodes);
    return true;
  }, [future, currentNodes, onRestore]);

  // Clear history
  const clearHistory = useCallback(() => {
    setPast([]);
    setFuture([]);
  }, []);

  const canUndo = past.length > 0;
  const canRedo = future.length > 0;

  return {
    saveState,
    undo,
    redo,
    clearHistory,
    canUndo,
    canRedo,
    historySize: past.length,
  };
};

// Keyboard shortcuts hook
export const useUndoRedoShortcuts = (undo: () => void, redo: () => void) => {
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // Ctrl+Z or Cmd+Z for undo
      if ((e.ctrlKey || e.metaKey) && e.key === "z" && !e.shiftKey) {
        e.preventDefault();
        undo();
      }

      // Ctrl+Shift+Z or Cmd+Shift+Z or Ctrl+Y for redo
      if (
        ((e.ctrlKey || e.metaKey) && e.shiftKey && e.key === "z") ||
        (e.ctrlKey && e.key === "y")
      ) {
        e.preventDefault();
        redo();
      }
    };

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [undo, redo]);
};
