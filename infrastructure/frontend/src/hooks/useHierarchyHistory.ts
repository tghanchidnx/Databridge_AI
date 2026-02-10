import { useState, useCallback, useRef } from 'react';

export interface HistoryState<T> {
  data: T;
  timestamp: number;
  description: string;
}

export interface UseHierarchyHistoryReturn<T> {
  current: T;
  setCurrent: (data: T, description: string) => void;
  undo: () => HistoryState<T> | null;
  redo: () => HistoryState<T> | null;
  canUndo: boolean;
  canRedo: boolean;
  undoStack: HistoryState<T>[];
  redoStack: HistoryState<T>[];
  clearHistory: () => void;
  getUndoDescription: () => string | null;
  getRedoDescription: () => string | null;
}

const MAX_HISTORY = 10;

export function useHierarchyHistory<T>(initialData: T): UseHierarchyHistoryReturn<T> {
  const [current, setCurrentState] = useState<T>(initialData);
  const [undoStack, setUndoStack] = useState<HistoryState<T>[]>([]);
  const [redoStack, setRedoStack] = useState<HistoryState<T>[]>([]);
  const isInitialized = useRef(false);

  const setCurrent = useCallback((data: T, description: string) => {
    setCurrentState((prev) => {
      // Only add to history if we have previous data
      if (isInitialized.current) {
        const historyEntry: HistoryState<T> = {
          data: prev,
          timestamp: Date.now(),
          description,
        };

        setUndoStack((stack) => {
          const newStack = [...stack, historyEntry];
          // Keep only last MAX_HISTORY items
          return newStack.slice(-MAX_HISTORY);
        });

        // Clear redo stack on new change
        setRedoStack([]);
      } else {
        isInitialized.current = true;
      }

      return data;
    });
  }, []);

  const undo = useCallback((): HistoryState<T> | null => {
    if (undoStack.length === 0) return null;

    const lastState = undoStack[undoStack.length - 1];

    // Save current state to redo stack
    setRedoStack((stack) => {
      const redoEntry: HistoryState<T> = {
        data: current,
        timestamp: Date.now(),
        description: `Redo: ${lastState.description}`,
      };
      return [...stack, redoEntry].slice(-MAX_HISTORY);
    });

    // Remove from undo stack
    setUndoStack((stack) => stack.slice(0, -1));

    // Restore previous state
    setCurrentState(lastState.data);

    return lastState;
  }, [undoStack, current]);

  const redo = useCallback((): HistoryState<T> | null => {
    if (redoStack.length === 0) return null;

    const nextState = redoStack[redoStack.length - 1];

    // Save current state to undo stack
    setUndoStack((stack) => {
      const undoEntry: HistoryState<T> = {
        data: current,
        timestamp: Date.now(),
        description: nextState.description.replace('Redo: ', ''),
      };
      return [...stack, undoEntry].slice(-MAX_HISTORY);
    });

    // Remove from redo stack
    setRedoStack((stack) => stack.slice(0, -1));

    // Apply next state
    setCurrentState(nextState.data);

    return nextState;
  }, [redoStack, current]);

  const clearHistory = useCallback(() => {
    setUndoStack([]);
    setRedoStack([]);
  }, []);

  const getUndoDescription = useCallback((): string | null => {
    if (undoStack.length === 0) return null;
    return undoStack[undoStack.length - 1].description;
  }, [undoStack]);

  const getRedoDescription = useCallback((): string | null => {
    if (redoStack.length === 0) return null;
    return redoStack[redoStack.length - 1].description.replace('Redo: ', '');
  }, [redoStack]);

  return {
    current,
    setCurrent,
    undo,
    redo,
    canUndo: undoStack.length > 0,
    canRedo: redoStack.length > 0,
    undoStack,
    redoStack,
    clearHistory,
    getUndoDescription,
    getRedoDescription,
  };
}
