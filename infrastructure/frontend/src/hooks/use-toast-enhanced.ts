/**
 * Enhanced Toast Hook with Undo Support
 * Provides toast notifications with action buttons and undo capability
 */
import { toast as sonnerToast } from "sonner";
import { useCallback, useRef } from "react";

export type ToastVariant = "default" | "success" | "error" | "warning" | "info";

export interface ToastAction {
  label: string;
  onClick: () => void | Promise<void>;
}

export interface EnhancedToastOptions {
  title: string;
  description?: string;
  variant?: ToastVariant;
  duration?: number;
  action?: ToastAction;
  onUndo?: () => void | Promise<void>;
  undoLabel?: string;
  dismissible?: boolean;
  id?: string;
}

export interface UndoableAction<T = unknown> {
  execute: () => T | Promise<T>;
  undo: () => void | Promise<void>;
  successMessage: string;
  errorMessage?: string;
}

export function useEnhancedToast() {
  const pendingUndoRef = useRef<Map<string, NodeJS.Timeout>>(new Map());

  const showToast = useCallback(({
    title,
    description,
    variant = "default",
    duration = 5000,
    action,
    onUndo,
    undoLabel = "Undo",
    dismissible = true,
    id,
  }: EnhancedToastOptions) => {
    const toastId = id || `toast-${Date.now()}`;

    const toastOptions: Parameters<typeof sonnerToast>[1] = {
      id: toastId,
      description,
      duration,
      dismissible,
      action: onUndo ? {
        label: undoLabel,
        onClick: async () => {
          // Clear any pending timeout
          const timeout = pendingUndoRef.current.get(toastId);
          if (timeout) {
            clearTimeout(timeout);
            pendingUndoRef.current.delete(toastId);
          }

          try {
            await onUndo();
            sonnerToast.success("Action undone", { duration: 2000 });
          } catch (error) {
            sonnerToast.error("Failed to undo", { duration: 2000 });
          }
        },
      } : action ? {
        label: action.label,
        onClick: action.onClick,
      } : undefined,
    };

    switch (variant) {
      case "success":
        sonnerToast.success(title, toastOptions);
        break;
      case "error":
        sonnerToast.error(title, toastOptions);
        break;
      case "warning":
        sonnerToast.warning(title, toastOptions);
        break;
      case "info":
        sonnerToast.info(title, toastOptions);
        break;
      default:
        sonnerToast(title, toastOptions);
    }

    return toastId;
  }, []);

  const success = useCallback((title: string, options?: Omit<EnhancedToastOptions, "title" | "variant">) => {
    return showToast({ title, variant: "success", ...options });
  }, [showToast]);

  const error = useCallback((title: string, options?: Omit<EnhancedToastOptions, "title" | "variant">) => {
    return showToast({ title, variant: "error", duration: 8000, ...options });
  }, [showToast]);

  const warning = useCallback((title: string, options?: Omit<EnhancedToastOptions, "title" | "variant">) => {
    return showToast({ title, variant: "warning", ...options });
  }, [showToast]);

  const info = useCallback((title: string, options?: Omit<EnhancedToastOptions, "title" | "variant">) => {
    return showToast({ title, variant: "info", ...options });
  }, [showToast]);

  const loading = useCallback((title: string, options?: Omit<EnhancedToastOptions, "title" | "variant">) => {
    return sonnerToast.loading(title, {
      description: options?.description,
      duration: options?.duration || Infinity,
      id: options?.id,
    });
  }, []);

  const dismiss = useCallback((toastId?: string) => {
    sonnerToast.dismiss(toastId);
  }, []);

  // Execute an action with automatic undo support
  const executeWithUndo = useCallback(async <T,>({
    execute,
    undo,
    successMessage,
    errorMessage = "Operation failed",
  }: UndoableAction<T>): Promise<T | null> => {
    const loadingId = loading("Processing...");

    try {
      const result = await execute();
      dismiss(loadingId);

      success(successMessage, {
        onUndo: undo,
        duration: 8000,
      });

      return result;
    } catch (err) {
      dismiss(loadingId);
      error(errorMessage, {
        description: err instanceof Error ? err.message : "An unexpected error occurred",
      });
      return null;
    }
  }, [loading, dismiss, success, error]);

  // Promise-based toast (shows loading, then success/error)
  const promise = useCallback(<T,>(
    promiseFn: Promise<T> | (() => Promise<T>),
    options: {
      loading: string;
      success: string | ((data: T) => string);
      error: string | ((err: Error) => string);
    }
  ) => {
    return sonnerToast.promise(
      typeof promiseFn === "function" ? promiseFn() : promiseFn,
      options
    );
  }, []);

  return {
    toast: showToast,
    success,
    error,
    warning,
    info,
    loading,
    dismiss,
    promise,
    executeWithUndo,
  };
}

// Export a simple toast function for one-off usage
export const toast = {
  success: (title: string, description?: string) => sonnerToast.success(title, { description }),
  error: (title: string, description?: string) => sonnerToast.error(title, { description }),
  warning: (title: string, description?: string) => sonnerToast.warning(title, { description }),
  info: (title: string, description?: string) => sonnerToast.info(title, { description }),
  loading: (title: string) => sonnerToast.loading(title),
  dismiss: (id?: string) => sonnerToast.dismiss(id),
};
