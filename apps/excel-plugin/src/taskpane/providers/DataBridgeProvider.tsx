/**
 * DataBridge Provider
 *
 * Provides DataBridge state and API methods to all components.
 * Handles project selection, hierarchy data, and connection management.
 */

import React, {
  createContext,
  useContext,
  useState,
  useEffect,
  useCallback,
  ReactNode,
} from 'react';
import {
  apiService,
  HierarchyProject,
  Hierarchy,
  ConnectionConfig,
} from '../../services/api.service';
import { useAuth } from './AuthProvider';

interface DataBridgeState {
  projects: HierarchyProject[];
  selectedProject: HierarchyProject | null;
  hierarchies: Hierarchy[];
  connections: ConnectionConfig[];
  isConnected: boolean;
  isLoading: boolean;
  error: string | null;
}

interface DataBridgeContextValue extends DataBridgeState {
  refreshProjects: () => Promise<void>;
  selectProject: (projectId: string) => Promise<void>;
  refreshHierarchies: () => Promise<void>;
  refreshConnections: () => Promise<void>;
  checkConnection: () => Promise<boolean>;
  clearError: () => void;
}

const DataBridgeContext = createContext<DataBridgeContextValue | null>(null);

export function useDataBridge(): DataBridgeContextValue {
  const context = useContext(DataBridgeContext);
  if (!context) {
    throw new Error('useDataBridge must be used within a DataBridgeProvider');
  }
  return context;
}

interface DataBridgeProviderProps {
  children: ReactNode;
}

export function DataBridgeProvider({ children }: DataBridgeProviderProps): JSX.Element {
  const { isAuthenticated } = useAuth();

  const [state, setState] = useState<DataBridgeState>({
    projects: [],
    selectedProject: null,
    hierarchies: [],
    connections: [],
    isConnected: false,
    isLoading: false,
    error: null,
  });

  // Check connection on mount and when auth changes
  useEffect(() => {
    if (isAuthenticated) {
      checkConnection();
    }
  }, [isAuthenticated]);

  const setLoading = (isLoading: boolean) => {
    setState((prev) => ({ ...prev, isLoading }));
  };

  const setError = (error: string | null) => {
    setState((prev) => ({ ...prev, error }));
  };

  const clearError = useCallback(() => {
    setError(null);
  }, []);

  const checkConnection = useCallback(async (): Promise<boolean> => {
    try {
      const result = await apiService.checkHealth();
      const isConnected = result.success;
      setState((prev) => ({ ...prev, isConnected }));

      if (isConnected) {
        // Load initial data
        refreshProjects();
        refreshConnections();
      }

      return isConnected;
    } catch (error) {
      setState((prev) => ({ ...prev, isConnected: false }));
      return false;
    }
  }, []);

  const refreshProjects = useCallback(async (): Promise<void> => {
    setLoading(true);

    try {
      const result = await apiService.listProjects();
      if (result.success && result.data) {
        setState((prev) => ({ ...prev, projects: result.data! }));
      }
      // Silently fail on 401 - user may need to re-authenticate
    } catch (error: any) {
      // Don't show auth errors on startup - just log them
      if (error.response?.status === 401) {
        console.warn('Projects endpoint requires authentication');
      } else {
        console.error('Failed to load projects:', error.message);
      }
    } finally {
      setLoading(false);
    }
  }, []);

  const selectProject = useCallback(async (projectId: string): Promise<void> => {
    const project = state.projects.find((p) => p.id === projectId);
    if (!project) {
      setError('Project not found');
      return;
    }

    setState((prev) => ({ ...prev, selectedProject: project }));

    // Load hierarchies for the project
    await refreshHierarchiesForProject(projectId);
  }, [state.projects]);

  const refreshHierarchiesForProject = async (projectId: string): Promise<void> => {
    setLoading(true);
    setError(null);

    try {
      const result = await apiService.listHierarchies(projectId);
      if (result.success && result.data) {
        setState((prev) => ({ ...prev, hierarchies: result.data! }));
      } else {
        setError(result.error || 'Failed to load hierarchies');
      }
    } catch (error: any) {
      setError(error.message);
    } finally {
      setLoading(false);
    }
  };

  const refreshHierarchies = useCallback(async (): Promise<void> => {
    if (state.selectedProject) {
      await refreshHierarchiesForProject(state.selectedProject.id);
    }
  }, [state.selectedProject]);

  const refreshConnections = useCallback(async (): Promise<void> => {
    try {
      const result = await apiService.listConnections();
      if (result.success && result.data) {
        setState((prev) => ({ ...prev, connections: result.data! }));
      }
    } catch (error: any) {
      // Silently fail - connections are optional
      // Don't show auth errors
      if (error.response?.status !== 401) {
        console.warn('Failed to load connections:', error.message);
      }
    }
  }, []);

  const value: DataBridgeContextValue = {
    ...state,
    refreshProjects,
    selectProject,
    refreshHierarchies,
    refreshConnections,
    checkConnection,
    clearError,
  };

  return (
    <DataBridgeContext.Provider value={value}>
      {children}
    </DataBridgeContext.Provider>
  );
}

export default DataBridgeProvider;
