/**
 * Hierarchy Viewer Page
 * Uses EnhancedHierarchyTree with drag-drop + mappings as child nodes + activity log
 */
import { useState, useEffect, useMemo, useCallback } from "react";
import {
  Eye,
  RefreshCw,
  Search,
  Layers,
  Folder,
  Database,
  Trash2,
  Clock,
  Table2,
  Server,
  Hash,
  Tag,
  Plus,
  CheckCircle,
  XCircle,
  ChevronRight,
  Link,
} from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { Checkbox } from "@/components/ui/checkbox";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";
import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";
import { projectService } from "@/services/api/hierarchy/project.service";
import { smartHierarchyService } from "@/services/api/hierarchy/hierarchy.service";
import {
  referenceTableService,
  ReferenceTableSummary,
} from "@/services/api/hierarchy/reference-table.service";
import { EnhancedHierarchyTree } from "@/components/hierarchy-knowledge-base/components/EnhancedHierarchyTree";
import { ReferenceTablesDialog } from "@/components/hierarchy-knowledge-base/dialogs/ReferenceTablesDialog";
import { useToast } from "@/hooks/use-toast";

// ============================================
// Types
// ============================================

interface LogEntry {
  id: string;
  timestamp: Date;
  type: 'create' | 'update' | 'delete' | 'move' | 'reorder' | 'info' | 'error';
  message: string;
  details?: string;
}

interface ReferenceConnection {
  mappingKey: string;           // `${hierarchyId}-${mapping_index}`
  tableName: string;            // Reference table name
  columnName: string;           // Selected column to match source_uid
  isConnected: boolean;
  appliedToAll: boolean;
}

interface ApplyAllDialogState {
  open: boolean;
  hierarchyId: string;
  mappingIndex: number;
  tableName: string;
  columnName: string;
  sourceTable: string;
  sourceColumn: string;
  similarCount: number;
}

// ============================================
// Activity Log Storage
// ============================================

const LOG_STORAGE_KEY = 'hierarchy-viewer-activity-log';
const CONNECTIONS_STORAGE_KEY = 'hierarchy-viewer-ref-connections';
const MAX_LOG_ENTRIES = 100;

function loadPersistedLog(): LogEntry[] {
  try {
    const stored = localStorage.getItem(LOG_STORAGE_KEY);
    if (stored) {
      const entries = JSON.parse(stored);
      return entries.map((e: any) => ({
        ...e,
        timestamp: new Date(e.timestamp),
      }));
    }
  } catch (e) {
    console.error('Failed to load activity log:', e);
  }
  return [];
}

function persistLog(entries: LogEntry[]) {
  try {
    const toStore = entries.slice(0, MAX_LOG_ENTRIES);
    localStorage.setItem(LOG_STORAGE_KEY, JSON.stringify(toStore));
  } catch (e) {
    console.error('Failed to persist activity log:', e);
  }
}

function loadPersistedConnections(projectId: string): Record<string, ReferenceConnection> {
  try {
    const stored = localStorage.getItem(`${CONNECTIONS_STORAGE_KEY}-${projectId}`);
    if (stored) {
      return JSON.parse(stored);
    }
  } catch (e) {
    console.error('Failed to load reference connections:', e);
  }
  return {};
}

function persistConnections(projectId: string, connections: Record<string, ReferenceConnection>) {
  try {
    localStorage.setItem(`${CONNECTIONS_STORAGE_KEY}-${projectId}`, JSON.stringify(connections));
  } catch (e) {
    console.error('Failed to persist reference connections:', e);
  }
}

// ============================================
// Activity Log Component
// ============================================

interface ActivityLogProps {
  entries: LogEntry[];
  onClear: () => void;
}

function ActivityLog({ entries, onClear }: ActivityLogProps) {
  const getTypeColor = (type: LogEntry['type']) => {
    switch (type) {
      case 'create': return 'text-green-600 bg-green-50 dark:bg-green-950';
      case 'update': return 'text-blue-600 bg-blue-50 dark:bg-blue-950';
      case 'delete': return 'text-red-600 bg-red-50 dark:bg-red-950';
      case 'move': return 'text-amber-600 bg-amber-50 dark:bg-amber-950';
      case 'reorder': return 'text-purple-600 bg-purple-50 dark:bg-purple-950';
      case 'error': return 'text-red-600 bg-red-50 dark:bg-red-950';
      default: return 'text-gray-600 bg-gray-50 dark:bg-gray-950';
    }
  };

  return (
    <Card className="flex flex-col h-full">
      <CardHeader className="py-2 px-3 border-b flex-shrink-0">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Clock className="h-4 w-4 text-muted-foreground" />
            <CardTitle className="text-sm font-medium">Activity Log</CardTitle>
            <Badge variant="secondary" className="text-xs">{entries.length}</Badge>
          </div>
          <Button
            variant="ghost"
            size="sm"
            className="h-7 px-2 text-xs"
            onClick={onClear}
          >
            <Trash2 className="h-3 w-3 mr-1" />
            Clear
          </Button>
        </div>
      </CardHeader>
      <CardContent className="flex-1 p-0 overflow-hidden">
        <ScrollArea className="h-full">
          <div className="p-2 space-y-1">
            {entries.length === 0 ? (
              <div className="text-center text-muted-foreground text-sm py-8">
                No activity yet. Changes will be logged here.
              </div>
            ) : (
              entries.map((entry) => (
                <div
                  key={entry.id}
                  className={cn(
                    "rounded-lg px-2 py-1.5 text-xs",
                    getTypeColor(entry.type)
                  )}
                >
                  <div className="flex items-start justify-between gap-2">
                    <span className="font-medium capitalize">{entry.type}</span>
                    <span className="text-muted-foreground flex-shrink-0">
                      {entry.timestamp.toLocaleTimeString()}
                    </span>
                  </div>
                  <div className="mt-0.5">{entry.message}</div>
                  {entry.details && (
                    <div className="mt-0.5 text-muted-foreground truncate">
                      {entry.details}
                    </div>
                  )}
                </div>
              ))
            )}
          </div>
        </ScrollArea>
      </CardContent>
    </Card>
  );
}

// ============================================
// Mapping Display Component
// ============================================

interface MappingDisplayProps {
  mappings: any[];
  hierarchyId: string;
  referenceTables: ReferenceTableSummary[];
  connections: Record<string, ReferenceConnection>;
  onColumnSelect: (
    hierarchyId: string,
    mappingIndex: number,
    tableName: string,
    columnName: string,
    sourceTable: string,
    sourceColumn: string
  ) => void;
}

function MappingDisplay({
  mappings,
  hierarchyId,
  referenceTables,
  connections,
  onColumnSelect,
}: MappingDisplayProps) {
  const [openPopovers, setOpenPopovers] = useState<Record<number, boolean>>({});
  const [expandedTables, setExpandedTables] = useState<Record<string, boolean>>({});

  if (!mappings || mappings.length === 0) return null;

  const togglePopover = (idx: number, open: boolean) => {
    setOpenPopovers((prev) => ({ ...prev, [idx]: open }));
  };

  const toggleTable = (tableName: string) => {
    setExpandedTables((prev) => ({ ...prev, [tableName]: !prev[tableName] }));
  };

  const getMatchingTables = (sourceTable: string | undefined) => {
    if (!sourceTable) return [];
    const normalizedSource = sourceTable.toLowerCase();
    return referenceTables.filter((t) => {
      const normalizedRef = t.name.toLowerCase();
      // Match if reference table name contains source table name or vice versa
      return (
        normalizedRef.includes(normalizedSource) ||
        normalizedSource.includes(normalizedRef) ||
        normalizedRef === normalizedSource
      );
    });
  };

  return (
    <TooltipProvider>
      <div className="mt-2 ml-6 space-y-1 border-l-2 border-green-200 dark:border-green-800 pl-3">
        <div className="text-xs text-muted-foreground font-medium mb-1">
          Source Mappings ({mappings.length})
        </div>
        {mappings.map((mapping, idx) => {
          const mappingKey = `${hierarchyId}-${mapping.mapping_index ?? idx}`;
          const connection = connections[mappingKey];
          const isConnected = connection?.isConnected ?? false;
          const matchingTables = getMatchingTables(mapping.source_table);
          const hasMatchingTable = matchingTables.length > 0;

          return (
            <div
              key={idx}
              className="flex flex-wrap items-center gap-1 py-1 px-2 rounded bg-muted/30 hover:bg-muted/50"
            >
              {/* Connection Status Icon */}
              <Tooltip>
                <TooltipTrigger asChild>
                  <span className="flex-shrink-0">
                    {isConnected ? (
                      <CheckCircle className="h-3 w-3 text-green-500" />
                    ) : hasMatchingTable ? (
                      <Link className="h-3 w-3 text-amber-500" />
                    ) : (
                      <XCircle className="h-3 w-3 text-gray-400" />
                    )}
                  </span>
                </TooltipTrigger>
                <TooltipContent>
                  {isConnected
                    ? `Connected: ${connection.tableName}.${connection.columnName}`
                    : hasMatchingTable
                    ? 'Reference table available - click + to connect'
                    : 'No matching reference table'}
                </TooltipContent>
              </Tooltip>

              <Table2 className="h-3 w-3 text-green-600 flex-shrink-0" />

              {mapping.source_database && (
                <Tooltip>
                  <TooltipTrigger>
                    <Badge variant="outline" className="text-xs px-1 py-0 bg-purple-50 dark:bg-purple-950 border-purple-200">
                      <Server className="h-2.5 w-2.5 mr-0.5 text-purple-600" />
                      {mapping.source_database}
                    </Badge>
                  </TooltipTrigger>
                  <TooltipContent>Database</TooltipContent>
                </Tooltip>
              )}

              {mapping.source_schema && (
                <Tooltip>
                  <TooltipTrigger>
                    <Badge variant="outline" className="text-xs px-1 py-0 bg-blue-50 dark:bg-blue-950 border-blue-200">
                      <Layers className="h-2.5 w-2.5 mr-0.5 text-blue-600" />
                      {mapping.source_schema}
                    </Badge>
                  </TooltipTrigger>
                  <TooltipContent>Schema</TooltipContent>
                </Tooltip>
              )}

              {mapping.source_table && (
                <Tooltip>
                  <TooltipTrigger>
                    <Badge variant="outline" className="text-xs px-1 py-0 bg-green-50 dark:bg-green-950 border-green-200">
                      <Table2 className="h-2.5 w-2.5 mr-0.5 text-green-600" />
                      {mapping.source_table}
                    </Badge>
                  </TooltipTrigger>
                  <TooltipContent>Table</TooltipContent>
                </Tooltip>
              )}

              {mapping.source_column && (
                <Tooltip>
                  <TooltipTrigger>
                    <Badge variant="outline" className="text-xs px-1 py-0 bg-amber-50 dark:bg-amber-950 border-amber-200">
                      <Hash className="h-2.5 w-2.5 mr-0.5 text-amber-600" />
                      {mapping.source_column}
                    </Badge>
                  </TooltipTrigger>
                  <TooltipContent>Column</TooltipContent>
                </Tooltip>
              )}

              {/* ID Value - Always show if present */}
              {mapping.source_uid && (
                <Tooltip>
                  <TooltipTrigger>
                    <Badge className="text-xs px-1.5 py-0.5 bg-rose-500 text-white font-mono">
                      <Tag className="h-2.5 w-2.5 mr-1" />
                      ID: {mapping.source_uid}
                    </Badge>
                  </TooltipTrigger>
                  <TooltipContent>Source UID / ID Value</TooltipContent>
                </Tooltip>
              )}

              {/* Mapping index */}
              <span className="text-xs text-muted-foreground ml-auto">
                #{mapping.mapping_index ?? idx}
              </span>

              {/* Expand/Plus Button for Column Selection */}
              <Popover
                open={openPopovers[idx] ?? false}
                onOpenChange={(open) => togglePopover(idx, open)}
              >
                <PopoverTrigger asChild>
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-5 w-5 ml-1 hover:bg-primary/10"
                  >
                    <Plus className="h-3 w-3" />
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="w-80 p-3" align="end">
                  <div className="space-y-3">
                    <div className="text-sm font-medium">Connect to Reference Table</div>
                    {isConnected && (
                      <div className="text-xs text-green-600 bg-green-50 dark:bg-green-950 rounded px-2 py-1">
                        Currently connected: {connection.tableName}.{connection.columnName}
                      </div>
                    )}

                    {matchingTables.length === 0 ? (
                      <div className="text-sm text-muted-foreground py-2">
                        No reference table matches "{mapping.source_table}"
                      </div>
                    ) : (
                      <div className="space-y-2">
                        {matchingTables.map((table) => (
                          <Collapsible
                            key={table.name}
                            open={expandedTables[table.name] ?? false}
                            onOpenChange={() => toggleTable(table.name)}
                          >
                            <CollapsibleTrigger className="flex items-center gap-2 w-full hover:bg-muted/50 rounded px-2 py-1 text-left">
                              <ChevronRight
                                className={cn(
                                  'h-4 w-4 transition-transform',
                                  expandedTables[table.name] && 'rotate-90'
                                )}
                              />
                              <span className="text-sm font-medium">{table.displayName}</span>
                              <Badge variant="secondary" className="ml-auto text-xs">
                                {table.rowCount} rows
                              </Badge>
                            </CollapsibleTrigger>
                            <CollapsibleContent>
                              <div className="ml-6 mt-1 space-y-1 border-l-2 pl-2">
                                {table.columns.map((col) => (
                                  <div
                                    key={col.name}
                                    className="flex items-center gap-2 py-1 px-2 hover:bg-muted/50 rounded cursor-pointer"
                                    onClick={() => {
                                      onColumnSelect(
                                        hierarchyId,
                                        mapping.mapping_index ?? idx,
                                        table.name,
                                        col.name,
                                        mapping.source_table || '',
                                        mapping.source_column || ''
                                      );
                                      togglePopover(idx, false);
                                    }}
                                  >
                                    <Checkbox
                                      checked={
                                        connection?.tableName === table.name &&
                                        connection?.columnName === col.name
                                      }
                                      className="pointer-events-none"
                                    />
                                    <span className="text-sm">{col.name}</span>
                                    <Badge variant="outline" className="text-xs ml-auto">
                                      {col.type}
                                    </Badge>
                                  </div>
                                ))}
                              </div>
                            </CollapsibleContent>
                          </Collapsible>
                        ))}
                      </div>
                    )}
                  </div>
                </PopoverContent>
              </Popover>
            </div>
          );
        })}
      </div>
    </TooltipProvider>
  );
}

// ============================================
// Main Component
// ============================================

export function HierarchyViewerPage() {
  const [projects, setProjects] = useState<any[]>([]);
  const [selectedProjectId, setSelectedProjectId] = useState("");
  const [hierarchies, setHierarchies] = useState<any[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [searchQuery, setSearchQuery] = useState("");
  const [expandedNodes, setExpandedNodes] = useState<Set<string>>(new Set());
  const [expandedDetails, setExpandedDetails] = useState<Set<string>>(new Set());
  const [activityLog, setActivityLog] = useState<LogEntry[]>(loadPersistedLog);

  // For EnhancedHierarchyTree
  const [selectedHierarchyId, setSelectedHierarchyId] = useState<string | null>(null);
  const [selectedForFormula, setSelectedForFormula] = useState<Set<string>>(new Set());

  // Reference Tables state
  const [referenceTablesDialogOpen, setReferenceTablesDialogOpen] = useState(false);
  const [referenceTables, setReferenceTables] = useState<ReferenceTableSummary[]>([]);
  const [referenceConnections, setReferenceConnections] = useState<Record<string, ReferenceConnection>>({});

  // Apply All dialog state
  const [applyAllDialog, setApplyAllDialog] = useState<ApplyAllDialogState>({
    open: false,
    hierarchyId: '',
    mappingIndex: 0,
    tableName: '',
    columnName: '',
    sourceTable: '',
    sourceColumn: '',
    similarCount: 0,
  });

  const { toast } = useToast();

  // Add log entry
  const addLogEntry = useCallback((
    type: LogEntry['type'],
    message: string,
    details?: string
  ) => {
    const entry: LogEntry = {
      id: `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
      timestamp: new Date(),
      type,
      message,
      details,
    };
    setActivityLog((prev) => {
      const updated = [entry, ...prev].slice(0, MAX_LOG_ENTRIES);
      persistLog(updated);
      return updated;
    });
  }, []);

  // Clear log
  const clearLog = useCallback(() => {
    setActivityLog([]);
    localStorage.removeItem(LOG_STORAGE_KEY);
    toast({ title: "Log cleared", description: "Activity log has been cleared." });
  }, [toast]);

  // Load reference tables
  const loadReferenceTables = useCallback(async () => {
    try {
      const tables = await referenceTableService.listTables();
      setReferenceTables(tables || []);
    } catch (error) {
      console.error('Failed to load reference tables:', error);
    }
  }, []);

  // Load reference connections from localStorage when project changes
  const loadReferenceConnections = useCallback((projectId: string) => {
    const connections = loadPersistedConnections(projectId);
    setReferenceConnections(connections);
  }, []);

  // Save a reference connection
  const saveReferenceConnection = useCallback((
    hierarchyId: string,
    mappingIndex: number,
    tableName: string,
    columnName: string,
    applyToAll: boolean
  ) => {
    const mappingKey = `${hierarchyId}-${mappingIndex}`;
    const connection: ReferenceConnection = {
      mappingKey,
      tableName,
      columnName,
      isConnected: true,
      appliedToAll: applyToAll,
    };

    setReferenceConnections((prev) => {
      const updated = { ...prev, [mappingKey]: connection };
      if (selectedProjectId) {
        persistConnections(selectedProjectId, updated);
      }
      return updated;
    });

    addLogEntry('update', `Connected mapping to ${tableName}.${columnName}`, `Hierarchy: ${hierarchyId}`);
  }, [selectedProjectId, addLogEntry]);

  // Find similar mappings (same source_table + source_column)
  const findSimilarMappings = useCallback((sourceTable: string, sourceColumn: string) => {
    const similar: Array<{ hierarchyId: string; mappingIndex: number }> = [];
    hierarchies.forEach((h) => {
      h.mapping?.forEach((m: any, idx: number) => {
        if (m.source_table === sourceTable && m.source_column === sourceColumn) {
          similar.push({ hierarchyId: h.hierarchyId, mappingIndex: m.mapping_index ?? idx });
        }
      });
    });
    return similar;
  }, [hierarchies]);

  // Handle column selection for reference connection
  const handleColumnSelect = useCallback((
    hierarchyId: string,
    mappingIndex: number,
    tableName: string,
    columnName: string,
    sourceTable: string,
    sourceColumn: string
  ) => {
    // Find how many similar mappings exist
    const similarMappings = findSimilarMappings(sourceTable, sourceColumn);
    const otherSimilar = similarMappings.filter(
      (m) => !(m.hierarchyId === hierarchyId && m.mappingIndex === mappingIndex)
    );

    if (otherSimilar.length > 0) {
      // Show Apply All dialog
      setApplyAllDialog({
        open: true,
        hierarchyId,
        mappingIndex,
        tableName,
        columnName,
        sourceTable,
        sourceColumn,
        similarCount: otherSimilar.length,
      });
    } else {
      // Apply directly (no similar mappings)
      saveReferenceConnection(hierarchyId, mappingIndex, tableName, columnName, false);
      toast({
        title: 'Connection Created',
        description: `Connected to ${tableName}.${columnName}`,
      });
    }
  }, [findSimilarMappings, saveReferenceConnection, toast]);

  // Apply connection (from dialog)
  const applyConnection = useCallback((applyToAll: boolean) => {
    const { hierarchyId, mappingIndex, tableName, columnName, sourceTable, sourceColumn } = applyAllDialog;

    // Save the current mapping's connection
    saveReferenceConnection(hierarchyId, mappingIndex, tableName, columnName, applyToAll);

    if (applyToAll) {
      // Apply to all similar mappings
      const similarMappings = findSimilarMappings(sourceTable, sourceColumn);
      similarMappings.forEach((m) => {
        if (!(m.hierarchyId === hierarchyId && m.mappingIndex === mappingIndex)) {
          saveReferenceConnection(m.hierarchyId, m.mappingIndex, tableName, columnName, true);
        }
      });

      toast({
        title: 'Applied to All',
        description: `Connected ${similarMappings.length} mappings to ${tableName}.${columnName}`,
      });
    } else {
      toast({
        title: 'Connection Created',
        description: `Connected to ${tableName}.${columnName}`,
      });
    }

    // Close dialog
    setApplyAllDialog((prev) => ({ ...prev, open: false }));
  }, [applyAllDialog, saveReferenceConnection, findSimilarMappings, toast]);

  // Handle reference tables change (from dialog)
  const handleReferenceTablesChanged = useCallback(() => {
    loadReferenceTables();
    addLogEntry('info', 'Reference tables updated');
  }, [loadReferenceTables, addLogEntry]);

  // Load projects and reference tables
  useEffect(() => {
    loadProjects();
    loadReferenceTables();
  }, [loadReferenceTables]);

  // Load hierarchies when project changes
  useEffect(() => {
    if (selectedProjectId) {
      loadHierarchies(selectedProjectId);
      loadReferenceConnections(selectedProjectId);
      localStorage.setItem("hierarchy-viewer-project", selectedProjectId);
    }
  }, [selectedProjectId, loadReferenceConnections]);

  const loadProjects = async () => {
    setIsLoading(true);
    try {
      const result = await projectService.getProjects();
      setProjects(result || []);
      const saved = localStorage.getItem("hierarchy-viewer-project");
      if (saved && result?.some((p: any) => p.id === saved)) {
        setSelectedProjectId(saved);
      }
      addLogEntry('info', `Loaded ${(result || []).length} projects`);
    } catch (error) {
      console.error("Failed to load projects:", error);
      addLogEntry('error', 'Failed to load projects', String(error));
    } finally {
      setIsLoading(false);
    }
  };

  const loadHierarchies = async (projectId: string) => {
    try {
      const result = await smartHierarchyService.findAll(projectId);
      setHierarchies(result || []);

      // Auto-expand root nodes
      const roots = (result || []).filter((h: any) => h.isRoot || !h.parentId);
      setExpandedNodes(new Set(roots.map((r: any) => r.hierarchyId)));

      const projectName = projects.find(p => p.id === projectId)?.name || projectId;
      addLogEntry('info', `Loaded ${(result || []).length} hierarchies`, `Project: ${projectName}`);
    } catch (error) {
      console.error("Failed to load hierarchies:", error);
      addLogEntry('error', 'Failed to load hierarchies', String(error));
    }
  };

  // Filter hierarchies by search
  const filteredHierarchies = useMemo(() => {
    if (!searchQuery) return hierarchies;
    const query = searchQuery.toLowerCase();
    return hierarchies.filter((h) =>
      h.hierarchyName?.toLowerCase().includes(query)
    );
  }, [hierarchies, searchQuery]);

  // Stats
  const stats = useMemo(() => {
    const roots = hierarchies.filter((h) => h.isRoot || !h.parentId);
    const withMappings = hierarchies.filter((h) => h.mapping?.length > 0);
    const totalMappings = hierarchies.reduce((sum, h) => sum + (h.mapping?.length || 0), 0);
    return {
      total: hierarchies.length,
      roots: roots.length,
      withMappings: withMappings.length,
      totalMappings,
    };
  }, [hierarchies]);

  // Handle hierarchy selection
  const handleSelect = useCallback((hierarchyId: string) => {
    const hierarchy = hierarchies.find(h => h.hierarchyId === hierarchyId);
    setSelectedHierarchyId(hierarchyId);
    addLogEntry('info', `Selected: ${hierarchy?.hierarchyName || hierarchyId}`);
  }, [hierarchies, addLogEntry]);

  // Handle checkbox toggle
  const handleToggleFormulaSelection = useCallback((hierarchyId: string) => {
    setSelectedForFormula((prev) => {
      const next = new Set(prev);
      if (next.has(hierarchyId)) {
        next.delete(hierarchyId);
      } else {
        next.add(hierarchyId);
      }
      return next;
    });
  }, []);

  // Handle reorder from drag-and-drop - persist to backend
  const handleReorder = useCallback(async (updatedHierarchies: any[]) => {
    // Find hierarchies that changed
    const changes: Array<{
      hierarchyId: string;
      hierarchyName: string;
      parentId?: string | null;
      isRoot?: boolean;
      sortOrder: number;
    }> = [];

    updatedHierarchies.forEach((updated) => {
      const original = hierarchies.find((h) => h.hierarchyId === updated.hierarchyId);
      if (original) {
        const parentChanged = original.parentId !== updated.parentId;
        const sortChanged = original.sortOrder !== updated.sortOrder;
        const rootChanged = original.isRoot !== updated.isRoot;

        if (parentChanged || sortChanged || rootChanged) {
          changes.push({
            hierarchyId: updated.hierarchyId,
            hierarchyName: updated.hierarchyName,
            parentId: updated.parentId,
            isRoot: updated.isRoot,
            sortOrder: updated.sortOrder,
          });
        }
      }
    });

    if (changes.length === 0) return;

    // Log the changes
    changes.forEach((change) => {
      const original = hierarchies.find(h => h.hierarchyId === change.hierarchyId);
      if (original?.parentId !== change.parentId) {
        const newParent = hierarchies.find(h => h.hierarchyId === change.parentId);
        addLogEntry('move', `Moved "${change.hierarchyName}"`, `New parent: ${newParent?.hierarchyName || 'Root'}`);
      } else {
        addLogEntry('reorder', `Reordered "${change.hierarchyName}"`, `New position: ${change.sortOrder}`);
      }
    });

    try {
      const result = await smartHierarchyService.bulkUpdateOrder(
        selectedProjectId,
        changes.map(c => ({
          hierarchyId: c.hierarchyId,
          parentId: c.parentId,
          isRoot: c.isRoot,
          sortOrder: c.sortOrder,
        }))
      );

      addLogEntry('info', `Saved ${result.updated} changes to backend`);
      await loadHierarchies(selectedProjectId);

      toast({
        title: "Changes saved",
        description: `${result.updated} item(s) updated.`,
      });
    } catch (error) {
      console.error("Failed to save:", error);
      addLogEntry('error', 'Failed to save changes', String(error));
      toast({
        title: "Error",
        description: "Failed to save changes.",
        variant: "destructive",
      });
      await loadHierarchies(selectedProjectId);
    }
  }, [hierarchies, selectedProjectId, toast, addLogEntry]);

  const selectedProject = projects.find((p) => p.id === selectedProjectId);
  const selectedHierarchy = hierarchies.find(h => h.hierarchyId === selectedHierarchyId);

  return (
    <div className="h-full flex flex-col p-4 gap-4">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-3">
          <Eye className="h-6 w-6 text-primary" />
          <div>
            <h1 className="text-2xl font-bold">Hierarchy Viewer</h1>
            <p className="text-sm text-muted-foreground">
              Drag to reorder, hold Shift to drop as child. Select a node to view mappings.
            </p>
          </div>
        </div>

        <div className="flex items-center gap-2">
          <Select value={selectedProjectId} onValueChange={setSelectedProjectId}>
            <SelectTrigger className="w-[250px]">
              <SelectValue placeholder="Select a project" />
            </SelectTrigger>
            <SelectContent>
              {projects.map((p) => (
                <SelectItem key={p.id} value={p.id}>{p.name}</SelectItem>
              ))}
            </SelectContent>
          </Select>

          <Button
            variant="outline"
            onClick={() => setReferenceTablesDialogOpen(true)}
            className="gap-2"
          >
            <Database className="h-4 w-4" />
            Reference Tables
            {referenceTables.length > 0 && (
              <Badge variant="secondary" className="ml-1">
                {referenceTables.length}
              </Badge>
            )}
          </Button>

          <Button
            variant="outline"
            size="icon"
            onClick={() => selectedProjectId && loadHierarchies(selectedProjectId)}
          >
            <RefreshCw className="h-4 w-4" />
          </Button>
        </div>
      </div>

      {/* Stats */}
      <div className="grid grid-cols-4 gap-3">
        <Card>
          <CardContent className="p-3 flex items-center gap-2">
            <Layers className="h-4 w-4 text-blue-500" />
            <div>
              <p className="text-xs text-muted-foreground">Hierarchies</p>
              <p className="text-xl font-bold">{stats.total}</p>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-3 flex items-center gap-2">
            <Folder className="h-4 w-4 text-amber-500" />
            <div>
              <p className="text-xs text-muted-foreground">Root Nodes</p>
              <p className="text-xl font-bold">{stats.roots}</p>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-3 flex items-center gap-2">
            <Database className="h-4 w-4 text-green-500" />
            <div>
              <p className="text-xs text-muted-foreground">With Mappings</p>
              <p className="text-xl font-bold">{stats.withMappings}</p>
            </div>
          </CardContent>
        </Card>
        <Card>
          <CardContent className="p-3 flex items-center gap-2">
            <Table2 className="h-4 w-4 text-purple-500" />
            <div>
              <p className="text-xs text-muted-foreground">Total Mappings</p>
              <p className="text-xl font-bold">{stats.totalMappings}</p>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex gap-4 overflow-hidden">
        {/* Tree View */}
        <Card className="flex-1 flex flex-col overflow-hidden">
          <CardHeader className="py-3 border-b flex-shrink-0">
            <div className="flex items-center justify-between">
              <CardTitle className="text-base">
                {selectedProject?.name || "Select a Project"}
              </CardTitle>
              <div className="relative">
                <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-muted-foreground" />
                <Input
                  placeholder="Search..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="pl-8 h-9 w-[180px]"
                />
              </div>
            </div>
          </CardHeader>

          <CardContent className="flex-1 p-0 overflow-hidden">
            {isLoading ? (
              <div className="p-4 space-y-2">
                {[1, 2, 3, 4, 5].map((i) => (
                  <Skeleton key={i} className="h-10 w-full" />
                ))}
              </div>
            ) : filteredHierarchies.length === 0 ? (
              <div className="flex items-center justify-center h-full text-muted-foreground">
                <div className="text-center">
                  <Layers className="h-12 w-12 mx-auto mb-2 opacity-50" />
                  <p>No hierarchies found</p>
                </div>
              </div>
            ) : (
              <ScrollArea className="h-full">
                <EnhancedHierarchyTree
                  hierarchies={filteredHierarchies}
                  selectedId={selectedHierarchyId}
                  selectedForFormula={selectedForFormula}
                  onSelect={handleSelect}
                  onToggleFormulaSelection={handleToggleFormulaSelection}
                  onReorder={handleReorder}
                  expandedNodes={expandedNodes}
                  expandedDetails={expandedDetails}
                  onExpandedNodesChange={setExpandedNodes}
                  onExpandedDetailsChange={setExpandedDetails}
                />
              </ScrollArea>
            )}
          </CardContent>
        </Card>

        {/* Right Panel: Mappings + Activity Log */}
        <div className="w-[350px] flex-shrink-0 flex flex-col gap-4">
          {/* Selected Hierarchy Mappings */}
          {selectedHierarchy && (
            <Card className="flex-shrink-0">
              <CardHeader className="py-2 px-3 border-b">
                <CardTitle className="text-sm font-medium flex items-center gap-2">
                  <Database className="h-4 w-4 text-green-500" />
                  Mappings: {selectedHierarchy.hierarchyName}
                </CardTitle>
              </CardHeader>
              <CardContent className="p-2 max-h-[300px] overflow-auto">
                {selectedHierarchy.mapping && selectedHierarchy.mapping.length > 0 ? (
                  <MappingDisplay
                    mappings={selectedHierarchy.mapping}
                    hierarchyId={selectedHierarchy.hierarchyId}
                    referenceTables={referenceTables}
                    connections={referenceConnections}
                    onColumnSelect={handleColumnSelect}
                  />
                ) : (
                  <div className="text-center text-muted-foreground text-sm py-4">
                    No mappings configured
                  </div>
                )}
              </CardContent>
            </Card>
          )}

          {/* Activity Log */}
          <div className="flex-1 min-h-0">
            <ActivityLog entries={activityLog} onClear={clearLog} />
          </div>
        </div>
      </div>

      {/* Reference Tables Dialog */}
      <ReferenceTablesDialog
        open={referenceTablesDialogOpen}
        onOpenChange={setReferenceTablesDialogOpen}
        onTablesChanged={handleReferenceTablesChanged}
      />

      {/* Apply to All Confirmation Dialog */}
      <AlertDialog open={applyAllDialog.open} onOpenChange={(open) => setApplyAllDialog((prev) => ({ ...prev, open }))}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Apply Reference to All Similar Columns?</AlertDialogTitle>
            <AlertDialogDescription>
              Found {applyAllDialog.similarCount} other mapping{applyAllDialog.similarCount !== 1 ? 's' : ''} with the same
              table ({applyAllDialog.sourceTable}) and column ({applyAllDialog.sourceColumn}).
              Apply this reference connection to all of them?
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel onClick={() => applyConnection(false)}>
              No, Only This One
            </AlertDialogCancel>
            <AlertDialogAction onClick={() => applyConnection(true)}>
              Yes, Apply to All ({applyAllDialog.similarCount + 1})
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

export default HierarchyViewerPage;
