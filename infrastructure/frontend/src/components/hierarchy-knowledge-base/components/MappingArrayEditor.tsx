import React, { useState, useEffect } from "react";
import type { SourceMapping } from "@/services/api/hierarchy";
import { smartHierarchyService } from "@/services/api/hierarchy";
import { axiosInstance } from "@/lib/axios";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Badge } from "@/components/ui/badge";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Plus,
  Trash2,
  GripVertical,
  ChevronDown,
  ChevronRight,
  X,
  Database,
  Table as TableIcon,
  List,
  Copy,
  Check,
  ChevronsUpDown,
} from "lucide-react";
import { HelpTooltip, HelpLabel } from "@/components/ui/help-tooltip";

interface MappingArrayEditorProps {
  mappings: SourceMapping[];
  onChange: (mappings: SourceMapping[]) => void;
  disabled: boolean;
  projectId: string;
  selectedMappings?: number[];
  onMappingSelectionChange?: (selectedIndexes: number[]) => void;
}

export const MappingArrayEditor: React.FC<MappingArrayEditorProps> = ({
  mappings,
  onChange,
  disabled,
  projectId,
  selectedMappings = [],
  onMappingSelectionChange,
}) => {
  const [expandedMappings, setExpandedMappings] = useState<Set<number>>(
    new Set()
  );
  const [viewMode, setViewMode] = useState<"collapse" | "table">("collapse");
  const [showCustomFlagDialog, setShowCustomFlagDialog] = useState(false);
  const [currentMappingIndex, setCurrentMappingIndex] = useState<number | null>(
    null
  );
  const [newFlagName, setNewFlagName] = useState("");
  const [customFlagError, setCustomFlagError] = useState("");
  const [connections, setConnections] = useState<
    Array<{
      id: string;
      connectionName: string;
      serverType?: string;
      description?: string;
      isSystemDefault?: boolean;
      isReadOnly?: boolean;
      snowflakeDatabase?: string;
      snowflakeSchema?: string;
    }>
  >([]);
  const [selectedConnectionId, setSelectedConnectionId] = useState<string>("");
  const [loadingConnections, setLoadingConnections] = useState(false);

  // Manual input toggle state per mapping (true = manual input, false = dropdowns from connection)
  const [manualInputMode, setManualInputMode] = useState<Map<number, boolean>>(
    new Map()
  );

  // Combobox open state
  const [openComboboxes, setOpenComboboxes] = useState<Map<string, boolean>>(
    new Map()
  );

  // Metadata states for dropdowns
  const [databases, setDatabases] = useState<string[]>([]);
  const [schemas, setSchemas] = useState<Map<number, string[]>>(new Map());
  const [tables, setTables] = useState<Map<number, string[]>>(new Map());
  const [columns, setColumns] = useState<Map<number, string[]>>(new Map());
  const [uuids, setUuids] = useState<Map<number, string[]>>(new Map());
  const [loadingMetadata, setLoadingMetadata] = useState<Map<number, string>>(
    new Map()
  );

  // Selection handlers
  const toggleMappingSelection = (mappingIndex: number) => {
    if (!onMappingSelectionChange) return;

    const newSelection = selectedMappings.includes(mappingIndex)
      ? selectedMappings.filter((idx) => idx !== mappingIndex)
      : [...selectedMappings, mappingIndex];

    onMappingSelectionChange(newSelection);
  };

  const toggleSelectAll = () => {
    if (!onMappingSelectionChange) return;

    if (selectedMappings.length === mappings.length) {
      onMappingSelectionChange([]);
    } else {
      onMappingSelectionChange(mappings.map((m) => m.mapping_index));
    }
  };

  // Fetch connections for the project
  useEffect(() => {
    const fetchConnections = async () => {
      setLoadingConnections(true);
      try {
        const response = await axiosInstance.get("/connections");
        console.log("Fetched connections:", response.data);
        setConnections(response.data);
      } catch (error) {
        console.error("Failed to fetch connections:", error);
      } finally {
        setLoadingConnections(false);
      }
    };
    if (disabled) {
      console.log("MappingArrayEditor is disabled; skipping fetchConnections.");
    }
    fetchConnections();
  }, [projectId]);

  // Fetch databases when connection is selected
  useEffect(() => {
    const fetchDatabases = async () => {
      if (!selectedConnectionId) {
        setDatabases([]);
        return;
      }

      try {
        const response = await axiosInstance.post(`/connections/databases`, {
          connectionId: selectedConnectionId,
        });
        setDatabases(response.data);
      } catch (error) {
        console.error("Failed to fetch databases:", error);
      }
    };

    fetchDatabases();
  }, [selectedConnectionId]);

  const toggleManualInput = (index: number) => {
    setManualInputMode((prev) => {
      const newMap = new Map(prev);
      newMap.set(index, !newMap.get(index));
      return newMap;
    });
  };

  const fetchSchemas = async (index: number, database: string) => {
    if (!selectedConnectionId || !database) return;

    setLoadingMetadata((prev) => new Map(prev).set(index, "schemas"));
    try {
      const response = await axiosInstance.post(`/connections/schemas`, {
        connectionId: selectedConnectionId,
        database,
      });
      setSchemas((prev) => new Map(prev).set(index, response.data));
    } catch (error) {
      console.error("Failed to fetch schemas:", error);
    } finally {
      setLoadingMetadata((prev) => {
        const newMap = new Map(prev);
        newMap.delete(index);
        return newMap;
      });
    }
  };

  const fetchTables = async (
    index: number,
    database: string,
    schema: string
  ) => {
    if (!selectedConnectionId || !database || !schema) return;

    setLoadingMetadata((prev) => new Map(prev).set(index, "tables"));
    try {
      const response = await axiosInstance.post(`/connections/tables`, {
        connectionId: selectedConnectionId,
        database,
        schema,
      });
      setTables((prev) => new Map(prev).set(index, response.data));
    } catch (error) {
      console.error("Failed to fetch tables:", error);
    } finally {
      setLoadingMetadata((prev) => {
        const newMap = new Map(prev);
        newMap.delete(index);
        return newMap;
      });
    }
  };

  const fetchColumns = async (
    index: number,
    database: string,
    schema: string,
    table: string
  ) => {
    if (!selectedConnectionId || !database || !schema || !table) return;

    setLoadingMetadata((prev) => new Map(prev).set(index, "columns"));
    try {
      const response = await axiosInstance.post(`/connections/columns`, {
        connectionId: selectedConnectionId,
        database,
        schema,
        table,
      });
      setColumns((prev) =>
        new Map(prev).set(
          index,
          response.data.map((col: any) => col.name)
        )
      );
    } catch (error) {
      console.error("Failed to fetch columns:", error);
    } finally {
      setLoadingMetadata((prev) => {
        const newMap = new Map(prev);
        newMap.delete(index);
        return newMap;
      });
    }
  };

  const fetchUUIDs = async (
    index: number,
    database: string,
    schema: string,
    table: string,
    column: string
  ) => {
    if (!selectedConnectionId || !database || !schema || !table || !column)
      return;

    setLoadingMetadata((prev) => new Map(prev).set(index, "uuids"));
    try {
      const response = await axiosInstance.post(`/connections/column-data`, {
        connectionId: selectedConnectionId,
        database,
        schema,
        table,
        column,
      });
      setUuids((prev) => new Map(prev).set(index, response.data));
    } catch (error) {
      console.error("Failed to fetch UUIDs:", error);
    } finally {
      setLoadingMetadata((prev) => {
        const newMap = new Map(prev);
        newMap.delete(index);
        return newMap;
      });
    }
  };

  const handleAdd = () => {
    const newMapping = smartHierarchyService.createEmptyMapping(
      mappings.length + 1
    );
    onChange([...mappings, newMapping]);
    setExpandedMappings((prev) => new Set([...prev, mappings.length]));
  };

  const handleDuplicate = (index: number) => {
    const mappingToDuplicate = mappings[index];
    const duplicatedMapping = {
      ...mappingToDuplicate,
      mapping_index: mappings.length + 1,
    };
    onChange([...mappings, duplicatedMapping]);
    setExpandedMappings((prev) => new Set([...prev, mappings.length]));
  };

  const handleRemove = (index: number) => {
    const updated = mappings.filter((_, i) => i !== index);
    // Reindex
    const reindexed = updated.map((m, i) => ({ ...m, mapping_index: i + 1 }));
    onChange(reindexed);
  };

  const handleChange = (
    index: number,
    field: keyof SourceMapping,
    value: any
  ) => {
    const updated = [...mappings];
    updated[index] = { ...updated[index], [field]: value };
    onChange(updated);
  };

  const handleFlagChange = (index: number, flag: string, value: boolean) => {
    const updated = [...mappings];
    updated[index] = {
      ...updated[index],
      flags: { ...updated[index].flags, [flag]: value },
    };
    onChange(updated);
  };

  const toggleExpand = (index: number) => {
    setExpandedMappings((prev) => {
      const newSet = new Set(prev);
      if (newSet.has(index)) {
        newSet.delete(index);
      } else {
        newSet.add(index);
      }
      return newSet;
    });
  };

  return (
    <div className="space-y-4">
      {/* Connection Selector */}
      {!disabled && (
        <div className="bg-card shadow-md rounded-lg p-4 ring-1 ring-border/50">
          <div className="space-y-3">
            <div className="flex items-center gap-2">
              <Database className="w-5 h-5 text-muted-foreground" />
              <HelpLabel topicId="sourceMapping" className="text-sm font-medium">
                Database Connection
              </HelpLabel>
            </div>

            <Select
              value={selectedConnectionId}
              onValueChange={setSelectedConnectionId}
              disabled={disabled || loadingConnections}
            >
              <SelectTrigger className="w-full">
                <SelectValue>
                  {selectedConnectionId
                    ? (() => {
                        const conn = connections.find(
                          (c) => c.id === selectedConnectionId
                        );
                        return conn ? (
                          <span className="flex items-center gap-2">
                            {conn.connectionName}
                            {conn.isSystemDefault && (
                              <span className="text-xs bg-secondary px-1.5 py-0.5 rounded">
                                System
                              </span>
                            )}
                            {conn.isReadOnly && (
                              <span className="text-xs border px-1.5 py-0.5 rounded">
                                Read-Only
                              </span>
                            )}
                          </span>
                        ) : (
                          "Select a connection"
                        );
                      })()
                    : loadingConnections
                    ? "Loading connections..."
                    : "Select a connection to load metadata"}
                </SelectValue>
              </SelectTrigger>
              <SelectContent>
                {connections.map((conn) => (
                  <SelectItem key={conn.id} value={conn.id}>
                    {conn.connectionName}
                    {conn.isSystemDefault && " (System)"}
                    {conn.isReadOnly && " (Read-Only)"}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>

            {/* Show connection details when selected */}
            {selectedConnectionId &&
              (() => {
                const selectedConn = connections.find(
                  (c) => c.id === selectedConnectionId
                );
                return (
                  selectedConn && (
                    <div className="text-xs text-muted-foreground space-y-1 pl-7 pt-1">
                      {selectedConn.serverType && (
                        <div>
                          Type:{" "}
                          <span className="font-medium">
                            {selectedConn.serverType}
                          </span>
                        </div>
                      )}
                      {selectedConn.snowflakeDatabase && (
                        <div>
                          Database:{" "}
                          <span className="font-medium">
                            {selectedConn.snowflakeDatabase}
                          </span>
                        </div>
                      )}
                      {selectedConn.snowflakeSchema && (
                        <div>
                          Schema:{" "}
                          <span className="font-medium">
                            {selectedConn.snowflakeSchema}
                          </span>
                        </div>
                      )}
                      {selectedConn.description && (
                        <div className="italic">{selectedConn.description}</div>
                      )}
                    </div>
                  )
                );
              })()}
          </div>
        </div>
      )}
      <div className="flex items-center justify-between">
        <div className="text-base font-semibold flex items-center gap-2">
          {onMappingSelectionChange && (
            <Checkbox
              checked={
                mappings.length > 0 &&
                selectedMappings.length === mappings.length
              }
              onCheckedChange={toggleSelectAll}
              // disabled={disabled || mappings.length === 0}
            />
          )}
          <span className="flex items-center gap-1">
            Source Mappings
            <HelpTooltip topicId="mappingEditor" iconOnly iconSize="sm" />
          </span>
          {selectedMappings.length > 0 && (
            <Badge variant="secondary">
              {selectedMappings.length} / {mappings.length} selected
            </Badge>
          )}
        </div>
        <div className="flex items-center gap-2">
          <div className="flex items-center bg-muted/50 rounded-md">
            <Button
              variant={viewMode === "collapse" ? "secondary" : "ghost"}
              size="sm"
              onClick={() => setViewMode("collapse")}
              className="rounded-r-none border-r"
              // disabled={disabled}
            >
              <List className="w-4 h-4" />
            </Button>
            <Button
              variant={viewMode === "table" ? "secondary" : "ghost"}
              size="sm"
              onClick={() => setViewMode("table")}
              className="rounded-l-none"
              // disabled={disabled}
            >
              <TableIcon className="w-4 h-4" />
            </Button>
          </div>
          <Button
            variant="outline"
            size="sm"
            onClick={handleAdd}
            disabled={disabled}
          >
            <Plus className="w-4 h-4" />
            Add Mapping
          </Button>
        </div>
      </div>

      {mappings.length === 0 ? (
        <div className="text-center py-8 text-muted-foreground text-sm bg-card rounded-lg">
          No mappings configured. Click "Add Mapping" to create one.
        </div>
      ) : viewMode === "collapse" ? (
        <div className="space-y-3">
          {mappings.map((mapping, index) => {
            const isExpanded = expandedMappings.has(index);

            const isSelected = selectedMappings.includes(index);

            return (
              <div
                key={index}
                className={`group bg-card rounded-lg overflow-hidden ${
                  isSelected ? "ring-2 ring-primary" : ""
                }`}
              >
                {/* Compact Header Row */}
                <div className="flex items-center gap-2 px-3 py-2 hover:bg-accent/50 cursor-pointer">
                  {onMappingSelectionChange && (
                    <Checkbox
                      checked={isSelected}
                      onCheckedChange={() => toggleMappingSelection(index)}
                      // disabled={disabled}
                      className="shrink-0"
                      onClick={(e) => e.stopPropagation()}
                    />
                  )}
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => toggleExpand(index)}
                    className="h-5 w-5 p-0 shrink-0"
                  >
                    {isExpanded ? (
                      <ChevronDown className="w-3.5 h-3.5" />
                    ) : (
                      <ChevronRight className="w-3.5 h-3.5" />
                    )}
                  </Button>
                  <GripVertical className="w-3.5 h-3.5 text-muted-foreground shrink-0" />
                  <div className="flex-1 min-w-0 flex items-center gap-2">
                    <span className="font-medium text-xs">
                      #{mapping.mapping_index}
                    </span>
                    <span className="text-xs truncate">
                      {mapping.source_database || "(unnamed)"}.
                      {mapping.source_schema}.{mapping.source_table}
                    </span>
                  </div>
                  {mapping.flags.active_flag && (
                    <span className="text-green-500 text-xs shrink-0">✓</span>
                  )}
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => handleDuplicate(index)}
                    disabled={disabled}
                    className="shrink-0 opacity-0 group-hover:opacity-100 h-5 w-5 p-0"
                    title="Duplicate mapping"
                  >
                    <Copy className="w-3 h-3 text-muted-foreground" />
                  </Button>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => handleRemove(index)}
                    disabled={disabled}
                    className="shrink-0 opacity-0 group-hover:opacity-100 h-5 w-5 p-0"
                  >
                    <Trash2 className="w-3 h-3 text-destructive" />
                  </Button>
                </div>

                {/* Expanded Content */}
                {isExpanded && (
                  <div className="space-y-4 px-3 pb-3 pt-2 border-t bg-muted/30">
                    {/* Manual Input Toggle */}
                    <div className="flex items-center justify-between pb-2 border-b">
                      <Label className="text-sm font-medium">
                        Manual Input Mode
                      </Label>
                      <Switch
                        checked={!!(manualInputMode.get(index) ?? true)}
                        onCheckedChange={() => toggleManualInput(index)}
                        disabled={disabled || !selectedConnectionId}
                      />
                    </div>

                    {manualInputMode.get(index) !== false ? (
                      // Manual Input Mode - Show Input fields
                      <>
                        <div className="grid grid-cols-2 gap-4">
                          <div>
                            <Label>Source Database</Label>
                            <Input
                              value={mapping.source_database}
                              onChange={(e) =>
                                handleChange(
                                  index,
                                  "source_database",
                                  e.target.value
                                )
                              }
                              disabled={disabled}
                              className="mt-2"
                              placeholder="DATABASE_NAME"
                            />
                          </div>
                          <div>
                            <Label>Source Schema</Label>
                            <Input
                              value={mapping.source_schema}
                              onChange={(e) =>
                                handleChange(
                                  index,
                                  "source_schema",
                                  e.target.value
                                )
                              }
                              disabled={disabled}
                              className="mt-2"
                              placeholder="SCHEMA_NAME"
                            />
                          </div>
                        </div>

                        <div className="grid grid-cols-2 gap-4">
                          <div>
                            <Label>Source Table</Label>
                            <Input
                              value={mapping.source_table}
                              onChange={(e) =>
                                handleChange(
                                  index,
                                  "source_table",
                                  e.target.value
                                )
                              }
                              disabled={disabled}
                              className="mt-2"
                              placeholder="TABLE_NAME"
                            />
                          </div>
                          <div>
                            <Label>Source Column</Label>
                            <Input
                              value={mapping.source_column}
                              onChange={(e) =>
                                handleChange(
                                  index,
                                  "source_column",
                                  e.target.value
                                )
                              }
                              disabled={disabled}
                              className="mt-2"
                              placeholder="COLUMN_NAME"
                            />
                          </div>
                        </div>
                      </>
                    ) : (
                      // Dropdown Mode - Show Select components from connection metadata
                      <>
                        <div className="grid grid-cols-2 gap-4">
                          <div>
                            <Label>Source Database</Label>
                            <Popover
                              open={openComboboxes.get(`db-${index}`) || false}
                              onOpenChange={(open) => {
                                const newMap = new Map(openComboboxes);
                                newMap.set(`db-${index}`, open);
                                setOpenComboboxes(newMap);
                              }}
                            >
                              <PopoverTrigger asChild>
                                <Button
                                  variant="outline"
                                  role="combobox"
                                  aria-expanded={
                                    openComboboxes.get(`db-${index}`) || false
                                  }
                                  className="mt-2 w-full justify-between font-normal"
                                  disabled={disabled || !selectedConnectionId}
                                >
                                  {mapping.source_database || "Select database"}
                                  <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                                </Button>
                              </PopoverTrigger>
                              <PopoverContent className="w-[300px] p-0">
                                <Command>
                                  <CommandInput placeholder="Search database..." />
                                  <CommandList>
                                    <CommandEmpty>
                                      No database found.
                                    </CommandEmpty>
                                    <CommandGroup>
                                      {databases.map((db) => (
                                        <CommandItem
                                          key={db}
                                          value={db}
                                          onSelect={() => {
                                            handleChange(
                                              index,
                                              "source_database",
                                              db
                                            );
                                            fetchSchemas(index, db);
                                            const newMap = new Map(
                                              openComboboxes
                                            );
                                            newMap.set(`db-${index}`, false);
                                            setOpenComboboxes(newMap);
                                          }}
                                        >
                                          <Check
                                            className={
                                              mapping.source_database === db
                                                ? "mr-2 h-4 w-4 opacity-100"
                                                : "mr-2 h-4 w-4 opacity-0"
                                            }
                                          />
                                          {db}
                                        </CommandItem>
                                      ))}
                                    </CommandGroup>
                                  </CommandList>
                                </Command>
                              </PopoverContent>
                            </Popover>
                          </div>
                          <div>
                            <Label>Source Schema</Label>
                            <Popover
                              open={
                                openComboboxes.get(`schema-${index}`) || false
                              }
                              onOpenChange={(open) => {
                                const newMap = new Map(openComboboxes);
                                newMap.set(`schema-${index}`, open);
                                setOpenComboboxes(newMap);
                              }}
                            >
                              <PopoverTrigger asChild>
                                <Button
                                  variant="outline"
                                  role="combobox"
                                  aria-expanded={
                                    openComboboxes.get(`schema-${index}`) ||
                                    false
                                  }
                                  className="mt-2 w-full justify-between font-normal"
                                  disabled={
                                    disabled ||
                                    !mapping.source_database ||
                                    !!loadingMetadata.get(index)
                                  }
                                >
                                  {mapping.source_schema || "Select schema"}
                                  <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                                </Button>
                              </PopoverTrigger>
                              <PopoverContent className="w-[300px] p-0">
                                <Command>
                                  <CommandInput placeholder="Search schema..." />
                                  <CommandList>
                                    <CommandEmpty>
                                      No schema found.
                                    </CommandEmpty>
                                    <CommandGroup>
                                      {(schemas.get(index) || []).map(
                                        (schema) => (
                                          <CommandItem
                                            key={schema}
                                            value={schema}
                                            onSelect={() => {
                                              handleChange(
                                                index,
                                                "source_schema",
                                                schema
                                              );
                                              fetchTables(
                                                index,
                                                mapping.source_database,
                                                schema
                                              );
                                              const newMap = new Map(
                                                openComboboxes
                                              );
                                              newMap.set(
                                                `schema-${index}`,
                                                false
                                              );
                                              setOpenComboboxes(newMap);
                                            }}
                                          >
                                            <Check
                                              className={
                                                mapping.source_schema === schema
                                                  ? "mr-2 h-4 w-4 opacity-100"
                                                  : "mr-2 h-4 w-4 opacity-0"
                                              }
                                            />
                                            {schema}
                                          </CommandItem>
                                        )
                                      )}
                                    </CommandGroup>
                                  </CommandList>
                                </Command>
                              </PopoverContent>
                            </Popover>
                          </div>
                        </div>

                        <div className="grid grid-cols-2 gap-4">
                          <div>
                            <Label>Source Table</Label>
                            <Popover
                              open={
                                openComboboxes.get(`table-${index}`) || false
                              }
                              onOpenChange={(open) => {
                                const newMap = new Map(openComboboxes);
                                newMap.set(`table-${index}`, open);
                                setOpenComboboxes(newMap);
                              }}
                            >
                              <PopoverTrigger asChild>
                                <Button
                                  variant="outline"
                                  role="combobox"
                                  aria-expanded={
                                    openComboboxes.get(`table-${index}`) ||
                                    false
                                  }
                                  className="mt-2 w-full justify-between font-normal"
                                  disabled={
                                    disabled ||
                                    !mapping.source_schema ||
                                    !!loadingMetadata.get(index)
                                  }
                                >
                                  {mapping.source_table ||
                                    (loadingMetadata.get(index)
                                      ? "Loading..."
                                      : "Select table")}
                                  <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                                </Button>
                              </PopoverTrigger>
                              <PopoverContent className="w-[300px] p-0">
                                <Command>
                                  <CommandInput placeholder="Search table..." />
                                  <CommandList>
                                    <CommandEmpty>No table found.</CommandEmpty>
                                    <CommandGroup>
                                      {(tables.get(index) || []).map(
                                        (table) => (
                                          <CommandItem
                                            key={table}
                                            value={table}
                                            onSelect={() => {
                                              handleChange(
                                                index,
                                                "source_table",
                                                table
                                              );
                                              fetchColumns(
                                                index,
                                                mapping.source_database,
                                                mapping.source_schema,
                                                table
                                              );
                                              const newMap = new Map(
                                                openComboboxes
                                              );
                                              newMap.set(
                                                `table-${index}`,
                                                false
                                              );
                                              setOpenComboboxes(newMap);
                                            }}
                                          >
                                            <Check
                                              className={
                                                mapping.source_table === table
                                                  ? "mr-2 h-4 w-4 opacity-100"
                                                  : "mr-2 h-4 w-4 opacity-0"
                                              }
                                            />
                                            {table}
                                          </CommandItem>
                                        )
                                      )}
                                    </CommandGroup>
                                  </CommandList>
                                </Command>
                              </PopoverContent>
                            </Popover>
                          </div>
                          <div>
                            <Label>Source Column</Label>
                            <Popover
                              open={
                                openComboboxes.get(`column-${index}`) || false
                              }
                              onOpenChange={(open) => {
                                const newMap = new Map(openComboboxes);
                                newMap.set(`column-${index}`, open);
                                setOpenComboboxes(newMap);
                              }}
                            >
                              <PopoverTrigger asChild>
                                <Button
                                  variant="outline"
                                  role="combobox"
                                  aria-expanded={
                                    openComboboxes.get(`column-${index}`) ||
                                    false
                                  }
                                  className="mt-2 w-full justify-between font-normal"
                                  disabled={
                                    disabled ||
                                    !mapping.source_table ||
                                    !!loadingMetadata.get(index)
                                  }
                                >
                                  {mapping.source_column ||
                                    (loadingMetadata.get(index)
                                      ? "Loading..."
                                      : "Select column")}
                                  <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                                </Button>
                              </PopoverTrigger>
                              <PopoverContent className="w-[300px] p-0">
                                <Command>
                                  <CommandInput placeholder="Search column..." />
                                  <CommandList>
                                    <CommandEmpty>
                                      No column found.
                                    </CommandEmpty>
                                    <CommandGroup>
                                      {(columns.get(index) || []).map(
                                        (column) => (
                                          <CommandItem
                                            key={column}
                                            value={column}
                                            onSelect={() => {
                                              handleChange(
                                                index,
                                                "source_column",
                                                column
                                              );
                                              fetchUUIDs(
                                                index,
                                                mapping.source_database,
                                                mapping.source_schema,
                                                mapping.source_table,
                                                column
                                              );
                                              const newMap = new Map(
                                                openComboboxes
                                              );
                                              newMap.set(
                                                `column-${index}`,
                                                false
                                              );
                                              setOpenComboboxes(newMap);
                                            }}
                                          >
                                            <Check
                                              className={
                                                mapping.source_column === column
                                                  ? "mr-2 h-4 w-4 opacity-100"
                                                  : "mr-2 h-4 w-4 opacity-0"
                                              }
                                            />
                                            {column}
                                          </CommandItem>
                                        )
                                      )}
                                    </CommandGroup>
                                  </CommandList>
                                </Command>
                              </PopoverContent>
                            </Popover>
                          </div>
                        </div>

                        <div className="grid grid-cols-2 gap-4">
                          <div>
                            <Label>Source UID (Optional)</Label>
                            <Popover
                              open={openComboboxes.get(`uid-${index}`) || false}
                              onOpenChange={(open) => {
                                const newMap = new Map(openComboboxes);
                                newMap.set(`uid-${index}`, open);
                                setOpenComboboxes(newMap);
                              }}
                            >
                              <PopoverTrigger asChild>
                                <Button
                                  variant="outline"
                                  role="combobox"
                                  aria-expanded={
                                    openComboboxes.get(`uid-${index}`) || false
                                  }
                                  className="mt-2 w-full justify-between font-normal"
                                  disabled={
                                    disabled ||
                                    !mapping.source_column ||
                                    !!loadingMetadata.get(index)
                                  }
                                >
                                  {mapping.source_uid ||
                                    (loadingMetadata.get(index)
                                      ? "Loading..."
                                      : "Select UUID value")}
                                  <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                                </Button>
                              </PopoverTrigger>
                              <PopoverContent className="w-[300px] p-0">
                                <Command>
                                  <CommandInput placeholder="Search UUID..." />
                                  <CommandList>
                                    <CommandEmpty>No UUID found.</CommandEmpty>
                                    <CommandGroup>
                                      {(uuids.get(index) || []).map((uuid) => (
                                        <CommandItem
                                          key={uuid}
                                          value={uuid}
                                          onSelect={() => {
                                            handleChange(
                                              index,
                                              "source_uid",
                                              uuid
                                            );
                                            const newMap = new Map(
                                              openComboboxes
                                            );
                                            newMap.set(`uid-${index}`, false);
                                            setOpenComboboxes(newMap);
                                          }}
                                        >
                                          <Check
                                            className={
                                              mapping.source_uid === uuid
                                                ? "mr-2 h-4 w-4 opacity-100"
                                                : "mr-2 h-4 w-4 opacity-0"
                                            }
                                          />
                                          {uuid}
                                        </CommandItem>
                                      ))}
                                    </CommandGroup>
                                  </CommandList>
                                </Command>
                              </PopoverContent>
                            </Popover>
                          </div>
                          <div>
                            <HelpLabel topicId="precedenceGroups">Precedence Group</HelpLabel>
                            <Input
                              value={mapping.precedence_group || ""}
                              onChange={(e) =>
                                handleChange(
                                  index,
                                  "precedence_group",
                                  e.target.value
                                )
                              }
                              disabled={disabled}
                              className="mt-2"
                              placeholder="Group identifier"
                            />
                          </div>
                        </div>
                      </>
                    )}

                    {manualInputMode.get(index) !== false ? (
                      // Show these inputs only in manual mode
                      <div className="grid grid-cols-2 gap-4">
                        <div>
                          <Label>Source UID (Optional)</Label>
                          <Input
                            value={mapping.source_uid || ""}
                            onChange={(e) =>
                              handleChange(index, "source_uid", e.target.value)
                            }
                            disabled={disabled}
                            className="mt-2"
                            placeholder="Unique identifier value"
                          />
                        </div>
                        <div>
                          <HelpLabel topicId="precedenceGroups">Precedence Group</HelpLabel>
                          <Input
                            value={mapping.precedence_group || ""}
                            onChange={(e) =>
                              handleChange(
                                index,
                                "precedence_group",
                                e.target.value
                              )
                            }
                            disabled={disabled}
                            className="mt-2"
                            placeholder="Group identifier"
                          />
                        </div>
                      </div>
                    ) : null}

                    {/* Mapping Flags */}
                    <div className="border-t pt-4">
                      <HelpLabel topicId="hierarchyFlags" className="text-sm font-medium mb-3 block">
                        Mapping Flags
                      </HelpLabel>
                      <div className="grid grid-cols-2 gap-3">
                        <div className="flex items-center justify-between">
                          <Label className="text-sm">Include</Label>
                          <Switch
                            checked={!!mapping.flags?.include_flag}
                            onCheckedChange={(v) =>
                              handleFlagChange(index, "include_flag", v)
                            }
                            disabled={disabled}
                          />
                        </div>
                        <div className="flex items-center justify-between">
                          <Label className="text-sm">Exclude</Label>
                          <Switch
                            checked={!!mapping.flags?.exclude_flag}
                            onCheckedChange={(v) =>
                              handleFlagChange(index, "exclude_flag", v)
                            }
                            disabled={disabled}
                          />
                        </div>
                        <div className="flex items-center justify-between">
                          <Label className="text-sm">Transform</Label>
                          <Switch
                            checked={!!mapping.flags?.transform_flag}
                            onCheckedChange={(v) =>
                              handleFlagChange(index, "transform_flag", v)
                            }
                            disabled={disabled}
                          />
                        </div>
                        <div className="flex items-center justify-between">
                          <Label className="text-sm">Active</Label>
                          <Switch
                            checked={!!mapping.flags?.active_flag}
                            onCheckedChange={(v) =>
                              handleFlagChange(index, "active_flag", v)
                            }
                            disabled={disabled}
                          />
                        </div>
                      </div>
                    </div>

                    {/* Custom Flags */}
                    <div className="border-t pt-4 mt-4">
                      <div className="flex items-center justify-between mb-3">
                        <Label className="text-sm font-medium">
                          Custom Flags
                        </Label>
                        {!disabled && (
                          <Button
                            variant="ghost"
                            size="sm"
                            className="h-6 text-xs"
                            onClick={() => {
                              setCurrentMappingIndex(index);
                              setNewFlagName("");
                              setCustomFlagError("");
                              setShowCustomFlagDialog(true);
                            }}
                          >
                            <Plus className="w-3 h-3 mr-1" />
                            Add
                          </Button>
                        )}
                      </div>
                      {mapping.flags.customFlags &&
                      Object.keys(mapping.flags.customFlags).length > 0 ? (
                        <div className="space-y-2">
                          {Object.entries(mapping.flags.customFlags).map(
                            ([key, value]) => (
                              <div
                                key={key}
                                className="flex items-center justify-between text-xs"
                              >
                                <Label className="text-xs capitalize">
                                  {key.replace(/_/g, " ")}
                                </Label>
                                <div className="flex items-center gap-2">
                                  <Switch
                                    checked={!!value}
                                    onCheckedChange={(v) => {
                                      const updated = [...mappings];
                                      const customFlags = {
                                        ...updated[index].flags.customFlags,
                                      };
                                      customFlags[key] = v;
                                      updated[index] = {
                                        ...updated[index],
                                        flags: {
                                          ...updated[index].flags,
                                          customFlags,
                                        },
                                      };
                                      onChange(updated);
                                    }}
                                    disabled={disabled}
                                  />
                                  {!disabled && (
                                    <Button
                                      variant="ghost"
                                      size="sm"
                                      className="h-5 w-5 p-0"
                                      onClick={() => {
                                        const updated = [...mappings];
                                        const customFlags = {
                                          ...updated[index].flags.customFlags,
                                        };
                                        delete customFlags[key];
                                        updated[index] = {
                                          ...updated[index],
                                          flags: {
                                            ...updated[index].flags,
                                            customFlags,
                                          },
                                        };
                                        onChange(updated);
                                      }}
                                    >
                                      <X className="w-2.5 h-2.5 text-destructive" />
                                    </Button>
                                  )}
                                </div>
                              </div>
                            )
                          )}
                        </div>
                      ) : (
                        <div className="text-center py-3 text-muted-foreground text-xs bg-muted/30 rounded">
                          No custom flags
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      ) : (
        /* Table View */
        <div className="bg-card rounded-lg overflow-hidden">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-muted/50 border-b">
                <tr>
                  {onMappingSelectionChange && (
                    <th className="px-3 py-2 text-center font-medium w-10">
                      <Checkbox
                        checked={
                          mappings.length > 0 &&
                          selectedMappings.length === mappings.length
                        }
                        onCheckedChange={toggleSelectAll}
                        disabled={disabled || mappings.length === 0}
                      />
                    </th>
                  )}
                  <th className="px-3 py-2 text-left font-medium">#</th>
                  <th className="px-3 py-2 text-left font-medium">Database</th>
                  <th className="px-3 py-2 text-left font-medium">Schema</th>
                  <th className="px-3 py-2 text-left font-medium">Table</th>
                  <th className="px-3 py-2 text-left font-medium">Column</th>
                  <th className="px-3 py-2 text-left font-medium">UID</th>
                  <th className="px-3 py-2 text-left font-medium">
                    Precedence
                  </th>
                  <th className="px-3 py-2 text-center font-medium">Active</th>
                  <th className="px-3 py-2 text-center font-medium">Actions</th>
                </tr>
              </thead>
              <tbody>
                {mappings.map((mapping, index) => {
                  const isSelected = selectedMappings.includes(index);

                  return (
                    <tr
                      key={index}
                      className={`border-b hover:bg-accent/50 ${
                        isSelected ? "bg-primary/5" : ""
                      }`}
                    >
                      {onMappingSelectionChange && (
                        <td className="px-3 py-2 text-center">
                          <Checkbox
                            checked={isSelected}
                            onCheckedChange={() =>
                              toggleMappingSelection(index)
                            }
                            disabled={disabled}
                          />
                        </td>
                      )}
                      <td className="px-3 py-2 font-medium">
                        #{mapping.mapping_index}
                      </td>
                      <td className="px-3 py-2">
                        <Input
                          value={mapping.source_database}
                          onChange={(e) =>
                            handleChange(
                              index,
                              "source_database",
                              e.target.value
                            )
                          }
                          disabled={disabled}
                          className="h-8 text-xs"
                          placeholder="DATABASE"
                        />
                      </td>
                      <td className="px-3 py-2">
                        <Input
                          value={mapping.source_schema}
                          onChange={(e) =>
                            handleChange(index, "source_schema", e.target.value)
                          }
                          disabled={disabled}
                          className="h-8 text-xs"
                          placeholder="SCHEMA"
                        />
                      </td>
                      <td className="px-3 py-2">
                        <Input
                          value={mapping.source_table}
                          onChange={(e) =>
                            handleChange(index, "source_table", e.target.value)
                          }
                          disabled={disabled}
                          className="h-8 text-xs"
                          placeholder="TABLE"
                        />
                      </td>
                      <td className="px-3 py-2">
                        <Input
                          value={mapping.source_column}
                          onChange={(e) =>
                            handleChange(index, "source_column", e.target.value)
                          }
                          disabled={disabled}
                          className="h-8 text-xs"
                          placeholder="COLUMN"
                        />
                      </td>
                      <td className="px-3 py-2">
                        <Input
                          value={mapping.source_uid || ""}
                          onChange={(e) =>
                            handleChange(index, "source_uid", e.target.value)
                          }
                          disabled={disabled}
                          className="h-8 text-xs"
                          placeholder="UID"
                        />
                      </td>
                      <td className="px-3 py-2">
                        <Input
                          value={mapping.precedence_group || ""}
                          onChange={(e) =>
                            handleChange(
                              index,
                              "precedence_group",
                              e.target.value
                            )
                          }
                          disabled={disabled}
                          className="h-8 text-xs"
                          placeholder="Group"
                        />
                      </td>
                      <td className="px-3 py-2 text-center">
                        <Switch
                          checked={!!mapping.flags?.active_flag}
                          onCheckedChange={(v) =>
                            handleFlagChange(index, "active_flag", v)
                          }
                          disabled={disabled}
                        />
                      </td>
                      <td className="px-3 py-2 text-center">
                        <div className="flex items-center justify-center gap-1">
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => handleDuplicate(index)}
                            disabled={disabled}
                            className="h-7 w-7 p-0"
                            title="Duplicate mapping"
                          >
                            <Copy className="w-3.5 h-3.5 text-muted-foreground" />
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => handleRemove(index)}
                            disabled={disabled}
                            className="h-7 w-7 p-0"
                          >
                            <Trash2 className="w-3.5 h-3.5 text-destructive" />
                          </Button>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Custom Flag Dialog */}
      <Dialog
        open={showCustomFlagDialog}
        onOpenChange={setShowCustomFlagDialog}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add Custom Mapping Flag</DialogTitle>
            <DialogDescription>
              Enter a name for your custom flag (e.g., is_primary_source,
              needs_validation)
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-4">
            <div>
              <Label htmlFor="mapping-flag-name">Flag Name</Label>
              <Input
                id="mapping-flag-name"
                value={newFlagName}
                onChange={(e) => {
                  setNewFlagName(e.target.value);
                  setCustomFlagError("");
                }}
                placeholder="e.g., is_primary_source"
                className="mt-2"
                onKeyDown={(e) => {
                  if (e.key === "Enter" && currentMappingIndex !== null) {
                    e.preventDefault();
                    const trimmedName = newFlagName.trim();
                    if (!trimmedName) {
                      setCustomFlagError("Flag name is required");
                      return;
                    }
                    if (!/^[a-z_][a-z0-9_]*$/i.test(trimmedName)) {
                      setCustomFlagError(
                        "Use only letters, numbers, and underscores. Must start with a letter."
                      );
                      return;
                    }
                    const updated = [...mappings];
                    const customFlags =
                      updated[currentMappingIndex].flags.customFlags || {};
                    if (customFlags[trimmedName]) {
                      setCustomFlagError("This flag already exists");
                      return;
                    }
                    updated[currentMappingIndex] = {
                      ...updated[currentMappingIndex],
                      flags: {
                        ...updated[currentMappingIndex].flags,
                        customFlags: { ...customFlags, [trimmedName]: false },
                      },
                    };
                    onChange(updated);
                    setShowCustomFlagDialog(false);
                    setNewFlagName("");
                    setCurrentMappingIndex(null);
                  }
                }}
              />
              {customFlagError && (
                <p className="text-sm text-destructive mt-2">
                  {customFlagError}
                </p>
              )}
            </div>
          </div>
          <DialogFooter>
            <Button
              variant="outline"
              onClick={() => {
                setShowCustomFlagDialog(false);
                setNewFlagName("");
                setCustomFlagError("");
                setCurrentMappingIndex(null);
              }}
            >
              Cancel
            </Button>
            <Button
              onClick={() => {
                if (currentMappingIndex === null) return;
                const trimmedName = newFlagName.trim();
                if (!trimmedName) {
                  setCustomFlagError("Flag name is required");
                  return;
                }
                if (!/^[a-z_][a-z0-9_]*$/i.test(trimmedName)) {
                  setCustomFlagError(
                    "Use only letters, numbers, and underscores. Must start with a letter."
                  );
                  return;
                }
                const updated = [...mappings];
                const customFlags =
                  updated[currentMappingIndex].flags.customFlags || {};
                if (customFlags[trimmedName]) {
                  setCustomFlagError("This flag already exists");
                  return;
                }
                updated[currentMappingIndex] = {
                  ...updated[currentMappingIndex],
                  flags: {
                    ...updated[currentMappingIndex].flags,
                    customFlags: { ...customFlags, [trimmedName]: false },
                  },
                };
                onChange(updated);
                setShowCustomFlagDialog(false);
                setNewFlagName("");
                setCurrentMappingIndex(null);
              }}
            >
              Add Flag
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
};
