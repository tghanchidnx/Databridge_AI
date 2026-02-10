import React, { useState } from "react";
import type { SmartHierarchyMaster } from "@/services/api/hierarchy";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Switch } from "@/components/ui/switch";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Save, X, Edit, Plus, Copy, GitBranch, Trash2 } from "lucide-react";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
  DropdownMenuSeparator,
} from "@/components/ui/dropdown-menu";
import { MappingArrayEditor } from "./MappingArrayEditor";
import { LevelConfigEditor } from "./LevelConfigEditor";
import { FormulaBuilder } from "./FormulaBuilder";
import { HierarchyDependencies } from "./HierarchyDependencies";
import { MappingQueryGenerator } from "./MappingQueryGenerator";
import { InheritedMappingsView } from "./InheritedMappingsView";
import { HelpTooltip, HelpLabel, HelpSection } from "@/components/ui/help-tooltip";

interface SmartHierarchyEditorProps {
  hierarchy: SmartHierarchyMaster;
  mode: "view" | "edit" | "create";
  onSave: (hierarchy: SmartHierarchyMaster) => void;
  onChange?: (hierarchy: SmartHierarchyMaster) => void;
  onCancel: () => void;
  onEdit: () => void;
  onDelete?: (hierarchyId: string) => void;
  allHierarchies?: SmartHierarchyMaster[];
  selectedForFormula?: Set<string>;
  onToggleFormulaSelection?: (id: string) => void;
  selectedHierarchiesForFormula?: SmartHierarchyMaster[];
  onClone?: (hierarchy: SmartHierarchyMaster, fullClone: boolean) => void;
  onAddChild?: (parentHierarchy: SmartHierarchyMaster) => void;
  databaseType?: "snowflake" | "postgres" | "mysql" | "sqlserver";
}

export const SmartHierarchyEditor: React.FC<SmartHierarchyEditorProps> = ({
  hierarchy,
  mode,
  onSave,
  onChange,
  onCancel,
  onEdit,
  onDelete,
  allHierarchies = [],
  selectedForFormula,
  onToggleFormulaSelection,
  selectedHierarchiesForFormula,
  onClone,
  onAddChild,
  databaseType,
}) => {
  const [editedHierarchy, setEditedHierarchy] = useState<SmartHierarchyMaster>(
    () => ({
      ...hierarchy,
      flags: {
        include_flag: hierarchy.flags?.include_flag ?? false,
        exclude_flag: hierarchy.flags?.exclude_flag ?? false,
        transform_flag: hierarchy.flags?.transform_flag ?? false,
        calculation_flag: hierarchy.flags?.calculation_flag ?? false,
        active_flag: hierarchy.flags?.active_flag ?? false,
        is_leaf_node: hierarchy.flags?.is_leaf_node ?? false,
        customFlags: hierarchy.flags?.customFlags ?? {},
      },
    })
  );
  const [showCustomFlagDialog, setShowCustomFlagDialog] = useState(false);
  const [showDeleteDialog, setShowDeleteDialog] = useState(false);
  const [newFlagName, setNewFlagName] = useState("");
  const [customFlagError, setCustomFlagError] = useState("");
  const [selectedMappingsForQuery, setSelectedMappingsForQuery] = useState<
    Array<{
      hierarchyId: string;
      hierarchyName: string;
      mappingIndex: number;
      database: string;
      schema: string;
      table: string;
      column: string;
      uid?: string;
      precedence_group?: string;
    }>
  >([]);
  const isEditing = mode === "edit" || mode === "create";

  // Update editedHierarchy when hierarchy prop changes
  React.useEffect(() => {
    if (hierarchy) {
      // Ensure all flags have boolean values (prevent controlled/uncontrolled switch)
      const normalizedHierarchy = {
        ...hierarchy,
        flags: {
          include_flag: hierarchy.flags?.include_flag ?? false,
          exclude_flag: hierarchy.flags?.exclude_flag ?? false,
          transform_flag: hierarchy.flags?.transform_flag ?? false,
          calculation_flag: hierarchy.flags?.calculation_flag ?? false,
          active_flag: hierarchy.flags?.active_flag ?? false,
          is_leaf_node: hierarchy.flags?.is_leaf_node ?? false,
          customFlags: hierarchy.flags?.customFlags ?? {},
        },
      };
      setEditedHierarchy(normalizedHierarchy);
    }
  }, [hierarchy]);

  // Safety check
  if (!hierarchy) {
    return (
      <div className="h-full flex items-center justify-center">
        <p className="text-muted-foreground">No hierarchy data available</p>
      </div>
    );
  }

  const handleChange = (field: keyof SmartHierarchyMaster, value: any) => {
    const updated = { ...editedHierarchy, [field]: value };
    setEditedHierarchy(updated);
    onChange?.(updated);
  };

  const handleFlagChange = (flag: string, value: boolean) => {
    const updated = {
      ...editedHierarchy,
      flags: { ...editedHierarchy.flags, [flag]: value },
    };
    setEditedHierarchy(updated);
    onChange?.(updated);
  };

  const handleMappingToggle = (mapping: any) => {
    setSelectedMappingsForQuery((prev) => {
      const exists = prev.some(
        (m) =>
          m.hierarchyId === mapping.hierarchyId &&
          m.mappingIndex === mapping.mappingIndex
      );
      if (exists) {
        return prev.filter(
          (m) =>
            !(
              m.hierarchyId === mapping.hierarchyId &&
              m.mappingIndex === mapping.mappingIndex
            )
        );
      }
      return [...prev, mapping];
    });
  };

  const handleSelectAllMappings = () => {
    const allMaps: any[] = [];
    [editedHierarchy].forEach((h) => {
      h.mapping?.forEach((m) => {
        allMaps.push({
          hierarchyId: h.hierarchyId,
          hierarchyName: h.hierarchyName,
          mappingIndex: m.mapping_index,
          database: m.source_database,
          schema: m.source_schema,
          table: m.source_table,
          column: m.source_column,
          uid: m.source_uid,
          precedence_group: m.precedence_group,
        });
      });
    });
    setSelectedMappingsForQuery(allMaps);
  };

  const handleClearAllMappings = () => {
    setSelectedMappingsForQuery([]);
  };

  const handleSave = () => {
    onSave(editedHierarchy);
  };

  return (
    <div className="h-full flex flex-col overflow-hidden">
      {/* Header */}
      <div className="border-b px-6 py-4 bg-muted/30">
        <div className="flex items-start justify-between">
          <div className="flex-1">
            {isEditing ? (
              <h3 className="text-lg font-semibold">
                {editedHierarchy.hierarchyName}
              </h3>
            ) : (
              <h3 className="text-lg font-semibold">
                {hierarchy.hierarchyName}
              </h3>
            )}

            {!isEditing &&
              (editedHierarchy.parentId ||
                editedHierarchy.sortOrder !== undefined) && (
                <div className="flex gap-4 mt-0 text-xs text-muted-foreground">
                  {editedHierarchy.parentId && (
                    <span>
                      Parent:{" "}
                      {allHierarchies.find(
                        (h) => h.hierarchyId === editedHierarchy.parentId
                      )?.hierarchyName || editedHierarchy.parentId}
                    </span>
                  )}
                  <span>Sort Order: {editedHierarchy.sortOrder || 0}</span>
                  <span>
                    Description: {hierarchy.description || "No description"}
                  </span>
                </div>
              )}
          </div>
          <div className="flex items-center gap-2 ml-4">
            {isEditing ? (
              <>
                <Button variant="default" size="sm" onClick={handleSave}>
                  <Save className="w-4 h-4 mr-1" />
                  Save
                </Button>
                <Button variant="outline" size="sm" onClick={onCancel}>
                  <X className="w-4 h-4 mr-1" />
                  Cancel
                </Button>
              </>
            ) : (
              <>
                <Button variant="outline" size="sm" onClick={onEdit}>
                  <Edit className="w-4 h-4 mr-1" />
                  Edit
                </Button>
                {onDelete && (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setShowDeleteDialog(true)}
                  >
                    <Trash2 className="w-4 h-4 mr-1 text-destructive" />
                    Delete
                  </Button>
                )}
                {(onClone || onAddChild) && (
                  <DropdownMenu>
                    <DropdownMenuTrigger asChild>
                      <Button variant="outline" size="sm">
                        <Plus className="w-4 h-4 mr-1" />
                        Actions
                      </Button>
                    </DropdownMenuTrigger>
                    <DropdownMenuContent align="end">
                      {onClone && (
                        <>
                          <DropdownMenuItem
                            onClick={() => onClone(hierarchy, true)}
                          >
                            <Copy className="w-4 h-4 mr-2" />
                            Clone (Full)
                          </DropdownMenuItem>
                          <DropdownMenuItem
                            onClick={() => onClone(hierarchy, false)}
                          >
                            <Copy className="w-4 h-4 mr-2" />
                            Clone (Structure Only)
                          </DropdownMenuItem>
                          {onAddChild && <DropdownMenuSeparator />}
                        </>
                      )}
                      {onAddChild && (
                        <DropdownMenuItem onClick={() => onAddChild(hierarchy)}>
                          <GitBranch className="w-4 h-4 mr-2" />
                          Add Child Hierarchy
                        </DropdownMenuItem>
                      )}
                    </DropdownMenuContent>
                  </DropdownMenu>
                )}
              </>
            )}
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-auto p-4">
        <Tabs defaultValue="mapping" className="w-full">
          <TabsList className="grid w-full grid-cols-4">
            <TabsTrigger value="mapping" className="flex items-center gap-1">
              Mapping
              <HelpTooltip topicId="sourceMapping" iconOnly iconSize="sm" />
            </TabsTrigger>
            <TabsTrigger value="details" className="flex items-center gap-1">
              Hierarchy Node Details
              <HelpTooltip topicId="hierarchyLevels" iconOnly iconSize="sm" />
            </TabsTrigger>
            <TabsTrigger value="formula" className="flex items-center gap-1">
              Formula
              <HelpTooltip topicId="formulaBuilder" iconOnly iconSize="sm" />
            </TabsTrigger>
            <TabsTrigger value="dependencies" className="flex items-center gap-1">
              Dependencies
              <HelpTooltip topicId="parentChild" iconOnly iconSize="sm" />
            </TabsTrigger>
          </TabsList>

          {/* Details Tab */}
          <TabsContent value="details" className="mt-3 space-y-3">
            <div className="grid gap-3">
              {/* Hierarchy Name and Description */}
              <div className="bg-card shadow-md rounded-lg p-4 space-y-2 ring-1 ring-border/50">
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <Label className="text-xs font-medium text-muted-foreground">
                      Hierarchy Name
                    </Label>
                    {isEditing ? (
                      <Input
                        value={editedHierarchy.hierarchyName}
                        onChange={(e) =>
                          handleChange("hierarchyName", e.target.value)
                        }
                        className="mt-1 h-8"
                        placeholder="Enter name"
                      />
                    ) : (
                      <div className="mt-1 text-sm font-medium">
                        {hierarchy.hierarchyName}
                      </div>
                    )}
                  </div>
                  <div>
                    <Label className="text-xs font-medium text-muted-foreground">
                      Description
                    </Label>
                    {isEditing ? (
                      <Input
                        value={editedHierarchy.description || ""}
                        onChange={(e) =>
                          handleChange("description", e.target.value)
                        }
                        className="mt-1 h-8"
                        placeholder="Enter description"
                      />
                    ) : (
                      <div className="mt-1 text-sm">
                        {hierarchy.description || "No description"}
                      </div>
                    )}
                  </div>
                </div>
              </div>

              {/* Parent and Sort Order */}
              <div className="bg-card shadow-md rounded-lg p-4 ring-1 ring-border/50">
                <div className="grid grid-cols-2 gap-3">
                  <div>
                    <HelpLabel topicId="parentChild" className="text-xs font-medium text-muted-foreground">
                      Parent Hierarchy
                    </HelpLabel>
                    {isEditing ? (
                      <Select
                        value={editedHierarchy.parentId || "none"}
                        onValueChange={(value) =>
                          handleChange(
                            "parentId",
                            value === "none" ? null : value
                          )
                        }
                      >
                        <SelectTrigger className="mt-1 h-8">
                          <SelectValue placeholder="Select parent" />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="none">No Parent (Root)</SelectItem>
                          {allHierarchies
                            .filter(
                              (h) => h.hierarchyId !== hierarchy.hierarchyId
                            )
                            .map((h) => (
                              <SelectItem
                                key={h.hierarchyId}
                                value={h.hierarchyId}
                              >
                                {h.hierarchyName}
                              </SelectItem>
                            ))}
                        </SelectContent>
                      </Select>
                    ) : (
                      <div className="mt-1 text-sm">
                        {editedHierarchy.parentId
                          ? allHierarchies.find(
                              (h) => h.hierarchyId === editedHierarchy.parentId
                            )?.hierarchyName || "Unknown"
                          : "No Parent (Root)"}
                      </div>
                    )}
                  </div>
                  <div>
                    <HelpLabel topicId="sortOrder" className="text-xs font-medium text-muted-foreground">
                      Sort Order
                    </HelpLabel>
                    {isEditing ? (
                      <Input
                        type="number"
                        min="0"
                        value={editedHierarchy.sortOrder || 0}
                        onChange={(e) =>
                          handleChange(
                            "sortOrder",
                            parseInt(e.target.value) || 0
                          )
                        }
                        className="mt-1 h-8"
                      />
                    ) : (
                      <div className="mt-1 text-sm">
                        {editedHierarchy.sortOrder || 0}
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </div>
            <div className="grid gap-3">
              {/* Hierarchy Flags Section */}
              <div className="flex items-center gap-2 mb-1">
                <span className="font-medium text-sm">Hierarchy Flags</span>
                <HelpTooltip topicId="hierarchyFlags" iconOnly iconSize="sm" />
              </div>
              <div className="grid grid-cols-3 gap-3">
                <div className="flex flex-col justify-between p-4 rounded-lg bg-card shadow-md ring-1 ring-border/50">
                  <div className="flex items-start justify-between mb-3">
                    <div className="flex-1">
                      <Label className="font-medium text-sm text-card-foreground">
                        Include Flag
                      </Label>
                      <p className="text-xs text-muted-foreground mt-1">
                        Include this hierarchy in processing
                      </p>
                    </div>
                    <Switch
                      checked={!!editedHierarchy.flags?.include_flag}
                      onCheckedChange={(v) =>
                        handleFlagChange("include_flag", v)
                      }
                      disabled={!isEditing}
                      className="ml-2"
                    />
                  </div>
                </div>

                <div className="flex flex-col justify-between p-4 rounded-lg bg-card shadow-md ring-1 ring-border/50">
                  <div className="flex items-start justify-between mb-3">
                    <div className="flex-1">
                      <Label className="font-medium text-sm text-card-foreground">
                        Exclude Flag
                      </Label>
                      <p className="text-xs text-muted-foreground mt-1">
                        Exclude this hierarchy from processing
                      </p>
                    </div>
                    <Switch
                      checked={!!editedHierarchy.flags?.exclude_flag}
                      onCheckedChange={(v) =>
                        handleFlagChange("exclude_flag", v)
                      }
                      disabled={!isEditing}
                      className="ml-2"
                    />
                  </div>
                </div>

                <div className="flex flex-col justify-between p-4 rounded-lg bg-card shadow-md ring-1 ring-border/50">
                  <div className="flex items-start justify-between mb-3">
                    <div className="flex-1">
                      <Label className="font-medium text-sm text-card-foreground">
                        Transform Flag
                      </Label>
                      <p className="text-xs text-muted-foreground mt-1">
                        Apply transformations during processing
                      </p>
                    </div>
                    <Switch
                      checked={!!editedHierarchy.flags?.transform_flag}
                      onCheckedChange={(v) =>
                        handleFlagChange("transform_flag", v)
                      }
                      disabled={!isEditing}
                      className="ml-2"
                    />
                  </div>
                </div>

                <div className="flex flex-col justify-between p-4 rounded-lg bg-card shadow-md ring-1 ring-border/50">
                  <div className="flex items-start justify-between mb-3">
                    <div className="flex-1">
                      <Label className="font-medium text-sm text-card-foreground">
                        Calculation Flag
                      </Label>
                      <p className="text-xs text-muted-foreground mt-1">
                        Mark as a calculated field
                      </p>
                    </div>
                    <Switch
                      checked={!!editedHierarchy.flags?.calculation_flag}
                      onCheckedChange={(v) =>
                        handleFlagChange("calculation_flag", v)
                      }
                      disabled={!isEditing}
                      className="ml-2"
                    />
                  </div>
                </div>
                <div className="flex flex-col justify-between p-4 rounded-lg bg-card shadow-md ring-1 ring-border/50">
                  <div className="flex items-start justify-between mb-3">
                    <div className="flex-1">
                      <Label className="font-medium text-sm text-card-foreground">
                        Active Flag
                      </Label>
                      <p className="text-xs text-muted-foreground mt-1">
                        Enable/disable this hierarchy
                      </p>
                    </div>
                    <Switch
                      checked={!!editedHierarchy.flags?.active_flag}
                      onCheckedChange={(v) =>
                        handleFlagChange("active_flag", v)
                      }
                      disabled={!isEditing}
                      className="ml-2"
                    />
                  </div>
                </div>

                <div className="flex flex-col justify-between p-4 rounded-lg bg-card shadow-md ring-1 ring-border/50">
                  <div className="flex items-start justify-between mb-3">
                    <div className="flex-1">
                      <Label className="font-medium text-sm text-card-foreground">
                        Is Leaf Node
                      </Label>
                      <p className="text-xs text-muted-foreground mt-1">
                        Mark as leaf node (no children)
                      </p>
                    </div>
                    <Switch
                      checked={!!editedHierarchy.flags?.is_leaf_node}
                      onCheckedChange={(v) =>
                        handleFlagChange("is_leaf_node", v)
                      }
                      disabled={!isEditing}
                      className="ml-2"
                    />
                  </div>
                </div>
              </div>

              {/* Custom Flags Section */}
              <div className="border-t pt-3 mt-3">
                <div className="flex items-center justify-between mb-4">
                  <div>
                    <Label className="font-medium text-base">
                      Custom Flags
                    </Label>
                    <p className="text-sm text-muted-foreground">
                      Add your own custom boolean flags
                    </p>
                  </div>
                  {isEditing && (
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => {
                        setNewFlagName("");
                        setCustomFlagError("");
                        setShowCustomFlagDialog(true);
                      }}
                    >
                      <Plus className="w-4 h-4 mr-1" />
                      Add Flag
                    </Button>
                  )}
                </div>
                {editedHierarchy.flags.customFlags &&
                Object.keys(editedHierarchy.flags.customFlags).length > 0 ? (
                  <div className="space-y-2">
                    {Object.entries(editedHierarchy.flags.customFlags).map(
                      ([key, value]) => (
                        <div
                          key={key}
                          className="flex items-center justify-between p-4 bg-card shadow-md rounded-lg ring-1 ring-border/50"
                        >
                          <Label className="font-medium capitalize">
                            {key.replace(/_/g, " ")}
                          </Label>
                          <div className="flex items-center gap-2">
                            <Switch
                              checked={!!value}
                              onCheckedChange={(v) => {
                                const customFlags = {
                                  ...editedHierarchy.flags.customFlags,
                                };
                                customFlags[key] = v;
                                handleChange("flags", {
                                  ...editedHierarchy.flags,
                                  customFlags,
                                });
                              }}
                              disabled={!isEditing}
                            />
                            {isEditing && (
                              <Button
                                variant="ghost"
                                size="sm"
                                className="h-7 w-7 p-0"
                                onClick={() => {
                                  const customFlags = {
                                    ...editedHierarchy.flags.customFlags,
                                  };
                                  delete customFlags[key];
                                  handleChange("flags", {
                                    ...editedHierarchy.flags,
                                    customFlags,
                                  });
                                }}
                              >
                                <X className="w-3 h-3 text-destructive" />
                              </Button>
                            )}
                          </div>
                        </div>
                      )
                    )}
                  </div>
                ) : (
                  <div className="text-center py-6 text-muted-foreground text-sm bg-card shadow-md rounded-lg ring-1 ring-border/50">
                    No custom flags.{" "}
                    {isEditing && "Click 'Add Flag' to create one."}
                  </div>
                )}
              </div>
            </div>
          </TabsContent>

          {/* Mapping Tab */}
          <TabsContent value="mapping" className="mt-3">
            <div className="grid grid-cols-1 gap-4">
              {/* Inherited Mappings Summary - Shows counts from children */}
              <InheritedMappingsView
                hierarchy={editedHierarchy}
                allHierarchies={allHierarchies}
              />

              {/* Mapping Array Editor with Checkboxes */}
              <MappingArrayEditor
                mappings={editedHierarchy.mapping}
                onChange={(mappings) => handleChange("mapping", mappings)}
                disabled={!isEditing}
                projectId={hierarchy.projectId}
                selectedMappings={selectedMappingsForQuery
                  .map((m) => {
                    // Find the array index for this mapping_index
                    return (
                      editedHierarchy.mapping?.findIndex(
                        (mapping) => mapping.mapping_index === m.mappingIndex
                      ) ?? -1
                    );
                  })
                  .filter((idx) => idx >= 0)}
                onMappingSelectionChange={(indexes) => {
                  const newSelected = indexes
                    .map((idx) => {
                      const mapping = editedHierarchy.mapping?.[idx];
                      if (!mapping) return null;
                      return {
                        hierarchyId: editedHierarchy.hierarchyId,
                        hierarchyName: editedHierarchy.hierarchyName,
                        mappingIndex: mapping.mapping_index,
                        database: mapping.source_database,
                        schema: mapping.source_schema,
                        table: mapping.source_table,
                        column: mapping.source_column,
                        uid: mapping.source_uid,
                        precedence_group: mapping.precedence_group,
                      };
                    })
                    .filter(Boolean) as any[];
                  setSelectedMappingsForQuery(newSelected);
                }}
              />

              {/* SELECT Query Generator */}
              {editedHierarchy.mapping &&
                editedHierarchy.mapping.length > 0 && (
                  <MappingQueryGenerator
                    hierarchy={editedHierarchy}
                    selectedMappingIndexes={selectedMappingsForQuery.map(
                      (m) => m.mappingIndex
                    )}
                    databaseType={databaseType}
                  />
                )}
            </div>
          </TabsContent>

          {/* Formula Tab */}
          <TabsContent value="formula" className="mt-3">
            <FormulaBuilder
              selectedHierarchies={
                selectedHierarchiesForFormula &&
                selectedHierarchiesForFormula.length > 0
                  ? selectedHierarchiesForFormula
                  : [editedHierarchy]
              }
              currentHierarchy={hierarchy}
              preSelectedMappings={selectedMappingsForQuery}
              allAvailableHierarchies={allHierarchies}
              databaseType={databaseType}
              onClear={() => {
                // Clear all selections
                if (onToggleFormulaSelection && selectedForFormula) {
                  selectedForFormula.forEach((id) =>
                    onToggleFormulaSelection(id)
                  );
                }
              }}
              onSaveFormula={(formulaConfig) => {
                const validConfig = {
                  formula_type: formulaConfig.formula_type.toUpperCase() as
                    | "SQL"
                    | "EXPRESSION"
                    | "AGGREGATE",
                  formula_text: formulaConfig.formula_text,
                  // Note: formula_group is managed separately, not part of formulaConfig DTO
                };
                handleChange("formulaConfig", validConfig);
                if (mode === "view") {
                  // Auto-save if in view mode
                  // Also sanitize filterConfig to remove groupName if present
                  const cleanHierarchy = {
                    ...editedHierarchy,
                    formulaConfig: validConfig,
                  };
                  if (cleanHierarchy.filterConfig) {
                    const { groupName, ...cleanFilterConfig } =
                      cleanHierarchy.filterConfig as any;
                    cleanHierarchy.filterConfig = cleanFilterConfig;
                  }
                  onSave(cleanHierarchy);
                }
              }}
              onSaveFilter={(filterConfig) => {
                // Remove UI-only properties that are not part of the DTO
                const { groupName, ...validConfig } = filterConfig as any;
                handleChange("filterConfig", validConfig);
                if (mode === "view") {
                  // Auto-save if in view mode
                  // Also sanitize formulaConfig to remove formula_group if present
                  const cleanHierarchy = {
                    ...editedHierarchy,
                    filterConfig: validConfig,
                  };
                  if (cleanHierarchy.formulaConfig) {
                    const { formula_group, ...cleanFormulaConfig } =
                      cleanHierarchy.formulaConfig as any;
                    cleanHierarchy.formulaConfig = cleanFormulaConfig;
                  }
                  onSave(cleanHierarchy);
                }
              }}
            />
          </TabsContent>
          {/* Dependencies Tab */}
          <TabsContent value="dependencies" className="mt-3">
            <HierarchyDependencies
              projectId={hierarchy.projectId}
              hierarchyId={hierarchy.hierarchyId}
            />
          </TabsContent>
        </Tabs>
      </div>

      {/* Custom Flag Dialog */}
      <Dialog
        open={showCustomFlagDialog}
        onOpenChange={setShowCustomFlagDialog}
      >
        <DialogContent className="sm:max-w-[500px] bg-background text-foreground">
          <DialogHeader>
            <DialogTitle className="text-foreground">
              Add Custom Flag
            </DialogTitle>
            <DialogDescription className="text-muted-foreground">
              Enter a name for your custom flag (e.g., is_validated,
              requires_approval)
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4 py-4">
            <div>
              <Label htmlFor="flag-name">Flag Name</Label>
              <Input
                id="flag-name"
                value={newFlagName}
                onChange={(e) => {
                  setNewFlagName(e.target.value);
                  setCustomFlagError("");
                }}
                placeholder="e.g., is_validated"
                className="mt-2"
                onKeyDown={(e) => {
                  if (e.key === "Enter") {
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
                    const customFlags = editedHierarchy.flags.customFlags || {};
                    if (customFlags[trimmedName]) {
                      setCustomFlagError("This flag already exists");
                      return;
                    }
                    handleChange("flags", {
                      ...editedHierarchy.flags,
                      customFlags: { ...customFlags, [trimmedName]: false },
                    });
                    setShowCustomFlagDialog(false);
                    setNewFlagName("");
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
              }}
            >
              Cancel
            </Button>
            <Button
              onClick={() => {
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
                const customFlags = editedHierarchy.flags.customFlags || {};
                if (customFlags[trimmedName]) {
                  setCustomFlagError("This flag already exists");
                  return;
                }
                handleChange("flags", {
                  ...editedHierarchy.flags,
                  customFlags: { ...customFlags, [trimmedName]: false },
                });
                setShowCustomFlagDialog(false);
                setNewFlagName("");
              }}
            >
              Add Flag
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Delete Confirmation Dialog */}
      <Dialog open={showDeleteDialog} onOpenChange={setShowDeleteDialog}>
        <DialogContent className="sm:max-w-[425px] bg-background text-foreground">
          <DialogHeader>
            <DialogTitle className="text-xl font-semibold text-foreground">
              Delete Hierarchy
            </DialogTitle>
            <DialogDescription className="text-base pt-2 text-muted-foreground">
              Are you sure you want to delete{" "}
              <span className="font-semibold text-foreground">
                "{hierarchy.hierarchyName}"
              </span>
              ? This action cannot be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter className="gap-2 sm:gap-0">
            <Button
              variant="outline"
              onClick={() => setShowDeleteDialog(false)}
            >
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={() => {
                if (onDelete) {
                  onDelete(hierarchy.hierarchyId);
                }
                setShowDeleteDialog(false);
              }}
            >
              <Trash2 className="w-4 h-4 mr-2" />
              Delete
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
};
