import React, { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
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
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { useToast } from "@/hooks/use-toast";
import {
  smartHierarchyService,
  SmartHierarchyMaster,
  HierarchyFormulaGroup,
  HierarchyFormulaRule,
} from "@/services/api/hierarchy";
import {
  Plus,
  X,
  ChevronDown,
  Check,
  Loader2,
  Save,
  Trash2,
  Edit,
  Calculator,
} from "lucide-react";
import { cn } from "@/lib/utils";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

interface FormulaGroupTabProps {
  projectId: string;
  selectedHierarchyIds: string[];
  allHierarchies: SmartHierarchyMaster[];
  onSuccess?: () => void;
}

export const FormulaGroupTab: React.FC<FormulaGroupTabProps> = ({
  projectId,
  selectedHierarchyIds,
  allHierarchies,
  onSuccess,
}) => {
  const { toast } = useToast();
  const [mode, setMode] = useState<"create" | "edit">("create");
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);

  // Form state
  const [mainHierarchyId, setMainHierarchyId] = useState<string>("");
  const [mainHierarchyName, setMainHierarchyName] = useState<string>("");
  const [rules, setRules] = useState<HierarchyFormulaRule[]>([]);

  // New rule form
  const [showAddRule, setShowAddRule] = useState(false);
  const [newRuleHierarchyId, setNewRuleHierarchyId] = useState<string>("");
  const [newRuleOperation, setNewRuleOperation] =
    useState<HierarchyFormulaRule["operation"]>("SUM");
  const [newRulePrecedence, setNewRulePrecedence] = useState<number>(1);
  const [newRuleParamRef, setNewRuleParamRef] = useState<string>("");
  const [newRuleConstant, setNewRuleConstant] = useState<string>("");
  const [newRuleRefSource, setNewRuleRefSource] = useState<string>("");
  const [newRuleRefTable, setNewRuleRefTable] = useState<string>("");

  // UI state
  const [mainHierarchySearchOpen, setMainHierarchySearchOpen] = useState(false);
  const [ruleHierarchySearchOpen, setRuleHierarchySearchOpen] = useState(false);
  const [activeTab, setActiveTab] = useState<"create" | "existing">("create");
  const [existingFormulaGroups, setExistingFormulaGroups] = useState<
    Array<{
      hierarchyId: string;
      hierarchyName: string;
      formulaGroup: HierarchyFormulaGroup;
    }>
  >([]);
  const [expandedGroups, setExpandedGroups] = useState<Set<string>>(new Set());
  const [editingGroupId, setEditingGroupId] = useState<string | null>(null);

  // Helper function to calculate hierarchy level
  const getHierarchyLevel = (hierarchy: SmartHierarchyMaster): number => {
    if (!hierarchy.hierarchyLevel) return 1;

    const levels = hierarchy.hierarchyLevel;
    let level = 0;

    // Count non-empty levels
    for (let i = 1; i <= 15; i++) {
      const key = `level_${i}` as keyof typeof levels;
      if (levels[key] && levels[key] !== "") {
        level = i;
      }
    }

    return level || 1;
  };

  // Helper function to get hierarchy level path
  const getHierarchyPath = (hierarchy: SmartHierarchyMaster): string => {
    if (!hierarchy.hierarchyLevel) return hierarchy.hierarchyName;

    const levels = hierarchy.hierarchyLevel;
    const path: string[] = [];

    for (let i = 1; i <= 15; i++) {
      const key = `level_${i}` as keyof typeof levels;
      if (levels[key] && levels[key] !== "") {
        path.push(levels[key] as string);
      }
    }

    return path.length > 0 ? path.join(" > ") : hierarchy.hierarchyName;
  };

  // Get selected hierarchies from IDs
  const selectedHierarchies = allHierarchies.filter((h) =>
    selectedHierarchyIds.includes(h.hierarchyId)
  );

  // Initialize with first selected hierarchy as main and others as rules
  useEffect(() => {
    if (
      mode === "create" &&
      selectedHierarchyIds.length > 0 &&
      !mainHierarchyId &&
      rules.length === 0
    ) {
      const firstHierarchy = selectedHierarchies[0];
      setMainHierarchyId(firstHierarchy.hierarchyId);
      setMainHierarchyName(firstHierarchy.hierarchyName);

      // Add remaining selected hierarchies as rules with default settings
      if (selectedHierarchies.length > 1) {
        const defaultRules: HierarchyFormulaRule[] = selectedHierarchies
          .slice(1)
          .map((h, index) => ({
            hierarchyId: h.hierarchyId,
            hierarchyName: h.hierarchyName,
            operation: "ADD" as const,
            precedence: 2,
          }));
        setRules(defaultRules);
      }
    }
  }, [mode, selectedHierarchyIds, mainHierarchyId, rules.length]);

  // Sync rules with selection changes - remove unchecked hierarchies
  useEffect(() => {
    if (mode === "create") {
      // Remove rules for hierarchies that are no longer selected
      if (rules.length > 0) {
        const stillSelected = rules.filter((rule) =>
          selectedHierarchyIds.includes(rule.hierarchyId)
        );
        if (stillSelected.length !== rules.length) {
          setRules(stillSelected);
        }
      }

      // Clear main hierarchy if it's no longer selected
      if (mainHierarchyId && !selectedHierarchyIds.includes(mainHierarchyId)) {
        setMainHierarchyId("");
        setMainHierarchyName("");
      }
    }
  }, [selectedHierarchyIds, mode]);

  // Load existing formula groups
  useEffect(() => {
    loadExistingFormulaGroups();
  }, [projectId]);

  const loadExistingFormulaGroups = async () => {
    if (!projectId) return;

    try {
      setLoading(true);
      const groups =
        await smartHierarchyService.listHierarchiesWithFormulaGroups(projectId);
      setExistingFormulaGroups(groups);
    } catch (error) {
      console.error("Failed to load existing formula groups:", error);
      toast({
        title: "Load Failed",
        description: "Failed to load existing formula groups",
        variant: "destructive",
      });
    } finally {
      setLoading(false);
    }
  };

  const handleCreateFormulaGroup = async () => {
    if (!mainHierarchyId) {
      toast({
        title: "Validation Error",
        description: "Please select a main hierarchy",
        variant: "destructive",
      });
      return;
    }

    if (!mainHierarchyName.trim()) {
      toast({
        title: "Validation Error",
        description: "Please enter a formula group name",
        variant: "destructive",
      });
      return;
    }

    if (rules.length === 0) {
      toast({
        title: "Validation Error",
        description: "Please add at least one rule",
        variant: "destructive",
      });
      return;
    }

    try {
      setSaving(true);
      await smartHierarchyService.createOrUpdateFormulaGroup(
        projectId,
        mainHierarchyId,
        {
          mainHierarchyName,
          rules,
        }
      );

      toast({
        title: "Success",
        description: "Formula group created successfully",
      });

      onSuccess?.();
      handleReset();
      setActiveTab("existing");
    } catch (error: any) {
      console.error("Failed to create formula group:", error);
      toast({
        title: "Creation Failed",
        description:
          error.response?.data?.message || "Failed to create formula group",
        variant: "destructive",
      });
    } finally {
      setSaving(false);
    }
  };

  const handleLoadFormulaGroup = (group: {
    hierarchyId: string;
    hierarchyName: string;
    formulaGroup: HierarchyFormulaGroup;
  }) => {
    setMode("edit");
    setEditingGroupId(group.hierarchyId);
    setMainHierarchyId(group.hierarchyId);
    setMainHierarchyName(group.formulaGroup.mainHierarchyName);
    setRules([...group.formulaGroup.rules]);
    setActiveTab("create");
  };

  const handleUpdateFormulaGroup = async () => {
    if (!editingGroupId) return;

    try {
      setSaving(true);
      await smartHierarchyService.createOrUpdateFormulaGroup(
        projectId,
        editingGroupId,
        {
          mainHierarchyName,
          rules,
        }
      );

      toast({
        title: "Success",
        description: "Formula group updated successfully",
      });

      await loadExistingFormulaGroups();
      handleReset();
      setMode("create");
      onSuccess?.();
    } catch (error: any) {
      console.error("Failed to update formula group:", error);
      toast({
        title: "Update Failed",
        description:
          error.response?.data?.message || "Failed to update formula group",
        variant: "destructive",
      });
    } finally {
      setSaving(false);
    }
  };

  const handleDeleteFormulaGroup = async (hierarchyId: string) => {
    try {
      await smartHierarchyService.deleteFormulaGroup(projectId, hierarchyId);
      toast({
        title: "Success",
        description: "Formula group deleted successfully",
      });
      await loadExistingFormulaGroups();
      onSuccess?.();
    } catch (error: any) {
      console.error("Failed to delete formula group:", error);
      toast({
        title: "Deletion Failed",
        description:
          error.response?.data?.message || "Failed to delete formula group",
        variant: "destructive",
      });
    }
  };

  const handleAddRule = () => {
    if (!newRuleHierarchyId) {
      toast({
        title: "Validation Error",
        description: "Please select a hierarchy for the rule",
        variant: "destructive",
      });
      return;
    }

    const hierarchy = allHierarchies.find(
      (h) => h.hierarchyId === newRuleHierarchyId
    );
    if (!hierarchy) return;

    // Check if already added
    if (rules.some((r) => r.hierarchyId === newRuleHierarchyId)) {
      toast({
        title: "Already Added",
        description: "This hierarchy is already in the rules list",
        variant: "destructive",
      });
      return;
    }

    const newRule: HierarchyFormulaRule = {
      hierarchyId: newRuleHierarchyId,
      hierarchyName: hierarchy.hierarchyName,
      operation: newRuleOperation,
      precedence: newRulePrecedence,
      parameterReference: newRuleParamRef || undefined,
      constantNumber: newRuleConstant ? parseFloat(newRuleConstant) : undefined,
      formulaRefSource: newRuleRefSource || undefined,
      formulaRefTable: newRuleRefTable || undefined,
    };

    setRules([...rules, newRule]);
    resetNewRuleForm();
    setShowAddRule(false);
  };

  const handleRemoveRule = (hierarchyId: string) => {
    setRules(rules.filter((r) => r.hierarchyId !== hierarchyId));
  };

  const resetNewRuleForm = () => {
    setNewRuleHierarchyId("");
    setNewRuleOperation("SUM");
    setNewRulePrecedence(1);
    setNewRuleParamRef("");
    setNewRuleConstant("");
    setNewRuleRefSource("");
    setNewRuleRefTable("");
  };

  const handleReset = () => {
    setMainHierarchyId("");
    setMainHierarchyName("");
    setRules([]);
    setEditingGroupId(null);
    resetNewRuleForm();
    setShowAddRule(false);
  };

  const toggleGroupExpansion = (hierarchyId: string) => {
    setExpandedGroups((prev) => {
      const newSet = new Set(prev);
      if (newSet.has(hierarchyId)) {
        newSet.delete(hierarchyId);
      } else {
        newSet.add(hierarchyId);
      }
      return newSet;
    });
  };

  const mainHierarchy = allHierarchies.find(
    (h) => h.hierarchyId === mainHierarchyId
  );

  return (
    <div className="flex flex-col h-full">
      <div className="mb-4">
        <h3 className="text-lg font-semibold flex items-center gap-2">
          <Calculator className="w-5 h-5" />
          Manage Formula Groups
        </h3>
      </div>

      <Tabs
        value={activeTab}
        onValueChange={(v) => setActiveTab(v as "create" | "existing")}
      >
        <TabsList className="grid w-full grid-cols-2 mb-4">
          <TabsTrigger value="create">
            {mode === "edit" ? "Edit Formula Group" : "Create Formula Group"}
          </TabsTrigger>
          <TabsTrigger value="existing">Existing Formula Groups</TabsTrigger>
        </TabsList>

        {/* Create/Edit Tab */}
        <TabsContent value="create" className="space-y-4 flex-1">
          <div className="space-y-4">
            {/* Main Hierarchy Selection */}
            <div className=" flex flex-row gap-4">
              <div className="space-y-2 w-1/2">
                <Label>Main Hierarchy</Label>
                <Popover
                  open={mainHierarchySearchOpen}
                  onOpenChange={setMainHierarchySearchOpen}
                >
                  <PopoverTrigger asChild>
                    <Button
                      variant="outline"
                      role="combobox"
                      disabled={mode === "edit"}
                      className="w-full justify-between"
                    >
                      {mainHierarchy?.hierarchyName ||
                        "Select main hierarchy..."}
                      <ChevronDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                    </Button>
                  </PopoverTrigger>
                  <PopoverContent className="w-[400px] p-0">
                    <Command>
                      <CommandInput placeholder="Search hierarchies..." />
                      <CommandEmpty>No hierarchy found.</CommandEmpty>
                      <CommandList>
                        <CommandGroup>
                          {selectedHierarchies.map((hierarchy) => (
                            <CommandItem
                              key={hierarchy.hierarchyId}
                              value={hierarchy.hierarchyName}
                              onSelect={() => {
                                setMainHierarchyId(hierarchy.hierarchyId);
                                setMainHierarchyName(hierarchy.hierarchyName);
                                setMainHierarchySearchOpen(false);
                              }}
                            >
                              <Check
                                className={cn(
                                  "mr-2 h-4 w-4",
                                  mainHierarchyId === hierarchy.hierarchyId
                                    ? "opacity-100"
                                    : "opacity-0"
                                )}
                              />
                              <div className="flex flex-col flex-1">
                                <div className="flex items-center gap-2">
                                  <span>{hierarchy.hierarchyName}</span>
                                  <Badge
                                    variant="outline"
                                    className="text-xs px-1.5 py-0.5"
                                  >
                                    L{getHierarchyLevel(hierarchy)}
                                  </Badge>
                                </div>
                                <span className="text-xs text-muted-foreground">
                                  {getHierarchyPath(hierarchy)}
                                </span>
                              </div>
                            </CommandItem>
                          ))}
                        </CommandGroup>
                      </CommandList>
                    </Command>
                  </PopoverContent>
                </Popover>
              </div>

              <div className="space-y-2  w-1/2">
                <Label>Formula Group Name</Label>
                <Input
                  value={mainHierarchyName}
                  onChange={(e) => setMainHierarchyName(e.target.value)}
                  placeholder="Enter formula group name"
                />
              </div>
            </div>

            <Separator />

            {/* Rules Section */}
            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <Label>Rules ({rules.length})</Label>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => setShowAddRule(!showAddRule)}
                >
                  <Plus className="w-4 h-4 mr-1" />
                  Add Rule
                </Button>
              </div>

              {/* Add Rule Form */}
              {showAddRule && (
                <Card className="p-4  border-dashed">
                  <div className="flex grow justify-between">
                    <div className="space-y-2 w-60">
                      <Label>Hierarchy</Label>

                      <Popover
                        open={ruleHierarchySearchOpen}
                        onOpenChange={setRuleHierarchySearchOpen}
                      >
                        <PopoverTrigger asChild className="w-60">
                          <Button variant="outline" className="justify-between">
                            {allHierarchies.find(
                              (h) => h.hierarchyId === newRuleHierarchyId
                            )?.hierarchyName || "Select hierarchy..."}
                            <ChevronDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                          </Button>
                        </PopoverTrigger>
                        <PopoverContent className="w-[350px] p-0">
                          <Command>
                            <CommandInput placeholder="Search hierarchies..." />
                            <CommandEmpty>No hierarchy found.</CommandEmpty>
                            <CommandList>
                              <CommandGroup>
                                {allHierarchies
                                  .filter(
                                    (h) => h.hierarchyId !== mainHierarchyId
                                  )
                                  .map((hierarchy) => (
                                    <CommandItem
                                      key={hierarchy.hierarchyId}
                                      value={hierarchy.hierarchyName}
                                      onSelect={() => {
                                        setNewRuleHierarchyId(
                                          hierarchy.hierarchyId
                                        );
                                        setRuleHierarchySearchOpen(false);
                                      }}
                                    >
                                      <div className="flex flex-col flex-1">
                                        <div className="flex items-center gap-2">
                                          <span>{hierarchy.hierarchyName}</span>
                                          <Badge
                                            variant="outline"
                                            className="text-xs px-1.5 py-0.5"
                                          >
                                            L{getHierarchyLevel(hierarchy)}
                                          </Badge>
                                        </div>
                                        <span className="text-xs text-muted-foreground">
                                          {getHierarchyPath(hierarchy)}
                                        </span>
                                      </div>
                                    </CommandItem>
                                  ))}
                              </CommandGroup>
                            </CommandList>
                          </Command>
                        </PopoverContent>
                      </Popover>
                    </div>
                    <div className="space-y-2  w-20">
                      <Label>Operation</Label>
                      <Select
                        value={newRuleOperation}
                        onValueChange={(v) =>
                          setNewRuleOperation(
                            v as HierarchyFormulaRule["operation"]
                          )
                        }
                      >
                        <SelectTrigger>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="ADD">ADD (+)</SelectItem>
                          <SelectItem value="SUBTRACT">SUBTRACT (-)</SelectItem>
                          <SelectItem value="MULTIPLY">MULTIPLY (×)</SelectItem>
                          <SelectItem value="DIVIDE">DIVIDE (÷)</SelectItem>
                          <SelectItem value="SUM">SUM</SelectItem>
                          <SelectItem value="AVERAGE">AVERAGE</SelectItem>
                          <SelectItem value="COUNT">COUNT</SelectItem>
                          <SelectItem value="MIN">MIN</SelectItem>
                          <SelectItem value="MAX">MAX</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>

                    <div className="space-y-2  w-25">
                      <Label>Precedence</Label>
                      <Input
                        type="number"
                        value={newRulePrecedence}
                        onChange={(e) =>
                          setNewRulePrecedence(parseInt(e.target.value) || 1)
                        }
                        min={1}
                      />
                    </div>

                    <div className="space-y-2 w-40">
                      <Label>Parameter Reference</Label>
                      <Input
                        value={newRuleParamRef}
                        onChange={(e) => setNewRuleParamRef(e.target.value)}
                        placeholder="e.g., column name"
                      />
                    </div>

                    <div className="space-y-2">
                      <Label>Constant Number </Label>
                      <Input
                        type="number"
                        value={newRuleConstant}
                        onChange={(e) => setNewRuleConstant(e.target.value)}
                        placeholder="e.g., 100"
                        step="any"
                      />
                    </div>
                  </div>

                  {/* Second row for Ref Source and Ref Table */}
                  <div className="grid grid-cols-2 gap-4">
                    <div className="space-y-2">
                      <Label>Formula Ref Source</Label>
                      <Input
                        value={newRuleRefSource}
                        onChange={(e) => setNewRuleRefSource(e.target.value)}
                        placeholder="e.g., source system"
                      />
                    </div>

                    <div className="space-y-2">
                      <Label>Formula Ref Table</Label>
                      <Input
                        value={newRuleRefTable}
                        onChange={(e) => setNewRuleRefTable(e.target.value)}
                        placeholder="e.g., table name"
                      />
                    </div>
                  </div>
                  <div className="flex justify-end gap-2">
                    <Button
                      size="sm"
                      variant="ghost"
                      onClick={() => {
                        resetNewRuleForm();
                        setShowAddRule(false);
                      }}
                    >
                      Cancel
                    </Button>
                    <Button size="sm" onClick={handleAddRule}>
                      Add Rule
                    </Button>
                  </div>
                </Card>
              )}

              {/* Rules List */}
              {rules.length > 0 && (
                <div className="space-y-2">
                  {rules.map((rule, index) => {
                    const ruleHierarchy = allHierarchies.find(
                      (h) => h.hierarchyId === rule.hierarchyId
                    );
                    return (
                      <Card key={rule.hierarchyId} className="p-3">
                        <div className="flex flex-col gap-2">
                          {/* First Row: Index, Name, Level, Operation */}
                          <div className="flex items-center justify-between gap-4">
                            {/* Rule Index */}
                            <div className="flex items-center justify-center min-w-10">
                              <Badge variant="secondary" className="text-xs">
                                #{index + 1}
                              </Badge>
                            </div>

                            {/* Hierarchy Name and Level */}
                            <div className="flex-1 min-w-0">
                              <div className="flex items-center gap-2">
                                <p className="font-medium text-sm truncate">
                                  {rule.hierarchyName}
                                </p>
                                {ruleHierarchy && (
                                  <Badge
                                    variant="outline"
                                    className="text-xs px-1.5 py-0.5"
                                  >
                                    Level {getHierarchyLevel(ruleHierarchy)}
                                  </Badge>
                                )}
                              </div>
                              {ruleHierarchy && (
                                <p className="text-xs text-muted-foreground mt-1">
                                  {getHierarchyPath(ruleHierarchy)}
                                </p>
                              )}
                            </div>

                            {/* Operation */}
                            <div className="flex items-center gap-2">
                              <Label className="text-xs text-muted-foreground">
                                Op:
                              </Label>
                              <Badge variant="secondary" className="font-mono">
                                {rule.operation}
                              </Badge>
                            </div>

                            {/* Precedence */}
                            <div className="flex items-center gap-2">
                              <Label className="text-xs text-muted-foreground">
                                Prec:
                              </Label>
                              <Badge variant="secondary">
                                {rule.precedence}
                              </Badge>
                            </div>

                            {/* Parameter Reference */}
                            {rule.parameterReference && (
                              <div className="flex items-center gap-2 max-w-[150px]">
                                <Label className="text-xs text-muted-foreground">
                                  Param:
                                </Label>
                                <span className="text-xs font-medium truncate">
                                  {rule.parameterReference}
                                </span>
                              </div>
                            )}

                            {/* Remove Button */}
                            <Button
                              size="icon"
                              variant="ghost"
                              className="h-8 w-8 shrink-0"
                              onClick={() => handleRemoveRule(rule.hierarchyId)}
                            >
                              <X className="w-4 h-4" />
                            </Button>
                          </div>

                          {/* Second Row: Additional Info */}
                          <div className="flex items-center gap-4 text-xs pl-12">
                            {/* Constant Number */}
                            {rule.constantNumber !== undefined && (
                              <div className="flex items-center gap-2">
                                <Label className="text-xs text-muted-foreground">
                                  Const:
                                </Label>
                                <span className="text-xs font-mono font-medium">
                                  {rule.constantNumber}
                                </span>
                              </div>
                            )}

                            {/* Formula Ref Source */}
                            {rule.formulaRefSource && (
                              <div className="flex items-center gap-2">
                                <Label className="text-xs text-muted-foreground">
                                  Source:
                                </Label>
                                <span className="text-xs font-medium">
                                  {rule.formulaRefSource}
                                </span>
                              </div>
                            )}

                            {/* Formula Ref Table */}
                            {rule.formulaRefTable && (
                              <div className="flex items-center gap-2">
                                <Label className="text-xs text-muted-foreground">
                                  Table:
                                </Label>
                                <span className="text-xs font-medium">
                                  {rule.formulaRefTable}
                                </span>
                              </div>
                            )}
                          </div>
                        </div>
                      </Card>
                    );
                  })}
                </div>
              )}

              {rules.length === 0 && !showAddRule && (
                <div className="text-center py-8 text-muted-foreground">
                  No rules added yet. Click "Add Rule" to get started.
                </div>
              )}
            </div>
          </div>
        </TabsContent>

        {/* Existing Formula Groups Tab */}
        <TabsContent value="existing">
          {loading ? (
            <div className="flex items-center justify-center ">
              <Loader2 className="w-6 h-6 animate-spin" />
            </div>
          ) : existingFormulaGroups.length === 0 ? (
            <div className="text-center  text-muted-foreground">
              No formula groups found in this project
            </div>
          ) : (
            <div className="space-y-3">
              {existingFormulaGroups.map((group) => {
                const mainHierarchy = allHierarchies.find(
                  (h) => h.hierarchyId === group.hierarchyId
                );
                return (
                  <Card key={group.hierarchyId} className="p-0 pt-1 ">
                    <Collapsible
                      open={expandedGroups.has(group.hierarchyId)}
                      onOpenChange={() =>
                        toggleGroupExpansion(group.hierarchyId)
                      }
                    >
                      <CardHeader className="">
                        <div className="flex items-center justify-between">
                          <CollapsibleTrigger className="flex items-center gap-2 hover:underline flex-1">
                            <ChevronDown
                              className={cn(
                                "w-4 h-4 transition-transform shrink-0",
                                expandedGroups.has(group.hierarchyId)
                                  ? ""
                                  : "-rotate-90"
                              )}
                            />
                            <div className="flex flex-col items-start flex-1">
                              <div className="flex items-center gap-2">
                                <CardTitle className="text-base">
                                  {group.formulaGroup.mainHierarchyName}
                                </CardTitle>
                                {mainHierarchy && (
                                  <Badge
                                    variant="outline"
                                    className="text-xs px-1.5 py-0.5"
                                  >
                                    Level {getHierarchyLevel(mainHierarchy)}
                                  </Badge>
                                )}
                                <Badge variant="secondary" className="text-xs">
                                  {group.formulaGroup.rules.length} rules
                                </Badge>
                              </div>
                              {mainHierarchy && (
                                <p className="text-xs text-muted-foreground mt-1 font-normal">
                                  {getHierarchyPath(mainHierarchy)}
                                </p>
                              )}
                            </div>
                          </CollapsibleTrigger>
                          <div className="flex items-center gap-2">
                            <Button
                              size="sm"
                              variant="ghost"
                              onClick={() => handleLoadFormulaGroup(group)}
                            >
                              <Edit className="w-4 h-4" />
                            </Button>
                            <Button
                              size="sm"
                              variant="ghost"
                              onClick={() =>
                                handleDeleteFormulaGroup(group.hierarchyId)
                              }
                            >
                              <Trash2 className="w-4 h-4" />
                            </Button>
                          </div>
                        </div>
                      </CardHeader>
                      <CollapsibleContent>
                        <CardContent className="pt-0 pb-3">
                          <div className="space-y-3">
                            <Separator />
                            <div className="space-y-2">
                              <div className="text-sm font-medium mb-2">
                                Rules ({group.formulaGroup.rules.length})
                              </div>
                              {group.formulaGroup.rules.map((rule, idx) => {
                                const ruleHierarchy = allHierarchies.find(
                                  (h) => h.hierarchyId === rule.hierarchyId
                                );
                                return (
                                  <Card key={idx} className="p-3 bg-muted/50">
                                    <div className="flex flex-col gap-2">
                                      {/* First Row: Index, Name, Level, Operation */}
                                      <div className="flex items-center gap-4">
                                        {/* Rule Index */}
                                        <div className="flex items-center justify-center min-w-10">
                                          <Badge
                                            variant="secondary"
                                            className="text-xs font-mono"
                                          >
                                            #{idx + 1}
                                          </Badge>
                                        </div>

                                        {/* Hierarchy Name and Level */}
                                        <div className="flex-1 min-w-0">
                                          <div className="flex items-center gap-2">
                                            <p className="font-medium text-sm truncate">
                                              {rule.hierarchyName}
                                            </p>
                                            {ruleHierarchy && (
                                              <Badge
                                                variant="outline"
                                                className="text-xs px-1.5 py-0.5"
                                              >
                                                Level{" "}
                                                {getHierarchyLevel(
                                                  ruleHierarchy
                                                )}
                                              </Badge>
                                            )}
                                          </div>
                                          {ruleHierarchy && (
                                            <p className="text-xs text-muted-foreground mt-1">
                                              {getHierarchyPath(ruleHierarchy)}
                                            </p>
                                          )}
                                        </div>

                                        {/* Operation */}
                                        <div className="flex items-center gap-2">
                                          <Label className="text-xs text-muted-foreground">
                                            Op:
                                          </Label>
                                          <Badge
                                            variant="default"
                                            className="font-mono"
                                          >
                                            {rule.operation}
                                          </Badge>
                                        </div>

                                        {/* Precedence */}
                                        <div className="flex items-center gap-2">
                                          <Label className="text-xs text-muted-foreground">
                                            Prec:
                                          </Label>
                                          <Badge
                                            variant="secondary"
                                            className="font-mono"
                                          >
                                            {rule.precedence}
                                          </Badge>
                                        </div>

                                        {/* Parameter Reference */}
                                        {rule.parameterReference && (
                                          <div className="flex items-center gap-2">
                                            <Label className="text-xs text-muted-foreground">
                                              Param:
                                            </Label>
                                            <span className="text-xs font-medium truncate">
                                              {rule.parameterReference}
                                            </span>
                                          </div>
                                        )}
                                      </div>

                                      {/* Second Row: Additional Info */}
                                      <div className="flex items-center gap-4 text-xs pl-12">
                                        {/* Constant Number */}
                                        {rule.constantNumber !== undefined && (
                                          <div className="flex items-center gap-2">
                                            <Label className="text-xs text-muted-foreground">
                                              Const:
                                            </Label>
                                            <span className="text-xs font-mono font-medium">
                                              {rule.constantNumber}
                                            </span>
                                          </div>
                                        )}

                                        {/* Formula Ref Source */}
                                        {rule.formulaRefSource && (
                                          <div className="flex items-center gap-2">
                                            <Label className="text-xs text-muted-foreground">
                                              Source:
                                            </Label>
                                            <span className="text-xs font-medium">
                                              {rule.formulaRefSource}
                                            </span>
                                          </div>
                                        )}

                                        {/* Formula Ref Table */}
                                        {rule.formulaRefTable && (
                                          <div className="flex items-center gap-2">
                                            <Label className="text-xs text-muted-foreground">
                                              Table:
                                            </Label>
                                            <span className="text-xs font-medium">
                                              {rule.formulaRefTable}
                                            </span>
                                          </div>
                                        )}
                                      </div>
                                    </div>
                                  </Card>
                                );
                              })}
                            </div>
                          </div>
                        </CardContent>
                      </CollapsibleContent>
                    </Collapsible>
                  </Card>
                );
              })}
            </div>
          )}
        </TabsContent>
      </Tabs>

      <div className="mt-4 flex justify-end gap-2">
        {activeTab === "create" && (
          <>
            {mode === "edit" ? (
              <>
                <Button
                  variant="outline"
                  onClick={() => {
                    handleReset();
                    setMode("create");
                  }}
                >
                  Reset
                </Button>
                <Button onClick={handleUpdateFormulaGroup} disabled={saving}>
                  {saving ? (
                    <>
                      <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                      Saving...
                    </>
                  ) : (
                    <>
                      <Save className="w-4 h-4 mr-2" />
                      Save Changes
                    </>
                  )}
                </Button>
              </>
            ) : (
              <Button onClick={handleCreateFormulaGroup} disabled={saving}>
                {saving ? (
                  <>
                    <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                    Creating...
                  </>
                ) : (
                  <>
                    <Plus className="w-4 h-4 mr-2" />
                    Create Formula Group
                  </>
                )}
              </Button>
            )}
          </>
        )}
      </div>
    </div>
  );
};
