import React, { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
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
  TotalFormula,
  TotalFormulaChild,
} from "@/services/api/hierarchy";
import {
  Calculator,
  Plus,
  X,
  ChevronDown,
  Check,
  Search,
  Loader2,
  Save,
  Trash2,
  Edit2,
  Edit,
} from "lucide-react";
import { cn } from "@/lib/utils";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

interface TotalFormulaTabProps {
  projectId: string;
  selectedHierarchyIds: string[];
  allHierarchies: SmartHierarchyMaster[];
  onSuccess?: () => void;
}

export const TotalFormulaTab: React.FC<TotalFormulaTabProps> = ({
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
  const [aggregation, setAggregation] = useState<
    "SUM" | "AVERAGE" | "COUNT" | "MIN" | "MAX"
  >("SUM");
  const [children, setChildren] = useState<TotalFormulaChild[]>([]);

  // UI state
  const [mainHierarchySearchOpen, setMainHierarchySearchOpen] = useState(false);
  const [childSearchOpen, setChildSearchOpen] = useState(false);
  const [activeTab, setActiveTab] = useState<"create" | "existing">("create");
  const [existingFormulas, setExistingFormulas] = useState<
    Array<{
      hierarchyId: string;
      hierarchyName: string;
      totalFormula: TotalFormula;
    }>
  >([]);
  const [expandedFormulas, setExpandedFormulas] = useState<Set<string>>(
    new Set()
  );
  const [editingFormulaId, setEditingFormulaId] = useState<string | null>(null);

  // Get selected hierarchies from IDs
  const selectedHierarchies = allHierarchies.filter((h) =>
    selectedHierarchyIds.includes(h.hierarchyId)
  );

  // Initialize children with selected hierarchies
  useEffect(() => {
    if (
      mode === "create" &&
      selectedHierarchyIds.length > 0 &&
      !mainHierarchyId
    ) {
      setChildren(
        selectedHierarchies.map((h) => ({
          hierarchyId: h.hierarchyId,
          hierarchyName: h.hierarchyName,
        }))
      );
    }
  }, [mode, selectedHierarchyIds, mainHierarchyId]);

  // Sync children with selection changes - remove unchecked hierarchies
  useEffect(() => {
    if (mode === "create" && children.length > 0) {
      const stillSelected = children.filter((child) =>
        selectedHierarchyIds.includes(child.hierarchyId)
      );
      if (stillSelected.length !== children.length) {
        setChildren(stillSelected);
      }
    }
  }, [selectedHierarchyIds, mode]);

  // Load existing formulas
  useEffect(() => {
    loadExistingFormulas();
  }, [projectId]);

  const loadExistingFormulas = async () => {
    if (!projectId) return;

    try {
      setLoading(true);
      const formulas =
        await smartHierarchyService.listHierarchiesWithTotalFormulas(projectId);
      setExistingFormulas(formulas);
    } catch (error) {
      console.error("Failed to load existing formulas:", error);
      toast({
        title: "Load Failed",
        description: "Failed to load existing total formulas",
        variant: "destructive",
      });
    } finally {
      setLoading(false);
    }
  };

  const handleCreateFormula = async () => {
    if (!mainHierarchyId) {
      toast({
        title: "Validation Error",
        description: "Please select a main hierarchy",
        variant: "destructive",
      });
      return;
    }

    if (children.length === 0) {
      toast({
        title: "Validation Error",
        description: "Please add at least one child hierarchy",
        variant: "destructive",
      });
      return;
    }

    const mainHierarchy = allHierarchies.find(
      (h) => h.hierarchyId === mainHierarchyId
    );
    if (!mainHierarchy) {
      toast({
        title: "Error",
        description: "Selected main hierarchy not found",
        variant: "destructive",
      });
      return;
    }

    try {
      setSaving(true);
      await smartHierarchyService.createOrUpdateTotalFormula(
        projectId,
        mainHierarchyId,
        {
          mainHierarchyName: mainHierarchy.hierarchyName,
          aggregation,
          children,
        }
      );

      toast({
        title: "Success",
        description: "Total formula created successfully",
      });

      onSuccess?.();
      handleReset();
      setActiveTab("existing");
    } catch (error: any) {
      console.error("Failed to create total formula:", error);
      toast({
        title: "Creation Failed",
        description:
          error.response?.data?.message || "Failed to create total formula",
        variant: "destructive",
      });
    } finally {
      setSaving(false);
    }
  };

  const handleLoadFormula = (formula: {
    hierarchyId: string;
    hierarchyName: string;
    totalFormula: TotalFormula;
  }) => {
    setMode("edit");
    setEditingFormulaId(formula.hierarchyId);
    setMainHierarchyId(formula.hierarchyId);
    setAggregation(formula.totalFormula.aggregation);
    setChildren([...formula.totalFormula.children]);
    setActiveTab("create");
  };

  const handleUpdateFormula = async () => {
    if (!editingFormulaId) return;

    const mainHierarchy = allHierarchies.find(
      (h) => h.hierarchyId === editingFormulaId
    );
    if (!mainHierarchy) {
      toast({
        title: "Error",
        description: "Main hierarchy not found",
        variant: "destructive",
      });
      return;
    }

    try {
      setSaving(true);
      await smartHierarchyService.createOrUpdateTotalFormula(
        projectId,
        editingFormulaId,
        {
          mainHierarchyName: mainHierarchy.hierarchyName,
          aggregation,
          children,
        }
      );

      toast({
        title: "Success",
        description: "Total formula updated successfully",
      });

      await loadExistingFormulas();
      handleReset();
      setMode("create");
      onSuccess?.();
    } catch (error: any) {
      console.error("Failed to update total formula:", error);
      toast({
        title: "Update Failed",
        description:
          error.response?.data?.message || "Failed to update total formula",
        variant: "destructive",
      });
    } finally {
      setSaving(false);
    }
  };

  const handleDeleteFormula = async (hierarchyId: string) => {
    try {
      await smartHierarchyService.deleteTotalFormula(projectId, hierarchyId);
      toast({
        title: "Success",
        description: "Total formula deleted successfully",
      });
      await loadExistingFormulas();
      onSuccess?.();
    } catch (error: any) {
      console.error("Failed to delete total formula:", error);
      toast({
        title: "Deletion Failed",
        description:
          error.response?.data?.message || "Failed to delete total formula",
        variant: "destructive",
      });
    }
  };

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

  const handleAddChild = (hierarchy: SmartHierarchyMaster) => {
    // Check if already added
    if (children.some((c) => c.hierarchyId === hierarchy.hierarchyId)) {
      toast({
        title: "Already Added",
        description: "This hierarchy is already in the children list",
        variant: "destructive",
      });
      return;
    }

    setChildren([
      ...children,
      {
        hierarchyId: hierarchy.hierarchyId,
        hierarchyName: hierarchy.hierarchyName,
        level: getHierarchyLevel(hierarchy),
      },
    ]);
    setChildSearchOpen(false);
  };

  const handleRemoveChild = (hierarchyId: string) => {
    setChildren(children.filter((c) => c.hierarchyId !== hierarchyId));
  };

  const handleReset = () => {
    setMainHierarchyId("");
    setAggregation("SUM");
    setChildren([]);
    setEditingFormulaId(null);
  };

  const toggleFormulaExpansion = (hierarchyId: string) => {
    setExpandedFormulas((prev) => {
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
          Manage Total Formulas
        </h3>
      </div>

      <Tabs
        value={activeTab}
        onValueChange={(v: any) => setActiveTab(v)}
        className="flex-1 flex flex-col overflow-hidden"
      >
        <TabsList className="grid w-full grid-cols-2">
          <TabsTrigger value="create">
            {mode === "edit" ? "Edit Formula" : "Create Formula"}
          </TabsTrigger>
          <TabsTrigger value="existing">Existing Formulas</TabsTrigger>
        </TabsList>

        {/* Create/Edit Tab */}
        <TabsContent
          value="create"
          className="flex-1 overflow-y-auto space-y-6 "
        >
          {/* Selected Hierarchies Info */}
          {mode === "create" && selectedHierarchies.length > 0 && (
            <Card className="gap-2">
              <CardHeader className="pb-0">
                <CardTitle className="text-sm font-medium">
                  Selected Hierarchies ({selectedHierarchies.length})
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="flex flex-wrap gap-2">
                  {selectedHierarchies.map((h) => (
                    <Badge key={h.hierarchyId} variant="secondary">
                      {h.hierarchyName}
                    </Badge>
                  ))}
                </div>
              </CardContent>
            </Card>
          )}

          {/* Main Hierarchy Selection */}
          <div className="flex gap-2">
            <div className="space-y-2 w-12/8">
              <Label htmlFor="main-hierarchy">Main Hierarchy *</Label>
              <Popover
                open={mainHierarchySearchOpen}
                onOpenChange={setMainHierarchySearchOpen}
              >
                <PopoverTrigger asChild>
                  <Button
                    variant="outline"
                    role="combobox"
                    className="w-full justify-between"
                    disabled={mode === "edit"}
                  >
                    {mainHierarchy ? (
                      <span className="font-medium">
                        {mainHierarchy.hierarchyName}
                      </span>
                    ) : (
                      <span className="text-muted-foreground">
                        Select main hierarchy...
                      </span>
                    )}
                    <ChevronDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="w-[500px] p-0">
                  <Command>
                    <CommandInput placeholder="Search hierarchies..." />
                    <CommandList>
                      <CommandEmpty>No hierarchies found.</CommandEmpty>
                      <CommandGroup>
                        {allHierarchies.map((h) => (
                          <CommandItem
                            key={h.hierarchyId}
                            value={`${h.hierarchyName} ${h.hierarchyId}`}
                            onSelect={() => {
                              setMainHierarchyId(h.hierarchyId);
                              setMainHierarchySearchOpen(false);
                            }}
                          >
                            <Check
                              className={cn(
                                "mr-2 h-4 w-4",
                                mainHierarchyId === h.hierarchyId
                                  ? "opacity-100"
                                  : "opacity-0"
                              )}
                            />
                            <div className="flex flex-col flex-1">
                              <div className="flex items-center gap-2">
                                <span className="font-medium">
                                  {h.hierarchyName}
                                </span>
                                <Badge
                                  variant="outline"
                                  className="text-xs px-1.5 py-0.5"
                                >
                                  L{getHierarchyLevel(h)}
                                </Badge>
                              </div>
                              <span className="text-xs text-muted-foreground">
                                {getHierarchyPath(h)}
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
            <div className="space-y-2 w-12/16">
              <Label htmlFor="aggregation">Aggregation Function *</Label>
              <Select
                value={aggregation}
                onValueChange={(value: any) => setAggregation(value)}
              >
                <SelectTrigger className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="SUM">SUM</SelectItem>
                  <SelectItem value="AVERAGE">AVERAGE</SelectItem>
                  <SelectItem value="COUNT">COUNT</SelectItem>
                  <SelectItem value="MIN">MIN</SelectItem>
                  <SelectItem value="MAX">MAX</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>

          {/* Aggregation Selection */}

          {/* Children List */}
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <Label>Child Hierarchies ({children.length})</Label>
              <Popover open={childSearchOpen} onOpenChange={setChildSearchOpen}>
                <PopoverTrigger asChild>
                  <Button size="sm" variant="outline">
                    <Plus className="w-4 h-4 mr-2" />
                    Add Child
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="w-[500px] p-0">
                  <Command>
                    <CommandInput placeholder="Search hierarchies..." />
                    <CommandList>
                      <CommandEmpty>No hierarchies found.</CommandEmpty>
                      <CommandGroup>
                        {allHierarchies
                          .filter((h) => h.hierarchyId !== mainHierarchyId)
                          .map((h) => (
                            <CommandItem
                              key={h.hierarchyId}
                              value={`${h.hierarchyName} ${h.hierarchyId}`}
                              onSelect={() => handleAddChild(h)}
                            >
                              <Plus className="mr-2 h-4 w-4" />
                              <div className="flex flex-col flex-1">
                                <div className="flex items-center gap-2">
                                  <span className="font-medium">
                                    {h.hierarchyName}
                                  </span>
                                  <Badge
                                    variant="outline"
                                    className="text-xs px-1.5 py-0.5"
                                  >
                                    L{getHierarchyLevel(h)}
                                  </Badge>
                                </div>
                                <span className="text-xs text-muted-foreground">
                                  {getHierarchyPath(h)}
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

            <Card>
              <CardContent className="">
                {children.length === 0 ? (
                  <div className="text-center text-muted-foreground py-8">
                    No child hierarchies added yet
                  </div>
                ) : (
                  <div className="space-y-2">
                    {children.map((child) => {
                      const childHierarchy = allHierarchies.find(
                        (h) => h.hierarchyId === child.hierarchyId
                      );
                      return (
                        <div
                          key={child.hierarchyId}
                          className="flex items-center justify-between py-2 px-4 rounded-lg border-muted-foreground/10 hover:bg-muted transition-colors border"
                        >
                          <div className="flex flex-col flex-1">
                            <div className="flex items-center gap-2">
                              <span className="font-normal text-sm">
                                {child.hierarchyName}
                              </span>
                              {childHierarchy && (
                                <Badge
                                  variant="outline"
                                  className="text-xs px-1.5 py-0.5"
                                >
                                  Level {getHierarchyLevel(childHierarchy)}
                                </Badge>
                              )}
                            </div>
                            {childHierarchy && (
                              <span className="text-xs text-muted-foreground mt-1">
                                {getHierarchyPath(childHierarchy)}
                              </span>
                            )}
                          </div>
                          <Button
                            size="sm"
                            variant="ghost"
                            onClick={() => handleRemoveChild(child.hierarchyId)}
                          >
                            <X className="w-4 h-4" />
                          </Button>
                        </div>
                      );
                    })}
                  </div>
                )}
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        {/* Existing Formulas Tab */}
        <TabsContent
          value="existing"
          className="flex-1 overflow-y-auto space-y-4 "
        >
          {loading ? (
            <div className="flex items-center justify-center ">
              <Loader2 className="w-6 h-6 animate-spin text-muted-foreground" />
            </div>
          ) : existingFormulas.length === 0 ? (
            <Card>
              <CardContent className="pt-6">
                <div className="text-center text-muted-foreground py-8">
                  No existing total formulas found
                </div>
              </CardContent>
            </Card>
          ) : (
            <div className="space-y-3">
              {existingFormulas.map((formula) => (
                <Card key={formula.hierarchyId} className="overflow-hidden p-0">
                  <Collapsible
                    open={expandedFormulas.has(formula.hierarchyId)}
                    onOpenChange={() =>
                      toggleFormulaExpansion(formula.hierarchyId)
                    }
                  >
                    <CardHeader className="p-2 bg-muted/80 px-4">
                      <div className="flex items-center justify-between gap-4 p-0">
                        <div className="flex items-center gap-3 flex-1">
                          <CollapsibleTrigger asChild>
                            <Button
                              variant="ghost"
                              size="sm"
                              className="p-0 h-auto"
                            >
                              <ChevronDown
                                className={cn(
                                  "w-4 h-4 transition-transform",
                                  expandedFormulas.has(formula.hierarchyId)
                                    ? "transform rotate-180"
                                    : ""
                                )}
                              />
                            </Button>
                          </CollapsibleTrigger>
                          <div className="flex flex-row gap-4 flex-1">
                            <span className="font-semibold text-sm mt-1">
                              {formula.hierarchyName}
                            </span>
                            <Badge
                              variant="secondary"
                              className="text-xs px-2 py-1.5"
                            >
                              {formula.totalFormula.aggregation}
                            </Badge>
                            <Badge
                              variant="secondary"
                              className="text-xs px-2 py-1.5"
                            >
                              {formula.totalFormula.children.length} children
                            </Badge>
                          </div>
                        </div>
                        <div className="flex gap-2">
                          <Button
                            size="sm"
                            variant="outline"
                            onClick={() => handleLoadFormula(formula)}
                          >
                            <Edit className="w-4 h-4" />
                          </Button>
                          <Button
                            size="sm"
                            variant="destructive"
                            onClick={() =>
                              handleDeleteFormula(formula.hierarchyId)
                            }
                          >
                            <Trash2 className="w-4 h-4" />
                          </Button>
                        </div>
                      </div>
                    </CardHeader>
                    <CollapsibleContent>
                      <CardContent className="py-4">
                        <div className="space-y-2">
                          <Label className="text-sm text-muted-foreground mb-2 block">
                            Child Hierarchies:
                          </Label>
                          {formula.totalFormula.children.map((child) => {
                            const childHierarchy = allHierarchies.find(
                              (h) => h.hierarchyId === child.hierarchyId
                            );
                            return (
                              <div
                                key={child.hierarchyId}
                                className="flex items-center justify-between py-2 px-4 rounded-lg border-muted-foreground/10 hover:bg-muted transition-colors border"
                              >
                                <div className="flex flex-col flex-1">
                                  <div className="flex items-center gap-2">
                                    <span className="font-medium">
                                      {child.hierarchyName}
                                    </span>
                                    {childHierarchy && (
                                      <Badge
                                        variant="outline"
                                        className="text-xs px-1.5 py-0.5"
                                      >
                                        Level{" "}
                                        {getHierarchyLevel(childHierarchy)}
                                      </Badge>
                                    )}
                                  </div>
                                  {childHierarchy && (
                                    <span className="text-xs text-muted-foreground mt-1">
                                      {getHierarchyPath(childHierarchy)}
                                    </span>
                                  )}
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      </CardContent>
                    </CollapsibleContent>
                  </Collapsible>
                </Card>
              ))}
            </div>
          )}
        </TabsContent>
      </Tabs>

      {activeTab === "create" && (
        <div className="mt-4 flex justify-end gap-2">
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
              <Button onClick={handleUpdateFormula} disabled={saving}>
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
            <Button onClick={handleCreateFormula} disabled={saving}>
              {saving ? (
                <>
                  <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                  Creating...
                </>
              ) : (
                <>
                  <Plus className="w-4 h-4 mr-2" />
                  Create Total Formula
                </>
              )}
            </Button>
          )}
        </div>
      )}
    </div>
  );
};
