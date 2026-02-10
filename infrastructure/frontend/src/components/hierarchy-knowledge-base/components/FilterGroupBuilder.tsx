import React, { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import {
  Plus,
  Trash2,
  Filter,
  Save,
  Search,
  X,
  Copy,
  Check,
  ChevronsUpDown,
} from "lucide-react";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
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
import { cn } from "@/lib/utils";
import type {
  SmartHierarchyMaster,
  FilterGroup,
  FilterCondition,
} from "@/services/api/hierarchy";
import { smartHierarchyService } from "@/services/api/hierarchy";
import { useToast } from "@/hooks/use-toast";

interface FilterGroupBuilderProps {
  value?: FilterGroup;
  onChange: (group: FilterGroup) => void;
  currentHierarchy?: SmartHierarchyMaster;
  projectId?: string;
  onSave?: (group: FilterGroup) => void;
  availableHierarchies?: SmartHierarchyMaster[];
}

const OPERATORS = [
  { value: "=", label: "Equals (=)" },
  { value: "!=", label: "Not Equals (!=)" },
  { value: ">", label: "Greater Than (>)" },
  { value: ">=", label: "Greater or Equal (>=)" },
  { value: "<", label: "Less Than (<)" },
  { value: "<=", label: "Less or Equal (<=)" },
  { value: "LIKE", label: "Like" },
  { value: "NOT LIKE", label: "Not Like" },
  { value: "IN", label: "In (comma separated)" },
  { value: "NOT IN", label: "Not In" },
  { value: "IS NULL", label: "Is Null" },
  { value: "IS NOT NULL", label: "Is Not Null" },
  { value: "BETWEEN", label: "Between" },
];

export const FilterGroupBuilder: React.FC<FilterGroupBuilderProps> = ({
  value,
  onChange,
  currentHierarchy,
  projectId,
  onSave,
  availableHierarchies = [],
}) => {
  const { toast } = useToast();
  const [group, setGroup] = useState<FilterGroup>({
    groupName: "",
    filter_conditions: [],
    ...value,
  });

  const [isSaving, setIsSaving] = useState(false);
  const [isLoadingFilters, setIsLoadingFilters] = useState(false);
  const [selectedHierarchyId, setSelectedHierarchyId] = useState<string>("");
  const [hierarchySearchOpen, setHierarchySearchOpen] = useState(false);

  useEffect(() => {
    if (value && JSON.stringify(value) !== JSON.stringify(group)) {
      setGroup(value);
    }
  }, [value]);

  const handleGroupChange = (field: keyof FilterGroup, val: any) => {
    const updated = { ...group, [field]: val };
    setGroup(updated);
    onChange(updated);
  };

  const addFilterCondition = () => {
    const newCondition: FilterCondition = {
      column: "",
      operator: "=",
      value: "",
      logic: group.filter_conditions.length > 0 ? "AND" : undefined,
    };

    const updated = {
      ...group,
      filter_conditions: [...group.filter_conditions, newCondition],
    };
    setGroup(updated);
    onChange(updated);
  };

  const updateFilterCondition = (
    index: number,
    field: keyof FilterCondition,
    val: any
  ) => {
    const updatedConditions = group.filter_conditions.map((cond, idx) =>
      idx === index ? { ...cond, [field]: val } : cond
    );

    const updated = { ...group, filter_conditions: updatedConditions };
    setGroup(updated);
    onChange(updated);
  };

  const removeFilterCondition = (index: number) => {
    const updatedConditions = group.filter_conditions.filter(
      (_, idx) => idx !== index
    );

    const updated = { ...group, filter_conditions: updatedConditions };
    setGroup(updated);
    onChange(updated);
  };

  const handleSave = async () => {
    // if (group.filter_conditions.length === 0 && !group.custom_sql) {
    //   toast({
    //     title: "Validation Error",
    //     description: "Please add at least one filter condition or custom SQL",
    //     variant: "destructive",
    //   });
    //   return;
    // }

    setIsSaving(true);
    try {
      if (onSave) {
        await onSave(group);
      }
      toast({
        title: "Filter Saved",
        description: "Filter configuration saved successfully",
      });
    } catch (error) {
      toast({
        title: "Save Failed",
        description: "Failed to save filter configuration",
        variant: "destructive",
      });
    } finally {
      setIsSaving(false);
    }
  };

  const handleCopyFiltersFromHierarchy = async () => {
    if (!selectedHierarchyId || !projectId) {
      toast({
        title: "Selection Required",
        description: "Please select a hierarchy to copy filters from",
        variant: "destructive",
      });
      return;
    }

    setIsLoadingFilters(true);
    try {
      const filterAttributes =
        await smartHierarchyService.getHierarchyFilterAttributes(
          projectId,
          selectedHierarchyId
        );

      // Copy filter attributes to the current group
      const updated = {
        ...group,
        filter_group_1: filterAttributes.filter_group_1 || group.filter_group_1,
        filter_group_1_type:
          filterAttributes.filter_group_1_type || group.filter_group_1_type,
        filter_group_2: filterAttributes.filter_group_2 || group.filter_group_2,
        filter_group_2_type:
          filterAttributes.filter_group_2_type || group.filter_group_2_type,
        filter_group_3: filterAttributes.filter_group_3 || group.filter_group_3,
        filter_group_3_type:
          filterAttributes.filter_group_3_type || group.filter_group_3_type,
        filter_group_4: filterAttributes.filter_group_4 || group.filter_group_4,
        filter_group_4_type:
          filterAttributes.filter_group_4_type || group.filter_group_4_type,
        filter_conditions:
          filterAttributes.filter_conditions.length > 0
            ? filterAttributes.filter_conditions
            : group.filter_conditions,
        custom_sql: filterAttributes.custom_sql || group.custom_sql,
      };

      setGroup(updated);
      onChange(updated);

      toast({
        title: "Filters Copied",
        description: `Filter attributes copied from ${filterAttributes.hierarchyName}`,
      });
    } catch (error) {
      toast({
        title: "Copy Failed",
        description: "Failed to copy filter attributes",
        variant: "destructive",
      });
    } finally {
      setIsLoadingFilters(false);
    }
  };

  return (
    <Card className="border-0">
      <CardHeader className="pb-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Filter className="w-5 h-5 text-primary" />
            <CardTitle className="text-lg">
              Filter Group Configuration
            </CardTitle>
          </div>
          {currentHierarchy && (
            <Badge variant="outline" className="text-xs">
              Current: {currentHierarchy.hierarchyName}
            </Badge>
          )}
        </div>
      </CardHeader>
      <CardContent>
        {!projectId && (
          <Alert variant="destructive" className="mb-4">
            <AlertDescription>
              Project ID is missing. Please select a hierarchy first to
              configure filters.
            </AlertDescription>
          </Alert>
        )}

        <div className="space-y-4">
          {/* Copy Filters from Other Hierarchy */}
          {/* {availableHierarchies.length > 0 && projectId && (
            <Card className="p-3 bg-blue-50 dark:bg-blue-950 border-blue-200 dark:border-blue-800">
              <div className="space-y-2">
                <Label className="text-sm font-semibold flex items-center gap-2">
                  <Copy className="w-4 h-4" />
                  Copy Filters from Another Hierarchy
                </Label>
                <div className="flex gap-2">
                  <Popover
                    open={hierarchySearchOpen}
                    onOpenChange={setHierarchySearchOpen}
                  >
                    <PopoverTrigger asChild>
                      <Button
                        variant="outline"
                        role="combobox"
                        aria-expanded={hierarchySearchOpen}
                        className="flex-1 justify-between"
                      >
                        {selectedHierarchyId
                          ? availableHierarchies.find(
                              (h) => h.hierarchyId === selectedHierarchyId
                            )?.hierarchyName
                          : "Select hierarchy to copy from..."}
                        <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                      </Button>
                    </PopoverTrigger>
                    <PopoverContent className="w-[400px] p-0" align="start">
                      <Command>
                        <CommandInput placeholder="Search hierarchies..." />
                        <CommandList>
                          <CommandEmpty>No hierarchy found.</CommandEmpty>
                          <CommandGroup>
                            {availableHierarchies
                              .filter(
                                (h) =>
                                  h.hierarchyId !==
                                  currentHierarchy?.hierarchyId
                              )
                              .map((h) => (
                                <CommandItem
                                  key={h.hierarchyId}
                                  value={`${h.hierarchyName} ${h.hierarchyId}`}
                                  onSelect={() => {
                                    setSelectedHierarchyId(h.hierarchyId);
                                    setHierarchySearchOpen(false);
                                  }}
                                >
                                  <Check
                                    className={cn(
                                      "mr-2 h-4 w-4",
                                      selectedHierarchyId === h.hierarchyId
                                        ? "opacity-100"
                                        : "opacity-0"
                                    )}
                                  />
                                  <div className="flex flex-col">
                                    <span className="font-medium">
                                      {h.hierarchyName}
                                    </span>
                                    <span className="text-xs text-muted-foreground">
                                      {h.hierarchyId}
                                    </span>
                                  </div>
                                </CommandItem>
                              ))}
                          </CommandGroup>
                        </CommandList>
                      </Command>
                    </PopoverContent>
                  </Popover>
                  <Button
                    variant="outline"
                    onClick={handleCopyFiltersFromHierarchy}
                    disabled={!selectedHierarchyId || isLoadingFilters}
                    className="gap-2"
                  >
                    <Copy className="w-4 h-4" />
                    {isLoadingFilters ? "Copying..." : "Copy"}
                  </Button>
                </div>
                <p className="text-xs text-muted-foreground">
                  This will copy all filter attributes from the selected
                  hierarchy to the form below
                </p>
              </div>
            </Card>
          )} */}

          {/* Filter Group Fields (4 inputs) */}
          <div className="space-y-2">
            <Label className="text-sm font-semibold">Filter Group Fields</Label>
            <div className="grid grid-cols-2 gap-3">
              <div className="grid grid-cols-2 gap-2">
                <div>
                  <Label
                    htmlFor="filter-group-1"
                    className="text-xs text-muted-foreground"
                  >
                    Filter Group 1
                  </Label>
                  <Input
                    id="filter-group-1"
                    placeholder="e.g., Department"
                    value={group.filter_group_1 || ""}
                    onChange={(e) =>
                      handleGroupChange("filter_group_1", e.target.value)
                    }
                  />
                </div>
                <div>
                  <Label
                    htmlFor="filter-group-1-type"
                    className="text-xs text-muted-foreground"
                  >
                    Filter Group 1 Type
                  </Label>
                  <Input
                    id="filter-group-1-type"
                    placeholder="e.g., DEPT_CODE"
                    value={group.filter_group_1_type || ""}
                    onChange={(e) =>
                      handleGroupChange("filter_group_1_type", e.target.value)
                    }
                  />
                </div>
              </div>
              <div className="grid grid-cols-2 gap-2">
                <div>
                  <Label
                    htmlFor="filter-group-2"
                    className="text-xs text-muted-foreground"
                  >
                    Filter Group 2
                  </Label>
                  <Input
                    id="filter-group-2"
                    placeholder="e.g., Region"
                    value={group.filter_group_2 || ""}
                    onChange={(e) =>
                      handleGroupChange("filter_group_2", e.target.value)
                    }
                  />
                </div>
                <div>
                  <Label
                    htmlFor="filter-group-2-type"
                    className="text-xs text-muted-foreground"
                  >
                    Filter Group 2 Type
                  </Label>
                  <Input
                    id="filter-group-2-type"
                    placeholder="e.g., CORP_CODE"
                    value={group.filter_group_2_type || ""}
                    onChange={(e) =>
                      handleGroupChange("filter_group_2_type", e.target.value)
                    }
                  />
                </div>
              </div>
              <div className="grid grid-cols-2 gap-2">
                <div>
                  <Label
                    htmlFor="filter-group-3"
                    className="text-xs text-muted-foreground"
                  >
                    Filter Group 3
                  </Label>
                  <Input
                    id="filter-group-3"
                    placeholder="e.g., Category"
                    value={group.filter_group_3 || ""}
                    onChange={(e) =>
                      handleGroupChange("filter_group_3", e.target.value)
                    }
                  />
                </div>
                <div>
                  <Label
                    htmlFor="filter-group-3-type"
                    className="text-xs text-muted-foreground"
                  >
                    Filter Group 3 Type
                  </Label>
                  <Input
                    id="filter-group-3-type"
                    placeholder="e.g., CATEGORY_CODE"
                    value={group.filter_group_3_type || ""}
                    onChange={(e) =>
                      handleGroupChange("filter_group_3_type", e.target.value)
                    }
                  />
                </div>
              </div>
              <div className="grid grid-cols-2 gap-2">
                <div>
                  <Label
                    htmlFor="filter-group-4"
                    className="text-xs text-muted-foreground"
                  >
                    Filter Group 4
                  </Label>
                  <Input
                    id="filter-group-4"
                    placeholder="e.g., Status"
                    value={group.filter_group_4 || ""}
                    onChange={(e) =>
                      handleGroupChange("filter_group_4", e.target.value)
                    }
                  />
                </div>
                <div>
                  <Label
                    htmlFor="filter-group-4-type"
                    className="text-xs text-muted-foreground"
                  >
                    Filter Group 4 Type
                  </Label>
                  <Input
                    id="filter-group-4-type"
                    placeholder="e.g., STATUS_CODE"
                    value={group.filter_group_4_type || ""}
                    onChange={(e) =>
                      handleGroupChange("filter_group_4_type", e.target.value)
                    }
                  />
                </div>
              </div>
            </div>
          </div>

          {/* Filter Conditions */}
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <Label className="text-sm font-semibold">Filter Conditions</Label>
              <Button
                variant="outline"
                size="sm"
                onClick={addFilterCondition}
                className="gap-2"
              >
                <Plus className="w-4 h-4" />
                Add Condition
              </Button>
            </div>

            {group?.filter_conditions?.length === 0 ? (
              <div className="p-4 border-2 border-dashed rounded-lg text-center text-sm text-muted-foreground">
                No filter conditions added yet. Click "Add Condition" to start.
              </div>
            ) : (
              <div className="space-y-3">
                {group?.filter_conditions?.map((condition, index) => (
                  <Card key={index} className="p-3 bg-muted/30">
                    <div className="space-y-3">
                      {index > 0 && (
                        <div className="flex items-center gap-2">
                          <Label className="text-xs">Logic:</Label>
                          <Select
                            value={condition.logic || "AND"}
                            onValueChange={(v) =>
                              updateFilterCondition(
                                index,
                                "logic",
                                v as "AND" | "OR"
                              )
                            }
                          >
                            <SelectTrigger className="w-24 h-8">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              <SelectItem value="AND">AND</SelectItem>
                              <SelectItem value="OR">OR</SelectItem>
                            </SelectContent>
                          </Select>
                        </div>
                      )}

                      <div className="grid grid-cols-12 gap-2 items-end">
                        <div className="col-span-4">
                          <Label className="text-xs">Column</Label>
                          <Input
                            placeholder="Column name"
                            value={condition.column}
                            onChange={(e) =>
                              updateFilterCondition(
                                index,
                                "column",
                                e.target.value
                              )
                            }
                            className="h-8"
                          />
                        </div>

                        <div className="col-span-3">
                          <Label className="text-xs">Operator</Label>
                          <Select
                            value={condition.operator}
                            onValueChange={(v) =>
                              updateFilterCondition(index, "operator", v)
                            }
                          >
                            <SelectTrigger className="h-8">
                              <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                              {OPERATORS.map((op) => (
                                <SelectItem key={op.value} value={op.value}>
                                  {op.label}
                                </SelectItem>
                              ))}
                            </SelectContent>
                          </Select>
                        </div>

                        <div className="col-span-4">
                          <Label className="text-xs">Value</Label>
                          <Input
                            placeholder="Filter value"
                            value={condition.value}
                            onChange={(e) =>
                              updateFilterCondition(
                                index,
                                "value",
                                e.target.value
                              )
                            }
                            className="h-8"
                            disabled={
                              condition.operator === "IS NULL" ||
                              condition.operator === "IS NOT NULL"
                            }
                          />
                        </div>

                        <div className="col-span-1">
                          <Button
                            variant="ghost"
                            size="icon"
                            onClick={() => removeFilterCondition(index)}
                            className="h-8 w-8"
                          >
                            <X className="w-4 h-4" />
                          </Button>
                        </div>
                      </div>
                    </div>
                  </Card>
                ))}
              </div>
            )}
          </div>

          {/* Custom SQL (Optional) */}
          <div className="space-y-2">
            <Label htmlFor="custom-sql" className="text-sm font-semibold">
              Custom SQL Filter (Optional)
            </Label>
            <Input
              id="custom-sql"
              placeholder="e.g., WHERE status = 'ACTIVE' AND year > 2020"
              value={group.custom_sql || ""}
              onChange={(e) => handleGroupChange("custom_sql", e.target.value)}
              className="font-mono text-xs"
            />
            <p className="text-xs text-muted-foreground">
              This will override the filter conditions above if provided
            </p>
          </div>

          {/* Save Button */}
          <div className="flex justify-end gap-2 pt-4 border-t">
            <Button onClick={handleSave} disabled={isSaving} className="gap-2">
              <Save className="w-4 h-4" />
              {isSaving ? "Saving..." : "Save Filters"}
            </Button>
          </div>
        </div>
      </CardContent>
    </Card>
  );
};
