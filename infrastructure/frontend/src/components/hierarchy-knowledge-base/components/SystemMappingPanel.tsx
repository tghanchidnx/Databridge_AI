/**
 * System Mapping Panel
 * Side-by-side view of mappings across ACTUALS, BUDGET, FORECAST systems
 */
import { useState, useMemo } from "react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
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
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Database,
  Table2,
  Columns,
  Check,
  AlertCircle,
  Plus,
  Copy,
  Trash2,
  MoreVertical,
  ArrowRight,
  LinkIcon,
  Unlink,
} from "lucide-react";

export type SystemType = "ACTUALS" | "BUDGET" | "FORECAST" | "PRIOR_YEAR" | "CUSTOM";
export type JoinType = "INNER" | "LEFT" | "RIGHT" | "FULL";
export type DimensionRole = "PRIMARY" | "SECONDARY" | "OPTIONAL";

export interface SourceMapping {
  mapping_index: number;
  source_database: string;
  source_schema: string;
  source_table: string;
  source_column: string;
  source_uid?: string;
  join_type?: JoinType;
  system_type?: SystemType;
  dimension_role?: DimensionRole;
  flags: {
    include_flag: boolean;
    exclude_flag: boolean;
    transform_flag: boolean;
    active_flag: boolean;
  };
}

export interface HierarchyWithMappings {
  id: string;
  hierarchyId: string;
  hierarchyName: string;
  mappings: SourceMapping[];
}

interface SystemMappingPanelProps {
  hierarchy: HierarchyWithMappings;
  onUpdateMapping: (mappingIndex: number, updates: Partial<SourceMapping>) => void;
  onAddMapping: (systemType: SystemType) => void;
  onRemoveMapping: (mappingIndex: number) => void;
  onCopyMapping: (fromIndex: number, toSystemType: SystemType) => void;
  className?: string;
}

const SYSTEM_TYPES: SystemType[] = ["ACTUALS", "BUDGET", "FORECAST", "PRIOR_YEAR"];

const SYSTEM_COLORS: Record<SystemType, string> = {
  ACTUALS: "bg-green-100 text-green-800 border-green-300",
  BUDGET: "bg-blue-100 text-blue-800 border-blue-300",
  FORECAST: "bg-purple-100 text-purple-800 border-purple-300",
  PRIOR_YEAR: "bg-orange-100 text-orange-800 border-orange-300",
  CUSTOM: "bg-gray-100 text-gray-800 border-gray-300",
};

const SYSTEM_LABELS: Record<SystemType, string> = {
  ACTUALS: "Actuals",
  BUDGET: "Budget",
  FORECAST: "Forecast",
  PRIOR_YEAR: "Prior Year",
  CUSTOM: "Custom",
};

const JOIN_TYPE_LABELS: Record<JoinType, { label: string; description: string }> = {
  INNER: { label: "INNER", description: "Required - must match" },
  LEFT: { label: "LEFT", description: "Optional - NULL if no match" },
  RIGHT: { label: "RIGHT", description: "Right side required" },
  FULL: { label: "FULL", description: "Include all rows" },
};

const DIMENSION_ROLE_LABELS: Record<DimensionRole, { label: string; color: string }> = {
  PRIMARY: { label: "Primary", color: "bg-red-100 text-red-800" },
  SECONDARY: { label: "Secondary", color: "bg-yellow-100 text-yellow-800" },
  OPTIONAL: { label: "Optional", color: "bg-gray-100 text-gray-800" },
};

function MappingCard({
  mapping,
  onUpdate,
  onRemove,
  onCopy,
}: {
  mapping: SourceMapping;
  onUpdate: (updates: Partial<SourceMapping>) => void;
  onRemove: () => void;
  onCopy: (toSystem: SystemType) => void;
}) {
  const systemType = mapping.system_type || "ACTUALS";
  const joinType = mapping.join_type || "LEFT";
  const dimensionRole = mapping.dimension_role || "SECONDARY";

  return (
    <Card className={cn("border-2 transition-all", SYSTEM_COLORS[systemType])}>
      <CardContent className="p-3 space-y-3">
        {/* Header with system badge and actions */}
        <div className="flex items-center justify-between">
          <Badge variant="outline" className={cn("font-medium", SYSTEM_COLORS[systemType])}>
            {SYSTEM_LABELS[systemType]}
          </Badge>
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" size="icon" className="h-6 w-6">
                <MoreVertical className="h-4 w-4" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              {SYSTEM_TYPES.filter((s) => s !== systemType).map((sys) => (
                <DropdownMenuItem key={sys} onClick={() => onCopy(sys)}>
                  <Copy className="h-4 w-4 mr-2" />
                  Copy to {SYSTEM_LABELS[sys]}
                </DropdownMenuItem>
              ))}
              <DropdownMenuItem onClick={onRemove} className="text-red-600">
                <Trash2 className="h-4 w-4 mr-2" />
                Remove
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        </div>

        {/* Source info */}
        <div className="space-y-1.5 text-sm">
          <div className="flex items-center gap-1.5 text-muted-foreground">
            <Database className="h-3.5 w-3.5" />
            <span className="truncate">{mapping.source_database}</span>
          </div>
          <div className="flex items-center gap-1.5">
            <Table2 className="h-3.5 w-3.5 text-muted-foreground" />
            <span className="font-medium truncate">{mapping.source_table}</span>
          </div>
          <div className="flex items-center gap-1.5">
            <Columns className="h-3.5 w-3.5 text-muted-foreground" />
            <span className="font-mono text-primary truncate">{mapping.source_column}</span>
          </div>
        </div>

        <Separator />

        {/* Join Type */}
        <div className="space-y-1">
          <label className="text-xs text-muted-foreground">Join Type</label>
          <Select
            value={joinType}
            onValueChange={(v) => onUpdate({ join_type: v as JoinType })}
          >
            <SelectTrigger className="h-8">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {Object.entries(JOIN_TYPE_LABELS).map(([type, { label, description }]) => (
                <SelectItem key={type} value={type}>
                  <div className="flex items-center gap-2">
                    <span className="font-mono text-xs">{label}</span>
                    <span className="text-xs text-muted-foreground">- {description}</span>
                  </div>
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {/* Dimension Role */}
        <div className="space-y-1">
          <label className="text-xs text-muted-foreground">Dimension Role</label>
          <Select
            value={dimensionRole}
            onValueChange={(v) => onUpdate({ dimension_role: v as DimensionRole })}
          >
            <SelectTrigger className="h-8">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {Object.entries(DIMENSION_ROLE_LABELS).map(([role, { label }]) => (
                <SelectItem key={role} value={role}>
                  {label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {/* Active indicator */}
        <div className="flex items-center gap-2 text-xs">
          {mapping.flags.active_flag ? (
            <Badge variant="outline" className="gap-1 bg-green-50 text-green-700 border-green-200">
              <Check className="h-3 w-3" />
              Active
            </Badge>
          ) : (
            <Badge variant="outline" className="gap-1 bg-gray-50 text-gray-500">
              Inactive
            </Badge>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

function EmptyMappingSlot({
  systemType,
  onAdd,
}: {
  systemType: SystemType;
  onAdd: () => void;
}) {
  return (
    <Card
      className={cn(
        "border-2 border-dashed cursor-pointer hover:border-primary/50 transition-colors",
        "bg-muted/30"
      )}
      onClick={onAdd}
    >
      <CardContent className="p-6 flex flex-col items-center justify-center text-center min-h-[200px]">
        <div className={cn("p-2 rounded-full mb-2", SYSTEM_COLORS[systemType].split(" ")[0])}>
          <Plus className="h-5 w-5 text-muted-foreground" />
        </div>
        <p className="text-sm font-medium">Add {SYSTEM_LABELS[systemType]} Mapping</p>
        <p className="text-xs text-muted-foreground mt-1">
          Connect this hierarchy to {SYSTEM_LABELS[systemType].toLowerCase()} data
        </p>
      </CardContent>
    </Card>
  );
}

export function SystemMappingPanel({
  hierarchy,
  onUpdateMapping,
  onAddMapping,
  onRemoveMapping,
  onCopyMapping,
  className,
}: SystemMappingPanelProps) {
  // Group mappings by system type
  const mappingsBySystem = useMemo(() => {
    const grouped: Record<SystemType, SourceMapping[]> = {
      ACTUALS: [],
      BUDGET: [],
      FORECAST: [],
      PRIOR_YEAR: [],
      CUSTOM: [],
    };

    hierarchy.mappings.forEach((mapping) => {
      const systemType = mapping.system_type || "ACTUALS";
      grouped[systemType].push(mapping);
    });

    return grouped;
  }, [hierarchy.mappings]);

  // Calculate coverage
  const coverage = useMemo(() => {
    const hasActuals = mappingsBySystem.ACTUALS.length > 0;
    const hasBudget = mappingsBySystem.BUDGET.length > 0;
    const hasForecast = mappingsBySystem.FORECAST.length > 0;
    const hasPriorYear = mappingsBySystem.PRIOR_YEAR.length > 0;
    const count = [hasActuals, hasBudget, hasForecast, hasPriorYear].filter(Boolean).length;
    return { count, total: 4, percentage: Math.round((count / 4) * 100) };
  }, [mappingsBySystem]);

  return (
    <Card className={cn("", className)}>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <div>
            <CardTitle className="text-base flex items-center gap-2">
              <LinkIcon className="h-5 w-5 text-primary" />
              Multi-System Mappings
            </CardTitle>
            <CardDescription>
              {hierarchy.hierarchyName}
            </CardDescription>
          </div>
          <Badge
            variant="outline"
            className={cn(
              "font-medium",
              coverage.percentage >= 75 && "border-green-500 text-green-700",
              coverage.percentage >= 50 && coverage.percentage < 75 && "border-yellow-500 text-yellow-700",
              coverage.percentage < 50 && "border-red-500 text-red-700"
            )}
          >
            {coverage.count}/{coverage.total} Systems
          </Badge>
        </div>
      </CardHeader>

      <Separator />

      <ScrollArea className="max-h-[600px]">
        <div className="p-4">
          {/* System columns grid */}
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            {SYSTEM_TYPES.map((systemType) => (
              <div key={systemType} className="space-y-3">
                {/* Column header */}
                <div className="flex items-center justify-between">
                  <h4 className="text-sm font-semibold">{SYSTEM_LABELS[systemType]}</h4>
                  <Badge variant="secondary" className="text-xs">
                    {mappingsBySystem[systemType].length}
                  </Badge>
                </div>

                {/* Mappings or empty slot */}
                {mappingsBySystem[systemType].length > 0 ? (
                  <div className="space-y-2">
                    {mappingsBySystem[systemType].map((mapping) => (
                      <MappingCard
                        key={mapping.mapping_index}
                        mapping={mapping}
                        onUpdate={(updates) => onUpdateMapping(mapping.mapping_index, updates)}
                        onRemove={() => onRemoveMapping(mapping.mapping_index)}
                        onCopy={(toSystem) => onCopyMapping(mapping.mapping_index, toSystem)}
                      />
                    ))}
                    <Button
                      variant="outline"
                      size="sm"
                      className="w-full gap-1"
                      onClick={() => onAddMapping(systemType)}
                    >
                      <Plus className="h-4 w-4" />
                      Add More
                    </Button>
                  </div>
                ) : (
                  <EmptyMappingSlot systemType={systemType} onAdd={() => onAddMapping(systemType)} />
                )}
              </div>
            ))}
          </div>

          {/* Missing mappings warning */}
          {coverage.count < 2 && (
            <div className="mt-6 p-4 bg-yellow-50 border border-yellow-200 rounded-lg flex items-start gap-3">
              <AlertCircle className="h-5 w-5 text-yellow-600 shrink-0 mt-0.5" />
              <div>
                <p className="text-sm font-medium text-yellow-800">
                  Limited variance analysis
                </p>
                <p className="text-xs text-yellow-700 mt-1">
                  Add mappings for at least 2 systems to enable variance calculations
                  (e.g., Actuals vs Budget).
                </p>
              </div>
            </div>
          )}
        </div>
      </ScrollArea>
    </Card>
  );
}

export default SystemMappingPanel;
