/**
 * Variance Configuration Panel
 * Configure variance calculations between different system types
 */
import { useState, useMemo } from "react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Switch } from "@/components/ui/switch";
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
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  ArrowRightLeft,
  Calculator,
  Plus,
  Trash2,
  TrendingUp,
  TrendingDown,
  Percent,
  AlertCircle,
  Check,
  Info,
} from "lucide-react";

type SystemType = "ACTUALS" | "BUDGET" | "FORECAST" | "PRIOR_YEAR" | "CUSTOM";

export interface VarianceComparison {
  id?: string;
  name: string;
  minuend: SystemType;      // What we subtract from
  subtrahend: SystemType;   // What we subtract
  includePercent: boolean;
}

export interface VarianceConfig {
  enabled: boolean;
  comparisons: VarianceComparison[];
}

interface VarianceConfigPanelProps {
  config: VarianceConfig;
  onChange: (config: VarianceConfig) => void;
  availableSystems?: SystemType[];
  className?: string;
}

const SYSTEM_OPTIONS: { value: SystemType; label: string; color: string }[] = [
  { value: "ACTUALS", label: "Actuals", color: "text-green-600" },
  { value: "BUDGET", label: "Budget", color: "text-blue-600" },
  { value: "FORECAST", label: "Forecast", color: "text-purple-600" },
  { value: "PRIOR_YEAR", label: "Prior Year", color: "text-orange-600" },
  { value: "CUSTOM", label: "Custom", color: "text-gray-600" },
];

const PRESET_COMPARISONS: VarianceComparison[] = [
  {
    name: "Actual vs Budget",
    minuend: "ACTUALS",
    subtrahend: "BUDGET",
    includePercent: true,
  },
  {
    name: "Actual vs Forecast",
    minuend: "ACTUALS",
    subtrahend: "FORECAST",
    includePercent: true,
  },
  {
    name: "Budget vs Forecast",
    minuend: "BUDGET",
    subtrahend: "FORECAST",
    includePercent: true,
  },
  {
    name: "YoY Variance",
    minuend: "ACTUALS",
    subtrahend: "PRIOR_YEAR",
    includePercent: true,
  },
];

function ComparisonRow({
  comparison,
  index,
  onChange,
  onRemove,
}: {
  comparison: VarianceComparison;
  index: number;
  onChange: (updates: Partial<VarianceComparison>) => void;
  onRemove: () => void;
}) {
  return (
    <Card className="bg-muted/30">
      <CardContent className="p-4 space-y-4">
        {/* Name */}
        <div className="flex items-start gap-4">
          <div className="flex-1">
            <Label htmlFor={`name-${index}`} className="text-xs text-muted-foreground">
              Comparison Name
            </Label>
            <Input
              id={`name-${index}`}
              value={comparison.name}
              onChange={(e) => onChange({ name: e.target.value })}
              placeholder="e.g., Actual vs Budget"
              className="mt-1 h-9"
            />
          </div>
          <Button
            variant="ghost"
            size="icon"
            className="text-muted-foreground hover:text-red-600"
            onClick={onRemove}
          >
            <Trash2 className="h-4 w-4" />
          </Button>
        </div>

        {/* Formula visualization */}
        <div className="flex items-center gap-3 bg-background rounded-lg p-3 border">
          <Select
            value={comparison.minuend}
            onValueChange={(v) => onChange({ minuend: v as SystemType })}
          >
            <SelectTrigger className="w-[140px] h-9">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {SYSTEM_OPTIONS.map((opt) => (
                <SelectItem key={opt.value} value={opt.value}>
                  <span className={opt.color}>{opt.label}</span>
                </SelectItem>
              ))}
            </SelectContent>
          </Select>

          <div className="flex items-center gap-2 text-muted-foreground">
            <span className="text-xl font-light">−</span>
          </div>

          <Select
            value={comparison.subtrahend}
            onValueChange={(v) => onChange({ subtrahend: v as SystemType })}
          >
            <SelectTrigger className="w-[140px] h-9">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {SYSTEM_OPTIONS.map((opt) => (
                <SelectItem key={opt.value} value={opt.value}>
                  <span className={opt.color}>{opt.label}</span>
                </SelectItem>
              ))}
            </SelectContent>
          </Select>

          <div className="flex items-center gap-2 text-muted-foreground">
            <span className="text-xl font-light">=</span>
          </div>

          <div className="flex items-center gap-2 px-3 py-1.5 bg-muted rounded-md">
            <Calculator className="h-4 w-4 text-primary" />
            <span className="text-sm font-medium">Variance</span>
          </div>
        </div>

        {/* Include percentage toggle */}
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Percent className="h-4 w-4 text-muted-foreground" />
            <Label htmlFor={`percent-${index}`} className="text-sm">
              Include Variance Percentage
            </Label>
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger>
                  <Info className="h-3.5 w-3.5 text-muted-foreground" />
                </TooltipTrigger>
                <TooltipContent>
                  <p className="max-w-xs text-xs">
                    Calculate variance as a percentage: (Variance / Base) × 100
                  </p>
                </TooltipContent>
              </Tooltip>
            </TooltipProvider>
          </div>
          <Switch
            id={`percent-${index}`}
            checked={comparison.includePercent}
            onCheckedChange={(checked) => onChange({ includePercent: checked })}
          />
        </div>

        {/* Preview */}
        <div className="text-xs text-muted-foreground bg-muted/50 rounded p-2">
          <span className="font-medium">Output columns: </span>
          <code className="text-primary">
            {comparison.name.replace(/\s+/g, "_").toUpperCase()}_VAR
          </code>
          {comparison.includePercent && (
            <>
              {", "}
              <code className="text-primary">
                {comparison.name.replace(/\s+/g, "_").toUpperCase()}_VAR_PCT
              </code>
            </>
          )}
        </div>
      </CardContent>
    </Card>
  );
}

export function VarianceConfigPanel({
  config,
  onChange,
  availableSystems = ["ACTUALS", "BUDGET", "FORECAST", "PRIOR_YEAR"],
  className,
}: VarianceConfigPanelProps) {
  const handleToggleEnabled = (enabled: boolean) => {
    onChange({ ...config, enabled });
  };

  const handleAddComparison = (comparison: VarianceComparison) => {
    onChange({
      ...config,
      comparisons: [...config.comparisons, { ...comparison, id: crypto.randomUUID() }],
    });
  };

  const handleUpdateComparison = (index: number, updates: Partial<VarianceComparison>) => {
    const newComparisons = [...config.comparisons];
    newComparisons[index] = { ...newComparisons[index], ...updates };
    onChange({ ...config, comparisons: newComparisons });
  };

  const handleRemoveComparison = (index: number) => {
    onChange({
      ...config,
      comparisons: config.comparisons.filter((_, i) => i !== index),
    });
  };

  // Filter presets that aren't already added
  const availablePresets = useMemo(() => {
    return PRESET_COMPARISONS.filter(
      (preset) =>
        !config.comparisons.some(
          (c) => c.minuend === preset.minuend && c.subtrahend === preset.subtrahend
        )
    );
  }, [config.comparisons]);

  return (
    <Card className={cn("", className)}>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <ArrowRightLeft className="h-5 w-5 text-primary" />
            <CardTitle className="text-base">Variance Configuration</CardTitle>
          </div>
          <Switch
            id="variance-enabled"
            checked={config.enabled}
            onCheckedChange={handleToggleEnabled}
          />
        </div>
        <CardDescription>
          Configure variance calculations between different data systems
        </CardDescription>
      </CardHeader>

      <Separator />

      {config.enabled ? (
        <ScrollArea className="max-h-[500px]">
          <div className="p-4 space-y-6">
            {/* Comparisons list */}
            {config.comparisons.length > 0 ? (
              <div className="space-y-3">
                {config.comparisons.map((comparison, index) => (
                  <ComparisonRow
                    key={comparison.id || index}
                    comparison={comparison}
                    index={index}
                    onChange={(updates) => handleUpdateComparison(index, updates)}
                    onRemove={() => handleRemoveComparison(index)}
                  />
                ))}
              </div>
            ) : (
              <div className="text-center py-8">
                <Calculator className="h-8 w-8 text-muted-foreground mx-auto mb-2" />
                <p className="text-sm font-medium">No variance comparisons defined</p>
                <p className="text-xs text-muted-foreground mt-1">
                  Add comparisons below or use a preset
                </p>
              </div>
            )}

            <Separator />

            {/* Add preset or custom */}
            <div className="space-y-3">
              <Label className="text-sm font-medium">Add Comparison</Label>

              {/* Preset buttons */}
              {availablePresets.length > 0 && (
                <div className="space-y-2">
                  <p className="text-xs text-muted-foreground">Quick presets:</p>
                  <div className="flex flex-wrap gap-2">
                    {availablePresets.map((preset, i) => (
                      <Button
                        key={i}
                        variant="outline"
                        size="sm"
                        onClick={() => handleAddComparison(preset)}
                        className="gap-1"
                      >
                        <Plus className="h-3 w-3" />
                        {preset.name}
                      </Button>
                    ))}
                  </div>
                </div>
              )}

              {/* Custom button */}
              <Button
                variant="outline"
                size="sm"
                onClick={() =>
                  handleAddComparison({
                    name: "Custom Variance",
                    minuend: "ACTUALS",
                    subtrahend: "BUDGET",
                    includePercent: true,
                  })
                }
                className="gap-2"
              >
                <Plus className="h-4 w-4" />
                Add Custom Comparison
              </Button>
            </div>

            {/* Summary */}
            {config.comparisons.length > 0 && (
              <>
                <Separator />
                <div className="space-y-2">
                  <p className="text-sm font-medium">Summary</p>
                  <div className="bg-muted/50 rounded-lg p-3 space-y-2">
                    <div className="flex items-center gap-2">
                      <Badge variant="secondary">{config.comparisons.length}</Badge>
                      <span className="text-sm text-muted-foreground">variance comparisons configured</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <Badge variant="secondary">
                        {config.comparisons.length + config.comparisons.filter((c) => c.includePercent).length}
                      </Badge>
                      <span className="text-sm text-muted-foreground">output columns will be generated</span>
                    </div>
                  </div>
                </div>
              </>
            )}
          </div>
        </ScrollArea>
      ) : (
        <CardContent className="py-8">
          <div className="text-center">
            <AlertCircle className="h-8 w-8 text-muted-foreground mx-auto mb-2" />
            <p className="text-sm font-medium">Variance calculations disabled</p>
            <p className="text-xs text-muted-foreground mt-1">
              Enable to configure variance comparisons between systems
            </p>
          </div>
        </CardContent>
      )}
    </Card>
  );
}

export default VarianceConfigPanel;
