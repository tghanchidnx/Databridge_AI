/**
 * Anomaly Alert Panel
 * Displays detected anomalies with severity indicators and fix suggestions
 */
import { useState, useMemo } from "react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Progress } from "@/components/ui/progress";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import {
  AlertTriangle,
  AlertCircle,
  Info,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  Wrench,
  RefreshCw,
  Filter,
  Eye,
  Lightbulb,
  Zap,
  Database,
  Calculator,
  FolderTree,
  Type,
  GitBranch,
  Copy,
  Link2,
} from "lucide-react";

export type AnomalyType =
  | "missing_mapping"
  | "type_mismatch"
  | "inconsistent_pattern"
  | "orphan_node"
  | "circular_reference"
  | "duplicate_mapping"
  | "missing_formula"
  | "formula_error"
  | "naming_convention"
  | "level_inconsistency";

export interface Anomaly {
  id: string;
  type: AnomalyType;
  severity: "error" | "warning" | "info";
  hierarchyId?: string;
  hierarchyName?: string;
  message: string;
  details: string;
  suggestion?: string;
  autoFixable: boolean;
  fixAction?: {
    type: string;
    params: Record<string, any>;
  };
}

export interface AnomalySummary {
  total: number;
  byType: Record<AnomalyType, number>;
  bySeverity: Record<"error" | "warning" | "info", number>;
  autoFixableCount: number;
}

interface AnomalyAlertPanelProps {
  anomalies: Anomaly[];
  summary: AnomalySummary;
  isLoading?: boolean;
  onRefresh: () => void;
  onNavigateToHierarchy?: (hierarchyId: string) => void;
  onAutoFix?: (anomaly: Anomaly) => void;
  onFixAll?: (anomalies: Anomaly[]) => void;
  className?: string;
}

function getSeverityIcon(severity: Anomaly["severity"]) {
  switch (severity) {
    case "error":
      return <AlertCircle className="h-4 w-4 text-red-600" />;
    case "warning":
      return <AlertTriangle className="h-4 w-4 text-yellow-600" />;
    case "info":
      return <Info className="h-4 w-4 text-blue-600" />;
  }
}

function getSeverityColors(severity: Anomaly["severity"]) {
  switch (severity) {
    case "error":
      return "bg-red-50 border-red-200 text-red-800";
    case "warning":
      return "bg-yellow-50 border-yellow-200 text-yellow-800";
    case "info":
      return "bg-blue-50 border-blue-200 text-blue-800";
  }
}

function getTypeIcon(type: AnomalyType) {
  const icons: Record<AnomalyType, React.ReactNode> = {
    missing_mapping: <Database className="h-4 w-4" />,
    type_mismatch: <Type className="h-4 w-4" />,
    inconsistent_pattern: <GitBranch className="h-4 w-4" />,
    orphan_node: <FolderTree className="h-4 w-4" />,
    circular_reference: <Link2 className="h-4 w-4" />,
    duplicate_mapping: <Copy className="h-4 w-4" />,
    missing_formula: <Calculator className="h-4 w-4" />,
    formula_error: <Calculator className="h-4 w-4" />,
    naming_convention: <Type className="h-4 w-4" />,
    level_inconsistency: <FolderTree className="h-4 w-4" />,
  };
  return icons[type];
}

function getTypeLabel(type: AnomalyType): string {
  const labels: Record<AnomalyType, string> = {
    missing_mapping: "Missing Mapping",
    type_mismatch: "Type Mismatch",
    inconsistent_pattern: "Pattern Issue",
    orphan_node: "Orphan Node",
    circular_reference: "Circular Reference",
    duplicate_mapping: "Duplicate Mapping",
    missing_formula: "Missing Formula",
    formula_error: "Formula Error",
    naming_convention: "Naming Issue",
    level_inconsistency: "Level Issue",
  };
  return labels[type];
}

function AnomalyCard({
  anomaly,
  onNavigate,
  onFix,
}: {
  anomaly: Anomaly;
  onNavigate?: () => void;
  onFix?: () => void;
}) {
  const [expanded, setExpanded] = useState(false);

  return (
    <Card className={cn("transition-all", getSeverityColors(anomaly.severity))}>
      <Collapsible open={expanded} onOpenChange={setExpanded}>
        <CollapsibleTrigger asChild>
          <CardHeader className="pb-2 cursor-pointer">
            <div className="flex items-start gap-3">
              <div className="mt-0.5">{getSeverityIcon(anomaly.severity)}</div>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2">
                  <CardTitle className="text-sm font-medium">{anomaly.message}</CardTitle>
                  <Badge variant="outline" className="text-xs shrink-0">
                    {getTypeLabel(anomaly.type)}
                  </Badge>
                </div>
                {anomaly.hierarchyName && (
                  <CardDescription className="text-xs mt-1">
                    in {anomaly.hierarchyName}
                  </CardDescription>
                )}
              </div>
              <div className="flex items-center gap-2">
                {anomaly.autoFixable && (
                  <TooltipProvider>
                    <Tooltip>
                      <TooltipTrigger>
                        <Zap className="h-4 w-4 text-yellow-600" />
                      </TooltipTrigger>
                      <TooltipContent>Auto-fixable</TooltipContent>
                    </Tooltip>
                  </TooltipProvider>
                )}
                {expanded ? (
                  <ChevronDown className="h-4 w-4 text-muted-foreground" />
                ) : (
                  <ChevronRight className="h-4 w-4 text-muted-foreground" />
                )}
              </div>
            </div>
          </CardHeader>
        </CollapsibleTrigger>

        <CollapsibleContent>
          <CardContent className="pt-0 space-y-3">
            <div className="text-sm bg-background/50 rounded p-2 border">
              {anomaly.details}
            </div>

            {anomaly.suggestion && (
              <div className="flex items-start gap-2 text-sm">
                <Lightbulb className="h-4 w-4 text-yellow-600 shrink-0 mt-0.5" />
                <span>{anomaly.suggestion}</span>
              </div>
            )}

            <div className="flex items-center gap-2 pt-1">
              {anomaly.hierarchyId && onNavigate && (
                <Button size="sm" variant="outline" onClick={onNavigate} className="gap-1">
                  <Eye className="h-4 w-4" />
                  View
                </Button>
              )}
              {anomaly.autoFixable && onFix && (
                <Button size="sm" onClick={onFix} className="gap-1">
                  <Wrench className="h-4 w-4" />
                  Fix
                </Button>
              )}
            </div>
          </CardContent>
        </CollapsibleContent>
      </Collapsible>
    </Card>
  );
}

function SeveritySummaryCard({
  label,
  count,
  severity,
  icon,
}: {
  label: string;
  count: number;
  severity: "error" | "warning" | "info";
  icon: React.ReactNode;
}) {
  const colorClasses = {
    error: "text-red-600 bg-red-100",
    warning: "text-yellow-600 bg-yellow-100",
    info: "text-blue-600 bg-blue-100",
  };

  return (
    <div className="flex items-center gap-3 p-3 rounded-lg border bg-background">
      <div className={cn("p-2 rounded-lg", colorClasses[severity])}>{icon}</div>
      <div>
        <div className="text-2xl font-bold">{count}</div>
        <div className="text-xs text-muted-foreground">{label}</div>
      </div>
    </div>
  );
}

export function AnomalyAlertPanel({
  anomalies,
  summary,
  isLoading,
  onRefresh,
  onNavigateToHierarchy,
  onAutoFix,
  onFixAll,
  className,
}: AnomalyAlertPanelProps) {
  const [filter, setFilter] = useState<"all" | "error" | "warning" | "info">("all");
  const [typeFilter, setTypeFilter] = useState<AnomalyType | "all">("all");

  const filteredAnomalies = useMemo(() => {
    return anomalies.filter((a) => {
      if (filter !== "all" && a.severity !== filter) return false;
      if (typeFilter !== "all" && a.type !== typeFilter) return false;
      return true;
    });
  }, [anomalies, filter, typeFilter]);

  const autoFixableAnomalies = filteredAnomalies.filter((a) => a.autoFixable);

  const healthScore = useMemo(() => {
    if (summary.total === 0) return 100;
    const errorWeight = summary.bySeverity.error * 10;
    const warningWeight = summary.bySeverity.warning * 5;
    const infoWeight = summary.bySeverity.info * 1;
    const totalWeight = errorWeight + warningWeight + infoWeight;
    return Math.max(0, 100 - Math.min(totalWeight, 100));
  }, [summary]);

  const healthColor =
    healthScore >= 80 ? "text-green-600" : healthScore >= 50 ? "text-yellow-600" : "text-red-600";

  if (isLoading) {
    return (
      <Card className={cn("", className)}>
        <CardContent className="flex flex-col items-center justify-center py-12">
          <RefreshCw className="h-8 w-8 text-primary animate-spin mb-3" />
          <p className="text-sm font-medium">Analyzing project...</p>
          <p className="text-xs text-muted-foreground mt-1">
            Checking for anomalies and issues
          </p>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className={cn("", className)}>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <AlertTriangle className="h-5 w-5 text-primary" />
            <CardTitle className="text-base">Anomaly Detection</CardTitle>
          </div>
          <div className="flex items-center gap-2">
            <Badge variant="outline" className={cn("font-bold", healthColor)}>
              {healthScore}% Health
            </Badge>
            <Button variant="ghost" size="sm" onClick={onRefresh} className="gap-1">
              <RefreshCw className="h-4 w-4" />
              Refresh
            </Button>
          </div>
        </div>
        <CardDescription>
          {summary.total === 0
            ? "No issues detected in your project"
            : `${summary.total} issues detected, ${summary.autoFixableCount} auto-fixable`}
        </CardDescription>
      </CardHeader>

      <Separator />

      {summary.total === 0 ? (
        <CardContent className="py-8">
          <div className="text-center">
            <CheckCircle2 className="h-12 w-12 text-green-600 mx-auto mb-3" />
            <p className="font-medium">All Clear!</p>
            <p className="text-sm text-muted-foreground mt-1">
              No anomalies or issues were detected in your project.
            </p>
          </div>
        </CardContent>
      ) : (
        <>
          {/* Summary cards */}
          <div className="p-4 grid grid-cols-3 gap-3">
            <SeveritySummaryCard
              label="Errors"
              count={summary.bySeverity.error}
              severity="error"
              icon={<AlertCircle className="h-4 w-4" />}
            />
            <SeveritySummaryCard
              label="Warnings"
              count={summary.bySeverity.warning}
              severity="warning"
              icon={<AlertTriangle className="h-4 w-4" />}
            />
            <SeveritySummaryCard
              label="Info"
              count={summary.bySeverity.info}
              severity="info"
              icon={<Info className="h-4 w-4" />}
            />
          </div>

          <Separator />

          {/* Filters */}
          <div className="p-4 flex items-center gap-2 flex-wrap">
            <div className="flex items-center gap-1">
              <Filter className="h-4 w-4 text-muted-foreground" />
              <span className="text-sm text-muted-foreground">Filter:</span>
            </div>
            {(["all", "error", "warning", "info"] as const).map((f) => (
              <Button
                key={f}
                variant={filter === f ? "secondary" : "ghost"}
                size="sm"
                onClick={() => setFilter(f)}
                className="text-xs capitalize"
              >
                {f}
                {f !== "all" && (
                  <Badge variant="outline" className="ml-1 text-xs">
                    {summary.bySeverity[f as keyof typeof summary.bySeverity]}
                  </Badge>
                )}
              </Button>
            ))}

            {autoFixableAnomalies.length > 0 && onFixAll && (
              <Button
                size="sm"
                variant="outline"
                onClick={() => onFixAll(autoFixableAnomalies)}
                className="ml-auto gap-1"
              >
                <Zap className="h-4 w-4" />
                Fix All ({autoFixableAnomalies.length})
              </Button>
            )}
          </div>

          <Separator />

          {/* Anomaly list */}
          <ScrollArea className="max-h-[400px]">
            <div className="p-4 space-y-3">
              {filteredAnomalies.length === 0 ? (
                <div className="text-center py-8 text-muted-foreground">
                  No issues match the current filter
                </div>
              ) : (
                filteredAnomalies.map((anomaly) => (
                  <AnomalyCard
                    key={anomaly.id}
                    anomaly={anomaly}
                    onNavigate={
                      anomaly.hierarchyId && onNavigateToHierarchy
                        ? () => onNavigateToHierarchy(anomaly.hierarchyId!)
                        : undefined
                    }
                    onFix={
                      anomaly.autoFixable && onAutoFix
                        ? () => onAutoFix(anomaly)
                        : undefined
                    }
                  />
                ))
              )}
            </div>
          </ScrollArea>
        </>
      )}
    </Card>
  );
}

// Compact inline alert bar
export function AnomalyAlertBar({
  summary,
  onClick,
}: {
  summary: AnomalySummary;
  onClick?: () => void;
}) {
  if (summary.total === 0) {
    return (
      <div className="flex items-center gap-2 px-3 py-1.5 bg-green-50 border border-green-200 rounded-lg text-sm">
        <CheckCircle2 className="h-4 w-4 text-green-600" />
        <span className="text-green-800">No issues detected</span>
      </div>
    );
  }

  return (
    <button
      onClick={onClick}
      className="flex items-center gap-3 px-3 py-1.5 bg-yellow-50 border border-yellow-200 rounded-lg text-sm hover:bg-yellow-100 transition-colors"
    >
      <AlertTriangle className="h-4 w-4 text-yellow-600" />
      <span className="text-yellow-800">
        {summary.bySeverity.error > 0 && (
          <span className="text-red-700 font-medium">{summary.bySeverity.error} errors</span>
        )}
        {summary.bySeverity.error > 0 && summary.bySeverity.warning > 0 && ", "}
        {summary.bySeverity.warning > 0 && (
          <span>{summary.bySeverity.warning} warnings</span>
        )}
        {(summary.bySeverity.error > 0 || summary.bySeverity.warning > 0) &&
          summary.bySeverity.info > 0 &&
          ", "}
        {summary.bySeverity.info > 0 && (
          <span className="text-blue-700">{summary.bySeverity.info} info</span>
        )}
      </span>
      <ChevronRight className="h-4 w-4 text-yellow-600 ml-auto" />
    </button>
  );
}
