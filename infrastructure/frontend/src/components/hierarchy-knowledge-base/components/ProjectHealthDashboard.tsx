/**
 * Project Health Dashboard
 * Shows overall project metrics, completion status, and recent activity
 */
import { useMemo } from "react";
import { cn } from "@/lib/utils";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Button } from "@/components/ui/button";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  GitBranch,
  Database,
  Calculator,
  AlertTriangle,
  CheckCircle2,
  Clock,
  TrendingUp,
  Layers,
  FileCode,
  ArrowUpRight,
  Calendar,
  User,
  RefreshCw,
} from "lucide-react";

interface ProjectStats {
  totalHierarchies: number;
  totalMappings: number;
  totalFormulas: number;
  mappedHierarchies: number;
  unmappedLeafNodes: number;
  validationErrors: number;
  lastDeployment?: {
    date: Date;
    status: "success" | "partial" | "failed";
    hierarchyCount: number;
  };
}

interface RecentActivity {
  id: string;
  type: "create" | "update" | "delete" | "deploy" | "import";
  description: string;
  timestamp: Date;
  user?: string;
}

interface ValidationIssue {
  id: string;
  severity: "error" | "warning" | "info";
  message: string;
  hierarchyId?: string;
  hierarchyName?: string;
}

interface ProjectHealthDashboardProps {
  projectName: string;
  stats: ProjectStats;
  recentActivities?: RecentActivity[];
  validationIssues?: ValidationIssue[];
  onRefresh?: () => void;
  onNavigateToHierarchy?: (id: string) => void;
  isLoading?: boolean;
  className?: string;
}

function StatCard({
  icon: Icon,
  label,
  value,
  description,
  trend,
  color = "default",
}: {
  icon: React.ElementType;
  label: string;
  value: number | string;
  description?: string;
  trend?: { value: number; positive: boolean };
  color?: "default" | "success" | "warning" | "error";
}) {
  const colorClasses = {
    default: "text-primary",
    success: "text-green-600",
    warning: "text-yellow-600",
    error: "text-red-600",
  };

  return (
    <Card>
      <CardContent className="p-4">
        <div className="flex items-start justify-between">
          <div className="space-y-1">
            <p className="text-sm text-muted-foreground">{label}</p>
            <div className="flex items-baseline gap-2">
              <p className={cn("text-2xl font-bold", colorClasses[color])}>
                {value}
              </p>
              {trend && (
                <span
                  className={cn(
                    "text-xs flex items-center",
                    trend.positive ? "text-green-600" : "text-red-600"
                  )}
                >
                  <TrendingUp
                    className={cn("h-3 w-3 mr-0.5", !trend.positive && "rotate-180")}
                  />
                  {trend.value}%
                </span>
              )}
            </div>
            {description && (
              <p className="text-xs text-muted-foreground">{description}</p>
            )}
          </div>
          <div className={cn("p-2 rounded-lg bg-muted", colorClasses[color])}>
            <Icon className="h-5 w-5" />
          </div>
        </div>
      </CardContent>
    </Card>
  );
}

function ActivityItem({ activity }: { activity: RecentActivity }) {
  const icons = {
    create: <Layers className="h-4 w-4 text-green-600" />,
    update: <RefreshCw className="h-4 w-4 text-blue-600" />,
    delete: <AlertTriangle className="h-4 w-4 text-red-600" />,
    deploy: <ArrowUpRight className="h-4 w-4 text-purple-600" />,
    import: <FileCode className="h-4 w-4 text-orange-600" />,
  };

  const timeAgo = formatTimeAgo(activity.timestamp);

  return (
    <div className="flex items-start gap-3 py-2">
      <div className="mt-0.5">{icons[activity.type]}</div>
      <div className="flex-1 min-w-0">
        <p className="text-sm">{activity.description}</p>
        <div className="flex items-center gap-2 mt-1 text-xs text-muted-foreground">
          <Clock className="h-3 w-3" />
          <span>{timeAgo}</span>
          {activity.user && (
            <>
              <span>·</span>
              <User className="h-3 w-3" />
              <span>{activity.user}</span>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

function ValidationIssueItem({
  issue,
  onNavigate,
}: {
  issue: ValidationIssue;
  onNavigate?: (id: string) => void;
}) {
  const icons = {
    error: <AlertTriangle className="h-4 w-4 text-red-600" />,
    warning: <AlertTriangle className="h-4 w-4 text-yellow-600" />,
    info: <CheckCircle2 className="h-4 w-4 text-blue-600" />,
  };

  const bgColors = {
    error: "bg-red-50 border-red-200",
    warning: "bg-yellow-50 border-yellow-200",
    info: "bg-blue-50 border-blue-200",
  };

  return (
    <div
      className={cn(
        "flex items-start gap-3 p-3 rounded-lg border",
        bgColors[issue.severity],
        issue.hierarchyId && onNavigate && "cursor-pointer hover:opacity-80"
      )}
      onClick={() => issue.hierarchyId && onNavigate?.(issue.hierarchyId)}
    >
      <div className="mt-0.5">{icons[issue.severity]}</div>
      <div className="flex-1 min-w-0">
        <p className="text-sm">{issue.message}</p>
        {issue.hierarchyName && (
          <p className="text-xs text-muted-foreground mt-1">
            in {issue.hierarchyName}
          </p>
        )}
      </div>
    </div>
  );
}

function formatTimeAgo(date: Date): string {
  const now = new Date();
  const diff = now.getTime() - date.getTime();
  const minutes = Math.floor(diff / 60000);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);

  if (minutes < 1) return "Just now";
  if (minutes < 60) return `${minutes}m ago`;
  if (hours < 24) return `${hours}h ago`;
  if (days < 7) return `${days}d ago`;
  return date.toLocaleDateString();
}

export function ProjectHealthDashboard({
  projectName,
  stats,
  recentActivities = [],
  validationIssues = [],
  onRefresh,
  onNavigateToHierarchy,
  isLoading,
  className,
}: ProjectHealthDashboardProps) {
  const coveragePercentage = useMemo(() => {
    if (stats.totalHierarchies === 0) return 0;
    return Math.round((stats.mappedHierarchies / stats.totalHierarchies) * 100);
  }, [stats.mappedHierarchies, stats.totalHierarchies]);

  const healthScore = useMemo(() => {
    let score = 100;
    // Deduct for unmapped nodes
    if (stats.unmappedLeafNodes > 0) {
      score -= Math.min(stats.unmappedLeafNodes * 5, 30);
    }
    // Deduct for validation errors
    score -= Math.min(stats.validationErrors * 10, 40);
    return Math.max(score, 0);
  }, [stats.unmappedLeafNodes, stats.validationErrors]);

  const healthColor = healthScore >= 80 ? "success" : healthScore >= 50 ? "warning" : "error";

  return (
    <div className={cn("space-y-6", className)}>
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-bold">{projectName}</h2>
          <p className="text-muted-foreground">Project Health Dashboard</p>
        </div>
        <div className="flex items-center gap-2">
          {onRefresh && (
            <Button variant="outline" size="sm" onClick={onRefresh} disabled={isLoading}>
              <RefreshCw className={cn("h-4 w-4 mr-2", isLoading && "animate-spin")} />
              Refresh
            </Button>
          )}
        </div>
      </div>

      {/* Health Score */}
      <Card>
        <CardContent className="p-6">
          <div className="flex items-center justify-between mb-4">
            <div>
              <h3 className="font-semibold">Overall Health Score</h3>
              <p className="text-sm text-muted-foreground">
                Based on mapping coverage and validation status
              </p>
            </div>
            <Badge
              variant="outline"
              className={cn(
                "text-lg font-bold px-4 py-1",
                healthColor === "success" && "border-green-500 text-green-700",
                healthColor === "warning" && "border-yellow-500 text-yellow-700",
                healthColor === "error" && "border-red-500 text-red-700"
              )}
            >
              {healthScore}%
            </Badge>
          </div>
          <Progress
            value={healthScore}
            className={cn(
              "h-3",
              healthColor === "success" && "[&>div]:bg-green-500",
              healthColor === "warning" && "[&>div]:bg-yellow-500",
              healthColor === "error" && "[&>div]:bg-red-500"
            )}
          />
        </CardContent>
      </Card>

      {/* Stats Grid */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard
          icon={GitBranch}
          label="Hierarchies"
          value={stats.totalHierarchies}
          description={`${coveragePercentage}% mapped`}
        />
        <StatCard
          icon={Database}
          label="Mappings"
          value={stats.totalMappings}
          color="success"
        />
        <StatCard
          icon={Calculator}
          label="Formulas"
          value={stats.totalFormulas}
          color="default"
        />
        <StatCard
          icon={AlertTriangle}
          label="Issues"
          value={stats.validationErrors + stats.unmappedLeafNodes}
          color={stats.validationErrors > 0 ? "error" : stats.unmappedLeafNodes > 0 ? "warning" : "success"}
        />
      </div>

      {/* Two column layout */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Validation Issues */}
        <Card>
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle className="text-lg">Validation Issues</CardTitle>
              <Badge variant="secondary">{validationIssues.length}</Badge>
            </div>
            <CardDescription>
              Issues that need attention before deployment
            </CardDescription>
          </CardHeader>
          <CardContent>
            <ScrollArea className="h-[200px]">
              {validationIssues.length > 0 ? (
                <div className="space-y-2">
                  {validationIssues.map((issue) => (
                    <ValidationIssueItem
                      key={issue.id}
                      issue={issue}
                      onNavigate={onNavigateToHierarchy}
                    />
                  ))}
                </div>
              ) : (
                <div className="flex flex-col items-center justify-center h-full text-center">
                  <CheckCircle2 className="h-8 w-8 text-green-600 mb-2" />
                  <p className="text-sm font-medium">All clear!</p>
                  <p className="text-xs text-muted-foreground">
                    No validation issues found
                  </p>
                </div>
              )}
            </ScrollArea>
          </CardContent>
        </Card>

        {/* Recent Activity */}
        <Card>
          <CardHeader className="pb-3">
            <div className="flex items-center justify-between">
              <CardTitle className="text-lg">Recent Activity</CardTitle>
              <Badge variant="secondary">{recentActivities.length}</Badge>
            </div>
            <CardDescription>Latest changes to the project</CardDescription>
          </CardHeader>
          <CardContent>
            <ScrollArea className="h-[200px]">
              {recentActivities.length > 0 ? (
                <div className="divide-y">
                  {recentActivities.map((activity) => (
                    <ActivityItem key={activity.id} activity={activity} />
                  ))}
                </div>
              ) : (
                <div className="flex flex-col items-center justify-center h-full text-center">
                  <Clock className="h-8 w-8 text-muted-foreground mb-2" />
                  <p className="text-sm font-medium">No recent activity</p>
                  <p className="text-xs text-muted-foreground">
                    Changes will appear here
                  </p>
                </div>
              )}
            </ScrollArea>
          </CardContent>
        </Card>
      </div>

      {/* Last Deployment */}
      {stats.lastDeployment && (
        <Card>
          <CardContent className="p-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <div
                  className={cn(
                    "p-2 rounded-lg",
                    stats.lastDeployment.status === "success" && "bg-green-100",
                    stats.lastDeployment.status === "partial" && "bg-yellow-100",
                    stats.lastDeployment.status === "failed" && "bg-red-100"
                  )}
                >
                  <ArrowUpRight
                    className={cn(
                      "h-5 w-5",
                      stats.lastDeployment.status === "success" && "text-green-600",
                      stats.lastDeployment.status === "partial" && "text-yellow-600",
                      stats.lastDeployment.status === "failed" && "text-red-600"
                    )}
                  />
                </div>
                <div>
                  <p className="font-medium">Last Deployment</p>
                  <p className="text-sm text-muted-foreground">
                    {stats.lastDeployment.hierarchyCount} hierarchies deployed
                  </p>
                </div>
              </div>
              <div className="text-right">
                <Badge
                  variant={
                    stats.lastDeployment.status === "success"
                      ? "default"
                      : stats.lastDeployment.status === "partial"
                      ? "secondary"
                      : "destructive"
                  }
                >
                  {stats.lastDeployment.status}
                </Badge>
                <p className="text-xs text-muted-foreground mt-1">
                  <Calendar className="h-3 w-3 inline mr-1" />
                  {stats.lastDeployment.date.toLocaleDateString()}
                </p>
              </div>
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
}
