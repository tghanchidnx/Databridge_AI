import {
  ChartBar,
  CheckCircle,
  Database,
  Lightning,
  TrendUp as TrendUpIcon,
  UsersThree,
  Warning,
  ChartLine,
} from "@phosphor-icons/react";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { useAuth } from "@/contexts/AuthContext";
import { dashboardAPI } from "@/lib/api";
import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";

interface DashboardStats {
  activeConnections: { count: number; total: number };
  hierarchyProjects: { count: number; change: string };
  schemaComparisons: { count: number; change: string };
  weeklyComparisons: { count: number; change: string };
  teamMembers: { count: number; change: string };
}

interface Activity {
  action: string;
  project: string;
  user: string;
  time: string;
  status: "success" | "warning";
}

interface Connection {
  id: string;
  name: string;
  type: string;
  status: string;
  host: string;
  lastUsed: string;
}

export function Dashboard() {
  const navigate = useNavigate();
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [activities, setActivities] = useState<Activity[]>([]);
  const [connections, setConnections] = useState<Connection[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const loadDashboardData = async () => {
      try {
        // Load dashboard stats from API
        const statsResponse = (await dashboardAPI.getStats()) as any;
        const statsData = statsResponse.data || statsResponse;
        setStats(statsData as DashboardStats);

        // Load recent activities from API
        const activitiesResponse = (await dashboardAPI.getActivities(4)) as any;
        const activitiesData = activitiesResponse.data || activitiesResponse;
        setActivities(activitiesData as Activity[]);

        // Load recent connections from API
        const connectionsResponse = (await dashboardAPI.getConnections(
          4
        )) as any;
        const connectionsData = connectionsResponse.data || connectionsResponse;
        setConnections(connectionsData as Connection[]);
      } catch (error) {
        // Use fallback data if API fails
        setStats({
          activeConnections: { count: 0, total: 0 },
          hierarchyProjects: { count: 0, change: "0 total" },
          schemaComparisons: { count: 0, change: "+0 this month" },
          weeklyComparisons: { count: 0, change: "+0 this week" },
          teamMembers: { count: 0, change: "0 active now" },
        });
      } finally {
        setLoading(false);
      }
    };

    loadDashboardData();
  }, []);

  // Use stats from API or fallback
  const displayStats = stats
    ? [
        {
          label: "Active Connections",
          value: stats.activeConnections?.count?.toString() || "0",
          change: `${stats.activeConnections?.total || 0} total`,
          icon: Database,
          trend: "up" as const,
        },
        {
          label: "Hierarchy Projects",
          value: stats.hierarchyProjects?.count?.toString() || "0",
          change: stats.hierarchyProjects?.change || "0 total",
          icon: ChartLine,
          trend: "up" as const,
        },
        {
          label: "Schema Comparisons",
          value: stats.schemaComparisons?.count?.toString() || "0",
          change: stats.schemaComparisons?.change || "+0 this month",
          icon: ChartBar,
          trend: "up" as const,
        },
        {
          label: "Team Members",
          value: stats.teamMembers?.count?.toString() || "0",
          change: stats.teamMembers?.change || "0 active now",
          icon: UsersThree,
          trend: "neutral" as const,
        },
      ]
    : [];

  // Use real activities from API or fallback
  const displayActivities =
    activities.length > 0
      ? activities
      : [
          {
            action: "Project created",
            project: "New Hierarchy Project",
            user: "You",
            time: "Just now",
            status: "success" as const,
          },
        ];

  return (
    <div className="p-6 space-y-6">
      <div>
        <h1 className="text-2xl lg:text-3xl font-bold tracking-tight">
          Dashboard
        </h1>
        <p className="text-muted-foreground mt-2">
          Welcome back! Here's what's happening with your workspace.
        </p>
      </div>

      {loading ? (
        <div className="flex items-center justify-center py-12">
          <div className="text-muted-foreground">Loading dashboard...</div>
        </div>
      ) : (
        <>
          <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-4 lg:gap-6">
            {displayStats.map((stat) => {
              const Icon = stat.icon;
              return (
                <Card
                  key={stat.label}
                  className="border-border shadow-sm hover:shadow-md transition-shadow"
                >
                  <CardHeader className="flex flex-row items-center justify-between space-y-0 pb-2">
                    <CardTitle className="text-sm font-medium">
                      {stat.label}
                    </CardTitle>
                    <Icon className="h-5 w-5 text-muted-foreground" />
                  </CardHeader>
                  <CardContent>
                    <div className="text-2xl lg:text-3xl font-bold">
                      {stat.value}
                    </div>
                    <p className="mt-1 text-xs text-muted-foreground flex items-center gap-1">
                      {stat.trend === "up" && (
                        <TrendUpIcon className="h-3 w-3 text-primary" />
                      )}
                      {stat.change}
                    </p>
                  </CardContent>
                </Card>
              );
            })}
          </div>

          <div className="grid grid-cols-1 gap-4 lg:grid-cols-2 lg:gap-6">
            <Card className="border-border shadow-sm">
              <CardHeader>
                <div className="flex items-center justify-between">
                  <div>
                    <CardTitle>Recent Activity</CardTitle>
                    <CardDescription>
                      Latest workspace actions and updates
                    </CardDescription>
                  </div>
                  <Button variant="ghost" size="sm">
                    View All
                  </Button>
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  {displayActivities.map((activity, index) => (
                    <div key={index}>
                      <div className="flex items-start gap-4">
                        <div className="flex h-8 w-8 items-center justify-center rounded-full bg-primary/10 border border-primary/20 mt-1 shrink-0">
                          {activity.status === "success" && (
                            <CheckCircle
                              weight="fill"
                              className="h-4 w-4 text-primary"
                            />
                          )}
                          {activity.status === "warning" && (
                            <Warning
                              weight="fill"
                              className="h-4 w-4 text-yellow-500"
                            />
                          )}
                        </div>
                        <div className="flex-1 space-y-1 min-w-0">
                          <p className="text-sm font-medium">
                            {activity.action}
                          </p>
                          <p className="text-sm text-muted-foreground truncate">
                            {activity.project} • {activity.user}
                          </p>
                          <p className="text-xs text-muted-foreground">
                            {activity.time}
                          </p>
                        </div>
                      </div>
                      {index < displayActivities.length - 1 && (
                        <Separator className="mt-4" />
                      )}
                    </div>
                  ))}
                </div>
              </CardContent>
            </Card>

            <Card className="border-border shadow-sm">
              <CardHeader>
                <div className="flex items-center justify-between">
                  <div>
                    <CardTitle>Database Connections</CardTitle>
                    <CardDescription>
                      Active and configured connections
                    </CardDescription>
                  </div>
                  <Button
                    size="sm"
                    className="gap-2"
                    onClick={() => navigate("/connections")}
                  >
                    <Lightning weight="fill" className="h-4 w-4" />
                    <span className="hidden sm:inline">Add Connection</span>
                  </Button>
                </div>
              </CardHeader>
              <CardContent>
                <div className="space-y-4">
                  {connections.length > 0 ? (
                    connections.map((conn, index) => (
                      <div key={conn.id}>
                        <div className="flex items-center justify-between gap-3">
                          <div className="flex items-center gap-3 min-w-0 flex-1">
                            <div className="flex h-10 w-10 items-center justify-center rounded-md bg-primary/10 border border-primary/20 shrink-0">
                              <Database className="h-5 w-5 text-primary" />
                            </div>
                            <div className="min-w-0 flex-1">
                              <p className="text-sm font-medium truncate">
                                {conn.name}
                              </p>
                              <p className="text-xs text-muted-foreground capitalize">
                                {conn.type}
                              </p>
                            </div>
                          </div>
                          <Badge
                            variant={
                              conn.status === "connected"
                                ? "default"
                                : "destructive"
                            }
                            className="text-xs"
                          >
                            {conn.status}
                          </Badge>
                        </div>
                        {index < connections.length - 1 && (
                          <Separator className="mt-4" />
                        )}
                      </div>
                    ))
                  ) : (
                    <div className="text-center py-8">
                      <Database className="h-12 w-12 mx-auto text-muted-foreground mb-3" />
                      <p className="text-sm text-muted-foreground mb-4">
                        No connections yet
                      </p>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => navigate("/connections")}
                      >
                        Add Your First Connection
                      </Button>
                    </div>
                  )}
                  {connections.length > 0 && (
                    <div className="pt-2">
                      <Button
                        variant="ghost"
                        size="sm"
                        className="w-full"
                        onClick={() => navigate("/connections")}
                      >
                        View All Connections
                      </Button>
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>
          </div>

          <Card className="border-border shadow-sm">
            <CardHeader>
              <CardTitle>Quick Actions</CardTitle>
              <CardDescription>Common tasks to get you started</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-2 gap-3 sm:grid-cols-2 lg:grid-cols-4 lg:gap-4">
                <Button
                  variant="outline"
                  className="h-auto flex-col gap-2 py-4 border-border hover:border-primary/50 hover:bg-primary/5"
                >
                  <ChartLine className="h-6 w-6" />
                  <span className="text-xs lg:text-sm font-medium">
                    Compare Schemas
                  </span>
                </Button>
                <Button
                  variant="outline"
                  className="h-auto flex-col gap-2 py-4 border-border hover:border-primary/50 hover:bg-primary/5"
                >
                  <ChartBar className="h-6 w-6" />
                  <span className="text-xs lg:text-sm font-medium">
                    Hierarchy Projects
                  </span>
                </Button>
                <Button
                  variant="outline"
                  className="h-auto flex-col gap-2 py-4 border-border hover:border-primary/50 hover:bg-primary/5"
                >
                  <Database className="h-6 w-6" />
                  <span className="text-xs lg:text-sm font-medium">
                    Add Connection
                  </span>
                </Button>
                <Button
                  variant="outline"
                  className="h-auto flex-col gap-2 py-4 border-border hover:border-primary/50 hover:bg-primary/5"
                >
                  <Lightning className="h-6 w-6" />
                  <span className="text-xs lg:text-sm font-medium">
                    Query Builder
                  </span>
                </Button>
              </div>
            </CardContent>
          </Card>
        </>
      )}
    </div>
  );
}
