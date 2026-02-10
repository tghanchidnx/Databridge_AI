import React, { useState, useEffect } from "react";
import { smartHierarchyService } from "@/services/api/hierarchy";
import { Loader2, Network, Users, Link2, AlertCircle } from "lucide-react";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Alert, AlertDescription } from "@/components/ui/alert";

interface HierarchyDependenciesProps {
  projectId: string;
  hierarchyId: string;
}

export const HierarchyDependencies: React.FC<HierarchyDependenciesProps> = ({
  projectId,
  hierarchyId,
}) => {
  const [loading, setLoading] = useState(true);
  const [dependencies, setDependencies] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    loadDependencies();
  }, [projectId, hierarchyId]);

  const loadDependencies = async () => {
    try {
      setLoading(true);
      setError(null);
      const deps = await smartHierarchyService.getDependencies(
        projectId,
        hierarchyId
      );
      setDependencies(deps);
    } catch (err: any) {
      console.error("Failed to load dependencies:", err);
      setError(err.response?.data?.message || "Failed to load dependencies");
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <div className="text-center">
          <Loader2 className="w-8 h-8 animate-spin mx-auto mb-3 text-primary" />
          <p className="text-sm text-muted-foreground">
            Loading dependencies...
          </p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <Alert variant="destructive">
        <AlertCircle className="h-4 w-4" />
        <AlertDescription>{error}</AlertDescription>
      </Alert>
    );
  }

  if (!dependencies) {
    return null;
  }

  return (
    <div className="space-y-6">
      {/* Hierarchy Tree Relationships */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Network className="w-5 h-5" />
            Hierarchy Tree
          </CardTitle>
          <CardDescription>
            Parent-child relationships in the hierarchy structure
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {/* Parent */}
          {dependencies.parent ? (
            <div className="space-y-2">
              <h4 className="text-sm font-medium">Parent Hierarchy</h4>
              <div className="p-4 bg-card shadow-md rounded-lg ring-1 ring-border/50">
                <div className="flex items-center gap-2">
                  <Badge variant="secondary">Parent</Badge>
                  <span className="font-medium">
                    {dependencies.parent.hierarchyName}
                  </span>
                </div>
                <p className="text-sm text-muted-foreground mt-1">
                  ID: {dependencies.parent.hierarchyId}
                </p>
              </div>
            </div>
          ) : (
            <div className="text-sm text-muted-foreground">
              <Badge variant="outline">Root</Badge>
              <span className="ml-2">This is a root hierarchy (no parent)</span>
            </div>
          )}

          {/* Children */}
          {dependencies.children && dependencies.children.length > 0 && (
            <div className="space-y-2">
              <h4 className="text-sm font-medium">
                Child Hierarchies ({dependencies.children.length})
              </h4>
              <div className="space-y-2">
                {dependencies.children.map((child: any, idx: number) => (
                  <div
                    key={idx}
                    className="p-4 bg-card shadow-md rounded-lg ring-1 ring-border/50"
                  >
                    <div className="flex items-center gap-2">
                      <Badge variant="secondary">Child</Badge>
                      <span className="font-medium">{child.hierarchyName}</span>
                    </div>
                    <p className="text-sm text-muted-foreground mt-1">
                      ID: {child.hierarchyId}
                    </p>
                  </div>
                ))}
              </div>
            </div>
          )}

          {!dependencies.parent &&
            (!dependencies.children || dependencies.children.length === 0) && (
              <div className="text-center py-6 text-muted-foreground">
                <Network className="w-12 h-12 mx-auto mb-2 opacity-20" />
                <p className="text-sm">No parent-child relationships</p>
              </div>
            )}
        </CardContent>
      </Card>

      {/* Data Source Dependencies */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Link2 className="w-5 h-5" />
            Data Source Dependencies
          </CardTitle>
          <CardDescription>
            Other hierarchies sharing the same data sources
          </CardDescription>
        </CardHeader>
        <CardContent>
          {dependencies.dependencies && dependencies.dependencies.length > 0 ? (
            <div className="space-y-4">
              {dependencies.dependencies.map((dep: any, idx: number) => (
                <div key={idx} className="space-y-2">
                  <div className="flex items-center gap-2">
                    <Badge variant="outline">{dep.source}</Badge>
                    <span className="text-sm text-muted-foreground">
                      {dep.relatedHierarchies.length} related{" "}
                      {dep.relatedHierarchies.length === 1
                        ? "hierarchy"
                        : "hierarchies"}
                    </span>
                  </div>
                  <div className="ml-6 space-y-1">
                    {dep.relatedHierarchies.map((rel: any, relIdx: number) => (
                      <div
                        key={relIdx}
                        className="p-2 bg-primary/5 rounded-md pl-3"
                      >
                        <p className="font-medium text-sm">
                          {rel.hierarchyName}
                        </p>
                        <p className="text-xs text-muted-foreground">
                          {rel.hierarchyId}
                        </p>
                      </div>
                    ))}
                  </div>
                </div>
              ))}
            </div>
          ) : (
            <div className="text-center py-6 text-muted-foreground">
              <Link2 className="w-12 h-12 mx-auto mb-2 opacity-20" />
              <p className="text-sm">
                No shared data sources with other hierarchies
              </p>
            </div>
          )}
        </CardContent>
      </Card>

      {/* Summary */}
      <Card>
        <CardHeader>
          <CardTitle className="flex items-center gap-2">
            <Users className="w-5 h-5" />
            Summary
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-3 gap-4">
            <div className="text-center p-4 bg-card shadow-sm rounded-lg">
              <div className="text-2xl font-bold">
                {dependencies.parent ? 1 : 0}
              </div>
              <div className="text-sm text-muted-foreground">Parent</div>
            </div>
            <div className="text-center p-4 bg-card shadow-md rounded-lg ring-1 ring-border/50">
              <div className="text-2xl font-bold">
                {dependencies.children?.length || 0}
              </div>
              <div className="text-sm text-muted-foreground">Children</div>
            </div>
            <div className="text-center p-4 bg-card shadow-md rounded-lg ring-1 ring-border/50">
              <div className="text-2xl font-bold">
                {dependencies.dependencyCount || 0}
              </div>
              <div className="text-sm text-muted-foreground">
                Source Dependencies
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    </div>
  );
};
