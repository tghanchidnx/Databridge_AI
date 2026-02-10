import { Eye, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Badge } from "@/components/ui/badge";

interface DeploymentLogsTabProps {
  deployments: any[];
  loading: boolean;
  onViewScript: (deployment: any) => void;
}

export function DeploymentLogsTab({
  deployments,
  loading,
  onViewScript,
}: DeploymentLogsTabProps) {
  const formatDate = (dateString: string) => {
    return new Date(dateString).toLocaleString("en-US", {
      year: "numeric",
      month: "short",
      day: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center py-8">
        <Loader2 className="h-6 w-6 animate-spin" />
        <span className="ml-2">Loading deployment history...</span>
      </div>
    );
  }

  if (deployments.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-12 text-muted-foreground">
        <p className="text-lg">No deployments yet</p>
        <p className="text-sm mt-2">
          Deploy hierarchies to Snowflake to see deployment history here
        </p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="rounded-md ">
        <Table>
          <TableHeader className="bg-muted">
            <TableRow>
              <TableHead>Deployed At</TableHead>
              <TableHead>Deployed By</TableHead>
              <TableHead>Database</TableHead>
              <TableHead>Status</TableHead>
              <TableHead className="text-right">Hierarchies</TableHead>
              <TableHead className="text-right">Success/Failed</TableHead>
              <TableHead className="text-center">Actions</TableHead>
            </TableRow>
          </TableHeader>
          <TableBody className="overflow-y-auto">
            {deployments.map((deployment) => (
              <TableRow key={deployment.id}>
                <TableCell className="font-medium">
                  {formatDate(deployment.deployedAt)}
                </TableCell>
                <TableCell>{deployment.deployedBy}</TableCell>
                <TableCell>
                  <div className="text-sm">
                    <div className="font-medium">
                      {deployment.database}.{deployment.schema}
                    </div>
                    <div className="text-muted-foreground">
                      {deployment.databaseType}
                    </div>
                  </div>
                </TableCell>
                <TableCell>
                  <Badge
                    variant={
                      deployment.status === "success"
                        ? "default"
                        : deployment.status === "partial"
                        ? "secondary"
                        : "destructive"
                    }
                  >
                    {deployment.status}
                  </Badge>
                </TableCell>
                <TableCell className="text-right">
                  {deployment.hierarchyIds?.length || 0}
                </TableCell>
                <TableCell className="text-right">
                  <span className="text-green-600">
                    {deployment.successCount || 0}
                  </span>
                  <span className="mx-1">/</span>
                  <span className="text-red-600">
                    {deployment.failedCount || 0}
                  </span>
                </TableCell>
                <TableCell className="text-center">
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => onViewScript(deployment)}
                  >
                    <Eye className="h-4 w-4" />
                  </Button>
                </TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </div>

      <div className="text-sm text-muted-foreground">
        Showing {deployments.length} deployment
        {deployments.length !== 1 ? "s" : ""}
      </div>
    </div>
  );
}
