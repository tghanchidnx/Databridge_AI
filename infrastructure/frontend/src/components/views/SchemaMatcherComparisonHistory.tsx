import { Clock, Trash, CaretRight } from "@phosphor-icons/react";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { ScrollArea } from "@/components/ui/scroll-area";
import type { SchemaComparison } from "@/types";
import { toast } from "sonner";

interface ComparisonHistoryProps {
  comparisons: SchemaComparison[];
  onLoadComparison: (comparison: SchemaComparison) => void;
}

export function ComparisonHistory({
  comparisons,
  onLoadComparison,
}: ComparisonHistoryProps) {
  const formatDate = (dateString: string) => {
    const date = new Date(dateString);
    return new Intl.DateTimeFormat("en-US", {
      month: "short",
      day: "numeric",
      year: "numeric",
      hour: "numeric",
      minute: "numeric",
    }).format(date);
  };

  const getStatusColor = (status: string) => {
    const colors = {
      completed: "bg-green-500",
      pending: "bg-yellow-500",
      running: "bg-blue-500",
      failed: "bg-red-500",
    };
    return colors[status as keyof typeof colors] || "bg-gray-500";
  };

  if (comparisons.length === 0) {
    return (
      <Card className="p-12 text-center">
        <Clock
          className="h-12 w-12 mx-auto text-muted-foreground mb-4"
          weight="light"
        />
        <h3 className="text-lg font-semibold mb-2">No Comparison History</h3>
        <p className="text-muted-foreground">
          Your comparison history will appear here once you run your first
          comparison.
        </p>
      </Card>
    );
  }

  return (
    <Card className="p-6">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h3 className="text-lg font-semibold">Comparison History</h3>
          <p className="text-sm text-muted-foreground mt-1">
            Load previous comparisons or start a new one
          </p>
        </div>
        <Badge variant="secondary">{comparisons.length} saved</Badge>
      </div>

      <ScrollArea className="h-[400px] pr-4">
        <div className="space-y-3">
          {comparisons
            .sort(
              (a, b) =>
                new Date(b.createdAt).getTime() -
                new Date(a.createdAt).getTime()
            )
            .map((comparison) => (
              <Card
                key={comparison.id}
                className="p-4 hover:bg-accent/50 cursor-pointer transition-colors"
                onClick={() => onLoadComparison(comparison)}
              >
                <div className="flex items-center gap-3">
                  <div
                    className={`h-2 w-2 rounded-full ${getStatusColor(
                      comparison.status
                    )}`}
                  />

                  <div className="flex-1 min-w-0">
                    <div className="font-semibold truncate">
                      {comparison.name}
                    </div>
                    <div className="flex items-center gap-2 mt-1 text-xs text-muted-foreground">
                      <span>{comparison.source.connectionName}</span>
                      <CaretRight className="h-3 w-3" />
                      <span>{comparison.target.connectionName}</span>
                      <span>•</span>
                      <span>{formatDate(comparison.createdAt)}</span>
                    </div>
                  </div>

                  {comparison.results?.summary && (
                    <div className="flex items-center gap-2">
                      {typeof comparison.results.summary.matched !==
                        "undefined" && (
                        <Badge
                          variant="outline"
                          className="bg-green-500/10 text-green-500 border-green-500/20"
                        >
                          {comparison.results.summary.matched}
                        </Badge>
                      )}
                      {typeof comparison.results.summary.modified !==
                        "undefined" && (
                        <Badge
                          variant="outline"
                          className="bg-yellow-500/10 text-yellow-500 border-yellow-500/20"
                        >
                          {comparison.results.summary.modified}
                        </Badge>
                      )}
                      {typeof comparison.results.summary.sourceOnly !==
                        "undefined" && (
                        <Badge
                          variant="outline"
                          className="bg-red-500/10 text-red-500 border-red-500/20"
                        >
                          {comparison.results.summary.sourceOnly}
                        </Badge>
                      )}
                      {typeof comparison.results.summary.targetOnly !==
                        "undefined" && (
                        <Badge
                          variant="outline"
                          className="bg-blue-500/10 text-blue-500 border-blue-500/20"
                        >
                          {comparison.results.summary.targetOnly}
                        </Badge>
                      )}
                    </div>
                  )}

                  <Badge variant="secondary" className="capitalize">
                    {comparison.status}
                  </Badge>
                </div>
              </Card>
            ))}
        </div>
      </ScrollArea>
    </Card>
  );
}
