import { useState, useEffect } from "react";
import {
  Play,
  FloppyDisk,
  ClockCounterClockwise,
  Gear,
  FileCode,
  CircleNotch,
} from "@phosphor-icons/react";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { ComparisonSetup } from "@/components/views/SchemaMatcherComparisonSetup";
import { ComparisonResults } from "@/components/views/SchemaMatcherComparisonResults";
import { DependencyGraph } from "@/components/views/SchemaMatcherDependencyGraph";
import { SchemaMatcherLineageGraph } from "@/components/views/SchemaMatcherLineageGraph";
import { ScriptGenerator } from "@/components/views/SchemaMatcherScriptGenerator";
import { ComparisonHistory } from "@/components/views/SchemaMatcherComparisonHistory";
import type { SchemaComparison, Connection } from "@/types";
import { toast } from "sonner";
import { apiService } from "@/lib/api-service";
import { useAuth } from "@/contexts/AuthContext";
import { useConnectionsStore } from "@/stores/connections.store";

export function SchemaMatcherView() {
  const { user } = useAuth();
  const {
    connections,
    loading: connectionsLoading,
    loadConnections,
  } = useConnectionsStore();
  const [comparisons, setComparisons] = useState<SchemaComparison[]>([]);
  const [currentComparison, setCurrentComparison] =
    useState<SchemaComparison | null>(null);
  const [activeTab, setActiveTab] = useState<
    "setup" | "results" | "dependencies" | "script"
  >("setup");
  const [isComparing, setIsComparing] = useState(false);
  const [isGeneratingScript, setIsGeneratingScript] = useState(false);
  const [isLoadingHistory, setIsLoadingHistory] = useState(false);

  useEffect(() => {
    loadConnections();
    loadComparisonHistory();
  }, [loadConnections]);

  const loadComparisonHistory = async () => {
    setIsLoadingHistory(true);
    try {
      const jobs = await apiService.getComparisonJobs();
      const mappedComparisons = jobs.map((job: any) => ({
        id: job.id,
        workspaceId: "current-workspace",
        name: `${job.sourceDatabase}.${job.sourceSchema} → ${job.targetDatabase}.${job.targetSchema}`,
        source: {
          connectionId: job.sourceConnectionId,
          connectionName: job.sourceConnectionId,
          database: job.sourceDatabase,
          schema: job.sourceSchema,
        },
        target: {
          connectionId: job.targetConnectionId,
          connectionName: job.targetConnectionId,
          database: job.targetDatabase,
          schema: job.targetSchema,
        },
        status: job.status.toLowerCase(),
        progress: job.status === "COMPLETED" ? 100 : 0,
        createdBy: job.userId,
        createdAt: job.createdAt,
        completedAt: job.completedAt,
        results: job.result,
      }));
      setComparisons(mappedComparisons);
    } catch (error: any) {
      console.error("Failed to load comparison history:", error);
    } finally {
      setIsLoadingHistory(false);
    }
  };

  const handleStartComparison = async (comparison: SchemaComparison) => {
    setIsComparing(true);
    toast.info("Starting schema comparison...");

    const updatedComparison = {
      ...comparison,
      status: "running" as const,
      progress: 0,
    };

    setCurrentComparison(updatedComparison);

    try {
      // Prepare request payload
      const requestPayload = {
        sourceConnectionId: comparison.source.connectionId,
        targetConnectionId: comparison.target.connectionId,
        sourceDatabase: comparison.source.database,
        targetDatabase: comparison.target.database,
        sourceSchema: comparison.source.schema || "PUBLIC",
        targetSchema: comparison.target.schema || "PUBLIC",
        comparisonType: (comparison.options as any)?.comparisonType || "D2D",
        options: comparison.options,
      };

      // Debug logging
      console.log("🚀 Starting schema comparison with payload:", {
        ...requestPayload,
        sourceName: comparison.source.connectionName,
        targetName: comparison.target.connectionName,
      });

      // Call the API to start comparison (userId comes from JWT token)
      const apiComparison = await apiService.compareSchemas(requestPayload);

      // Poll for results
      const jobId = apiComparison.jobId;
      const pollInterval = setInterval(async () => {
        try {
          const result = await apiService.getComparisonResult(jobId);

          if (result.status === "COMPLETED") {
            clearInterval(pollInterval);

            const completedComparison: SchemaComparison = {
              ...updatedComparison,
              id: jobId,
              status: "completed" as const,
              progress: 100,
              results: result.result,
              completedAt: new Date().toISOString(),
            };

            setCurrentComparison(completedComparison);
            setComparisons((current) => [completedComparison, ...current]);
            setActiveTab("results");
            setIsComparing(false);

            toast.success("Comparison completed successfully!");
          } else if (result.status === "FAILED") {
            clearInterval(pollInterval);
            setIsComparing(false);
            toast.error("Comparison failed", {
              description: result.result?.error || "Unknown error occurred",
            });
          }
        } catch (error: any) {
          clearInterval(pollInterval);
          setIsComparing(false);
          toast.error("Failed to fetch comparison results", {
            description: error.message,
          });
        }
      }, 2000); // Poll every 2 seconds
    } catch (error: any) {
      setIsComparing(false);
      console.error("Comparison failed:", error);

      // Better error message with details
      const errorMessage = error.message || "Unknown error occurred";
      const errorDetails =
        error.response?.data?.message || error.details?.message || "";

      toast.error("Failed to start comparison", {
        description: errorDetails || errorMessage,
      });
    }
  };

  const handleSaveComparison = () => {
    if (!currentComparison) return;
    toast.success("Comparison saved successfully");
  };

  const handleLoadComparison = async (comparison: SchemaComparison) => {
    try {
      const result = await apiService.getComparisonResult(comparison.id);
      const loadedComparison: SchemaComparison = {
        ...comparison,
        results: result.result,
        status: result.status.toLowerCase() as any,
      };
      setCurrentComparison(loadedComparison);
      setActiveTab("results");
    } catch (error: any) {
      toast.error("Failed to load comparison", {
        description: error.message,
      });
    }
  };

  const handleGenerateScript = async () => {
    if (!currentComparison || !user?.id) {
      toast.error("No comparison available to generate script");
      return;
    }

    setIsGeneratingScript(true);
    toast.info("Generating deployment script...");

    try {
      const script = await apiService.generateDeploymentScript(
        currentComparison.id
      );

      toast.success("Deployment script generated successfully!");
      setIsGeneratingScript(false);
      setActiveTab("script");

      return script;
    } catch (error: any) {
      setIsGeneratingScript(false);
      toast.error("Failed to generate deployment script", {
        description: error.message,
      });
      return null;
    }
  };

  return (
    <div className="h-full flex flex-col gap-6">
      <div className="shrink-0 px-6 pt-6">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold tracking-tight">
              Schema Matcher
            </h1>
            <p className="text-muted-foreground mt-1">
              Compare database schemas and generate deployment scripts
            </p>
          </div>

          <div className="flex items-center gap-2">
            <Button variant="outline" onClick={() => setActiveTab("setup")}>
              <ClockCounterClockwise className="h-4 w-4 mr-2" />
              History
            </Button>

            {currentComparison?.status === "completed" && (
              <>
                <Button variant="outline" onClick={handleSaveComparison}>
                  <FloppyDisk className="h-4 w-4 mr-2" />
                  Save
                </Button>

                <Button
                  onClick={handleGenerateScript}
                  disabled={isGeneratingScript}
                >
                  {isGeneratingScript ? (
                    <CircleNotch className="h-4 w-4 mr-2 animate-spin" />
                  ) : (
                    <FileCode className="h-4 w-4 mr-2" />
                  )}
                  Generate Script
                </Button>
              </>
            )}
          </div>
        </div>
      </div>

      {currentComparison && currentComparison.status === "running" && (
        <Card className="p-6">
          <div className="space-y-4">
            <div className="flex items-center justify-between">
              <div>
                <h3 className="font-semibold">Running Comparison...</h3>
                <p className="text-sm text-muted-foreground">
                  Comparing {currentComparison.source.connectionName} →{" "}
                  {currentComparison.target.connectionName}
                </p>
              </div>
              <Badge variant="secondary">
                <Play className="h-3 w-3 mr-1 animate-pulse" weight="fill" />
                In Progress
              </Badge>
            </div>

            <div className="space-y-2">
              <div className="flex items-center justify-between text-sm">
                <span className="text-muted-foreground">Progress</span>
                <span className="font-medium">
                  {currentComparison.progress}%
                </span>
              </div>
              <div className="h-2 bg-secondary rounded-full overflow-hidden">
                <div
                  className="h-full bg-primary transition-all duration-500"
                  style={{ width: `${currentComparison.progress}%` }}
                />
              </div>
            </div>
          </div>
        </Card>
      )}

      <Tabs
        value={activeTab}
        onValueChange={(v) => setActiveTab(v as any)}
        className="flex-1 flex flex-col min-h-0 px-6 pb-6"
      >
        <TabsList className="grid w-full grid-cols-4 shrink-0">
          <TabsTrigger value="setup">Setup</TabsTrigger>
          <TabsTrigger value="results" disabled={!currentComparison?.results}>
            Results
            {currentComparison?.results && (
              <Badge variant="secondary" className="ml-2">
                {currentComparison.results.summary.totalObjects}
              </Badge>
            )}
          </TabsTrigger>
          <TabsTrigger
            value="dependencies"
            disabled={!currentComparison?.results}
          >
            Dependencies
          </TabsTrigger>
          <TabsTrigger value="script" disabled={!currentComparison?.results}>
            Script
          </TabsTrigger>
        </TabsList>

        <TabsContent value="setup" className="flex-1 overflow-auto mt-4">
          <ComparisonSetup onStartComparison={handleStartComparison} />

          <ComparisonHistory
            comparisons={comparisons}
            onLoadComparison={handleLoadComparison}
          />
        </TabsContent>

        <TabsContent
          value="results"
          className="flex-1 overflow-hidden mt-4"
          style={{ minHeight: 0 }}
        >
          {currentComparison?.results && (
            <div className="h-full">
              <ComparisonResults
                comparison={currentComparison}
                onGenerateScript={() => setActiveTab("script")}
              />
            </div>
          )}
        </TabsContent>

        <TabsContent
          value="dependencies"
          className="flex-1 overflow-hidden mt-4"
          style={{ minHeight: 0 }}
        >
          {currentComparison?.id && (
            <div className="h-full">
              <SchemaMatcherLineageGraph jobId={currentComparison.id} />
            </div>
          )}
        </TabsContent>

        <TabsContent value="script" className="flex-1 overflow-auto mt-4">
          {currentComparison?.results && (
            <ScriptGenerator comparison={currentComparison} />
          )}
        </TabsContent>
      </Tabs>
    </div>
  );
}
