import { useState, useEffect } from "react";
import {
  Database,
  CaretDown,
  Gear,
  ArrowsClockwise,
} from "@phosphor-icons/react";
import { Button } from "@/components/ui/button";
import { Card } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Checkbox } from "@/components/ui/checkbox";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";
import type { Connection, SchemaComparison, ComparisonOptions } from "@/types";
import { useConnectionsStore } from "@/stores/connections.store";
import { apiService } from "@/lib/api-service";
import { toast } from "sonner";

interface ComparisonSetupProps {
  onStartComparison: (comparison: SchemaComparison) => void;
}

export function ComparisonSetup({ onStartComparison }: ComparisonSetupProps) {
  const {
    connections,
    loading: connectionsLoading,
    refreshConnections,
  } = useConnectionsStore();
  const [sourceConnection, setSourceConnection] = useState<string>("");
  const [sourceDatabase, setSourceDatabase] = useState<string>("");
  const [sourceSchema, setSourceSchema] = useState<string>("");
  const [sourceDatabases, setSourceDatabases] = useState<string[]>([]);
  const [sourceSchemas, setSourceSchemas] = useState<string[]>([]);
  const [loadingSourceDatabases, setLoadingSourceDatabases] = useState(false);
  const [loadingSourceSchemas, setLoadingSourceSchemas] = useState(false);

  const [targetConnection, setTargetConnection] = useState<string>("");
  const [targetDatabase, setTargetDatabase] = useState<string>("");
  const [targetSchema, setTargetSchema] = useState<string>("");
  const [targetDatabases, setTargetDatabases] = useState<string[]>([]);
  const [targetSchemas, setTargetSchemas] = useState<string[]>([]);
  const [loadingTargetDatabases, setLoadingTargetDatabases] = useState(false);
  const [loadingTargetSchemas, setLoadingTargetSchemas] = useState(false);

  const [comparisonName, setComparisonName] = useState<string>("");
  const [showAdvanced, setShowAdvanced] = useState(false);

  const [options, setOptions] = useState<ComparisonOptions>({
    mode: "standard",
    ignoreCase: false,
    ignoreWhitespace: true,
    ignoreComments: true,
    compareRowCounts: true,
    compareDataSamples: false,
    sampleSize: 1000,
    includeSystemObjects: false,
    objectFilters: {
      includePatterns: [],
      excludePatterns: ["temp_*", "bak_*"],
    },
    objectTypes: {
      tables: true,
      views: true,
      procedures: true,
      functions: true,
      sequences: true,
      indexes: true,
      triggers: true,
    },
  });

  // Refresh connections on component mount to ensure we have latest IDs
  useEffect(() => {
    refreshConnections();
  }, []);

  // Load databases when source connection changes
  useEffect(() => {
    if (sourceConnection) {
      loadSourceDatabases();
    } else {
      setSourceDatabases([]);
      setSourceDatabase("");
      setSourceSchema("");
    }
  }, [sourceConnection]);

  // Load schemas when source database changes
  useEffect(() => {
    if (sourceConnection && sourceDatabase) {
      loadSourceSchemas();
    } else {
      setSourceSchemas([]);
      setSourceSchema("");
    }
  }, [sourceConnection, sourceDatabase]);

  // Load databases when target connection changes
  useEffect(() => {
    if (targetConnection) {
      loadTargetDatabases();
    } else {
      setTargetDatabases([]);
      setTargetDatabase("");
      setTargetSchema("");
    }
  }, [targetConnection]);

  // Load schemas when target database changes
  useEffect(() => {
    if (targetConnection && targetDatabase) {
      loadTargetSchemas();
    } else {
      setTargetSchemas([]);
      setTargetSchema("");
    }
  }, [targetConnection, targetDatabase]);

  const loadSourceDatabases = async () => {
    setLoadingSourceDatabases(true);
    try {
      const databases = await apiService.getDatabases(sourceConnection);
      setSourceDatabases(Array.isArray(databases) ? databases : []);
    } catch (error: any) {
      console.error("Failed to load databases:", error);
      setSourceDatabases([]);
      toast.error("Failed to load databases", {
        description: error.message,
      });
    } finally {
      setLoadingSourceDatabases(false);
    }
  };

  const loadSourceSchemas = async () => {
    setLoadingSourceSchemas(true);
    try {
      const schemas = await apiService.getSchemas(
        sourceConnection,
        sourceDatabase
      );
      setSourceSchemas(Array.isArray(schemas) ? schemas : []);
    } catch (error: any) {
      console.error("Failed to load schemas:", error);
      setSourceSchemas([]);
      toast.error("Failed to load schemas", {
        description: error.message,
      });
    } finally {
      setLoadingSourceSchemas(false);
    }
  };

  const loadTargetDatabases = async () => {
    setLoadingTargetDatabases(true);
    try {
      const databases = await apiService.getDatabases(targetConnection);
      setTargetDatabases(Array.isArray(databases) ? databases : []);
    } catch (error: any) {
      console.error("Failed to load databases:", error);
      setTargetDatabases([]);
      toast.error("Failed to load databases", {
        description: error.message,
      });
    } finally {
      setLoadingTargetDatabases(false);
    }
  };

  const loadTargetSchemas = async () => {
    setLoadingTargetSchemas(true);
    try {
      const schemas = await apiService.getSchemas(
        targetConnection,
        targetDatabase
      );
      setTargetSchemas(Array.isArray(schemas) ? schemas : []);
    } catch (error: any) {
      console.error("Failed to load schemas:", error);
      setTargetSchemas([]);
      toast.error("Failed to load schemas", {
        description: error.message,
      });
    } finally {
      setLoadingTargetSchemas(false);
    }
  };

  const handleStartComparison = () => {
    const sourceConn = connections.find((c) => c.id === sourceConnection);
    const targetConn = connections.find((c) => c.id === targetConnection);

    console.log("🔍 Connection validation:", {
      sourceConnection,
      targetConnection,
      sourceFound: !!sourceConn,
      targetFound: !!targetConn,
      sourceConnDetails: sourceConn
        ? {
            id: sourceConn.id,
            name: sourceConn.connectionName || sourceConn.name,
            type: sourceConn.serverType || sourceConn.type,
          }
        : null,
      targetConnDetails: targetConn
        ? {
            id: targetConn.id,
            name: targetConn.connectionName || targetConn.name,
            type: targetConn.serverType || targetConn.type,
          }
        : null,
      totalConnections: connections.length,
      availableConnectionIds: connections.map((c) => c.id),
    });

    if (!sourceConn || !targetConn) {
      toast.error("Connection not found", {
        description: !sourceConn
          ? "Source connection not found. Try refreshing the page or recreating the connection."
          : "Target connection not found. Try refreshing the page or recreating the connection.",
      });
      return;
    }

    // Determine comparison type based on connection types
    const sourceType = (
      sourceConn.serverType ||
      sourceConn.type ||
      ""
    ).toLowerCase();
    const targetType = (
      targetConn.serverType ||
      targetConn.type ||
      ""
    ).toLowerCase();

    let comparisonType: "D2D" | "D2S" | "S2D" | "S2S" = "D2D";

    if (sourceType === "snowflake" && targetType === "snowflake") {
      comparisonType = "S2S"; // Snowflake to Snowflake
    } else if (sourceType === "snowflake") {
      comparisonType = "S2D"; // Snowflake to Database
    } else if (targetType === "snowflake") {
      comparisonType = "D2S"; // Database to Snowflake
    } else {
      comparisonType = "D2D"; // Database to Database
    }

    const comparison: SchemaComparison = {
      id: `cmp-${Date.now()}`,
      workspaceId: "current-workspace",
      name: comparisonName || `${sourceConn.name} → ${targetConn.name}`,
      source: {
        connectionId: sourceConnection,
        connectionName: sourceConn.name,
        database: sourceDatabase,
        schema: sourceSchema,
      },
      target: {
        connectionId: targetConnection,
        connectionName: targetConn.name,
        database: targetDatabase,
        schema: targetSchema,
      },
      options: {
        ...options,
        comparisonType, // Add comparison type to options
      },
      status: "pending",
      progress: 0,
      createdBy: "current-user",
      createdAt: new Date().toISOString(),
    };

    onStartComparison(comparison);
  };

  const isValid =
    sourceConnection &&
    targetConnection &&
    sourceDatabase &&
    targetDatabase &&
    sourceSchema &&
    targetSchema;

  return (
    <Card className="p-6">
      <div className="space-y-6">
        <div className="flex items-center justify-between">
          <h3 className="text-lg font-semibold">Comparison Configuration</h3>
          <Button
            variant="outline"
            size="sm"
            onClick={() => refreshConnections()}
            disabled={connectionsLoading}
            className="gap-2"
          >
            <ArrowsClockwise
              className={`h-4 w-4 ${connectionsLoading ? "animate-spin" : ""}`}
            />
            Refresh Connections
          </Button>
        </div>

        <div>
          <div className="space-y-4">
            <div className="grid gap-4">
              <div>
                <Label htmlFor="comparison-name">
                  Comparison Name (Optional)
                </Label>
                <Input
                  id="comparison-name"
                  placeholder="e.g., Production to Staging Sync"
                  value={comparisonName}
                  onChange={(e) => setComparisonName(e.target.value)}
                  className="mt-1.5"
                />
              </div>
            </div>
          </div>
        </div>

        <div className="grid lg:grid-cols-2 gap-6">
          <Card className="p-4 bg-secondary/50">
            <div className="flex items-center gap-2 mb-4">
              <Database className="h-5 w-5 text-primary" weight="fill" />
              <h4 className="font-semibold">Source Connection</h4>
            </div>

            <div className="space-y-3">
              <div>
                <Label htmlFor="source-connection">Connection</Label>
                <Select
                  value={sourceConnection}
                  onValueChange={setSourceConnection}
                >
                  <SelectTrigger id="source-connection" className="mt-1.5">
                    <SelectValue placeholder="Select connection..." />
                  </SelectTrigger>
                  <SelectContent>
                    {connections.map((conn) => (
                      <SelectItem key={conn.id} value={conn.id}>
                        <div className="flex items-center gap-2">
                          <div
                            className={`h-2 w-2 rounded-full shrink-0 ${
                              conn.status === "active"
                                ? "bg-green-500"
                                : conn.status === "error"
                                ? "bg-red-500"
                                : "bg-gray-500"
                            }`}
                          />
                          <span className="font-medium">
                            {conn.connectionName || conn.name}
                          </span>
                          <span className="text-xs px-1.5 py-0.5 rounded bg-primary/10 text-primary">
                            {conn.serverType || conn.type || "Snowflake"}
                          </span>
                          {(conn.snowflakeUser || conn.username) && (
                            <span className="text-xs text-muted-foreground ml-1">
                              ({conn.snowflakeUser || conn.username})
                            </span>
                          )}
                        </div>
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div>
                <Label htmlFor="source-database">Database</Label>
                <Select
                  value={sourceDatabase}
                  onValueChange={setSourceDatabase}
                >
                  <SelectTrigger
                    id="source-database"
                    className="mt-1.5"
                    disabled={!sourceConnection || loadingSourceDatabases}
                  >
                    <SelectValue
                      placeholder={
                        loadingSourceDatabases
                          ? "Loading..."
                          : "Select database..."
                      }
                    />
                  </SelectTrigger>
                  <SelectContent>
                    {sourceDatabases.length === 0 && !loadingSourceDatabases ? (
                      <SelectItem value="__empty__" disabled>
                        No databases found
                      </SelectItem>
                    ) : (
                      sourceDatabases.map((db) => (
                        <SelectItem key={db} value={db}>
                          {db}
                        </SelectItem>
                      ))
                    )}
                  </SelectContent>
                </Select>
              </div>

              <div>
                <Label htmlFor="source-schema">Schema</Label>
                <Select value={sourceSchema} onValueChange={setSourceSchema}>
                  <SelectTrigger
                    id="source-schema"
                    className="mt-1.5"
                    disabled={!sourceDatabase || loadingSourceSchemas}
                  >
                    <SelectValue
                      placeholder={
                        loadingSourceSchemas ? "Loading..." : "Select schema..."
                      }
                    />
                  </SelectTrigger>
                  <SelectContent>
                    {sourceSchemas.length === 0 && !loadingSourceSchemas ? (
                      <SelectItem value="__empty__" disabled>
                        No schemas found
                      </SelectItem>
                    ) : (
                      sourceSchemas.map((schema) => (
                        <SelectItem key={schema} value={schema}>
                          {schema}
                        </SelectItem>
                      ))
                    )}
                  </SelectContent>
                </Select>
              </div>
            </div>
          </Card>

          <Card className="p-4 bg-accent/10">
            <div className="flex items-center gap-2 mb-4">
              <Database className="h-5 w-5 text-accent" weight="fill" />
              <h4 className="font-semibold">Target Connection</h4>
            </div>

            <div className="space-y-3">
              <div>
                <Label htmlFor="target-connection">Connection</Label>
                <Select
                  value={targetConnection}
                  onValueChange={setTargetConnection}
                >
                  <SelectTrigger id="target-connection" className="mt-1.5">
                    <SelectValue placeholder="Select connection..." />
                  </SelectTrigger>
                  <SelectContent>
                    {connections.map((conn) => (
                      <SelectItem key={conn.id} value={conn.id}>
                        <div className="flex items-center gap-2">
                          <div
                            className={`h-2 w-2 rounded-full shrink-0 ${
                              conn.status === "active"
                                ? "bg-green-500"
                                : conn.status === "error"
                                ? "bg-red-500"
                                : "bg-gray-500"
                            }`}
                          />
                          <span className="font-medium">
                            {conn.connectionName || conn.name}
                          </span>
                          <span className="text-xs px-1.5 py-0.5 rounded bg-primary/10 text-primary">
                            {conn.serverType || conn.type || "Snowflake"}
                          </span>
                          {(conn.snowflakeUser || conn.username) && (
                            <span className="text-xs text-muted-foreground ml-1">
                              ({conn.snowflakeUser || conn.username})
                            </span>
                          )}
                        </div>
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div>
                <Label htmlFor="target-database">Database</Label>
                <Select
                  value={targetDatabase}
                  onValueChange={setTargetDatabase}
                >
                  <SelectTrigger
                    id="target-database"
                    className="mt-1.5"
                    disabled={!targetConnection || loadingTargetDatabases}
                  >
                    <SelectValue
                      placeholder={
                        loadingTargetDatabases
                          ? "Loading..."
                          : "Select database..."
                      }
                    />
                  </SelectTrigger>
                  <SelectContent>
                    {targetDatabases.length === 0 && !loadingTargetDatabases ? (
                      <SelectItem value="__empty__" disabled>
                        No databases found
                      </SelectItem>
                    ) : (
                      targetDatabases.map((db) => (
                        <SelectItem key={db} value={db}>
                          {db}
                        </SelectItem>
                      ))
                    )}
                  </SelectContent>
                </Select>
              </div>

              <div>
                <Label htmlFor="target-schema">Schema</Label>
                <Select value={targetSchema} onValueChange={setTargetSchema}>
                  <SelectTrigger
                    id="target-schema"
                    className="mt-1.5"
                    disabled={!targetDatabase || loadingTargetSchemas}
                  >
                    <SelectValue
                      placeholder={
                        loadingTargetSchemas ? "Loading..." : "Select schema..."
                      }
                    />
                  </SelectTrigger>
                  <SelectContent>
                    {targetSchemas.length === 0 && !loadingTargetSchemas ? (
                      <SelectItem value="__empty__" disabled>
                        No schemas found
                      </SelectItem>
                    ) : (
                      targetSchemas.map((schema) => (
                        <SelectItem key={schema} value={schema}>
                          {schema}
                        </SelectItem>
                      ))
                    )}
                  </SelectContent>
                </Select>
              </div>
            </div>
          </Card>
        </div>

        <Collapsible open={showAdvanced} onOpenChange={setShowAdvanced}>
          <CollapsibleTrigger asChild>
            <Button variant="ghost" className="w-full justify-between">
              <div className="flex items-center gap-2">
                <Gear className="h-4 w-4" />
                Advanced Options
              </div>
              <CaretDown
                className={`h-4 w-4 transition-transform ${
                  showAdvanced ? "rotate-180" : ""
                }`}
              />
            </Button>
          </CollapsibleTrigger>

          <CollapsibleContent className="pt-4">
            <Card className="p-4 bg-muted/50">
              <div className="space-y-4">
                <div>
                  <Label className="text-base font-semibold">
                    Object Types
                  </Label>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-3 mt-3">
                    {Object.entries(options.objectTypes).map(([key, value]) => (
                      <div key={key} className="flex items-center space-x-2">
                        <Checkbox
                          id={`obj-${key}`}
                          checked={value}
                          onCheckedChange={(checked) =>
                            setOptions({
                              ...options,
                              objectTypes: {
                                ...options.objectTypes,
                                [key]: !!checked,
                              },
                            })
                          }
                        />
                        <Label
                          htmlFor={`obj-${key}`}
                          className="capitalize cursor-pointer text-sm"
                        >
                          {key}
                        </Label>
                      </div>
                    ))}
                  </div>
                </div>

                <div>
                  <Label className="text-base font-semibold">
                    Comparison Options
                  </Label>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-3 mt-3">
                    <div className="flex items-center space-x-2">
                      <Checkbox
                        id="compare-row-counts"
                        checked={options.compareRowCounts}
                        onCheckedChange={(checked) =>
                          setOptions({
                            ...options,
                            compareRowCounts: !!checked,
                          })
                        }
                      />
                      <Label
                        htmlFor="compare-row-counts"
                        className="cursor-pointer text-sm"
                      >
                        Compare Row Counts
                      </Label>
                    </div>

                    <div className="flex items-center space-x-2">
                      <Checkbox
                        id="ignore-whitespace"
                        checked={options.ignoreWhitespace}
                        onCheckedChange={(checked) =>
                          setOptions({
                            ...options,
                            ignoreWhitespace: !!checked,
                          })
                        }
                      />
                      <Label
                        htmlFor="ignore-whitespace"
                        className="cursor-pointer text-sm"
                      >
                        Ignore Whitespace
                      </Label>
                    </div>

                    <div className="flex items-center space-x-2">
                      <Checkbox
                        id="ignore-comments"
                        checked={options.ignoreComments}
                        onCheckedChange={(checked) =>
                          setOptions({ ...options, ignoreComments: !!checked })
                        }
                      />
                      <Label
                        htmlFor="ignore-comments"
                        className="cursor-pointer text-sm"
                      >
                        Ignore Comments
                      </Label>
                    </div>

                    <div className="flex items-center space-x-2">
                      <Checkbox
                        id="compare-data-samples"
                        checked={options.compareDataSamples}
                        onCheckedChange={(checked) =>
                          setOptions({
                            ...options,
                            compareDataSamples: !!checked,
                          })
                        }
                      />
                      <Label
                        htmlFor="compare-data-samples"
                        className="cursor-pointer text-sm"
                      >
                        Deep Data Comparison
                      </Label>
                    </div>
                  </div>
                </div>
              </div>
            </Card>
          </CollapsibleContent>
        </Collapsible>

        <div className="flex justify-end gap-3 pt-4 border-t">
          <Button
            size="lg"
            disabled={!isValid}
            onClick={handleStartComparison}
            className="px-8"
          >
            Start Comparison
          </Button>
        </div>
      </div>
    </Card>
  );
}
