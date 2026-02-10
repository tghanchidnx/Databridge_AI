import React, { useEffect, useState } from "react";
import type { Connection } from "@/services/api/connection";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Checkbox } from "@/components/ui/checkbox";
import { Switch } from "@/components/ui/switch";
import { Loader2, Play, Check, ChevronsUpDown } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { smartHierarchyService } from "@/services/api";

interface DeploymentDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  projectId: string; // Added
  deploymentConfig: {
    connectionId: string;
    database: string;
    schema: string;
    masterTableName: string;
    masterViewName: string;
    databaseType: "snowflake" | "postgres" | "mysql" | "sqlserver";
    createTables: boolean;
    createViews: boolean;
    createDynamicTables: boolean;
  };
  onConfigChange: (
    config: Partial<DeploymentDialogProps["deploymentConfig"]>
  ) => void;
  availableConnections: Connection[];
  availableDatabases: string[];
  availableSchemas: string[];
  loadingDatabases: boolean;
  loadingSchemas: boolean;
  deploymentLoading: boolean;
  deployAll: boolean;
  setDeployAll: (value: boolean) => void;
  onDeploy: () => void;
  toast: any;
}

export const DeploymentDialog: React.FC<DeploymentDialogProps> = ({
  open,
  onOpenChange,
  projectId,
  deploymentConfig,
  onConfigChange,
  availableConnections,
  availableDatabases,
  availableSchemas,
  loadingDatabases,
  loadingSchemas,
  deploymentLoading,
  deployAll,
  setDeployAll,
  onDeploy,
  toast,
}) => {
  const [savedConfig, setSavedConfig] = useState<any>(null);
  const [loadingConfig, setLoadingConfig] = useState(false);
  const [useNewLocation, setUseNewLocation] = useState(false);
  const [saveConfig, setSaveConfig] = useState(false);

  // Load saved deployment config when dialog opens
  useEffect(() => {
    if (open && projectId) {
      loadSavedConfig();
    }
  }, [open, projectId]);

  const loadSavedConfig = async () => {
    setLoadingConfig(true);
    try {
      const config = await smartHierarchyService.getDeploymentConfig(projectId);
      if (config) {
        setSavedConfig(config);
        // Apply saved config to form
        if (!useNewLocation) {
          onConfigChange(config);
        }
      }
    } catch (error) {
      console.error("Failed to load deployment config:", error);
    } finally {
      setLoadingConfig(false);
    }
  };

  // Toggle between saved config and new location
  const handleUseNewLocationChange = (checked: boolean) => {
    setUseNewLocation(checked);
    if (!checked && savedConfig) {
      // Switch back to saved config
      onConfigChange(savedConfig);
    } else {
      // Clear fields for new location
      onConfigChange({
        connectionId: "",
        database: "",
        schema: "",
        masterTableName: "HIERARCHY_MASTER",
        masterViewName: "VW_{PROJECT}_HIERARCHY_MASTER",
      });
    }
  };

  const fieldsDisabled = savedConfig && !useNewLocation;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl!">
        <DialogHeader>
          <DialogTitle>Deploy to Database</DialogTitle>
          <DialogDescription>
            Configure deployment settings and execute scripts on your database
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4">
          {/* Saved Config Info & Use New Location Switch */}
          {savedConfig && (
            <div className="p-3 bg-blue-50 dark:bg-blue-950 border border-blue-200 dark:border-blue-800 rounded-md space-y-2">
              <div className="flex items-center justify-between">
                <div>
                  <p className="text-sm font-medium">
                    Saved Configuration Found
                  </p>
                  <p className="text-xs text-muted-foreground">
                    {savedConfig.database}.{savedConfig.schema} (
                    {savedConfig.databaseType})
                  </p>
                </div>
                <div className="flex items-center space-x-2">
                  <Label htmlFor="use-new-location" className="text-sm">
                    Use Different Location
                  </Label>
                  <Switch
                    id="use-new-location"
                    checked={useNewLocation}
                    onCheckedChange={handleUseNewLocationChange}
                  />
                </div>
              </div>
            </div>
          )}

          <div className="space-y-2">
            <Label htmlFor="deployment-connection">Database Connection</Label>
            <Select
              value={deploymentConfig.connectionId}
              disabled={fieldsDisabled}
              onValueChange={(value) =>
                onConfigChange({
                  connectionId: value,
                  database: "",
                  schema: "",
                })
              }
            >
              <SelectTrigger id="deployment-connection" className="w-full">
                <SelectValue placeholder="Select connection..." />
              </SelectTrigger>
              <SelectContent>
                {availableConnections.map((conn) => (
                  <SelectItem key={conn.id} value={conn.id}>
                    {conn.connectionName} ({conn.connectionType})
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
          <div className="flex flex-row flex-wrap gap-4">
            <div className="space-y-2 w-[45%]">
              <Label htmlFor="deployment-database">Database</Label>
              <Popover>
                <PopoverTrigger asChild>
                  <Button
                    variant="outline"
                    role="combobox"
                    className="w-full justify-between"
                    disabled={
                      fieldsDisabled ||
                      !deploymentConfig.connectionId ||
                      loadingDatabases
                    }
                  >
                    {deploymentConfig.database || "Select database..."}
                    <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="w-full p-0" align="start">
                  <Command>
                    <CommandInput placeholder="Search database..." />
                    <CommandList>
                      <CommandEmpty>
                        {loadingDatabases ? "Loading..." : "No database found."}
                      </CommandEmpty>
                      <CommandGroup>
                        {availableDatabases.map((db) => (
                          <CommandItem
                            key={db}
                            value={db}
                            onSelect={() => {
                              onConfigChange({
                                database: db,
                                schema: "",
                              });
                            }}
                          >
                            <Check
                              className={`mr-2 h-4 w-4 ${
                                deploymentConfig.database === db
                                  ? "opacity-100"
                                  : "opacity-0"
                              }`}
                            />
                            {db}
                          </CommandItem>
                        ))}
                      </CommandGroup>
                    </CommandList>
                  </Command>
                </PopoverContent>
              </Popover>
            </div>

            <div className="space-y-2 w-1/2">
              <Label htmlFor="deployment-schema">Schema</Label>
              <Popover>
                <PopoverTrigger asChild>
                  <Button
                    variant="outline"
                    role="combobox"
                    className="w-full justify-between"
                    disabled={
                      fieldsDisabled ||
                      !deploymentConfig.database ||
                      loadingSchemas
                    }
                  >
                    {deploymentConfig.schema || "Select schema..."}
                    <ChevronsUpDown className="ml-2 h-4 w-4 shrink-0 opacity-50" />
                  </Button>
                </PopoverTrigger>
                <PopoverContent className="w-full p-0" align="start">
                  <Command>
                    <CommandInput placeholder="Search schema..." />
                    <CommandList>
                      <CommandEmpty>
                        {loadingSchemas ? "Loading..." : "No schema found."}
                      </CommandEmpty>
                      <CommandGroup>
                        {availableSchemas.map((schema) => (
                          <CommandItem
                            key={schema}
                            value={schema}
                            onSelect={() => {
                              onConfigChange({
                                schema: schema,
                              });
                            }}
                          >
                            <Check
                              className={`mr-2 h-4 w-4 ${
                                deploymentConfig.schema === schema
                                  ? "opacity-100"
                                  : "opacity-0"
                              }`}
                            />
                            {schema}
                          </CommandItem>
                        ))}
                      </CommandGroup>
                    </CommandList>
                  </Command>
                </PopoverContent>
              </Popover>
            </div>

            <div className="space-y-2 ">
              <Label htmlFor="master-table-name">Master Table Name</Label>
              <Input
                id="master-table-name"
                placeholder="HIERARCHY_MASTER"
                disabled={fieldsDisabled}
                value={deploymentConfig.masterTableName}
                onChange={(e) =>
                  onConfigChange({
                    masterTableName: e.target.value,
                  })
                }
              />
              <p className="text-xs text-muted-foreground">
                Shared table that holds all hierarchies across projects
              </p>
            </div>

            <div className="space-y-2 w-1/2">
              <Label htmlFor="master-view-name">Master View Name</Label>
              <Input
                id="master-view-name"
                placeholder="VW_{PROJECT}_HIERARCHY_MASTER"
                disabled={fieldsDisabled}
                value={deploymentConfig.masterViewName}
                onChange={(e) =>
                  onConfigChange({
                    masterViewName: e.target.value,
                  })
                }
              />
              <p className="text-xs text-muted-foreground">
                View combining all hierarchies
              </p>
            </div>
          </div>

          <div className="space-y-3 p-4 bg-muted/50 rounded-md">
            <Label className="text-sm font-semibold">Deployment Options</Label>
            <div className="space-y-2 flex flex-row gap-4">
              <div className="flex items-center space-x-2">
                <Checkbox
                  id="deploy-all"
                  checked={deployAll}
                  onCheckedChange={(checked) => setDeployAll(checked === true)}
                />
                <Label
                  htmlFor="deploy-all"
                  className="text-sm font-normal cursor-pointer"
                >
                  Deploy All
                </Label>
              </div>
              <div className="flex items-center space-x-2">
                <Checkbox
                  id="create-tables"
                  checked={deploymentConfig.createTables}
                  onCheckedChange={(checked) =>
                    onConfigChange({ createTables: checked as boolean })
                  }
                />
                <Label
                  htmlFor="create-tables"
                  className="text-sm font-normal cursor-pointer"
                >
                  Create INSERT Tables
                </Label>
              </div>
              <div className="flex items-center space-x-2">
                <Checkbox
                  id="create-views"
                  checked={deploymentConfig.createViews}
                  onCheckedChange={(checked) =>
                    onConfigChange({ createViews: checked as boolean })
                  }
                />
                <Label
                  htmlFor="create-views"
                  className="text-sm font-normal cursor-pointer"
                >
                  Create Views
                </Label>
              </div>
              <div className="flex items-center space-x-2 -mt-1.5">
                <Checkbox
                  id="create-dynamic-tables"
                  checked={deploymentConfig.createDynamicTables}
                  onCheckedChange={(checked) =>
                    onConfigChange({ createDynamicTables: checked as boolean })
                  }
                />
                <Label
                  htmlFor="create-dynamic-tables"
                  className="text-sm font-normal cursor-pointer"
                >
                  Create Dynamic Tables
                </Label>
              </div>
            </div>
            {/* Save Configuration Checkbox */}
            {(useNewLocation || !savedConfig) && (
              <div className="flex items-center space-x-2 pt-2 border-t">
                <Checkbox
                  id="save-config"
                  checked={saveConfig}
                  onCheckedChange={(checked) =>
                    setSaveConfig(checked as boolean)
                  }
                />
                <Label
                  htmlFor="save-config"
                  className="text-sm font-normal cursor-pointer"
                >
                  Save as default deployment configuration
                </Label>
              </div>
            )}
          </div>

          <div className="p-4 bg-muted rounded-md space-y-2">
            <h4 className="text-sm font-semibold">Deployment Summary</h4>
            <ul className="text-xs space-y-1 text-muted-foreground">
              <li>
                • Master table:{" "}
                <code className="bg-background px-1 rounded">
                  {deploymentConfig.masterTableName || "HIERARCHY_MASTER"}
                </code>
              </li>
              <li>
                • Unified view:{" "}
                <code className="bg-background px-1 rounded">
                  VW_{"{PROJECT}"}_HIERARCHY_MASTER
                </code>
              </li>
              <li>
                • Dynamic table:{" "}
                <code className="bg-background px-1 rounded">
                  DT_{"{PROJECT}"}_EXPANSION
                </code>
              </li>
              <li className="text-xs text-muted-foreground mt-2">
                Note: Single view and table contain ALL hierarchies
              </li>
            </ul>
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button
            onClick={onDeploy}
            disabled={
              !deploymentConfig.connectionId ||
              !deploymentConfig.database ||
              !deploymentConfig.schema ||
              !deploymentConfig.masterTableName ||
              !deploymentConfig.masterViewName ||
              deploymentLoading
            }
          >
            {deploymentLoading ? (
              <>
                <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                Deploying...
              </>
            ) : (
              <>
                <Play className="w-4 h-4 mr-2" />
                Deploy Scripts
              </>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
};
