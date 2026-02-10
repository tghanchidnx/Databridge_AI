import React, { useState, useEffect } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import { Alert, AlertDescription } from "@/components/ui/alert";
import {
  Calculator,
  Link2,
  AlertCircle,
  CheckCircle,
  Loader2,
} from "lucide-react";
import type { SmartHierarchyMaster } from "@/services/api/hierarchy";
import { smartHierarchyService } from "@/services/api/hierarchy";
import { useToast } from "@/hooks/use-toast";
import { cn } from "@/lib/utils";

// Helper function to calculate hierarchy level
const getHierarchyLevel = (hierarchy: SmartHierarchyMaster): number => {
  if (!hierarchy.hierarchyLevel) return 1;

  const levels = hierarchy.hierarchyLevel;
  let level = 0;

  // Count non-empty levels
  for (let i = 1; i <= 15; i++) {
    const key = `level_${i}` as keyof typeof levels;
    if (levels[key] && levels[key] !== "") {
      level = i;
    }
  }

  return level || 1;
};

// Helper function to get hierarchy level path
const getHierarchyPath = (hierarchy: SmartHierarchyMaster): string => {
  if (!hierarchy.hierarchyLevel) return hierarchy.hierarchyName;

  const levels = hierarchy.hierarchyLevel;
  const path: string[] = [];

  for (let i = 1; i <= 15; i++) {
    const key = `level_${i}` as keyof typeof levels;
    if (levels[key] && levels[key] !== "") {
      path.push(levels[key] as string);
    }
  }

  return path.length > 0 ? path.join(" > ") : hierarchy.hierarchyName;
};

interface FormulaGroupBuilderProps {
  availableHierarchies: SmartHierarchyMaster[];
  currentHierarchy?: SmartHierarchyMaster;
  projectId?: string;
  value?: any;
  onChange?: (group: any) => void;
  onSave?: (group: any) => void;
}

interface FormulaInfo {
  isFormulaOwner: boolean;
  isContributor: boolean;
  ownFormula?: {
    groupName: string;
    mainHierarchyId: string;
    mainHierarchyName: string;
    rules: any[];
    formula_params?: any;
    formulaType?: string;
  };
  contributorOf?: {
    mainHierarchyId: string;
    mainHierarchyName: string;
    groupName: string;
    myRole: string;
    myPrecedence?: number;
    allRules: any[];
    formulaType?: string;
  };
}

interface HierarchyFormulaData {
  hierarchy: SmartHierarchyMaster;
  formulaInfo: FormulaInfo | null;
}

export const FormulaGroupBuilder: React.FC<FormulaGroupBuilderProps> = ({
  currentHierarchy,
  projectId,
  availableHierarchies,
}) => {
  const { toast } = useToast();
  const [loading, setLoading] = useState(false);
  const [allFormulaData, setAllFormulaData] = useState<HierarchyFormulaData[]>(
    []
  );

  useEffect(() => {
    if (currentHierarchy?.hierarchyId && projectId) {
      loadAllFormulaInfo();
    }
  }, [currentHierarchy?.hierarchyId, projectId]);

  const loadAllFormulaInfo = async () => {
    if (!currentHierarchy || !projectId) return;

    setLoading(true);
    try {
      // Get all hierarchies including children
      const allHierarchies = await smartHierarchyService.findAll(projectId);

      // Find current hierarchy and all its children
      const hierarchiesToCheck: SmartHierarchyMaster[] = [currentHierarchy];

      // Find all children recursively
      const findChildren = (parentId: string) => {
        const children = allHierarchies.filter((h) => h.parentId === parentId);
        children.forEach((child) => {
          hierarchiesToCheck.push(child);
          findChildren(child.hierarchyId); // Recursively find grandchildren
        });
      };

      findChildren(currentHierarchy.hierarchyId);

      // Load formula info for each hierarchy
      const formulaDataPromises = hierarchiesToCheck.map(async (hierarchy) => {
        try {
          const info = await smartHierarchyService.getHierarchyFormulaInfo(
            projectId,
            hierarchy.hierarchyId
          );
          return {
            hierarchy,
            formulaInfo: info,
          };
        } catch (error) {
          console.error(
            `Error loading formula info for ${hierarchy.hierarchyName}:`,
            error
          );
          return {
            hierarchy,
            formulaInfo: null,
          };
        }
      });

      const results = await Promise.all(formulaDataPromises);

      // Filter to only hierarchies with formula involvement
      const withFormulas = results.filter(
        (data) =>
          data.formulaInfo &&
          (data.formulaInfo.isFormulaOwner || data.formulaInfo.isContributor)
      );

      setAllFormulaData(withFormulas);
    } catch (error: any) {
      toast({
        variant: "destructive",
        title: "Error loading formula information",
        description: error.message || "Failed to load formula information",
      });
    } finally {
      setLoading(false);
    }
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
      </div>
    );
  }

  if (allFormulaData.length === 0) {
    return (
      <Card className="border-dashed">
        <CardContent className="py-12">
          <div className="flex flex-col items-center justify-center text-center space-y-3">
            <div className="rounded-full bg-muted p-3">
              <AlertCircle className="h-6 w-6 text-muted-foreground" />
            </div>
            <div className="space-y-1">
              <p className="text-sm font-medium">No Formula Assignment</p>
              <p className="text-sm text-muted-foreground">
                This hierarchy and its children are not part of any formula
                group
              </p>
            </div>
          </div>
        </CardContent>
      </Card>
    );
  }

  // Separate parent and children data
  const parentData = allFormulaData.find(
    (data) => data.hierarchy.hierarchyId === currentHierarchy?.hierarchyId
  );
  const childrenData = allFormulaData.filter(
    (data) => data.hierarchy.hierarchyId !== currentHierarchy?.hierarchyId
  );

  return (
    <div className="space-y-6">
      {/* Parent Hierarchy Card */}
      {parentData && parentData.formulaInfo && (
        <div className="space-y-4">
          {/* Formula Owner Section */}
          {parentData.formulaInfo.isFormulaOwner &&
            parentData.formulaInfo.ownFormula && (
              <Card>
                <CardHeader>
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <Calculator className="h-5 w-5 text-primary" />
                      <CardTitle>Formula Owner</CardTitle>
                      {parentData.formulaInfo.ownFormula.formulaType && (
                        <Badge variant="outline" className="text-xs ml-2">
                          {parentData.formulaInfo.ownFormula.formulaType ===
                          "TOTAL_FORMULA"
                            ? "Total Formula"
                            : "Formula Group"}
                        </Badge>
                      )}
                    </div>
                    <Badge variant="default" className="bg-green-600">
                      <CheckCircle className="h-3 w-3 mr-1" />
                      Owner
                    </Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <Separator />

                  <div className="space-y-2">
                    <Label className="text-sm font-medium">
                      {parentData.formulaInfo.ownFormula.formulaType ===
                      "TOTAL_FORMULA"
                        ? "Children Hierarchies"
                        : "Formula Rules"}{" "}
                      ({parentData.formulaInfo.ownFormula.rules.length})
                    </Label>
                    <div className="space-y-2">
                      {parentData.formulaInfo.ownFormula.rules.map(
                        (rule: any, idx: number) => {
                          const ruleHierarchy = availableHierarchies.find(
                            (h) => h.hierarchyId === rule.hierarchyId
                          );
                          return (
                            <div
                              key={idx}
                              className="flex items-center justify-between p-3 bg-muted/50 rounded-md"
                            >
                              <div className="flex flex-col gap-1 flex-1">
                                <div className="flex items-center gap-3">
                                  <Badge variant="outline">
                                    {rule.operation}
                                  </Badge>
                                  <span className="text-sm font-medium">
                                    {rule.hierarchyName}
                                  </span>
                                  {ruleHierarchy && (
                                    <Badge
                                      variant="outline"
                                      className="text-xs px-1.5 py-0.5"
                                    >
                                      Level {getHierarchyLevel(ruleHierarchy)}
                                    </Badge>
                                  )}
                                </div>
                                {ruleHierarchy && (
                                  <span className="text-xs text-muted-foreground ml-2">
                                    {getHierarchyPath(ruleHierarchy)}
                                  </span>
                                )}
                              </div>
                              {rule.FORMULA_PRECEDENCE && (
                                <Badge variant="secondary" className="text-xs">
                                  Precedence: {rule.FORMULA_PRECEDENCE}
                                </Badge>
                              )}
                            </div>
                          );
                        }
                      )}
                    </div>
                  </div>

                  {parentData.formulaInfo.ownFormula.formula_params &&
                    Object.keys(
                      parentData.formulaInfo.ownFormula.formula_params
                    ).length > 0 && (
                      <>
                        <Separator />
                        <div className="space-y-2">
                          <Label className="text-sm font-medium">
                            Formula Parameters
                          </Label>
                          <div className="bg-muted/30 rounded-md p-3">
                            <pre className="text-xs overflow-auto">
                              {JSON.stringify(
                                parentData.formulaInfo.ownFormula
                                  .formula_params,
                                null,
                                2
                              )}
                            </pre>
                          </div>
                        </div>
                      </>
                    )}
                </CardContent>
              </Card>
            )}

          {/* Contributor Section */}
          {parentData.formulaInfo.isContributor &&
            parentData.formulaInfo.contributorOf && (
              <Card>
                <CardHeader>
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <Link2 className="h-5 w-5 text-blue-600" />
                      <CardTitle>Formula Contributor</CardTitle>
                      {parentData.formulaInfo.contributorOf.formulaType && (
                        <Badge variant="outline" className="text-xs ml-2">
                          {parentData.formulaInfo.contributorOf.formulaType ===
                          "TOTAL_FORMULA"
                            ? "Total Formula"
                            : "Formula Group"}
                        </Badge>
                      )}
                    </div>
                    <Badge
                      variant="secondary"
                      className="bg-blue-600 text-white"
                    >
                      Contributor
                    </Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <Alert>
                    <AlertDescription>
                      This hierarchy is part of the formula group owned by{" "}
                      <strong>
                        {parentData.formulaInfo.contributorOf.mainHierarchyName}
                      </strong>
                    </AlertDescription>
                  </Alert>

                  <Separator />

                  <div className="space-y-2">
                    <Label className="text-sm font-medium">
                      Role in Formula
                    </Label>
                    <div className="flex items-center gap-2">
                      <Badge variant="outline" className="text-sm">
                        {parentData.formulaInfo.contributorOf.myRole}
                      </Badge>
                      {parentData.formulaInfo.contributorOf.myPrecedence !==
                        undefined && (
                        <Badge variant="secondary" className="text-xs">
                          Precedence:{" "}
                          {parentData.formulaInfo.contributorOf.myPrecedence}
                        </Badge>
                      )}
                    </div>
                  </div>

                  <Separator />

                  <div className="space-y-2">
                    <Label className="text-sm font-medium">
                      {parentData.formulaInfo.contributorOf.formulaType ===
                      "TOTAL_FORMULA"
                        ? "All Children in Total Formula"
                        : "All Formula Rules"}{" "}
                      ({parentData.formulaInfo.contributorOf.allRules.length})
                    </Label>
                    <div className="space-y-2">
                      {parentData.formulaInfo.contributorOf.allRules.map(
                        (rule: any, idx: number) => {
                          const isMyRule =
                            rule.hierarchyId ===
                            parentData.hierarchy.hierarchyId;
                          const ruleHierarchy = availableHierarchies.find(
                            (h) => h.hierarchyId === rule.hierarchyId
                          );
                          return (
                            <div
                              key={idx}
                              className={cn(
                                "flex items-center justify-between p-3 rounded-md",
                                isMyRule ? "bg-muted" : "bg-muted/50"
                              )}
                            >
                              <div className="flex flex-col gap-1 flex-1">
                                <div className="flex items-center gap-3">
                                  <Badge
                                    variant={isMyRule ? "default" : "outline"}
                                    className={isMyRule ? "bg-blue-600" : ""}
                                  >
                                    {rule.operation}
                                  </Badge>
                                  <span
                                    className={cn(
                                      "text-sm",
                                      isMyRule ? "font-semibold" : "font-medium"
                                    )}
                                  >
                                    {rule.hierarchyName}
                                    {isMyRule && " (This Hierarchy)"}
                                  </span>
                                  {ruleHierarchy && (
                                    <Badge
                                      variant="outline"
                                      className="text-xs px-1.5 py-0.5"
                                    >
                                      Level {getHierarchyLevel(ruleHierarchy)}
                                    </Badge>
                                  )}
                                </div>
                                {ruleHierarchy && (
                                  <span className="text-xs text-muted-foreground ml-2">
                                    {getHierarchyPath(ruleHierarchy)}
                                  </span>
                                )}
                              </div>
                              {rule.FORMULA_PRECEDENCE && (
                                <Badge variant="secondary" className="text-xs">
                                  Precedence: {rule.FORMULA_PRECEDENCE}
                                </Badge>
                              )}
                            </div>
                          );
                        }
                      )}
                    </div>
                  </div>
                </CardContent>
              </Card>
            )}
        </div>
      )}

      {/* Children Hierarchies Section */}
      {childrenData.length > 0 && (
        <Card className="border-dashed">
          <CardHeader>
            <CardTitle className="text-base">
              Child Hierarchies with Formulas
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {childrenData.map((data) => (
                <div
                  key={data.hierarchy.hierarchyId}
                  className="flex items-center justify-between p-3 bg-muted/30 rounded-md"
                >
                  <div className="flex items-center gap-3">
                    <Badge variant="outline" className="text-xs">
                      Child
                    </Badge>
                    <span className="text-sm font-medium">
                      {data.hierarchy.hierarchyName}
                    </span>
                  </div>
                  <div className="flex items-center gap-2">
                    {data.formulaInfo?.isFormulaOwner && (
                      <Badge variant="default" className="bg-green-600 text-xs">
                        Owner
                      </Badge>
                    )}
                    {data.formulaInfo?.isContributor && (
                      <Badge
                        variant="secondary"
                        className="bg-blue-600 text-white text-xs"
                      >
                        Contributor
                      </Badge>
                    )}
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}
    </div>
  );
};
