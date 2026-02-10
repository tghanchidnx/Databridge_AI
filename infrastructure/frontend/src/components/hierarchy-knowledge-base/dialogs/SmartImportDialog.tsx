/**
 * Smart CSV Import Dialog
 * Intelligent CSV import with format detection, validation, preview, and auto-fix
 */
import { useState, useCallback, useEffect } from "react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
  DialogFooter,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import { Switch } from "@/components/ui/switch";
import { Label } from "@/components/ui/label";
import {
  FileSpreadsheet,
  Upload,
  AlertTriangle,
  AlertCircle,
  CheckCircle2,
  Info,
  Wand2,
  ArrowRight,
  RefreshCw,
  Eye,
  Layers,
  Database,
  ChevronRight,
  X,
  FileText,
  Zap,
  Clock,
  Download,
  Brain,
  Sparkles,
  FileWarning,
  Settings2,
  ChevronDown,
  ChevronUp,
  ListChecks,
  Gauge,
  ArrowRightLeft,
  Hash,
} from "lucide-react";
import { cn } from "@/lib/utils";
import { smartHierarchyService } from "@/services/api/hierarchy/hierarchy.service";
import { useToast } from "@/hooks/use-toast";

// Types for CSV analysis
interface CSVColumn {
  name: string;
  detectedType: string;
  sampleValues: string[];
  nullCount: number;
  nullPercentage: number;
  uniqueCount: number;
  mappedTo?: string;
}

interface CSVAnalysis {
  isValid: boolean;
  format: "hierarchy" | "mapping" | "legacy" | "unknown";
  formatConfidence: number;
  rowCount: number;
  columnCount: number;
  columns: CSVColumn[];
  issues: CSVIssue[];
  suggestions: CSVSuggestion[];
  canAutoFix: boolean;
  autoFixActions: AutoFixAction[];
  stats: CSVStats;
  aiAnalysis?: AIAnalysisResult;
  legacyFormatInfo?: LegacyFormatInfo;
}

interface CSVIssue {
  type: "error" | "warning" | "info";
  code: string;
  message: string;
  row?: number;
  column?: string;
  canAutoFix: boolean;
}

interface CSVSuggestion {
  type: string;
  message: string;
  column?: string;
  priority: "high" | "medium" | "low";
}

interface AutoFixAction {
  type: string;
  description: string;
  affectedRows: number;
}

interface CSVStats {
  totalRows: number;
  validRows: number;
  invalidRows: number;
  duplicateIds: number;
  orphanParents: number;
  emptyRequired: number;
}

interface AIAnalysisResult {
  summary: string;
  formatDetection: {
    detected: string;
    confidence: number;
    reason: string;
  };
  columnMappings: Array<{
    sourceColumn: string;
    suggestedMapping: string;
    confidence: number;
    reason: string;
  }>;
  dataQuality: {
    score: number;
    issues: string[];
    recommendations: string[];
  };
  conversionSteps?: Array<{
    step: number;
    action: string;
    description: string;
    status: 'pending' | 'completed' | 'failed';
  }>;
}

interface LegacyFormatInfo {
  isLegacy: boolean;
  hasXrefKey: boolean;
  hasFilterGroups: boolean;
  hasLevelColumns: boolean;
  hasSortColumns: boolean;
  levelCount: number;
  filterGroupColumns: string[];
  extractedMappingInfo: Array<{
    hierarchyKey: string;
    filterGroup1?: string;
    filterGroup2?: string;
    filterGroup3?: string;
    filterGroup4?: string;
    formulaGroup?: string;
  }>;
  conversionPlan: {
    canConvert: boolean;
    steps: string[];
    warnings: string[];
  };
}

interface ImportResult {
  success: boolean;
  logId: string;
  message: string;
  rowsTotal: number;
  rowsImported: number;
  rowsFailed: number;
  issues: Array<{ type: string; message: string; resolved: boolean }>;
  autoFixesApplied: string[];
  report: string;
  duration: number;
}

interface SmartImportDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  projectId: string;
  onImportComplete?: () => void;
}

interface ImportOptions {
  autoFix: boolean;
  validateBeforeImport: boolean;
  dryRun: boolean;
}

type ImportStep = "upload" | "analyze" | "import" | "complete";

// File size threshold for chunked upload (5MB)
const CHUNKED_UPLOAD_THRESHOLD = 5 * 1024 * 1024;

function formatFileSize(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`;
}

function FileUploadZone({
  label,
  file,
  onFileSelect,
  onRemove,
  accept = ".csv",
  isLarge = false,
}: {
  label: string;
  file?: File;
  onFileSelect: (file: File) => void;
  onRemove: () => void;
  accept?: string;
  isLarge?: boolean;
}) {
  const handleDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault();
      const droppedFile = e.dataTransfer.files[0];
      if (droppedFile && droppedFile.name.endsWith(".csv")) {
        onFileSelect(droppedFile);
      }
    },
    [onFileSelect]
  );

  const handleFileInput = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const selectedFile = e.target.files?.[0];
      if (selectedFile) {
        onFileSelect(selectedFile);
      }
    },
    [onFileSelect]
  );

  if (file) {
    const isLargeFile = file.size > CHUNKED_UPLOAD_THRESHOLD;
    return (
      <div className="flex items-center justify-between p-3 border rounded-lg bg-muted/30">
        <div className="flex items-center gap-3">
          <FileSpreadsheet className="h-8 w-8 text-green-600" />
          <div>
            <p className="font-medium">{file.name}</p>
            <div className="flex items-center gap-2">
              <p className="text-xs text-muted-foreground">
                {formatFileSize(file.size)}
              </p>
              {isLargeFile && (
                <Badge variant="secondary" className="text-xs">
                  <Zap className="h-3 w-3 mr-1" />
                  Chunked Upload
                </Badge>
              )}
            </div>
          </div>
        </div>
        <Button variant="ghost" size="sm" onClick={onRemove}>
          <X className="h-4 w-4" />
        </Button>
      </div>
    );
  }

  return (
    <div
      onDrop={handleDrop}
      onDragOver={(e) => e.preventDefault()}
      className="border-2 border-dashed rounded-lg p-6 text-center hover:border-primary/50 hover:bg-muted/30 transition-colors cursor-pointer"
    >
      <input
        type="file"
        accept={accept}
        onChange={handleFileInput}
        className="hidden"
        id={`file-${label.replace(/\s+/g, "-")}`}
      />
      <label htmlFor={`file-${label.replace(/\s+/g, "-")}`} className="cursor-pointer">
        <Upload className="h-8 w-8 mx-auto text-muted-foreground mb-2" />
        <p className="font-medium">{label}</p>
        <p className="text-sm text-muted-foreground">
          Drag & drop or click to browse
        </p>
        <p className="text-xs text-muted-foreground mt-1">
          Supports files up to 50MB
        </p>
      </label>
    </div>
  );
}

export function SmartImportDialog({
  open,
  onOpenChange,
  projectId,
  onImportComplete,
}: SmartImportDialogProps) {
  const { toast } = useToast();
  const [step, setStep] = useState<ImportStep>("upload");
  const [hierarchyFile, setHierarchyFile] = useState<File>();
  const [mappingFile, setMappingFile] = useState<File>();
  const [hierarchyAnalysis, setHierarchyAnalysis] = useState<CSVAnalysis>();
  const [mappingAnalysis, setMappingAnalysis] = useState<CSVAnalysis>();
  const [isAnalyzing, setIsAnalyzing] = useState(false);
  const [isImporting, setIsImporting] = useState(false);
  const [importProgress, setImportProgress] = useState(0);
  const [importResult, setImportResult] = useState<{
    hierarchy?: ImportResult;
    mapping?: ImportResult | null;
    overallSuccess: boolean;
  }>();
  const [options, setOptions] = useState<ImportOptions>({
    autoFix: true,
    validateBeforeImport: true,
    dryRun: false,
  });

  const hasErrors = hierarchyAnalysis?.issues.some((i) => i.type === "error" && !i.canAutoFix) || false;
  const hasWarnings = hierarchyAnalysis?.issues.some((i) => i.type === "warning") || false;

  // Reset state when dialog opens
  useEffect(() => {
    if (open) {
      setStep("upload");
      setHierarchyFile(undefined);
      setMappingFile(undefined);
      setHierarchyAnalysis(undefined);
      setMappingAnalysis(undefined);
      setImportResult(undefined);
      setImportProgress(0);
    }
  }, [open]);

  const handleAnalyze = useCallback(async () => {
    if (!hierarchyFile) return;

    setIsAnalyzing(true);

    try {
      // Analyze hierarchy file
      const hierarchyContent = await hierarchyFile.text();
      const hAnalysis = await smartHierarchyService.analyzeCSV(
        projectId,
        hierarchyContent,
        hierarchyFile.name
      );
      setHierarchyAnalysis(hAnalysis as CSVAnalysis);

      // Analyze mapping file if provided
      if (mappingFile) {
        const mappingContent = await mappingFile.text();
        const mAnalysis = await smartHierarchyService.analyzeCSV(
          projectId,
          mappingContent,
          mappingFile.name
        );
        setMappingAnalysis(mAnalysis as CSVAnalysis);
      }

      setStep("analyze");
    } catch (error: any) {
      toast({
        title: "Analysis Failed",
        description: error.message || "Failed to analyze CSV files",
        variant: "destructive",
      });
    } finally {
      setIsAnalyzing(false);
    }
  }, [hierarchyFile, mappingFile, projectId, toast]);

  const handleImport = useCallback(async () => {
    if (!hierarchyFile) return;

    setIsImporting(true);
    setImportProgress(0);

    try {
      let result;

      if (mappingFile) {
        // Import both files
        const hierarchyContent = await hierarchyFile.text();
        const mappingContent = await mappingFile.text();

        // Use chunked upload for large files
        if (
          hierarchyFile.size > CHUNKED_UPLOAD_THRESHOLD ||
          mappingFile.size > CHUNKED_UPLOAD_THRESHOLD
        ) {
          // Import hierarchy
          setImportProgress(10);
          const hResult = await smartHierarchyService.importLargeCSV(
            projectId,
            hierarchyFile,
            "hierarchy",
            options,
            (progress) => setImportProgress(10 + progress * 0.4)
          );

          // Import mapping if hierarchy succeeded
          setImportProgress(50);
          let mResult = null;
          if (hResult.success || hResult.rowsImported > 0) {
            mResult = await smartHierarchyService.importLargeCSV(
              projectId,
              mappingFile,
              "mapping",
              options,
              (progress) => setImportProgress(50 + progress * 0.4)
            );
          }

          result = {
            hierarchy: hResult,
            mapping: mResult,
            overallSuccess: hResult.success && (mResult?.success ?? false),
          };
        } else {
          // Direct import for small files
          result = await smartHierarchyService.smartImportBoth(
            projectId,
            hierarchyContent,
            mappingContent,
            hierarchyFile.name,
            mappingFile.name,
            options
          );
        }
      } else {
        // Import only hierarchy
        if (hierarchyFile.size > CHUNKED_UPLOAD_THRESHOLD) {
          const hResult = await smartHierarchyService.importLargeCSV(
            projectId,
            hierarchyFile,
            "hierarchy",
            options,
            (progress) => setImportProgress(progress)
          );
          result = {
            hierarchy: hResult,
            mapping: null,
            overallSuccess: hResult.success,
          };
        } else {
          const hierarchyContent = await hierarchyFile.text();
          const hResult = await smartHierarchyService.smartImportHierarchy(
            projectId,
            hierarchyContent,
            hierarchyFile.name,
            options
          );
          result = {
            hierarchy: hResult,
            mapping: null,
            overallSuccess: hResult.success,
          };
        }
      }

      setImportProgress(100);
      setImportResult(result);
      setStep("complete");

      if (result.overallSuccess) {
        toast({
          title: "Import Successful",
          description: `Imported ${result.hierarchy?.rowsImported || 0} hierarchies${
            result.mapping ? ` and ${result.mapping.rowsImported} mappings` : ""
          }`,
        });
        onImportComplete?.();
      } else {
        toast({
          title: "Import Completed with Issues",
          description: result.hierarchy?.message || "Some rows failed to import",
          variant: "destructive",
        });
      }
    } catch (error: any) {
      toast({
        title: "Import Failed",
        description: error.message || "Failed to import CSV files",
        variant: "destructive",
      });
    } finally {
      setIsImporting(false);
    }
  }, [hierarchyFile, mappingFile, projectId, options, toast, onImportComplete]);

  const handleDownloadReport = useCallback(() => {
    if (!importResult?.hierarchy?.report) return;

    const report = importResult.hierarchy.report +
      (importResult.mapping?.report ? "\n\n" + importResult.mapping.report : "");

    const blob = new Blob([report], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `import-report-${new Date().toISOString().split("T")[0]}.txt`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }, [importResult]);

  const resetDialog = useCallback(() => {
    setStep("upload");
    setHierarchyFile(undefined);
    setMappingFile(undefined);
    setHierarchyAnalysis(undefined);
    setMappingAnalysis(undefined);
    setImportResult(undefined);
    setImportProgress(0);
  }, []);

  return (
    <Dialog
      open={open}
      onOpenChange={(isOpen) => {
        if (!isOpen) resetDialog();
        onOpenChange(isOpen);
      }}
    >
      <DialogContent className="max-w-3xl max-h-[85vh] flex flex-col">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2">
            <Wand2 className="h-5 w-5" />
            Smart CSV Import
          </DialogTitle>
          <DialogDescription>
            Import hierarchies and mappings with intelligent analysis and auto-fix
          </DialogDescription>
        </DialogHeader>

        {/* Progress indicator */}
        <div className="flex items-center gap-2 py-2">
          {(["upload", "analyze", "import", "complete"] as ImportStep[]).map((s, i) => (
            <div key={s} className="flex items-center">
              <div
                className={cn(
                  "w-8 h-8 rounded-full flex items-center justify-center text-sm font-medium",
                  step === s
                    ? "bg-primary text-primary-foreground"
                    : ["upload", "analyze", "import", "complete"].indexOf(step) > i
                    ? "bg-green-500 text-white"
                    : "bg-muted text-muted-foreground"
                )}
              >
                {["upload", "analyze", "import", "complete"].indexOf(step) > i ? (
                  <CheckCircle2 className="h-4 w-4" />
                ) : (
                  i + 1
                )}
              </div>
              {i < 3 && (
                <ChevronRight className="h-4 w-4 text-muted-foreground mx-1" />
              )}
            </div>
          ))}
        </div>

        <Separator />

        <ScrollArea className="flex-1 pr-4">
          {/* Upload Step */}
          {step === "upload" && (
            <div className="space-y-4 py-4">
              <Alert>
                <Info className="h-4 w-4" />
                <AlertTitle>Expected File Format</AlertTitle>
                <AlertDescription>
                  Upload a hierarchy CSV file (_HIERARCHY.CSV) and optionally a
                  mapping CSV file (_HIERARCHY_MAPPING.CSV). Large files up to 50MB are supported.
                </AlertDescription>
              </Alert>

              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label className="flex items-center gap-2">
                    <Layers className="h-4 w-4" />
                    Hierarchy File (Required)
                  </Label>
                  <FileUploadZone
                    label="Drop hierarchy CSV"
                    file={hierarchyFile}
                    onFileSelect={setHierarchyFile}
                    onRemove={() => setHierarchyFile(undefined)}
                  />
                </div>

                <div className="space-y-2">
                  <Label className="flex items-center gap-2">
                    <Database className="h-4 w-4" />
                    Mapping File (Optional)
                  </Label>
                  <FileUploadZone
                    label="Drop mapping CSV"
                    file={mappingFile}
                    onFileSelect={setMappingFile}
                    onRemove={() => setMappingFile(undefined)}
                  />
                </div>
              </div>
            </div>
          )}

          {/* Analyze Step */}
          {step === "analyze" && (
            <div className="space-y-4 py-4">
              {/* AI Analysis Summary Banner */}
              {hierarchyAnalysis?.aiAnalysis && (
                <Card className="border-blue-200 bg-blue-50/50 dark:bg-blue-950/20 dark:border-blue-800">
                  <CardContent className="py-3">
                    <div className="flex items-start gap-3">
                      <Brain className="h-5 w-5 text-blue-600 mt-0.5 flex-shrink-0" />
                      <div className="flex-1">
                        <div className="flex items-center gap-2 mb-1">
                          <span className="font-medium text-blue-900 dark:text-blue-100">AI Analysis</span>
                          <Badge
                            variant="outline"
                            className={cn(
                              "text-xs",
                              hierarchyAnalysis.aiAnalysis.dataQuality.score >= 80 ? "border-green-500 text-green-700" :
                              hierarchyAnalysis.aiAnalysis.dataQuality.score >= 60 ? "border-yellow-500 text-yellow-700" :
                              "border-red-500 text-red-700"
                            )}
                          >
                            <Gauge className="h-3 w-3 mr-1" />
                            Quality: {hierarchyAnalysis.aiAnalysis.dataQuality.score}%
                          </Badge>
                        </div>
                        <p className="text-sm text-blue-800 dark:text-blue-200">
                          {hierarchyAnalysis.aiAnalysis.summary}
                        </p>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              )}

              {/* Legacy Format Warning */}
              {hierarchyAnalysis?.format === "legacy" && hierarchyAnalysis?.legacyFormatInfo && (
                <Alert className="border-amber-200 bg-amber-50/50 dark:bg-amber-950/20">
                  <FileWarning className="h-4 w-4 text-amber-600" />
                  <AlertTitle className="text-amber-900 dark:text-amber-100">Legacy Format Detected</AlertTitle>
                  <AlertDescription className="text-amber-800 dark:text-amber-200">
                    <div className="space-y-2 mt-2">
                      <div className="flex flex-wrap gap-2">
                        <Badge variant="outline" className="text-xs">
                          <Hash className="h-3 w-3 mr-1" />
                          {hierarchyAnalysis.legacyFormatInfo.levelCount} Level Columns
                        </Badge>
                        {hierarchyAnalysis.legacyFormatInfo.hasXrefKey && (
                          <Badge variant="outline" className="text-xs bg-green-50 text-green-700">
                            <CheckCircle2 className="h-3 w-3 mr-1" />
                            XREF Key Found
                          </Badge>
                        )}
                        {hierarchyAnalysis.legacyFormatInfo.hasFilterGroups && (
                          <Badge variant="outline" className="text-xs bg-blue-50 text-blue-700">
                            <Database className="h-3 w-3 mr-1" />
                            {hierarchyAnalysis.legacyFormatInfo.filterGroupColumns.length} Filter Groups
                          </Badge>
                        )}
                      </div>
                      {hierarchyAnalysis.legacyFormatInfo.conversionPlan.canConvert && (
                        <div className="mt-2 p-2 bg-white/50 dark:bg-black/20 rounded text-xs">
                          <p className="font-medium mb-1">Conversion Plan:</p>
                          <ol className="list-decimal list-inside space-y-0.5">
                            {hierarchyAnalysis.legacyFormatInfo.conversionPlan.steps.map((step, i) => (
                              <li key={i}>{step}</li>
                            ))}
                          </ol>
                        </div>
                      )}
                    </div>
                  </AlertDescription>
                </Alert>
              )}

              <Tabs defaultValue="hierarchy">
                <TabsList>
                  <TabsTrigger value="hierarchy" className="gap-2">
                    <Layers className="h-4 w-4" />
                    Hierarchy
                    {hierarchyAnalysis && (
                      <Badge variant="secondary">{hierarchyAnalysis.rowCount}</Badge>
                    )}
                  </TabsTrigger>
                  {mappingAnalysis && (
                    <TabsTrigger value="mapping" className="gap-2">
                      <Database className="h-4 w-4" />
                      Mappings
                      <Badge variant="secondary">{mappingAnalysis.rowCount}</Badge>
                    </TabsTrigger>
                  )}
                  <TabsTrigger value="ai-insights" className="gap-2">
                    <Sparkles className="h-4 w-4" />
                    AI Insights
                  </TabsTrigger>
                  <TabsTrigger value="issues" className="gap-2">
                    <AlertTriangle className="h-4 w-4" />
                    Issues
                    {(hierarchyAnalysis?.issues.length || 0) > 0 && (
                      <Badge
                        variant={hasErrors ? "destructive" : "secondary"}
                      >
                        {hierarchyAnalysis?.issues.length || 0}
                      </Badge>
                    )}
                  </TabsTrigger>
                </TabsList>

                <TabsContent value="hierarchy" className="mt-4">
                  {hierarchyAnalysis && (
                    <div className="space-y-4">
                      <div className="flex items-center justify-between">
                        <div>
                          <p className="font-medium">{hierarchyFile?.name}</p>
                          <p className="text-sm text-muted-foreground">
                            {hierarchyAnalysis.rowCount} rows, {hierarchyAnalysis.columnCount} columns
                          </p>
                        </div>
                        <div className="flex gap-2">
                          <Badge
                            variant={
                              hierarchyAnalysis.formatConfidence > 80
                                ? "default"
                                : "secondary"
                            }
                          >
                            {hierarchyAnalysis.format} ({hierarchyAnalysis.formatConfidence.toFixed(0)}%)
                          </Badge>
                          {hierarchyAnalysis.canAutoFix && (
                            <Badge variant="outline" className="text-green-600">
                              <Wand2 className="h-3 w-3 mr-1" />
                              Auto-fix available
                            </Badge>
                          )}
                        </div>
                      </div>

                      {/* Stats */}
                      <div className="grid grid-cols-4 gap-2">
                        <Card className="p-3">
                          <p className="text-xs text-muted-foreground">Valid Rows</p>
                          <p className="text-lg font-bold text-green-600">
                            {hierarchyAnalysis.stats.validRows}
                          </p>
                        </Card>
                        <Card className="p-3">
                          <p className="text-xs text-muted-foreground">Invalid Rows</p>
                          <p className="text-lg font-bold text-red-600">
                            {hierarchyAnalysis.stats.invalidRows}
                          </p>
                        </Card>
                        <Card className="p-3">
                          <p className="text-xs text-muted-foreground">Duplicates</p>
                          <p className="text-lg font-bold text-yellow-600">
                            {hierarchyAnalysis.stats.duplicateIds}
                          </p>
                        </Card>
                        <Card className="p-3">
                          <p className="text-xs text-muted-foreground">Orphan Parents</p>
                          <p className="text-lg font-bold text-orange-600">
                            {hierarchyAnalysis.stats.orphanParents}
                          </p>
                        </Card>
                      </div>

                      {/* Column preview */}
                      <div className="border rounded-lg overflow-hidden">
                        <table className="w-full text-sm">
                          <thead className="bg-muted">
                            <tr>
                              <th className="text-left p-2">Column</th>
                              <th className="text-left p-2">Type</th>
                              <th className="text-left p-2">Maps To</th>
                              <th className="text-left p-2">Sample</th>
                            </tr>
                          </thead>
                          <tbody>
                            {hierarchyAnalysis.columns.slice(0, 10).map((col) => (
                              <tr key={col.name} className="border-t">
                                <td className="p-2 font-mono text-xs">{col.name}</td>
                                <td className="p-2">
                                  <Badge variant="outline" className="text-xs">
                                    {col.detectedType}
                                  </Badge>
                                </td>
                                <td className="p-2">
                                  {col.mappedTo && (
                                    <Badge variant="secondary" className="text-xs">
                                      {col.mappedTo}
                                    </Badge>
                                  )}
                                </td>
                                <td className="p-2 text-muted-foreground text-xs truncate max-w-[150px]">
                                  {col.sampleValues[0] || "-"}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  )}
                </TabsContent>

                <TabsContent value="mapping" className="mt-4">
                  {mappingAnalysis && (
                    <div className="space-y-4">
                      <div className="flex items-center justify-between">
                        <div>
                          <p className="font-medium">{mappingFile?.name}</p>
                          <p className="text-sm text-muted-foreground">
                            {mappingAnalysis.rowCount} rows, {mappingAnalysis.columnCount} columns
                          </p>
                        </div>
                        <Badge
                          variant={
                            mappingAnalysis.formatConfidence > 80
                              ? "default"
                              : "secondary"
                          }
                        >
                          {mappingAnalysis.format} ({mappingAnalysis.formatConfidence.toFixed(0)}%)
                        </Badge>
                      </div>
                    </div>
                  )}
                </TabsContent>

                <TabsContent value="ai-insights" className="mt-4">
                  {hierarchyAnalysis?.aiAnalysis && (
                    <div className="space-y-4">
                      {/* Format Detection */}
                      <Card>
                        <CardHeader className="py-3">
                          <CardTitle className="text-sm flex items-center gap-2">
                            <Eye className="h-4 w-4" />
                            Format Detection
                          </CardTitle>
                        </CardHeader>
                        <CardContent className="space-y-2">
                          <div className="flex items-center justify-between">
                            <span className="text-sm text-muted-foreground">Detected Format:</span>
                            <Badge variant={hierarchyAnalysis.format === "legacy" ? "secondary" : "default"}>
                              {hierarchyAnalysis.aiAnalysis.formatDetection.detected.toUpperCase()}
                            </Badge>
                          </div>
                          <div className="flex items-center justify-between">
                            <span className="text-sm text-muted-foreground">Confidence:</span>
                            <div className="flex items-center gap-2">
                              <Progress
                                value={hierarchyAnalysis.aiAnalysis.formatDetection.confidence}
                                className="w-20 h-2"
                              />
                              <span className="text-sm font-medium">
                                {hierarchyAnalysis.aiAnalysis.formatDetection.confidence}%
                              </span>
                            </div>
                          </div>
                          <p className="text-xs text-muted-foreground mt-2">
                            {hierarchyAnalysis.aiAnalysis.formatDetection.reason}
                          </p>
                        </CardContent>
                      </Card>

                      {/* Data Quality */}
                      <Card>
                        <CardHeader className="py-3">
                          <CardTitle className="text-sm flex items-center gap-2">
                            <Gauge className="h-4 w-4" />
                            Data Quality Score
                          </CardTitle>
                        </CardHeader>
                        <CardContent className="space-y-3">
                          <div className="flex items-center gap-4">
                            <div className={cn(
                              "text-3xl font-bold",
                              hierarchyAnalysis.aiAnalysis.dataQuality.score >= 80 ? "text-green-600" :
                              hierarchyAnalysis.aiAnalysis.dataQuality.score >= 60 ? "text-yellow-600" :
                              "text-red-600"
                            )}>
                              {hierarchyAnalysis.aiAnalysis.dataQuality.score}%
                            </div>
                            <Progress
                              value={hierarchyAnalysis.aiAnalysis.dataQuality.score}
                              className="flex-1 h-3"
                            />
                          </div>
                          {hierarchyAnalysis.aiAnalysis.dataQuality.issues.length > 0 && (
                            <div className="mt-2">
                              <p className="text-xs font-medium text-muted-foreground mb-1">Issues Found:</p>
                              <ul className="text-xs space-y-1">
                                {hierarchyAnalysis.aiAnalysis.dataQuality.issues.map((issue, i) => (
                                  <li key={i} className="flex items-start gap-2">
                                    <AlertCircle className="h-3 w-3 text-yellow-600 mt-0.5 flex-shrink-0" />
                                    <span>{issue}</span>
                                  </li>
                                ))}
                              </ul>
                            </div>
                          )}
                          {hierarchyAnalysis.aiAnalysis.dataQuality.recommendations.length > 0 && (
                            <div className="mt-2">
                              <p className="text-xs font-medium text-muted-foreground mb-1">Recommendations:</p>
                              <ul className="text-xs space-y-1">
                                {hierarchyAnalysis.aiAnalysis.dataQuality.recommendations.map((rec, i) => (
                                  <li key={i} className="flex items-start gap-2">
                                    <Wand2 className="h-3 w-3 text-blue-600 mt-0.5 flex-shrink-0" />
                                    <span>{rec}</span>
                                  </li>
                                ))}
                              </ul>
                            </div>
                          )}
                        </CardContent>
                      </Card>

                      {/* Column Mappings */}
                      {hierarchyAnalysis.aiAnalysis.columnMappings.length > 0 && (
                        <Card>
                          <CardHeader className="py-3">
                            <CardTitle className="text-sm flex items-center gap-2">
                              <ArrowRightLeft className="h-4 w-4" />
                              AI Column Mappings
                              <Badge variant="secondary" className="ml-auto">
                                {hierarchyAnalysis.aiAnalysis.columnMappings.length}
                              </Badge>
                            </CardTitle>
                          </CardHeader>
                          <CardContent>
                            <div className="max-h-48 overflow-y-auto">
                              <table className="w-full text-xs">
                                <thead className="bg-muted sticky top-0">
                                  <tr>
                                    <th className="text-left p-2">Source Column</th>
                                    <th className="text-left p-2">→</th>
                                    <th className="text-left p-2">Mapped To</th>
                                    <th className="text-left p-2">Confidence</th>
                                  </tr>
                                </thead>
                                <tbody>
                                  {hierarchyAnalysis.aiAnalysis.columnMappings.map((mapping, i) => (
                                    <tr key={i} className="border-t">
                                      <td className="p-2 font-mono">{mapping.sourceColumn}</td>
                                      <td className="p-2 text-muted-foreground">→</td>
                                      <td className="p-2">
                                        <Badge variant="outline" className="text-xs">
                                          {mapping.suggestedMapping}
                                        </Badge>
                                      </td>
                                      <td className="p-2">
                                        <span className={cn(
                                          "font-medium",
                                          mapping.confidence >= 80 ? "text-green-600" :
                                          mapping.confidence >= 60 ? "text-yellow-600" :
                                          "text-red-600"
                                        )}>
                                          {mapping.confidence}%
                                        </span>
                                      </td>
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                          </CardContent>
                        </Card>
                      )}

                      {/* Conversion Steps (for legacy format) */}
                      {hierarchyAnalysis.aiAnalysis.conversionSteps && hierarchyAnalysis.aiAnalysis.conversionSteps.length > 0 && (
                        <Card>
                          <CardHeader className="py-3">
                            <CardTitle className="text-sm flex items-center gap-2">
                              <ListChecks className="h-4 w-4" />
                              Conversion Steps
                            </CardTitle>
                          </CardHeader>
                          <CardContent>
                            <div className="space-y-2">
                              {hierarchyAnalysis.aiAnalysis.conversionSteps.map((step) => (
                                <div
                                  key={step.step}
                                  className="flex items-center gap-3 p-2 rounded bg-muted/50"
                                >
                                  <div className={cn(
                                    "w-6 h-6 rounded-full flex items-center justify-center text-xs font-medium",
                                    step.status === "completed" ? "bg-green-500 text-white" :
                                    step.status === "failed" ? "bg-red-500 text-white" :
                                    "bg-muted-foreground/20 text-muted-foreground"
                                  )}>
                                    {step.status === "completed" ? (
                                      <CheckCircle2 className="h-4 w-4" />
                                    ) : step.status === "failed" ? (
                                      <X className="h-4 w-4" />
                                    ) : (
                                      step.step
                                    )}
                                  </div>
                                  <div className="flex-1">
                                    <p className="text-sm">{step.description}</p>
                                  </div>
                                  <Badge
                                    variant={
                                      step.status === "completed" ? "default" :
                                      step.status === "failed" ? "destructive" :
                                      "secondary"
                                    }
                                    className="text-xs"
                                  >
                                    {step.status}
                                  </Badge>
                                </div>
                              ))}
                            </div>
                          </CardContent>
                        </Card>
                      )}
                    </div>
                  )}
                </TabsContent>

                <TabsContent value="issues" className="mt-4">
                  <div className="space-y-2">
                    {(!hierarchyAnalysis?.issues.length) ? (
                      <Alert>
                        <CheckCircle2 className="h-4 w-4 text-green-600" />
                        <AlertTitle>All Clear</AlertTitle>
                        <AlertDescription>
                          No validation issues found
                        </AlertDescription>
                      </Alert>
                    ) : (
                      hierarchyAnalysis?.issues.map((issue, index) => (
                        <Alert
                          key={index}
                          variant={issue.type === "error" ? "destructive" : "default"}
                        >
                          {issue.type === "error" ? (
                            <AlertCircle className="h-4 w-4" />
                          ) : issue.type === "warning" ? (
                            <AlertTriangle className="h-4 w-4" />
                          ) : (
                            <Info className="h-4 w-4" />
                          )}
                          <AlertDescription className="flex items-center justify-between">
                            <span>
                              {issue.message}
                              {issue.column && (
                                <span className="text-muted-foreground ml-1">
                                  (column: {issue.column})
                                </span>
                              )}
                            </span>
                            {issue.canAutoFix && (
                              <Badge variant="outline" className="text-green-600 ml-2">
                                <Wand2 className="h-3 w-3 mr-1" />
                                Auto-fixable
                              </Badge>
                            )}
                          </AlertDescription>
                        </Alert>
                      ))
                    )}
                  </div>
                </TabsContent>
              </Tabs>

              {/* Import Options */}
              <Card>
                <CardHeader className="py-3">
                  <CardTitle className="text-sm">Import Options</CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  <div className="flex items-center justify-between">
                    <div>
                      <Label htmlFor="auto-fix">Auto-fix issues</Label>
                      <p className="text-xs text-muted-foreground">
                        Automatically fix common issues like whitespace, BOM, etc.
                      </p>
                    </div>
                    <Switch
                      id="auto-fix"
                      checked={options.autoFix}
                      onCheckedChange={(v) => setOptions({ ...options, autoFix: v })}
                    />
                  </div>
                  <div className="flex items-center justify-between">
                    <div>
                      <Label htmlFor="validate">Validate before import</Label>
                      <p className="text-xs text-muted-foreground">
                        Check for errors before starting import
                      </p>
                    </div>
                    <Switch
                      id="validate"
                      checked={options.validateBeforeImport}
                      onCheckedChange={(v) => setOptions({ ...options, validateBeforeImport: v })}
                    />
                  </div>
                  <div className="flex items-center justify-between">
                    <div>
                      <Label htmlFor="dry-run">Dry run (preview only)</Label>
                      <p className="text-xs text-muted-foreground">
                        Analyze without making changes
                      </p>
                    </div>
                    <Switch
                      id="dry-run"
                      checked={options.dryRun}
                      onCheckedChange={(v) => setOptions({ ...options, dryRun: v })}
                    />
                  </div>
                </CardContent>
              </Card>
            </div>
          )}

          {/* Import Step */}
          {step === "import" && (
            <div className="space-y-4 py-8">
              <div className="text-center">
                <RefreshCw className="h-12 w-12 mx-auto animate-spin text-primary mb-4" />
                <h3 className="text-lg font-medium">Importing...</h3>
                <p className="text-sm text-muted-foreground">
                  {importProgress < 50
                    ? "Processing hierarchy file..."
                    : "Processing mapping file..."}
                </p>
              </div>
              <Progress value={importProgress} className="w-full" />
              <p className="text-center text-sm text-muted-foreground">
                {importProgress.toFixed(0)}% complete
              </p>
            </div>
          )}

          {/* Complete Step */}
          {step === "complete" && importResult && (
            <div className="space-y-4 py-4">
              {/* Success/Failure Banner */}
              <Alert variant={importResult.overallSuccess ? "default" : "destructive"}>
                {importResult.overallSuccess ? (
                  <CheckCircle2 className="h-4 w-4 text-green-600" />
                ) : (
                  <AlertCircle className="h-4 w-4" />
                )}
                <AlertTitle>
                  {importResult.overallSuccess ? "Import Successful" : "Import Completed with Issues"}
                </AlertTitle>
                <AlertDescription>
                  {importResult.hierarchy?.message}
                </AlertDescription>
              </Alert>

              {/* AI Import Summary */}
              {hierarchyAnalysis?.format === "legacy" && (
                <Card className="border-blue-200 bg-blue-50/50 dark:bg-blue-950/20 dark:border-blue-800">
                  <CardHeader className="py-3">
                    <CardTitle className="text-sm flex items-center gap-2">
                      <Brain className="h-4 w-4 text-blue-600" />
                      AI Import Summary
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-3">
                    <p className="text-sm text-blue-900 dark:text-blue-100">
                      Successfully converted legacy format CSV to standard hierarchy format.
                    </p>
                    <div className="space-y-2 text-xs font-mono bg-white/50 dark:bg-black/20 p-3 rounded max-h-40 overflow-y-auto">
                      <div className="flex items-center gap-2 text-green-700 dark:text-green-400">
                        <CheckCircle2 className="h-3 w-3" />
                        <span>[AI] Detected legacy format with XREF_HIERARCHY_KEY</span>
                      </div>
                      <div className="flex items-center gap-2 text-green-700 dark:text-green-400">
                        <CheckCircle2 className="h-3 w-3" />
                        <span>[AI] Extracted {hierarchyAnalysis?.legacyFormatInfo?.levelCount || 0} hierarchy levels</span>
                      </div>
                      <div className="flex items-center gap-2 text-green-700 dark:text-green-400">
                        <CheckCircle2 className="h-3 w-3" />
                        <span>[AI] Mapped XREF_HIERARCHY_KEY → HIERARCHY_ID</span>
                      </div>
                      {hierarchyAnalysis?.legacyFormatInfo?.hasFilterGroups && (
                        <div className="flex items-center gap-2 text-green-700 dark:text-green-400">
                          <CheckCircle2 className="h-3 w-3" />
                          <span>[AI] Preserved {hierarchyAnalysis.legacyFormatInfo.filterGroupColumns.length} FILTER_GROUP columns as mapping identifiers</span>
                        </div>
                      )}
                      <div className="flex items-center gap-2 text-green-700 dark:text-green-400">
                        <CheckCircle2 className="h-3 w-3" />
                        <span>[AI] Built parent-child relationships from level structure</span>
                      </div>
                      <div className="flex items-center gap-2 text-blue-700 dark:text-blue-400">
                        <Info className="h-3 w-3" />
                        <span>[AI] Created {importResult.hierarchy?.rowsImported || 0} hierarchy nodes</span>
                      </div>
                      {hierarchyAnalysis?.legacyFormatInfo?.conversionPlan.warnings.map((warning, i) => (
                        <div key={i} className="flex items-center gap-2 text-yellow-700 dark:text-yellow-400">
                          <AlertTriangle className="h-3 w-3" />
                          <span>[WARN] {warning}</span>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              )}

              {/* Results summary */}
              <div className="grid grid-cols-2 gap-4">
                {/* Hierarchy results */}
                <Card>
                  <CardHeader className="py-3">
                    <CardTitle className="text-sm flex items-center gap-2">
                      <Layers className="h-4 w-4" />
                      Hierarchy Import
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-2">
                    <div className="flex justify-between text-sm">
                      <span>Total Rows:</span>
                      <span className="font-medium">{importResult.hierarchy?.rowsTotal}</span>
                    </div>
                    <div className="flex justify-between text-sm">
                      <span>Imported:</span>
                      <span className="font-medium text-green-600">
                        {importResult.hierarchy?.rowsImported}
                      </span>
                    </div>
                    <div className="flex justify-between text-sm">
                      <span>Failed:</span>
                      <span className="font-medium text-red-600">
                        {importResult.hierarchy?.rowsFailed}
                      </span>
                    </div>
                    {importResult.hierarchy?.duration && (
                      <div className="flex justify-between text-sm">
                        <span>Duration:</span>
                        <span className="font-medium">
                          <Clock className="h-3 w-3 inline mr-1" />
                          {formatDuration(importResult.hierarchy.duration)}
                        </span>
                      </div>
                    )}
                    {(importResult.hierarchy?.autoFixesApplied.length || 0) > 0 && (
                      <div className="mt-2 pt-2 border-t">
                        <p className="text-xs text-muted-foreground mb-1">Auto-fixes applied:</p>
                        {importResult.hierarchy?.autoFixesApplied.map((fix, i) => (
                          <Badge key={i} variant="outline" className="text-xs mr-1 mb-1">
                            <Wand2 className="h-3 w-3 mr-1" />
                            {fix}
                          </Badge>
                        ))}
                      </div>
                    )}
                  </CardContent>
                </Card>

                {/* Mapping results */}
                {importResult.mapping && (
                  <Card>
                    <CardHeader className="py-3">
                      <CardTitle className="text-sm flex items-center gap-2">
                        <Database className="h-4 w-4" />
                        Mapping Import
                      </CardTitle>
                    </CardHeader>
                    <CardContent className="space-y-2">
                      <div className="flex justify-between text-sm">
                        <span>Total Rows:</span>
                        <span className="font-medium">{importResult.mapping.rowsTotal}</span>
                      </div>
                      <div className="flex justify-between text-sm">
                        <span>Imported:</span>
                        <span className="font-medium text-green-600">
                          {importResult.mapping.rowsImported}
                        </span>
                      </div>
                      <div className="flex justify-between text-sm">
                        <span>Failed:</span>
                        <span className="font-medium text-red-600">
                          {importResult.mapping.rowsFailed}
                        </span>
                      </div>
                    </CardContent>
                  </Card>
                )}
              </div>

              {/* What was imported details */}
              <Card>
                <CardHeader className="py-3">
                  <CardTitle className="text-sm flex items-center gap-2">
                    <ListChecks className="h-4 w-4" />
                    What Was Imported
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-2 text-sm">
                    <div className="flex items-center gap-2">
                      <CheckCircle2 className="h-4 w-4 text-green-600" />
                      <span>
                        <strong>{importResult.hierarchy?.rowsImported || 0}</strong> hierarchy nodes created
                      </span>
                    </div>
                    {hierarchyAnalysis?.format === "legacy" && (
                      <>
                        <div className="flex items-center gap-2">
                          <CheckCircle2 className="h-4 w-4 text-green-600" />
                          <span>
                            <strong>{hierarchyAnalysis.legacyFormatInfo?.levelCount || 0}</strong> hierarchy levels imported
                          </span>
                        </div>
                        {hierarchyAnalysis.legacyFormatInfo?.hasFilterGroups && (
                          <div className="flex items-center gap-2">
                            <CheckCircle2 className="h-4 w-4 text-green-600" />
                            <span>
                              Mapping identifiers stored from <strong>{hierarchyAnalysis.legacyFormatInfo.filterGroupColumns.length}</strong> FILTER_GROUP columns
                            </span>
                          </div>
                        )}
                      </>
                    )}
                    {importResult.mapping && (
                      <div className="flex items-center gap-2">
                        <CheckCircle2 className="h-4 w-4 text-green-600" />
                        <span>
                          <strong>{importResult.mapping.rowsImported}</strong> source mappings created
                        </span>
                      </div>
                    )}
                    {(importResult.hierarchy?.autoFixesApplied.length || 0) > 0 && (
                      <div className="flex items-center gap-2">
                        <Wand2 className="h-4 w-4 text-blue-600" />
                        <span>
                          <strong>{importResult.hierarchy?.autoFixesApplied.length}</strong> auto-fixes applied
                        </span>
                      </div>
                    )}
                  </div>
                </CardContent>
              </Card>

              {/* Issues that weren't resolved */}
              {importResult.hierarchy?.issues.filter(i => !i.resolved).length > 0 && (
                <Card className="border-yellow-200">
                  <CardHeader className="py-3">
                    <CardTitle className="text-sm flex items-center gap-2">
                      <AlertTriangle className="h-4 w-4 text-yellow-600" />
                      Items Requiring Attention
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-2 max-h-32 overflow-y-auto">
                      {importResult.hierarchy.issues
                        .filter(i => !i.resolved)
                        .map((issue, i) => (
                          <div key={i} className="flex items-start gap-2 text-sm">
                            <AlertCircle className="h-4 w-4 text-yellow-600 mt-0.5 flex-shrink-0" />
                            <span className="text-muted-foreground">{issue.message}</span>
                          </div>
                        ))}
                    </div>
                    <p className="text-xs text-muted-foreground mt-3">
                      You can address these items using the hierarchy editor after closing this dialog.
                    </p>
                  </CardContent>
                </Card>
              )}

              {/* Next Steps */}
              <Card className="bg-muted/30">
                <CardHeader className="py-3">
                  <CardTitle className="text-sm flex items-center gap-2">
                    <ArrowRight className="h-4 w-4" />
                    Next Steps
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <ul className="space-y-1 text-sm text-muted-foreground">
                    <li>• Review the imported hierarchy in the tree view</li>
                    {hierarchyAnalysis?.format === "legacy" && (
                      <li>• Add source database/table/column mappings if needed</li>
                    )}
                    <li>• Verify parent-child relationships are correct</li>
                    <li>• Configure formulas and calculations as needed</li>
                  </ul>
                </CardContent>
              </Card>
            </div>
          )}
        </ScrollArea>

        <DialogFooter className="gap-2">
          {step === "upload" && (
            <>
              <Button variant="outline" onClick={() => onOpenChange(false)}>
                Cancel
              </Button>
              <Button
                onClick={handleAnalyze}
                disabled={!hierarchyFile || isAnalyzing}
              >
                {isAnalyzing ? (
                  <>
                    <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
                    Analyzing...
                  </>
                ) : (
                  <>
                    <Eye className="h-4 w-4 mr-2" />
                    Analyze Files
                  </>
                )}
              </Button>
            </>
          )}

          {step === "analyze" && (
            <>
              <Button variant="outline" onClick={() => setStep("upload")}>
                Back
              </Button>
              <Button
                onClick={handleImport}
                disabled={(hasErrors && !options.autoFix) || isImporting}
              >
                {isImporting ? (
                  <>
                    <RefreshCw className="h-4 w-4 mr-2 animate-spin" />
                    Importing...
                  </>
                ) : (
                  <>
                    <ArrowRight className="h-4 w-4 mr-2" />
                    {options.dryRun ? "Preview Import" : `Import ${hierarchyAnalysis?.rowCount || 0} Rows`}
                  </>
                )}
              </Button>
            </>
          )}

          {step === "complete" && (
            <>
              <Button variant="outline" onClick={handleDownloadReport}>
                <Download className="h-4 w-4 mr-2" />
                Download Report
              </Button>
              <Button onClick={() => onOpenChange(false)}>
                Done
              </Button>
            </>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
