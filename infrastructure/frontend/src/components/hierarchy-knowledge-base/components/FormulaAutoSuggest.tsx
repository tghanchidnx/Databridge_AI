/**
 * Formula Auto-Suggest Component
 * AI-powered formula suggestions for hierarchy calculations
 */
import { useState, useMemo } from "react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  Calculator,
  Plus,
  Minus,
  X as Multiply,
  Divide,
  Brackets,
  Check,
  AlertTriangle,
  Info,
  Sparkles,
  ChevronRight,
  Code,
  Play,
  RotateCcw,
  Lightbulb,
  Sigma,
} from "lucide-react";

export interface FormulaSuggestion {
  formulaType: "SUM" | "SUBTRACT" | "MULTIPLY" | "DIVIDE" | "CUSTOM";
  formulaText: string;
  variables: string[];
  confidence: number;
  reasoning: string;
  category: string;
}

export interface FormulaValidation {
  valid: boolean;
  errors: string[];
  warnings: string[];
}

interface FormulaAutoSuggestProps {
  hierarchyName: string;
  suggestions: FormulaSuggestion[];
  availableVariables: string[];
  currentFormula?: string;
  onAccept: (formula: string, type: string) => void;
  onValidate?: (formula: string) => FormulaValidation;
  isLoading?: boolean;
  className?: string;
}

function getFormulaIcon(type: FormulaSuggestion["formulaType"]) {
  switch (type) {
    case "SUM":
      return <Sigma className="h-4 w-4" />;
    case "SUBTRACT":
      return <Minus className="h-4 w-4" />;
    case "MULTIPLY":
      return <Multiply className="h-4 w-4" />;
    case "DIVIDE":
      return <Divide className="h-4 w-4" />;
    case "CUSTOM":
      return <Brackets className="h-4 w-4" />;
  }
}

function getCategoryColor(category: string): string {
  const colors: Record<string, string> = {
    "Income Statement": "bg-blue-100 text-blue-800",
    "Balance Sheet": "bg-green-100 text-green-800",
    "Cash Flow": "bg-purple-100 text-purple-800",
    Ratios: "bg-orange-100 text-orange-800",
    "Variance Analysis": "bg-pink-100 text-pink-800",
    "Growth Metrics": "bg-teal-100 text-teal-800",
    Aggregation: "bg-gray-100 text-gray-800",
    Calculated: "bg-yellow-100 text-yellow-800",
  };
  return colors[category] || "bg-gray-100 text-gray-800";
}

function SuggestionCard({
  suggestion,
  onAccept,
}: {
  suggestion: FormulaSuggestion;
  onAccept: () => void;
}) {
  const [showVariables, setShowVariables] = useState(false);
  const confidencePercent = Math.round(suggestion.confidence * 100);

  return (
    <Card className="hover:border-primary/50 transition-colors">
      <CardContent className="p-4 space-y-3">
        <div className="flex items-start justify-between">
          <div className="flex items-center gap-2">
            <div className="p-1.5 rounded bg-primary/10 text-primary">
              {getFormulaIcon(suggestion.formulaType)}
            </div>
            <div>
              <div className="font-medium text-sm">{suggestion.formulaType}</div>
              <Badge variant="secondary" className={cn("text-xs", getCategoryColor(suggestion.category))}>
                {suggestion.category}
              </Badge>
            </div>
          </div>
          <Badge variant="outline" className="font-mono">
            {confidencePercent}%
          </Badge>
        </div>

        <div className="font-mono text-sm bg-muted/50 p-2 rounded border">
          {suggestion.formulaText}
        </div>

        <div className="text-xs text-muted-foreground flex items-start gap-1">
          <Lightbulb className="h-3 w-3 mt-0.5 shrink-0" />
          <span>{suggestion.reasoning}</span>
        </div>

        <button
          onClick={() => setShowVariables(!showVariables)}
          className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground"
        >
          <ChevronRight
            className={cn("h-3 w-3 transition-transform", showVariables && "rotate-90")}
          />
          Variables ({suggestion.variables.length})
        </button>

        {showVariables && (
          <div className="flex flex-wrap gap-1">
            {suggestion.variables.map((variable) => (
              <Badge key={variable} variant="outline" className="font-mono text-xs">
                {variable}
              </Badge>
            ))}
          </div>
        )}

        <Button size="sm" onClick={onAccept} className="w-full gap-1">
          <Check className="h-4 w-4" />
          Use This Formula
        </Button>
      </CardContent>
    </Card>
  );
}

function FormulaBuilder({
  availableVariables,
  currentFormula,
  onAccept,
  onValidate,
}: {
  availableVariables: string[];
  currentFormula?: string;
  onAccept: (formula: string) => void;
  onValidate?: (formula: string) => FormulaValidation;
}) {
  const [formula, setFormula] = useState(currentFormula || "");
  const [validation, setValidation] = useState<FormulaValidation | null>(null);

  const insertVariable = (variable: string) => {
    setFormula((prev) => prev + `{${variable}}`);
  };

  const insertOperator = (op: string) => {
    setFormula((prev) => prev + ` ${op} `);
  };

  const handleValidate = () => {
    if (onValidate) {
      const result = onValidate(formula);
      setValidation(result);
    }
  };

  return (
    <div className="space-y-4">
      <div className="space-y-2">
        <Label>Formula Expression</Label>
        <div className="relative">
          <Input
            value={formula}
            onChange={(e) => {
              setFormula(e.target.value);
              setValidation(null);
            }}
            placeholder="e.g., {Revenue} - {COGS}"
            className="font-mono pr-20"
          />
          <div className="absolute right-2 top-1/2 -translate-y-1/2 flex gap-1">
            <Button
              size="icon"
              variant="ghost"
              className="h-6 w-6"
              onClick={() => setFormula("")}
            >
              <RotateCcw className="h-3 w-3" />
            </Button>
            <Button size="icon" variant="ghost" className="h-6 w-6" onClick={handleValidate}>
              <Play className="h-3 w-3" />
            </Button>
          </div>
        </div>
      </div>

      {/* Validation results */}
      {validation && (
        <div className="space-y-2">
          {validation.valid && validation.errors.length === 0 && (
            <div className="flex items-center gap-2 text-sm text-green-600 bg-green-50 p-2 rounded">
              <Check className="h-4 w-4" />
              Formula is valid
            </div>
          )}
          {validation.errors.map((error, i) => (
            <div
              key={i}
              className="flex items-center gap-2 text-sm text-red-600 bg-red-50 p-2 rounded"
            >
              <AlertTriangle className="h-4 w-4" />
              {error}
            </div>
          ))}
          {validation.warnings.map((warning, i) => (
            <div
              key={i}
              className="flex items-center gap-2 text-sm text-yellow-600 bg-yellow-50 p-2 rounded"
            >
              <Info className="h-4 w-4" />
              {warning}
            </div>
          ))}
        </div>
      )}

      {/* Operators */}
      <div className="space-y-2">
        <Label className="text-xs text-muted-foreground">Operators</Label>
        <div className="flex flex-wrap gap-1">
          {["+", "-", "*", "/", "(", ")"].map((op) => (
            <Button
              key={op}
              size="sm"
              variant="outline"
              className="h-8 w-8 font-mono"
              onClick={() => insertOperator(op)}
            >
              {op}
            </Button>
          ))}
        </div>
      </div>

      {/* Available variables */}
      <div className="space-y-2">
        <Label className="text-xs text-muted-foreground">Available Variables</Label>
        <ScrollArea className="h-[120px] border rounded p-2">
          <div className="flex flex-wrap gap-1">
            {availableVariables.map((variable) => (
              <Button
                key={variable}
                size="sm"
                variant="outline"
                className="font-mono text-xs h-7"
                onClick={() => insertVariable(variable)}
              >
                {variable}
              </Button>
            ))}
          </div>
        </ScrollArea>
      </div>

      <Button
        onClick={() => onAccept(formula)}
        disabled={!formula || (validation && !validation.valid)}
        className="w-full gap-2"
      >
        <Check className="h-4 w-4" />
        Apply Custom Formula
      </Button>
    </div>
  );
}

export function FormulaAutoSuggest({
  hierarchyName,
  suggestions,
  availableVariables,
  currentFormula,
  onAccept,
  onValidate,
  isLoading,
  className,
}: FormulaAutoSuggestProps) {
  const [activeTab, setActiveTab] = useState<"suggestions" | "custom">("suggestions");

  const categorizedSuggestions = useMemo(() => {
    const categories = new Map<string, FormulaSuggestion[]>();
    suggestions.forEach((s) => {
      if (!categories.has(s.category)) {
        categories.set(s.category, []);
      }
      categories.get(s.category)!.push(s);
    });
    return categories;
  }, [suggestions]);

  return (
    <Card className={cn("", className)}>
      <CardHeader className="pb-3">
        <div className="flex items-center gap-2">
          <Calculator className="h-5 w-5 text-primary" />
          <CardTitle className="text-base">Formula Suggestions</CardTitle>
        </div>
        <CardDescription>
          AI-recommended formulas for "{hierarchyName}"
        </CardDescription>
      </CardHeader>

      <Separator />

      <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as any)}>
        <div className="px-4 pt-3">
          <TabsList className="grid w-full grid-cols-2">
            <TabsTrigger value="suggestions" className="gap-1">
              <Sparkles className="h-4 w-4" />
              AI Suggestions
            </TabsTrigger>
            <TabsTrigger value="custom" className="gap-1">
              <Code className="h-4 w-4" />
              Custom
            </TabsTrigger>
          </TabsList>
        </div>

        <TabsContent value="suggestions" className="m-0">
          <ScrollArea className="max-h-[400px]">
            <div className="p-4 space-y-4">
              {isLoading ? (
                <div className="flex flex-col items-center py-8">
                  <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-primary"></div>
                  <p className="text-sm text-muted-foreground mt-2">Analyzing patterns...</p>
                </div>
              ) : suggestions.length === 0 ? (
                <div className="text-center py-8">
                  <Calculator className="h-8 w-8 text-muted-foreground mx-auto mb-2" />
                  <p className="text-sm font-medium">No formula suggestions</p>
                  <p className="text-xs text-muted-foreground mt-1">
                    Try creating a custom formula
                  </p>
                </div>
              ) : (
                <>
                  {/* Best match */}
                  {suggestions[0] && (
                    <div className="space-y-2">
                      <div className="flex items-center gap-2 text-sm font-medium">
                        <Sparkles className="h-4 w-4 text-primary" />
                        Best Match
                      </div>
                      <SuggestionCard
                        suggestion={suggestions[0]}
                        onAccept={() => onAccept(suggestions[0].formulaText, suggestions[0].formulaType)}
                      />
                    </div>
                  )}

                  {/* Other suggestions by category */}
                  {suggestions.length > 1 && (
                    <div className="space-y-4 pt-2">
                      <Separator />
                      <div className="text-sm font-medium">Other Options</div>
                      {suggestions.slice(1).map((suggestion, index) => (
                        <SuggestionCard
                          key={index}
                          suggestion={suggestion}
                          onAccept={() => onAccept(suggestion.formulaText, suggestion.formulaType)}
                        />
                      ))}
                    </div>
                  )}
                </>
              )}
            </div>
          </ScrollArea>
        </TabsContent>

        <TabsContent value="custom" className="m-0">
          <div className="p-4">
            <FormulaBuilder
              availableVariables={availableVariables}
              currentFormula={currentFormula}
              onAccept={(formula) => onAccept(formula, "CUSTOM")}
              onValidate={onValidate}
            />
          </div>
        </TabsContent>
      </Tabs>
    </Card>
  );
}

// Compact inline version
export function InlineFormulaSuggestion({
  suggestion,
  onAccept,
}: {
  suggestion: FormulaSuggestion;
  onAccept: () => void;
}) {
  return (
    <div className="flex items-center gap-2 p-2 rounded-lg border bg-muted/30 hover:border-primary/50 transition-colors">
      <div className="p-1 rounded bg-primary/10 text-primary">
        {getFormulaIcon(suggestion.formulaType)}
      </div>
      <div className="flex-1 min-w-0">
        <div className="font-mono text-xs truncate">{suggestion.formulaText}</div>
        <div className="text-xs text-muted-foreground truncate">{suggestion.reasoning}</div>
      </div>
      <TooltipProvider>
        <Tooltip>
          <TooltipTrigger asChild>
            <Button size="sm" variant="ghost" className="h-7 gap-1" onClick={onAccept}>
              <Check className="h-4 w-4" />
              Use
            </Button>
          </TooltipTrigger>
          <TooltipContent>Apply this formula</TooltipContent>
        </Tooltip>
      </TooltipProvider>
    </div>
  );
}
