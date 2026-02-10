/**
 * AI Mapping Suggester Component
 * Shows AI-powered mapping suggestions with confidence scores
 */
import { useState, useCallback } from "react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Progress } from "@/components/ui/progress";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  Sparkles,
  Database,
  Table2,
  Columns,
  Check,
  X,
  ThumbsUp,
  ThumbsDown,
  Loader2,
  RefreshCw,
  ChevronDown,
  ChevronRight,
  Lightbulb,
  Brain,
  History,
  Zap,
} from "lucide-react";

export interface MappingSuggestion {
  hierarchyId: string;
  suggestedMapping: {
    sourceDatabase: string;
    sourceSchema: string;
    sourceTable: string;
    sourceColumn: string;
    sourceUid?: string;
  };
  confidence: number;
  reasoning: string;
  source: "ai" | "pattern" | "similar";
}

interface AIMappingSuggesterProps {
  hierarchyId: string;
  hierarchyName: string;
  suggestions: MappingSuggestion[];
  isLoading?: boolean;
  onAccept: (suggestion: MappingSuggestion) => void;
  onReject: (suggestion: MappingSuggestion) => void;
  onRefresh: () => void;
  onFeedback?: (suggestion: MappingSuggestion, accepted: boolean) => void;
  className?: string;
}

function getConfidenceColor(confidence: number): string {
  if (confidence >= 0.8) return "text-green-600";
  if (confidence >= 0.6) return "text-yellow-600";
  return "text-orange-600";
}

function getConfidenceBg(confidence: number): string {
  if (confidence >= 0.8) return "bg-green-100 border-green-200";
  if (confidence >= 0.6) return "bg-yellow-100 border-yellow-200";
  return "bg-orange-100 border-orange-200";
}

function getSourceIcon(source: MappingSuggestion["source"]) {
  switch (source) {
    case "ai":
      return <Brain className="h-4 w-4 text-purple-600" />;
    case "pattern":
      return <Zap className="h-4 w-4 text-blue-600" />;
    case "similar":
      return <History className="h-4 w-4 text-teal-600" />;
  }
}

function getSourceLabel(source: MappingSuggestion["source"]): string {
  switch (source) {
    case "ai":
      return "AI Suggestion";
    case "pattern":
      return "Pattern Match";
    case "similar":
      return "Similar Mapping";
  }
}

function SuggestionCard({
  suggestion,
  onAccept,
  onReject,
  isFirst,
}: {
  suggestion: MappingSuggestion;
  onAccept: () => void;
  onReject: () => void;
  isFirst: boolean;
}) {
  const [expanded, setExpanded] = useState(isFirst);
  const confidencePercent = Math.round(suggestion.confidence * 100);

  return (
    <Card
      className={cn(
        "transition-all",
        getConfidenceBg(suggestion.confidence),
        isFirst && "ring-2 ring-primary"
      )}
    >
      <CardHeader className="pb-2">
        <div className="flex items-start justify-between">
          <div className="flex items-center gap-2">
            <TooltipProvider>
              <Tooltip>
                <TooltipTrigger>{getSourceIcon(suggestion.source)}</TooltipTrigger>
                <TooltipContent>{getSourceLabel(suggestion.source)}</TooltipContent>
              </Tooltip>
            </TooltipProvider>

            <div>
              <CardTitle className="text-sm font-medium flex items-center gap-2">
                {suggestion.suggestedMapping.sourceTable}
                <span className="text-muted-foreground">.</span>
                <span className="text-primary">{suggestion.suggestedMapping.sourceColumn}</span>
              </CardTitle>
              <CardDescription className="text-xs mt-0.5">
                {suggestion.suggestedMapping.sourceDatabase}
                {suggestion.suggestedMapping.sourceSchema &&
                  `.${suggestion.suggestedMapping.sourceSchema}`}
              </CardDescription>
            </div>
          </div>

          <div className="flex items-center gap-2">
            <Badge
              variant="outline"
              className={cn("font-mono", getConfidenceColor(suggestion.confidence))}
            >
              {confidencePercent}%
            </Badge>
            {isFirst && (
              <Badge variant="default" className="gap-1">
                <Sparkles className="h-3 w-3" />
                Best Match
              </Badge>
            )}
          </div>
        </div>
      </CardHeader>

      <CardContent className="space-y-3">
        {/* Confidence bar */}
        <div className="space-y-1">
          <div className="flex justify-between text-xs text-muted-foreground">
            <span>Confidence</span>
            <span>{confidencePercent}%</span>
          </div>
          <Progress
            value={confidencePercent}
            className={cn(
              "h-1.5",
              suggestion.confidence >= 0.8 && "[&>div]:bg-green-500",
              suggestion.confidence >= 0.6 && suggestion.confidence < 0.8 && "[&>div]:bg-yellow-500",
              suggestion.confidence < 0.6 && "[&>div]:bg-orange-500"
            )}
          />
        </div>

        {/* Reasoning */}
        <button
          onClick={() => setExpanded(!expanded)}
          className="flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground transition-colors"
        >
          {expanded ? (
            <ChevronDown className="h-3 w-3" />
          ) : (
            <ChevronRight className="h-3 w-3" />
          )}
          <Lightbulb className="h-3 w-3" />
          Why this suggestion?
        </button>

        {expanded && (
          <div className="text-xs text-muted-foreground bg-background/50 rounded p-2 border">
            {suggestion.reasoning}
          </div>
        )}

        {/* Actions */}
        <div className="flex items-center gap-2 pt-1">
          <Button size="sm" onClick={onAccept} className="flex-1 gap-1">
            <Check className="h-4 w-4" />
            Accept
          </Button>
          <Button size="sm" variant="outline" onClick={onReject} className="gap-1">
            <X className="h-4 w-4" />
            Reject
          </Button>
        </div>
      </CardContent>
    </Card>
  );
}

export function AIMappingSuggester({
  hierarchyId,
  hierarchyName,
  suggestions,
  isLoading,
  onAccept,
  onReject,
  onRefresh,
  onFeedback,
  className,
}: AIMappingSuggesterProps) {
  const [rejectedIds, setRejectedIds] = useState<Set<string>>(new Set());

  const handleAccept = useCallback(
    (suggestion: MappingSuggestion) => {
      onAccept(suggestion);
      onFeedback?.(suggestion, true);
    },
    [onAccept, onFeedback]
  );

  const handleReject = useCallback(
    (suggestion: MappingSuggestion) => {
      const id = `${suggestion.suggestedMapping.sourceTable}-${suggestion.suggestedMapping.sourceColumn}`;
      setRejectedIds((prev) => new Set([...prev, id]));
      onReject(suggestion);
      onFeedback?.(suggestion, false);
    },
    [onReject, onFeedback]
  );

  const visibleSuggestions = suggestions.filter((s) => {
    const id = `${s.suggestedMapping.sourceTable}-${s.suggestedMapping.sourceColumn}`;
    return !rejectedIds.has(id);
  });

  if (isLoading) {
    return (
      <Card className={cn("", className)}>
        <CardContent className="flex flex-col items-center justify-center py-8">
          <Loader2 className="h-8 w-8 text-primary animate-spin mb-3" />
          <p className="text-sm font-medium">Analyzing mappings...</p>
          <p className="text-xs text-muted-foreground mt-1">
            Finding the best source columns for "{hierarchyName}"
          </p>
        </CardContent>
      </Card>
    );
  }

  if (visibleSuggestions.length === 0 && suggestions.length === 0) {
    return (
      <Card className={cn("", className)}>
        <CardContent className="flex flex-col items-center justify-center py-8">
          <Database className="h-8 w-8 text-muted-foreground mb-3" />
          <p className="text-sm font-medium">No suggestions available</p>
          <p className="text-xs text-muted-foreground mt-1 text-center max-w-xs">
            We couldn't find matching source columns. Try adding more data sources or
            manually configure the mapping.
          </p>
          <Button variant="outline" size="sm" onClick={onRefresh} className="mt-4 gap-2">
            <RefreshCw className="h-4 w-4" />
            Try Again
          </Button>
        </CardContent>
      </Card>
    );
  }

  if (visibleSuggestions.length === 0) {
    return (
      <Card className={cn("", className)}>
        <CardContent className="flex flex-col items-center justify-center py-8">
          <ThumbsDown className="h-8 w-8 text-muted-foreground mb-3" />
          <p className="text-sm font-medium">All suggestions rejected</p>
          <p className="text-xs text-muted-foreground mt-1">
            Would you like to see more options?
          </p>
          <Button
            variant="outline"
            size="sm"
            onClick={() => {
              setRejectedIds(new Set());
              onRefresh();
            }}
            className="mt-4 gap-2"
          >
            <RefreshCw className="h-4 w-4" />
            Get New Suggestions
          </Button>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className={cn("", className)}>
      <CardHeader className="pb-3">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Sparkles className="h-5 w-5 text-primary" />
            <CardTitle className="text-base">AI Mapping Suggestions</CardTitle>
          </div>
          <Button variant="ghost" size="sm" onClick={onRefresh} className="gap-1">
            <RefreshCw className="h-4 w-4" />
            Refresh
          </Button>
        </div>
        <CardDescription>
          Suggested source mappings for "{hierarchyName}"
        </CardDescription>
      </CardHeader>

      <Separator />

      <ScrollArea className="max-h-[400px]">
        <div className="p-4 space-y-3">
          {visibleSuggestions.map((suggestion, index) => (
            <SuggestionCard
              key={`${suggestion.suggestedMapping.sourceTable}-${suggestion.suggestedMapping.sourceColumn}`}
              suggestion={suggestion}
              onAccept={() => handleAccept(suggestion)}
              onReject={() => handleReject(suggestion)}
              isFirst={index === 0}
            />
          ))}
        </div>
      </ScrollArea>

      {visibleSuggestions.length > 0 && (
        <>
          <Separator />
          <div className="p-3 text-xs text-muted-foreground text-center">
            <span className="inline-flex items-center gap-1">
              <Brain className="h-3 w-3" />
              AI learns from your choices to improve future suggestions
            </span>
          </div>
        </>
      )}
    </Card>
  );
}

// Compact inline version for mapping panels
export function InlineMappingSuggestion({
  suggestion,
  onAccept,
  onReject,
}: {
  suggestion: MappingSuggestion;
  onAccept: () => void;
  onReject: () => void;
}) {
  const confidencePercent = Math.round(suggestion.confidence * 100);

  return (
    <div
      className={cn(
        "flex items-center gap-2 p-2 rounded-lg border",
        getConfidenceBg(suggestion.confidence)
      )}
    >
      <TooltipProvider>
        <Tooltip>
          <TooltipTrigger>{getSourceIcon(suggestion.source)}</TooltipTrigger>
          <TooltipContent>{getSourceLabel(suggestion.source)}</TooltipContent>
        </Tooltip>
      </TooltipProvider>

      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-1 text-sm font-medium truncate">
          <Table2 className="h-3 w-3 text-muted-foreground shrink-0" />
          <span className="truncate">{suggestion.suggestedMapping.sourceTable}</span>
          <span className="text-muted-foreground">.</span>
          <Columns className="h-3 w-3 text-muted-foreground shrink-0" />
          <span className="truncate text-primary">
            {suggestion.suggestedMapping.sourceColumn}
          </span>
        </div>
        <div className="text-xs text-muted-foreground truncate">
          {suggestion.reasoning}
        </div>
      </div>

      <Badge
        variant="outline"
        className={cn("shrink-0 font-mono text-xs", getConfidenceColor(suggestion.confidence))}
      >
        {confidencePercent}%
      </Badge>

      <div className="flex items-center gap-1 shrink-0">
        <Button size="icon" variant="ghost" className="h-7 w-7" onClick={onAccept}>
          <ThumbsUp className="h-4 w-4 text-green-600" />
        </Button>
        <Button size="icon" variant="ghost" className="h-7 w-7" onClick={onReject}>
          <ThumbsDown className="h-4 w-4 text-red-600" />
        </Button>
      </div>
    </div>
  );
}
