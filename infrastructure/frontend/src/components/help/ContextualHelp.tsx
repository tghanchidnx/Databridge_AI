/**
 * Contextual Help System
 * Provides tooltips, popovers, and inline help throughout the application
 */
import { useState, ReactNode } from "react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  HoverCard,
  HoverCardContent,
  HoverCardTrigger,
} from "@/components/ui/hover-card";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import {
  HelpCircle,
  Info,
  Lightbulb,
  ExternalLink,
  BookOpen,
  Video,
  MessageSquare,
  Keyboard,
  ChevronRight,
  Sparkles,
  X,
} from "lucide-react";

// Help content database
export const HELP_CONTENT: Record<
  string,
  {
    title: string;
    description: string;
    tips?: string[];
    shortcuts?: Array<{ key: string; action: string }>;
    learnMoreUrl?: string;
    videoUrl?: string;
    relatedTopics?: string[];
  }
> = {
  "hierarchy-tree": {
    title: "Hierarchy Tree",
    description:
      "The hierarchy tree is your main workspace for building and organizing financial structures. Each node represents a line item in your report.",
    tips: [
      "Drag and drop nodes to reorder them",
      "Right-click for quick actions menu",
      "Double-click to rename a node",
      "Use arrow keys to navigate",
    ],
    shortcuts: [
      { key: "↑↓", action: "Navigate between nodes" },
      { key: "←→", action: "Collapse/Expand" },
      { key: "Enter", action: "Edit selected node" },
      { key: "Ctrl+N", action: "Add new child" },
    ],
    learnMoreUrl: "/docs/hierarchy-tree",
  },
  "source-mapping": {
    title: "Source Mapping",
    description:
      "Map hierarchy nodes to source data columns. This defines where the data comes from for each line item in your report.",
    tips: [
      "AI suggests mappings based on node names",
      "Multiple mappings can be added per node",
      "Use precedence groups for complex logic",
      "Test mappings before deploying",
    ],
    shortcuts: [
      { key: "Ctrl+M", action: "Open mapping panel" },
      { key: "Tab", action: "Accept AI suggestion" },
    ],
    learnMoreUrl: "/docs/source-mapping",
  },
  "formula-builder": {
    title: "Formula Builder",
    description:
      "Create calculated fields using formulas. Combine child nodes with arithmetic operations to compute parent values.",
    tips: [
      "Use SUM for totals and subtotals",
      "Use SUBTRACT for difference calculations (e.g., Gross Profit)",
      "Reference other nodes using {NodeName} syntax",
      "AI auto-detects common patterns like EBITDA",
    ],
    shortcuts: [
      { key: "Ctrl+F", action: "Open formula builder" },
      { key: "Ctrl+Enter", action: "Apply formula" },
    ],
    learnMoreUrl: "/docs/formulas",
  },
  "ai-suggestions": {
    title: "AI Suggestions",
    description:
      "Our AI analyzes your hierarchy names and existing patterns to suggest the best source mappings and formulas.",
    tips: [
      "Accept suggestions with a single click",
      "AI learns from your choices over time",
      "Confidence scores indicate suggestion quality",
      "Hover over suggestions for explanations",
    ],
    learnMoreUrl: "/docs/ai-features",
  },
  "csv-import": {
    title: "CSV Import",
    description:
      "Import hierarchies from CSV files with smart format detection and validation.",
    tips: [
      "Supports both legacy and new CSV formats",
      "Auto-detects column mappings",
      "Preview changes before applying",
      "Use diff view to compare with existing data",
    ],
    learnMoreUrl: "/docs/import-export",
  },
  "template-gallery": {
    title: "Template Gallery",
    description:
      "Browse pre-built hierarchy templates for common financial structures and industry-specific reports.",
    tips: [
      "Filter by industry or category",
      "Preview template structure before using",
      "Save your projects as custom templates",
      "Templates include formula suggestions",
    ],
    learnMoreUrl: "/docs/templates",
  },
  "anomaly-detection": {
    title: "Anomaly Detection",
    description:
      "Automatic detection of issues like missing mappings, type mismatches, and inconsistent patterns.",
    tips: [
      "Review issues before deploying",
      "One-click fixes for common problems",
      "Filter by severity level",
      "Export issues as a report",
    ],
    learnMoreUrl: "/docs/validation",
  },
  "ai-chat": {
    title: "AI Chat Assistant",
    description:
      "Chat with AI to get help, ask questions, and perform actions using natural language.",
    tips: [
      "Ask questions about your project",
      "Request actions like 'find unmapped nodes'",
      "Get explanations for errors",
      "AI can navigate to specific nodes",
    ],
    learnMoreUrl: "/docs/ai-chat",
  },
};

interface HelpTooltipProps {
  topic: string;
  children: ReactNode;
  side?: "top" | "bottom" | "left" | "right";
  showIcon?: boolean;
}

export function HelpTooltip({
  topic,
  children,
  side = "top",
  showIcon = true,
}: HelpTooltipProps) {
  const content = HELP_CONTENT[topic];

  if (!content) {
    return <>{children}</>;
  }

  return (
    <TooltipProvider>
      <Tooltip delayDuration={300}>
        <TooltipTrigger asChild>
          <span className="inline-flex items-center gap-1 cursor-help">
            {children}
            {showIcon && (
              <HelpCircle className="h-3.5 w-3.5 text-muted-foreground hover:text-foreground" />
            )}
          </span>
        </TooltipTrigger>
        <TooltipContent side={side} className="max-w-xs">
          <p className="font-medium">{content.title}</p>
          <p className="text-xs text-muted-foreground mt-1">{content.description}</p>
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

interface HelpPopoverProps {
  topic: string;
  trigger?: ReactNode;
  side?: "top" | "bottom" | "left" | "right";
}

export function HelpPopover({ topic, trigger, side = "right" }: HelpPopoverProps) {
  const [open, setOpen] = useState(false);
  const content = HELP_CONTENT[topic];

  if (!content) {
    return null;
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        {trigger || (
          <Button variant="ghost" size="icon" className="h-6 w-6">
            <HelpCircle className="h-4 w-4 text-muted-foreground" />
          </Button>
        )}
      </PopoverTrigger>
      <PopoverContent side={side} className="w-80">
        <div className="space-y-3">
          <div className="flex items-start justify-between">
            <div className="flex items-center gap-2">
              <Info className="h-5 w-5 text-primary" />
              <h4 className="font-semibold">{content.title}</h4>
            </div>
            <Button
              variant="ghost"
              size="icon"
              className="h-6 w-6"
              onClick={() => setOpen(false)}
            >
              <X className="h-4 w-4" />
            </Button>
          </div>

          <p className="text-sm text-muted-foreground">{content.description}</p>

          {content.tips && content.tips.length > 0 && (
            <>
              <Separator />
              <div>
                <h5 className="text-xs font-medium text-muted-foreground mb-2 flex items-center gap-1">
                  <Lightbulb className="h-3 w-3" />
                  Tips
                </h5>
                <ul className="space-y-1">
                  {content.tips.map((tip, i) => (
                    <li key={i} className="text-xs flex items-start gap-2">
                      <ChevronRight className="h-3 w-3 mt-0.5 text-primary shrink-0" />
                      <span>{tip}</span>
                    </li>
                  ))}
                </ul>
              </div>
            </>
          )}

          {content.shortcuts && content.shortcuts.length > 0 && (
            <>
              <Separator />
              <div>
                <h5 className="text-xs font-medium text-muted-foreground mb-2 flex items-center gap-1">
                  <Keyboard className="h-3 w-3" />
                  Shortcuts
                </h5>
                <div className="space-y-1">
                  {content.shortcuts.map((shortcut, i) => (
                    <div key={i} className="flex items-center justify-between text-xs">
                      <kbd className="px-1.5 py-0.5 bg-muted rounded text-xs font-mono">
                        {shortcut.key}
                      </kbd>
                      <span className="text-muted-foreground">{shortcut.action}</span>
                    </div>
                  ))}
                </div>
              </div>
            </>
          )}

          {content.learnMoreUrl && (
            <>
              <Separator />
              <Button
                variant="ghost"
                size="sm"
                className="w-full justify-start gap-2"
                asChild
              >
                <a href={content.learnMoreUrl} target="_blank" rel="noopener">
                  <BookOpen className="h-4 w-4" />
                  Learn more
                  <ExternalLink className="h-3 w-3 ml-auto" />
                </a>
              </Button>
            </>
          )}
        </div>
      </PopoverContent>
    </Popover>
  );
}

interface FeatureHighlightProps {
  title: string;
  description: string;
  isNew?: boolean;
  icon?: ReactNode;
  children: ReactNode;
}

export function FeatureHighlight({
  title,
  description,
  isNew = false,
  icon,
  children,
}: FeatureHighlightProps) {
  return (
    <HoverCard openDelay={200}>
      <HoverCardTrigger asChild>{children}</HoverCardTrigger>
      <HoverCardContent className="w-80">
        <div className="flex items-start gap-3">
          {icon && <div className="p-2 rounded-lg bg-primary/10">{icon}</div>}
          <div className="flex-1">
            <div className="flex items-center gap-2">
              <h4 className="font-medium text-sm">{title}</h4>
              {isNew && (
                <Badge variant="default" className="text-xs">
                  New
                </Badge>
              )}
            </div>
            <p className="text-xs text-muted-foreground mt-1">{description}</p>
          </div>
        </div>
      </HoverCardContent>
    </HoverCard>
  );
}

// Inline help banner component
interface HelpBannerProps {
  topic: string;
  onDismiss?: () => void;
  className?: string;
}

export function HelpBanner({ topic, onDismiss, className }: HelpBannerProps) {
  const content = HELP_CONTENT[topic];

  if (!content) {
    return null;
  }

  return (
    <div
      className={cn(
        "flex items-start gap-3 p-4 rounded-lg border bg-primary/5 border-primary/20",
        className
      )}
    >
      <div className="p-2 rounded-full bg-primary/10">
        <Sparkles className="h-4 w-4 text-primary" />
      </div>
      <div className="flex-1 min-w-0">
        <h4 className="font-medium text-sm">{content.title}</h4>
        <p className="text-xs text-muted-foreground mt-1">{content.description}</p>
        {content.tips && content.tips.length > 0 && (
          <p className="text-xs text-muted-foreground mt-2">
            <Lightbulb className="h-3 w-3 inline mr-1" />
            Tip: {content.tips[0]}
          </p>
        )}
      </div>
      {onDismiss && (
        <Button variant="ghost" size="icon" className="h-6 w-6" onClick={onDismiss}>
          <X className="h-4 w-4" />
        </Button>
      )}
    </div>
  );
}

// Quick Help Panel - Sidebar or floating panel
interface QuickHelpPanelProps {
  open: boolean;
  onClose: () => void;
  currentContext?: string;
}

export function QuickHelpPanel({ open, onClose, currentContext }: QuickHelpPanelProps) {
  const contextContent = currentContext ? HELP_CONTENT[currentContext] : null;
  const allTopics = Object.entries(HELP_CONTENT);

  if (!open) return null;

  return (
    <div className="fixed right-0 top-0 bottom-0 w-80 bg-background border-l shadow-lg z-50 flex flex-col">
      <div className="p-4 border-b flex items-center justify-between">
        <div className="flex items-center gap-2">
          <BookOpen className="h-5 w-5 text-primary" />
          <h3 className="font-semibold">Help</h3>
        </div>
        <Button variant="ghost" size="icon" onClick={onClose}>
          <X className="h-4 w-4" />
        </Button>
      </div>

      <ScrollArea className="flex-1">
        <div className="p-4 space-y-4">
          {/* Current Context Help */}
          {contextContent && (
            <div className="space-y-3">
              <h4 className="font-medium text-sm flex items-center gap-2">
                <Info className="h-4 w-4 text-primary" />
                Current Context
              </h4>
              <div className="p-3 rounded-lg border bg-muted/50">
                <h5 className="font-medium text-sm">{contextContent.title}</h5>
                <p className="text-xs text-muted-foreground mt-1">
                  {contextContent.description}
                </p>
              </div>
            </div>
          )}

          <Separator />

          {/* All Topics */}
          <div className="space-y-3">
            <h4 className="font-medium text-sm">All Topics</h4>
            <div className="space-y-2">
              {allTopics.map(([key, content]) => (
                <button
                  key={key}
                  className="w-full text-left p-2 rounded-lg hover:bg-muted transition-colors"
                >
                  <div className="font-medium text-sm">{content.title}</div>
                  <div className="text-xs text-muted-foreground line-clamp-1">
                    {content.description}
                  </div>
                </button>
              ))}
            </div>
          </div>

          <Separator />

          {/* Quick Actions */}
          <div className="space-y-2">
            <h4 className="font-medium text-sm">Quick Actions</h4>
            <Button variant="outline" size="sm" className="w-full justify-start gap-2">
              <MessageSquare className="h-4 w-4" />
              Chat with AI Assistant
            </Button>
            <Button variant="outline" size="sm" className="w-full justify-start gap-2">
              <Video className="h-4 w-4" />
              Watch Tutorial Videos
            </Button>
            <Button variant="outline" size="sm" className="w-full justify-start gap-2">
              <BookOpen className="h-4 w-4" />
              View Full Documentation
            </Button>
          </div>
        </div>
      </ScrollArea>
    </div>
  );
}

// Help trigger button
export function HelpButton({ onClick }: { onClick: () => void }) {
  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <Button
            variant="ghost"
            size="icon"
            onClick={onClick}
            className="h-8 w-8"
          >
            <HelpCircle className="h-5 w-5" />
          </Button>
        </TooltipTrigger>
        <TooltipContent>Help (F1)</TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}
