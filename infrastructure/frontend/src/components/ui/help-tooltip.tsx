import React, { useState } from "react";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Badge } from "@/components/ui/badge";
import { HelpCircle, ExternalLink, BookOpen, X } from "lucide-react";
import { getHelpTopic, getRelatedTopics, type HelpTopic } from "@/lib/help-content";
import ReactMarkdown from "react-markdown";

interface HelpTooltipProps {
  topicId: string;
  children?: React.ReactNode;
  iconOnly?: boolean;
  iconSize?: "sm" | "md" | "lg";
  className?: string;
}

/**
 * HelpTooltip Component
 *
 * Displays a help icon that shows a tooltip on hover and opens a
 * detailed documentation dialog when clicked.
 */
export const HelpTooltip: React.FC<HelpTooltipProps> = ({
  topicId,
  children,
  iconOnly = false,
  iconSize = "sm",
  className = "",
}) => {
  const [dialogOpen, setDialogOpen] = useState(false);
  const [currentTopic, setCurrentTopic] = useState<HelpTopic | null>(null);

  const topic = getHelpTopic(topicId);

  if (!topic) {
    console.warn(`HelpTooltip: Topic "${topicId}" not found`);
    return children ? <>{children}</> : null;
  }

  const iconSizeClass = {
    sm: "w-3.5 h-3.5",
    md: "w-4 h-4",
    lg: "w-5 h-5",
  }[iconSize];

  const openDialog = (t: HelpTopic) => {
    setCurrentTopic(t);
    setDialogOpen(true);
  };

  const relatedTopics = currentTopic ? getRelatedTopics(currentTopic.id) : [];

  return (
    <>
      <TooltipProvider delayDuration={300}>
        <Tooltip>
          <TooltipTrigger asChild>
            <span
              className={`inline-flex items-center gap-1.5 cursor-help ${className}`}
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                openDialog(topic);
              }}
            >
              {!iconOnly && children}
              <HelpCircle
                className={`${iconSizeClass} text-muted-foreground hover:text-primary transition-colors flex-shrink-0`}
              />
            </span>
          </TooltipTrigger>
          <TooltipContent
            side="top"
            className="max-w-xs p-3 bg-popover border shadow-lg"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="space-y-2">
              <div className="font-medium text-sm">{topic.title}</div>
              <p className="text-xs text-muted-foreground leading-relaxed">
                {topic.shortDescription}
              </p>
              <Button
                variant="link"
                size="sm"
                className="h-auto p-0 text-xs text-primary"
                onClick={(e) => {
                  e.preventDefault();
                  e.stopPropagation();
                  openDialog(topic);
                }}
              >
                <BookOpen className="w-3 h-3 mr-1" />
                Learn more
              </Button>
            </div>
          </TooltipContent>
        </Tooltip>
      </TooltipProvider>

      {/* Documentation Dialog */}
      <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
        <DialogContent className="max-w-2xl max-h-[85vh] p-0 gap-0">
          <DialogHeader className="px-6 py-4 border-b bg-muted/30">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <BookOpen className="w-5 h-5 text-primary" />
                <DialogTitle className="text-lg">
                  {currentTopic?.title || topic.title}
                </DialogTitle>
              </div>
            </div>
            <DialogDescription className="text-sm text-muted-foreground mt-1">
              {currentTopic?.shortDescription || topic.shortDescription}
            </DialogDescription>
          </DialogHeader>

          <ScrollArea className="flex-1 max-h-[60vh]">
            <div className="px-6 py-4">
              {/* Markdown Content */}
              <div className="prose prose-sm dark:prose-invert max-w-none">
                <ReactMarkdown
                  components={{
                    h2: ({ children }) => (
                      <h2 className="text-lg font-semibold mt-4 mb-2 text-foreground">
                        {children}
                      </h2>
                    ),
                    h3: ({ children }) => (
                      <h3 className="text-base font-medium mt-3 mb-1.5 text-foreground">
                        {children}
                      </h3>
                    ),
                    p: ({ children }) => (
                      <p className="text-sm text-muted-foreground leading-relaxed mb-2">
                        {children}
                      </p>
                    ),
                    ul: ({ children }) => (
                      <ul className="text-sm text-muted-foreground list-disc pl-4 mb-2 space-y-1">
                        {children}
                      </ul>
                    ),
                    ol: ({ children }) => (
                      <ol className="text-sm text-muted-foreground list-decimal pl-4 mb-2 space-y-1">
                        {children}
                      </ol>
                    ),
                    li: ({ children }) => <li className="leading-relaxed">{children}</li>,
                    code: ({ children, className }) => {
                      const isBlock = className?.includes("language-");
                      if (isBlock) {
                        return (
                          <pre className="bg-muted rounded-md p-3 overflow-x-auto text-xs my-2">
                            <code>{children}</code>
                          </pre>
                        );
                      }
                      return (
                        <code className="bg-muted px-1 py-0.5 rounded text-xs font-mono">
                          {children}
                        </code>
                      );
                    },
                    pre: ({ children }) => <>{children}</>,
                    table: ({ children }) => (
                      <div className="overflow-x-auto my-2">
                        <table className="text-sm w-full border-collapse">
                          {children}
                        </table>
                      </div>
                    ),
                    thead: ({ children }) => (
                      <thead className="bg-muted/50">{children}</thead>
                    ),
                    th: ({ children }) => (
                      <th className="border border-border px-3 py-1.5 text-left font-medium text-xs">
                        {children}
                      </th>
                    ),
                    td: ({ children }) => (
                      <td className="border border-border px-3 py-1.5 text-xs text-muted-foreground">
                        {children}
                      </td>
                    ),
                    strong: ({ children }) => (
                      <strong className="font-semibold text-foreground">{children}</strong>
                    ),
                  }}
                >
                  {currentTopic?.detailedContent || topic.detailedContent}
                </ReactMarkdown>
              </div>
            </div>
          </ScrollArea>

          {/* Related Topics */}
          {relatedTopics.length > 0 && (
            <div className="px-6 py-3 border-t bg-muted/20">
              <div className="flex items-center gap-2 flex-wrap">
                <span className="text-xs text-muted-foreground">Related:</span>
                {relatedTopics.map((related) => (
                  <Badge
                    key={related.id}
                    variant="secondary"
                    className="cursor-pointer hover:bg-primary/20 text-xs"
                    onClick={() => openDialog(related)}
                  >
                    {related.title}
                  </Badge>
                ))}
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </>
  );
};

/**
 * HelpLabel Component
 *
 * A label with an integrated help tooltip - useful for form fields
 */
interface HelpLabelProps {
  topicId: string;
  children: React.ReactNode;
  className?: string;
  required?: boolean;
}

export const HelpLabel: React.FC<HelpLabelProps> = ({
  topicId,
  children,
  className = "",
  required = false,
}) => {
  return (
    <div className={`flex items-center gap-1 ${className}`}>
      <span className="text-sm font-medium">
        {children}
        {required && <span className="text-destructive ml-0.5">*</span>}
      </span>
      <HelpTooltip topicId={topicId} iconOnly iconSize="sm" />
    </div>
  );
};

/**
 * HelpSection Component
 *
 * A section header with help tooltip - useful for card/section titles
 */
interface HelpSectionProps {
  topicId: string;
  title: string;
  description?: string;
  icon?: React.ReactNode;
  className?: string;
}

export const HelpSection: React.FC<HelpSectionProps> = ({
  topicId,
  title,
  description,
  icon,
  className = "",
}) => {
  return (
    <div className={`flex items-start justify-between ${className}`}>
      <div className="flex items-center gap-2">
        {icon}
        <div>
          <div className="flex items-center gap-1.5">
            <span className="font-medium">{title}</span>
            <HelpTooltip topicId={topicId} iconOnly iconSize="sm" />
          </div>
          {description && (
            <p className="text-xs text-muted-foreground mt-0.5">{description}</p>
          )}
        </div>
      </div>
    </div>
  );
};

export default HelpTooltip;
