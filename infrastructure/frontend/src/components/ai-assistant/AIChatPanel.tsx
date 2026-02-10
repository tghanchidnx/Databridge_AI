/**
 * AI Chat Panel
 * Embedded AI chat assistant for hierarchy operations
 */
import { useState, useCallback, useRef, useEffect } from "react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
  SheetTrigger,
} from "@/components/ui/sheet";
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@/components/ui/tooltip";
import {
  MessageSquare,
  Send,
  Bot,
  User,
  Loader2,
  Sparkles,
  X,
  Maximize2,
  Minimize2,
  RotateCcw,
  Copy,
  Check,
  ChevronRight,
  AlertCircle,
  Play,
  Lightbulb,
  FolderTree,
  Database,
  Calculator,
  Search,
  Download,
} from "lucide-react";

export interface ChatMessage {
  id: string;
  role: "user" | "assistant" | "system";
  content: string;
  timestamp: Date;
  functionCall?: {
    name: string;
    arguments: Record<string, any>;
    requiresConfirmation: boolean;
    status?: "pending" | "confirmed" | "rejected" | "executed";
  };
}

export interface ChatContext {
  projectId?: string;
  projectName?: string;
  currentHierarchyId?: string;
  currentHierarchyName?: string;
  hierarchyPath?: string[];
  projectStats?: {
    totalHierarchies: number;
    totalMappings: number;
    unmappedCount: number;
  };
}

interface AIChatPanelProps {
  sessionId: string;
  context: ChatContext;
  messages: ChatMessage[];
  isLoading?: boolean;
  onSendMessage: (message: string) => void;
  onConfirmAction: (messageId: string, confirmed: boolean) => void;
  onClearHistory: () => void;
  className?: string;
}

function getFunctionIcon(name: string) {
  const icons: Record<string, React.ReactNode> = {
    navigateToHierarchy: <FolderTree className="h-4 w-4" />,
    createHierarchy: <FolderTree className="h-4 w-4" />,
    updateMapping: <Database className="h-4 w-4" />,
    suggestFormula: <Calculator className="h-4 w-4" />,
    runValidation: <AlertCircle className="h-4 w-4" />,
    exportHierarchy: <Download className="h-4 w-4" />,
    findUnmappedNodes: <Search className="h-4 w-4" />,
    searchHierarchies: <Search className="h-4 w-4" />,
  };
  return icons[name] || <Play className="h-4 w-4" />;
}

function formatFunctionName(name: string): string {
  return name
    .replace(/([A-Z])/g, " $1")
    .replace(/^./, (str) => str.toUpperCase())
    .trim();
}

function ChatMessageItem({
  message,
  onConfirmAction,
}: {
  message: ChatMessage;
  onConfirmAction?: (confirmed: boolean) => void;
}) {
  const [copied, setCopied] = useState(false);
  const isUser = message.role === "user";
  const isSystem = message.role === "system";

  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(message.content);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }, [message.content]);

  if (isSystem) {
    return (
      <div className="flex justify-center py-2">
        <Badge variant="secondary" className="text-xs">
          {message.content}
        </Badge>
      </div>
    );
  }

  return (
    <div className={cn("flex gap-3 group", isUser && "flex-row-reverse")}>
      <div
        className={cn(
          "w-8 h-8 rounded-full flex items-center justify-center shrink-0",
          isUser ? "bg-primary text-primary-foreground" : "bg-muted"
        )}
      >
        {isUser ? <User className="h-4 w-4" /> : <Bot className="h-4 w-4" />}
      </div>

      <div className={cn("flex flex-col gap-1 max-w-[85%]", isUser && "items-end")}>
        <div
          className={cn(
            "rounded-lg px-3 py-2 text-sm",
            isUser ? "bg-primary text-primary-foreground" : "bg-muted"
          )}
        >
          <div className="whitespace-pre-wrap">{message.content}</div>

          {/* Function call card */}
          {message.functionCall && (
            <div className="mt-2 p-2 bg-background/50 rounded border">
              <div className="flex items-center gap-2 text-xs font-medium mb-2">
                {getFunctionIcon(message.functionCall.name)}
                {formatFunctionName(message.functionCall.name)}
              </div>

              <div className="text-xs font-mono bg-muted/50 p-1.5 rounded mb-2">
                {Object.entries(message.functionCall.arguments).map(([key, value]) => (
                  <div key={key}>
                    <span className="text-muted-foreground">{key}:</span>{" "}
                    <span>{String(value)}</span>
                  </div>
                ))}
              </div>

              {message.functionCall.requiresConfirmation &&
                message.functionCall.status === "pending" && (
                  <div className="flex gap-2">
                    <Button
                      size="sm"
                      className="h-7 text-xs flex-1"
                      onClick={() => onConfirmAction?.(true)}
                    >
                      <Check className="h-3 w-3 mr-1" />
                      Confirm
                    </Button>
                    <Button
                      size="sm"
                      variant="outline"
                      className="h-7 text-xs flex-1"
                      onClick={() => onConfirmAction?.(false)}
                    >
                      <X className="h-3 w-3 mr-1" />
                      Cancel
                    </Button>
                  </div>
                )}

              {message.functionCall.status === "confirmed" && (
                <Badge variant="default" className="text-xs">
                  <Check className="h-3 w-3 mr-1" />
                  Confirmed
                </Badge>
              )}

              {message.functionCall.status === "executed" && (
                <Badge variant="secondary" className="text-xs">
                  <Check className="h-3 w-3 mr-1" />
                  Executed
                </Badge>
              )}

              {message.functionCall.status === "rejected" && (
                <Badge variant="destructive" className="text-xs">
                  <X className="h-3 w-3 mr-1" />
                  Cancelled
                </Badge>
              )}
            </div>
          )}
        </div>

        <div className="flex items-center gap-2 opacity-0 group-hover:opacity-100 transition-opacity">
          <span className="text-xs text-muted-foreground">
            {message.timestamp.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}
          </span>
          {!isUser && (
            <Button size="icon" variant="ghost" className="h-6 w-6" onClick={handleCopy}>
              {copied ? (
                <Check className="h-3 w-3 text-green-600" />
              ) : (
                <Copy className="h-3 w-3" />
              )}
            </Button>
          )}
        </div>
      </div>
    </div>
  );
}

function SuggestionChip({
  suggestion,
  onClick,
}: {
  suggestion: string;
  onClick: () => void;
}) {
  return (
    <button
      onClick={onClick}
      className="inline-flex items-center gap-1 px-2.5 py-1 text-xs bg-muted hover:bg-muted/80 rounded-full transition-colors"
    >
      <ChevronRight className="h-3 w-3" />
      {suggestion}
    </button>
  );
}

export function AIChatPanel({
  sessionId,
  context,
  messages,
  isLoading,
  onSendMessage,
  onConfirmAction,
  onClearHistory,
  className,
}: AIChatPanelProps) {
  const [input, setInput] = useState("");
  const [isExpanded, setIsExpanded] = useState(false);
  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const handleSend = useCallback(() => {
    if (!input.trim() || isLoading) return;
    onSendMessage(input.trim());
    setInput("");
  }, [input, isLoading, onSendMessage]);

  const handleSuggestion = useCallback(
    (suggestion: string) => {
      onSendMessage(suggestion);
    },
    [onSendMessage]
  );

  const suggestions = [
    "Find unmapped nodes",
    "Run validation",
    "Show project stats",
    "Export as CSV",
  ];

  return (
    <div
      className={cn(
        "flex flex-col bg-background border rounded-lg shadow-lg",
        isExpanded ? "fixed inset-4 z-50" : "h-[500px]",
        className
      )}
    >
      {/* Header */}
      <div className="flex items-center justify-between p-3 border-b">
        <div className="flex items-center gap-2">
          <div className="w-8 h-8 rounded-full bg-primary/10 flex items-center justify-center">
            <Sparkles className="h-4 w-4 text-primary" />
          </div>
          <div>
            <div className="font-medium text-sm">AI Assistant</div>
            {context.projectName && (
              <div className="text-xs text-muted-foreground">
                Project: {context.projectName}
              </div>
            )}
          </div>
        </div>
        <div className="flex items-center gap-1">
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  size="icon"
                  variant="ghost"
                  className="h-8 w-8"
                  onClick={onClearHistory}
                >
                  <RotateCcw className="h-4 w-4" />
                </Button>
              </TooltipTrigger>
              <TooltipContent>Clear history</TooltipContent>
            </Tooltip>
          </TooltipProvider>
          <TooltipProvider>
            <Tooltip>
              <TooltipTrigger asChild>
                <Button
                  size="icon"
                  variant="ghost"
                  className="h-8 w-8"
                  onClick={() => setIsExpanded(!isExpanded)}
                >
                  {isExpanded ? (
                    <Minimize2 className="h-4 w-4" />
                  ) : (
                    <Maximize2 className="h-4 w-4" />
                  )}
                </Button>
              </TooltipTrigger>
              <TooltipContent>{isExpanded ? "Minimize" : "Expand"}</TooltipContent>
            </Tooltip>
          </TooltipProvider>
        </div>
      </div>

      {/* Context bar */}
      {context.currentHierarchyName && (
        <div className="px-3 py-2 bg-muted/50 border-b text-xs flex items-center gap-2">
          <FolderTree className="h-3 w-3" />
          <span className="text-muted-foreground">Viewing:</span>
          <span className="font-medium">{context.currentHierarchyName}</span>
        </div>
      )}

      {/* Messages */}
      <ScrollArea className="flex-1 p-4">
        <div className="space-y-4">
          {messages.length === 0 ? (
            <div className="text-center py-8">
              <Bot className="h-12 w-12 text-muted-foreground mx-auto mb-3" />
              <p className="font-medium">How can I help?</p>
              <p className="text-sm text-muted-foreground mt-1">
                Ask me about your hierarchies, mappings, or formulas
              </p>
              <div className="flex flex-wrap gap-2 justify-center mt-4">
                {suggestions.map((suggestion) => (
                  <SuggestionChip
                    key={suggestion}
                    suggestion={suggestion}
                    onClick={() => handleSuggestion(suggestion)}
                  />
                ))}
              </div>
            </div>
          ) : (
            messages.map((message) => (
              <ChatMessageItem
                key={message.id}
                message={message}
                onConfirmAction={(confirmed) => onConfirmAction(message.id, confirmed)}
              />
            ))
          )}
          {isLoading && (
            <div className="flex gap-3">
              <div className="w-8 h-8 rounded-full bg-muted flex items-center justify-center">
                <Loader2 className="h-4 w-4 animate-spin" />
              </div>
              <div className="bg-muted rounded-lg px-3 py-2 text-sm">
                <span className="inline-flex gap-1">
                  <span className="animate-bounce">.</span>
                  <span className="animate-bounce" style={{ animationDelay: "0.1s" }}>
                    .
                  </span>
                  <span className="animate-bounce" style={{ animationDelay: "0.2s" }}>
                    .
                  </span>
                </span>
              </div>
            </div>
          )}
          <div ref={messagesEndRef} />
        </div>
      </ScrollArea>

      {/* Quick suggestions */}
      {messages.length > 0 && (
        <div className="px-3 py-2 border-t">
          <div className="flex flex-wrap gap-1">
            {suggestions.slice(0, 3).map((suggestion) => (
              <SuggestionChip
                key={suggestion}
                suggestion={suggestion}
                onClick={() => handleSuggestion(suggestion)}
              />
            ))}
          </div>
        </div>
      )}

      {/* Input */}
      <div className="p-3 border-t">
        <div className="flex gap-2">
          <Input
            ref={inputRef}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Ask anything about your project..."
            onKeyDown={(e) => e.key === "Enter" && !e.shiftKey && handleSend()}
            disabled={isLoading}
            className="flex-1"
          />
          <Button onClick={handleSend} disabled={!input.trim() || isLoading} size="icon">
            <Send className="h-4 w-4" />
          </Button>
        </div>
      </div>
    </div>
  );
}

// Floating chat button with panel
export function FloatingAIChat({
  sessionId,
  context,
  messages,
  isLoading,
  onSendMessage,
  onConfirmAction,
  onClearHistory,
}: Omit<AIChatPanelProps, "className">) {
  const [open, setOpen] = useState(false);

  return (
    <Sheet open={open} onOpenChange={setOpen}>
      <SheetTrigger asChild>
        <Button
          size="lg"
          className="fixed bottom-6 right-6 h-14 w-14 rounded-full shadow-lg z-40"
        >
          <MessageSquare className="h-6 w-6" />
        </Button>
      </SheetTrigger>
      <SheetContent className="w-[400px] sm:w-[540px] p-0">
        <AIChatPanel
          sessionId={sessionId}
          context={context}
          messages={messages}
          isLoading={isLoading}
          onSendMessage={onSendMessage}
          onConfirmAction={onConfirmAction}
          onClearHistory={onClearHistory}
          className="h-full border-0 rounded-none shadow-none"
        />
      </SheetContent>
    </Sheet>
  );
}
