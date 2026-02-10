/**
 * Natural Language Hierarchy Builder Dialog
 * Chat-like interface for building hierarchies from natural language
 */
import { useState, useCallback, useRef, useEffect } from "react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Textarea } from "@/components/ui/textarea";
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
import {
  MessageSquare,
  Send,
  Sparkles,
  Loader2,
  ChevronDown,
  ChevronRight,
  FolderTree,
  RefreshCw,
  Check,
  Edit3,
  AlertTriangle,
  Wand2,
  Settings,
  TreePine,
} from "lucide-react";

interface GeneratedHierarchy {
  id: string;
  name: string;
  description?: string;
  level: number;
  children?: GeneratedHierarchy[];
  suggestedFormula?: {
    type: string;
    text: string;
  };
}

interface ChatMessage {
  id: string;
  role: "user" | "assistant";
  content: string;
  timestamp: Date;
  hierarchy?: GeneratedHierarchy;
  stats?: {
    totalNodes: number;
    maxDepth: number;
    leafNodes: number;
  };
}

interface NLHierarchyBuilderDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onAccept: (hierarchy: GeneratedHierarchy) => void;
  projectId?: string;
}

function HierarchyPreviewNode({
  node,
  depth = 0,
  expanded: defaultExpanded = true,
}: {
  node: GeneratedHierarchy;
  depth?: number;
  expanded?: boolean;
}) {
  const [expanded, setExpanded] = useState(defaultExpanded && depth < 2);
  const hasChildren = node.children && node.children.length > 0;

  return (
    <div className="select-none">
      <div
        className={cn(
          "flex items-center gap-2 py-1 px-2 rounded hover:bg-muted/50",
          depth === 0 && "font-medium"
        )}
        style={{ paddingLeft: `${depth * 16 + 8}px` }}
      >
        {hasChildren ? (
          <button onClick={() => setExpanded(!expanded)} className="p-0.5">
            {expanded ? (
              <ChevronDown className="h-4 w-4 text-muted-foreground" />
            ) : (
              <ChevronRight className="h-4 w-4 text-muted-foreground" />
            )}
          </button>
        ) : (
          <span className="w-5" />
        )}

        <TreePine className="h-4 w-4 text-primary shrink-0" />

        <span className="text-sm truncate">{node.name}</span>

        {node.suggestedFormula && (
          <Badge variant="outline" className="text-xs ml-auto">
            {node.suggestedFormula.type}
          </Badge>
        )}
      </div>

      {hasChildren && expanded && (
        <div>
          {node.children!.map((child) => (
            <HierarchyPreviewNode
              key={child.id}
              node={child}
              depth={depth + 1}
              expanded={depth < 1}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function ChatMessageBubble({ message }: { message: ChatMessage }) {
  const isUser = message.role === "user";

  return (
    <div className={cn("flex gap-3", isUser && "flex-row-reverse")}>
      <div
        className={cn(
          "w-8 h-8 rounded-full flex items-center justify-center shrink-0",
          isUser ? "bg-primary text-primary-foreground" : "bg-muted"
        )}
      >
        {isUser ? (
          <MessageSquare className="h-4 w-4" />
        ) : (
          <Sparkles className="h-4 w-4" />
        )}
      </div>

      <div className={cn("flex flex-col gap-2 max-w-[80%]", isUser && "items-end")}>
        <div
          className={cn(
            "rounded-lg px-3 py-2 text-sm",
            isUser ? "bg-primary text-primary-foreground" : "bg-muted"
          )}
        >
          {message.content}
        </div>

        {message.hierarchy && (
          <div className="w-full border rounded-lg overflow-hidden bg-background">
            <div className="p-2 bg-muted/50 border-b flex items-center justify-between">
              <div className="flex items-center gap-2 text-xs font-medium">
                <FolderTree className="h-4 w-4" />
                Generated Hierarchy
              </div>
              {message.stats && (
                <div className="flex gap-2 text-xs text-muted-foreground">
                  <span>{message.stats.totalNodes} nodes</span>
                  <span>{message.stats.maxDepth} levels</span>
                </div>
              )}
            </div>
            <ScrollArea className="max-h-[300px]">
              <div className="p-2">
                <HierarchyPreviewNode node={message.hierarchy} />
              </div>
            </ScrollArea>
          </div>
        )}

        <span className="text-xs text-muted-foreground">
          {message.timestamp.toLocaleTimeString()}
        </span>
      </div>
    </div>
  );
}

export function NLHierarchyBuilderDialog({
  open,
  onOpenChange,
  onAccept,
  projectId,
}: NLHierarchyBuilderDialogProps) {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [input, setInput] = useState("");
  const [isGenerating, setIsGenerating] = useState(false);
  const [currentHierarchy, setCurrentHierarchy] = useState<GeneratedHierarchy | null>(null);
  const [showSettings, setShowSettings] = useState(false);
  const [settings, setSettings] = useState({
    industry: "",
    hierarchyType: "" as "" | "income_statement" | "balance_sheet" | "cash_flow" | "custom",
    maxLevels: 5,
    includeFormulas: true,
  });

  const messagesEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  useEffect(() => {
    if (open) {
      inputRef.current?.focus();
      if (messages.length === 0) {
        // Add welcome message
        setMessages([
          {
            id: "welcome",
            role: "assistant",
            content:
              "Hi! I can help you build a financial hierarchy from a description. Tell me what kind of hierarchy you need, for example:\n\n• \"Create a standard P&L for a manufacturing company\"\n• \"Build an Oil & Gas Lease Operating Statement\"\n• \"Generate a SaaS company income statement with ARR tracking\"",
            timestamp: new Date(),
          },
        ]);
      }
    }
  }, [open]);

  const handleSend = useCallback(async () => {
    if (!input.trim() || isGenerating) return;

    const userMessage: ChatMessage = {
      id: `user-${Date.now()}`,
      role: "user",
      content: input.trim(),
      timestamp: new Date(),
    };

    setMessages((prev) => [...prev, userMessage]);
    setInput("");
    setIsGenerating(true);

    try {
      // Simulate API call - in production, this would call the backend
      await new Promise((resolve) => setTimeout(resolve, 2000));

      // Generate mock hierarchy based on input
      const hierarchy = generateMockHierarchy(input.trim(), settings);
      const stats = calculateStats(hierarchy);

      const assistantMessage: ChatMessage = {
        id: `assistant-${Date.now()}`,
        role: "assistant",
        content: currentHierarchy
          ? "I've updated the hierarchy based on your feedback. Here's the revised structure:"
          : "I've generated a hierarchy based on your description. You can ask me to make changes, or accept it when you're satisfied.",
        timestamp: new Date(),
        hierarchy,
        stats,
      };

      setMessages((prev) => [...prev, assistantMessage]);
      setCurrentHierarchy(hierarchy);
    } catch (error) {
      const errorMessage: ChatMessage = {
        id: `error-${Date.now()}`,
        role: "assistant",
        content: "Sorry, I encountered an error generating the hierarchy. Please try again.",
        timestamp: new Date(),
      };
      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setIsGenerating(false);
    }
  }, [input, isGenerating, currentHierarchy, settings]);

  const handleAccept = useCallback(() => {
    if (currentHierarchy) {
      onAccept(currentHierarchy);
      onOpenChange(false);
      // Reset state for next use
      setMessages([]);
      setCurrentHierarchy(null);
      setInput("");
    }
  }, [currentHierarchy, onAccept, onOpenChange]);

  const handleReset = useCallback(() => {
    setMessages([
      {
        id: "welcome-reset",
        role: "assistant",
        content: "Let's start fresh. Describe the hierarchy you'd like to build.",
        timestamp: new Date(),
      },
    ]);
    setCurrentHierarchy(null);
    setInput("");
  }, []);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl h-[80vh] flex flex-col p-0">
        <DialogHeader className="px-6 pt-6 pb-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <Wand2 className="h-5 w-5 text-primary" />
              <DialogTitle>AI Hierarchy Builder</DialogTitle>
            </div>
            <Button
              variant="ghost"
              size="sm"
              onClick={() => setShowSettings(!showSettings)}
              className="gap-1"
            >
              <Settings className="h-4 w-4" />
              Settings
            </Button>
          </div>
          <DialogDescription>
            Describe the hierarchy you want to create in natural language
          </DialogDescription>
        </DialogHeader>

        {/* Settings panel */}
        <Collapsible open={showSettings} onOpenChange={setShowSettings}>
          <CollapsibleContent>
            <div className="px-6 pb-4 space-y-4 border-b">
              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label>Industry</Label>
                  <Select
                    value={settings.industry}
                    onValueChange={(v) => setSettings((s) => ({ ...s, industry: v }))}
                  >
                    <SelectTrigger>
                      <SelectValue placeholder="Select industry..." />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="general">General</SelectItem>
                      <SelectItem value="oil_gas">Oil & Gas</SelectItem>
                      <SelectItem value="manufacturing">Manufacturing</SelectItem>
                      <SelectItem value="saas">SaaS</SelectItem>
                      <SelectItem value="transportation">Transportation</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="space-y-2">
                  <Label>Hierarchy Type</Label>
                  <Select
                    value={settings.hierarchyType}
                    onValueChange={(v: any) => setSettings((s) => ({ ...s, hierarchyType: v }))}
                  >
                    <SelectTrigger>
                      <SelectValue placeholder="Select type..." />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="income_statement">Income Statement</SelectItem>
                      <SelectItem value="balance_sheet">Balance Sheet</SelectItem>
                      <SelectItem value="cash_flow">Cash Flow</SelectItem>
                      <SelectItem value="custom">Custom</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
              <div className="flex items-center gap-4">
                <div className="space-y-2 flex-1">
                  <Label>Max Levels</Label>
                  <Input
                    type="number"
                    min={2}
                    max={10}
                    value={settings.maxLevels}
                    onChange={(e) =>
                      setSettings((s) => ({ ...s, maxLevels: parseInt(e.target.value) || 5 }))
                    }
                  />
                </div>
                <div className="flex items-center gap-2 pt-6">
                  <input
                    type="checkbox"
                    id="includeFormulas"
                    checked={settings.includeFormulas}
                    onChange={(e) =>
                      setSettings((s) => ({ ...s, includeFormulas: e.target.checked }))
                    }
                    className="rounded"
                  />
                  <Label htmlFor="includeFormulas">Include formula suggestions</Label>
                </div>
              </div>
            </div>
          </CollapsibleContent>
        </Collapsible>

        {/* Chat messages */}
        <ScrollArea className="flex-1 px-6">
          <div className="space-y-4 py-4">
            {messages.map((message) => (
              <ChatMessageBubble key={message.id} message={message} />
            ))}
            {isGenerating && (
              <div className="flex gap-3">
                <div className="w-8 h-8 rounded-full bg-muted flex items-center justify-center">
                  <Loader2 className="h-4 w-4 animate-spin" />
                </div>
                <div className="bg-muted rounded-lg px-3 py-2 text-sm text-muted-foreground">
                  Generating hierarchy...
                </div>
              </div>
            )}
            <div ref={messagesEndRef} />
          </div>
        </ScrollArea>

        <Separator />

        {/* Input area */}
        <div className="p-4 space-y-3">
          <div className="flex gap-2">
            <Input
              ref={inputRef}
              value={input}
              onChange={(e) => setInput(e.target.value)}
              placeholder={
                currentHierarchy
                  ? "Tell me what to change..."
                  : "Describe the hierarchy you need..."
              }
              onKeyDown={(e) => e.key === "Enter" && !e.shiftKey && handleSend()}
              disabled={isGenerating}
              className="flex-1"
            />
            <Button onClick={handleSend} disabled={!input.trim() || isGenerating}>
              <Send className="h-4 w-4" />
            </Button>
          </div>

          {currentHierarchy && (
            <div className="flex items-center justify-between">
              <Button variant="ghost" size="sm" onClick={handleReset} className="gap-1">
                <RefreshCw className="h-4 w-4" />
                Start Over
              </Button>
              <Button onClick={handleAccept} className="gap-1">
                <Check className="h-4 w-4" />
                Accept & Create Hierarchy
              </Button>
            </div>
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}

// Helper functions
function generateMockHierarchy(
  description: string,
  settings: { industry: string; hierarchyType: string; maxLevels: number; includeFormulas: boolean }
): GeneratedHierarchy {
  const lowerDesc = description.toLowerCase();

  if (lowerDesc.includes("p&l") || lowerDesc.includes("income") || settings.hierarchyType === "income_statement") {
    return {
      id: "gen-1",
      name: "Income Statement",
      level: 0,
      children: [
        {
          id: "gen-rev",
          name: "Revenue",
          level: 1,
          suggestedFormula: settings.includeFormulas ? { type: "SUM", text: "SUM(children)" } : undefined,
          children: [
            { id: "gen-prod", name: "Product Revenue", level: 2 },
            { id: "gen-serv", name: "Service Revenue", level: 2 },
          ],
        },
        {
          id: "gen-cogs",
          name: "Cost of Goods Sold",
          level: 1,
          suggestedFormula: settings.includeFormulas ? { type: "SUM", text: "SUM(children)" } : undefined,
          children: [
            { id: "gen-mat", name: "Materials", level: 2 },
            { id: "gen-lab", name: "Direct Labor", level: 2 },
          ],
        },
        {
          id: "gen-gp",
          name: "Gross Profit",
          level: 1,
          suggestedFormula: settings.includeFormulas ? { type: "SUBTRACT", text: "Revenue - COGS" } : undefined,
        },
        {
          id: "gen-opex",
          name: "Operating Expenses",
          level: 1,
          suggestedFormula: settings.includeFormulas ? { type: "SUM", text: "SUM(children)" } : undefined,
          children: [
            { id: "gen-sm", name: "Sales & Marketing", level: 2 },
            { id: "gen-rd", name: "R&D", level: 2 },
            { id: "gen-ga", name: "G&A", level: 2 },
          ],
        },
        {
          id: "gen-opinc",
          name: "Operating Income",
          level: 1,
          suggestedFormula: settings.includeFormulas ? { type: "SUBTRACT", text: "Gross Profit - OpEx" } : undefined,
        },
        {
          id: "gen-ni",
          name: "Net Income",
          level: 1,
          suggestedFormula: settings.includeFormulas ? { type: "SUBTRACT", text: "Operating Income - Tax" } : undefined,
        },
      ],
    };
  }

  // Default generic hierarchy
  return {
    id: "gen-root",
    name: description.split(" ").slice(0, 3).join(" "),
    level: 0,
    children: [
      {
        id: "gen-cat1",
        name: "Category 1",
        level: 1,
        children: [
          { id: "gen-sub1", name: "Subcategory 1.1", level: 2 },
          { id: "gen-sub2", name: "Subcategory 1.2", level: 2 },
        ],
      },
      {
        id: "gen-cat2",
        name: "Category 2",
        level: 1,
        children: [
          { id: "gen-sub3", name: "Subcategory 2.1", level: 2 },
          { id: "gen-sub4", name: "Subcategory 2.2", level: 2 },
        ],
      },
    ],
  };
}

function calculateStats(hierarchy: GeneratedHierarchy): { totalNodes: number; maxDepth: number; leafNodes: number } {
  let totalNodes = 0;
  let maxDepth = 0;
  let leafNodes = 0;

  const traverse = (node: GeneratedHierarchy, depth: number) => {
    totalNodes++;
    maxDepth = Math.max(maxDepth, depth);
    if (!node.children || node.children.length === 0) {
      leafNodes++;
    }
    node.children?.forEach((child) => traverse(child, depth + 1));
  };

  traverse(hierarchy, 1);
  return { totalNodes, maxDepth, leafNodes };
}
