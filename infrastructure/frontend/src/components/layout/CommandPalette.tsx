/**
 * Command Palette with Slash Commands
 * Supports /templates, /ai, /project, /help, etc.
 */
import { useState, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import {
  CommandDialog,
  CommandInput,
  CommandList,
  CommandEmpty,
  CommandGroup,
  CommandItem,
  CommandSeparator,
  CommandShortcut,
} from "@/components/ui/command";
import {
  FileText,
  Brain,
  Folder,
  Database,
  Code,
  Settings,
  HelpCircle,
  Plus,
  Upload,
  Download,
  Sparkles,
  LayoutTemplate,
  Users,
  Zap,
  GitBranch,
  BookOpen,
  Play,
  Search,
  Calculator,
  Table,
  ArrowRight,
} from "lucide-react";
import { useToast } from "@/hooks/use-toast";

interface Command {
  id: string;
  label: string;
  description: string;
  icon: React.ReactNode;
  shortcut?: string;
  category: "navigation" | "actions" | "templates" | "ai" | "help";
  action: () => void;
}

interface CommandPaletteProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onViewChange?: (view: string) => void;
  initialQuery?: string;
}

export function CommandPalette({
  open,
  onOpenChange,
  onViewChange,
  initialQuery = "",
}: CommandPaletteProps) {
  const [query, setQuery] = useState(initialQuery);
  const { toast } = useToast();
  const navigate = useNavigate();

  // Update query when initialQuery changes
  useEffect(() => {
    if (open && initialQuery) {
      setQuery(initialQuery);
    }
  }, [open, initialQuery]);

  const handleViewChange = useCallback(
    (view: string) => {
      if (onViewChange) {
        onViewChange(view);
      } else {
        navigate(`/${view}`);
      }
      onOpenChange(false);
    },
    [onViewChange, navigate, onOpenChange]
  );

  const commands: Command[] = [
    // Navigation Commands
    {
      id: "nav-dashboard",
      label: "Go to Dashboard",
      description: "View analytics and overview",
      icon: <Search className="h-4 w-4" />,
      shortcut: "⌘D",
      category: "navigation",
      action: () => handleViewChange("dashboard"),
    },
    {
      id: "nav-hierarchy",
      label: "Go to Hierarchy Knowledge Base",
      description: "Manage hierarchies and mappings",
      icon: <GitBranch className="h-4 w-4" />,
      shortcut: "⌘H",
      category: "navigation",
      action: () => handleViewChange("hierarchy-knowledge-base"),
    },
    {
      id: "nav-connections",
      label: "Go to Connections",
      description: "Manage database connections",
      icon: <Database className="h-4 w-4" />,
      shortcut: "⌘C",
      category: "navigation",
      action: () => handleViewChange("connections"),
    },
    {
      id: "nav-ai-config",
      label: "Go to AI Configuration",
      description: "Configure AI providers and settings",
      icon: <Brain className="h-4 w-4" />,
      shortcut: "⌘I",
      category: "navigation",
      action: () => handleViewChange("ai-config"),
    },
    {
      id: "nav-settings",
      label: "Go to Settings",
      description: "Profile and workspace settings",
      icon: <Settings className="h-4 w-4" />,
      shortcut: "⌘,",
      category: "navigation",
      action: () => handleViewChange("settings"),
    },

    // Action Commands
    {
      id: "action-new-project",
      label: "Create New Project",
      description: "Start a new hierarchy project",
      icon: <Plus className="h-4 w-4" />,
      shortcut: "⌘N",
      category: "actions",
      action: () => {
        handleViewChange("hierarchy-knowledge-base");
        toast({
          title: "Create Project",
          description: "Opening project creation dialog...",
        });
      },
    },
    {
      id: "action-import",
      label: "Import CSV/Excel",
      description: "Import hierarchies from file",
      icon: <Upload className="h-4 w-4" />,
      category: "actions",
      action: () => {
        handleViewChange("hierarchy-knowledge-base");
        toast({
          title: "Import",
          description: "Opening import dialog...",
        });
      },
    },
    {
      id: "action-export",
      label: "Export Project",
      description: "Export hierarchies to CSV/Excel",
      icon: <Download className="h-4 w-4" />,
      category: "actions",
      action: () => {
        handleViewChange("hierarchy-knowledge-base");
        toast({
          title: "Export",
          description: "Opening export dialog...",
        });
      },
    },
    {
      id: "action-generate-script",
      label: "Generate SQL Script",
      description: "Generate deployment scripts",
      icon: <Code className="h-4 w-4" />,
      category: "actions",
      action: () => {
        handleViewChange("hierarchy-knowledge-base");
        toast({
          title: "Generate Script",
          description: "Opening script generation...",
        });
      },
    },
    {
      id: "action-formula",
      label: "Manage Formulas",
      description: "Create and edit calculation formulas",
      icon: <Calculator className="h-4 w-4" />,
      category: "actions",
      action: () => {
        handleViewChange("hierarchy-knowledge-base");
        toast({
          title: "Formulas",
          description: "Opening formula manager...",
        });
      },
    },

    // Template Commands
    {
      id: "template-browse",
      label: "Browse Templates",
      description: "View all available templates",
      icon: <LayoutTemplate className="h-4 w-4" />,
      category: "templates",
      action: () => {
        handleViewChange("ai-config");
        toast({
          title: "Templates",
          description: "Opening template gallery...",
        });
      },
    },
    {
      id: "template-pl",
      label: "Standard P&L Template",
      description: "Income statement structure",
      icon: <FileText className="h-4 w-4" />,
      category: "templates",
      action: () => {
        toast({
          title: "Loading Template",
          description: "Creating project from Standard P&L template...",
        });
      },
    },
    {
      id: "template-bs",
      label: "Balance Sheet Template",
      description: "Balance sheet structure",
      icon: <FileText className="h-4 w-4" />,
      category: "templates",
      action: () => {
        toast({
          title: "Loading Template",
          description: "Creating project from Balance Sheet template...",
        });
      },
    },
    {
      id: "template-oil-gas",
      label: "Oil & Gas LOS Template",
      description: "Lease Operating Statement",
      icon: <FileText className="h-4 w-4" />,
      category: "templates",
      action: () => {
        toast({
          title: "Loading Template",
          description: "Creating project from Oil & Gas LOS template...",
        });
      },
    },
    {
      id: "template-saas",
      label: "SaaS P&L Template",
      description: "SaaS metrics and ARR/MRR",
      icon: <FileText className="h-4 w-4" />,
      category: "templates",
      action: () => {
        toast({
          title: "Loading Template",
          description: "Creating project from SaaS P&L template...",
        });
      },
    },

    // AI Commands
    {
      id: "ai-suggest",
      label: "AI Suggest Mappings",
      description: "Get AI-powered mapping suggestions",
      icon: <Sparkles className="h-4 w-4" />,
      category: "ai",
      action: () => {
        toast({
          title: "AI Suggestions",
          description: "AI will analyze your hierarchy and suggest mappings. Select a hierarchy first.",
        });
      },
    },
    {
      id: "ai-build",
      label: "AI Build Hierarchy",
      description: "Build hierarchy from natural language",
      icon: <Zap className="h-4 w-4" />,
      category: "ai",
      action: () => {
        toast({
          title: "AI Builder",
          description: "Opening natural language hierarchy builder...",
        });
      },
    },
    {
      id: "ai-chat",
      label: "AI Chat Assistant",
      description: "Chat with AI for help",
      icon: <Brain className="h-4 w-4" />,
      category: "ai",
      action: () => {
        toast({
          title: "AI Chat",
          description: "Opening AI assistant...",
        });
      },
    },
    {
      id: "ai-config",
      label: "Configure AI Provider",
      description: "Set up Claude, OpenAI, or other AI",
      icon: <Settings className="h-4 w-4" />,
      category: "ai",
      action: () => handleViewChange("ai-config"),
    },

    // Help Commands
    {
      id: "help-docs",
      label: "Documentation",
      description: "View user guide and docs",
      icon: <BookOpen className="h-4 w-4" />,
      shortcut: "⌘?",
      category: "help",
      action: () => handleViewChange("docs"),
    },
    {
      id: "help-demo",
      label: "Feature Demo",
      description: "Interactive feature tour",
      icon: <Play className="h-4 w-4" />,
      category: "help",
      action: () => handleViewChange("demo"),
    },
    {
      id: "help-shortcuts",
      label: "Keyboard Shortcuts",
      description: "View all shortcuts",
      icon: <HelpCircle className="h-4 w-4" />,
      shortcut: "?",
      category: "help",
      action: () => {
        const event = new KeyboardEvent("keydown", { key: "?" });
        document.dispatchEvent(event);
        onOpenChange(false);
      },
    },
  ];

  // Filter commands based on query
  const filteredCommands = commands.filter((cmd) => {
    const searchLower = query.toLowerCase().replace(/^\//, "");
    return (
      cmd.label.toLowerCase().includes(searchLower) ||
      cmd.description.toLowerCase().includes(searchLower) ||
      cmd.category.includes(searchLower)
    );
  });

  // Group commands by category
  const groupedCommands = {
    navigation: filteredCommands.filter((c) => c.category === "navigation"),
    actions: filteredCommands.filter((c) => c.category === "actions"),
    templates: filteredCommands.filter((c) => c.category === "templates"),
    ai: filteredCommands.filter((c) => c.category === "ai"),
    help: filteredCommands.filter((c) => c.category === "help"),
  };

  // Show relevant groups based on query
  const showGroup = (category: string) => {
    const q = query.toLowerCase();
    if (!q || q === "/") return true;
    if (q.startsWith("/template")) return category === "templates";
    if (q.startsWith("/ai")) return category === "ai";
    if (q.startsWith("/help") || q.startsWith("/?")) return category === "help";
    if (q.startsWith("/go") || q.startsWith("/nav")) return category === "navigation";
    if (q.startsWith("/action") || q.startsWith("/do")) return category === "actions";
    return groupedCommands[category as keyof typeof groupedCommands].length > 0;
  };

  return (
    <CommandDialog open={open} onOpenChange={onOpenChange}>
      <CommandInput
        placeholder="Type a command or search... (try /templates, /ai, /help)"
        value={query}
        onValueChange={setQuery}
      />
      <CommandList>
        <CommandEmpty>
          No commands found. Try /templates, /ai, or /help
        </CommandEmpty>

        {showGroup("navigation") && groupedCommands.navigation.length > 0 && (
          <CommandGroup heading="Navigation">
            {groupedCommands.navigation.map((cmd) => (
              <CommandItem key={cmd.id} onSelect={cmd.action}>
                {cmd.icon}
                <div className="flex flex-col flex-1 ml-2">
                  <span>{cmd.label}</span>
                  <span className="text-xs text-muted-foreground">
                    {cmd.description}
                  </span>
                </div>
                {cmd.shortcut && (
                  <CommandShortcut>{cmd.shortcut}</CommandShortcut>
                )}
              </CommandItem>
            ))}
          </CommandGroup>
        )}

        {showGroup("actions") && groupedCommands.actions.length > 0 && (
          <>
            <CommandSeparator />
            <CommandGroup heading="Actions">
              {groupedCommands.actions.map((cmd) => (
                <CommandItem key={cmd.id} onSelect={cmd.action}>
                  {cmd.icon}
                  <div className="flex flex-col flex-1 ml-2">
                    <span>{cmd.label}</span>
                    <span className="text-xs text-muted-foreground">
                      {cmd.description}
                    </span>
                  </div>
                  {cmd.shortcut && (
                    <CommandShortcut>{cmd.shortcut}</CommandShortcut>
                  )}
                </CommandItem>
              ))}
            </CommandGroup>
          </>
        )}

        {showGroup("templates") && groupedCommands.templates.length > 0 && (
          <>
            <CommandSeparator />
            <CommandGroup heading="Templates">
              {groupedCommands.templates.map((cmd) => (
                <CommandItem key={cmd.id} onSelect={cmd.action}>
                  {cmd.icon}
                  <div className="flex flex-col flex-1 ml-2">
                    <span>{cmd.label}</span>
                    <span className="text-xs text-muted-foreground">
                      {cmd.description}
                    </span>
                  </div>
                  <ArrowRight className="h-4 w-4 text-muted-foreground" />
                </CommandItem>
              ))}
            </CommandGroup>
          </>
        )}

        {showGroup("ai") && groupedCommands.ai.length > 0 && (
          <>
            <CommandSeparator />
            <CommandGroup heading="AI Features">
              {groupedCommands.ai.map((cmd) => (
                <CommandItem key={cmd.id} onSelect={cmd.action}>
                  {cmd.icon}
                  <div className="flex flex-col flex-1 ml-2">
                    <span>{cmd.label}</span>
                    <span className="text-xs text-muted-foreground">
                      {cmd.description}
                    </span>
                  </div>
                  <Sparkles className="h-4 w-4 text-purple-500" />
                </CommandItem>
              ))}
            </CommandGroup>
          </>
        )}

        {showGroup("help") && groupedCommands.help.length > 0 && (
          <>
            <CommandSeparator />
            <CommandGroup heading="Help">
              {groupedCommands.help.map((cmd) => (
                <CommandItem key={cmd.id} onSelect={cmd.action}>
                  {cmd.icon}
                  <div className="flex flex-col flex-1 ml-2">
                    <span>{cmd.label}</span>
                    <span className="text-xs text-muted-foreground">
                      {cmd.description}
                    </span>
                  </div>
                  {cmd.shortcut && (
                    <CommandShortcut>{cmd.shortcut}</CommandShortcut>
                  )}
                </CommandItem>
              ))}
            </CommandGroup>
          </>
        )}
      </CommandList>
    </CommandDialog>
  );
}

// Export slash command categories for MCP integration
export const SLASH_COMMANDS = {
  "/templates": "Browse and use hierarchy templates",
  "/template pl": "Use Standard P&L template",
  "/template bs": "Use Balance Sheet template",
  "/template oil-gas": "Use Oil & Gas LOS template",
  "/template saas": "Use SaaS P&L template",
  "/ai": "AI features and suggestions",
  "/ai suggest": "Get AI mapping suggestions",
  "/ai build": "Build hierarchy with natural language",
  "/ai chat": "Open AI assistant",
  "/ai config": "Configure AI provider",
  "/go dashboard": "Navigate to dashboard",
  "/go hierarchy": "Navigate to hierarchy knowledge base",
  "/go connections": "Navigate to connections",
  "/go settings": "Navigate to settings",
  "/new project": "Create a new project",
  "/import": "Import CSV/Excel file",
  "/export": "Export project",
  "/generate script": "Generate SQL scripts",
  "/help": "View documentation",
  "/help demo": "Start feature demo",
  "/help shortcuts": "View keyboard shortcuts",
};
