/**
 * Documentation Page
 * Comprehensive user guide and documentation for all features
 */
import { useState, useMemo } from "react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import {
  BookOpen,
  Search,
  ChevronRight,
  ExternalLink,
  Video,
  Code,
  Lightbulb,
  AlertCircle,
  CheckCircle,
  Keyboard,
  Sparkles,
  GitBranch,
  Database,
  Calculator,
  MessageSquare,
  FileUp,
  LayoutGrid,
  BarChart3,
  AlertTriangle,
  Wand2,
  Play,
  Copy,
  Check,
  Layers,
} from "lucide-react";

interface DocSection {
  id: string;
  title: string;
  icon: React.ReactNode;
  description: string;
  content: DocContent[];
}

interface DocContent {
  type: "text" | "code" | "tip" | "warning" | "steps" | "shortcuts" | "faq";
  content: any;
}

const DOCUMENTATION: DocSection[] = [
  {
    id: "getting-started",
    title: "Getting Started",
    icon: <BookOpen className="h-5 w-5" />,
    description: "Learn the basics of DataBridge AI and set up your first project",
    content: [
      {
        type: "text",
        content: `DataBridge AI is a powerful tool for building and managing financial hierarchies.
        It uses AI to suggest mappings, formulas, and help you work faster.`,
      },
      {
        type: "steps",
        content: [
          {
            title: "Create a New Project",
            description: "Click 'New Project' and give it a name. Choose an industry template or start from scratch.",
          },
          {
            title: "Build Your Hierarchy",
            description: "Add nodes to create your hierarchy structure. Use drag-and-drop to organize.",
          },
          {
            title: "Configure Mappings",
            description: "Connect hierarchy nodes to your data sources. AI will suggest mappings automatically.",
          },
          {
            title: "Set Up Formulas",
            description: "Define calculated fields using the formula builder. AI suggests common patterns.",
          },
          {
            title: "Validate and Deploy",
            description: "Run validation checks and deploy your hierarchy to Snowflake or export as CSV.",
          },
        ],
      },
      {
        type: "tip",
        content: "Press '?' at any time to see keyboard shortcuts and get help.",
      },
    ],
  },
  {
    id: "hierarchy-building",
    title: "Building Hierarchies",
    icon: <GitBranch className="h-5 w-5" />,
    description: "Learn how to create and manage hierarchy structures",
    content: [
      {
        type: "text",
        content: `Hierarchies are tree structures that represent your financial reporting needs.
        Each node can be a line item, category, or calculated value.`,
      },
      {
        type: "steps",
        content: [
          {
            title: "Adding Nodes",
            description: "Right-click a node and select 'Add Child' or use Ctrl+N. Give it a descriptive name.",
          },
          {
            title: "Moving Nodes",
            description: "Drag nodes to reorder them. Hold Shift to move to a different parent.",
          },
          {
            title: "Editing Nodes",
            description: "Double-click or press Enter to edit. Change name, description, or properties.",
          },
          {
            title: "Deleting Nodes",
            description: "Select and press Delete. Confirm deletion of nodes with children.",
          },
        ],
      },
      {
        type: "shortcuts",
        content: [
          { key: "Ctrl+N", action: "Add new child node" },
          { key: "Enter", action: "Edit selected node" },
          { key: "Delete", action: "Delete selected node" },
          { key: "Ctrl+D", action: "Duplicate node" },
          { key: "Ctrl+↑/↓", action: "Move node up/down" },
        ],
      },
      {
        type: "tip",
        content: "Use the Natural Language Builder to describe your hierarchy in plain English and let AI generate it for you.",
      },
    ],
  },
  {
    id: "source-mapping",
    title: "Source Mapping",
    icon: <Database className="h-5 w-5" />,
    description: "Connect hierarchies to your data sources",
    content: [
      {
        type: "text",
        content: `Source mappings define where data comes from for each hierarchy node.
        You can map to database columns, and the AI will suggest the best matches.`,
      },
      {
        type: "steps",
        content: [
          {
            title: "Select a Node",
            description: "Click on a hierarchy node to open the mapping panel.",
          },
          {
            title: "Review AI Suggestions",
            description: "Look at the suggested mappings. Higher confidence means better match.",
          },
          {
            title: "Accept or Configure",
            description: "Click to accept a suggestion, or manually select database, table, and column.",
          },
          {
            title: "Add Multiple Mappings",
            description: "Some nodes need multiple mappings. Use precedence groups to control priority.",
          },
        ],
      },
      {
        type: "warning",
        content: "Make sure all leaf nodes have mappings before deploying. Use the Anomaly Detection panel to find unmapped nodes.",
      },
      {
        type: "faq",
        content: [
          {
            q: "What if the AI suggestion is wrong?",
            a: "Reject the suggestion and the AI will learn from your choice. You can always manually configure mappings.",
          },
          {
            q: "Can I map to multiple columns?",
            a: "Yes, use multiple mappings with different precedence groups to combine data from multiple sources.",
          },
        ],
      },
    ],
  },
  {
    id: "formulas",
    title: "Formulas & Calculations",
    icon: <Calculator className="h-5 w-5" />,
    description: "Create calculated fields with formulas",
    content: [
      {
        type: "text",
        content: `Formulas define how parent nodes are calculated from their children or other nodes.
        Common patterns like Gross Profit, Operating Income, and EBITDA are auto-detected.`,
      },
      {
        type: "steps",
        content: [
          {
            title: "Open Formula Builder",
            description: "Select a non-leaf node and click 'Add Formula' or use Ctrl+F.",
          },
          {
            title: "Choose Formula Type",
            description: "Select SUM, SUBTRACT, MULTIPLY, DIVIDE, or CUSTOM.",
          },
          {
            title: "Define Variables",
            description: "Reference other nodes using {NodeName} syntax or click to add.",
          },
          {
            title: "Validate and Apply",
            description: "Check the preview and click Apply. Errors are highlighted.",
          },
        ],
      },
      {
        type: "code",
        content: `// Common Formula Examples
Gross Profit = {Revenue} - {Cost of Goods Sold}
Operating Income = {Gross Profit} - {Operating Expenses}
EBITDA = {Operating Income} + {Depreciation} + {Amortization}
Net Income = {Operating Income} + {Other Income} - {Tax Expense}
Gross Margin = {Gross Profit} / {Revenue} * 100`,
      },
      {
        type: "tip",
        content: "The AI automatically suggests formulas based on standard financial patterns. Look for the sparkle icon!",
      },
    ],
  },
  {
    id: "ai-features",
    title: "AI Features",
    icon: <Sparkles className="h-5 w-5" />,
    description: "Leverage AI for smarter hierarchy building",
    content: [
      {
        type: "text",
        content: `DataBridge AI uses advanced AI models to help you work faster and more accurately.
        All AI features learn from your choices to improve over time.`,
      },
      {
        type: "steps",
        content: [
          {
            title: "AI Mapping Suggestions",
            description: "Automatic suggestions for source mappings based on node names and patterns.",
          },
          {
            title: "Formula Auto-Suggest",
            description: "Detects common financial patterns and suggests appropriate formulas.",
          },
          {
            title: "Natural Language Builder",
            description: "Describe what you need and let AI generate the hierarchy structure.",
          },
          {
            title: "AI Chat Assistant",
            description: "Ask questions, get help, and execute actions using natural language.",
          },
          {
            title: "Anomaly Detection",
            description: "Automatic detection of issues and inconsistencies in your project.",
          },
        ],
      },
      {
        type: "tip",
        content: "The more you use the AI features, the better they become at understanding your patterns and preferences.",
      },
    ],
  },
  {
    id: "ai-chat",
    title: "AI Chat Assistant",
    icon: <MessageSquare className="h-5 w-5" />,
    description: "Get help and execute actions with natural language",
    content: [
      {
        type: "text",
        content: `The AI Chat Assistant understands natural language and can help you with questions,
        explanations, and even execute actions directly.`,
      },
      {
        type: "steps",
        content: [
          {
            title: "Ask Questions",
            description: "\"What is this node mapped to?\" or \"How do I add a formula?\"",
          },
          {
            title: "Request Actions",
            description: "\"Find all unmapped nodes\" or \"Navigate to Revenue\"",
          },
          {
            title: "Get Suggestions",
            description: "\"Suggest a formula for Gross Profit\" or \"What should I map Operating Expenses to?\"",
          },
          {
            title: "Debug Issues",
            description: "\"Why is this validation failing?\" or \"What's wrong with my formula?\"",
          },
        ],
      },
      {
        type: "code",
        content: `// Example Chat Commands
"Find unmapped leaf nodes"
"Show me the project health score"
"Navigate to the Revenue hierarchy"
"Suggest formulas for all calculated fields"
"Export the hierarchy as CSV"
"Run validation on this project"`,
      },
    ],
  },
  {
    id: "templates",
    title: "Templates",
    icon: <LayoutGrid className="h-5 w-5" />,
    description: "Use pre-built templates to get started quickly",
    content: [
      {
        type: "text",
        content: `Templates provide pre-built hierarchy structures for common financial reports and industries.
        Start with a template and customize it for your needs.`,
      },
      {
        type: "steps",
        content: [
          {
            title: "Browse Templates",
            description: "Open the Template Gallery and filter by category or industry.",
          },
          {
            title: "Preview Structure",
            description: "Click 'Preview' to see the hierarchy structure before using.",
          },
          {
            title: "Use Template",
            description: "Click 'Use Template' to create a new project based on it.",
          },
          {
            title: "Customize",
            description: "Modify the structure, add mappings, and adjust formulas for your needs.",
          },
        ],
      },
      {
        type: "tip",
        content: "Save your own projects as templates to reuse them later or share with your team.",
      },
      {
        type: "faq",
        content: [
          {
            q: "What templates are available?",
            a: "We have 20+ templates including Standard P&L, Balance Sheet, Cash Flow, and industry-specific templates for Oil & Gas, SaaS, Manufacturing, and more.",
          },
          {
            q: "Can I create my own templates?",
            a: "Yes! Any project can be saved as a custom template from the project menu.",
          },
        ],
      },
    ],
  },
  {
    id: "import-export",
    title: "Import & Export",
    icon: <FileUp className="h-5 w-5" />,
    description: "Import from CSV and export to various formats",
    content: [
      {
        type: "text",
        content: `DataBridge AI supports importing hierarchies from CSV files and exporting to CSV, JSON, and SQL formats.`,
      },
      {
        type: "steps",
        content: [
          {
            title: "Import from CSV",
            description: "Click Import and select your CSV file. The smart importer will detect the format.",
          },
          {
            title: "Review Mappings",
            description: "Check the column mappings and adjust if needed.",
          },
          {
            title: "Preview Changes",
            description: "Use the diff view to see what will be added, modified, or removed.",
          },
          {
            title: "Apply Import",
            description: "Select the changes you want to apply and click Import.",
          },
        ],
      },
      {
        type: "warning",
        content: "Always backup your project before importing large changes. Use the diff view to review changes carefully.",
      },
    ],
  },
  {
    id: "keyboard-shortcuts",
    title: "Keyboard Shortcuts",
    icon: <Keyboard className="h-5 w-5" />,
    description: "Work faster with keyboard shortcuts",
    content: [
      {
        type: "text",
        content: `DataBridge AI includes 30+ keyboard shortcuts to help you work faster.
        Press '?' at any time to see the full list.`,
      },
      {
        type: "shortcuts",
        content: [
          { key: "↑ ↓", action: "Navigate between nodes" },
          { key: "← →", action: "Collapse/Expand node" },
          { key: "Enter", action: "Edit selected node" },
          { key: "Ctrl+N", action: "Add new child node" },
          { key: "Ctrl+S", action: "Save project" },
          { key: "Ctrl+Z", action: "Undo" },
          { key: "Ctrl+Shift+Z", action: "Redo" },
          { key: "Ctrl+F", action: "Open formula builder" },
          { key: "Ctrl+M", action: "Open mapping panel" },
          { key: "Delete", action: "Delete selected" },
          { key: "Ctrl+D", action: "Duplicate node" },
          { key: "Ctrl+/", action: "Toggle AI chat" },
          { key: "?", action: "Show shortcuts help" },
          { key: "Escape", action: "Close dialogs/Cancel" },
        ],
      },
    ],
  },
  {
    id: "validation",
    title: "Validation & Anomaly Detection",
    icon: <AlertTriangle className="h-5 w-5" />,
    description: "Detect and fix issues in your project",
    content: [
      {
        type: "text",
        content: `The validation system automatically checks for common issues like missing mappings,
        formula errors, and inconsistent patterns. Review and fix issues before deploying.`,
      },
      {
        type: "steps",
        content: [
          {
            title: "View Anomaly Panel",
            description: "Open the Anomaly Detection panel to see all detected issues.",
          },
          {
            title: "Filter by Severity",
            description: "Filter by errors, warnings, or info to focus on what matters.",
          },
          {
            title: "Navigate to Issue",
            description: "Click on an issue to navigate to the affected hierarchy node.",
          },
          {
            title: "Apply Fixes",
            description: "Use 'Auto-Fix' for supported issues or manually resolve them.",
          },
        ],
      },
      {
        type: "faq",
        content: [
          {
            q: "What issues are detected?",
            a: "Missing mappings, type mismatches, duplicate mappings, circular references, missing formulas, formula errors, naming issues, and pattern inconsistencies.",
          },
          {
            q: "Can I ignore certain warnings?",
            a: "Yes, you can dismiss individual warnings or configure validation rules in project settings.",
          },
        ],
      },
    ],
  },
];

function CodeBlock({ code }: { code: string }) {
  const [copied, setCopied] = useState(false);

  const handleCopy = () => {
    navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="relative">
      <pre className="bg-muted p-4 rounded-lg text-sm overflow-x-auto">
        <code>{code}</code>
      </pre>
      <Button
        size="icon"
        variant="ghost"
        className="absolute top-2 right-2 h-8 w-8"
        onClick={handleCopy}
      >
        {copied ? <Check className="h-4 w-4" /> : <Copy className="h-4 w-4" />}
      </Button>
    </div>
  );
}

function DocContentRenderer({ content }: { content: DocContent }) {
  switch (content.type) {
    case "text":
      return <p className="text-muted-foreground leading-relaxed">{content.content}</p>;

    case "code":
      return <CodeBlock code={content.content} />;

    case "tip":
      return (
        <div className="flex items-start gap-3 p-4 rounded-lg bg-blue-50 border border-blue-200">
          <Lightbulb className="h-5 w-5 text-blue-600 shrink-0 mt-0.5" />
          <p className="text-sm text-blue-800">{content.content}</p>
        </div>
      );

    case "warning":
      return (
        <div className="flex items-start gap-3 p-4 rounded-lg bg-yellow-50 border border-yellow-200">
          <AlertCircle className="h-5 w-5 text-yellow-600 shrink-0 mt-0.5" />
          <p className="text-sm text-yellow-800">{content.content}</p>
        </div>
      );

    case "steps":
      return (
        <div className="space-y-4">
          {content.content.map((step: any, i: number) => (
            <div key={i} className="flex gap-4">
              <div className="flex-shrink-0 w-8 h-8 rounded-full bg-primary/10 text-primary flex items-center justify-center font-medium text-sm">
                {i + 1}
              </div>
              <div>
                <h4 className="font-medium">{step.title}</h4>
                <p className="text-sm text-muted-foreground mt-1">{step.description}</p>
              </div>
            </div>
          ))}
        </div>
      );

    case "shortcuts":
      return (
        <div className="grid grid-cols-2 gap-3">
          {content.content.map((shortcut: any, i: number) => (
            <div
              key={i}
              className="flex items-center justify-between p-2 rounded-lg bg-muted/50"
            >
              <kbd className="px-2 py-1 bg-background border rounded text-sm font-mono">
                {shortcut.key}
              </kbd>
              <span className="text-sm text-muted-foreground">{shortcut.action}</span>
            </div>
          ))}
        </div>
      );

    case "faq":
      return (
        <Accordion type="single" collapsible className="w-full">
          {content.content.map((item: any, i: number) => (
            <AccordionItem key={i} value={`faq-${i}`}>
              <AccordionTrigger className="text-left">{item.q}</AccordionTrigger>
              <AccordionContent className="text-muted-foreground">
                {item.a}
              </AccordionContent>
            </AccordionItem>
          ))}
        </Accordion>
      );

    default:
      return null;
  }
}

export function DocumentationPage() {
  const [searchQuery, setSearchQuery] = useState("");
  const [activeSection, setActiveSection] = useState("getting-started");

  const filteredDocs = useMemo(() => {
    if (!searchQuery) return DOCUMENTATION;
    const query = searchQuery.toLowerCase();
    return DOCUMENTATION.filter(
      (section) =>
        section.title.toLowerCase().includes(query) ||
        section.description.toLowerCase().includes(query)
    );
  }, [searchQuery]);

  const currentSection = DOCUMENTATION.find((s) => s.id === activeSection);

  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <div className="border-b bg-muted/30">
        <div className="container mx-auto px-6 py-8">
          <div className="flex items-center gap-3 mb-4">
            <div className="p-2 rounded-lg bg-primary/10">
              <BookOpen className="h-6 w-6 text-primary" />
            </div>
            <div>
              <h1 className="text-2xl font-bold">Documentation</h1>
              <p className="text-muted-foreground">Learn how to use DataBridge AI</p>
            </div>
          </div>
          <div className="relative max-w-md">
            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
            <Input
              placeholder="Search documentation..."
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="pl-10"
            />
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="container mx-auto px-6 py-8">
        <div className="grid grid-cols-12 gap-8">
          {/* Sidebar */}
          <div className="col-span-3">
            <Card className="sticky top-4">
              <CardHeader className="pb-2">
                <CardTitle className="text-sm">Contents</CardTitle>
              </CardHeader>
              <CardContent className="p-0">
                <ScrollArea className="h-[calc(100vh-200px)]">
                  <div className="p-2 space-y-1">
                    {filteredDocs.map((section) => (
                      <button
                        key={section.id}
                        onClick={() => setActiveSection(section.id)}
                        className={cn(
                          "w-full text-left px-3 py-2 rounded-lg transition-colors flex items-center gap-2",
                          activeSection === section.id
                            ? "bg-primary/10 text-primary"
                            : "hover:bg-muted"
                        )}
                      >
                        {section.icon}
                        <span className="text-sm">{section.title}</span>
                      </button>
                    ))}
                  </div>
                </ScrollArea>
              </CardContent>
            </Card>
          </div>

          {/* Main Content */}
          <div className="col-span-9">
            {currentSection && (
              <div className="space-y-8">
                <div>
                  <div className="flex items-center gap-3 mb-2">
                    {currentSection.icon}
                    <h2 className="text-2xl font-bold">{currentSection.title}</h2>
                  </div>
                  <p className="text-muted-foreground">{currentSection.description}</p>
                </div>

                <Separator />

                <div className="space-y-8">
                  {currentSection.content.map((item, i) => (
                    <DocContentRenderer key={i} content={item} />
                  ))}
                </div>

                {/* Navigation */}
                <Separator />
                <div className="flex items-center justify-between">
                  {DOCUMENTATION.findIndex((s) => s.id === activeSection) > 0 && (
                    <Button
                      variant="ghost"
                      onClick={() => {
                        const idx = DOCUMENTATION.findIndex((s) => s.id === activeSection);
                        setActiveSection(DOCUMENTATION[idx - 1].id);
                      }}
                    >
                      <ChevronRight className="h-4 w-4 rotate-180 mr-2" />
                      Previous
                    </Button>
                  )}
                  <div />
                  {DOCUMENTATION.findIndex((s) => s.id === activeSection) <
                    DOCUMENTATION.length - 1 && (
                    <Button
                      onClick={() => {
                        const idx = DOCUMENTATION.findIndex((s) => s.id === activeSection);
                        setActiveSection(DOCUMENTATION[idx + 1].id);
                      }}
                    >
                      Next
                      <ChevronRight className="h-4 w-4 ml-2" />
                    </Button>
                  )}
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

export default DocumentationPage;
