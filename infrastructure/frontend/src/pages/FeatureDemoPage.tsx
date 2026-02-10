/**
 * Feature Demo Page
 * Interactive demonstration of all new V2 features
 */
import { useState, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { useOnboardingTour } from "@/components/onboarding/OnboardingTour";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Separator } from "@/components/ui/separator";
import { Progress } from "@/components/ui/progress";
import {
  Sparkles,
  Brain,
  Wand2,
  MessageSquare,
  LayoutGrid,
  Keyboard,
  FileUp,
  BarChart3,
  AlertTriangle,
  GitBranch,
  Database,
  Calculator,
  Play,
  Pause,
  RotateCcw,
  ChevronRight,
  Check,
  ExternalLink,
  Video,
  BookOpen,
  Code,
  Zap,
  Target,
  Layers,
  Eye,
  MousePointer,
  ArrowRight,
} from "lucide-react";

// Import demo components
import { AIMappingSuggester, MappingSuggestion } from "@/components/hierarchy-knowledge-base/components/AIMappingSuggester";
import { FormulaAutoSuggest, FormulaSuggestion } from "@/components/hierarchy-knowledge-base/components/FormulaAutoSuggest";
import { AnomalyAlertPanel, Anomaly, AnomalySummary } from "@/components/hierarchy-knowledge-base/components/AnomalyAlertPanel";
import { ProjectHealthDashboard } from "@/components/hierarchy-knowledge-base/components/ProjectHealthDashboard";
import { MappingCoverageHeatmap, HeatmapNode } from "@/components/hierarchy-knowledge-base/components/MappingCoverageHeatmap";
import { AIChatPanel, ChatMessage, ChatContext } from "@/components/ai-assistant/AIChatPanel";

interface FeatureSection {
  id: string;
  title: string;
  description: string;
  icon: React.ReactNode;
  category: "ai" | "productivity" | "visualization" | "import";
  isNew: boolean;
  demoComponent?: React.ReactNode;
  highlights: string[];
  shortcuts?: Array<{ key: string; action: string }>;
}

const FEATURE_SECTIONS: FeatureSection[] = [
  {
    id: "ai-mapping",
    title: "AI-Powered Mapping Suggestions",
    description:
      "Our AI analyzes hierarchy names and existing patterns to suggest the best source column mappings with confidence scores.",
    icon: <Brain className="h-6 w-6 text-purple-600" />,
    category: "ai",
    isNew: true,
    highlights: [
      "Automatic pattern recognition from hierarchy names",
      "Learning from your accepted/rejected suggestions",
      "Confidence scores help you make decisions",
      "Supports Claude AI and OpenAI backends",
    ],
  },
  {
    id: "formula-suggest",
    title: "Formula Auto-Suggest",
    description:
      "AI detects common financial patterns and suggests appropriate formulas for calculated fields.",
    icon: <Calculator className="h-6 w-6 text-blue-600" />,
    category: "ai",
    isNew: true,
    highlights: [
      "Recognizes patterns like Gross Profit, EBITDA, Net Income",
      "Industry-specific formula suggestions",
      "Custom formula builder with syntax validation",
      "Preview formulas before applying",
    ],
  },
  {
    id: "ai-chat",
    title: "AI Chat Assistant",
    description:
      "Chat with AI to get help, ask questions, and perform actions using natural language.",
    icon: <MessageSquare className="h-6 w-6 text-green-600" />,
    category: "ai",
    isNew: true,
    highlights: [
      "Natural language queries and commands",
      "Context-aware responses based on current project",
      "Execute actions like 'find unmapped nodes'",
      "Function calling for direct manipulation",
    ],
  },
  {
    id: "nl-builder",
    title: "Natural Language Hierarchy Builder",
    description:
      "Describe what you need in plain English and let AI generate the complete hierarchy structure.",
    icon: <Wand2 className="h-6 w-6 text-orange-600" />,
    category: "ai",
    isNew: true,
    highlights: [
      "Generate hierarchies from descriptions",
      "Industry-specific structure generation",
      "Iterative refinement through conversation",
      "Includes formula suggestions",
    ],
  },
  {
    id: "anomaly-detection",
    title: "Anomaly Detection",
    description:
      "Automatic detection of mapping issues, missing formulas, type mismatches, and inconsistent patterns.",
    icon: <AlertTriangle className="h-6 w-6 text-yellow-600" />,
    category: "ai",
    isNew: true,
    highlights: [
      "Real-time validation as you work",
      "Severity levels: error, warning, info",
      "One-click fixes for common issues",
      "Exportable validation reports",
    ],
  },
  {
    id: "keyboard-shortcuts",
    title: "Keyboard Shortcuts",
    description:
      "Work faster with comprehensive keyboard shortcuts for navigation, editing, and actions.",
    icon: <Keyboard className="h-6 w-6 text-gray-600" />,
    category: "productivity",
    isNew: false,
    highlights: [
      "30+ keyboard shortcuts available",
      "Customizable key bindings",
      "Press '?' for quick reference",
      "Vim-style navigation option",
    ],
    shortcuts: [
      { key: "↑↓", action: "Navigate nodes" },
      { key: "Enter", action: "Edit node" },
      { key: "Ctrl+N", action: "New child" },
      { key: "Ctrl+S", action: "Save" },
      { key: "?", action: "Show shortcuts" },
    ],
  },
  {
    id: "template-gallery",
    title: "Template Gallery",
    description:
      "Browse and use pre-built hierarchy templates for P&L, Balance Sheet, and industry-specific reports.",
    icon: <LayoutGrid className="h-6 w-6 text-blue-600" />,
    category: "productivity",
    isNew: true,
    highlights: [
      "20+ built-in templates",
      "Industry-specific options (Oil & Gas, SaaS, etc.)",
      "Save your projects as custom templates",
      "Template recommendations based on context",
    ],
  },
  {
    id: "smart-import",
    title: "Smart CSV Import",
    description:
      "Import hierarchies with intelligent format detection, validation, and side-by-side diff preview.",
    icon: <FileUp className="h-6 w-6 text-orange-600" />,
    category: "import",
    isNew: true,
    highlights: [
      "Auto-detect CSV format (legacy vs new)",
      "Column mapping suggestions",
      "Validation before import",
      "Diff view for comparing changes",
    ],
  },
  {
    id: "health-dashboard",
    title: "Project Health Dashboard",
    description:
      "Monitor project health with mapping coverage, validation status, and activity tracking.",
    icon: <BarChart3 className="h-6 w-6 text-green-600" />,
    category: "visualization",
    isNew: true,
    highlights: [
      "Health score calculation",
      "Mapping coverage metrics",
      "Recent activity timeline",
      "Deployment history tracking",
    ],
  },
  {
    id: "coverage-heatmap",
    title: "Mapping Coverage Heatmap",
    description:
      "Visualize mapping completeness across your hierarchy with color-coded status indicators.",
    icon: <Target className="h-6 w-6 text-red-600" />,
    category: "visualization",
    isNew: true,
    highlights: [
      "Color-coded coverage status",
      "Filter by mapped/unmapped",
      "Recursive coverage calculation",
      "Click to navigate to nodes",
    ],
  },
  {
    id: "canvas-builder",
    title: "Visual Canvas Builder",
    description:
      "Drag-and-drop node-based editor for visual hierarchy building with connections.",
    icon: <Layers className="h-6 w-6 text-purple-600" />,
    category: "visualization",
    isNew: true,
    highlights: [
      "Node-based visual editor",
      "Drag-drop positioning",
      "Zoom and pan controls",
      "Connection lines between nodes",
    ],
  },
];

// Sample data for demos
const SAMPLE_MAPPING_SUGGESTIONS: MappingSuggestion[] = [
  {
    hierarchyId: "h1",
    suggestedMapping: {
      sourceDatabase: "FINANCE_DB",
      sourceSchema: "REPORTING",
      sourceTable: "GL_ACCOUNTS",
      sourceColumn: "REVENUE_AMOUNT",
    },
    confidence: 0.92,
    reasoning: "Column name 'REVENUE_AMOUNT' closely matches hierarchy 'Revenue'",
    source: "ai",
  },
  {
    hierarchyId: "h1",
    suggestedMapping: {
      sourceDatabase: "FINANCE_DB",
      sourceSchema: "SALES",
      sourceTable: "SALES_DATA",
      sourceColumn: "TOTAL_SALES",
    },
    confidence: 0.78,
    reasoning: "Based on similar mapping patterns in project",
    source: "pattern",
  },
];

const SAMPLE_FORMULA_SUGGESTIONS: FormulaSuggestion[] = [
  {
    formulaType: "SUBTRACT",
    formulaText: "{Revenue} - {Cost of Goods Sold}",
    variables: ["Revenue", "Cost of Goods Sold"],
    confidence: 0.95,
    reasoning: "Gross Profit = Revenue - COGS (standard financial formula)",
    category: "Income Statement",
  },
  {
    formulaType: "SUM",
    formulaText: "SUM({Product Revenue}, {Service Revenue})",
    variables: ["Product Revenue", "Service Revenue"],
    confidence: 0.85,
    reasoning: "Sum of child hierarchies",
    category: "Aggregation",
  },
];

const SAMPLE_ANOMALIES: Anomaly[] = [
  {
    id: "a1",
    type: "missing_mapping",
    severity: "warning",
    hierarchyId: "h5",
    hierarchyName: "Other Revenue",
    message: "Leaf node without mapping",
    details: "This is a leaf node but has no source mapping configured",
    suggestion: "Add a source mapping or convert to a calculated field",
    autoFixable: false,
  },
  {
    id: "a2",
    type: "type_mismatch",
    severity: "warning",
    hierarchyId: "h3",
    hierarchyName: "Revenue Amount",
    message: "Potential type mismatch",
    details: "Mapped to VARCHAR column but appears to be a value field",
    suggestion: "Verify source column type or add conversion",
    autoFixable: false,
  },
];

const SAMPLE_ANOMALY_SUMMARY: AnomalySummary = {
  total: 5,
  byType: {
    missing_mapping: 2,
    type_mismatch: 1,
    inconsistent_pattern: 1,
    missing_formula: 1,
    orphan_node: 0,
    circular_reference: 0,
    duplicate_mapping: 0,
    formula_error: 0,
    naming_convention: 0,
    level_inconsistency: 0,
  },
  bySeverity: { error: 0, warning: 4, info: 1 },
  autoFixableCount: 2,
};

function FeatureCard({ feature }: { feature: FeatureSection }) {
  const categoryColors = {
    ai: "border-purple-200 bg-purple-50",
    productivity: "border-blue-200 bg-blue-50",
    visualization: "border-green-200 bg-green-50",
    import: "border-orange-200 bg-orange-50",
  };

  const categoryLabels = {
    ai: "AI Features",
    productivity: "Productivity",
    visualization: "Visualization",
    import: "Import/Export",
  };

  return (
    <Card className={cn("hover:shadow-md transition-shadow", categoryColors[feature.category])}>
      <CardHeader className="pb-3">
        <div className="flex items-start justify-between">
          <div className="p-2 rounded-lg bg-background">{feature.icon}</div>
          <div className="flex items-center gap-2">
            {feature.isNew && (
              <Badge variant="default" className="text-xs">
                New
              </Badge>
            )}
            <Badge variant="secondary" className="text-xs">
              {categoryLabels[feature.category]}
            </Badge>
          </div>
        </div>
        <CardTitle className="text-lg mt-3">{feature.title}</CardTitle>
        <CardDescription className="text-sm">{feature.description}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-4">
        <ul className="space-y-2">
          {feature.highlights.map((highlight, i) => (
            <li key={i} className="flex items-start gap-2 text-sm">
              <Check className="h-4 w-4 text-green-600 shrink-0 mt-0.5" />
              <span>{highlight}</span>
            </li>
          ))}
        </ul>

        {feature.shortcuts && (
          <div className="pt-2">
            <h4 className="text-xs font-medium text-muted-foreground mb-2">Shortcuts</h4>
            <div className="grid grid-cols-2 gap-2">
              {feature.shortcuts.map((shortcut, i) => (
                <div key={i} className="flex items-center justify-between text-xs">
                  <kbd className="px-1.5 py-0.5 bg-background rounded border text-xs">
                    {shortcut.key}
                  </kbd>
                  <span className="text-muted-foreground">{shortcut.action}</span>
                </div>
              ))}
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

function InteractiveDemo({ featureId }: { featureId: string }) {
  const [chatMessages, setChatMessages] = useState<ChatMessage[]>([
    {
      id: "1",
      role: "assistant",
      content: "Hi! I'm your AI assistant. Try asking me something like 'find unmapped nodes' or 'show project stats'.",
      timestamp: new Date(),
    },
  ]);

  const handleSendMessage = (message: string) => {
    const userMsg: ChatMessage = {
      id: `user-${Date.now()}`,
      role: "user",
      content: message,
      timestamp: new Date(),
    };
    setChatMessages((prev) => [...prev, userMsg]);

    // Simulate AI response
    setTimeout(() => {
      const response: ChatMessage = {
        id: `ai-${Date.now()}`,
        role: "assistant",
        content: `I understood your request: "${message}". In the full application, I would execute this action for you.`,
        timestamp: new Date(),
        functionCall: message.toLowerCase().includes("unmapped")
          ? {
              name: "findUnmappedNodes",
              arguments: {},
              requiresConfirmation: false,
              status: "executed",
            }
          : undefined,
      };
      setChatMessages((prev) => [...prev, response]);
    }, 1000);
  };

  switch (featureId) {
    case "ai-mapping":
      return (
        <AIMappingSuggester
          hierarchyId="demo-1"
          hierarchyName="Revenue"
          suggestions={SAMPLE_MAPPING_SUGGESTIONS}
          onAccept={(s) => console.log("Accepted:", s)}
          onReject={(s) => console.log("Rejected:", s)}
          onRefresh={() => console.log("Refresh")}
        />
      );

    case "formula-suggest":
      return (
        <FormulaAutoSuggest
          hierarchyName="Gross Profit"
          suggestions={SAMPLE_FORMULA_SUGGESTIONS}
          availableVariables={["Revenue", "Cost of Goods Sold", "Operating Expenses", "Tax"]}
          onAccept={(formula, type) => console.log("Applied:", formula, type)}
        />
      );

    case "ai-chat":
      return (
        <AIChatPanel
          sessionId="demo"
          context={{
            projectName: "Demo Project",
            projectStats: { totalHierarchies: 45, totalMappings: 32, unmappedCount: 13 },
          }}
          messages={chatMessages}
          onSendMessage={handleSendMessage}
          onConfirmAction={() => {}}
          onClearHistory={() => setChatMessages([])}
          className="h-[400px]"
        />
      );

    case "anomaly-detection":
      return (
        <AnomalyAlertPanel
          anomalies={SAMPLE_ANOMALIES}
          summary={SAMPLE_ANOMALY_SUMMARY}
          onRefresh={() => {}}
          onNavigateToHierarchy={(id) => console.log("Navigate to:", id)}
          onAutoFix={(a) => console.log("Fix:", a)}
        />
      );

    case "health-dashboard":
      return (
        <ProjectHealthDashboard
          projectName="Demo P&L Project"
          stats={{
            totalHierarchies: 45,
            totalMappings: 32,
            totalFormulas: 12,
            mappedHierarchies: 32,
            unmappedLeafNodes: 8,
            validationErrors: 2,
            lastDeployment: {
              date: new Date(),
              status: "success",
              hierarchyCount: 45,
            },
          }}
          recentActivities={[
            {
              id: "1",
              type: "create",
              description: "Created hierarchy 'Operating Expenses'",
              timestamp: new Date(Date.now() - 3600000),
              user: "John",
            },
            {
              id: "2",
              type: "update",
              description: "Updated mapping for 'Revenue'",
              timestamp: new Date(Date.now() - 7200000),
              user: "Jane",
            },
          ]}
          validationIssues={[
            { id: "1", severity: "warning", message: "3 leaf nodes without mappings", hierarchyName: "Other Income" },
          ]}
          onRefresh={() => {}}
        />
      );

    default:
      return (
        <div className="text-center py-12 text-muted-foreground">
          <Eye className="h-12 w-12 mx-auto mb-4 opacity-50" />
          <p>Interactive demo not available for this feature.</p>
          <p className="text-sm mt-2">See the feature in action in the main application.</p>
        </div>
      );
  }
}

export function FeatureDemoPage() {
  const [selectedFeature, setSelectedFeature] = useState<string>("ai-mapping");
  const [activeCategory, setActiveCategory] = useState<string>("all");
  const { startTour, isActive } = useOnboardingTour();
  const navigate = useNavigate();

  const categories = [
    { id: "all", label: "All Features", count: FEATURE_SECTIONS.length },
    { id: "ai", label: "AI Features", count: FEATURE_SECTIONS.filter((f) => f.category === "ai").length },
    { id: "productivity", label: "Productivity", count: FEATURE_SECTIONS.filter((f) => f.category === "productivity").length },
    { id: "visualization", label: "Visualization", count: FEATURE_SECTIONS.filter((f) => f.category === "visualization").length },
    { id: "import", label: "Import/Export", count: FEATURE_SECTIONS.filter((f) => f.category === "import").length },
  ];

  const filteredFeatures =
    activeCategory === "all"
      ? FEATURE_SECTIONS
      : FEATURE_SECTIONS.filter((f) => f.category === activeCategory);

  const selectedFeatureData = FEATURE_SECTIONS.find((f) => f.id === selectedFeature);

  return (
    <div className="min-h-screen bg-background">
      {/* Hero Section */}
      <div className="bg-gradient-to-br from-primary/10 via-purple-500/5 to-background border-b">
        <div className="container mx-auto px-6 py-16">
          <div className="text-center max-w-3xl mx-auto">
            <Badge variant="default" className="mb-4">
              <Sparkles className="h-3 w-3 mr-1" />
              DataBridge AI V2
            </Badge>
            <h1 className="text-4xl font-bold mb-4">Feature Showcase</h1>
            <p className="text-lg text-muted-foreground mb-8">
              Explore the new AI-powered features and productivity enhancements that make building
              financial hierarchies faster and smarter than ever.
            </p>
            <div className="flex items-center justify-center gap-4">
              <Button size="lg" className="gap-2" onClick={() => startTour()} disabled={isActive}>
                <Play className="h-5 w-5" />
                Start Interactive Tour
              </Button>
              <Button size="lg" variant="outline" className="gap-2" onClick={() => navigate("/docs")}>
                <BookOpen className="h-5 w-5" />
                View Documentation
              </Button>
            </div>
          </div>

          {/* Stats */}
          <div className="grid grid-cols-4 gap-6 mt-12 max-w-2xl mx-auto">
            {[
              { label: "AI Features", value: "6+", icon: <Brain className="h-5 w-5" /> },
              { label: "Templates", value: "20+", icon: <LayoutGrid className="h-5 w-5" /> },
              { label: "Shortcuts", value: "30+", icon: <Keyboard className="h-5 w-5" /> },
              { label: "Industries", value: "8", icon: <Target className="h-5 w-5" /> },
            ].map((stat) => (
              <div key={stat.label} className="text-center">
                <div className="inline-flex items-center justify-center w-10 h-10 rounded-full bg-primary/10 text-primary mb-2">
                  {stat.icon}
                </div>
                <div className="text-2xl font-bold">{stat.value}</div>
                <div className="text-sm text-muted-foreground">{stat.label}</div>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Main Content */}
      <div className="container mx-auto px-6 py-12">
        <Tabs defaultValue="overview" className="space-y-8">
          <TabsList className="grid w-full max-w-md mx-auto grid-cols-2">
            <TabsTrigger value="overview">Feature Overview</TabsTrigger>
            <TabsTrigger value="interactive">Interactive Demo</TabsTrigger>
          </TabsList>

          {/* Overview Tab */}
          <TabsContent value="overview" className="space-y-8">
            {/* Category Filter */}
            <div className="flex items-center justify-center gap-2 flex-wrap">
              {categories.map((cat) => (
                <Button
                  key={cat.id}
                  variant={activeCategory === cat.id ? "default" : "outline"}
                  size="sm"
                  onClick={() => setActiveCategory(cat.id)}
                  className="gap-1"
                >
                  {cat.label}
                  <Badge variant="secondary" className="ml-1">
                    {cat.count}
                  </Badge>
                </Button>
              ))}
            </div>

            {/* Feature Grid */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
              {filteredFeatures.map((feature) => (
                <FeatureCard key={feature.id} feature={feature} />
              ))}
            </div>
          </TabsContent>

          {/* Interactive Demo Tab */}
          <TabsContent value="interactive" className="space-y-8">
            <div className="grid grid-cols-12 gap-8">
              {/* Feature List */}
              <div className="col-span-4">
                <Card>
                  <CardHeader className="pb-3">
                    <CardTitle className="text-base">Select Feature</CardTitle>
                    <CardDescription>Click to see interactive demo</CardDescription>
                  </CardHeader>
                  <CardContent className="p-0">
                    <ScrollArea className="h-[500px]">
                      <div className="p-2">
                        {FEATURE_SECTIONS.filter((f) =>
                          ["ai-mapping", "formula-suggest", "ai-chat", "anomaly-detection", "health-dashboard"].includes(
                            f.id
                          )
                        ).map((feature) => (
                          <button
                            key={feature.id}
                            onClick={() => setSelectedFeature(feature.id)}
                            className={cn(
                              "w-full text-left p-3 rounded-lg transition-colors flex items-start gap-3",
                              selectedFeature === feature.id
                                ? "bg-primary/10 border border-primary/20"
                                : "hover:bg-muted"
                            )}
                          >
                            <div className="p-1.5 rounded bg-background">{feature.icon}</div>
                            <div className="flex-1 min-w-0">
                              <div className="flex items-center gap-2">
                                <span className="font-medium text-sm">{feature.title}</span>
                                {feature.isNew && (
                                  <Badge variant="default" className="text-xs">
                                    New
                                  </Badge>
                                )}
                              </div>
                              <p className="text-xs text-muted-foreground line-clamp-2 mt-1">
                                {feature.description}
                              </p>
                            </div>
                            <ChevronRight
                              className={cn(
                                "h-4 w-4 shrink-0 transition-transform",
                                selectedFeature === feature.id && "rotate-90"
                              )}
                            />
                          </button>
                        ))}
                      </div>
                    </ScrollArea>
                  </CardContent>
                </Card>
              </div>

              {/* Demo Area */}
              <div className="col-span-8">
                <Card className="h-full">
                  <CardHeader className="pb-3">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center gap-3">
                        {selectedFeatureData?.icon}
                        <div>
                          <CardTitle className="text-base">{selectedFeatureData?.title}</CardTitle>
                          <CardDescription>{selectedFeatureData?.description}</CardDescription>
                        </div>
                      </div>
                      <Badge variant="outline">Interactive Demo</Badge>
                    </div>
                  </CardHeader>
                  <Separator />
                  <CardContent className="p-4">
                    <InteractiveDemo featureId={selectedFeature} />
                  </CardContent>
                </Card>
              </div>
            </div>
          </TabsContent>
        </Tabs>
      </div>

      {/* CTA Section */}
      <div className="bg-muted/50 border-t">
        <div className="container mx-auto px-6 py-16 text-center">
          <h2 className="text-2xl font-bold mb-4">Ready to Get Started?</h2>
          <p className="text-muted-foreground mb-8 max-w-xl mx-auto">
            Start building smarter financial hierarchies with AI-powered suggestions, templates, and
            more.
          </p>
          <div className="flex items-center justify-center gap-4">
            <Button size="lg" className="gap-2" onClick={() => navigate("/hierarchy-knowledge-base")}>
              <Zap className="h-5 w-5" />
              Create New Project
            </Button>
            <Button size="lg" variant="outline" className="gap-2" onClick={() => navigate("/hierarchy-knowledge-base")}>
              <LayoutGrid className="h-5 w-5" />
              Browse Templates
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}

export default FeatureDemoPage;
