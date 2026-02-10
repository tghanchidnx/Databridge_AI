/**
 * Template Gallery Dialog
 * Browse and use pre-built hierarchy templates
 */
import { useState, useMemo, useCallback } from "react";
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
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
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
  LayoutGrid,
  List,
  Search,
  FileSpreadsheet,
  BarChart3,
  Wallet,
  Building2,
  Fuel,
  Factory,
  Truck,
  Cloud,
  Globe,
  Users,
  Package,
  Scale,
  ChevronDown,
  ChevronRight,
  Star,
  Download,
  Eye,
  Check,
  TreePine,
  TrendingUp,
  DollarSign,
} from "lucide-react";

export interface HierarchyTemplate {
  id: string;
  name: string;
  description: string;
  category: "accounting" | "finance" | "operations";
  industry: string;
  structure: any; // Hierarchy structure
  nodeCount: number;
  maxDepth: number;
  tags: string[];
  popularity?: number;
  isRecommended?: boolean;
}

interface TemplateGalleryDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  templates: HierarchyTemplate[];
  onSelectTemplate: (template: HierarchyTemplate) => void;
  onSaveAsTemplate?: (name: string, category: string) => void;
  currentProjectName?: string;
}

const CATEGORY_INFO = {
  accounting: {
    label: "Accounting",
    icon: FileSpreadsheet,
    color: "text-blue-600",
    bgColor: "bg-blue-100",
  },
  finance: {
    label: "Finance",
    icon: DollarSign,
    color: "text-green-600",
    bgColor: "bg-green-100",
  },
  operations: {
    label: "Operations",
    icon: Package,
    color: "text-purple-600",
    bgColor: "bg-purple-100",
  },
};

const INDUSTRY_ICONS: Record<string, React.ElementType> = {
  general: Globe,
  "oil_gas": Fuel,
  "oil_gas_upstream": Fuel,
  "oil_gas_midstream": Fuel,
  "oil_gas_services": Fuel,
  manufacturing: Factory,
  "industrial_services": Factory,
  saas: Cloud,
  technology: Cloud,
  transportation: Truck,
  logistics: Truck,
};

function getIndustryIcon(industry: string): React.ElementType {
  return INDUSTRY_ICONS[industry.toLowerCase()] || Building2;
}

function TemplatePreview({ template }: { template: HierarchyTemplate }) {
  const [expanded, setExpanded] = useState(true);

  const renderNode = (node: any, depth = 0): React.ReactNode => {
    const hasChildren = node.children && node.children.length > 0;

    return (
      <div key={node.id || node.name}>
        <div
          className="flex items-center gap-2 py-1 hover:bg-muted/50 rounded"
          style={{ paddingLeft: `${depth * 16 + 8}px` }}
        >
          {hasChildren ? (
            <ChevronDown className="h-3 w-3 text-muted-foreground" />
          ) : (
            <span className="w-3" />
          )}
          <TreePine className="h-3 w-3 text-primary" />
          <span className="text-xs">{node.name}</span>
          {node.suggestedFormula && (
            <Badge variant="outline" className="text-xs ml-auto">
              {node.suggestedFormula.type}
            </Badge>
          )}
        </div>
        {hasChildren && depth < 2 && (
          <div>{node.children.map((child: any) => renderNode(child, depth + 1))}</div>
        )}
      </div>
    );
  };

  return (
    <Card>
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <CardTitle className="text-sm">Structure Preview</CardTitle>
          <Button
            variant="ghost"
            size="sm"
            onClick={() => setExpanded(!expanded)}
            className="h-6 px-2"
          >
            {expanded ? (
              <ChevronDown className="h-4 w-4" />
            ) : (
              <ChevronRight className="h-4 w-4" />
            )}
          </Button>
        </div>
      </CardHeader>
      {expanded && (
        <CardContent className="pt-0">
          <ScrollArea className="h-[200px] border rounded p-2">
            {template.structure && renderNode(template.structure)}
          </ScrollArea>
        </CardContent>
      )}
    </Card>
  );
}

function TemplateCard({
  template,
  isSelected,
  onSelect,
  onPreview,
  view,
}: {
  template: HierarchyTemplate;
  isSelected: boolean;
  onSelect: () => void;
  onPreview: () => void;
  view: "grid" | "list";
}) {
  const categoryInfo = CATEGORY_INFO[template.category];
  const IndustryIcon = getIndustryIcon(template.industry);

  if (view === "list") {
    return (
      <div
        className={cn(
          "flex items-center gap-4 p-3 rounded-lg border cursor-pointer transition-all hover:border-primary/50",
          isSelected && "border-primary ring-2 ring-primary/20"
        )}
        onClick={onSelect}
      >
        <div className={cn("p-2 rounded-lg", categoryInfo.bgColor)}>
          <categoryInfo.icon className={cn("h-5 w-5", categoryInfo.color)} />
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <span className="font-medium text-sm">{template.name}</span>
            {template.isRecommended && (
              <Badge variant="default" className="text-xs gap-1">
                <Star className="h-3 w-3" />
                Recommended
              </Badge>
            )}
          </div>
          <p className="text-xs text-muted-foreground truncate">{template.description}</p>
        </div>
        <div className="flex items-center gap-2 text-xs text-muted-foreground shrink-0">
          <IndustryIcon className="h-4 w-4" />
          <span>{template.industry}</span>
        </div>
        <div className="flex items-center gap-3 text-xs text-muted-foreground shrink-0">
          <span>{template.nodeCount} nodes</span>
          <span>{template.maxDepth} levels</span>
        </div>
        <Button size="sm" variant="ghost" onClick={(e) => { e.stopPropagation(); onPreview(); }}>
          <Eye className="h-4 w-4" />
        </Button>
      </div>
    );
  }

  return (
    <Card
      className={cn(
        "cursor-pointer transition-all hover:border-primary/50 hover:shadow-md",
        isSelected && "border-primary ring-2 ring-primary/20"
      )}
      onClick={onSelect}
    >
      <CardHeader className="pb-2">
        <div className="flex items-start justify-between">
          <div className={cn("p-2 rounded-lg", categoryInfo.bgColor)}>
            <categoryInfo.icon className={cn("h-5 w-5", categoryInfo.color)} />
          </div>
          {template.isRecommended && (
            <Badge variant="default" className="text-xs gap-1">
              <Star className="h-3 w-3" />
              Recommended
            </Badge>
          )}
        </div>
        <CardTitle className="text-base mt-2">{template.name}</CardTitle>
        <CardDescription className="text-xs line-clamp-2">
          {template.description}
        </CardDescription>
      </CardHeader>
      <CardContent className="pt-0 space-y-3">
        <div className="flex items-center gap-2">
          <IndustryIcon className="h-4 w-4 text-muted-foreground" />
          <span className="text-xs text-muted-foreground">{template.industry}</span>
        </div>
        <div className="flex items-center gap-3 text-xs text-muted-foreground">
          <div className="flex items-center gap-1">
            <TreePine className="h-3 w-3" />
            {template.nodeCount} nodes
          </div>
          <div className="flex items-center gap-1">
            <TrendingUp className="h-3 w-3" />
            {template.maxDepth} levels
          </div>
        </div>
        <div className="flex flex-wrap gap-1">
          {template.tags.slice(0, 3).map((tag) => (
            <Badge key={tag} variant="outline" className="text-xs">
              {tag}
            </Badge>
          ))}
          {template.tags.length > 3 && (
            <Badge variant="outline" className="text-xs">
              +{template.tags.length - 3}
            </Badge>
          )}
        </div>
        <Button
          variant="outline"
          size="sm"
          className="w-full"
          onClick={(e) => {
            e.stopPropagation();
            onPreview();
          }}
        >
          <Eye className="h-4 w-4 mr-2" />
          Preview
        </Button>
      </CardContent>
    </Card>
  );
}

export function TemplateGalleryDialog({
  open,
  onOpenChange,
  templates,
  onSelectTemplate,
  onSaveAsTemplate,
  currentProjectName,
}: TemplateGalleryDialogProps) {
  const [searchQuery, setSearchQuery] = useState("");
  const [categoryFilter, setCategoryFilter] = useState<string>("all");
  const [industryFilter, setIndustryFilter] = useState<string>("all");
  const [view, setView] = useState<"grid" | "list">("grid");
  const [selectedTemplate, setSelectedTemplate] = useState<HierarchyTemplate | null>(null);
  const [showPreview, setShowPreview] = useState(false);
  const [activeTab, setActiveTab] = useState<"browse" | "save">("browse");
  const [newTemplateName, setNewTemplateName] = useState("");
  const [newTemplateCategory, setNewTemplateCategory] = useState<string>("accounting");

  const industries = useMemo(() => {
    const set = new Set(templates.map((t) => t.industry));
    return Array.from(set);
  }, [templates]);

  const filteredTemplates = useMemo(() => {
    return templates.filter((t) => {
      if (searchQuery) {
        const query = searchQuery.toLowerCase();
        const matches =
          t.name.toLowerCase().includes(query) ||
          t.description.toLowerCase().includes(query) ||
          t.tags.some((tag) => tag.toLowerCase().includes(query));
        if (!matches) return false;
      }
      if (categoryFilter !== "all" && t.category !== categoryFilter) return false;
      if (industryFilter !== "all" && t.industry !== industryFilter) return false;
      return true;
    });
  }, [templates, searchQuery, categoryFilter, industryFilter]);

  const recommendedTemplates = useMemo(() => {
    return filteredTemplates.filter((t) => t.isRecommended);
  }, [filteredTemplates]);

  const handleUseTemplate = useCallback(() => {
    if (selectedTemplate) {
      onSelectTemplate(selectedTemplate);
      onOpenChange(false);
    }
  }, [selectedTemplate, onSelectTemplate, onOpenChange]);

  const handleSaveAsTemplate = useCallback(() => {
    if (newTemplateName && onSaveAsTemplate) {
      onSaveAsTemplate(newTemplateName, newTemplateCategory);
      onOpenChange(false);
    }
  }, [newTemplateName, newTemplateCategory, onSaveAsTemplate, onOpenChange]);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-4xl h-[80vh] flex flex-col p-0">
        <DialogHeader className="px-6 pt-6 pb-4">
          <DialogTitle className="flex items-center gap-2">
            <LayoutGrid className="h-5 w-5 text-primary" />
            Template Gallery
          </DialogTitle>
          <DialogDescription>
            Browse pre-built templates or save your current project as a template
          </DialogDescription>
        </DialogHeader>

        <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as any)} className="flex-1 flex flex-col">
          <div className="px-6">
            <TabsList className="grid w-full grid-cols-2">
              <TabsTrigger value="browse" className="gap-2">
                <Download className="h-4 w-4" />
                Browse Templates
              </TabsTrigger>
              <TabsTrigger value="save" className="gap-2" disabled={!onSaveAsTemplate}>
                <Star className="h-4 w-4" />
                Save as Template
              </TabsTrigger>
            </TabsList>
          </div>

          <TabsContent value="browse" className="flex-1 flex flex-col m-0 overflow-hidden">
            {/* Filters */}
            <div className="px-6 py-4 space-y-4 border-b">
              <div className="flex items-center gap-4">
                <div className="relative flex-1">
                  <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-muted-foreground" />
                  <Input
                    placeholder="Search templates..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="pl-9"
                  />
                </div>
                <Select value={categoryFilter} onValueChange={setCategoryFilter}>
                  <SelectTrigger className="w-[150px]">
                    <SelectValue placeholder="Category" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All Categories</SelectItem>
                    <SelectItem value="accounting">Accounting</SelectItem>
                    <SelectItem value="finance">Finance</SelectItem>
                    <SelectItem value="operations">Operations</SelectItem>
                  </SelectContent>
                </Select>
                <Select value={industryFilter} onValueChange={setIndustryFilter}>
                  <SelectTrigger className="w-[180px]">
                    <SelectValue placeholder="Industry" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="all">All Industries</SelectItem>
                    {industries.map((industry) => (
                      <SelectItem key={industry} value={industry}>
                        {industry}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                <div className="flex items-center border rounded-lg">
                  <Button
                    variant={view === "grid" ? "secondary" : "ghost"}
                    size="sm"
                    onClick={() => setView("grid")}
                    className="rounded-r-none"
                  >
                    <LayoutGrid className="h-4 w-4" />
                  </Button>
                  <Button
                    variant={view === "list" ? "secondary" : "ghost"}
                    size="sm"
                    onClick={() => setView("list")}
                    className="rounded-l-none"
                  >
                    <List className="h-4 w-4" />
                  </Button>
                </div>
              </div>
            </div>

            {/* Template list */}
            <ScrollArea className="flex-1 px-6">
              <div className="py-4 space-y-6">
                {/* Recommended section */}
                {recommendedTemplates.length > 0 && !searchQuery && (
                  <div className="space-y-3">
                    <h3 className="font-medium flex items-center gap-2">
                      <Star className="h-4 w-4 text-yellow-500" />
                      Recommended for You
                    </h3>
                    <div
                      className={cn(
                        view === "grid"
                          ? "grid grid-cols-2 lg:grid-cols-3 gap-4"
                          : "space-y-2"
                      )}
                    >
                      {recommendedTemplates.slice(0, 3).map((template) => (
                        <TemplateCard
                          key={template.id}
                          template={template}
                          isSelected={selectedTemplate?.id === template.id}
                          onSelect={() => setSelectedTemplate(template)}
                          onPreview={() => {
                            setSelectedTemplate(template);
                            setShowPreview(true);
                          }}
                          view={view}
                        />
                      ))}
                    </div>
                    <Separator className="mt-4" />
                  </div>
                )}

                {/* All templates */}
                <div className="space-y-3">
                  <h3 className="font-medium">
                    {searchQuery
                      ? `Search Results (${filteredTemplates.length})`
                      : "All Templates"}
                  </h3>
                  {filteredTemplates.length === 0 ? (
                    <div className="text-center py-12">
                      <FileSpreadsheet className="h-12 w-12 text-muted-foreground mx-auto mb-3" />
                      <p className="font-medium">No templates found</p>
                      <p className="text-sm text-muted-foreground mt-1">
                        Try adjusting your search or filters
                      </p>
                    </div>
                  ) : (
                    <div
                      className={cn(
                        view === "grid"
                          ? "grid grid-cols-2 lg:grid-cols-3 gap-4"
                          : "space-y-2"
                      )}
                    >
                      {filteredTemplates.map((template) => (
                        <TemplateCard
                          key={template.id}
                          template={template}
                          isSelected={selectedTemplate?.id === template.id}
                          onSelect={() => setSelectedTemplate(template)}
                          onPreview={() => {
                            setSelectedTemplate(template);
                            setShowPreview(true);
                          }}
                          view={view}
                        />
                      ))}
                    </div>
                  )}
                </div>
              </div>
            </ScrollArea>

            {/* Preview panel */}
            {showPreview && selectedTemplate && (
              <div className="border-t p-4">
                <TemplatePreview template={selectedTemplate} />
              </div>
            )}

            {/* Footer */}
            <div className="p-4 border-t flex items-center justify-between">
              <div className="text-sm text-muted-foreground">
                {selectedTemplate
                  ? `Selected: ${selectedTemplate.name}`
                  : "Select a template to use"}
              </div>
              <div className="flex items-center gap-2">
                <Button variant="outline" onClick={() => onOpenChange(false)}>
                  Cancel
                </Button>
                <Button onClick={handleUseTemplate} disabled={!selectedTemplate}>
                  <Check className="h-4 w-4 mr-2" />
                  Use Template
                </Button>
              </div>
            </div>
          </TabsContent>

          <TabsContent value="save" className="flex-1 m-0">
            <div className="p-6 space-y-6">
              <Card>
                <CardHeader>
                  <CardTitle className="text-base">Save Current Project as Template</CardTitle>
                  <CardDescription>
                    Create a reusable template from your current project structure
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="space-y-2">
                    <Label>Template Name</Label>
                    <Input
                      value={newTemplateName}
                      onChange={(e) => setNewTemplateName(e.target.value)}
                      placeholder="e.g., My Custom P&L Template"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label>Category</Label>
                    <Select value={newTemplateCategory} onValueChange={setNewTemplateCategory}>
                      <SelectTrigger>
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="accounting">Accounting</SelectItem>
                        <SelectItem value="finance">Finance</SelectItem>
                        <SelectItem value="operations">Operations</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>
                  {currentProjectName && (
                    <div className="p-3 bg-muted rounded-lg">
                      <div className="text-sm text-muted-foreground">
                        Source Project: <span className="font-medium">{currentProjectName}</span>
                      </div>
                    </div>
                  )}
                </CardContent>
              </Card>

              <div className="flex justify-end gap-2">
                <Button variant="outline" onClick={() => onOpenChange(false)}>
                  Cancel
                </Button>
                <Button onClick={handleSaveAsTemplate} disabled={!newTemplateName}>
                  <Star className="h-4 w-4 mr-2" />
                  Save as Template
                </Button>
              </div>
            </div>
          </TabsContent>
        </Tabs>
      </DialogContent>
    </Dialog>
  );
}

// Sample templates for demonstration
export const SAMPLE_TEMPLATES: HierarchyTemplate[] = [
  {
    id: "standard_pl",
    name: "Standard P&L",
    description: "Standard income statement for most businesses with revenue, COGS, and operating expenses",
    category: "accounting",
    industry: "General",
    structure: {
      name: "Income Statement",
      children: [
        { name: "Revenue", children: [{ name: "Product Revenue" }, { name: "Service Revenue" }] },
        { name: "Cost of Goods Sold", children: [{ name: "Materials" }, { name: "Labor" }] },
        { name: "Gross Profit", suggestedFormula: { type: "SUBTRACT" } },
        { name: "Operating Expenses", children: [{ name: "S&M" }, { name: "R&D" }, { name: "G&A" }] },
        { name: "Operating Income", suggestedFormula: { type: "SUBTRACT" } },
        { name: "Net Income", suggestedFormula: { type: "SUBTRACT" } },
      ],
    },
    nodeCount: 14,
    maxDepth: 3,
    tags: ["income statement", "P&L", "profit and loss"],
    popularity: 95,
    isRecommended: true,
  },
  {
    id: "standard_bs",
    name: "Standard Balance Sheet",
    description: "Assets, liabilities, and equity structure for financial reporting",
    category: "accounting",
    industry: "General",
    structure: {
      name: "Balance Sheet",
      children: [
        { name: "Assets", children: [{ name: "Current Assets" }, { name: "Non-Current Assets" }] },
        { name: "Liabilities", children: [{ name: "Current Liabilities" }, { name: "Long-Term Liabilities" }] },
        { name: "Equity", suggestedFormula: { type: "SUBTRACT" } },
      ],
    },
    nodeCount: 10,
    maxDepth: 3,
    tags: ["balance sheet", "assets", "liabilities", "equity"],
    popularity: 90,
    isRecommended: true,
  },
  {
    id: "oil_gas_los",
    name: "Oil & Gas LOS",
    description: "Lease Operating Statement for upstream operations",
    category: "accounting",
    industry: "Oil & Gas",
    structure: {
      name: "Lease Operating Statement",
      children: [
        { name: "Oil Revenue" },
        { name: "Gas Revenue" },
        { name: "NGL Revenue" },
        { name: "Lease Operating Expenses", children: [{ name: "Labor" }, { name: "Chemicals" }, { name: "Utilities" }] },
        { name: "Net Operating Income", suggestedFormula: { type: "SUBTRACT" } },
      ],
    },
    nodeCount: 12,
    maxDepth: 3,
    tags: ["oil & gas", "LOS", "upstream", "lease operating"],
    popularity: 75,
  },
  {
    id: "saas_pl",
    name: "SaaS Company P&L",
    description: "Income statement with ARR/MRR tracking and unit economics",
    category: "accounting",
    industry: "SaaS",
    structure: {
      name: "SaaS Income Statement",
      children: [
        { name: "Subscription Revenue", children: [{ name: "MRR" }, { name: "Annual Contracts" }] },
        { name: "Professional Services" },
        { name: "Total Revenue", suggestedFormula: { type: "SUM" } },
        { name: "Cost of Revenue", children: [{ name: "Hosting" }, { name: "Support" }] },
        { name: "Gross Profit", suggestedFormula: { type: "SUBTRACT" } },
      ],
    },
    nodeCount: 15,
    maxDepth: 3,
    tags: ["SaaS", "subscription", "ARR", "MRR"],
    popularity: 80,
    isRecommended: true,
  },
  {
    id: "cost_center",
    name: "Cost Center Hierarchy",
    description: "Expense allocation and responsibility structure",
    category: "finance",
    industry: "General",
    structure: {
      name: "Cost Centers",
      children: [
        { name: "Corporate", children: [{ name: "Executive" }, { name: "Finance" }, { name: "Legal" }] },
        { name: "Operations", children: [{ name: "Production" }, { name: "Quality" }, { name: "Logistics" }] },
        { name: "Commercial", children: [{ name: "Sales" }, { name: "Marketing" }, { name: "Customer Success" }] },
      ],
    },
    nodeCount: 13,
    maxDepth: 3,
    tags: ["cost center", "budget", "expense allocation"],
    popularity: 70,
  },
  {
    id: "geographic",
    name: "Geographic Hierarchy",
    description: "Global regions, countries, states, and cities",
    category: "operations",
    industry: "General",
    structure: {
      name: "Global",
      children: [
        { name: "Americas", children: [{ name: "North America" }, { name: "Latin America" }] },
        { name: "EMEA", children: [{ name: "Western Europe" }, { name: "Middle East" }] },
        { name: "APAC", children: [{ name: "Greater China" }, { name: "Southeast Asia" }] },
      ],
    },
    nodeCount: 10,
    maxDepth: 3,
    tags: ["geography", "regions", "international"],
    popularity: 65,
  },
];
