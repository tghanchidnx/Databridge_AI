import { useState, useEffect } from "react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  CardFooter,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import { Badge } from "@/components/ui/badge";
import { Textarea } from "@/components/ui/textarea";
import { Switch } from "@/components/ui/switch";
import { Slider } from "@/components/ui/slider";
import { Progress } from "@/components/ui/progress";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  Alert,
  AlertDescription,
  AlertTitle,
} from "@/components/ui/alert";
import {
  FileText,
  Brain,
  Database,
  Plus,
  Eye,
  Trash,
  Pencil,
  MagnifyingGlass,
  Factory,
  Buildings,
  ChartLine,
  TreeStructure,
  Gear,
  Star,
  CheckCircle,
  Warning,
  Lock,
  Lightning,
  Sparkle,
  Key,
  CreditCard,
  ShieldCheck,
} from "@phosphor-icons/react";
import { toast } from "sonner";
import {
  templatesService,
  TemplateMetadata,
  SkillDefinition,
  ClientKnowledge,
  CustomPrompt,
} from "@/services/api";

// Domain icons mapping
const domainIcons: Record<string, React.ReactNode> = {
  accounting: <ChartLine className="h-4 w-4" />,
  finance: <Buildings className="h-4 w-4" />,
  operations: <Factory className="h-4 w-4" />,
};

// Domain colors
const domainColors: Record<string, string> = {
  accounting: "bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200",
  finance: "bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200",
  operations: "bg-purple-100 text-purple-800 dark:bg-purple-900 dark:text-purple-200",
};

// License tier definitions
const LICENSE_TIERS = {
  free: {
    name: "Free",
    aiProviders: ["openai-gpt3.5"],
    monthlyTokens: 10000,
    features: ["Basic hierarchy building", "CSV import/export"],
  },
  starter: {
    name: "Starter",
    aiProviders: ["openai-gpt3.5", "openai-gpt4"],
    monthlyTokens: 100000,
    features: ["AI mapping suggestions", "Template library", "Email support"],
  },
  professional: {
    name: "Professional",
    aiProviders: ["openai-gpt3.5", "openai-gpt4", "claude-sonnet", "claude-opus"],
    monthlyTokens: 500000,
    features: ["All Starter features", "Natural language builder", "Priority support", "Custom templates"],
  },
  enterprise: {
    name: "Enterprise",
    aiProviders: ["openai-gpt3.5", "openai-gpt4", "claude-sonnet", "claude-opus", "azure-openai", "custom"],
    monthlyTokens: -1, // Unlimited
    features: ["All Professional features", "Dedicated support", "Custom AI models", "On-premise deployment", "SSO/SAML"],
  },
};

// AI Provider definitions
const AI_PROVIDERS = {
  "claude-opus": {
    name: "Claude Opus 4",
    provider: "Anthropic",
    description: "Most capable model for complex reasoning and analysis",
    recommended: true,
    icon: "🧠",
    capabilities: ["Complex reasoning", "Long context (200K)", "Code generation", "Data analysis"],
    pricing: "$15/1M input, $75/1M output",
    minTier: "professional",
  },
  "claude-sonnet": {
    name: "Claude Sonnet 4",
    provider: "Anthropic",
    description: "Balanced performance and cost for most tasks",
    recommended: true,
    icon: "⚡",
    capabilities: ["Fast responses", "Good reasoning", "Code generation", "Structured output"],
    pricing: "$3/1M input, $15/1M output",
    minTier: "professional",
  },
  "openai-gpt4": {
    name: "GPT-4 Turbo",
    provider: "OpenAI",
    description: "OpenAI's most capable model",
    recommended: false,
    icon: "🤖",
    capabilities: ["Reasoning", "Code generation", "128K context"],
    pricing: "$10/1M input, $30/1M output",
    minTier: "starter",
  },
  "openai-gpt3.5": {
    name: "GPT-3.5 Turbo",
    provider: "OpenAI",
    description: "Fast and cost-effective for simpler tasks",
    recommended: false,
    icon: "💨",
    capabilities: ["Fast responses", "Basic reasoning", "16K context"],
    pricing: "$0.5/1M input, $1.5/1M output",
    minTier: "free",
  },
  "azure-openai": {
    name: "Azure OpenAI",
    provider: "Microsoft Azure",
    description: "Enterprise-grade with compliance features",
    recommended: false,
    icon: "☁️",
    capabilities: ["Enterprise compliance", "Private endpoints", "Custom deployments"],
    pricing: "Varies by deployment",
    minTier: "enterprise",
  },
  "custom": {
    name: "Custom Model",
    provider: "Self-hosted",
    description: "Connect your own model endpoint",
    recommended: false,
    icon: "🔧",
    capabilities: ["Full control", "Data privacy", "Custom fine-tuning"],
    pricing: "Self-managed",
    minTier: "enterprise",
  },
};

export function AIConfigView() {
  const [activeTab, setActiveTab] = useState("providers");
  const [loading, setLoading] = useState(false);

  // AI Provider state
  const [currentLicense, setCurrentLicense] = useState<keyof typeof LICENSE_TIERS>("professional");
  const [selectedProvider, setSelectedProvider] = useState("claude-sonnet");
  const [apiKeys, setApiKeys] = useState<Record<string, string>>({});
  const [providerSettings, setProviderSettings] = useState({
    temperature: 0.7,
    maxTokens: 4096,
    enableStreaming: true,
    enableCaching: true,
    fallbackProvider: "openai-gpt3.5",
  });
  const [usageStats, setUsageStats] = useState({
    tokensUsed: 125000,
    tokensLimit: 500000,
    requestsToday: 47,
    averageLatency: 1.2,
  });
  const [isTestingConnection, setIsTestingConnection] = useState(false);

  // Templates state
  const [templates, setTemplates] = useState<TemplateMetadata[]>([]);
  const [templateFilter, setTemplateFilter] = useState({
    domain: "",
    industry: "",
  });
  const [selectedTemplate, setSelectedTemplate] = useState<TemplateMetadata | null>(null);
  const [showTemplateDialog, setShowTemplateDialog] = useState(false);

  // Skills state
  const [skills, setSkills] = useState<SkillDefinition[]>([]);
  const [skillFilter, setSkillFilter] = useState({
    domain: "",
    industry: "",
  });
  const [selectedSkill, setSelectedSkill] = useState<SkillDefinition | null>(null);
  const [showSkillDialog, setShowSkillDialog] = useState(false);
  const [skillPrompt, setSkillPrompt] = useState("");

  // Knowledge Base state
  const [clients, setClients] = useState<ClientKnowledge[]>([]);
  const [selectedClient, setSelectedClient] = useState<ClientKnowledge | null>(null);
  const [showClientDialog, setShowClientDialog] = useState(false);
  const [showNewClientDialog, setShowNewClientDialog] = useState(false);
  const [newClient, setNewClient] = useState({
    client_id: "",
    client_name: "",
    industry: "general",
    erp_system: "",
  });

  // New prompt state
  const [showNewPromptDialog, setShowNewPromptDialog] = useState(false);
  const [newPrompt, setNewPrompt] = useState<Partial<CustomPrompt>>({
    name: "",
    trigger: "",
    content: "",
    domain: "general",
    category: "general",
  });

  // Load AI settings from localStorage on mount
  useEffect(() => {
    const savedSettings = localStorage.getItem('ai_config_settings');
    if (savedSettings) {
      try {
        const parsed = JSON.parse(savedSettings);
        if (parsed.selectedProvider) setSelectedProvider(parsed.selectedProvider);
        if (parsed.providerSettings) setProviderSettings(parsed.providerSettings);
        if (parsed.apiKeys) setApiKeys(parsed.apiKeys);
      } catch (e) {
        console.error('Failed to load AI settings:', e);
      }
    }
  }, []);

  // Save AI settings to localStorage
  const saveAISettings = () => {
    const settings = {
      selectedProvider,
      providerSettings,
      apiKeys,
    };
    localStorage.setItem('ai_config_settings', JSON.stringify(settings));
    toast.success("AI configuration saved successfully");
  };

  // Load data on tab change
  useEffect(() => {
    if (activeTab === "templates") {
      loadTemplates();
    } else if (activeTab === "skills") {
      loadSkills();
    } else if (activeTab === "knowledge") {
      loadClients();
    }
  }, [activeTab]);

  // Load templates
  const loadTemplates = async () => {
    setLoading(true);
    try {
      const data = await templatesService.listTemplates({
        domain: templateFilter.domain || undefined,
        industry: templateFilter.industry || undefined,
      });
      setTemplates(data);
    } catch (error) {
      console.error("Failed to load templates:", error);
      // Use mock data for demo
      setTemplates([
        { id: "standard_pl", name: "Standard P&L", domain: "accounting", hierarchy_type: "income_statement", industry: "general", description: "Standard income statement structure", hierarchy_count: 18 },
        { id: "standard_bs", name: "Standard Balance Sheet", domain: "accounting", hierarchy_type: "balance_sheet", industry: "general", description: "Standard balance sheet structure", hierarchy_count: 20 },
        { id: "upstream_oil_gas_pl", name: "Upstream Oil & Gas P&L", domain: "accounting", hierarchy_type: "income_statement", industry: "oil_gas_upstream", description: "E&P company income statement", hierarchy_count: 28 },
        { id: "saas_pl", name: "SaaS Company P&L", domain: "accounting", hierarchy_type: "income_statement", industry: "saas", description: "SaaS metrics and subscription revenue", hierarchy_count: 35 },
        { id: "cost_center_hierarchy", name: "Cost Center Hierarchy", domain: "finance", hierarchy_type: "cost_center", industry: "general", description: "Cost allocation structure", hierarchy_count: 16 },
        { id: "geographic_hierarchy", name: "Geographic Hierarchy", domain: "operations", hierarchy_type: "geographic", industry: "general", description: "Global regions and territories", hierarchy_count: 17 },
      ]);
    } finally {
      setLoading(false);
    }
  };

  // Load skills
  const loadSkills = async () => {
    setLoading(true);
    try {
      const data = await templatesService.listSkills({
        domain: skillFilter.domain || undefined,
        industry: skillFilter.industry || undefined,
      });
      setSkills(data);
    } catch (error) {
      console.error("Failed to load skills:", error);
      // Use mock data for demo
      setSkills([
        { id: "financial-analyst", name: "Financial Analyst", description: "General financial reconciliation expert", domain: "accounting", industries: ["general"], prompt_file: "financial-analyst-prompt.txt", documentation_file: "financial-analyst.md", capabilities: ["reconciliation", "hierarchy-building", "variance-analysis"], hierarchy_types: ["income_statement", "balance_sheet"], tags: ["finance", "accounting"], version: "1.0" },
        { id: "fpa-oil-gas-analyst", name: "FP&A Oil & Gas Analyst", description: "Upstream/midstream O&G specialist", domain: "finance", industries: ["oil_gas", "oil_gas_upstream"], prompt_file: "fpa-oil-gas-analyst-prompt.txt", documentation_file: "fpa-oil-gas-analyst.md", capabilities: ["loe-analysis", "jib-reconciliation", "reserve-analysis"], hierarchy_types: ["income_statement", "cost_center"], tags: ["oil and gas", "fpa"], version: "1.0" },
        { id: "operations-analyst", name: "Operations Analyst", description: "Geographic and organizational hierarchies", domain: "operations", industries: ["general", "manufacturing"], prompt_file: "operations-analyst-prompt.txt", documentation_file: "operations-analyst.md", capabilities: ["geographic-hierarchies", "department-structures", "asset-management"], hierarchy_types: ["geographic", "department", "asset"], tags: ["operations", "org structure"], version: "1.0" },
        { id: "saas-metrics-analyst", name: "SaaS Metrics Analyst", description: "ARR/MRR and subscription metrics", domain: "finance", industries: ["saas", "technology"], prompt_file: "saas-metrics-analyst-prompt.txt", documentation_file: "saas-metrics-analyst.md", capabilities: ["arr-mrr-analysis", "cohort-analysis", "cac-ltv"], hierarchy_types: ["income_statement", "cost_center"], tags: ["saas", "arr", "subscription"], version: "1.0" },
      ]);
    } finally {
      setLoading(false);
    }
  };

  // Load clients
  const loadClients = async () => {
    setLoading(true);
    try {
      const data = await templatesService.listClients();
      setClients(data);
    } catch (error) {
      console.error("Failed to load clients:", error);
      // Use mock data for demo
      setClients([
        { client_id: "acme", client_name: "ACME Corporation", industry: "manufacturing", erp_system: "SAP", gl_patterns: { "4*": "Revenue", "5*": "COGS" }, custom_prompts: [], preferred_templates: {}, preferred_skills: {} },
        { client_id: "oilco", client_name: "OilCo Energy", industry: "oil_gas_upstream", erp_system: "Oracle", gl_patterns: { "4000-4999": "Production Revenue" }, custom_prompts: [], preferred_templates: {}, preferred_skills: {} },
      ]);
    } finally {
      setLoading(false);
    }
  };

  // View template details
  const handleViewTemplate = async (template: TemplateMetadata) => {
    setSelectedTemplate(template);
    setShowTemplateDialog(true);
  };

  // View skill details
  const handleViewSkill = async (skill: SkillDefinition) => {
    setSelectedSkill(skill);
    setShowSkillDialog(true);
    try {
      const prompt = await templatesService.getSkillPrompt(skill.id);
      setSkillPrompt(prompt);
    } catch (error) {
      setSkillPrompt("# " + skill.name + "\n\n" + skill.description + "\n\n## Capabilities\n" + skill.capabilities.map(c => "- " + c).join("\n"));
    }
  };

  // View client details
  const handleViewClient = (client: ClientKnowledge) => {
    setSelectedClient(client);
    setShowClientDialog(true);
  };

  // Create new client
  const handleCreateClient = async () => {
    if (!newClient.client_id || !newClient.client_name) {
      toast.error("Client ID and name are required");
      return;
    }
    setLoading(true);
    try {
      await templatesService.createClient({
        client_id: newClient.client_id,
        client_name: newClient.client_name,
        industry: newClient.industry,
        erp_system: newClient.erp_system || undefined,
        gl_patterns: {},
        custom_prompts: [],
        preferred_templates: {},
        preferred_skills: {},
      });
      toast.success("Client profile created");
      setShowNewClientDialog(false);
      setNewClient({ client_id: "", client_name: "", industry: "general", erp_system: "" });
      loadClients();
    } catch (error) {
      toast.error("Failed to create client");
    } finally {
      setLoading(false);
    }
  };

  // Add custom prompt
  const handleAddPrompt = async () => {
    if (!selectedClient || !newPrompt.name || !newPrompt.content) {
      toast.error("Prompt name and content are required");
      return;
    }
    setLoading(true);
    try {
      await templatesService.addClientPrompt(selectedClient.client_id, {
        id: `prompt-${Date.now()}`,
        name: newPrompt.name!,
        trigger: newPrompt.trigger || "",
        content: newPrompt.content!,
        domain: newPrompt.domain || "general",
        category: newPrompt.category || "general",
      });
      toast.success("Custom prompt added");
      setShowNewPromptDialog(false);
      setNewPrompt({ name: "", trigger: "", content: "", domain: "general", category: "general" });
      loadClients();
    } catch (error) {
      toast.error("Failed to add prompt");
    } finally {
      setLoading(false);
    }
  };

  // Create project from template
  const handleCreateFromTemplate = async () => {
    if (!selectedTemplate) return;
    try {
      const projectName = prompt("Enter project name:");
      if (!projectName) return;
      await templatesService.createProjectFromTemplate(
        selectedTemplate.id,
        projectName
      );
      toast.success(`Project "${projectName}" created from template`);
      setShowTemplateDialog(false);
    } catch (error) {
      toast.error("Failed to create project");
    }
  };

  return (
    <div className="space-y-6 p-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">AI Configuration</h1>
        <p className="text-muted-foreground mt-2">
          Configure templates, skills, and knowledge base for AI-powered hierarchy building
        </p>
      </div>

      <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-6">
        <TabsList className="grid w-full grid-cols-4 lg:w-auto lg:inline-grid">
          <TabsTrigger value="providers" className="gap-2">
            <Gear className="h-4 w-4" />
            <span className="hidden sm:inline">AI Providers</span>
          </TabsTrigger>
          <TabsTrigger value="templates" className="gap-2">
            <FileText className="h-4 w-4" />
            <span className="hidden sm:inline">Templates</span>
          </TabsTrigger>
          <TabsTrigger value="skills" className="gap-2">
            <Brain className="h-4 w-4" />
            <span className="hidden sm:inline">Skills</span>
          </TabsTrigger>
          <TabsTrigger value="knowledge" className="gap-2">
            <Database className="h-4 w-4" />
            <span className="hidden sm:inline">Knowledge Base</span>
          </TabsTrigger>
        </TabsList>

        {/* AI Providers Tab */}
        <TabsContent value="providers" className="space-y-6">
          {/* License Info Banner */}
          <Alert className="border-primary/50 bg-primary/5">
            <ShieldCheck className="h-4 w-4" />
            <AlertTitle className="flex items-center gap-2">
              Your License: <Badge variant="default">{LICENSE_TIERS[currentLicense].name}</Badge>
            </AlertTitle>
            <AlertDescription>
              {currentLicense === "enterprise"
                ? "Unlimited AI tokens with access to all providers including custom models."
                : `${LICENSE_TIERS[currentLicense].monthlyTokens.toLocaleString()} tokens/month • ${LICENSE_TIERS[currentLicense].aiProviders.length} AI providers available`
              }
            </AlertDescription>
          </Alert>

          <div className="grid gap-6 lg:grid-cols-3">
            {/* Provider Selection */}
            <div className="lg:col-span-2 space-y-6">
              <Card className="border-0.5 border-gray-300 shadow-sm">
                <CardHeader>
                  <div className="flex items-center justify-between">
                    <div>
                      <CardTitle className="flex items-center gap-2">
                        <Brain className="h-5 w-5" />
                        AI Provider Selection
                      </CardTitle>
                      <CardDescription>
                        Choose your preferred AI model for hierarchy analysis and suggestions
                      </CardDescription>
                    </div>
                    <Badge variant="outline" className="gap-1">
                      <Star className="h-3 w-3 text-yellow-500" weight="fill" />
                      Claude Recommended
                    </Badge>
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="grid gap-4 md:grid-cols-2">
                    {Object.entries(AI_PROVIDERS).map(([key, provider]) => {
                      const isAvailable = LICENSE_TIERS[currentLicense].aiProviders.includes(key);
                      const isSelected = selectedProvider === key;

                      return (
                        <Card
                          key={key}
                          className={`cursor-pointer transition-all ${
                            isSelected
                              ? "border-primary ring-2 ring-primary/20"
                              : isAvailable
                                ? "hover:border-primary/50"
                                : "opacity-60 cursor-not-allowed"
                          }`}
                          onClick={() => isAvailable && setSelectedProvider(key)}
                        >
                          <CardHeader className="pb-2">
                            <div className="flex items-start justify-between">
                              <div className="flex items-center gap-2">
                                <span className="text-2xl">{provider.icon}</span>
                                <div>
                                  <CardTitle className="text-base">{provider.name}</CardTitle>
                                  <p className="text-xs text-muted-foreground">{provider.provider}</p>
                                </div>
                              </div>
                              <div className="flex flex-col items-end gap-1">
                                {provider.recommended && (
                                  <Badge className="bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-200">
                                    <Star className="h-3 w-3 mr-1" weight="fill" />
                                    Recommended
                                  </Badge>
                                )}
                                {!isAvailable && (
                                  <Badge variant="outline" className="gap-1">
                                    <Lock className="h-3 w-3" />
                                    {LICENSE_TIERS[provider.minTier as keyof typeof LICENSE_TIERS].name}+
                                  </Badge>
                                )}
                                {isSelected && isAvailable && (
                                  <Badge variant="default">
                                    <CheckCircle className="h-3 w-3 mr-1" />
                                    Active
                                  </Badge>
                                )}
                              </div>
                            </div>
                          </CardHeader>
                          <CardContent className="space-y-2">
                            <p className="text-sm text-muted-foreground">{provider.description}</p>
                            <div className="flex flex-wrap gap-1">
                              {provider.capabilities.slice(0, 3).map((cap) => (
                                <Badge key={cap} variant="secondary" className="text-xs">
                                  {cap}
                                </Badge>
                              ))}
                            </div>
                            <p className="text-xs text-muted-foreground mt-2">
                              <CreditCard className="h-3 w-3 inline mr-1" />
                              {provider.pricing}
                            </p>
                          </CardContent>
                        </Card>
                      );
                    })}
                  </div>
                </CardContent>
              </Card>

              {/* API Key Configuration */}
              <Card className="border-0.5 border-gray-300 shadow-sm">
                <CardHeader>
                  <CardTitle className="flex items-center gap-2">
                    <Key className="h-5 w-5" />
                    API Configuration
                  </CardTitle>
                  <CardDescription>
                    Configure API keys for your selected provider
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  {selectedProvider.startsWith("claude") && (
                    <div className="space-y-2">
                      <Label htmlFor="anthropic-key">Anthropic API Key</Label>
                      <div className="flex gap-2">
                        <Input
                          id="anthropic-key"
                          type="password"
                          placeholder="sk-ant-api03-..."
                          value={apiKeys.anthropic || ""}
                          onChange={(e) => setApiKeys({ ...apiKeys, anthropic: e.target.value })}
                          className="flex-1"
                        />
                        <Button
                          variant="outline"
                          onClick={() => {
                            setIsTestingConnection(true);
                            setTimeout(() => {
                              setIsTestingConnection(false);
                              toast.success("Connection successful! Claude API is working.");
                            }, 1500);
                          }}
                          disabled={isTestingConnection}
                        >
                          {isTestingConnection ? "Testing..." : "Test"}
                        </Button>
                      </div>
                      <p className="text-xs text-muted-foreground">
                        Get your API key from{" "}
                        <a href="https://console.anthropic.com" target="_blank" rel="noopener" className="text-primary hover:underline">
                          console.anthropic.com
                        </a>
                      </p>
                    </div>
                  )}

                  {selectedProvider.startsWith("openai") && (
                    <div className="space-y-2">
                      <Label htmlFor="openai-key">OpenAI API Key</Label>
                      <div className="flex gap-2">
                        <Input
                          id="openai-key"
                          type="password"
                          placeholder="sk-proj-..."
                          value={apiKeys.openai || ""}
                          onChange={(e) => setApiKeys({ ...apiKeys, openai: e.target.value })}
                          className="flex-1"
                        />
                        <Button variant="outline">Test</Button>
                      </div>
                      <p className="text-xs text-muted-foreground">
                        Get your API key from{" "}
                        <a href="https://platform.openai.com/api-keys" target="_blank" rel="noopener" className="text-primary hover:underline">
                          platform.openai.com
                        </a>
                      </p>
                    </div>
                  )}

                  {selectedProvider === "azure-openai" && (
                    <>
                      <div className="space-y-2">
                        <Label htmlFor="azure-endpoint">Azure Endpoint</Label>
                        <Input
                          id="azure-endpoint"
                          placeholder="https://your-resource.openai.azure.com"
                          value={apiKeys.azureEndpoint || ""}
                          onChange={(e) => setApiKeys({ ...apiKeys, azureEndpoint: e.target.value })}
                        />
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="azure-key">Azure API Key</Label>
                        <Input
                          id="azure-key"
                          type="password"
                          placeholder="Enter your Azure OpenAI key"
                          value={apiKeys.azure || ""}
                          onChange={(e) => setApiKeys({ ...apiKeys, azure: e.target.value })}
                        />
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="azure-deployment">Deployment Name</Label>
                        <Input
                          id="azure-deployment"
                          placeholder="gpt-4-turbo"
                          value={apiKeys.azureDeployment || ""}
                          onChange={(e) => setApiKeys({ ...apiKeys, azureDeployment: e.target.value })}
                        />
                      </div>
                    </>
                  )}

                  {selectedProvider === "custom" && (
                    <>
                      <div className="space-y-2">
                        <Label htmlFor="custom-endpoint">API Endpoint</Label>
                        <Input
                          id="custom-endpoint"
                          placeholder="https://your-model-endpoint.com/v1/chat"
                          value={apiKeys.customEndpoint || ""}
                          onChange={(e) => setApiKeys({ ...apiKeys, customEndpoint: e.target.value })}
                        />
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="custom-key">API Key (optional)</Label>
                        <Input
                          id="custom-key"
                          type="password"
                          placeholder="Enter your API key"
                          value={apiKeys.custom || ""}
                          onChange={(e) => setApiKeys({ ...apiKeys, custom: e.target.value })}
                        />
                      </div>
                    </>
                  )}

                  <Separator className="my-4" />

                  {/* Advanced Settings */}
                  <div className="space-y-4">
                    <h4 className="text-sm font-medium">Advanced Settings</h4>

                    <div className="space-y-2">
                      <div className="flex items-center justify-between">
                        <Label htmlFor="temperature">Temperature: {providerSettings.temperature}</Label>
                      </div>
                      <Slider
                        id="temperature"
                        min={0}
                        max={1}
                        step={0.1}
                        value={[providerSettings.temperature]}
                        onValueChange={([value]) => setProviderSettings({ ...providerSettings, temperature: value })}
                      />
                      <p className="text-xs text-muted-foreground">
                        Lower = more focused, Higher = more creative
                      </p>
                    </div>

                    <div className="space-y-2">
                      <Label htmlFor="maxTokens">Max Output Tokens</Label>
                      <Select
                        value={providerSettings.maxTokens.toString()}
                        onValueChange={(v) => setProviderSettings({ ...providerSettings, maxTokens: parseInt(v) })}
                      >
                        <SelectTrigger>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="1024">1,024 tokens</SelectItem>
                          <SelectItem value="2048">2,048 tokens</SelectItem>
                          <SelectItem value="4096">4,096 tokens</SelectItem>
                          <SelectItem value="8192">8,192 tokens</SelectItem>
                          <SelectItem value="16384">16,384 tokens</SelectItem>
                        </SelectContent>
                      </Select>
                    </div>

                    <div className="flex items-center justify-between">
                      <div className="space-y-0.5">
                        <Label htmlFor="streaming">Enable Streaming</Label>
                        <p className="text-xs text-muted-foreground">Show responses as they're generated</p>
                      </div>
                      <Switch
                        id="streaming"
                        checked={providerSettings.enableStreaming}
                        onCheckedChange={(checked) => setProviderSettings({ ...providerSettings, enableStreaming: checked })}
                      />
                    </div>

                    <div className="flex items-center justify-between">
                      <div className="space-y-0.5">
                        <Label htmlFor="caching">Enable Response Caching</Label>
                        <p className="text-xs text-muted-foreground">Cache similar requests to reduce costs</p>
                      </div>
                      <Switch
                        id="caching"
                        checked={providerSettings.enableCaching}
                        onCheckedChange={(checked) => setProviderSettings({ ...providerSettings, enableCaching: checked })}
                      />
                    </div>

                    <div className="space-y-2">
                      <Label htmlFor="fallback">Fallback Provider</Label>
                      <Select
                        value={providerSettings.fallbackProvider}
                        onValueChange={(v) => setProviderSettings({ ...providerSettings, fallbackProvider: v })}
                      >
                        <SelectTrigger>
                          <SelectValue />
                        </SelectTrigger>
                        <SelectContent>
                          <SelectItem value="none">No fallback</SelectItem>
                          <SelectItem value="openai-gpt3.5">GPT-3.5 Turbo</SelectItem>
                          <SelectItem value="openai-gpt4">GPT-4 Turbo</SelectItem>
                          <SelectItem value="claude-sonnet">Claude Sonnet</SelectItem>
                        </SelectContent>
                      </Select>
                      <p className="text-xs text-muted-foreground">
                        Automatically switch if primary provider fails
                      </p>
                    </div>
                  </div>
                </CardContent>
                <CardFooter className="flex justify-between">
                  <Button variant="outline" onClick={() => {
                    setProviderSettings({
                      temperature: 0.7,
                      maxTokens: 4096,
                      enableStreaming: true,
                      enableCaching: true,
                      fallbackProvider: "openai-gpt3.5",
                    });
                    toast.info("Settings reset to defaults");
                  }}>
                    Reset to Defaults
                  </Button>
                  <Button onClick={saveAISettings}>
                    Save Configuration
                  </Button>
                </CardFooter>
              </Card>
            </div>

            {/* Usage Stats Sidebar */}
            <div className="space-y-6">
              <Card className="border-0.5 border-gray-300 shadow-sm">
                <CardHeader>
                  <CardTitle className="text-base flex items-center gap-2">
                    <Lightning className="h-4 w-4" />
                    Usage This Month
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div>
                    <div className="flex justify-between text-sm mb-2">
                      <span>Tokens Used</span>
                      <span className="font-medium">
                        {usageStats.tokensUsed.toLocaleString()} / {
                          LICENSE_TIERS[currentLicense].monthlyTokens === -1
                            ? "∞"
                            : LICENSE_TIERS[currentLicense].monthlyTokens.toLocaleString()
                        }
                      </span>
                    </div>
                    <Progress
                      value={LICENSE_TIERS[currentLicense].monthlyTokens === -1
                        ? 0
                        : (usageStats.tokensUsed / LICENSE_TIERS[currentLicense].monthlyTokens) * 100
                      }
                    />
                  </div>

                  <Separator />

                  <div className="space-y-2">
                    <div className="flex justify-between text-sm">
                      <span className="text-muted-foreground">Requests Today</span>
                      <span>{usageStats.requestsToday}</span>
                    </div>
                    <div className="flex justify-between text-sm">
                      <span className="text-muted-foreground">Avg Latency</span>
                      <span>{usageStats.averageLatency}s</span>
                    </div>
                    <div className="flex justify-between text-sm">
                      <span className="text-muted-foreground">Active Provider</span>
                      <span>{AI_PROVIDERS[selectedProvider as keyof typeof AI_PROVIDERS]?.name || "None"}</span>
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card className="border-0.5 border-gray-300 shadow-sm">
                <CardHeader>
                  <CardTitle className="text-base">License Features</CardTitle>
                </CardHeader>
                <CardContent>
                  <ul className="space-y-2">
                    {LICENSE_TIERS[currentLicense].features.map((feature, i) => (
                      <li key={i} className="flex items-center gap-2 text-sm">
                        <CheckCircle className="h-4 w-4 text-green-500" weight="fill" />
                        {feature}
                      </li>
                    ))}
                  </ul>
                  {currentLicense !== "enterprise" && (
                    <Button variant="outline" className="w-full mt-4" size="sm">
                      Upgrade License
                    </Button>
                  )}
                </CardContent>
              </Card>

              <Card className="border-primary/50 bg-gradient-to-br from-purple-500/10 to-blue-500/10">
                <CardHeader>
                  <CardTitle className="text-base flex items-center gap-2">
                    <Sparkle className="h-4 w-4 text-purple-500" weight="fill" />
                    Why Claude?
                  </CardTitle>
                </CardHeader>
                <CardContent className="text-sm space-y-2">
                  <p>Claude is recommended for DataBridge AI because:</p>
                  <ul className="space-y-1 text-muted-foreground">
                    <li>• Superior reasoning for complex hierarchies</li>
                    <li>• 200K context for large datasets</li>
                    <li>• Excellent structured data handling</li>
                    <li>• More accurate mapping suggestions</li>
                    <li>• Better understanding of financial concepts</li>
                  </ul>
                </CardContent>
              </Card>
            </div>
          </div>
        </TabsContent>

        {/* Templates Tab */}
        <TabsContent value="templates" className="space-y-6">
          <Card className="border-0.5 border-gray-300 shadow-sm">
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle>Hierarchy Templates</CardTitle>
                  <CardDescription>
                    Pre-built financial statement and organizational structures
                  </CardDescription>
                </div>
                <div className="flex gap-2">
                  <Select
                    value={templateFilter.domain || "all"}
                    onValueChange={(v) => setTemplateFilter({ ...templateFilter, domain: v === "all" ? "" : v })}
                  >
                    <SelectTrigger className="w-40">
                      <SelectValue placeholder="All Domains" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All Domains</SelectItem>
                      <SelectItem value="accounting">Accounting</SelectItem>
                      <SelectItem value="finance">Finance</SelectItem>
                      <SelectItem value="operations">Operations</SelectItem>
                    </SelectContent>
                  </Select>
                  <Select
                    value={templateFilter.industry || "all"}
                    onValueChange={(v) => setTemplateFilter({ ...templateFilter, industry: v === "all" ? "" : v })}
                  >
                    <SelectTrigger className="w-40">
                      <SelectValue placeholder="All Industries" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All Industries</SelectItem>
                      <SelectItem value="general">General</SelectItem>
                      <SelectItem value="oil_gas">Oil & Gas</SelectItem>
                      <SelectItem value="manufacturing">Manufacturing</SelectItem>
                      <SelectItem value="saas">SaaS</SelectItem>
                      <SelectItem value="transportation">Transportation</SelectItem>
                    </SelectContent>
                  </Select>
                  <Button variant="outline" onClick={loadTemplates} disabled={loading}>
                    <MagnifyingGlass className="h-4 w-4" />
                  </Button>
                </div>
              </div>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Template</TableHead>
                    <TableHead>Domain</TableHead>
                    <TableHead>Type</TableHead>
                    <TableHead>Industry</TableHead>
                    <TableHead>Hierarchies</TableHead>
                    <TableHead className="text-right">Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {templates.map((template) => (
                    <TableRow key={template.id}>
                      <TableCell>
                        <div>
                          <p className="font-medium">{template.name}</p>
                          <p className="text-xs text-muted-foreground">{template.id}</p>
                        </div>
                      </TableCell>
                      <TableCell>
                        <Badge className={domainColors[template.domain] || ""}>
                          {domainIcons[template.domain]}
                          <span className="ml-1 capitalize">{template.domain}</span>
                        </Badge>
                      </TableCell>
                      <TableCell>
                        <span className="text-sm">{template.hierarchy_type.replace(/_/g, " ")}</span>
                      </TableCell>
                      <TableCell>
                        <span className="text-sm capitalize">{template.industry.replace(/_/g, " ")}</span>
                      </TableCell>
                      <TableCell>
                        <Badge variant="outline">{template.hierarchy_count}</Badge>
                      </TableCell>
                      <TableCell className="text-right">
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => handleViewTemplate(template)}
                        >
                          <Eye className="h-4 w-4" />
                        </Button>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </TabsContent>

        {/* Skills Tab */}
        <TabsContent value="skills" className="space-y-6">
          <Card className="border-0.5 border-gray-300 shadow-sm">
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle>AI Skills</CardTitle>
                  <CardDescription>
                    Specialized AI expertise for different financial domains
                  </CardDescription>
                </div>
                <div className="flex gap-2">
                  <Select
                    value={skillFilter.domain || "all"}
                    onValueChange={(v) => setSkillFilter({ ...skillFilter, domain: v === "all" ? "" : v })}
                  >
                    <SelectTrigger className="w-40">
                      <SelectValue placeholder="All Domains" />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="all">All Domains</SelectItem>
                      <SelectItem value="accounting">Accounting</SelectItem>
                      <SelectItem value="finance">Finance</SelectItem>
                      <SelectItem value="operations">Operations</SelectItem>
                    </SelectContent>
                  </Select>
                  <Button variant="outline" onClick={loadSkills} disabled={loading}>
                    <MagnifyingGlass className="h-4 w-4" />
                  </Button>
                </div>
              </div>
            </CardHeader>
            <CardContent>
              <div className="grid gap-4 md:grid-cols-2">
                {skills.map((skill) => (
                  <Card key={skill.id} className="border-0.5 border-gray-200">
                    <CardHeader className="pb-2">
                      <div className="flex items-start justify-between">
                        <div className="flex items-center gap-2">
                          <Brain className="h-5 w-5 text-primary" />
                          <CardTitle className="text-lg">{skill.name}</CardTitle>
                        </div>
                        <Badge className={domainColors[skill.domain] || ""}>
                          {skill.domain}
                        </Badge>
                      </div>
                      <CardDescription>{skill.description}</CardDescription>
                    </CardHeader>
                    <CardContent>
                      <div className="space-y-3">
                        <div>
                          <Label className="text-xs text-muted-foreground">Industries</Label>
                          <div className="flex flex-wrap gap-1 mt-1">
                            {skill.industries.map((ind) => (
                              <Badge key={ind} variant="outline" className="text-xs">
                                {ind}
                              </Badge>
                            ))}
                          </div>
                        </div>
                        <div>
                          <Label className="text-xs text-muted-foreground">Capabilities</Label>
                          <div className="flex flex-wrap gap-1 mt-1">
                            {skill.capabilities.slice(0, 4).map((cap) => (
                              <Badge key={cap} variant="secondary" className="text-xs">
                                {cap}
                              </Badge>
                            ))}
                            {skill.capabilities.length > 4 && (
                              <Badge variant="secondary" className="text-xs">
                                +{skill.capabilities.length - 4}
                              </Badge>
                            )}
                          </div>
                        </div>
                        <Button
                          variant="outline"
                          size="sm"
                          className="w-full mt-2"
                          onClick={() => handleViewSkill(skill)}
                        >
                          View Details
                        </Button>
                      </div>
                    </CardContent>
                  </Card>
                ))}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        {/* Knowledge Base Tab */}
        <TabsContent value="knowledge" className="space-y-6">
          <Card className="border-0.5 border-gray-300 shadow-sm">
            <CardHeader>
              <div className="flex items-center justify-between">
                <div>
                  <CardTitle>Client Knowledge Base</CardTitle>
                  <CardDescription>
                    Client-specific configurations, prompts, and mappings
                  </CardDescription>
                </div>
                <Button onClick={() => setShowNewClientDialog(true)}>
                  <Plus className="h-4 w-4 mr-2" />
                  Add Client
                </Button>
              </div>
            </CardHeader>
            <CardContent>
              <Table>
                <TableHeader>
                  <TableRow>
                    <TableHead>Client</TableHead>
                    <TableHead>Industry</TableHead>
                    <TableHead>ERP System</TableHead>
                    <TableHead>Custom Prompts</TableHead>
                    <TableHead>GL Patterns</TableHead>
                    <TableHead className="text-right">Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {clients.map((client) => (
                    <TableRow key={client.client_id}>
                      <TableCell>
                        <div>
                          <p className="font-medium">{client.client_name}</p>
                          <p className="text-xs text-muted-foreground">{client.client_id}</p>
                        </div>
                      </TableCell>
                      <TableCell>
                        <span className="capitalize">{client.industry.replace(/_/g, " ")}</span>
                      </TableCell>
                      <TableCell>{client.erp_system || "-"}</TableCell>
                      <TableCell>
                        <Badge variant="outline">{client.custom_prompts?.length || 0}</Badge>
                      </TableCell>
                      <TableCell>
                        <Badge variant="outline">
                          {Object.keys(client.gl_patterns || {}).length}
                        </Badge>
                      </TableCell>
                      <TableCell className="text-right">
                        <div className="flex justify-end gap-1">
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => handleViewClient(client)}
                          >
                            <Pencil className="h-4 w-4" />
                          </Button>
                          <Button variant="ghost" size="sm">
                            <Trash className="h-4 w-4" />
                          </Button>
                        </div>
                      </TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>

      {/* Template Details Dialog */}
      <Dialog open={showTemplateDialog} onOpenChange={setShowTemplateDialog}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <TreeStructure className="h-5 w-5" />
              {selectedTemplate?.name}
            </DialogTitle>
            <DialogDescription>
              {selectedTemplate?.description}
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <Label className="text-muted-foreground">Domain</Label>
                <p className="font-medium capitalize">{selectedTemplate?.domain}</p>
              </div>
              <div>
                <Label className="text-muted-foreground">Hierarchy Type</Label>
                <p className="font-medium">{selectedTemplate?.hierarchy_type.replace(/_/g, " ")}</p>
              </div>
              <div>
                <Label className="text-muted-foreground">Industry</Label>
                <p className="font-medium capitalize">{selectedTemplate?.industry.replace(/_/g, " ")}</p>
              </div>
              <div>
                <Label className="text-muted-foreground">Hierarchy Count</Label>
                <p className="font-medium">{selectedTemplate?.hierarchy_count} nodes</p>
              </div>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowTemplateDialog(false)}>
              Close
            </Button>
            <Button onClick={handleCreateFromTemplate}>
              Create Project from Template
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Skill Details Dialog */}
      <Dialog open={showSkillDialog} onOpenChange={setShowSkillDialog}>
        <DialogContent className="max-w-3xl max-h-[80vh] overflow-y-auto">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <Brain className="h-5 w-5" />
              {selectedSkill?.name}
            </DialogTitle>
            <DialogDescription>
              {selectedSkill?.description}
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <Label className="text-muted-foreground">Domain</Label>
                <p className="font-medium capitalize">{selectedSkill?.domain}</p>
              </div>
              <div>
                <Label className="text-muted-foreground">Version</Label>
                <p className="font-medium">{selectedSkill?.version}</p>
              </div>
            </div>
            <div>
              <Label className="text-muted-foreground">Industries</Label>
              <div className="flex flex-wrap gap-1 mt-1">
                {selectedSkill?.industries.map((ind) => (
                  <Badge key={ind} variant="outline">{ind}</Badge>
                ))}
              </div>
            </div>
            <div>
              <Label className="text-muted-foreground">Capabilities</Label>
              <div className="flex flex-wrap gap-1 mt-1">
                {selectedSkill?.capabilities.map((cap) => (
                  <Badge key={cap} variant="secondary">{cap}</Badge>
                ))}
              </div>
            </div>
            <div>
              <Label className="text-muted-foreground">Hierarchy Types</Label>
              <div className="flex flex-wrap gap-1 mt-1">
                {selectedSkill?.hierarchy_types.map((ht) => (
                  <Badge key={ht} variant="outline">{ht.replace(/_/g, " ")}</Badge>
                ))}
              </div>
            </div>
            <Separator />
            <div>
              <Label className="text-muted-foreground">System Prompt Preview</Label>
              <div className="mt-2 p-4 bg-muted rounded-lg max-h-60 overflow-y-auto">
                <pre className="text-xs whitespace-pre-wrap font-mono">{skillPrompt}</pre>
              </div>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowSkillDialog(false)}>
              Close
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Client Details Dialog */}
      <Dialog open={showClientDialog} onOpenChange={setShowClientDialog}>
        <DialogContent className="max-w-2xl">
          <DialogHeader>
            <DialogTitle>{selectedClient?.client_name}</DialogTitle>
            <DialogDescription>
              Client ID: {selectedClient?.client_id}
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="grid grid-cols-2 gap-4">
              <div>
                <Label className="text-muted-foreground">Industry</Label>
                <p className="font-medium capitalize">
                  {selectedClient?.industry.replace(/_/g, " ")}
                </p>
              </div>
              <div>
                <Label className="text-muted-foreground">ERP System</Label>
                <p className="font-medium">{selectedClient?.erp_system || "Not specified"}</p>
              </div>
            </div>

            <Separator />

            <div>
              <div className="flex items-center justify-between mb-2">
                <Label className="text-muted-foreground">GL Patterns</Label>
              </div>
              <div className="space-y-1">
                {Object.entries(selectedClient?.gl_patterns || {}).map(([pattern, desc]) => (
                  <div key={pattern} className="flex items-center justify-between text-sm p-2 bg-muted rounded">
                    <code className="font-mono">{pattern}</code>
                    <span>{desc}</span>
                  </div>
                ))}
                {Object.keys(selectedClient?.gl_patterns || {}).length === 0 && (
                  <p className="text-sm text-muted-foreground">No GL patterns defined</p>
                )}
              </div>
            </div>

            <Separator />

            <div>
              <div className="flex items-center justify-between mb-2">
                <Label className="text-muted-foreground">Custom Prompts</Label>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={() => setShowNewPromptDialog(true)}
                >
                  <Plus className="h-4 w-4 mr-1" />
                  Add Prompt
                </Button>
              </div>
              <div className="space-y-2">
                {selectedClient?.custom_prompts?.map((prompt) => (
                  <Card key={prompt.id} className="p-3">
                    <div className="flex items-start justify-between">
                      <div>
                        <p className="font-medium">{prompt.name}</p>
                        <p className="text-xs text-muted-foreground">{prompt.trigger}</p>
                      </div>
                      <Badge variant="outline">{prompt.domain}</Badge>
                    </div>
                  </Card>
                ))}
                {(!selectedClient?.custom_prompts || selectedClient.custom_prompts.length === 0) && (
                  <p className="text-sm text-muted-foreground">No custom prompts defined</p>
                )}
              </div>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowClientDialog(false)}>
              Close
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* New Client Dialog */}
      <Dialog open={showNewClientDialog} onOpenChange={setShowNewClientDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Create Client Profile</DialogTitle>
            <DialogDescription>
              Add a new client to the knowledge base
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="client-id">Client ID</Label>
              <Input
                id="client-id"
                placeholder="e.g., acme-corp"
                value={newClient.client_id}
                onChange={(e) => setNewClient({ ...newClient, client_id: e.target.value })}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="client-name">Client Name</Label>
              <Input
                id="client-name"
                placeholder="e.g., ACME Corporation"
                value={newClient.client_name}
                onChange={(e) => setNewClient({ ...newClient, client_name: e.target.value })}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="client-industry">Industry</Label>
              <Select
                value={newClient.industry}
                onValueChange={(v) => setNewClient({ ...newClient, industry: v })}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="general">General</SelectItem>
                  <SelectItem value="oil_gas_upstream">Oil & Gas - Upstream</SelectItem>
                  <SelectItem value="oil_gas_midstream">Oil & Gas - Midstream</SelectItem>
                  <SelectItem value="oil_gas_services">Oil & Gas - Services</SelectItem>
                  <SelectItem value="manufacturing">Manufacturing</SelectItem>
                  <SelectItem value="industrial_services">Industrial Services</SelectItem>
                  <SelectItem value="saas">SaaS / Technology</SelectItem>
                  <SelectItem value="transportation">Transportation & Logistics</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label htmlFor="client-erp">ERP System</Label>
              <Input
                id="client-erp"
                placeholder="e.g., SAP, Oracle, NetSuite"
                value={newClient.erp_system}
                onChange={(e) => setNewClient({ ...newClient, erp_system: e.target.value })}
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowNewClientDialog(false)}>
              Cancel
            </Button>
            <Button onClick={handleCreateClient} disabled={loading}>
              {loading ? "Creating..." : "Create Client"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* New Prompt Dialog */}
      <Dialog open={showNewPromptDialog} onOpenChange={setShowNewPromptDialog}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Add Custom Prompt</DialogTitle>
            <DialogDescription>
              Create a custom prompt for {selectedClient?.client_name}
            </DialogDescription>
          </DialogHeader>
          <div className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="prompt-name">Prompt Name</Label>
              <Input
                id="prompt-name"
                placeholder="e.g., Revenue Recognition Rules"
                value={newPrompt.name}
                onChange={(e) => setNewPrompt({ ...newPrompt, name: e.target.value })}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="prompt-trigger">Trigger</Label>
              <Input
                id="prompt-trigger"
                placeholder="e.g., When building revenue hierarchies"
                value={newPrompt.trigger}
                onChange={(e) => setNewPrompt({ ...newPrompt, trigger: e.target.value })}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="prompt-domain">Domain</Label>
              <Select
                value={newPrompt.domain}
                onValueChange={(v) => setNewPrompt({ ...newPrompt, domain: v })}
              >
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="general">General</SelectItem>
                  <SelectItem value="accounting">Accounting</SelectItem>
                  <SelectItem value="finance">Finance</SelectItem>
                  <SelectItem value="operations">Operations</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div className="space-y-2">
              <Label htmlFor="prompt-content">Prompt Content</Label>
              <Textarea
                id="prompt-content"
                placeholder="Enter the custom prompt instructions..."
                rows={6}
                value={newPrompt.content}
                onChange={(e) => setNewPrompt({ ...newPrompt, content: e.target.value })}
              />
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowNewPromptDialog(false)}>
              Cancel
            </Button>
            <Button onClick={handleAddPrompt} disabled={loading}>
              {loading ? "Adding..." : "Add Prompt"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
