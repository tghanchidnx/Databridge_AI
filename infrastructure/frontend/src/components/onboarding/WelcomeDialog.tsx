/**
 * Welcome Dialog
 * First-time user welcome screen with feature highlights and tour option
 */
import { useState } from "react";
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
import { Checkbox } from "@/components/ui/checkbox";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import {
  Sparkles,
  Rocket,
  Brain,
  Wand2,
  MessageSquare,
  GitBranch,
  LayoutGrid,
  Keyboard,
  Play,
  ArrowRight,
  Star,
  Zap,
  Shield,
  RefreshCw,
} from "lucide-react";

interface WelcomeDialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onStartTour: () => void;
  onSkipTour: () => void;
  userName?: string;
  isNewUser?: boolean;
  version?: string;
}

const NEW_FEATURES = [
  {
    icon: <Brain className="h-5 w-5 text-purple-600" />,
    title: "AI-Powered Mapping",
    description: "Smart suggestions learn from your patterns",
    isNew: true,
  },
  {
    icon: <Wand2 className="h-5 w-5 text-blue-600" />,
    title: "Natural Language Builder",
    description: "Describe hierarchies in plain English",
    isNew: true,
  },
  {
    icon: <MessageSquare className="h-5 w-5 text-green-600" />,
    title: "AI Chat Assistant",
    description: "Get help and execute actions via chat",
    isNew: true,
  },
  {
    icon: <LayoutGrid className="h-5 w-5 text-orange-600" />,
    title: "Template Gallery",
    description: "Start with pre-built industry templates",
    isNew: true,
  },
  {
    icon: <Keyboard className="h-5 w-5 text-gray-600" />,
    title: "Keyboard Shortcuts",
    description: "Work faster with hotkeys (press ? for help)",
    isNew: false,
  },
  {
    icon: <Shield className="h-5 w-5 text-red-600" />,
    title: "Anomaly Detection",
    description: "Automatic issue detection and fixes",
    isNew: true,
  },
];

const QUICK_STATS = [
  { label: "AI Features", value: "6+", icon: <Sparkles className="h-4 w-4" /> },
  { label: "Templates", value: "20+", icon: <LayoutGrid className="h-4 w-4" /> },
  { label: "Shortcuts", value: "30+", icon: <Keyboard className="h-4 w-4" /> },
];

export function WelcomeDialog({
  open,
  onOpenChange,
  onStartTour,
  onSkipTour,
  userName,
  isNewUser = true,
  version = "2.0",
}: WelcomeDialogProps) {
  const [dontShowAgain, setDontShowAgain] = useState(false);

  const handleStartTour = () => {
    if (dontShowAgain) {
      localStorage.setItem("hideWelcomeDialog", "true");
    }
    onOpenChange(false);
    onStartTour();
  };

  const handleSkip = () => {
    if (dontShowAgain) {
      localStorage.setItem("hideWelcomeDialog", "true");
    }
    onOpenChange(false);
    onSkipTour();
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl">
        <DialogHeader className="text-center pb-2">
          <div className="mx-auto w-16 h-16 rounded-full bg-gradient-to-br from-primary to-purple-600 flex items-center justify-center mb-4">
            <Rocket className="h-8 w-8 text-white" />
          </div>
          <DialogTitle className="text-2xl">
            {isNewUser
              ? `Welcome${userName ? `, ${userName}` : ""}!`
              : `What's New in V${version}`}
          </DialogTitle>
          <DialogDescription className="text-base">
            {isNewUser
              ? "DataBridge AI helps you build and manage financial hierarchies with the power of AI."
              : "We've added powerful new AI features to supercharge your workflow."}
          </DialogDescription>
        </DialogHeader>

        {/* Quick Stats */}
        <div className="grid grid-cols-3 gap-4 py-4">
          {QUICK_STATS.map((stat) => (
            <div
              key={stat.label}
              className="text-center p-3 rounded-lg bg-muted/50"
            >
              <div className="flex items-center justify-center text-primary mb-1">
                {stat.icon}
              </div>
              <div className="text-2xl font-bold">{stat.value}</div>
              <div className="text-xs text-muted-foreground">{stat.label}</div>
            </div>
          ))}
        </div>

        <Separator />

        {/* New Features Grid */}
        <div className="py-4">
          <h3 className="font-semibold mb-3 flex items-center gap-2">
            <Star className="h-4 w-4 text-yellow-500" />
            {isNewUser ? "Key Features" : "New in This Version"}
          </h3>
          <div className="grid grid-cols-2 gap-3">
            {NEW_FEATURES.map((feature) => (
              <div
                key={feature.title}
                className="flex items-start gap-3 p-3 rounded-lg border bg-card hover:border-primary/50 transition-colors"
              >
                <div className="p-2 rounded-lg bg-muted shrink-0">
                  {feature.icon}
                </div>
                <div className="min-w-0">
                  <div className="flex items-center gap-2">
                    <span className="font-medium text-sm">{feature.title}</span>
                    {feature.isNew && (
                      <Badge variant="default" className="text-xs px-1.5 py-0">
                        New
                      </Badge>
                    )}
                  </div>
                  <p className="text-xs text-muted-foreground mt-0.5">
                    {feature.description}
                  </p>
                </div>
              </div>
            ))}
          </div>
        </div>

        <Separator />

        {/* Action Section */}
        <div className="py-4 space-y-4">
          <div className="bg-primary/5 border border-primary/20 rounded-lg p-4">
            <div className="flex items-start gap-3">
              <div className="p-2 rounded-full bg-primary/10">
                <Play className="h-5 w-5 text-primary" />
              </div>
              <div className="flex-1">
                <h4 className="font-medium">Take a Quick Tour</h4>
                <p className="text-sm text-muted-foreground mt-1">
                  Learn the key features in just 2 minutes with our interactive guide.
                </p>
              </div>
            </div>
          </div>

          <div className="flex items-center gap-2">
            <Checkbox
              id="dontShow"
              checked={dontShowAgain}
              onCheckedChange={(checked) => setDontShowAgain(checked as boolean)}
            />
            <Label htmlFor="dontShow" className="text-sm text-muted-foreground">
              Don't show this again
            </Label>
          </div>
        </div>

        <DialogFooter className="gap-2 sm:gap-0">
          <Button variant="ghost" onClick={handleSkip}>
            Skip for Now
          </Button>
          <Button onClick={handleStartTour} className="gap-2">
            <Play className="h-4 w-4" />
            Start Tour
            <ArrowRight className="h-4 w-4" />
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

// Hook to manage welcome dialog state
export function useWelcomeDialog() {
  const [isOpen, setIsOpen] = useState(false);

  const checkShouldShow = () => {
    const hideWelcome = localStorage.getItem("hideWelcomeDialog");
    const lastVersion = localStorage.getItem("lastSeenVersion");
    const currentVersion = "2.0";

    if (hideWelcome === "true" && lastVersion === currentVersion) {
      return false;
    }

    localStorage.setItem("lastSeenVersion", currentVersion);
    return true;
  };

  const showWelcome = () => {
    if (checkShouldShow()) {
      setIsOpen(true);
    }
  };

  const hideWelcome = () => {
    setIsOpen(false);
  };

  const resetWelcome = () => {
    localStorage.removeItem("hideWelcomeDialog");
    localStorage.removeItem("lastSeenVersion");
  };

  return {
    isOpen,
    setIsOpen,
    showWelcome,
    hideWelcome,
    resetWelcome,
    checkShouldShow,
  };
}
