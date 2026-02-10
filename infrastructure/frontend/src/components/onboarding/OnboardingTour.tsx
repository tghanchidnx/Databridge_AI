/**
 * Onboarding Tour System
 * Interactive step-by-step tour with tooltips for new users
 */
import { useState, useEffect, useCallback, createContext, useContext, ReactNode } from "react";
import { cn } from "@/lib/utils";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import {
  X,
  ChevronLeft,
  ChevronRight,
  SkipForward,
  Check,
  Sparkles,
  GitBranch,
  Database,
  Calculator,
  MessageSquare,
  Wand2,
  LayoutGrid,
  AlertTriangle,
  Keyboard,
  FileUp,
  BarChart3,
  Play,
  Pause,
  RotateCcw,
} from "lucide-react";

export interface TourStep {
  id: string;
  title: string;
  description: string;
  content?: ReactNode;
  target?: string; // CSS selector for element to highlight
  position?: "top" | "bottom" | "left" | "right" | "center";
  spotlightPadding?: number;
  action?: {
    label: string;
    onClick: () => void;
  };
  icon?: ReactNode;
  category?: string;
}

interface OnboardingTourContextType {
  isActive: boolean;
  currentStep: number;
  steps: TourStep[];
  startTour: (steps?: TourStep[]) => void;
  endTour: () => void;
  nextStep: () => void;
  prevStep: () => void;
  goToStep: (index: number) => void;
  skipTour: () => void;
  pauseTour: () => void;
  resumeTour: () => void;
  isPaused: boolean;
}

const OnboardingTourContext = createContext<OnboardingTourContextType | null>(null);

export function useOnboardingTour() {
  const context = useContext(OnboardingTourContext);
  if (!context) {
    throw new Error("useOnboardingTour must be used within OnboardingTourProvider");
  }
  return context;
}

// Default tour steps for the application
export const DEFAULT_TOUR_STEPS: TourStep[] = [
  {
    id: "welcome",
    title: "Welcome to DataBridge AI V2!",
    description: "Let's take a quick tour of the new features and improvements. This will only take a few minutes.",
    position: "center",
    icon: <Sparkles className="h-8 w-8 text-primary" />,
    category: "Getting Started",
  },
  {
    id: "hierarchy-tree",
    title: "Hierarchy Tree View",
    description: "This is your main workspace. Build and manage your financial hierarchies here. Drag nodes to reorder, right-click for actions.",
    target: "[data-tour='hierarchy-tree']",
    position: "right",
    icon: <GitBranch className="h-6 w-6" />,
    category: "Navigation",
  },
  {
    id: "ai-suggestions",
    title: "AI-Powered Suggestions",
    description: "Our new AI engine suggests source mappings based on your hierarchy names and patterns. Look for the sparkle icon!",
    target: "[data-tour='ai-suggestions']",
    position: "left",
    icon: <Wand2 className="h-6 w-6 text-purple-600" />,
    category: "AI Features",
  },
  {
    id: "mapping-panel",
    title: "Smart Mapping Panel",
    description: "Configure source mappings with intelligent auto-complete. The AI learns from your choices to improve future suggestions.",
    target: "[data-tour='mapping-panel']",
    position: "left",
    icon: <Database className="h-6 w-6 text-green-600" />,
    category: "Mapping",
  },
  {
    id: "formula-builder",
    title: "Formula Auto-Suggest",
    description: "Create calculated fields with AI-suggested formulas. Common patterns like Gross Profit, Operating Income are auto-detected.",
    target: "[data-tour='formula-panel']",
    position: "left",
    icon: <Calculator className="h-6 w-6 text-blue-600" />,
    category: "Formulas",
  },
  {
    id: "keyboard-shortcuts",
    title: "Keyboard Shortcuts",
    description: "Work faster with keyboard shortcuts. Press '?' anytime to see all available shortcuts.",
    position: "center",
    icon: <Keyboard className="h-6 w-6" />,
    category: "Productivity",
    content: (
      <div className="grid grid-cols-2 gap-2 mt-4 text-sm">
        <div className="flex items-center gap-2">
          <kbd className="px-2 py-1 bg-muted rounded text-xs">↑↓</kbd>
          <span>Navigate</span>
        </div>
        <div className="flex items-center gap-2">
          <kbd className="px-2 py-1 bg-muted rounded text-xs">Enter</kbd>
          <span>Select/Edit</span>
        </div>
        <div className="flex items-center gap-2">
          <kbd className="px-2 py-1 bg-muted rounded text-xs">Ctrl+N</kbd>
          <span>New Node</span>
        </div>
        <div className="flex items-center gap-2">
          <kbd className="px-2 py-1 bg-muted rounded text-xs">Ctrl+S</kbd>
          <span>Save</span>
        </div>
        <div className="flex items-center gap-2">
          <kbd className="px-2 py-1 bg-muted rounded text-xs">Del</kbd>
          <span>Delete</span>
        </div>
        <div className="flex items-center gap-2">
          <kbd className="px-2 py-1 bg-muted rounded text-xs">?</kbd>
          <span>Help</span>
        </div>
      </div>
    ),
  },
  {
    id: "ai-chat",
    title: "AI Chat Assistant",
    description: "Need help? Chat with our AI assistant. Ask questions, request actions, or get suggestions in natural language.",
    target: "[data-tour='ai-chat']",
    position: "left",
    icon: <MessageSquare className="h-6 w-6 text-primary" />,
    category: "AI Features",
  },
  {
    id: "smart-import",
    title: "Smart CSV Import",
    description: "Import hierarchies from CSV with intelligent format detection, validation, and side-by-side diff preview.",
    target: "[data-tour='import-button']",
    position: "bottom",
    icon: <FileUp className="h-6 w-6 text-orange-600" />,
    category: "Import/Export",
  },
  {
    id: "anomaly-detection",
    title: "Anomaly Detection",
    description: "Automatic detection of mapping issues, missing formulas, and inconsistencies. Fix them with one click.",
    target: "[data-tour='anomaly-panel']",
    position: "left",
    icon: <AlertTriangle className="h-6 w-6 text-yellow-600" />,
    category: "Validation",
  },
  {
    id: "template-gallery",
    title: "Template Gallery",
    description: "Start faster with pre-built templates for P&L, Balance Sheet, and industry-specific hierarchies.",
    target: "[data-tour='template-button']",
    position: "bottom",
    icon: <LayoutGrid className="h-6 w-6 text-blue-600" />,
    category: "Templates",
  },
  {
    id: "health-dashboard",
    title: "Project Health Dashboard",
    description: "Monitor your project's health score, mapping coverage, and validation status at a glance.",
    target: "[data-tour='health-dashboard']",
    position: "bottom",
    icon: <BarChart3 className="h-6 w-6 text-green-600" />,
    category: "Analytics",
  },
  {
    id: "complete",
    title: "You're All Set!",
    description: "You now know the key features. Explore at your own pace, and remember - press '?' for help anytime!",
    position: "center",
    icon: <Check className="h-8 w-8 text-green-600" />,
    category: "Complete",
  },
];

interface OnboardingTourProviderProps {
  children: ReactNode;
  onComplete?: () => void;
  onSkip?: () => void;
}

export function OnboardingTourProvider({
  children,
  onComplete,
  onSkip,
}: OnboardingTourProviderProps) {
  const [isActive, setIsActive] = useState(false);
  const [isPaused, setIsPaused] = useState(false);
  const [currentStep, setCurrentStep] = useState(0);
  const [steps, setSteps] = useState<TourStep[]>(DEFAULT_TOUR_STEPS);

  const startTour = useCallback((customSteps?: TourStep[]) => {
    if (customSteps) {
      setSteps(customSteps);
    }
    setCurrentStep(0);
    setIsActive(true);
    setIsPaused(false);
  }, []);

  const endTour = useCallback(() => {
    setIsActive(false);
    setCurrentStep(0);
    onComplete?.();
  }, [onComplete]);

  const skipTour = useCallback(() => {
    setIsActive(false);
    setCurrentStep(0);
    onSkip?.();
  }, [onSkip]);

  const nextStep = useCallback(() => {
    if (currentStep < steps.length - 1) {
      setCurrentStep((prev) => prev + 1);
    } else {
      endTour();
    }
  }, [currentStep, steps.length, endTour]);

  const prevStep = useCallback(() => {
    if (currentStep > 0) {
      setCurrentStep((prev) => prev - 1);
    }
  }, [currentStep]);

  const goToStep = useCallback((index: number) => {
    if (index >= 0 && index < steps.length) {
      setCurrentStep(index);
    }
  }, [steps.length]);

  const pauseTour = useCallback(() => {
    setIsPaused(true);
  }, []);

  const resumeTour = useCallback(() => {
    setIsPaused(false);
  }, []);

  return (
    <OnboardingTourContext.Provider
      value={{
        isActive,
        currentStep,
        steps,
        startTour,
        endTour,
        nextStep,
        prevStep,
        goToStep,
        skipTour,
        pauseTour,
        resumeTour,
        isPaused,
      }}
    >
      {children}
      {isActive && !isPaused && <TourOverlay />}
    </OnboardingTourContext.Provider>
  );
}

function TourOverlay() {
  const { currentStep, steps, nextStep, prevStep, skipTour, endTour } = useOnboardingTour();
  const step = steps[currentStep];
  const progress = ((currentStep + 1) / steps.length) * 100;
  const isFirstStep = currentStep === 0;
  const isLastStep = currentStep === steps.length - 1;

  const [targetRect, setTargetRect] = useState<DOMRect | null>(null);

  useEffect(() => {
    if (step.target) {
      const element = document.querySelector(step.target);
      if (element) {
        const rect = element.getBoundingClientRect();
        setTargetRect(rect);
        element.scrollIntoView({ behavior: "smooth", block: "center" });
      } else {
        setTargetRect(null);
      }
    } else {
      setTargetRect(null);
    }
  }, [step.target]);

  // Calculate tooltip position
  const getTooltipPosition = () => {
    if (!targetRect || step.position === "center") {
      return {
        top: "50%",
        left: "50%",
        transform: "translate(-50%, -50%)",
      };
    }

    const padding = step.spotlightPadding || 12;
    const tooltipWidth = 400;
    const tooltipHeight = 300;

    switch (step.position) {
      case "top":
        return {
          top: `${targetRect.top - tooltipHeight - padding}px`,
          left: `${targetRect.left + targetRect.width / 2}px`,
          transform: "translateX(-50%)",
        };
      case "bottom":
        return {
          top: `${targetRect.bottom + padding}px`,
          left: `${targetRect.left + targetRect.width / 2}px`,
          transform: "translateX(-50%)",
        };
      case "left":
        return {
          top: `${targetRect.top + targetRect.height / 2}px`,
          left: `${targetRect.left - tooltipWidth - padding}px`,
          transform: "translateY(-50%)",
        };
      case "right":
        return {
          top: `${targetRect.top + targetRect.height / 2}px`,
          left: `${targetRect.right + padding}px`,
          transform: "translateY(-50%)",
        };
      default:
        return {
          top: "50%",
          left: "50%",
          transform: "translate(-50%, -50%)",
        };
    }
  };

  return (
    <div className="fixed inset-0 z-[100]">
      {/* Backdrop with spotlight hole */}
      <div className="absolute inset-0 bg-black/70">
        {targetRect && (
          <div
            className="absolute bg-transparent rounded-lg ring-4 ring-primary ring-offset-4 ring-offset-transparent"
            style={{
              top: targetRect.top - 8,
              left: targetRect.left - 8,
              width: targetRect.width + 16,
              height: targetRect.height + 16,
              boxShadow: "0 0 0 9999px rgba(0, 0, 0, 0.7)",
            }}
          />
        )}
      </div>

      {/* Tooltip Card */}
      <Card
        className="fixed w-[400px] max-w-[90vw] z-[101] shadow-2xl animate-in fade-in-0 zoom-in-95 duration-200"
        style={getTooltipPosition()}
      >
        <CardHeader className="pb-3">
          <div className="flex items-start justify-between">
            <div className="flex items-center gap-3">
              {step.icon && (
                <div className="p-2 rounded-lg bg-primary/10">{step.icon}</div>
              )}
              <div>
                {step.category && (
                  <Badge variant="secondary" className="text-xs mb-1">
                    {step.category}
                  </Badge>
                )}
                <CardTitle className="text-lg">{step.title}</CardTitle>
              </div>
            </div>
            <Button size="icon" variant="ghost" onClick={skipTour} className="h-8 w-8">
              <X className="h-4 w-4" />
            </Button>
          </div>
        </CardHeader>

        <CardContent className="space-y-4">
          <CardDescription className="text-sm leading-relaxed">
            {step.description}
          </CardDescription>

          {step.content}

          {step.action && (
            <Button
              variant="outline"
              size="sm"
              onClick={step.action.onClick}
              className="w-full"
            >
              {step.action.label}
            </Button>
          )}

          {/* Progress */}
          <div className="space-y-2">
            <div className="flex items-center justify-between text-xs text-muted-foreground">
              <span>
                Step {currentStep + 1} of {steps.length}
              </span>
              <span>{Math.round(progress)}% complete</span>
            </div>
            <Progress value={progress} className="h-1" />
          </div>

          {/* Navigation */}
          <div className="flex items-center justify-between pt-2">
            <Button
              variant="ghost"
              size="sm"
              onClick={prevStep}
              disabled={isFirstStep}
              className="gap-1"
            >
              <ChevronLeft className="h-4 w-4" />
              Back
            </Button>

            <div className="flex items-center gap-1">
              {steps.map((_, index) => (
                <StepIndicator
                  key={index}
                  index={index}
                  currentStep={currentStep}
                />
              ))}
            </div>

            <Button size="sm" onClick={nextStep} className="gap-1">
              {isLastStep ? (
                <>
                  Finish
                  <Check className="h-4 w-4" />
                </>
              ) : (
                <>
                  Next
                  <ChevronRight className="h-4 w-4" />
                </>
              )}
            </Button>
          </div>

          {/* Skip link */}
          {!isLastStep && (
            <button
              onClick={skipTour}
              className="text-xs text-muted-foreground hover:text-foreground w-full text-center"
            >
              Skip tour
            </button>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

// Step indicator component (fixes React hooks violation)
function StepIndicator({ index, currentStep }: { index: number; currentStep: number }) {
  const { goToStep } = useOnboardingTour();

  return (
    <button
      onClick={() => goToStep(index)}
      className={cn(
        "w-2 h-2 rounded-full transition-colors",
        index === currentStep
          ? "bg-primary"
          : index < currentStep
          ? "bg-primary/50"
          : "bg-muted"
      )}
    />
  );
}

// Tour trigger button component
export function TourTriggerButton({ className }: { className?: string }) {
  const { startTour, isActive } = useOnboardingTour();

  return (
    <Button
      variant="outline"
      size="sm"
      onClick={() => startTour()}
      disabled={isActive}
      className={cn("gap-2", className)}
    >
      <Play className="h-4 w-4" />
      Start Tour
    </Button>
  );
}
