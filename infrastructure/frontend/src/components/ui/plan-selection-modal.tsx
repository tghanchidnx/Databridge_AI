import { useState } from "react";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Check, Database, Lightning, Rocket } from "@phosphor-icons/react";
import { cn } from "@/lib/utils";

interface PlanSelectionModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  currentPlan: "free" | "pro" | "enterprise";
  onSelectPlan: (plan: "free" | "pro" | "enterprise") => Promise<void>;
  loading?: boolean;
}

export function PlanSelectionModal({
  open,
  onOpenChange,
  currentPlan,
  onSelectPlan,
  loading = false,
}: PlanSelectionModalProps) {
  const [selectedPlan, setSelectedPlan] = useState<
    "free" | "pro" | "enterprise"
  >(currentPlan);

  const pricingPlans = [
    {
      id: "free" as const,
      name: "Free",
      price: "$0",
      period: "forever",
      icon: Database,
      description: "Perfect for getting started",
      features: [
        "Up to 2 database connections",
        "10 schema comparisons/month",
        "Basic query builder",
        "Community support",
        "1 team member",
      ],
      popular: false,
    },
    {
      id: "pro" as const,
      name: "Pro",
      price: "$49",
      period: "per month",
      icon: Lightning,
      description: "For growing teams",
      features: [
        "Unlimited database connections",
        "Unlimited schema comparisons",
        "AI-powered query builder",
        "Priority support",
        "Up to 10 team members",
        "GitHub integration",
        "Advanced reporting",
      ],
      popular: true,
    },
    {
      id: "enterprise" as const,
      name: "Enterprise",
      price: "$99",
      period: "per month",
      icon: Rocket,
      description: "For large organizations",
      features: [
        "Everything in Pro",
        "Unlimited team members",
        "SSO & advanced security",
        "Custom integrations",
        "Dedicated support",
        "SLA guarantee",
        "Custom training",
        "API access",
      ],
      popular: false,
    },
  ];

  const handleConfirm = async () => {
    if (selectedPlan !== currentPlan) {
      await onSelectPlan(selectedPlan);
    }
    onOpenChange(false);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-[85vw] lg:max-w-6xl max-h-[90vh] overflow-y-auto p-6">
        <DialogHeader className="space-y-3 pb-4">
          <DialogTitle className="text-3xl font-bold">
            Choose Your Plan
          </DialogTitle>
          <DialogDescription className="text-base">
            Select the plan that best fits your team's needs
          </DialogDescription>
        </DialogHeader>

        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6 mt-6">
          {pricingPlans.map((plan) => {
            const Icon = plan.icon;
            const isSelected = selectedPlan === plan.id;
            const isCurrent = currentPlan === plan.id;

            return (
              <Card
                key={plan.id}
                className={cn(
                  "relative cursor-pointer transition-all hover:shadow-xl border-2",
                  isSelected &&
                    "ring-4 ring-primary/20 shadow-xl border-primary",
                  !isSelected && "hover:border-primary/50",
                  plan.popular && !isSelected && "border-primary/50"
                )}
                onClick={() => setSelectedPlan(plan.id)}
              >
                {plan.popular && (
                  <div className="absolute -top-4 left-1/2 -translate-x-1/2 z-10">
                    <span className="bg-primary text-primary-foreground text-sm font-bold px-4 py-1.5 rounded-full shadow-lg">
                      Most Popular
                    </span>
                  </div>
                )}

                {isCurrent && (
                  <div className="absolute -top-4 right-4 z-10">
                    <span className="bg-primary text-primary-foreground text-sm font-bold px-4 py-1.5 rounded-full shadow-lg">
                      Current Plan
                    </span>
                  </div>
                )}

                <CardContent className="p-6 space-y-5 flex flex-col h-full">
                  <div className="space-y-3">
                    <div className="w-12 h-12 bg-primary/10 rounded-xl flex items-center justify-center">
                      <Icon className="w-6 h-6 text-primary" weight="duotone" />
                    </div>
                    <h3 className="text-2xl font-bold">{plan.name}</h3>
                    <p className="text-sm text-muted-foreground leading-relaxed">
                      {plan.description}
                    </p>
                  </div>

                  <div className="space-y-1 py-2">
                    <div className="flex items-baseline gap-1">
                      <span className="text-3xl font-bold">{plan.price}</span>
                      <span className="text-muted-foreground text-sm">
                        /{plan.period}
                      </span>
                    </div>
                  </div>

                  <div className="space-y-2.5 pt-3 border-t flex-1">
                    {plan.features.map((feature, idx) => (
                      <div key={idx} className="flex items-start gap-2.5">
                        <Check
                          className="w-4 h-4 text-primary shrink-0 mt-0.5"
                          weight="bold"
                        />
                        <span className="text-sm leading-snug">{feature}</span>
                      </div>
                    ))}
                  </div>

                  <Button
                    className="w-full mt-4"
                    variant={isSelected || isCurrent ? "default" : "outline"}
                    size="lg"
                    type="button"
                  >
                    {isCurrent && !isSelected
                      ? "Current Plan"
                      : isSelected
                      ? "Select Plan"
                      : "Select Plan"}
                  </Button>
                </CardContent>
              </Card>
            );
          })}
        </div>

        <div className="flex justify-end gap-3 mt-8 pt-6 border-t">
          <Button
            variant="outline"
            onClick={() => onOpenChange(false)}
            disabled={loading}
            size="lg"
          >
            Cancel
          </Button>
          <Button
            onClick={handleConfirm}
            disabled={loading || selectedPlan === currentPlan}
            size="lg"
          >
            {loading
              ? "Updating..."
              : selectedPlan === currentPlan
              ? "Already on this plan"
              : "Confirm Change"}
          </Button>
        </div>
      </DialogContent>
    </Dialog>
  );
}
