import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Input } from "@/components/ui/input";
import { Card, CardContent } from "@/components/ui/card";
import { Progress } from "@/components/ui/progress";
import { Textarea } from "@/components/ui/textarea";
import {
  ArrowRight,
  Check,
  Users,
  Sparkle,
  Database,
  Lightning,
  Rocket,
} from "@phosphor-icons/react";
import { cn } from "@/lib/utils";
import logo from "@/assets/logo.svg";
import { toast } from "sonner";
import { useAuthStore } from "@/stores/auth.store";
import { useOrganizationStore } from "@/stores/organization.store";

export function OnboardingView() {
  const navigate = useNavigate();
  const { user, setUser, token, isAuthenticated } = useAuthStore();
  const { createOrganization, isLoading: orgLoading } = useOrganizationStore();
  const [currentStep, setCurrentStep] = useState(0);
  const [isSubmitting, setIsSubmitting] = useState(false);

  const [organizationName, setOrganizationName] = useState(
    `${user?.name}'s Organization`
  );
  const [teamSize, setTeamSize] = useState("");
  const [primaryUseCase, setPrimaryUseCase] = useState("");
  const [bio, setBio] = useState("");

  // Redirect if not authenticated
  useEffect(() => {
    if (!isAuthenticated || !token) {
      console.log("OnboardingView: Not authenticated, redirecting to login");
      navigate("/auth", { replace: true });
    }
  }, [isAuthenticated, token, navigate]);
  const [selectedPlan, setSelectedPlan] = useState<
    "free" | "pro" | "enterprise"
  >("free");

  // Organization members skip plan selection
  const isOrganizationMember =
    user?.organizationId && !user?.isOrganizationOwner;

  const steps = isOrganizationMember
    ? [
        {
          id: "welcome",
          title: "Welcome",
          description: "Let's get you started",
        },
        {
          id: "profile",
          title: "Profile",
          description: "Tell us about yourself",
        },
        {
          id: "complete",
          title: "Ready",
          description: "Start managing databases",
        },
      ]
    : [
        {
          id: "welcome",
          title: "Welcome",
          description: "Let's get you started",
        },
        {
          id: "profile",
          title: "Profile",
          description: "Tell us about yourself",
        },
        {
          id: "organization",
          title: "Organization",
          description: "Set up your organization",
        },
        {
          id: "plan",
          title: "Plan",
          description: "Choose your plan",
        },
        {
          id: "complete",
          title: "Ready",
          description: "Start managing databases",
        },
      ];

  const progress = ((currentStep + 1) / steps.length) * 100;

  const handleNext = () => {
    if (currentStep < steps.length - 1) {
      setCurrentStep(currentStep + 1);
    }
  };

  const handleComplete = async () => {
    // If user is organization member, skip organization creation
    if (isOrganizationMember) {
      console.log(
        "OnboardingView: Member completing onboarding (no org creation needed)"
      );

      setIsSubmitting(true);
      try {
        // Update user onboardingCompleted flag via API
        // For now, just update local state and navigate
        if (user) {
          const updatedUser = {
            ...user,
            onboardingCompleted: true,
          };
          setUser(updatedUser);
          console.log("OnboardingView: Member onboarding completed");
        }

        toast.success("Welcome to Data Amplifier!");
        navigate("/dashboard");
      } catch (error: any) {
        console.error("Onboarding error:", error);
        toast.error("Failed to complete onboarding");
      } finally {
        setIsSubmitting(false);
      }
      return;
    }

    // Organization owner flow
    if (!organizationName.trim()) {
      toast.error("Please enter an organization name");
      return;
    }

    console.log("OnboardingView: Starting organization creation", {
      hasUser: !!user,
      hasToken: !!token,
      tokenInLocalStorage: !!localStorage.getItem("auth_token"),
      userId: user?.id,
      userEmail: user?.email,
    });

    setIsSubmitting(true);
    try {
      // Create organization via API - backend will automatically update user.organizationId and onboardingCompleted
      const organization = await createOrganization({
        name: organizationName.trim(),
        plan: selectedPlan,
        description: `${teamSize ? teamSize + " team, " : ""}${
          primaryUseCase || "General use"
        }`,
      });

      console.log(
        "OnboardingView: Organization created successfully",
        organization
      );

      // Update local user state to reflect completed onboarding
      if (user) {
        const updatedUser = {
          ...user,
          organizationId: organization.id,
          onboardingCompleted: true,
        };
        setUser(updatedUser);
        console.log("OnboardingView: User state updated", updatedUser);
      }

      toast.success(
        `Welcome to Data Amplifier! You're on the ${selectedPlan} plan.`
      );
      navigate("/dashboard");
    } catch (error: any) {
      console.error("Onboarding error:", error);
      console.error("Error details:", {
        response: error.response?.data,
        status: error.response?.status,
        message: error.message,
      });
      // Error toast already shown in store
    } finally {
      setIsSubmitting(false);
    }
  };

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

  const renderStepContent = () => {
    switch (currentStep) {
      case 0:
        return (
          <div className="text-center space-y-8 max-w-2xl mx-auto">
            <div className="flex justify-center mb-6">
              <img src={logo} alt="Data Amplifier" className="w-24 h-24" />
            </div>
            <h1 className="text-4xl md:text-5xl font-bold">
              Welcome to Data Amplifier
            </h1>
            <p className="text-xl text-muted-foreground max-w-xl mx-auto">
              Streamline your database management with powerful schema
              comparison, report matching, and AI-powered assistance.
            </p>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 pt-8">
              <Card>
                <CardContent className="p-6 text-center space-y-3">
                  <div className="w-12 h-12 bg-primary/10 rounded-lg flex items-center justify-center mx-auto">
                    <Database
                      className="w-6 h-6 text-primary"
                      weight="duotone"
                    />
                  </div>
                  <h3 className="font-semibold">Connect Snowflake</h3>
                  <p className="text-sm text-muted-foreground">
                    Easy integration with multiple databases
                  </p>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="p-6 text-center space-y-3">
                  <div className="w-12 h-12 bg-primary/10 rounded-lg flex items-center justify-center mx-auto">
                    <Users className="w-6 h-6 text-primary" weight="duotone" />
                  </div>
                  <h3 className="font-semibold">Team Collaboration</h3>
                  <p className="text-sm text-muted-foreground">
                    Work together with your data team
                  </p>
                </CardContent>
              </Card>
              <Card>
                <CardContent className="p-6 text-center space-y-3">
                  <div className="w-12 h-12 bg-primary/10 rounded-lg flex items-center justify-center mx-auto">
                    <Sparkle
                      className="w-6 h-6 text-primary"
                      weight="duotone"
                    />
                  </div>
                  <h3 className="font-semibold">AI Assistant</h3>
                  <p className="text-sm text-muted-foreground">
                    Smart suggestions and automation
                  </p>
                </CardContent>
              </Card>
            </div>
          </div>
        );

      case 1:
        return (
          <div className="w-full max-w-md mx-auto space-y-6">
            <div className="text-center space-y-2">
              <h2 className="text-3xl font-bold">Complete Your Profile</h2>
              <p className="text-muted-foreground">
                Help us personalize your experience
              </p>
            </div>
            <div className="space-y-4 pt-4">
              <div className="space-y-2">
                <Label htmlFor="user-name">Full Name</Label>
                <Input
                  id="user-name"
                  value={user?.name || ""}
                  disabled
                  className="text-base bg-muted"
                />
                <p className="text-xs text-muted-foreground">
                  Your name from your authentication provider
                </p>
              </div>
              <div className="space-y-2">
                <Label htmlFor="user-email">Email</Label>
                <Input
                  id="user-email"
                  value={user?.email || ""}
                  disabled
                  className="text-base bg-muted"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="bio">Bio (Optional)</Label>
                <Textarea
                  id="bio"
                  placeholder="Tell us a little about yourself..."
                  value={bio}
                  onChange={(e) => setBio(e.target.value)}
                  rows={4}
                  className="text-base resize-none"
                />
              </div>
            </div>
          </div>
        );

      case 2:
        return (
          <div className="w-full max-w-md mx-auto space-y-6">
            <div className="text-center space-y-2">
              <h2 className="text-3xl font-bold">Set Up Your Organization</h2>
              <p className="text-muted-foreground">
                Tell us about your team and how you'll use Data Amplifier
              </p>
            </div>
            <div className="space-y-4 pt-4">
              <div className="space-y-2">
                <Label htmlFor="organization-name">Organization Name</Label>
                <Input
                  id="organization-name"
                  placeholder="My Company"
                  value={organizationName}
                  onChange={(e) => setOrganizationName(e.target.value)}
                  className="text-base"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="team-size">Team Size</Label>
                <Input
                  id="team-size"
                  placeholder="e.g., 5-10 people"
                  value={teamSize}
                  onChange={(e) => setTeamSize(e.target.value)}
                  className="text-base"
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="use-case">Primary Use Case</Label>
                <Input
                  id="use-case"
                  placeholder="e.g., Schema comparison, Data migration"
                  value={primaryUseCase}
                  onChange={(e) => setPrimaryUseCase(e.target.value)}
                  className="text-base"
                />
              </div>
            </div>
          </div>
        );

      case 3:
        return (
          <div className="w-full max-w-5xl mx-auto space-y-8">
            <div className="text-center space-y-2">
              <h2 className="text-3xl font-bold">Choose Your Plan</h2>
              <p className="text-muted-foreground text-lg">
                Select the plan that best fits your needs
              </p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-6 pt-4">
              {pricingPlans.map((plan) => {
                const Icon = plan.icon;
                return (
                  <Card
                    key={plan.id}
                    className={cn(
                      "relative cursor-pointer transition-all hover:shadow-lg",
                      selectedPlan === plan.id &&
                        "ring-2 ring-primary shadow-lg",
                      plan.popular && "border-primary"
                    )}
                    onClick={() => setSelectedPlan(plan.id)}
                  >
                    {plan.popular && (
                      <div className="absolute -top-3 left-1/2 -translate-x-1/2">
                        <span className="bg-primary text-primary-foreground text-xs font-bold px-3 py-1 rounded-full">
                          Most Popular
                        </span>
                      </div>
                    )}

                    <CardContent className="p-6 space-y-4">
                      <div className="space-y-2">
                        <div className="w-12 h-12 bg-primary/10 rounded-lg flex items-center justify-center">
                          <Icon
                            className="w-6 h-6 text-primary"
                            weight="duotone"
                          />
                        </div>
                        <h3 className="text-2xl font-bold">{plan.name}</h3>
                        <p className="text-sm text-muted-foreground">
                          {plan.description}
                        </p>
                      </div>

                      <div className="space-y-1">
                        <div className="flex items-baseline gap-1">
                          <span className="text-4xl font-bold">
                            {plan.price}
                          </span>
                          <span className="text-muted-foreground text-sm">
                            /{plan.period}
                          </span>
                        </div>
                      </div>

                      <div className="space-y-3 pt-4">
                        {plan.features.map((feature, idx) => (
                          <div key={idx} className="flex items-start gap-2">
                            <Check
                              className="w-5 h-5 text-primary shrink-0 mt-0.5"
                              weight="bold"
                            />
                            <span className="text-sm">{feature}</span>
                          </div>
                        ))}
                      </div>

                      <Button
                        className="w-full mt-4"
                        variant={
                          selectedPlan === plan.id ? "default" : "outline"
                        }
                        type="button"
                      >
                        {selectedPlan === plan.id ? "Selected" : "Select Plan"}
                      </Button>
                    </CardContent>
                  </Card>
                );
              })}
            </div>

            <div className="text-center text-sm text-muted-foreground">
              <p>
                All plans include a 14-day free trial. No credit card required
                to start.
              </p>
            </div>
          </div>
        );

      case 4:
        return (
          <div className="text-center space-y-8 max-w-2xl mx-auto">
            <div className="w-20 h-20 bg-primary rounded-full flex items-center justify-center mx-auto">
              <Check
                className="w-10 h-10 text-primary-foreground"
                weight="bold"
              />
            </div>
            <div className="space-y-2">
              <h2 className="text-4xl font-bold">You're All Set!</h2>
              <p className="text-lg text-muted-foreground max-w-md mx-auto">
                {isOrganizationMember
                  ? "Ready to start collaborating with your organization!"
                  : `Ready to start managing your databases with the ${
                      selectedPlan.charAt(0).toUpperCase() +
                      selectedPlan.slice(1)
                    } plan`}
              </p>
            </div>
            <Card className="bg-muted/50 border max-w-md mx-auto">
              <CardContent className="p-6 space-y-4">
                <h3 className="font-semibold">Quick Start Guide:</h3>
                <ol className="text-left text-sm space-y-2">
                  <li className="flex items-start gap-2">
                    <span className="font-bold text-primary">1.</span>
                    <span>
                      Connect your first database in the Connections panel
                    </span>
                  </li>
                  <li className="flex items-start gap-2">
                    <span className="font-bold text-primary">2.</span>
                    <span>Explore Schema Matcher to compare environments</span>
                  </li>
                  <li className="flex items-start gap-2">
                    <span className="font-bold text-primary">3.</span>
                    <span>Try the AI Assistant for query help</span>
                  </li>
                  {!isOrganizationMember && (
                    <li className="flex items-start gap-2">
                      <span className="font-bold text-primary">4.</span>
                      <span>Invite team members in Settings</span>
                    </li>
                  )}
                </ol>
              </CardContent>
            </Card>
          </div>
        );

      default:
        return null;
    }
  };

  return (
    <div className="min-h-screen bg-background flex flex-col">
      <div className="container mx-auto px-4 py-8 flex-1 flex flex-col">
        <div className="mb-8">
          <Progress value={progress} className="h-2" />
          <div className="flex justify-between mt-4">
            {steps.map((step, index) => (
              <div
                key={step.id}
                className={`flex-1 text-center ${
                  index <= currentStep
                    ? "text-foreground"
                    : "text-muted-foreground"
                }`}
              >
                <div className="text-xs hidden md:block">{step.title}</div>
              </div>
            ))}
          </div>
        </div>

        <div className="flex-1 flex items-center justify-center py-8">
          {renderStepContent()}
        </div>

        <div className="flex items-center justify-between pt-8">
          <div className="flex items-center gap-2">
            <Button
              variant="ghost"
              onClick={() => setCurrentStep(Math.max(0, currentStep - 1))}
              disabled={currentStep === 0 || isSubmitting || orgLoading}
            >
              Back
            </Button>
            <Button
              variant="ghost"
              onClick={handleComplete}
              className="text-muted-foreground"
              disabled={isSubmitting || orgLoading}
            >
              Skip
            </Button>
          </div>

          {currentStep < steps.length - 1 ? (
            <Button
              onClick={handleNext}
              className="gap-2"
              disabled={isSubmitting || orgLoading}
            >
              Next
              <ArrowRight className="w-4 h-4" weight="bold" />
            </Button>
          ) : (
            <Button
              onClick={handleComplete}
              className="gap-2"
              disabled={isSubmitting || orgLoading}
            >
              {isSubmitting || orgLoading ? "Setting up..." : "Get Started"}
              <ArrowRight className="w-4 h-4" weight="bold" />
            </Button>
          )}
        </div>
      </div>
    </div>
  );
}
