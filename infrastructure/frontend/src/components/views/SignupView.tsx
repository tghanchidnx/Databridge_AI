import { useState, FormEvent, useEffect } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { useAuthStore } from "@/stores/auth.store";
import { useOrganizationStore } from "@/stores/organization.store";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import {
  Eye,
  EyeSlash,
  Lock,
  EnvelopeSimple,
  User,
  MicrosoftOutlookLogo,
  Buildings,
} from "@phosphor-icons/react";
import { Checkbox } from "@/components/ui/checkbox";
import { toast } from "sonner";
import logo from "@/assets/logo.png";

export function SignupView() {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const {
    signup,
    loginWithMicrosoft,
    isLoading: authLoading,
    user,
  } = useAuthStore();
  const { loadOrganizations } = useOrganizationStore();
  const [fullName, setFullName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [showConfirmPassword, setShowConfirmPassword] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [agreedToTerms, setAgreedToTerms] = useState(false);
  const [organizationKey, setOrganizationKey] = useState("");
  const [isJoiningOrganization, setIsJoiningOrganization] = useState(false);

  // Check for organization key in URL
  useEffect(() => {
    const orgKey = searchParams.get("orgKey");
    if (orgKey) {
      setOrganizationKey(orgKey);
      setIsJoiningOrganization(true);
      toast.info("You're joining an existing organization!");
    }
  }, [searchParams]);

  // Redirect if already logged in
  useEffect(() => {
    if (user) {
      if (user.organizationId && user.onboardingCompleted) {
        loadOrganizations();
        navigate("/dashboard");
      } else {
        navigate("/onboarding");
      }
    }
  }, [user, navigate, loadOrganizations]);

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();

    if (!fullName.trim()) {
      toast.error("Please enter your full name");
      return;
    }

    if (!email.trim()) {
      toast.error("Please enter your email");
      return;
    }

    if (password.length < 8) {
      toast.error("Password must be at least 8 characters");
      return;
    }

    if (password !== confirmPassword) {
      toast.error("Passwords do not match");
      return;
    }

    if (!agreedToTerms) {
      toast.error("Please agree to the Terms of Service and Privacy Policy");
      return;
    }

    setIsLoading(true);
    try {
      await signup(fullName, email, password, organizationKey || undefined);
      // Navigation will be handled by useEffect when user state updates
    } catch (error) {
      // Error already handled in store
      console.error("Signup error:", error);
    } finally {
      setIsLoading(false);
    }
  };

  const handleMicrosoftSignup = async () => {
    try {
      await loginWithMicrosoft();
      // Navigation will be handled by useEffect when user state updates
    } catch (error: any) {
      // Error already handled in store
      console.error("Signup error:", error);
    }
  };

  return (
    <div className="flex min-h-screen items-center justify-center bg-background p-4">
      <Card className="w-full max-w-md">
        <CardHeader className="space-y-4 text-center">
          <div className="mx-auto flex  items-center justify-center">
            <img src={logo} alt="Data Amplifier" className="h-20 " />
          </div>
          <div>
            <CardTitle className="text-2xl font-bold">
              Create your account
            </CardTitle>
            <CardDescription className="mt-2">
              Get started with Data Amplifier today
            </CardDescription>
          </div>
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="grid gap-3">
            <Button
              variant="outline"
              onClick={handleMicrosoftSignup}
              disabled={authLoading || isLoading}
              className="w-full gap-2"
            >
              <MicrosoftOutlookLogo className="h-5 w-5" weight="fill" />
              {authLoading ? "Connecting..." : "Continue with Microsoft"}
            </Button>
          </div>

          <div className="relative">
            <div className="absolute inset-0 flex items-center">
              <Separator />
            </div>
            <div className="relative flex justify-center text-xs uppercase">
              <span className="bg-card px-2 text-muted-foreground">
                Or continue with email
              </span>
            </div>
          </div>

          <form onSubmit={handleSubmit} className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="fullName">Full Name</Label>
              <div className="relative">
                <User className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                <Input
                  id="fullName"
                  type="text"
                  placeholder="John Doe"
                  value={fullName}
                  onChange={(e) => setFullName(e.target.value)}
                  className="pl-10"
                  required
                />
              </div>
            </div>

            <div className="space-y-2">
              <Label htmlFor="email">Email</Label>
              <div className="relative">
                <EnvelopeSimple className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                <Input
                  id="email"
                  type="email"
                  placeholder="john.doe@acmecorp.com"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  className="pl-10"
                  required
                />
              </div>
            </div>

            {isJoiningOrganization && (
              <div className="rounded-md bg-blue-50 dark:bg-blue-950 border border-blue-200 dark:border-blue-800 p-3">
                <div className="flex items-start gap-2">
                  <Buildings className="h-5 w-5 text-blue-600 dark:text-blue-400 mt-0.5" />
                  <div>
                    <p className="text-sm font-medium text-blue-900 dark:text-blue-100">
                      Joining Organization
                    </p>
                    <p className="text-xs text-blue-700 dark:text-blue-300 mt-1">
                      You're joining an existing organization. Package selection
                      will be managed by your organization owner.
                    </p>
                  </div>
                </div>
              </div>
            )}

            {!isJoiningOrganization && (
              <div className="space-y-2">
                <Label htmlFor="organizationKey">
                  Organization Key{" "}
                  <span className="text-muted-foreground font-normal">
                    (Optional)
                  </span>
                </Label>
                <div className="relative">
                  <Buildings className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                  <Input
                    id="organizationKey"
                    type="text"
                    placeholder="Enter organization key to join existing organization"
                    value={organizationKey}
                    onChange={(e) => setOrganizationKey(e.target.value)}
                    className="pl-10"
                  />
                </div>
                <p className="text-xs text-muted-foreground">
                  Have an organization key? Enter it to join an existing
                  organization
                </p>
              </div>
            )}

            <div className="space-y-2">
              <Label htmlFor="password">Password</Label>
              <div className="relative">
                <Lock className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                <Input
                  id="password"
                  type={showPassword ? "text" : "password"}
                  placeholder="At least 8 characters"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className="pl-10 pr-10"
                  required
                />
                <button
                  type="button"
                  onClick={() => setShowPassword(!showPassword)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                >
                  {showPassword ? (
                    <EyeSlash className="h-4 w-4" />
                  ) : (
                    <Eye className="h-4 w-4" />
                  )}
                </button>
              </div>
              <p className="text-xs text-muted-foreground">
                Must be at least 8 characters long
              </p>
            </div>

            <div className="space-y-2">
              <Label htmlFor="confirmPassword">Confirm Password</Label>
              <div className="relative">
                <Lock className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                <Input
                  id="confirmPassword"
                  type={showConfirmPassword ? "text" : "password"}
                  placeholder="Re-enter your password"
                  value={confirmPassword}
                  onChange={(e) => setConfirmPassword(e.target.value)}
                  className="pl-10 pr-10"
                  required
                />
                <button
                  type="button"
                  onClick={() => setShowConfirmPassword(!showConfirmPassword)}
                  className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                >
                  {showConfirmPassword ? (
                    <EyeSlash className="h-4 w-4" />
                  ) : (
                    <Eye className="h-4 w-4" />
                  )}
                </button>
              </div>
            </div>

            <div className="flex items-start gap-2">
              <input
                type="checkbox"
                id="terms"
                checked={agreedToTerms}
                onChange={(e) => setAgreedToTerms(e.target.checked)}
                className="mt-1 h-4 w-4 rounded border-input"
              />
              <label
                htmlFor="terms"
                className="text-xs text-muted-foreground leading-relaxed"
              >
                I agree to the{" "}
                <button type="button" className="text-primary hover:underline">
                  Terms of Service
                </button>{" "}
                and{" "}
                <button type="button" className="text-primary hover:underline">
                  Privacy Policy
                </button>
              </label>
            </div>

            <Button
              type="submit"
              className="w-full"
              disabled={isLoading || authLoading}
            >
              {isLoading ? "Creating account..." : "Create account"}
            </Button>
          </form>

          <div className="text-center">
            <p className="text-sm text-muted-foreground">
              Already have an account?{" "}
              <button
                type="button"
                onClick={() => navigate("/auth")}
                className="text-primary hover:underline font-medium"
              >
                Sign in
              </button>
            </p>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
