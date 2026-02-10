import { useState, FormEvent, useEffect } from "react";
import { useNavigate } from "react-router-dom";
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
  MicrosoftOutlookLogo,
  Snowflake,
} from "@phosphor-icons/react";
import { toast } from "sonner";
import logo from "@/assets/logo.png";
import { snowflakeAPI } from "@/lib/snowflake-api";

export function LoginView() {
  const navigate = useNavigate();
  const {
    loginWithMicrosoft,
    loginWithEmail,
    isLoading: authLoading,
    user,
    error: authError,
  } = useAuthStore();
  const { loadOrganizations } = useOrganizationStore();
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  const [loginError, setLoginError] = useState<string | null>(null);

  // Redirect if already logged in
  useEffect(() => {
    console.log("LoginView: user state changed", {
      user,
      organizationId: user?.organizationId,
      onboardingCompleted: user?.onboardingCompleted,
    });

    if (user) {
      // Check if user has completed onboarding (has organization)
      if (user.organizationId && user.onboardingCompleted) {
        console.log("LoginView: Redirecting to dashboard");
        loadOrganizations();
        navigate("/dashboard", { replace: true });
      } else {
        // User needs to complete onboarding
        console.log("LoginView: Redirecting to onboarding");
        navigate("/onboarding", { replace: true });
      }
    }
  }, [user, navigate, loadOrganizations]);

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setIsLoading(true);
    setLoginError(null); // Clear previous errors

    try {
      await loginWithEmail(email, password);
      // Navigation will be handled by useEffect when user state updates
    } catch (error: any) {
      // Set error message to display on screen
      const errorMsg =
        error.response?.data?.message ||
        error.message ||
        "Invalid username or password";
      setLoginError(errorMsg);
      console.error("Login error:", error);
    } finally {
      setIsLoading(false);
    }
  };

  const handleMicrosoftLogin = async () => {
    try {
      await loginWithMicrosoft();

      // Navigation will be handled by useEffect when user state updates
    } catch (error: any) {
      // Error already handled in store
      console.error("Login error:", error);
    }
  };

  const handleSnowflakeLogin = async () => {
    console.log("🔵 Snowflake login button clicked");
    try {
      setIsLoading(true);
      const snowflakeAccount =
        import.meta.env.VITE_SNOWFLAKE_ACCOUNT ||
        "zf07542.south-central-us.azure";

      console.log(
        "🔵 Initiating Snowflake SSO with account:",
        snowflakeAccount
      );

      const response = await snowflakeAPI.initSSO({
        snowflakeAccount,
      });

      console.log("🔵 Full response:", response);

      // Handle nested response structure from backend
      const authUrl =
        (response as any).data?.authorizationUrl || response.authorizationUrl;

      if (!authUrl) {
        console.error("❌ No authorizationUrl in response!");
        toast.error("Invalid response from server");
        setIsLoading(false);
        return;
      }

      console.log("🔵 Redirecting to:", authUrl);

      // Redirect to Snowflake OAuth
      window.location.href = authUrl;
    } catch (error: any) {
      console.error("❌ Snowflake SSO error:", error);
      toast.error(error.message || "Failed to initiate Snowflake login");
      setIsLoading(false);
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
            <CardTitle className="text-2xl font-bold">Welcome back</CardTitle>
            <CardDescription className="mt-2">
              Sign in to your Data Amplifier account
            </CardDescription>
          </div>
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="grid gap-3">
            <Button
              variant="outline"
              onClick={handleMicrosoftLogin}
              disabled={authLoading || isLoading}
              className="w-full gap-2"
            >
              <MicrosoftOutlookLogo className="h-5 w-5" weight="fill" />
              {authLoading ? "Connecting..." : "Continue with Microsoft"}
            </Button>

            <Button
              variant="outline"
              onClick={handleSnowflakeLogin}
              disabled={authLoading || isLoading}
              className="w-full gap-2"
            >
              <Snowflake className="h-5 w-5" weight="fill" />
              {isLoading ? "Connecting..." : "Continue with Snowflake"}
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
              <Label htmlFor="email">Email</Label>
              <div className="relative">
                <EnvelopeSimple className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                <Input
                  id="email"
                  type="email"
                  placeholder="john.doe@acmecorp.com"
                  value={email}
                  onChange={(e) => {
                    setEmail(e.target.value);
                    setLoginError(null); // Clear error when user types
                  }}
                  className="pl-10"
                  required
                />
              </div>
            </div>

            <div className="space-y-2">
              <div className="flex items-center justify-between">
                <Label htmlFor="password">Password</Label>
                <button
                  type="button"
                  className="text-xs text-primary hover:underline"
                >
                  Forgot password?
                </button>
              </div>
              <div className="relative">
                <Lock className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                <Input
                  id="password"
                  type={showPassword ? "text" : "password"}
                  placeholder="Enter your password"
                  value={password}
                  onChange={(e) => {
                    setPassword(e.target.value);
                    setLoginError(null); // Clear error when user types
                  }}
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
            </div>

            {loginError && (
              <div className="rounded-md bg-destructive/10 border border-destructive/20 p-3 text-sm text-destructive flex items-start gap-2">
                <p className="flex-1 gap-3 flex justify-center items-center align-middle">
                  <span className="text-base mt-[-5px]">⚠️</span>
                  {loginError}
                </p>
              </div>
            )}

            <Button
              type="submit"
              className="w-full"
              disabled={isLoading || authLoading}
            >
              {isLoading ? "Signing in..." : "Sign in"}
            </Button>
          </form>

          <div className="text-center">
            <p className="text-sm text-muted-foreground">
              Don't have an account?{" "}
              <button
                onClick={() => navigate("/signup")}
                className="text-primary hover:underline font-medium"
              >
                Create account
              </button>
            </p>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}
