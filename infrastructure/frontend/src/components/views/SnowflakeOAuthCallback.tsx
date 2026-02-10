/**
 * Snowflake OAuth Callback Handler
 *
 * This component handles the OAuth callback from Snowflake.
 * It processes the authorization code and sends it back to the parent window.
 *
 * Route: /auth/snowflake-callback
 */

import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { CircleNotch, CheckCircle, Warning } from "@phosphor-icons/react";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  CardDescription,
} from "@/components/ui/card";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { snowflakeAPI, setAuthToken, setUserData } from "@/lib/snowflake-api";
import { useAuthStore } from "@/stores/auth.store";

export function SnowflakeOAuthCallback() {
  const navigate = useNavigate();
  const { setUser, setToken } = useAuthStore();
  const [status, setStatus] = useState<"processing" | "success" | "error">(
    "processing"
  );
  const [message, setMessage] = useState("Authenticating with Snowflake...");

  useEffect(() => {
    const handleCallback = async () => {
      // Parse the OAuth code or error from URL
      const params = new URLSearchParams(window.location.search);
      const code = params.get("code");
      const state = params.get("state");
      const error = params.get("error");
      const errorDescription = params.get("error_description");

      if (error) {
        setStatus("error");
        setMessage(errorDescription || error || "Authentication failed");

        // Send error to parent window if opened as popup
        if (window.opener) {
          window.opener.postMessage(
            {
              type: "snowflake-oauth",
              error: error,
              errorDescription: errorDescription,
            },
            window.location.origin
          );

          setTimeout(() => {
            window.close();
          }, 3000);
        } else {
          // Redirect to login with error
          setTimeout(() => {
            navigate("/login?error=" + encodeURIComponent(error));
          }, 3000);
        }
        return;
      }

      if (code) {
        try {
          // Exchange code for tokens and login user via backend
          const snowflakeAccount =
            import.meta.env.VITE_SNOWFLAKE_ACCOUNT ||
            "zf07542.south-central-us.azure";
          const authResponse = await snowflakeAPI.ssoCallback({
            code,
            snowflakeAccount,
          });

          setStatus("success");
          setMessage("Login successful! Redirecting...");

          // Store auth token and user data in store
          setToken(authResponse.token);
          setUser(authResponse.user);

          // Also store in localStorage as backup
          setAuthToken(authResponse.token);
          setUserData(authResponse.user);

          // If opened as popup, notify parent
          if (window.opener) {
            window.opener.postMessage(
              {
                type: "snowflake-oauth-login",
                success: true,
                user: authResponse.user,
              },
              window.location.origin
            );

            setTimeout(() => {
              window.close();
            }, 1500);
          } else {
            // Redirect based on onboarding status
            setTimeout(() => {
              if (
                authResponse.user.organizationId &&
                authResponse.user.onboardingCompleted
              ) {
                navigate("/dashboard");
              } else {
                navigate("/onboarding");
              }
            }, 1500);
          }
        } catch (err: any) {
          setStatus("error");
          setMessage(err.message || "Failed to complete authentication");

          setTimeout(() => {
            if (window.opener) {
              window.close();
            } else {
              navigate("/login");
            }
          }, 3000);
        }
      } else {
        setStatus("error");
        setMessage("No authorization code received");

        setTimeout(() => {
          if (window.opener) {
            window.close();
          } else {
            navigate("/login");
          }
        }, 3000);
      }
    };

    handleCallback();
    handleCallback();
  }, [navigate, setUser, setToken]);

  return (
    <div className="flex items-center justify-center min-h-screen bg-background p-4">
      <Card className="w-full max-w-md">
        <CardHeader className="text-center">
          <div className="flex justify-center mb-4">
            {status === "processing" && (
              <CircleNotch className="w-12 h-12 animate-spin text-primary" />
            )}
            {status === "success" && (
              <CheckCircle className="w-12 h-12 text-success" />
            )}
            {status === "error" && <Warning className="w-12 h-12 text-error" />}
          </div>
          <CardTitle className="text-2xl">
            {status === "processing" && "Connecting to Snowflake"}
            {status === "success" && "Connection Successful!"}
            {status === "error" && "Connection Failed"}
          </CardTitle>
          <CardDescription>{message}</CardDescription>
        </CardHeader>
        <CardContent>
          {status === "processing" && (
            <div className="text-center text-sm text-muted-foreground">
              <p>Please wait while we complete the authentication.</p>
            </div>
          )}
          {status === "success" && (
            <Alert className="bg-green-50 border-green-200">
              <AlertDescription>
                Your Snowflake login was successful! Redirecting to dashboard...
              </AlertDescription>
            </Alert>
          )}
          {status === "error" && (
            <Alert className="bg-red-50 border-red-200">
              <AlertDescription>
                {message}
                <br />
                Redirecting to login page...
              </AlertDescription>
            </Alert>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

export default SnowflakeOAuthCallback;
