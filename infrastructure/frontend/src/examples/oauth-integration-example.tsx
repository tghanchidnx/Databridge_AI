/**
 * Example: Microsoft OAuth Integration in LoginView
 *
 * This example shows how to integrate Microsoft OAuth with the Data Amplifier app.
 * Add this to your LoginView.tsx component.
 */

import { useAuth } from "@/contexts/AuthContext";
import { toast } from "sonner";

// Microsoft OAuth Configuration
const MS_CLIENT_ID = import.meta.env.VITE_MS_CLIENT_ID;
const MS_TENANT_ID = import.meta.env.VITE_MS_TENANT_ID;
const REDIRECT_URI = window.location.origin + "/auth/callback";

// Microsoft OAuth Scopes
const SCOPES = ["openid", "profile", "User.Read", "email"].join(" ");

/**
 * Step 1: Redirect user to Microsoft login
 */
export const initiateOAuthLogin = () => {
  const authUrl = new URL(
    `https://login.microsoftonline.com/${MS_TENANT_ID}/oauth2/v2.0/authorize`
  );

  authUrl.searchParams.append("client_id", MS_CLIENT_ID);
  authUrl.searchParams.append("response_type", "token");
  authUrl.searchParams.append("redirect_uri", REDIRECT_URI);
  authUrl.searchParams.append("scope", SCOPES);
  authUrl.searchParams.append("response_mode", "fragment");
  authUrl.searchParams.append("state", generateRandomState());
  authUrl.searchParams.append("nonce", generateRandomNonce());

  window.location.href = authUrl.toString();
};

/**
 * Step 2: Handle OAuth callback
 */
export const handleOAuthCallback = async () => {
  const { loginWithMicrosoft } = useAuth();

  // Parse hash parameters
  const hash = window.location.hash.substring(1);
  const params = new URLSearchParams(hash);

  const accessToken = params.get("access_token");
  const error = params.get("error");

  if (error) {
    toast.error(`Authentication failed: ${error}`);
    return;
  }

  if (accessToken) {
    try {
      await loginWithMicrosoft(accessToken);
      // Redirect to dashboard after successful login
      window.location.href = "/dashboard";
    } catch (error) {
      console.error("Login failed:", error);
    }
  }
};

/**
 * Example: Login Button Component
 */
export const MicrosoftLoginButton = () => {
  return (
    <button
      onClick={initiateOAuthLogin}
      className="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
    >
      <svg className="w-5 h-5" viewBox="0 0 23 23">
        <path fill="#f3f3f3" d="M0 0h23v23H0z" />
        <path fill="#f35325" d="M1 1h10v10H1z" />
        <path fill="#81bc06" d="M12 1h10v10H12z" />
        <path fill="#05a6f0" d="M1 12h10v10H1z" />
        <path fill="#ffba08" d="M12 12h10v10H12z" />
      </svg>
      Sign in with Microsoft
    </button>
  );
};

/**
 * Example: Snowflake OAuth Integration
 */
const SNOWFLAKE_ACCOUNT = import.meta.env.VITE_SNOWFLAKE_ACCOUNT;
const SNOWFLAKE_CLIENT_ID = import.meta.env.VITE_SNOWFLAKE_CLIENT_ID;

export const initiateSnowflakeLogin = () => {
  const authUrl = new URL(
    `https://${SNOWFLAKE_ACCOUNT}.snowflakecomputing.com/oauth/authorize`
  );

  authUrl.searchParams.append("client_id", SNOWFLAKE_CLIENT_ID);
  authUrl.searchParams.append("response_type", "code");
  authUrl.searchParams.append("redirect_uri", REDIRECT_URI);
  authUrl.searchParams.append(
    "scope",
    "refresh_token session:role:ACCOUNTADMIN"
  );
  authUrl.searchParams.append("state", generateRandomState());

  window.location.href = authUrl.toString();
};

// Utility functions
function generateRandomState(): string {
  return Math.random().toString(36).substring(2, 15);
}

function generateRandomNonce(): string {
  return Math.random().toString(36).substring(2, 15);
}

/**
 * Usage in LoginView.tsx:
 *
 * import { MicrosoftLoginButton, handleOAuthCallback } from './oauth-example'
 *
 * export function LoginView() {
 *   useEffect(() => {
 *     // Check if we're on the callback URL
 *     if (window.location.pathname === '/auth/callback') {
 *       handleOAuthCallback()
 *     }
 *   }, [])
 *
 *   return (
 *     <div>
 *       <MicrosoftLoginButton />
 *     </div>
 *   )
 * }
 */
