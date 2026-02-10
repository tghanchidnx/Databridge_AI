import { Navigate, useLocation } from "react-router-dom";
import { useAuthStore } from "@/stores/auth.store";
import { useOrganizationStore } from "@/stores/organization.store";

interface ProtectedRouteProps {
  children: React.ReactNode;
  requireOrganization?: boolean;
}

export function ProtectedRoute({
  children,
  requireOrganization = true,
}: ProtectedRouteProps) {
  const { isAuthenticated, user, token } = useAuthStore();
  const { currentOrganization } = useOrganizationStore();
  const location = useLocation();

  // Check token validity
  const tokenExpired = token ? isTokenExpired(token) : true;

  // If not authenticated or token is invalid/expired
  if (!isAuthenticated || !token || tokenExpired) {
    return <Navigate to="/auth" state={{ from: location.pathname }} replace />;
  }

  // If organization is required but user hasn't completed onboarding
  if (requireOrganization) {
    // Check if user has organizationId AND onboardingCompleted flag
    if (!user?.organizationId || !user?.onboardingCompleted) {
      return <Navigate to="/onboarding" replace />;
    }
  }

  return <>{children}</>;
}

function isTokenExpired(token: string): boolean {
  try {
    const payload = JSON.parse(atob(token.split(".")[1]));
    const exp = payload.exp * 1000; // Convert to milliseconds
    return Date.now() >= exp;
  } catch {
    return true;
  }
}
