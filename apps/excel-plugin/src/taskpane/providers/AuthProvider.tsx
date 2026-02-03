/**
 * Authentication Provider
 *
 * Provides authentication state and methods to all components.
 */

import React, { createContext, useContext, useState, useEffect, useCallback, ReactNode } from 'react';
import { authService, AuthState } from '../../services/auth.service';

interface AuthContextValue extends AuthState {
  loginWithApiKey: (apiKey: string) => Promise<boolean>;
  loginWithJwt: (email: string, password: string) => Promise<boolean>;
  logout: () => void;
  isLoading: boolean;
}

const AuthContext = createContext<AuthContextValue | null>(null);

export function useAuth(): AuthContextValue {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}

interface AuthProviderProps {
  children: ReactNode;
}

export function AuthProvider({ children }: AuthProviderProps): JSX.Element {
  const [state, setState] = useState<AuthState>(authService.getState());
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    // Subscribe to auth state changes
    const unsubscribe = authService.subscribe((newState) => {
      setState(newState);
    });

    return () => unsubscribe();
  }, []);

  const loginWithApiKey = useCallback(async (apiKey: string): Promise<boolean> => {
    setIsLoading(true);
    try {
      return await authService.loginWithApiKey({ apiKey });
    } finally {
      setIsLoading(false);
    }
  }, []);

  const loginWithJwt = useCallback(async (email: string, password: string): Promise<boolean> => {
    setIsLoading(true);
    try {
      return await authService.loginWithJwt({ email, password });
    } finally {
      setIsLoading(false);
    }
  }, []);

  const logout = useCallback(() => {
    authService.logout();
  }, []);

  const value: AuthContextValue = {
    ...state,
    loginWithApiKey,
    loginWithJwt,
    logout,
    isLoading,
  };

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export default AuthProvider;
