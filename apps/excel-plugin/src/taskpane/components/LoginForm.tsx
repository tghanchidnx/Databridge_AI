/**
 * Login Form Component
 *
 * Provides authentication UI for API Key or JWT login.
 */

import React, { useState } from 'react';
import {
  Input,
  Button,
  Field,
  Spinner,
  MessageBar,
  MessageBarBody,
  Tab,
  TabList,
} from '@fluentui/react-components';
import { Key24Regular, Person24Regular } from '@fluentui/react-icons';
import { useAuth } from '../providers/AuthProvider';

type AuthMode = 'apiKey' | 'jwt';

export function LoginForm(): JSX.Element {
  const { loginWithApiKey, loginWithJwt, isLoading } = useAuth();
  const [authMode, setAuthMode] = useState<AuthMode>('apiKey');
  const [apiKey, setApiKey] = useState('');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState<string | null>(null);

  const handleApiKeyLogin = async () => {
    setError(null);
    console.log('Login button clicked, API key:', apiKey ? '[HIDDEN]' : '[EMPTY]');

    if (!apiKey.trim()) {
      setError('Please enter an API key');
      return;
    }

    try {
      console.log('Calling loginWithApiKey...');
      const success = await loginWithApiKey(apiKey);
      console.log('loginWithApiKey returned:', success);

      if (!success) {
        setError('Invalid API key or server not reachable');
      }
    } catch (err) {
      console.error('Login error:', err);
      setError('Connection error: ' + (err as Error).message);
    }
  };

  const handleJwtLogin = async () => {
    setError(null);
    if (!email.trim() || !password.trim()) {
      setError('Please enter email and password');
      return;
    }

    const success = await loginWithJwt(email, password);
    if (!success) {
      setError('Invalid credentials');
    }
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (authMode === 'apiKey') {
      handleApiKeyLogin();
    } else {
      handleJwtLogin();
    }
  };

  return (
    <div className="login-container">
      <div className="login-card">
        <div className="login-header">
          <h1>DataBridge AI</h1>
          <p>Connect to start managing hierarchies</p>
        </div>

        <TabList
          selectedValue={authMode}
          onTabSelect={(_, data) => {
            setAuthMode(data.value as AuthMode);
            setError(null);
          }}
          className="login-tabs"
        >
          <Tab value="apiKey" icon={<Key24Regular />}>
            API Key
          </Tab>
          <Tab value="jwt" icon={<Person24Regular />}>
            Sign In
          </Tab>
        </TabList>

        {error && (
          <MessageBar intent="error" className="login-error">
            <MessageBarBody>{error}</MessageBarBody>
          </MessageBar>
        )}

        <form onSubmit={handleSubmit} className="login-form">
          {authMode === 'apiKey' ? (
            <Field label="API Key" required>
              <Input
                type="password"
                value={apiKey}
                onChange={(e, data) => setApiKey(data.value)}
                placeholder="Enter your API key"
                disabled={isLoading}
              />
              <span className="form-hint">
                Use v2-dev-key-1 for development
              </span>
            </Field>
          ) : (
            <>
              <Field label="Email" required>
                <Input
                  type="email"
                  value={email}
                  onChange={(e, data) => setEmail(data.value)}
                  placeholder="Enter your email"
                  disabled={isLoading}
                />
              </Field>
              <Field label="Password" required>
                <Input
                  type="password"
                  value={password}
                  onChange={(e, data) => setPassword(data.value)}
                  placeholder="Enter your password"
                  disabled={isLoading}
                />
              </Field>
            </>
          )}

          <Button
            appearance="primary"
            type="submit"
            disabled={isLoading}
            className="login-button"
          >
            {isLoading ? <Spinner size="tiny" /> : 'Connect'}
          </Button>
        </form>

        <div className="login-footer">
          <p>
            Server: <code>localhost:3002</code>
          </p>
        </div>
      </div>

      <style>{`
        .login-container {
          display: flex;
          align-items: center;
          justify-content: center;
          height: 100%;
          padding: 16px;
          background: linear-gradient(135deg, #f5f5f5 0%, #e8e8e8 100%);
        }

        .login-card {
          background: white;
          border-radius: 12px;
          box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
          padding: 24px;
          width: 100%;
          max-width: 340px;
        }

        .login-header {
          text-align: center;
          margin-bottom: 24px;
        }

        .login-header h1 {
          margin: 0 0 8px 0;
          font-size: 24px;
          color: #0078d4;
        }

        .login-header p {
          margin: 0;
          color: #605e5c;
          font-size: 14px;
        }

        .login-tabs {
          margin-bottom: 16px;
        }

        .login-error {
          margin-bottom: 16px;
        }

        .login-form {
          display: flex;
          flex-direction: column;
          gap: 16px;
        }

        .login-button {
          width: 100%;
          margin-top: 8px;
        }

        .login-footer {
          margin-top: 24px;
          text-align: center;
          font-size: 12px;
          color: #605e5c;
        }

        .login-footer code {
          background: #f3f2f1;
          padding: 2px 6px;
          border-radius: 4px;
        }
      `}</style>
    </div>
  );
}

export default LoginForm;
