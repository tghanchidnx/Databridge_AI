/**
 * Settings Panel Component
 *
 * Configure API connection and plugin settings.
 */

import React, { useState, useEffect } from 'react';
import {
  Input,
  Button,
  Field,
  Switch,
  Divider,
  Badge,
} from '@fluentui/react-components';
import {
  Settings24Regular,
  PlugConnected24Regular,
  Person24Regular,
  SignOut24Regular,
} from '@fluentui/react-icons';
import { useAuth } from '../providers/AuthProvider';
import { useDataBridge } from '../providers/DataBridgeProvider';
import { apiService } from '../../services/api.service';

export function SettingsPanel(): JSX.Element {
  const { isAuthenticated, authType, userName, logout } = useAuth();
  const { isConnected, checkConnection } = useDataBridge();

  const [apiUrl, setApiUrl] = useState('');
  const [isSaving, setIsSaving] = useState(false);
  const [saveMessage, setSaveMessage] = useState<string | null>(null);

  useEffect(() => {
    // Load current settings
    const storedUrl = localStorage.getItem('databridge_api_url');
    setApiUrl(storedUrl || 'http://localhost:8001/api');
  }, []);

  const handleSaveUrl = async () => {
    setIsSaving(true);
    setSaveMessage(null);

    try {
      apiService.setBaseUrl(apiUrl);
      const connected = await checkConnection();

      if (connected) {
        setSaveMessage('Settings saved successfully');
      } else {
        setSaveMessage('Settings saved, but could not connect to server');
      }
    } catch (e) {
      setSaveMessage('Failed to save settings');
    } finally {
      setIsSaving(false);
    }
  };

  const handleLogout = () => {
    if (confirm('Are you sure you want to sign out?')) {
      logout();
    }
  };

  return (
    <div className="settings-panel">
      {/* Connection Settings */}
      <div className="card">
        <div className="card-header">
          <h2 className="card-title">
            <PlugConnected24Regular />
            Connection
          </h2>
          <Badge
            appearance="filled"
            color={isConnected ? 'success' : 'danger'}
          >
            {isConnected ? 'Connected' : 'Disconnected'}
          </Badge>
        </div>

        <Field label="API Server URL">
          <Input
            value={apiUrl}
            onChange={(e, data) => setApiUrl(data.value)}
            placeholder="http://localhost:8001/api"
          />
          <span className="form-hint">
            The URL of the DataBridge backend server
          </span>
        </Field>

        <div className="button-group right">
          <Button appearance="secondary" onClick={() => checkConnection()}>
            Test Connection
          </Button>
          <Button
            appearance="primary"
            disabled={isSaving}
            onClick={handleSaveUrl}
          >
            Save
          </Button>
        </div>

        {saveMessage && (
          <p className={`save-message ${isConnected ? 'success' : 'warning'}`}>
            {saveMessage}
          </p>
        )}
      </div>

      {/* Account Info */}
      <div className="card">
        <div className="card-header">
          <h2 className="card-title">
            <Person24Regular />
            Account
          </h2>
        </div>

        {isAuthenticated ? (
          <>
            <div className="account-info">
              <div className="info-row">
                <span className="info-label">Authentication</span>
                <Badge appearance="outline">
                  {authType === 'apiKey' ? 'API Key' : 'JWT Token'}
                </Badge>
              </div>
              {userName && (
                <div className="info-row">
                  <span className="info-label">User</span>
                  <span>{userName}</span>
                </div>
              )}
            </div>

            <Divider />

            <div className="button-group">
              <Button
                appearance="secondary"
                icon={<SignOut24Regular />}
                onClick={handleLogout}
              >
                Sign Out
              </Button>
            </div>
          </>
        ) : (
          <p className="form-hint">Not signed in</p>
        )}
      </div>

      {/* About */}
      <div className="card">
        <div className="card-header">
          <h2 className="card-title">
            <Settings24Regular />
            About
          </h2>
        </div>

        <div className="about-info">
          <p><strong>DataBridge AI Excel Add-in</strong></p>
          <p>Version 1.0.0</p>
          <p className="form-hint">
            Hierarchy Management & Data Reconciliation for Excel
          </p>
        </div>

        <div className="feature-list">
          <h4>Features</h4>
          <ul>
            <li>Map Excel data to hierarchies with AI suggestions</li>
            <li>Profile data quality and detect issues</li>
            <li>Reconcile data between sheets</li>
            <li>Execute database queries and load results</li>
            <li>Connect to Snowflake, PostgreSQL, MySQL</li>
          </ul>
        </div>
      </div>

      <style>{`
        .save-message {
          margin-top: 12px;
          padding: 8px 12px;
          border-radius: 4px;
          font-size: 13px;
        }

        .save-message.success {
          background: #dff6dd;
          color: #107c10;
        }

        .save-message.warning {
          background: #fff4ce;
          color: #797775;
        }

        .account-info {
          margin-bottom: 16px;
        }

        .info-row {
          display: flex;
          justify-content: space-between;
          align-items: center;
          padding: 8px 0;
        }

        .info-label {
          color: #605e5c;
          font-size: 13px;
        }

        .about-info {
          margin-bottom: 16px;
        }

        .about-info p {
          margin: 4px 0;
        }

        .feature-list h4 {
          margin: 0 0 8px 0;
          font-size: 14px;
        }

        .feature-list ul {
          margin: 0;
          padding-left: 20px;
        }

        .feature-list li {
          font-size: 13px;
          color: #605e5c;
          margin-bottom: 4px;
        }
      `}</style>
    </div>
  );
}

export default SettingsPanel;
