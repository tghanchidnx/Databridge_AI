/**
 * DataBridge AI - Excel Add-in Main App
 */

import React, { useState, useEffect } from 'react';
import {
  FluentProvider,
  webLightTheme,
  Tab,
  TabList,
  Spinner,
  MessageBar,
  MessageBarBody,
  MessageBarTitle,
  Button,
} from '@fluentui/react-components';
import {
  PlugConnected24Regular,
  Database24Regular,
  TableSimple24Regular,
  DocumentSearch24Regular,
  Settings24Regular,
} from '@fluentui/react-icons';

import { AuthProvider, useAuth } from './providers/AuthProvider';
import { DataBridgeProvider, useDataBridge } from './providers/DataBridgeProvider';

// Components
import { LoginForm } from './components/LoginForm';
import { ConnectionManager } from './components/ConnectionManager';
import { DataLoader } from './components/DataLoader';
import { HierarchyMapper } from './components/HierarchyMapper';
import { SettingsPanel } from './components/SettingsPanel';

import './App.css';

type TabValue = 'connections' | 'data' | 'hierarchies' | 'settings';

function AppContent(): JSX.Element {
  const { isAuthenticated, isLoading: authLoading } = useAuth();
  const { isConnected, isLoading: dataLoading, error, clearError } = useDataBridge();
  const [selectedTab, setSelectedTab] = useState<TabValue>('hierarchies');

  // Check for command from ribbon button
  useEffect(() => {
    const command = sessionStorage.getItem('databridge_command');
    if (command) {
      sessionStorage.removeItem('databridge_command');

      switch (command) {
        case 'mapHierarchy':
          setSelectedTab('hierarchies');
          break;
        case 'reconcileSheets':
          setSelectedTab('data');
          break;
        case 'profileData':
          setSelectedTab('data');
          break;
      }
    }
  }, []);

  // Show loading state
  if (authLoading) {
    return (
      <div className="app-loading">
        <Spinner size="large" label="Loading..." />
      </div>
    );
  }

  // Show login form if not authenticated
  if (!isAuthenticated) {
    return <LoginForm />;
  }

  // Main app content
  return (
    <div className="app-container">
      {/* Header */}
      <header className="app-header">
        <div className="header-title">
          <h1>DataBridge AI</h1>
          <span className={`connection-status ${isConnected ? 'connected' : 'disconnected'}`}>
            <PlugConnected24Regular />
            {isConnected ? 'Connected' : 'Disconnected'}
          </span>
        </div>
      </header>

      {/* Error Message */}
      {error && (
        <MessageBar intent="error" className="error-bar">
          <MessageBarBody>
            <MessageBarTitle>Error</MessageBarTitle>
            {error}
          </MessageBarBody>
          <Button appearance="transparent" onClick={clearError}>
            Dismiss
          </Button>
        </MessageBar>
      )}

      {/* Navigation Tabs */}
      <nav className="app-nav">
        <TabList
          selectedValue={selectedTab}
          onTabSelect={(_, data) => setSelectedTab(data.value as TabValue)}
        >
          <Tab value="hierarchies" icon={<TableSimple24Regular />}>
            Hierarchies
          </Tab>
          <Tab value="data" icon={<DocumentSearch24Regular />}>
            Data
          </Tab>
          <Tab value="connections" icon={<Database24Regular />}>
            Connections
          </Tab>
          <Tab value="settings" icon={<Settings24Regular />}>
            Settings
          </Tab>
        </TabList>
      </nav>

      {/* Main Content */}
      <main className="app-content">
        {dataLoading && (
          <div className="content-loading">
            <Spinner size="small" />
          </div>
        )}

        {selectedTab === 'hierarchies' && <HierarchyMapper />}
        {selectedTab === 'data' && <DataLoader />}
        {selectedTab === 'connections' && <ConnectionManager />}
        {selectedTab === 'settings' && <SettingsPanel />}
      </main>

      {/* Footer */}
      <footer className="app-footer">
        <span>DataBridge AI v1.0</span>
      </footer>
    </div>
  );
}

export function App(): JSX.Element {
  return (
    <FluentProvider theme={webLightTheme}>
      <AuthProvider>
        <DataBridgeProvider>
          <AppContent />
        </DataBridgeProvider>
      </AuthProvider>
    </FluentProvider>
  );
}

export default App;
