/**
 * Connection Manager Component
 *
 * Manage database connections for data loading.
 */

import React, { useState } from 'react';
import {
  Button,
  Input,
  Field,
  Dropdown,
  Option,
  Spinner,
  Badge,
  MessageBar,
  MessageBarBody,
  MessageBarTitle,
  Link,
} from '@fluentui/react-components';
import {
  Database24Regular,
  CheckmarkCircle24Regular,
  DismissCircle24Regular,
  Add24Regular,
} from '@fluentui/react-icons';
import { useDataBridge } from '../providers/DataBridgeProvider';
import { apiService, ConnectionConfig } from '../../services/api.service';

export function ConnectionManager(): JSX.Element {
  const { connections, refreshConnections, isLoading } = useDataBridge();
  const [testingId, setTestingId] = useState<string | null>(null);
  const [testResults, setTestResults] = useState<Record<string, boolean>>({});
  const [showAddMessage, setShowAddMessage] = useState(false);

  const handleTestConnection = async (connectionId: string) => {
    setTestingId(connectionId);
    try {
      const result = await apiService.testConnection(connectionId);
      setTestResults((prev) => ({
        ...prev,
        [connectionId]: result.success && result.data?.connected === true,
      }));
    } catch {
      setTestResults((prev) => ({ ...prev, [connectionId]: false }));
    } finally {
      setTestingId(null);
    }
  };

  const getConnectionIcon = (serverType: string) => {
    return <Database24Regular />;
  };

  const getStatusBadge = (connectionId: string) => {
    if (testingId === connectionId) {
      return <Spinner size="tiny" />;
    }

    if (connectionId in testResults) {
      return testResults[connectionId] ? (
        <Badge appearance="filled" color="success" icon={<CheckmarkCircle24Regular />}>
          Connected
        </Badge>
      ) : (
        <Badge appearance="filled" color="danger" icon={<DismissCircle24Regular />}>
          Failed
        </Badge>
      );
    }

    return null;
  };

  if (isLoading && connections.length === 0) {
    return (
      <div className="empty-state">
        <Spinner size="large" />
        <p>Loading connections...</p>
      </div>
    );
  }

  return (
    <div className="connection-manager">
      <div className="card">
        <div className="card-header">
          <h2 className="card-title">
            <Database24Regular />
            Database Connections
          </h2>
          <Button
            appearance="subtle"
            icon={<Add24Regular />}
            onClick={() => setShowAddMessage(true)}
          >
            Add
          </Button>
        </div>

        {showAddMessage && (
          <MessageBar intent="info" style={{ marginBottom: 12 }}>
            <MessageBarBody>
              <MessageBarTitle>Create Connections</MessageBarTitle>
              Connection management coming soon. For now, connections can be added via the API.
              <Button
                appearance="transparent"
                size="small"
                onClick={() => setShowAddMessage(false)}
                style={{ marginLeft: 8 }}
              >
                Dismiss
              </Button>
            </MessageBarBody>
          </MessageBar>
        )}

        {connections.length === 0 ? (
          <div className="empty-state">
            <Database24Regular className="empty-state-icon" />
            <p className="empty-state-title">No Connections</p>
            <p className="empty-state-description">
              Create database connections in the DataBridge web application to use them here.
            </p>
          </div>
        ) : (
          <div className="connection-list">
            {connections.map((conn) => (
              <div key={conn.id} className="list-item">
                <span className="list-item-icon">{getConnectionIcon(conn.serverType)}</span>
                <div className="list-item-content">
                  <span className="list-item-title">{conn.connectionName}</span>
                  <span className="list-item-subtitle">
                    {conn.serverType} • {conn.databaseName || conn.host}
                  </span>
                </div>
                <div className="connection-actions">
                  {getStatusBadge(conn.id!)}
                  <Button
                    appearance="subtle"
                    size="small"
                    disabled={testingId === conn.id}
                    onClick={() => handleTestConnection(conn.id!)}
                  >
                    Test
                  </Button>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="card">
        <div className="card-header">
          <h2 className="card-title">Quick Query</h2>
        </div>

        {connections.length > 0 ? (
          <QuickQueryForm connections={connections} />
        ) : (
          <p className="form-hint">Add a connection to run queries.</p>
        )}
      </div>

      <style>{`
        .connection-list {
          display: flex;
          flex-direction: column;
          gap: 4px;
        }

        .connection-actions {
          display: flex;
          align-items: center;
          gap: 8px;
        }
      `}</style>
    </div>
  );
}

interface QuickQueryFormProps {
  connections: ConnectionConfig[];
}

function QuickQueryForm({ connections }: QuickQueryFormProps): JSX.Element {
  const [selectedConnection, setSelectedConnection] = useState<string>('');
  const [query, setQuery] = useState('');
  const [isRunning, setIsRunning] = useState(false);
  const [result, setResult] = useState<any>(null);
  const [error, setError] = useState<string | null>(null);

  const handleRunQuery = async () => {
    if (!selectedConnection || !query.trim()) return;

    setIsRunning(true);
    setError(null);
    setResult(null);

    try {
      const response = await apiService.executeQuery(selectedConnection, query);
      if (response.success) {
        setResult(response.data);
      } else {
        setError(response.error || 'Query failed');
      }
    } catch (e: any) {
      setError(e.message);
    } finally {
      setIsRunning(false);
    }
  };

  return (
    <div className="quick-query">
      <Field label="Connection">
        <Dropdown
          value={selectedConnection}
          onOptionSelect={(_, data) => setSelectedConnection(data.optionValue as string)}
          placeholder="Select connection"
        >
          {connections.map((conn) => (
            <Option key={conn.id} value={conn.id!}>
              {conn.connectionName}
            </Option>
          ))}
        </Dropdown>
      </Field>

      <Field label="SQL Query">
        <textarea
          className="query-input"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          placeholder="SELECT * FROM table LIMIT 10"
          rows={4}
        />
      </Field>

      <div className="button-group right">
        <Button
          appearance="primary"
          disabled={!selectedConnection || !query.trim() || isRunning}
          onClick={handleRunQuery}
        >
          {isRunning ? <Spinner size="tiny" /> : 'Run Query'}
        </Button>
      </div>

      {error && (
        <div className="query-error">
          <strong>Error:</strong> {error}
        </div>
      )}

      {result && (
        <div className="data-preview">
          <table>
            <thead>
              <tr>
                {result.columns?.map((col: string, i: number) => (
                  <th key={i}>{col}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {result.rows?.slice(0, 10).map((row: any[], rowIdx: number) => (
                <tr key={rowIdx}>
                  {row.map((cell, cellIdx) => (
                    <td key={cellIdx}>{String(cell ?? '')}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
          {result.rowCount > 10 && (
            <p className="form-hint">Showing 10 of {result.rowCount} rows</p>
          )}
        </div>
      )}

      <style>{`
        .query-input {
          width: 100%;
          padding: 8px;
          font-family: 'Consolas', monospace;
          font-size: 13px;
          border: 1px solid #8a8886;
          border-radius: 4px;
          resize: vertical;
        }

        .query-input:focus {
          outline: none;
          border-color: #0078d4;
        }

        .query-error {
          margin-top: 16px;
          padding: 12px;
          background: #fed9cc;
          border-radius: 4px;
          color: #d13438;
          font-size: 13px;
        }
      `}</style>
    </div>
  );
}

export default ConnectionManager;
