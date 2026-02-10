/**
 * Example: Integrating API Service with Connections View
 *
 * This example shows how to use the API service in your ConnectionsView component
 */

import { useState, useEffect } from "react";
import { useAuth } from "@/contexts/AuthContext";
import { apiService, Connection } from "@/lib/api-service";
import { toast } from "sonner";

export function ConnectionsViewExample() {
  const { user } = useAuth();
  const [connections, setConnections] = useState<Connection[]>([]);
  const [loading, setLoading] = useState(false);

  // Fetch connections on mount
  useEffect(() => {
    if (user?.id) {
      fetchConnections();
    }
  }, [user?.id]);

  const fetchConnections = async () => {
    if (!user?.id) return;

    setLoading(true);
    try {
      const data = await apiService.fetchConnections(user.id);
      setConnections(data);
    } catch (error: any) {
      toast.error("Failed to fetch connections: " + error.message);
    } finally {
      setLoading(false);
    }
  };

  // Test OAuth Connection
  const handleTestOAuthConnection = async (
    name: string,
    type: string,
    token: string
  ) => {
    try {
      const result = await apiService.testOAuthConnection({
        connectionName: name,
        connectionType: type,
        accessToken: token,
      });

      if (result.success) {
        toast.success("Connection test successful!");
      } else {
        toast.error("Connection test failed");
      }
    } catch (error: any) {
      toast.error("Connection test failed: " + error.message);
    }
  };

  // Create OAuth Connection
  const handleCreateOAuthConnection = async (
    name: string,
    type: string,
    token: string,
    refreshToken?: string
  ) => {
    if (!user?.id) return;

    try {
      const connection = await apiService.createOAuthConnection({
        userId: user.id,
        connectionName: name,
        connectionType: type,
        accessToken: token,
        refreshToken,
      });

      setConnections([...connections, connection]);
      toast.success("Connection created successfully!");
    } catch (error: any) {
      toast.error("Failed to create connection: " + error.message);
    }
  };

  // Create Password Connection
  const handleCreatePasswordConnection = async (data: {
    name: string;
    type: string;
    host: string;
    port: number;
    database: string;
    username: string;
    password: string;
  }) => {
    if (!user?.id) return;

    try {
      const connection = await apiService.createPasswordConnection({
        userId: user.id,
        connectionName: data.name,
        connectionType: data.type,
        host: data.host,
        port: data.port,
        database: data.database,
        username: data.username,
        password: data.password,
      });

      setConnections([...connections, connection]);
      toast.success("Connection created successfully!");
    } catch (error: any) {
      toast.error("Failed to create connection: " + error.message);
    }
  };

  // Delete Connection
  const handleDeleteConnection = async (connectionId: string) => {
    try {
      await apiService.removeConnection(connectionId);
      setConnections(connections.filter((c) => c.id !== connectionId));
      toast.success("Connection removed successfully!");
    } catch (error: any) {
      toast.error("Failed to remove connection: " + error.message);
    }
  };

  // Restore Connection
  const handleRestoreConnection = async (connectionId: string) => {
    try {
      await apiService.restoreConnection(connectionId);
      await fetchConnections(); // Refresh list
      toast.success("Connection restored successfully!");
    } catch (error: any) {
      toast.error("Failed to restore connection: " + error.message);
    }
  };

  return (
    <div>
      {loading ? (
        <div>Loading connections...</div>
      ) : (
        <div className="space-y-4">
          {connections.map((connection) => (
            <div key={connection.id} className="border p-4 rounded">
              <h3>{connection.name}</h3>
              <p>Type: {connection.type}</p>
              <p>Status: {connection.status}</p>
              <button onClick={() => handleDeleteConnection(connection.id)}>
                Delete
              </button>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

/**
 * Example: Schema Matcher Integration
 */
export function SchemaMatcherExample() {
  const { user } = useAuth();
  const [databases, setDatabases] = useState<string[]>([]);
  const [jobId, setJobId] = useState<string | null>(null);
  const [comparing, setComparing] = useState(false);

  // Fetch databases for a connection
  const fetchDatabases = async (connectionId: string) => {
    if (!user?.id) return;

    try {
      const data = await apiService.fetchSchemaDatabases({
        userId: user.id,
        connectionId,
        connectionType: "snowflake",
      });
      setDatabases(data);
    } catch (error: any) {
      toast.error("Failed to fetch databases: " + error.message);
    }
  };

  // Start schema comparison
  const startComparison = async (
    sourceConnId: string,
    targetConnId: string,
    sourceDb: string,
    targetDb: string
  ) => {
    if (!user?.id) return;

    setComparing(true);
    try {
      const result = await apiService.compareSchemas({
        userId: user.id,
        sourceConnectionId: sourceConnId,
        targetConnectionId: targetConnId,
        sourceDatabase: sourceDb,
        targetDatabase: targetDb,
      });

      setJobId(result.jobId);
      toast.success("Comparison started! Job ID: " + result.jobId);

      // Poll for results
      pollJobStatus(result.jobId, sourceConnId);
    } catch (error: any) {
      toast.error("Failed to start comparison: " + error.message);
    } finally {
      setComparing(false);
    }
  };

  // Poll job status
  const pollJobStatus = async (jobId: string, connectionId: string) => {
    if (!user?.id) return;

    const interval = setInterval(async () => {
      try {
        const result = await apiService.fetchJobResult({
          userId: user.id!,
          connectionId,
          jobId,
        });

        if (result.summary) {
          clearInterval(interval);
          toast.success("Comparison complete!");
          console.log("Results:", result);
        }
      } catch (error) {
        // Job not ready yet, keep polling
      }
    }, 3000); // Poll every 3 seconds
  };

  // Generate deployment script
  const generateScript = async (
    connectionId: string,
    jobId: string,
    database: string,
    schema: string,
    objects: string[]
  ) => {
    if (!user?.id) return;

    try {
      const script = await apiService.generateDeploymentScript({
        userId: user.id,
        connectionId,
        jobId,
        database,
        schema,
        objects,
      });

      console.log("Deployment Script:", script.script_header);
      console.log("Revert Script:", script.revert_script_header);
      toast.success("Script generated successfully!");
    } catch (error: any) {
      toast.error("Failed to generate script: " + error.message);
    }
  };

  return <div>{/* Your UI here */}</div>;
}

/**
 * Example: Hierarchy & Mapping Integration
 */
export function HierarchyMappingExample() {
  const { user } = useAuth();
  const [hierarchyGroups, setHierarchyGroups] = useState<string[]>([]);

  // Fetch hierarchy groups
  const fetchHierarchyGroups = async (connectionId: string) => {
    if (!user?.id) return;

    try {
      const groups = await apiService.fetchHierarchyGroupNames({
        userId: user.id,
        connectionId,
      });
      setHierarchyGroups(groups);
    } catch (error: any) {
      toast.error("Failed to fetch hierarchy groups: " + error.message);
    }
  };

  // Fetch hierarchy data
  const fetchHierarchyData = async (connectionId: string, group: string) => {
    if (!user?.id) return;

    try {
      const data = await apiService.fetchHierarchyTabular({
        userId: user.id,
        connectionId,
        hierarchyGroup: group,
      });
      console.log("Hierarchy data:", data);
    } catch (error: any) {
      toast.error("Failed to fetch hierarchy data: " + error.message);
    }
  };

  // Insert hierarchy record
  const insertRecord = async (
    connectionId: string,
    group: string,
    record: any
  ) => {
    if (!user?.id) return;

    try {
      await apiService.insertHierarchyRecord({
        userId: user.id,
        connectionId,
        hierarchyGroup: group,
        record,
      });
      toast.success("Record inserted successfully!");
    } catch (error: any) {
      toast.error("Failed to insert record: " + error.message);
    }
  };

  // Update hierarchy record
  const updateRecord = async (
    connectionId: string,
    group: string,
    recordId: string,
    updates: any
  ) => {
    if (!user?.id) return;

    try {
      await apiService.updateHierarchyRecord({
        userId: user.id,
        connectionId,
        hierarchyGroup: group,
        recordId,
        updates,
      });
      toast.success("Record updated successfully!");
    } catch (error: any) {
      toast.error("Failed to update record: " + error.message);
    }
  };

  // Delete hierarchy record
  const deleteRecord = async (
    connectionId: string,
    group: string,
    recordId: string
  ) => {
    if (!user?.id) return;

    try {
      await apiService.deleteHierarchyRecord({
        userId: user.id,
        connectionId,
        hierarchyGroup: group,
        recordId,
      });
      toast.success("Record deleted successfully!");
    } catch (error: any) {
      toast.error("Failed to delete record: " + error.message);
    }
  };

  return <div>{/* Your UI here */}</div>;
}
