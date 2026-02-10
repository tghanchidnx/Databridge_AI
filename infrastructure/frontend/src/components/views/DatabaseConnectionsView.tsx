import React, { useState, useEffect } from "react";
import { useConnectionsStore } from "@/stores/connections.store";
import { useAuthStore } from "@/stores/auth.store";
import { useOrganizationStore } from "@/stores/organization.store";
import { toast } from "sonner";
import {
  snowflakeAPI,
  type SnowflakePasswordConnectionRequest,
  type SnowflakeOAuthConnectionRequest,
} from "@/lib/snowflake-api";
import {
  CaretDown,
  Database,
  Plus,
  Trash,
  TestTube,
  ArrowsClockwise,
} from "@phosphor-icons/react";
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog";

// Modal components for different database types
import { SnowflakeConnectionModal } from "./modals/SnowflakeConnectionModal";
import { MySQLConnectionModal } from "./modals/MySQLConnectionModal";
import { PostgreSQLConnectionModal } from "./modals/PostgreSQLConnectionModal";

type DatabaseType =
  | "snowflake"
  | "mysql"
  | "postgresql"
  | "sqlserver"
  | "oracle";

export const DatabaseConnectionsView: React.FC = () => {
  const { user } = useAuthStore();
  const { currentOrganization } = useOrganizationStore();
  const {
    connections,
    loading,
    loadConnections,
    refreshConnections,
    addConnection,
    removeConnection,
  } = useConnectionsStore();

  const [showAddMenu, setShowAddMenu] = useState(false);
  const [showModal, setShowModal] = useState(false);
  const [selectedDbType, setSelectedDbType] = useState<DatabaseType | null>(
    null
  );
  const [testingConnection, setTestingConnection] = useState<string | null>(
    null
  );
  const [deleteDialogOpen, setDeleteDialogOpen] = useState(false);
  const [connectionToDelete, setConnectionToDelete] = useState<{
    id: string;
    name: string;
  } | null>(null);

  useEffect(() => {
    loadConnections();
  }, [loadConnections]);

  const handleAddConnection = (dbType: DatabaseType) => {
    setSelectedDbType(dbType);
    setShowModal(true);
    setShowAddMenu(false);
  };

  const handleTestConnection = async (connectionId: string) => {
    setTestingConnection(connectionId);
    toast.loading("Testing connection...", { id: "test-connection" });

    try {
      const response: any = await snowflakeAPI.testConnection(connectionId);
      // Handle wrapped response from TransformInterceptor
      const result = response?.data || response;

      if (result?.success) {
        toast.success("✅ Connection test successful!", {
          id: "test-connection",
        });
      } else {
        toast.error(
          `❌ Connection test failed: ${result?.message || "Unknown error"}`,
          { id: "test-connection" }
        );
      }
    } catch (error: any) {
      console.error("Test connection error:", error);
      toast.error(
        `❌ Failed to test connection: ${error.message || "Unknown error"}`,
        { id: "test-connection" }
      );
    } finally {
      setTestingConnection(null);
    }
  };

  const handleDeleteConnection = async () => {
    if (!connectionToDelete) return;

    try {
      await snowflakeAPI.deleteConnection(connectionToDelete.id);
      removeConnection(connectionToDelete.id);
      toast.success("Connection deleted successfully");
      setDeleteDialogOpen(false);
      setConnectionToDelete(null);
    } catch (error: any) {
      toast.error(error.message || "Failed to delete connection");
    }
  };

  const openDeleteDialog = (connectionId: string, connectionName: string) => {
    setConnectionToDelete({ id: connectionId, name: connectionName });
    setDeleteDialogOpen(true);
  };

  const getConnectionIcon = (type: string): React.ReactElement => {
    const iconMap: Record<string, React.ReactElement> = {
      snowflake: <span className="text-2xl">❄️</span>,
      mysql: <span className="text-2xl">🐬</span>,
      postgresql: <span className="text-2xl">🐘</span>,
      sqlserver: <span className="text-2xl">📊</span>,
      oracle: <span className="text-2xl">🔴</span>,
    };
    return iconMap[type?.toLowerCase()] || <span className="text-2xl">💾</span>;
  };

  const getConnectionTypeLabel = (type: string) => {
    const labels: Record<string, string> = {
      snowflake: "Snowflake",
      mysql: "MySQL",
      postgresql: "PostgreSQL",
      sqlserver: "SQL Server",
      oracle: "Oracle",
    };
    return labels[type] || type;
  };

  const getStatusColor = (status?: string) => {
    switch (status) {
      case "active":
      case "connected":
        return "bg-emerald-500/10 text-emerald-600 border-emerald-500/30";
      case "error":
      case "failed":
        return "bg-red-500/10 text-red-600 border-red-500/30";
      default:
        return "bg-gray-500/10 text-gray-600 border-gray-500/30";
    }
  };

  const workspaceInfo = {
    hasOrg: !!currentOrganization,
    name: currentOrganization?.name || "No Organization Selected",
  };

  return (
    <div className="min-h-screen bg-background p-6">
      <div className="max-w-7xl mx-auto space-y-6">
        {/* Header */}
        <div className="flex items-start justify-between">
          <div className="flex flex-col gap-2">
            <h1 className="text-3xl font-bold text-foreground m-0">
              Database Connections
            </h1>
            <p className="text-muted-foreground text-sm">
              Manage secure connections to all your databases
            </p>
            {workspaceInfo.hasOrg && (
              <div className="inline-flex items-center gap-2 px-3 py-1 bg-primary/10 border border-primary/30 rounded-md text-sm font-medium text-primary w-fit">
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  width="16"
                  height="16"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                >
                  <path d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4" />
                </svg>
                <span>{workspaceInfo.name}</span>
              </div>
            )}
          </div>

          {/* Action Buttons */}
          <div className="flex items-center gap-3">
            <button
              onClick={() => refreshConnections()}
              disabled={loading}
              className="inline-flex items-center gap-2 px-4 py-2 bg-background border border-border text-foreground rounded-lg font-semibold text-sm transition-all hover:bg-accent hover:text-accent-foreground disabled:opacity-50 disabled:cursor-not-allowed"
              title="Refresh connections"
            >
              <ArrowsClockwise
                className={`w-5 h-5 ${loading ? "animate-spin" : ""}`}
              />
              Refresh
            </button>

            {/* Add Connection Dropdown */}
            <div className="relative">
              <button
                onClick={() => setShowAddMenu(!showAddMenu)}
                className="inline-flex items-center gap-2 px-4 py-2 bg-primary text-primary-foreground rounded-lg font-semibold text-sm transition-all hover:bg-primary/90 hover:shadow-lg"
              >
                <Plus className="w-5 h-5" weight="bold" />
                Add Connection
                <CaretDown
                  className={`w-4 h-4 transition-transform ${
                    showAddMenu ? "rotate-180" : ""
                  }`}
                />
              </button>

              {showAddMenu && (
                <div className="absolute right-0 mt-2 w-56 bg-background border border-border rounded-lg shadow-lg z-50">
                  <div className="py-2">
                    <button
                      onClick={() => handleAddConnection("snowflake")}
                      className="w-full px-4 py-2.5 text-left hover:bg-accent transition-colors flex items-center gap-3"
                    >
                      <span className="text-xl">❄️</span>
                      <span className="font-medium text-foreground">
                        Snowflake
                      </span>
                    </button>
                    <button
                      onClick={() => handleAddConnection("mysql")}
                      className="w-full px-4 py-2.5 text-left hover:bg-accent transition-colors flex items-center gap-3"
                    >
                      <span className="text-xl">🐬</span>
                      <span className="font-medium text-foreground">MySQL</span>
                    </button>
                    <button
                      onClick={() => handleAddConnection("postgresql")}
                      className="w-full px-4 py-2.5 text-left hover:bg-accent transition-colors flex items-center gap-3"
                    >
                      <span className="text-xl">🐘</span>
                      <span className="font-medium text-foreground">
                        PostgreSQL
                      </span>
                    </button>
                    <button
                      onClick={() => handleAddConnection("sqlserver")}
                      className="w-full px-4 py-2.5 text-left hover:bg-accent transition-colors flex items-center gap-3"
                    >
                      <span className="text-xl">📊</span>
                      <span className="font-medium text-foreground">
                        SQL Server
                      </span>
                    </button>
                    <button
                      onClick={() => handleAddConnection("oracle")}
                      className="w-full px-4 py-2.5 text-left hover:bg-accent transition-colors flex items-center gap-3"
                    >
                      <span className="text-xl">🔴</span>
                      <span className="font-medium text-foreground">
                        Oracle
                      </span>
                    </button>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Connections Grid */}
        {loading && connections.length === 0 ? (
          <div className="text-center py-10 text-muted-foreground">
            Loading connections...
          </div>
        ) : connections.length === 0 ? (
          <div className="text-center py-16">
            <Database
              className="mx-auto w-16 h-16 text-muted-foreground/50 mb-4"
              weight="duotone"
            />
            <h3 className="text-xl font-semibold text-foreground mb-2">
              No connections yet
            </h3>
            <p className="text-muted-foreground mb-6">
              Get started by adding your first database connection
            </p>
            <button
              onClick={() => setShowAddMenu(true)}
              className="px-6 py-2.5 bg-primary text-primary-foreground rounded-lg hover:bg-primary/90 transition-colors font-semibold inline-flex items-center gap-2"
            >
              <Plus className="w-5 h-5" weight="bold" />
              Add Connection
            </button>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-2 gap-4">
            {connections.map((connection) => (
              <div
                key={connection.id}
                className="bg-card border border-border rounded-xl p-6 hover:shadow-lg transition-all hover:border-primary/50"
              >
                {/* Connection Header */}
                <div className="flex items-start gap-3 mb-4">
                  <div className="flex items-center justify-center w-14 h-14 rounded-xl bg-primary/10 text-primary shrink-0">
                    {getConnectionIcon(
                      connection.serverType || connection.type || "snowflake"
                    )}
                  </div>
                  <div className="flex-1 min-w-0">
                    <h3 className="font-bold text-foreground text-base mb-1 truncate">
                      {connection.connectionName || connection.name}
                    </h3>
                    <div className="flex items-center gap-2 flex-wrap">
                      <span
                        className={`text-xs px-2.5 pb-1 rounded-full border font-medium ${getStatusColor(
                          connection.status
                        )}`}
                      >
                        {connection.status || "unknown"}
                      </span>
                      <span className="text-xs px-2.5 pb-1 rounded-full bg-primary/10 text-primary border border-primary/30 font-semibold">
                        {getConnectionTypeLabel(
                          connection.serverType ||
                            connection.type ||
                            "snowflake"
                        )}
                      </span>
                    </div>
                  </div>
                </div>

                {/* Connection Details */}
                <div className="space-y-2.5 mb-4 text-sm">
                  {/* Account/Host */}
                  <div className="flex justify-between items-start gap-2">
                    <span className="text-muted-foreground font-medium shrink-0">
                      {connection.serverType === "mysql" ||
                      connection.serverType === "postgresql"
                        ? "Host:"
                        : "Account:"}
                    </span>
                    <span className="text-foreground font-semibold text-right break-all">
                      {connection.snowflakeAccount || connection.host || "N/A"}
                    </span>
                  </div>

                  {/* Port (for MySQL/PostgreSQL) */}
                  {(connection.serverType === "mysql" ||
                    connection.serverType === "postgresql") &&
                    connection.port && (
                      <div className="flex justify-between items-start gap-2">
                        <span className="text-muted-foreground font-medium shrink-0">
                          Port:
                        </span>
                        <span className="text-foreground font-semibold text-right break-all">
                          {connection.port}
                        </span>
                      </div>
                    )}

                  {/* Username */}
                  {(connection.snowflakeUser || connection.username) && (
                    <div className="flex justify-between items-start gap-2">
                      <span className="text-muted-foreground font-medium shrink-0">
                        Username:
                      </span>
                      <span className="text-foreground font-semibold text-right break-all">
                        {connection.snowflakeUser || connection.username}
                      </span>
                    </div>
                  )}

                  {/* Warehouse (Snowflake only) */}
                  {connection.snowflakeWarehouse && (
                    <div className="flex justify-between items-start gap-2">
                      <span className="text-muted-foreground font-medium shrink-0">
                        Warehouse:
                      </span>
                      <span className="text-foreground font-semibold text-right break-all">
                        {connection.snowflakeWarehouse}
                      </span>
                    </div>
                  )}

                  {/* Database */}
                  <div className="flex justify-between items-start gap-2">
                    <span className="text-muted-foreground font-medium shrink-0">
                      Database:
                    </span>
                    <span className="text-foreground font-semibold text-right break-all">
                      {connection.snowflakeDatabase ||
                        connection.databaseName ||
                        "N/A"}
                    </span>
                  </div>
                </div>

                {/* Actions */}
                <div className="flex items-center gap-2 pt-4 border-t border-border">
                  <button
                    onClick={() => handleTestConnection(connection.id)}
                    disabled={testingConnection === connection.id}
                    className="flex-1 px-4 py-2.5 bg-primary text-primary-foreground rounded-lg hover:bg-primary/90 transition-all font-semibold text-sm inline-flex items-center justify-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed shadow-sm"
                  >
                    {testingConnection === connection.id ? (
                      <>
                        <div className="w-4 h-4 border-2 border-primary-foreground border-t-transparent rounded-full animate-spin" />
                        Testing...
                      </>
                    ) : (
                      <>
                        <TestTube className="w-4 h-4" weight="bold" />
                        Test
                      </>
                    )}
                  </button>
                  <button
                    onClick={() =>
                      openDeleteDialog(
                        connection.id,
                        connection.connectionName ||
                          connection.name ||
                          "this connection"
                      )
                    }
                    className="px-3 py-2.5 bg-destructive/10 text-destructive rounded-lg hover:bg-destructive/20 transition-all font-semibold text-sm inline-flex items-center justify-center hover:scale-105 shadow-sm"
                    title="Delete connection"
                  >
                    <Trash className="w-5 h-5" weight="bold" />
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Modals */}
      {showModal && selectedDbType === "snowflake" && (
        <SnowflakeConnectionModal
          isOpen={showModal}
          onClose={() => {
            setShowModal(false);
            setSelectedDbType(null);
          }}
          onSuccess={(connection) => {
            addConnection(connection);
            setShowModal(false);
            setSelectedDbType(null);
            refreshConnections(); // Auto-reload connections
          }}
        />
      )}

      {showModal && selectedDbType === "mysql" && (
        <MySQLConnectionModal
          isOpen={showModal}
          onClose={() => {
            setShowModal(false);
            setSelectedDbType(null);
          }}
          onSuccess={(connection) => {
            addConnection(connection);
            setShowModal(false);
            setSelectedDbType(null);
            refreshConnections(); // Auto-reload connections
          }}
        />
      )}

      {showModal && selectedDbType === "postgresql" && (
        <PostgreSQLConnectionModal
          isOpen={showModal}
          onClose={() => {
            setShowModal(false);
            setSelectedDbType(null);
          }}
          onSuccess={(connection) => {
            addConnection(connection);
            setShowModal(false);
            setSelectedDbType(null);
            refreshConnections(); // Auto-reload connections
          }}
        />
      )}

      {/* Delete Confirmation Dialog */}
      <AlertDialog open={deleteDialogOpen} onOpenChange={setDeleteDialogOpen}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Delete Connection</AlertDialogTitle>
            <AlertDialogDescription>
              Are you sure you want to delete the connection{" "}
              <span className="font-semibold text-foreground">
                {connectionToDelete?.name}
              </span>
              ? This action cannot be undone and will permanently remove the
              connection from your workspace.
            </AlertDialogDescription>
          </AlertDialogHeader>
          <AlertDialogFooter>
            <AlertDialogCancel
              onClick={() => {
                setDeleteDialogOpen(false);
                setConnectionToDelete(null);
              }}
            >
              Cancel
            </AlertDialogCancel>
            <AlertDialogAction
              onClick={handleDeleteConnection}
              className="bg-destructive text-destructive-foreground hover:bg-destructive/90"
            >
              Delete Connection
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>

      {/* Click outside to close dropdown */}
      {showAddMenu && (
        <div
          className="fixed inset-0 z-40"
          onClick={() => setShowAddMenu(false)}
        />
      )}
    </div>
  );
};
