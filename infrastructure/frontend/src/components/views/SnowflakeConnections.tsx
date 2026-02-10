import React, { useState, useEffect } from "react";
import {
  snowflakeAPI,
  type SnowflakePasswordConnectionRequest,
  type SnowflakeOAuthConnectionRequest,
} from "../../lib/snowflake-api";
import { useAuthStore } from "@/stores/auth.store";
import { useOrganizationStore } from "@/stores/organization.store";
import { useConnectionsStore } from "@/stores/connections.store";
import { toast } from "sonner";

export const SnowflakeConnections: React.FC = () => {
  const { user } = useAuthStore();
  const { currentOrganization, organizations, loadOrganizations } =
    useOrganizationStore();
  const {
    connections,
    loading,
    loadConnections,
    refreshConnections,
    addConnection,
    updateConnection,
    removeConnection,
  } = useConnectionsStore();

  const [error, setError] = useState<string | null>(null);
  const [showModal, setShowModal] = useState(false);
  const [authMethod, setAuthMethod] = useState<
    "password" | "keypair" | "oauth" | "sso"
  >("password");
  const [testingConnection, setTestingConnection] = useState<string | null>(
    null
  );

  // Form state with pre-filled values from env
  const [formData, setFormData] = useState({
    connectionName: "",
    snowflakeAccount: "zf07542.south-central-us.azure",
    snowflakeWarehouse: "COMPUTE_WH",
    snowflakeRole: "SYSADMIN",
    databaseName: "TRANSFORMATION",
    schemaName: "CONFIGURATION",
    username: "",
    password: "",
    privateKey: "",
    privateKeyPassphrase: "",
    snowflakeClientId: "",
    snowflakeClientSecret: "",
  });

  useEffect(() => {
    console.log("🔍 SnowflakeConnections mounted");
    console.log("👤 User:", user);
    console.log("🏢 Current Organization:", currentOrganization);
    console.log("📋 Organizations:", organizations);

    const storedOrg = localStorage.getItem("organization-storage");
    console.log("💾 Stored organization data:", storedOrg);

    // Load connections from store (uses cache if available)
    loadConnections();

    if (user) {
      console.log("🔄 Loading organizations...");
      loadOrganizations().catch((err) => {
        console.log("⚠️ No organizations yet or failed to load:", err);
      });
    }
  }, [user]);

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFormData({
      ...formData,
      [e.target.name]: e.target.value,
    });
  };

  const handleOAuthConnection = async () => {
    const redirectUri = `${window.location.origin}/connections/snowflake/callback`;
    const authUrl = snowflakeAPI.generateOAuthUrl(
      formData.snowflakeClientId,
      formData.snowflakeAccount,
      redirectUri
    );

    const popup = window.open(
      authUrl,
      "Snowflake OAuth",
      "width=600,height=700"
    );

    window.addEventListener("message", async (event) => {
      if (event.data.type === "snowflake-oauth-callback") {
        popup?.close();

        try {
          const request: SnowflakeOAuthConnectionRequest = {
            code: event.data.code,
            connectionName: formData.connectionName,
            snowflakeClientId: formData.snowflakeClientId,
            snowflakeClientSecret: formData.snowflakeClientSecret,
            snowflakeAccount: formData.snowflakeAccount,
            snowflakeWarehouse: formData.snowflakeWarehouse,
            databaseName: formData.databaseName,
            schemaName: formData.schemaName,
          };

          const newConnection = await snowflakeAPI.createOAuthConnection(
            request
          );
          addConnection(newConnection);
          setShowModal(false);
          resetForm();
          toast.success("OAuth connection created successfully!");
        } catch (err: any) {
          const errorMsg = err.message || "Failed to create OAuth connection";
          setError(errorMsg);
          toast.error(errorMsg);
        }
      }
    });
  };

  const handlePasswordConnection = async () => {
    if (!user) {
      toast.error("Please log in to create a connection");
      return;
    }

    setError(null);
    try {
      const request: SnowflakePasswordConnectionRequest = {
        connectionName: formData.connectionName,
        username: formData.username,
        password: formData.password,
        snowflakeAccount: formData.snowflakeAccount,
        snowflakeWarehouse: formData.snowflakeWarehouse,
        databaseName: formData.databaseName,
        schemaName: formData.schemaName,
      };

      toast.loading("Testing connection...", { id: "create-connection" });
      const newConnection = await snowflakeAPI.createPasswordConnection(
        request
      );

      addConnection(newConnection);
      toast.success("Connection created and tested successfully!", {
        id: "create-connection",
      });
      setShowModal(false);
      resetForm();
    } catch (err: any) {
      const errorMsg = err.message || "Failed to create connection";
      setError(errorMsg);
      toast.error(errorMsg, { id: "create-connection" });
    }
  };

  const handleSSOLogin = async () => {
    if (!user) {
      toast.error("Please log in to create a connection");
      return;
    }

    try {
      toast.loading("Initiating SSO login...", { id: "sso-login" });
      const response = await snowflakeAPI.initSSO({
        snowflakeAccount: formData.snowflakeAccount,
      });

      const authUrl =
        (response as any).data?.authorizationUrl || response.authorizationUrl;

      if (!authUrl) {
        throw new Error("No authorization URL received");
      }

      toast.success("Redirecting to Snowflake SSO...", { id: "sso-login" });
      window.location.href = authUrl;
    } catch (err: any) {
      const errorMsg = err.message || "Failed to initiate SSO";
      toast.error(errorMsg, { id: "sso-login" });
    }
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (authMethod === "oauth") {
      handleOAuthConnection();
    } else if (authMethod === "sso") {
      handleSSOLogin();
    } else {
      handlePasswordConnection();
    }
  };

  const handleTestConnection = async (id: string) => {
    setTestingConnection(id);
    toast.loading("Testing connection...", { id: "test-connection" });

    try {
      const response: any = await snowflakeAPI.testConnection(id);

      // Handle wrapped response from backend TransformInterceptor
      const result = response.data || response;

      if (result.success) {
        toast.success("✅ Connection successful! Database is accessible.", {
          id: "test-connection",
        });
      } else {
        toast.error(`❌ Connection failed: ${result.message}`, {
          id: "test-connection",
        });
      }

      await loadConnections();
    } catch (err: any) {
      toast.error(`❌ Connection test failed: ${err.message}`, {
        id: "test-connection",
      });
    } finally {
      setTestingConnection(null);
    }
  };

  const handleDeleteConnection = async (id: string) => {
    if (!confirm("Are you sure you want to delete this connection?")) return;

    toast.loading("Deleting connection...", { id: "delete-connection" });

    try {
      await snowflakeAPI.deleteConnection(id);
      toast.success("Connection deleted successfully", {
        id: "delete-connection",
      });
      await loadConnections();
    } catch (err: any) {
      toast.error(`Failed to delete connection: ${err.message}`, {
        id: "delete-connection",
      });
    }
  };

  const resetForm = () => {
    setFormData({
      connectionName: "",
      snowflakeAccount: "zf07542.south-central-us.azure",
      snowflakeWarehouse: "COMPUTE_WH",
      snowflakeRole: "SYSADMIN",
      databaseName: "TRANSFORMATION",
      schemaName: "CONFIGURATION",
      username: "",
      password: "",
      privateKey: "",
      privateKeyPassphrase: "",
      snowflakeClientId: "",
      snowflakeClientSecret: "",
    });
    setAuthMethod("password");
  };

  // Get workspace display info
  const getWorkspaceInfo = () => {
    if (currentOrganization) {
      return { name: currentOrganization.name, hasOrg: true };
    }
    if (user?.organizationId) {
      return { name: `Workspace (${user.organizationId})`, hasOrg: true };
    }
    return { name: "No workspace", hasOrg: false };
  };

  const workspaceInfo = getWorkspaceInfo();

  return (
    <div className="p-6 max-w-7xl mx-auto">
      <div className="flex justify-between items-center mb-6">
        <div className="flex flex-col gap-2">
          <h1 className="text-3xl font-bold text-foreground m-0">
            Snowflake Connections
          </h1>
          {workspaceInfo.hasOrg ? (
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
          ) : (
            <div className="inline-flex items-center gap-2 px-3 py-1 bg-destructive/10 border border-destructive/30 rounded-md text-sm font-medium text-destructive w-fit">
              <svg
                xmlns="http://www.w3.org/2000/svg"
                width="16"
                height="16"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
              >
                <path d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
              </svg>
              <span>{workspaceInfo.name}</span>
            </div>
          )}
        </div>
        <div className="flex items-center gap-3">
          <button
            onClick={() => refreshConnections()}
            disabled={loading}
            className="inline-flex items-center gap-2 px-4 py-2 bg-background border border-border text-foreground rounded-lg font-semibold text-sm transition-all hover:bg-accent hover:text-accent-foreground disabled:opacity-50 disabled:cursor-not-allowed"
            title="Refresh connections"
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              width="20"
              height="20"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
              />
            </svg>
            Refresh
          </button>
          <button
            className="inline-flex items-center gap-2 px-4 py-2 bg-primary text-primary-foreground rounded-lg font-semibold text-sm transition-all hover:bg-primary/90 hover:shadow-lg"
            onClick={() => setShowModal(true)}
          >
            <svg
              xmlns="http://www.w3.org/2000/svg"
              width="20"
              height="20"
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M12 4v16m8-8H4"
              />
            </svg>
            New Connection
          </button>
        </div>
      </div>

      {error && (
        <div className="flex items-center justify-between p-4 mb-4 bg-destructive/10 border border-destructive/30 rounded-lg text-destructive">
          <span>{error}</span>
          <button
            onClick={() => setError(null)}
            className="text-destructive hover:text-destructive/80 font-bold text-xl"
          >
            ×
          </button>
        </div>
      )}

      {loading && connections.length === 0 ? (
        <div className="text-center py-10 text-muted-foreground">
          Loading connections...
        </div>
      ) : connections.length === 0 ? (
        <div className="text-center py-16">
          <svg
            xmlns="http://www.w3.org/2000/svg"
            className="mx-auto w-16 h-16 text-muted-foreground/50 mb-4"
            fill="none"
            viewBox="0 0 24 24"
            stroke="currentColor"
          >
            <path
              strokeLinecap="round"
              strokeLinejoin="round"
              strokeWidth={2}
              d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4"
            />
          </svg>
          <h3 className="text-xl font-semibold text-foreground mb-2">
            No connections yet
          </h3>
          <p className="text-muted-foreground">
            Create your first Snowflake connection to get started
          </p>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
          {Array.isArray(connections) &&
            connections.map((conn) => (
              <div
                key={conn.id}
                className="bg-card border border-border rounded-xl p-5 shadow-sm hover:shadow-md transition-shadow"
              >
                <div className="flex justify-between items-start mb-4">
                  <h3 className="text-lg font-semibold text-card-foreground">
                    {conn.connectionName}
                  </h3>
                  <span
                    className={`px-3 py-1 rounded-full text-xs font-semibold ${
                      conn.status === "active"
                        ? "bg-accent/20 text-accent"
                        : "bg-muted text-muted-foreground"
                    }`}
                  >
                    {conn.status}
                  </span>
                </div>
                <div className="space-y-2 mb-4">
                  <div className="flex justify-between py-2 border-b border-border">
                    <span className="text-sm text-muted-foreground">
                      Account:
                    </span>
                    <span className="text-sm font-medium text-foreground">
                      {conn.snowflakeAccount}
                    </span>
                  </div>
                  <div className="flex justify-between py-2 border-b border-border">
                    <span className="text-sm text-muted-foreground">Type:</span>
                    <span className="text-sm font-medium text-foreground">
                      {conn.connectionType}
                    </span>
                  </div>
                  <div className="flex justify-between py-2 border-b border-border">
                    <span className="text-sm text-muted-foreground">
                      Warehouse:
                    </span>
                    <span className="text-sm font-medium text-foreground">
                      {conn.snowflakeWarehouse || "N/A"}
                    </span>
                  </div>
                  <div className="flex justify-between py-2">
                    <span className="text-sm text-muted-foreground">
                      Database:
                    </span>
                    <span className="text-sm font-medium text-foreground">
                      {conn.snowflakeDatabase || "N/A"}
                    </span>
                  </div>
                </div>
                <div className="flex gap-2">
                  <button
                    className="flex-1 inline-flex items-center justify-center gap-2 px-3 py-2 bg-primary text-primary-foreground rounded-lg text-sm font-medium transition-colors hover:bg-primary/90 disabled:opacity-50"
                    onClick={() => handleTestConnection(conn.id)}
                    disabled={testingConnection === conn.id}
                  >
                    {testingConnection === conn.id ? (
                      <svg
                        className="animate-spin h-4 w-4"
                        xmlns="http://www.w3.org/2000/svg"
                        fill="none"
                        viewBox="0 0 24 24"
                      >
                        <circle
                          className="opacity-25"
                          cx="12"
                          cy="12"
                          r="10"
                          stroke="currentColor"
                          strokeWidth="4"
                        ></circle>
                        <path
                          className="opacity-75"
                          fill="currentColor"
                          d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"
                        ></path>
                      </svg>
                    ) : (
                      <svg
                        xmlns="http://www.w3.org/2000/svg"
                        width="16"
                        height="16"
                        viewBox="0 0 24 24"
                        fill="none"
                        stroke="currentColor"
                        strokeWidth="2"
                      >
                        <path d="M13 2L3 14h9l-1 8 10-12h-9l1-8z" />
                      </svg>
                    )}
                    Test
                  </button>
                  <button
                    className="px-3 py-2 bg-destructive/10 text-destructive rounded-lg text-sm font-medium transition-colors hover:bg-destructive/20"
                    onClick={() => handleDeleteConnection(conn.id)}
                  >
                    <svg
                      xmlns="http://www.w3.org/2000/svg"
                      width="16"
                      height="16"
                      viewBox="0 0 24 24"
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="2"
                    >
                      <path d="M3 6h18M19 6v14a2 2 0 01-2 2H7a2 2 0 01-2-2V6m3 0V4a2 2 0 012-2h4a2 2 0 012 2v2" />
                    </svg>
                  </button>
                </div>
              </div>
            ))}
        </div>
      )}

      {showModal && (
        <div
          className="fixed inset-0 bg-black/50 dark:bg-black/80 flex items-center justify-center z-50 p-4"
          onClick={() => setShowModal(false)}
        >
          <div
            className="bg-card border border-border rounded-2xl p-6 max-w-2xl w-full max-h-[90vh] overflow-y-auto shadow-2xl"
            onClick={(e) => e.stopPropagation()}
          >
            <div className="flex justify-between items-center mb-6">
              <h2 className="text-2xl font-bold text-card-foreground">
                New Snowflake Connection
              </h2>
              <button
                className="text-muted-foreground hover:text-foreground text-3xl font-light leading-none"
                onClick={() => setShowModal(false)}
              >
                ×
              </button>
            </div>

            <div className="flex gap-2 mb-6 border-b border-border pb-1">
              <button
                className={`flex items-center gap-2 px-4 py-2 text-sm font-semibold rounded-t-lg transition-colors ${
                  authMethod === "password"
                    ? "text-primary border-b-2 border-primary -mb-px"
                    : "text-muted-foreground hover:text-foreground hover:bg-muted/50"
                }`}
                onClick={() => setAuthMethod("password")}
              >
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  width="16"
                  height="16"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                >
                  <rect
                    x="3"
                    y="11"
                    width="18"
                    height="11"
                    rx="2"
                    ry="2"
                  ></rect>
                  <path d="M7 11V7a5 5 0 0 1 10 0v4"></path>
                </svg>
                Password
              </button>
              <button
                className={`flex items-center gap-2 px-4 py-2 text-sm font-semibold rounded-t-lg transition-colors ${
                  authMethod === "keypair"
                    ? "text-primary border-b-2 border-primary -mb-px"
                    : "text-muted-foreground hover:text-foreground hover:bg-muted/50"
                }`}
                onClick={() => setAuthMethod("keypair")}
              >
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  width="16"
                  height="16"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                >
                  <path d="M21 2l-2 2m-7.61 7.61a5.5 5.5 0 1 1-7.778 7.778 5.5 5.5 0 0 1 7.777-7.777zm0 0L15.5 7.5m0 0l3 3L22 7l-3-3m-3.5 3.5L19 4"></path>
                </svg>
                Key Pair
              </button>
              <button
                className={`flex items-center gap-2 px-4 py-2 text-sm font-semibold rounded-t-lg transition-colors ${
                  authMethod === "oauth"
                    ? "text-primary border-b-2 border-primary -mb-px"
                    : "text-muted-foreground hover:text-foreground hover:bg-muted/50"
                }`}
                onClick={() => setAuthMethod("oauth")}
              >
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  width="16"
                  height="16"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                >
                  <path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"></path>
                </svg>
                OAuth
              </button>
              <button
                className={`flex items-center gap-2 px-4 py-2 text-sm font-semibold rounded-t-lg transition-colors ${
                  authMethod === "sso"
                    ? "text-primary border-b-2 border-primary -mb-px"
                    : "text-muted-foreground hover:text-foreground hover:bg-muted/50"
                }`}
                onClick={() => setAuthMethod("sso")}
              >
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  width="16"
                  height="16"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                >
                  <path d="M20 21v-2a4 4 0 0 0-4-4H8a4 4 0 0 0-4 4v2"></path>
                  <circle cx="12" cy="7" r="4"></circle>
                </svg>
                SSO
              </button>
            </div>

            <form onSubmit={handleSubmit} className="space-y-4">
              {workspaceInfo.hasOrg ? (
                <div className="flex items-center gap-3 p-3 bg-primary/10 border border-primary/30 rounded-lg">
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    className="w-5 h-5 text-primary"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth={2}
                      d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4"
                    />
                  </svg>
                  <span className="text-sm text-primary">
                    Workspace: <strong>{workspaceInfo.name}</strong>
                  </span>
                </div>
              ) : (
                <div className="flex items-center gap-3 p-3 bg-destructive/10 border border-destructive/30 rounded-lg">
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    className="w-5 h-5 text-destructive"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth={2}
                      d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z"
                    />
                  </svg>
                  <span className="text-sm text-destructive font-medium">
                    ⚠️ No workspace selected. Connection will be created for
                    your personal account.
                  </span>
                </div>
              )}

              <div className="space-y-2">
                <label className="text-sm font-semibold text-foreground">
                  Connection Name *
                </label>
                <input
                  type="text"
                  name="connectionName"
                  value={formData.connectionName}
                  onChange={handleInputChange}
                  required
                  placeholder="My Snowflake Connection"
                  className="w-full px-3 py-2 bg-input border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
                />
              </div>

              <div className="space-y-2">
                <label className="text-sm font-semibold text-foreground">
                  Snowflake Account *
                </label>
                <input
                  type="text"
                  name="snowflakeAccount"
                  value={formData.snowflakeAccount}
                  onChange={handleInputChange}
                  required
                  placeholder="zf07542.south-central-us.azure"
                  className="w-full px-3 py-2 bg-input border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
                />
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <label className="text-sm font-semibold text-foreground">
                    Warehouse *
                  </label>
                  <input
                    type="text"
                    name="snowflakeWarehouse"
                    value={formData.snowflakeWarehouse}
                    onChange={handleInputChange}
                    required
                    placeholder="COMPUTE_WH"
                    className="w-full px-3 py-2 bg-input border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
                  />
                </div>

                <div className="space-y-2">
                  <label className="text-sm font-semibold text-foreground">
                    Role (Optional)
                  </label>
                  <input
                    type="text"
                    name="snowflakeRole"
                    value={formData.snowflakeRole}
                    onChange={handleInputChange}
                    placeholder="SYSADMIN"
                    className="w-full px-3 py-2 bg-input border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
                  />
                </div>
              </div>

              <div className="grid grid-cols-2 gap-4">
                <div className="space-y-2">
                  <label className="text-sm font-semibold text-foreground">
                    Database *
                  </label>
                  <input
                    type="text"
                    name="databaseName"
                    value={formData.databaseName}
                    onChange={handleInputChange}
                    required
                    placeholder="TRANSFORMATION"
                    className="w-full px-3 py-2 bg-input border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
                  />
                </div>

                <div className="space-y-2">
                  <label className="text-sm font-semibold text-foreground">
                    Schema (Optional)
                  </label>
                  <input
                    type="text"
                    name="schemaName"
                    value={formData.schemaName}
                    onChange={handleInputChange}
                    placeholder="CONFIGURATION"
                    className="w-full px-3 py-2 bg-input border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
                  />
                </div>
              </div>

              {authMethod === "password" && (
                <>
                  <div className="space-y-2">
                    <label className="text-sm font-semibold text-foreground">
                      Username *
                    </label>
                    <input
                      type="text"
                      name="username"
                      value={formData.username}
                      onChange={handleInputChange}
                      required
                      placeholder="your_username"
                      className="w-full px-3 py-2 bg-input border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
                    />
                  </div>

                  <div className="space-y-2">
                    <label className="text-sm font-semibold text-foreground">
                      Password *
                    </label>
                    <input
                      type="password"
                      name="password"
                      value={formData.password}
                      onChange={handleInputChange}
                      required
                      placeholder="••••••••"
                      className="w-full px-3 py-2 bg-input border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
                    />
                  </div>
                </>
              )}

              {authMethod === "keypair" && (
                <>
                  <div className="space-y-2">
                    <label className="text-sm font-semibold text-foreground">
                      Username *
                    </label>
                    <input
                      type="text"
                      name="username"
                      value={formData.username}
                      onChange={handleInputChange}
                      required
                      placeholder="your_username"
                      className="w-full px-3 py-2 bg-input border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
                    />
                  </div>

                  <div className="space-y-2">
                    <label className="text-sm font-semibold text-foreground">
                      Private Key (PEM format) *
                    </label>
                    <textarea
                      name="privateKey"
                      value={formData.privateKey}
                      onChange={(e) =>
                        setFormData({ ...formData, privateKey: e.target.value })
                      }
                      required
                      placeholder="-----BEGIN PRIVATE KEY-----&#10;...&#10;-----END PRIVATE KEY-----"
                      className="w-full px-3 py-2 bg-input border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring font-mono text-xs min-h-[120px] resize-y"
                    />
                  </div>

                  <div className="space-y-2">
                    <label className="text-sm font-semibold text-foreground">
                      Private Key Passphrase (Optional)
                    </label>
                    <input
                      type="password"
                      name="privateKeyPassphrase"
                      value={formData.privateKeyPassphrase}
                      onChange={handleInputChange}
                      placeholder="••••••••"
                      className="w-full px-3 py-2 bg-input border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
                    />
                  </div>
                </>
              )}

              {authMethod === "oauth" && (
                <>
                  <div className="space-y-2">
                    <label className="text-sm font-semibold text-foreground">
                      Client ID *
                    </label>
                    <input
                      type="text"
                      name="snowflakeClientId"
                      value={formData.snowflakeClientId}
                      onChange={handleInputChange}
                      required
                      placeholder="Your Snowflake Client ID"
                      className="w-full px-3 py-2 bg-input border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
                    />
                  </div>

                  <div className="space-y-2">
                    <label className="text-sm font-semibold text-foreground">
                      Client Secret *
                    </label>
                    <input
                      type="password"
                      name="snowflakeClientSecret"
                      value={formData.snowflakeClientSecret}
                      onChange={handleInputChange}
                      required
                      placeholder="Your Snowflake Client Secret"
                      className="w-full px-3 py-2 bg-input border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
                    />
                  </div>
                </>
              )}

              {authMethod === "sso" && (
                <div className="flex items-start gap-3 p-4 bg-primary/10 border border-primary/30 rounded-lg mt-4">
                  <svg
                    xmlns="http://www.w3.org/2000/svg"
                    className="w-5 h-5 text-primary mt-0.5 shrink-0"
                    fill="none"
                    viewBox="0 0 24 24"
                    stroke="currentColor"
                  >
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth={2}
                      d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
                    />
                  </svg>
                  <span className="text-sm text-primary">
                    SSO will redirect you to Snowflake for authentication. Make
                    sure to complete the connection setup after authentication.
                  </span>
                </div>
              )}

              <div className="flex justify-end gap-3 pt-4">
                <button
                  type="button"
                  className="px-4 py-2 border border-border rounded-lg text-foreground font-medium text-sm transition-colors hover:bg-muted"
                  onClick={() => setShowModal(false)}
                >
                  Cancel
                </button>
                <button
                  type="submit"
                  className="px-4 py-2 bg-primary text-primary-foreground rounded-lg font-semibold text-sm transition-all hover:bg-primary/90 disabled:opacity-50"
                  disabled={loading}
                >
                  {loading
                    ? "Processing..."
                    : authMethod === "sso"
                    ? "Login with SSO"
                    : "Create Connection"}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
};

export default SnowflakeConnections;
