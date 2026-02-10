import { useState, useEffect } from "react";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Textarea } from "@/components/ui/textarea";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
  DialogFooter,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Database,
  Lightning,
  Trash,
  PencilSimple,
  CircleNotch,
  CheckCircle,
  Warning,
  Key,
  ShieldCheck,
  UserCircle,
} from "@phosphor-icons/react";
import { toast } from "sonner";
import type { Connection } from "@/types";
import { useAuth } from "@/contexts/AuthContext";
import { apiService } from "@/lib/api-service";
import { SnowflakeConnections } from "@/components/views/SnowflakeConnections";

export function ConnectionsView() {
  const { currentWorkspace, user } = useAuth();
  const [connections, setConnections] = useState<Connection[]>([]);
  const [isDialogOpen, setIsDialogOpen] = useState(false);
  const [editingConnection, setEditingConnection] = useState<Connection | null>(
    null
  );
  const [isTesting, setIsTesting] = useState(false);
  const [isSaving, setIsSaving] = useState(false);
  const [snowflakeOAuthToken, setSnowflakeOAuthToken] = useState<{
    access: string;
    refresh?: string;
  } | null>(null);
  const [formData, setFormData] = useState({
    name: "",
    type: "snowflake" as Connection["type"],
    host: "",
    port: 443,
    database: "",
    username: "",
    password: "",
    authType: "password" as "password" | "keypair" | "oauth" | "sso",
    warehouse: "",
    schema: "",
    role: "",
    account: "",
    privateKey: "",
  });

  // Initialize from localStorage
  useEffect(() => {
    const stored = localStorage.getItem("connections");
    if (stored) {
      setConnections(JSON.parse(stored));
    } else {
      setConnections([]);
    }
  }, []);

  // Persist to localStorage
  useEffect(() => {
    if (connections.length > 0) {
      localStorage.setItem("connections", JSON.stringify(connections));
    }
  }, [connections]);

  // Fetch connections from API on mount
  useEffect(() => {
    if (user?.id) {
      fetchConnections();
    }
  }, [user?.id]);

  const fetchConnections = async () => {
    if (!user?.id) return;

    try {
      const apiConnections = await apiService.fetchConnections(user.id);
      // Merge with local connections
      setConnections(apiConnections as any);
    } catch (error: any) {
      console.error("Failed to fetch connections:", error);
      // Keep using local connections on error
    }
  };

  const workspaceConnections = (connections ?? []).filter(
    (conn) => conn.workspaceId === currentWorkspace?.id
  );

  const handleOpenDialog = (connection?: Connection) => {
    if (connection) {
      setEditingConnection(connection);
      setFormData({
        name: connection.name,
        type: connection.type,
        host: connection.host,
        port: connection.port,
        database: connection.database,
        username: connection.username,
        password: "",
        authType: connection.authType || "password",
        warehouse: connection.warehouse || "",
        schema: connection.schema || "",
        role: connection.role || "",
        account: connection.account || "",
        privateKey: "",
      });
    } else {
      setEditingConnection(null);
      setFormData({
        name: "",
        type: "snowflake",
        host: "",
        port: 443,
        database: "",
        username: "",
        password: "",
        authType: "password",
        warehouse: "",
        schema: "",
        role: "",
        account: "",
        privateKey: "",
      });
    }
    setIsDialogOpen(true);
  };

  const handleSaveConnection = async () => {
    if (!currentWorkspace) {
      toast.error("No workspace selected");
      return;
    }

    if (!user?.id) {
      toast.error("User not authenticated");
      return;
    }

    setIsSaving(true);

    try {
      if (formData.authType === "oauth" && formData.type === "snowflake") {
        // OAuth connection
        if (!snowflakeOAuthToken) {
          toast.error("Please authenticate with Snowflake OAuth first");
          setIsSaving(false);
          return;
        }

        const connection = await apiService.createOAuthConnection({
          userId: user.id,
          connectionName: formData.name,
          connectionType: "snowflake",
          accessToken: snowflakeOAuthToken.access,
          refreshToken: snowflakeOAuthToken.refresh,
        });

        setConnections((current) => [...(current ?? []), connection as any]);
        toast.success("Snowflake OAuth connection created successfully");
      } else {
        // Password-based connection
        const connection = await apiService.createPasswordConnection({
          userId: user.id,
          connectionName: formData.name,
          connectionType: formData.type,
          host: formData.host,
          port: formData.port,
          database: formData.database,
          username: formData.username,
          password: formData.password,
        });

        setConnections((current) => [...(current ?? []), connection as any]);
        toast.success("Connection created successfully");
      }

      setIsDialogOpen(false);
      setSnowflakeOAuthToken(null);
    } catch (error: any) {
      toast.error(error.message || "Failed to create connection");
    } finally {
      setIsSaving(false);
    }
  };

  const handleDeleteConnection = async (id: string) => {
    try {
      await apiService.removeConnection(id);
      setConnections((current) =>
        (current ?? []).filter((conn) => conn.id !== id)
      );
      toast.success("Connection deleted");
    } catch (error: any) {
      toast.error(error.message || "Failed to delete connection");
    }
  };

  const handleTestConnection = async (id: string) => {
    const connection = connections?.find((c) => c.id === id);
    if (!connection) return;

    setIsTesting(true);
    toast.loading("Testing connection...", { id: "test-conn" });

    try {
      if (connection.authType === "oauth") {
        await apiService.validateOAuthConnection(id);
      } else {
        // For password connections, we'll use the test endpoint
        await apiService.testPasswordConnection({
          connectionName: connection.name,
          connectionType: connection.type,
          host: connection.host,
          port: connection.port,
          database: connection.database,
          username: connection.username,
          password: "", // Password not stored
        });
      }

      setConnections((current) =>
        (current ?? []).map((conn) =>
          conn.id === id
            ? {
                ...conn,
                status: "connected",
                lastUsed: new Date().toISOString(),
              }
            : conn
        )
      );
      toast.success("Connection successful", { id: "test-conn" });
    } catch (error: any) {
      setConnections((current) =>
        (current ?? []).map((conn) =>
          conn.id === id ? { ...conn, status: "error" } : conn
        )
      );
      toast.error(error.message || "Connection failed", { id: "test-conn" });
    } finally {
      setIsTesting(false);
    }
  };

  // Snowflake OAuth handler
  const handleSnowflakeOAuth = () => {
    const SNOWFLAKE_ACCOUNT = import.meta.env.VITE_SNOWFLAKE_ACCOUNT;
    const SNOWFLAKE_CLIENT_ID = import.meta.env.VITE_SNOWFLAKE_CLIENT_ID;
    const REDIRECT_URI = window.location.origin + "/auth/snowflake-callback";

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
    authUrl.searchParams.append(
      "state",
      Math.random().toString(36).substring(2)
    );

    // Open OAuth in popup
    const popup = window.open(
      authUrl.toString(),
      "Snowflake OAuth",
      "width=600,height=700"
    );

    // Listen for OAuth callback
    window.addEventListener("message", (event) => {
      if (event.data.type === "snowflake-oauth") {
        setSnowflakeOAuthToken({
          access: event.data.accessToken,
          refresh: event.data.refreshToken,
        });
        toast.success("Snowflake authentication successful!");
        popup?.close();
      }
    });
  };

  const getDbIcon = (type: Connection["type"]) => {
    return <Database weight="fill" className="h-5 w-5" />;
  };

  const getStatusBadge = (status: Connection["status"]) => {
    const variants = {
      connected: {
        variant: "default" as const,
        icon: <CheckCircle weight="fill" className="h-3 w-3 mr-1" />,
      },
      disconnected: {
        variant: "secondary" as const,
        icon: <CircleNotch className="h-3 w-3 mr-1" />,
      },
      error: {
        variant: "destructive" as const,
        icon: <Warning weight="fill" className="h-3 w-3 mr-1" />,
      },
    };
    const config = variants[status];
    return (
      <Badge
        variant={config.variant}
        className="text-xs flex items-center w-fit"
      >
        {config.icon}
        {status}
      </Badge>
    );
  };

  const formatLastUsed = (dateString: string) => {
    const date = new Date(dateString);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMs / 3600000);

    if (diffMins < 60) return `${diffMins} min ago`;
    if (diffHours < 24)
      return `${diffHours} hour${diffHours > 1 ? "s" : ""} ago`;
    return date.toLocaleDateString();
  };

  const renderSnowflakeAuthForm = () => {
    return (
      <Tabs
        value={formData.authType}
        onValueChange={(value) =>
          setFormData({
            ...formData,
            authType: value as typeof formData.authType,
          })
        }
      >
        <TabsList className="grid w-full grid-cols-4">
          <TabsTrigger value="password" className="text-xs">
            <UserCircle className="w-3 h-3 mr-1" />
            Password
          </TabsTrigger>
          <TabsTrigger value="keypair" className="text-xs">
            <Key className="w-3 h-3 mr-1" />
            Key Pair
          </TabsTrigger>
          <TabsTrigger value="oauth" className="text-xs">
            <ShieldCheck className="w-3 h-3 mr-1" />
            OAuth
          </TabsTrigger>
          <TabsTrigger value="sso" className="text-xs">
            <ShieldCheck className="w-3 h-3 mr-1" />
            SSO
          </TabsTrigger>
        </TabsList>

        <TabsContent value="password" className="space-y-4 mt-4">
          <div className="grid gap-2">
            <Label htmlFor="conn-username">Username</Label>
            <Input
              id="conn-username"
              placeholder="db_user"
              value={formData.username}
              onChange={(e) =>
                setFormData({ ...formData, username: e.target.value })
              }
            />
          </div>
          <div className="grid gap-2">
            <Label htmlFor="conn-password">Password</Label>
            <Input
              id="conn-password"
              type="password"
              placeholder={
                editingConnection
                  ? "Leave blank to keep current"
                  : "Enter password"
              }
              value={formData.password}
              onChange={(e) =>
                setFormData({ ...formData, password: e.target.value })
              }
            />
          </div>
        </TabsContent>

        <TabsContent value="keypair" className="space-y-4 mt-4">
          <div className="grid gap-2">
            <Label htmlFor="conn-username-key">Username</Label>
            <Input
              id="conn-username-key"
              placeholder="db_user"
              value={formData.username}
              onChange={(e) =>
                setFormData({ ...formData, username: e.target.value })
              }
            />
          </div>
          <div className="grid gap-2">
            <Label htmlFor="conn-private-key">Private Key</Label>
            <Textarea
              id="conn-private-key"
              placeholder="-----BEGIN ENCRYPTED PRIVATE KEY-----&#10;...&#10;-----END ENCRYPTED PRIVATE KEY-----"
              value={formData.privateKey}
              onChange={(e) =>
                setFormData({ ...formData, privateKey: e.target.value })
              }
              rows={6}
              className="font-mono text-xs"
            />
            <p className="text-xs text-muted-foreground">
              Paste your encrypted private key in PEM format
            </p>
          </div>
          <div className="grid gap-2">
            <Label htmlFor="conn-key-passphrase">
              Passphrase (if encrypted)
            </Label>
            <Input
              id="conn-key-passphrase"
              type="password"
              placeholder="Private key passphrase"
              value={formData.password}
              onChange={(e) =>
                setFormData({ ...formData, password: e.target.value })
              }
            />
          </div>
        </TabsContent>

        <TabsContent value="oauth" className="space-y-4 mt-4">
          <div className="rounded-lg border border-border bg-muted/50 p-4 space-y-3">
            <div className="flex items-center gap-2">
              <ShieldCheck className="w-5 h-5 text-primary" weight="fill" />
              <h4 className="font-semibold">OAuth 2.0 Authentication</h4>
            </div>
            <p className="text-sm text-muted-foreground">
              Click "Authorize" to authenticate with Snowflake using OAuth 2.0.
              You'll be redirected to Snowflake's login page.
            </p>
            {snowflakeOAuthToken ? (
              <div className="flex items-center gap-2 text-sm text-green-600">
                <CheckCircle weight="fill" className="w-4 h-4" />
                <span>Successfully authenticated with Snowflake</span>
              </div>
            ) : (
              <Button
                variant="outline"
                className="w-full"
                onClick={handleSnowflakeOAuth}
                type="button"
              >
                <ShieldCheck className="w-4 h-4 mr-2" />
                Authorize with Snowflake
              </Button>
            )}
          </div>
          <div className="grid gap-2">
            <Label htmlFor="conn-username-oauth">Username</Label>
            <Input
              id="conn-username-oauth"
              placeholder="Your Snowflake username"
              value={formData.username}
              onChange={(e) =>
                setFormData({ ...formData, username: e.target.value })
              }
            />
          </div>
        </TabsContent>

        <TabsContent value="sso" className="space-y-4 mt-4">
          <div className="rounded-lg border border-border bg-muted/50 p-4 space-y-3">
            <div className="flex items-center gap-2">
              <ShieldCheck className="w-5 h-5 text-primary" weight="fill" />
              <h4 className="font-semibold">Single Sign-On (SSO)</h4>
            </div>
            <p className="text-sm text-muted-foreground">
              Authenticate using your organization's identity provider (Okta,
              Azure AD, etc.)
            </p>
            <Button variant="outline" className="w-full">
              Sign in with SSO
            </Button>
          </div>
          <div className="grid gap-2">
            <Label htmlFor="conn-username-sso">Username</Label>
            <Input
              id="conn-username-sso"
              placeholder="Your SSO username"
              value={formData.username}
              onChange={(e) =>
                setFormData({ ...formData, username: e.target.value })
              }
            />
          </div>
        </TabsContent>
      </Tabs>
    );
  };

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold tracking-tight">
            Database Connections
          </h1>
          <p className="text-muted-foreground mt-2">
            Manage secure connections to your databases
          </p>
        </div>
      </div>

      <Tabs defaultValue="all" className="w-full">
        <TabsList className="grid w-full max-w-md grid-cols-2">
          <TabsTrigger value="all">All Connections</TabsTrigger>
          <TabsTrigger value="snowflake">Snowflake Management</TabsTrigger>
        </TabsList>

        <TabsContent value="all" className="space-y-6 mt-6">
          <div className="flex justify-end">
            <Dialog open={isDialogOpen} onOpenChange={setIsDialogOpen}>
              <DialogTrigger asChild>
                <Button onClick={() => handleOpenDialog()} className="gap-2">
                  <Lightning weight="fill" className="h-4 w-4" />
                  Add Connection
                </Button>
              </DialogTrigger>
              <DialogContent className="max-w-2xl max-h-[90vh] overflow-y-auto">
                <DialogHeader>
                  <DialogTitle>
                    {editingConnection
                      ? "Edit Connection"
                      : "Add New Connection"}
                  </DialogTitle>
                  <DialogDescription>
                    Configure your database connection details
                  </DialogDescription>
                </DialogHeader>
                <div className="grid gap-4 py-4">
                  <div className="grid gap-2">
                    <Label htmlFor="conn-name">Connection Name</Label>
                    <Input
                      id="conn-name"
                      placeholder="Production Database"
                      value={formData.name}
                      onChange={(e) =>
                        setFormData({ ...formData, name: e.target.value })
                      }
                    />
                  </div>

                  <div className="grid gap-2">
                    <Label htmlFor="conn-type">Database Type</Label>
                    <Select
                      value={formData.type}
                      onValueChange={(value) =>
                        setFormData({
                          ...formData,
                          type: value as Connection["type"],
                        })
                      }
                    >
                      <SelectTrigger id="conn-type">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="snowflake">Snowflake</SelectItem>
                        <SelectItem value="postgresql">PostgreSQL</SelectItem>
                        <SelectItem value="mysql">MySQL</SelectItem>
                        <SelectItem value="sqlserver">SQL Server</SelectItem>
                        <SelectItem value="oracle">Oracle</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>

                  {formData.type === "snowflake" && (
                    <>
                      <div className="grid gap-2">
                        <Label htmlFor="conn-account">Account Identifier</Label>
                        <Input
                          id="conn-account"
                          placeholder="myorg-account123"
                          value={formData.account}
                          onChange={(e) =>
                            setFormData({
                              ...formData,
                              account: e.target.value,
                            })
                          }
                        />
                        <p className="text-xs text-muted-foreground">
                          Format: organization-account or account.region.cloud
                        </p>
                      </div>

                      <div className="grid grid-cols-2 gap-4">
                        <div className="space-y-2">
                          <Label htmlFor="conn-warehouse">Warehouse</Label>
                          <Input
                            id="conn-warehouse"
                            placeholder="COMPUTE_WH"
                            value={formData.warehouse}
                            onChange={(e) =>
                              setFormData({
                                ...formData,
                                warehouse: e.target.value,
                              })
                            }
                          />
                        </div>
                        <div className="space-y-2">
                          <Label htmlFor="conn-role">Role (Optional)</Label>
                          <Input
                            id="conn-role"
                            placeholder="SYSADMIN"
                            value={formData.role}
                            onChange={(e) =>
                              setFormData({ ...formData, role: e.target.value })
                            }
                          />
                        </div>
                      </div>
                    </>
                  )}

                  {formData.type !== "snowflake" && (
                    <div className="grid grid-cols-4 gap-4">
                      <div className="col-span-3 space-y-2">
                        <Label htmlFor="conn-host">Host</Label>
                        <Input
                          id="conn-host"
                          placeholder="db.example.com"
                          value={formData.host}
                          onChange={(e) =>
                            setFormData({ ...formData, host: e.target.value })
                          }
                        />
                      </div>
                      <div className="space-y-2">
                        <Label htmlFor="conn-port">Port</Label>
                        <Input
                          id="conn-port"
                          type="number"
                          value={formData.port}
                          onChange={(e) =>
                            setFormData({
                              ...formData,
                              port: parseInt(e.target.value),
                            })
                          }
                        />
                      </div>
                    </div>
                  )}

                  <div className="grid gap-2">
                    <Label htmlFor="conn-database">Database</Label>
                    <Input
                      id="conn-database"
                      placeholder={
                        formData.type === "snowflake"
                          ? "PRODUCTION_DB"
                          : "production_db"
                      }
                      value={formData.database}
                      onChange={(e) =>
                        setFormData({ ...formData, database: e.target.value })
                      }
                    />
                  </div>

                  {formData.type === "snowflake" && (
                    <div className="grid gap-2">
                      <Label htmlFor="conn-schema">Schema (Optional)</Label>
                      <Input
                        id="conn-schema"
                        placeholder="PUBLIC"
                        value={formData.schema}
                        onChange={(e) =>
                          setFormData({ ...formData, schema: e.target.value })
                        }
                      />
                    </div>
                  )}

                  {formData.type === "snowflake" ? (
                    <div className="space-y-4">
                      <div className="border-t border-border pt-4">
                        <Label className="text-base">
                          Authentication Method
                        </Label>
                        <p className="text-xs text-muted-foreground mt-1 mb-3">
                          Choose how you want to authenticate with Snowflake
                        </p>
                        {renderSnowflakeAuthForm()}
                      </div>
                    </div>
                  ) : (
                    <>
                      <div className="grid gap-2">
                        <Label htmlFor="conn-username-basic">Username</Label>
                        <Input
                          id="conn-username-basic"
                          placeholder="db_user"
                          value={formData.username}
                          onChange={(e) =>
                            setFormData({
                              ...formData,
                              username: e.target.value,
                            })
                          }
                        />
                      </div>

                      <div className="grid gap-2">
                        <Label htmlFor="conn-password-basic">Password</Label>
                        <Input
                          id="conn-password-basic"
                          type="password"
                          placeholder={
                            editingConnection
                              ? "Leave blank to keep current"
                              : "Enter password"
                          }
                          value={formData.password}
                          onChange={(e) =>
                            setFormData({
                              ...formData,
                              password: e.target.value,
                            })
                          }
                        />
                      </div>
                    </>
                  )}
                </div>
                <DialogFooter>
                  <Button
                    variant="outline"
                    onClick={() => setIsDialogOpen(false)}
                  >
                    Cancel
                  </Button>
                  <Button
                    onClick={handleSaveConnection}
                    disabled={
                      isSaving ||
                      (formData.authType === "oauth" && !snowflakeOAuthToken)
                    }
                  >
                    {isSaving ? (
                      <>
                        <CircleNotch className="w-4 h-4 mr-2 animate-spin" />
                        Saving...
                      </>
                    ) : editingConnection ? (
                      "Save Changes"
                    ) : (
                      "Add Connection"
                    )}
                  </Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>
          </div>

          <div className="grid gap-6 sm:grid-cols-2 lg:grid-cols-3">
            {workspaceConnections.map((connection) => (
              <Card key={connection.id} className="relative overflow-hidden">
                <CardHeader className="pb-3">
                  <div className="flex items-start justify-between">
                    <div className="flex items-center gap-3">
                      <div className="flex h-10 w-10 items-center justify-center rounded-md bg-primary/10 text-primary">
                        {getDbIcon(connection.type)}
                      </div>
                      <div>
                        <CardTitle className="text-base">
                          {connection.name}
                        </CardTitle>
                        <CardDescription className="text-xs capitalize">
                          {connection.type}
                          {connection.authType && ` · ${connection.authType}`}
                        </CardDescription>
                      </div>
                    </div>
                    {getStatusBadge(connection.status)}
                  </div>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="space-y-2 text-sm">
                    {connection.type === "snowflake" && connection.account && (
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">Account:</span>
                        <span className="font-mono text-xs truncate max-w-[180px]">
                          {connection.account}
                        </span>
                      </div>
                    )}
                    {connection.type !== "snowflake" && (
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">Host:</span>
                        <span className="font-mono text-xs truncate max-w-[180px]">
                          {connection.host}
                        </span>
                      </div>
                    )}
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Database:</span>
                      <span className="font-mono text-xs truncate max-w-[180px]">
                        {connection.database}
                      </span>
                    </div>
                    {connection.warehouse && (
                      <div className="flex justify-between">
                        <span className="text-muted-foreground">
                          Warehouse:
                        </span>
                        <span className="font-mono text-xs truncate max-w-[180px]">
                          {connection.warehouse}
                        </span>
                      </div>
                    )}
                    <div className="flex justify-between">
                      <span className="text-muted-foreground">Last Used:</span>
                      <span className="text-xs">
                        {formatLastUsed(connection.lastUsed)}
                      </span>
                    </div>
                  </div>

                  <div className="flex gap-2">
                    <Button
                      variant="outline"
                      size="sm"
                      className="flex-1"
                      onClick={() => handleTestConnection(connection.id)}
                      disabled={isTesting}
                    >
                      {isTesting ? (
                        <>
                          <CircleNotch className="h-4 w-4 mr-1 animate-spin" />
                          Testing...
                        </>
                      ) : (
                        <>
                          <Lightning className="h-4 w-4 mr-1" />
                          Test
                        </>
                      )}
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => handleOpenDialog(connection)}
                    >
                      <PencilSimple className="h-4 w-4" />
                    </Button>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => handleDeleteConnection(connection.id)}
                    >
                      <Trash className="h-4 w-4" />
                    </Button>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>

          {workspaceConnections.length === 0 && (
            <Card>
              <CardContent className="flex flex-col items-center justify-center py-16">
                <Database className="h-16 w-16 text-muted-foreground mb-4" />
                <h3 className="text-lg font-semibold mb-2">
                  No connections yet
                </h3>
                <p className="text-sm text-muted-foreground mb-6 text-center max-w-md">
                  Add your first database connection to get started with schema
                  comparisons and data operations
                </p>
                <Button onClick={() => handleOpenDialog()} className="gap-2">
                  <Lightning weight="fill" className="h-4 w-4" />
                  Add Connection
                </Button>
              </CardContent>
            </Card>
          )}
        </TabsContent>

        <TabsContent value="snowflake" className="mt-6">
          <SnowflakeConnections />
        </TabsContent>
      </Tabs>
    </div>
  );
}
