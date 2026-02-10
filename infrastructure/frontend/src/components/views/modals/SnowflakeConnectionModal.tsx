import React, { useState } from "react";
import {
  snowflakeAPI,
  type SnowflakePasswordConnectionRequest,
} from "@/lib/snowflake-api";
import { useAuthStore } from "@/stores/auth.store";
import { useOrganizationStore } from "@/stores/organization.store";
import { toast } from "sonner";
import { X } from "@phosphor-icons/react";

interface SnowflakeConnectionModalProps {
  isOpen: boolean;
  onClose: () => void;
  onSuccess: (connection: any) => void;
}

export const SnowflakeConnectionModal: React.FC<
  SnowflakeConnectionModalProps
> = ({ isOpen, onClose, onSuccess }) => {
  const { user } = useAuthStore();
  const { currentOrganization } = useOrganizationStore();
  const [authMethod, setAuthMethod] = useState<
    "password" | "keypair" | "oauth" | "sso"
  >("password");
  const [formData, setFormData] = useState({
    connectionName: "",
    snowflakeAccount: "",
    snowflakeWarehouse: "COMPUTE_WH",
    snowflakeRole: "SYSADMIN",
    databaseName: "",
    schemaName: "",
    username: "",
    password: "",
  });

  if (!isOpen) return null;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (!user) {
      toast.error("Please log in to create a connection");
      return;
    }

    try {
      toast.loading("Creating connection...", { id: "create-connection" });
      const request: SnowflakePasswordConnectionRequest = {
        connectionName: formData.connectionName,
        username: formData.username,
        password: formData.password,
        snowflakeAccount: formData.snowflakeAccount,
        snowflakeWarehouse: formData.snowflakeWarehouse,
        databaseName: formData.databaseName,
        schemaName: formData.schemaName,
      };

      const newConnection = await snowflakeAPI.createPasswordConnection(
        request
      );
      toast.success("Snowflake connection created successfully!", {
        id: "create-connection",
      });
      onSuccess(newConnection);
    } catch (err: any) {
      toast.error(err.message || "Failed to create connection", {
        id: "create-connection",
      });
    }
  };

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setFormData({
      ...formData,
      [e.target.name]: e.target.value,
    });
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="bg-background border border-border rounded-lg shadow-xl w-full max-w-2xl max-h-[90vh] overflow-y-auto m-4">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-border">
          <div className="flex items-center gap-3">
            <span className="text-3xl">❄️</span>
            <h2 className="text-2xl font-bold text-foreground">
              Add Snowflake Connection
            </h2>
          </div>
          <button
            onClick={onClose}
            className="text-muted-foreground hover:text-foreground transition-colors"
          >
            <X className="w-6 h-6" />
          </button>
        </div>

        {/* Content */}
        <form onSubmit={handleSubmit} className="p-6 space-y-4">
          {/* Organization Badge */}
          {currentOrganization && (
            <div className="flex items-center gap-3 p-3 bg-primary/10 border border-primary/30 rounded-lg">
              <svg
                xmlns="http://www.w3.org/2000/svg"
                className="w-5 h-5 text-primary"
                fill="none"
                viewBox="0 0 24 24"
                stroke="currentColor"
                strokeWidth={2}
              >
                <path d="M19 21V5a2 2 0 00-2-2H7a2 2 0 00-2 2v16m14 0h2m-2 0h-5m-9 0H3m2 0h5M9 7h1m-1 4h1m4-4h1m-1 4h1m-5 10v-5a1 1 0 011-1h2a1 1 0 011 1v5m-4 0h4" />
              </svg>
              <span className="text-sm text-primary">
                Workspace: <strong>{currentOrganization.name}</strong>
              </span>
            </div>
          )}

          {/* Connection Name */}
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

          {/* Snowflake Account */}
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
              placeholder="account.region.cloud (e.g., xy12345.us-east-1.aws)"
              className="w-full px-3 py-2 bg-input border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
            />
          </div>

          {/* Warehouse and Role */}
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
                Role
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

          {/* Database and Schema */}
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
                placeholder="MY_DATABASE"
                className="w-full px-3 py-2 bg-input border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
              />
            </div>
            <div className="space-y-2">
              <label className="text-sm font-semibold text-foreground">
                Schema *
              </label>
              <input
                type="text"
                name="schemaName"
                value={formData.schemaName}
                onChange={handleInputChange}
                required
                placeholder="PUBLIC"
                className="w-full px-3 py-2 bg-input border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
              />
            </div>
          </div>

          {/* Username and Password */}
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

          {/* Footer */}
          <div className="flex items-center justify-end gap-3 pt-4 border-t border-border">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 bg-background border border-border text-foreground rounded-lg hover:bg-accent transition-colors font-semibold"
            >
              Cancel
            </button>
            <button
              type="submit"
              className="px-6 py-2 bg-primary text-primary-foreground rounded-lg hover:bg-primary/90 transition-colors font-semibold"
            >
              Create Connection
            </button>
          </div>
        </form>
      </div>
    </div>
  );
};
