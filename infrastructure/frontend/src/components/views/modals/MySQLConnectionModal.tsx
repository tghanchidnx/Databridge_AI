import React, { useState } from "react";
import { useAuthStore } from "@/stores/auth.store";
import { useOrganizationStore } from "@/stores/organization.store";
import { toast } from "sonner";
import { X } from "@phosphor-icons/react";
import { apiService } from "@/lib/api-service";

interface MySQLConnectionModalProps {
  isOpen: boolean;
  onClose: () => void;
  onSuccess: (connection: any) => void;
}

export const MySQLConnectionModal: React.FC<MySQLConnectionModalProps> = ({
  isOpen,
  onClose,
  onSuccess,
}) => {
  const { user } = useAuthStore();
  const { currentOrganization } = useOrganizationStore();
  const [formData, setFormData] = useState({
    connectionName: "",
    host: "",
    port: 3306,
    database: "",
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
      toast.loading("Creating MySQL connection...", {
        id: "create-connection",
      });

      // Call backend API to create MySQL connection
      const newConnection = await apiService.createMySQLConnection(formData);

      toast.success("MySQL connection created successfully!", {
        id: "create-connection",
      });
      onSuccess(newConnection);
    } catch (err: any) {
      toast.error(err.message || "Failed to create MySQL connection", {
        id: "create-connection",
      });
    }
  };

  const handleInputChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value =
      e.target.name === "port" ? parseInt(e.target.value) : e.target.value;
    setFormData({
      ...formData,
      [e.target.name]: value,
    });
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="bg-background border border-border rounded-lg shadow-xl w-full max-w-2xl max-h-[90vh] overflow-y-auto m-4">
        {/* Header */}
        <div className="flex items-center justify-between p-6 border-b border-border">
          <div className="flex items-center gap-3">
            <span className="text-3xl">🐬</span>
            <h2 className="text-2xl font-bold text-foreground">
              Add MySQL Connection
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
              placeholder="My MySQL Connection"
              className="w-full px-3 py-2 bg-input border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
            />
          </div>

          {/* Host and Port */}
          <div className="grid grid-cols-3 gap-4">
            <div className="space-y-2 col-span-2">
              <label className="text-sm font-semibold text-foreground">
                Host *
              </label>
              <input
                type="text"
                name="host"
                value={formData.host}
                onChange={handleInputChange}
                required
                placeholder="localhost or 192.168.1.100"
                className="w-full px-3 py-2 bg-input border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
              />
            </div>
            <div className="space-y-2">
              <label className="text-sm font-semibold text-foreground">
                Port *
              </label>
              <input
                type="number"
                name="port"
                value={formData.port}
                onChange={handleInputChange}
                required
                placeholder="3306"
                className="w-full px-3 py-2 bg-input border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
              />
            </div>
          </div>

          {/* Database */}
          <div className="space-y-2">
            <label className="text-sm font-semibold text-foreground">
              Database *
            </label>
            <input
              type="text"
              name="database"
              value={formData.database}
              onChange={handleInputChange}
              required
              placeholder="my_database"
              className="w-full px-3 py-2 bg-input border border-border rounded-lg text-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring"
            />
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
              placeholder="root"
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

          {/* Info Box */}
          <div className="flex items-start gap-3 p-3 bg-blue-500/10 border border-blue-500/30 rounded-lg">
            <svg
              xmlns="http://www.w3.org/2000/svg"
              className="w-5 h-5 text-blue-600 shrink-0 mt-0.5"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
              strokeWidth={2}
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
              />
            </svg>
            <span className="text-sm text-blue-600">
              Connection will be tested before being saved
            </span>
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
