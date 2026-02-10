import { create } from "zustand";
import { persist } from "zustand/middleware";
import { snowflakeAPI, type Connection } from "@/lib/snowflake-api";
import { toast } from "sonner";

interface ConnectionsState {
  connections: Connection[];
  loading: boolean;
  error: string | null;
  lastFetched: number | null;

  // Actions
  loadConnections: (force?: boolean) => Promise<void>;
  refreshConnections: () => Promise<void>;
  addConnection: (connection: Connection) => void;
  updateConnection: (id: string, updates: Partial<Connection>) => void;
  removeConnection: (id: string) => void;
  getConnection: (id: string) => Connection | undefined;
  clearConnections: () => void;
}

const CACHE_DURATION = 5 * 60 * 1000; // 5 minutes

export const useConnectionsStore = create<ConnectionsState>()(
  persist(
    (set, get) => ({
      connections: [],
      loading: false,
      error: null,
      lastFetched: null,

      loadConnections: async (force = false) => {
        const { lastFetched, loading } = get();

        // Skip if already loading
        if (loading) return;

        // Use cache if available and not forcing refresh
        if (
          !force &&
          lastFetched &&
          Date.now() - lastFetched < CACHE_DURATION
        ) {
          console.log("📦 Using cached connections");
          return;
        }

        set({ loading: true, error: null });

        try {
          console.log("🔄 Fetching connections from API...");
          const connections = await snowflakeAPI.getConnections();

          set({
            connections,
            loading: false,
            error: null,
            lastFetched: Date.now(),
          });

          console.log(`✅ Loaded ${connections.length} connections`);
        } catch (err: any) {
          const errorMsg = err.message || "Failed to load connections";
          console.error("❌ Failed to load connections:", err);

          set({
            loading: false,
            error: errorMsg,
          });

          // Don't show toast for auth errors
          if (!errorMsg.includes("401") && !errorMsg.includes("Unauthorized")) {
            toast.error("Failed to load connections", {
              description: errorMsg,
            });
          }
        }
      },

      refreshConnections: async () => {
        console.log("🔄 Forcing refresh of connections");
        await get().loadConnections(true);
        toast.success("Connections refreshed");
      },

      addConnection: (connection) => {
        set((state) => ({
          connections: [...state.connections, connection],
          lastFetched: Date.now(),
        }));
      },

      updateConnection: (id, updates) => {
        set((state) => ({
          connections: state.connections.map((conn) =>
            conn.id === id ? { ...conn, ...updates } : conn
          ),
          lastFetched: Date.now(),
        }));
      },

      removeConnection: (id) => {
        set((state) => ({
          connections: state.connections.filter((conn) => conn.id !== id),
          lastFetched: Date.now(),
        }));
      },

      getConnection: (id) => {
        return get().connections.find((conn) => conn.id === id);
      },

      clearConnections: () => {
        set({
          connections: [],
          loading: false,
          error: null,
          lastFetched: null,
        });
      },
    }),
    {
      name: "connections-storage",
      partialize: (state) => ({
        connections: state.connections,
        lastFetched: state.lastFetched,
      }),
    }
  )
);
