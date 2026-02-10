import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react-swc";
import { defineConfig } from "vite";
import { resolve } from "node:path";

// https://vite.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": resolve(__dirname, "./src"),
    },
  },
  optimizeDeps: {
    include: ["@phosphor-icons/react"],
  },
  build: {
    target: "esnext",
    minify: "esbuild",
    outDir: "dist",
    sourcemap: false,
  },
  server: {
    host: "0.0.0.0",
    port: 5174,
    allowedHosts: [
      "localhost",
      "127.0.0.1",
      "hierarchybuilder.dataamplifier.io",
      ".dataamplifier.io",
    ],
  },
});
