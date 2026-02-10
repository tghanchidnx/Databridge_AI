/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_API_URL: string;
  readonly VITE_BACKEND_URL: string;
  readonly VITE_MS_CLIENT_ID: string;
  readonly VITE_MS_TENANT_ID: string;
  readonly VITE_SNOWFLAKE_CLIENT_SECRET: string;
  readonly VITE_SNOWFLAKE_CLIENT_ID: string;
  readonly VITE_SNOWFLAKE_ACCOUNT: string;
  readonly VITE_FRONT_END_URL: string;
  readonly VITE_FRONT_END_CALLBACK: string;
  readonly VITE_GOOGLE_CLIENT_ID: string;
  readonly VITE_GOOGLE_CLIENT_SECRET: string;
  readonly VITE_OPENAI_API_KEY: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
