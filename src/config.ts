import "dotenv/config";

function num(name: string, fallback: number): number {
  const raw = process.env[name];
  const n = raw ? Number(raw) : NaN;
  return Number.isFinite(n) ? n : fallback;
}

export const config = {
  port: num("PORT", 8000),
  corsOrigin: process.env.CORS_ORIGIN ?? "*",
  databaseUrl: process.env.DATABASE_URL?.trim() ?? "",

  jwt: {
    secret: process.env.JWT_SECRET?.trim() || "dev-insecure-secret-change-me",
    expiresInSec: num("JWT_EXPIRES_IN_SEC", 7 * 24 * 60 * 60), // 7 days
  },

  databento: {
    apiKey: process.env.DATABENTO_API_KEY?.trim() ?? "",
    dataset: process.env.DATABENTO_DATASET?.trim() || "GLBX.MDP3",
    quotePollMs: num("QUOTE_POLL_MS", 1500),
  },
} as const;

if (config.jwt.secret === "dev-insecure-secret-change-me") {
  console.warn("[auth] JWT_SECRET not set — using an insecure dev secret. Set JWT_SECRET in production.");
}

/** Use the live Databento feed only when an API key is present. */
export const useDatabento = config.databento.apiKey.length > 0;

/** Use PostgreSQL (pg) for persistence when a connection string is present. */
export const useDatabase = config.databaseUrl.length > 0;
