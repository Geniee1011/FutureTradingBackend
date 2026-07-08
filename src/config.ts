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

  /**
   * Market-data delivery model — lets us switch between the two licensing models
   * without code changes:
   *  - "shared" (Model A): one master Databento key, fanned out to all users.
   *    Simple, but counts as redistribution (needs a redistribution license).
   *  - "byo"    (Model B): each user brings their own Databento account; the app
   *    streams only data their own license covers (no redistribution).
   *  - "dxfeed" : real-time via dxFeed's dxLink WebSocket feed (additive; leaves
   *    the Databento paths untouched). Selected only when explicitly set.
   * Defaults to "shared" (current behaviour). See README "Market-data models".
   */
  marketDataMode: (
    process.env.MARKET_DATA_MODE === "byo"
      ? "byo"
      : process.env.MARKET_DATA_MODE === "dxfeed"
        ? "dxfeed"
        : "shared"
  ) as "shared" | "byo" | "dxfeed",

  /** dxFeed (dxLink) real-time feed — used only when MARKET_DATA_MODE=dxfeed.
   *  Defaults to the public demo endpoint so the wiring can be validated before
   *  the operator drops in a production endpoint + token. */
  dxfeed: {
    endpoint: process.env.DXFEED_ENDPOINT?.trim() || "wss://demo.dxfeed.com/market-data/dxlink-ws",
    token: process.env.DXFEED_TOKEN?.trim() ?? "",
  },

  jwt: {
    secret: process.env.JWT_SECRET?.trim() || "dev-insecure-secret-change-me",
    expiresInSec: num("JWT_EXPIRES_IN_SEC", 7 * 24 * 60 * 60), // 7 days
  },

  databento: {
    apiKey: process.env.DATABENTO_API_KEY?.trim() ?? "",
    dataset: process.env.DATABENTO_DATASET?.trim() || "GLBX.MDP3",
    quotePollMs: num("QUOTE_POLL_MS", 1500),
    /** Use the real-time Live TCP feed instead of Historical HTTP polling. */
    live: process.env.DATABENTO_LIVE === "1",
  },

  /** Secret used to encrypt each user's Databento key at rest (Model B / byo).
   *  Any long random string; required only when MARKET_DATA_MODE=byo. */
  marketDataEncKey: process.env.MARKET_DATA_ENC_KEY?.trim() ?? "",

  /** Bootstrap admin (optional). When both are set, the backend ensures an ADMIN
   *  user with this email exists on startup — creating it, or promoting an
   *  existing user of that email to ADMIN. Lets you provision an admin on a fresh
   *  deploy without the seed/CLI. */
  adminEmail: process.env.ADMIN_EMAIL?.trim() ?? "",
  adminPassword: process.env.ADMIN_PASSWORD ?? "",

  /** Auto-seed demo data on a fresh (empty) database at boot. Opt-in because the
   *  seed creates well-known demo credentials — DO NOT enable in production. */
  seedDemo: process.env.SEED_DEMO === "1",
} as const;

if (config.jwt.secret === "dev-insecure-secret-change-me") {
  console.warn("[auth] JWT_SECRET not set — using an insecure dev secret. Set JWT_SECRET in production.");
}

/** Use the live Databento feed only when an API key is present. */
export const useDatabento = config.databento.apiKey.length > 0;

/** Use PostgreSQL (pg) for persistence when a connection string is present. */
export const useDatabase = config.databaseUrl.length > 0;
