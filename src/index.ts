import { config, useDatabento, useDatabase } from "./config.js";
import { MarketHub } from "./core/hub.js";
import { createMarketServer } from "./server/server.js";
import { SimulationProvider } from "./providers/simulation.js";
import { DatabentoProvider } from "./providers/databento.js";
import { DatabentoLiveProvider } from "./providers/databento-live.js";
import type { MarketDataProvider } from "./providers/provider.js";
import { INSTRUMENTS, SYMBOLS } from "./instruments.js";
import { computeContractCode } from "./contract-code.js";
import { AuthService } from "./auth/service.js";
import { createUserStore } from "./auth/store-factory.js";
import { ensureBootstrapAdmin } from "./auth/bootstrap-admin.js";
import { seedIfEmpty } from "./db/seed.js";
import { byoSessions } from "./market-data/byo-session.js";
import { AccountStream } from "./realtime/account-stream.js";
import { OrderEngine } from "./trading/order-engine.js";
import { RiskEngine } from "./trading/risk-engine.js";
import { startResetSweeper } from "./trading/reset-sweeper.js";

function buildProvider(): MarketDataProvider {
  // Model B — "byo": per-user (bring-your-own) Databento accounts. Chart data
  // (history + polled quote) is served per-request from each user's OWN key via
  // REST (/api/history, /api/market-data/quote) — NO shared market-data fan-out,
  // which is what keeps Model B out of redistribution. The shared provider below
  // therefore carries no real market data; it only backs the simulated execution
  // engine (order fills / mark for the eval), so charts are the user's real data
  // while trading runs on the simulation. (Per-user fill pricing is a follow-up.)
  if (config.marketDataMode === "byo") {
    if (!config.marketDataEncKey) {
      console.warn("[provider] MARKET_DATA_MODE=byo but MARKET_DATA_ENC_KEY is unset — users cannot connect a key.");
    }
    console.log("[provider] Market-data model: BYO (Model B — per-user keys; charts via REST, execution simulated)");
    return new SimulationProvider();
  }

  // Model A — "shared": one master key, fanned out to all users (current behaviour).
  console.log("[provider] Market-data model: SHARED (Model A — single master key, fanned out)");
  if (useDatabento && config.databento.live) {
    console.log("[provider] Databento LIVE feed (raw TCP / DBN) — dataset", config.databento.dataset);
    return new DatabentoLiveProvider(config.databento.apiKey, config.databento.dataset);
  }
  if (useDatabento) {
    console.log("[provider] Databento feed (Historical HTTP polling) — dataset", config.databento.dataset);
    return new DatabentoProvider(config.databento.apiKey, config.databento.dataset, config.databento.quotePollMs);
  }
  console.log("[provider] Simulation (no DATABENTO_API_KEY set) — random-walk prices");
  return new SimulationProvider();
}

// Auth. Postgres-backed (PgUserStore) when DATABASE_URL is set, else in-memory.
const auth = new AuthService(createUserStore());

// Provision an admin from ADMIN_EMAIL/ADMIN_PASSWORD if set (no seed/CLI needed).
if (useDatabase) {
  void ensureBootstrapAdmin().catch((e) => console.error("[admin] bootstrap failed:", (e as Error).message));
}

const provider = buildProvider();
const hub = new MarketHub(provider);
hub.start();

// Real-time gateway for authenticated channels (positions/account/orders/admin).
const accountStream = new AccountStream(provider);
accountStream.start();

// Model B: the shared provider streams nothing (no one subscribes to it), so feed
// each user's real per-user live prices into the mark map — this is what gives the
// execution engine fill prices for market orders and live marks for position P&L.
let houseFeed: DatabentoLiveProvider | null = null;
if (config.marketDataMode === "byo") {
  byoSessions.setMarkSink((symbol, price) => accountStream.setExternalMark(symbol, price));

  // House mark feed: ONE always-on server-side LIVE session on the operator's own key
  // (DATABENTO_API_KEY), used purely for INTERNAL marks — admin P&L, resting SL/TP/limit
  // triggering, and market-order fills. Without it the mark map only warms while some
  // trader session is live, so with everyone offline (or right after a restart) the admin
  // P&L reads $0 and resting stop-losses never fire. This is NOT wired into MarketHub, so
  // it's never fanned out to users as chart data — charts stay strictly per-user (their own
  // key via REST), preserving Model B's no-redistribution property for market data.
  if (useDatabento && config.databento.live && config.databento.apiKey) {
    houseFeed = new DatabentoLiveProvider(config.databento.apiKey, config.databento.dataset);
    houseFeed.on("quote", (q) => accountStream.setExternalMark(q.symbol, q.price));
    houseFeed.start();
    console.log("[provider] House mark feed (Databento LIVE, operator key) — internal marks only, all instruments");
  } else {
    console.warn(
      "[provider] BYO mode without a house mark feed (DATABENTO_API_KEY / DATABENTO_LIVE unset): " +
        "admin P&L and resting-order triggering only work while a trader session is live.",
    );
  }
}

// Order engine (real, Postgres-backed) — fills market orders, nets positions,
// and runs the resting-order monitor that triggers limit/stop orders.
const orderEngine = new OrderEngine(accountStream);
orderEngine.start();

// Risk/evaluation engine — enforces daily-loss / drawdown / profit-target rules.
// Driven by the AccountStream tick on live equity; liquidates + fails or passes.
const riskEngine = new RiskEngine(orderEngine, accountStream);
accountStream.setRiskEngine(riskEngine);

// Auto-reset sweeper: resets FAILED accounts 12h after the trader requests it (self-service).
if (useDatabase) startResetSweeper(accountStream);

// Contract codes (root → e.g. ESM6). Seed with a date-based approximation so the
// endpoint is never empty; override with Databento-accurate codes when available.
const contractCodes: Record<string, string> = {};
for (const inst of INSTRUMENTS) {
  contractCodes[inst.symbol] = computeContractCode(inst.symbol, inst.category, Date.now());
}
if (provider instanceof DatabentoProvider || provider instanceof DatabentoLiveProvider) {
  void provider
    .resolveContractCodes()
    .then((map) => {
      Object.assign(contractCodes, map);
      const n = Object.keys(map).length;
      if (n) console.log(`[contracts] resolved ${n} live contract codes from Databento`);
    })
    .catch(() => {});
}

const server = createMarketServer(hub, {
  port: config.port,
  corsOrigin: config.corsOrigin,
  providerName: provider.name,
  auth,
  accountStream,
  orderEngine,
  contractCodes,
});

server.on("error", (err: NodeJS.ErrnoException) => {
  if (err.code === "EADDRINUSE") {
    console.error(
      `\n[fatal] Port ${config.port} is already in use — another TradingBackend is probably running.\n` +
        `        Stop it first, or start this one on a different port with  PORT=8001 node dist/index.js\n`,
    );
    process.exit(1);
  }
  console.error("[fatal] server error:", err);
  process.exit(1);
});

server.listen(config.port, () => {
  console.log(`TradingBackend listening on http://localhost:${config.port}`);
  console.log(`  WebSocket    ws://localhost:${config.port}/ws`);
  console.log(`  History      http://localhost:${config.port}/api/history?symbol=ES&resolution=60&count=240`);
  console.log(`  Instruments  http://localhost:${config.port}/api/instruments`);
  console.log(`  Auth         POST http://localhost:${config.port}/api/auth/login  ·  GET /api/auth/me`);
  console.log(`  Health       http://localhost:${config.port}/health`);
  console.log(`  Symbols      ${SYMBOLS.join(", ")}`);

  // Auto-seed demo data on a fresh DB (SEED_DEMO=1 only). Runs after the server
  // is listening so the seed's live-mark fetch (own WS) can connect.
  if (useDatabase) {
    void seedIfEmpty().catch((e) => console.error("[seed] auto-seed failed:", (e as Error).message));
  }
});

function shutdown() {
  console.log("\nShutting down…");
  hub.stop();
  houseFeed?.stop();
  accountStream.stop();
  orderEngine.stop();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 2000).unref();
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

// Last-resort guard: a transient fault (e.g. a Postgres ECONNRESET on a detached
// background task — daily-stats poll, WS message handler) must not take the whole
// server down. Log it and keep serving; the DB pool reconnects on the next query.
process.on("unhandledRejection", (reason) => {
  console.error("[process] unhandled rejection:", reason instanceof Error ? reason.message : reason);
});
