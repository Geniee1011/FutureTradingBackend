import { config, useDatabento } from "./config.js";
import { MarketHub } from "./core/hub.js";
import { createMarketServer } from "./server/server.js";
import { SimulationProvider } from "./providers/simulation.js";
import { DatabentoProvider } from "./providers/databento.js";
import type { MarketDataProvider } from "./providers/provider.js";
import { INSTRUMENTS, SYMBOLS } from "./instruments.js";
import { computeContractCode } from "./contract-code.js";
import { AuthService } from "./auth/service.js";
import { createUserStore } from "./auth/store-factory.js";
import { AccountStream } from "./realtime/account-stream.js";

function buildProvider(): MarketDataProvider {
  if (useDatabento) {
    console.log("[provider] Databento live feed (Historical HTTP) — dataset", config.databento.dataset);
    return new DatabentoProvider(config.databento.apiKey, config.databento.dataset, config.databento.quotePollMs);
  }
  console.log("[provider] Simulation (no DATABENTO_API_KEY set) — random-walk prices");
  return new SimulationProvider();
}

// Auth. Postgres-backed (PgUserStore) when DATABASE_URL is set, else in-memory.
const auth = new AuthService(createUserStore());

const provider = buildProvider();
const hub = new MarketHub(provider);
hub.start();

// Real-time gateway for authenticated channels (positions/account/orders/admin).
const accountStream = new AccountStream(provider);
accountStream.start();

// Contract codes (root → e.g. ESM6). Seed with a date-based approximation so the
// endpoint is never empty; override with Databento-accurate codes when available.
const contractCodes: Record<string, string> = {};
for (const inst of INSTRUMENTS) {
  contractCodes[inst.symbol] = computeContractCode(inst.symbol, inst.category, Date.now());
}
if (provider instanceof DatabentoProvider) {
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
  contractCodes,
});

server.listen(config.port, () => {
  console.log(`TradingBackend listening on http://localhost:${config.port}`);
  console.log(`  WebSocket    ws://localhost:${config.port}/ws`);
  console.log(`  History      http://localhost:${config.port}/api/history?symbol=ES&resolution=60&count=240`);
  console.log(`  Instruments  http://localhost:${config.port}/api/instruments`);
  console.log(`  Auth         POST http://localhost:${config.port}/api/auth/login  ·  GET /api/auth/me`);
  console.log(`  Health       http://localhost:${config.port}/health`);
  console.log(`  Symbols      ${SYMBOLS.join(", ")}`);
});

function shutdown() {
  console.log("\nShutting down…");
  hub.stop();
  accountStream.stop();
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 2000).unref();
}
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
