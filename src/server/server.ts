import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { WebSocketServer, type WebSocket } from "ws";
import type { MarketHub } from "../core/hub.js";
import { INSTRUMENTS, SYMBOLS } from "../instruments.js";
import type { AuthService } from "../auth/service.js";
import { bearerToken, verifyToken } from "../auth/jwt.js";
import { useDatabase } from "../config.js";
import {
  createEvaluationAccount,
  getAccountDetail,
  getAccountIdByUserId,
  getEquityCurve,
  listOrders,
  listPositions,
  listTransactions,
  listViolations,
} from "../trading/repository.js";
import type { AccountStream } from "../realtime/account-stream.js";
import type { OrderEngine, PlaceOrderInput } from "../trading/order-engine.js";
import {
  adminAdjustBalance,
  adminGetTraderDetail,
  adminListAccounts,
  adminListActivity,
  adminListRules,
  adminListTraders,
  adminListViolations,
  adminResetAccount,
  adminSetAccountStatus,
  adminSetTraderStatus,
  adminUpdateRule,
  logActivity,
  type AdminAction,
} from "../trading/admin-repository.js";
import { getPool } from "../db/pool.js";

interface ServerOptions {
  port: number;
  corsOrigin: string;
  providerName: string;
  auth: AuthService;
  accountStream: AccountStream;
  orderEngine: OrderEngine;
  /** Mutable root→contract-code map (filled in asynchronously after startup). */
  contractCodes: Record<string, string>;
}

const HEARTBEAT_MS = 30_000;

/** Build the combined HTTP (REST history + health) and WebSocket (/ws) server. */
export function createMarketServer(hub: MarketHub, opts: ServerOptions) {
  const http = createServer((req, res) => handleHttp(req, res, hub, opts));
  const wss = new WebSocketServer({ noServer: true });

  // Track liveness for heartbeat.
  const alive = new WeakMap<WebSocket, boolean>();

  wss.on("connection", (ws: WebSocket) => {
    hub.addClient(ws);
    opts.accountStream.addClient(ws);
    alive.set(ws, true);
    ws.on("pong", () => alive.set(ws, true));
    ws.on("message", (data) => {
      const text = data.toString();
      hub.handleMessage(ws, text); // market-data channel
      void opts.accountStream.handleMessage(ws, text); // positions/account/orders/admin
    });
    ws.on("close", () => {
      hub.removeClient(ws);
      opts.accountStream.removeClient(ws);
    });
    ws.on("error", () => ws.terminate());
  });

  http.on("upgrade", (req, socket, head) => {
    const url = new URL(req.url ?? "/", "http://localhost");
    if (url.pathname !== "/ws") {
      socket.destroy();
      return;
    }
    wss.handleUpgrade(req, socket, head, (ws) => wss.emit("connection", ws, req));
  });

  // Heartbeat: drop clients that stop responding to pings.
  const heartbeat = setInterval(() => {
    for (const ws of wss.clients) {
      if (alive.get(ws) === false) {
        ws.terminate();
        continue;
      }
      alive.set(ws, false);
      ws.ping();
    }
  }, HEARTBEAT_MS);

  http.on("close", () => clearInterval(heartbeat));

  return http;
}

function handleHttp(req: IncomingMessage, res: ServerResponse, hub: MarketHub, opts: ServerOptions) {
  res.setHeader("Access-Control-Allow-Origin", opts.corsOrigin);
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");

  if (req.method === "OPTIONS") {
    res.writeHead(204).end();
    return;
  }

  const url = new URL(req.url ?? "/", "http://localhost");

  // --- Auth ---
  if (url.pathname === "/api/auth/login" && req.method === "POST") {
    return void handleLogin(req, res, opts.auth);
  }
  if (url.pathname === "/api/auth/register" && req.method === "POST") {
    return void handleRegister(req, res, opts.auth);
  }
  if (url.pathname === "/api/auth/me" && req.method === "GET") {
    return void handleMe(req, res, opts.auth);
  }

  // --- Trading (authenticated, DB-backed) ---
  if (url.pathname === "/api/positions" && req.method === "GET") {
    return void handlePositions(req, res);
  }
  if (url.pathname === "/api/orders" && req.method === "GET") {
    return void handleOrders(req, res);
  }
  if (url.pathname === "/api/orders" && req.method === "POST") {
    return void handlePlaceOrder(req, res, opts.orderEngine);
  }
  if (/^\/api\/orders\/[^/]+\/cancel$/.test(url.pathname) && req.method === "POST") {
    return void handleCancelOrder(url, req, res, opts.orderEngine);
  }
  if (url.pathname === "/api/positions/close" && req.method === "POST") {
    return void handleClosePosition(req, res, opts.orderEngine);
  }
  if (url.pathname === "/api/account" && req.method === "GET") {
    return void handleAccount(req, res);
  }
  if (url.pathname === "/api/transactions" && req.method === "GET") {
    return void handleTransactions(req, res);
  }
  if (url.pathname === "/api/violations" && req.method === "GET") {
    return void handleViolations(req, res);
  }
  if (url.pathname === "/api/equity-curve" && req.method === "GET") {
    return void handleEquityCurve(req, res);
  }

  // --- Admin (ADMIN role only) ---
  if (url.pathname === "/api/admin/traders" && req.method === "GET") return void handleAdmin(req, res, adminListTraders);
  if (/^\/api\/admin\/traders\/[^/]+$/.test(url.pathname) && req.method === "GET")
    return void handleAdmin(req, res, () => adminGetTraderDetail(url.pathname.split("/")[4]!));
  if (url.pathname === "/api/admin/accounts" && req.method === "GET") return void handleAdmin(req, res, adminListAccounts);
  if (url.pathname === "/api/admin/activity" && req.method === "GET") return void handleAdmin(req, res, () => adminListActivity(200));
  if (url.pathname === "/api/admin/violations" && req.method === "GET") return void handleAdmin(req, res, () => adminListViolations(200));
  if (url.pathname === "/api/admin/rules" && req.method === "GET") return void handleAdmin(req, res, adminListRules);
  if (/^\/api\/admin\/traders\/[^/]+\/status$/.test(url.pathname) && req.method === "POST")
    return void handleAdminStatus(url, req, res, opts.accountStream, "trader");
  if (/^\/api\/admin\/accounts\/[^/]+\/status$/.test(url.pathname) && req.method === "POST")
    return void handleAdminStatus(url, req, res, opts.accountStream, "account");
  if (/^\/api\/admin\/accounts\/[^/]+\/(reset|adjust-balance|close-all|liquidate|cancel-orders)$/.test(url.pathname) && req.method === "POST")
    return void handleAdminAccountAction(url, req, res, opts.orderEngine, opts.accountStream);
  if (/^\/api\/admin\/rules\/[^/]+$/.test(url.pathname) && req.method === "POST")
    return void handleAdminRuleUpdate(url, req, res);

  if (url.pathname === "/health") {
    return json(res, 200, {
      status: "ok",
      provider: opts.providerName,
      symbols: SYMBOLS,
      clients: hub.clientCount(),
    });
  }

  if (url.pathname === "/api/instruments") {
    return json(
      res,
      200,
      INSTRUMENTS.map((i) => ({
        symbol: i.symbol,
        name: i.name,
        category: i.category,
        pricePrecision: i.pricePrecision,
        tickSize: i.tickSize,
        contractCode: opts.contractCodes[i.symbol] ?? i.symbol,
      })),
    );
  }

  if (url.pathname === "/api/history" && req.method === "GET") {
    return void handleHistory(url, res, hub);
  }

  json(res, 404, { error: "not found" });
}

async function handleHistory(url: URL, res: ServerResponse, hub: MarketHub) {
  const symbol = url.searchParams.get("symbol");
  const resolution = Number(url.searchParams.get("resolution") ?? "60");
  const count = Math.min(Number(url.searchParams.get("count") ?? "240"), 1000);

  if (!symbol || !SYMBOLS.includes(symbol)) {
    return json(res, 400, { error: "invalid or missing symbol" });
  }
  if (!Number.isFinite(resolution) || resolution <= 0) {
    return json(res, 400, { error: "invalid resolution" });
  }

  try {
    const candles = await hub.getProvider().getHistory(symbol, resolution, count);
    json(res, 200, candles);
  } catch (err) {
    console.error("[history] error:", (err as Error).message);
    json(res, 502, { error: "history fetch failed" });
  }
}

async function handleLogin(req: IncomingMessage, res: ServerResponse, auth: AuthService) {
  const body = await readJson<{ email?: string; password?: string }>(req);
  if (!body?.email || !body?.password) return json(res, 400, { error: "email and password required" });
  const result = await auth.login(body.email, body.password);
  if (!result) return json(res, 401, { error: "Invalid email or password." });
  // Audit the login (best-effort; never blocks auth).
  if (useDatabase) {
    void (async () => {
      try {
        const accountId = await getAccountIdByUserId(result.user.id);
        if (accountId) await logActivity(getPool(), accountId, "USER_LOGIN", "Signed in", clientIp(req));
      } catch {
        /* ignore audit failures */
      }
    })();
  }
  json(res, 200, result);
}

/** Best-effort client IP for audit logs. */
function clientIp(req: IncomingMessage): string | undefined {
  const fwd = req.headers["x-forwarded-for"];
  if (typeof fwd === "string" && fwd) return fwd.split(",")[0]!.trim();
  return req.socket.remoteAddress ?? undefined;
}

/** True only for a valid ADMIN-role bearer token. */
function requireAdmin(req: IncomingMessage): boolean {
  if (!useDatabase) return false;
  const payload = verifyToken(bearerToken(req.headers.authorization) ?? "");
  return !!payload && payload.role === "ADMIN";
}

async function handleAdmin(req: IncomingMessage, res: ServerResponse, load: () => Promise<unknown>) {
  if (!requireAdmin(req)) return json(res, 403, { error: "admin access required" });
  try {
    json(res, 200, await load());
  } catch (err) {
    console.error("[admin] query failed:", (err as Error).message);
    json(res, 500, { error: "admin query failed" });
  }
}

async function handleAdminStatus(url: URL, req: IncomingMessage, res: ServerResponse, accountStream: AccountStream, kind: "trader" | "account") {
  if (!requireAdmin(req)) return json(res, 403, { error: "admin access required" });
  const id = url.pathname.split("/")[4]!; // /api/admin/{traders|accounts}/:id/status
  const body = await readJson<{ status?: string }>(req);
  const action: AdminAction | null = body?.status === "suspended" ? "suspended" : body?.status === "active" ? "active" : null;
  if (!action) return json(res, 400, { error: "status must be 'active' or 'suspended'" });
  const ok = kind === "trader" ? await adminSetTraderStatus(id, action) : await adminSetAccountStatus(id, action);
  if (ok) accountStream.publishAdminUpdate({ kind: `${kind}_${action}`, id }); // live-refresh admin dashboards
  json(res, ok ? 200 : 404, { ok, error: ok ? undefined : "not found or no change" });
}

async function handleAdminAccountAction(url: URL, req: IncomingMessage, res: ServerResponse, orderEngine: OrderEngine, accountStream: AccountStream) {
  if (!requireAdmin(req)) return json(res, 403, { error: "admin access required" });
  const parts = url.pathname.split("/");
  const accountId = parts[4]!; // /api/admin/accounts/:id/:action
  const action = parts[5]!;
  try {
    let result: Record<string, unknown> = { ok: true };
    switch (action) {
      case "reset": {
        const ok = await adminResetAccount(accountId);
        if (!ok) return json(res, 404, { ok: false, error: "account not found" });
        break;
      }
      case "adjust-balance": {
        const body = await readJson<{ amount?: number }>(req);
        const amount = Number(body?.amount);
        if (!Number.isFinite(amount) || amount === 0) return json(res, 400, { error: "amount must be a non-zero number" });
        const ok = await adminAdjustBalance(accountId, amount);
        if (!ok) return json(res, 404, { ok: false, error: "account not found" });
        result = { ok: true, amount };
        break;
      }
      case "close-all":
        result = { ok: true, closed: await orderEngine.closeAllPositions(accountId) };
        break;
      case "cancel-orders":
        result = { ok: true, cancelled: await orderEngine.cancelAllOrders(accountId) };
        break;
      case "liquidate": {
        // Flatten at market WHILE still ACTIVE (closes only fill on active accounts), then freeze.
        const closed = await orderEngine.closeAllPositions(accountId);
        await adminSetAccountStatus(accountId, "suspended");
        result = { ok: true, closed };
        break;
      }
      default:
        return json(res, 400, { error: "unknown action" });
    }
    await accountStream.refreshAccount(accountId).catch(() => {});
    accountStream.publishAdminUpdate({ kind: `account_${action}`, id: accountId });
    json(res, 200, result);
  } catch (err) {
    console.error(`[admin] account action ${action} failed:`, (err as Error).message);
    json(res, 500, { error: "action failed" });
  }
}

async function handleAdminRuleUpdate(url: URL, req: IncomingMessage, res: ServerResponse) {
  if (!requireAdmin(req)) return json(res, 403, { error: "admin access required" });
  const accountId = url.pathname.split("/")[4]!; // /api/admin/rules/:accountId
  const body = await readJson<{ maxDailyLoss?: number; maxDrawdown?: number; profitTarget?: number; maxContracts?: number; allowedInstruments?: string[] }>(req);
  if (!body) return json(res, 400, { error: "body required" });
  const ok = await adminUpdateRule(accountId, body);
  json(res, ok ? 200 : 400, { ok, error: ok ? undefined : "no valid fields to update" });
}

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

async function handleRegister(req: IncomingMessage, res: ServerResponse, auth: AuthService) {
  const body = await readJson<{ email?: string; password?: string; name?: string }>(req);
  const name = body?.name?.trim();
  const email = body?.email?.trim();
  const password = body?.password ?? "";

  if (!name || !email || !password) return json(res, 400, { error: "Name, email and password are required." });
  if (name.length < 2) return json(res, 400, { error: "Please enter your full name." });
  if (!EMAIL_RE.test(email)) return json(res, 400, { error: "Please enter a valid email address." });
  if (password.length < 6) return json(res, 400, { error: "Password must be at least 6 characters." });

  try {
    const result = await auth.register({ email, password, name });
    // Provision a default evaluation account so the new trader has real data.
    if (useDatabase) {
      try {
        await createEvaluationAccount(result.user.id);
      } catch (e) {
        console.error("[register] account provisioning failed:", (e as Error).message);
      }
    }
    json(res, 201, result);
  } catch (err) {
    const message = (err as Error).message;
    const status = message === "email already registered" ? 409 : 500;
    json(res, status, { error: status === 409 ? "That email is already registered." : "Registration failed." });
  }
}

async function handleMe(req: IncomingMessage, res: ServerResponse, auth: AuthService) {
  const token = bearerToken(req.headers.authorization);
  if (!token) return json(res, 401, { error: "missing bearer token" });
  const user = await auth.me(token);
  if (!user) return json(res, 401, { error: "invalid or expired token" });
  json(res, 200, { user });
}

/** Resolve the caller's account id from the bearer token, or null if unauthorized. */
async function requireAccount(req: IncomingMessage): Promise<string | null> {
  if (!useDatabase) return null;
  const payload = verifyToken(bearerToken(req.headers.authorization) ?? "");
  if (!payload) return null;
  return getAccountIdByUserId(payload.sub);
}

async function handlePositions(req: IncomingMessage, res: ServerResponse) {
  const accountId = await requireAccount(req);
  if (!accountId) return json(res, 401, { error: "unauthorized" });
  json(res, 200, await listPositions(accountId));
}

async function handleOrders(req: IncomingMessage, res: ServerResponse) {
  const accountId = await requireAccount(req);
  if (!accountId) return json(res, 401, { error: "unauthorized" });
  json(res, 200, await listOrders(accountId));
}

async function handleAccount(req: IncomingMessage, res: ServerResponse) {
  const accountId = await requireAccount(req);
  if (!accountId) return json(res, 401, { error: "unauthorized" });
  const account = await getAccountDetail(accountId);
  if (!account) return json(res, 404, { error: "account not found" });
  json(res, 200, account);
}

async function handleTransactions(req: IncomingMessage, res: ServerResponse) {
  const accountId = await requireAccount(req);
  if (!accountId) return json(res, 401, { error: "unauthorized" });
  json(res, 200, await listTransactions(accountId));
}

async function handleEquityCurve(req: IncomingMessage, res: ServerResponse) {
  const accountId = await requireAccount(req);
  if (!accountId) return json(res, 401, { error: "unauthorized" });
  json(res, 200, await getEquityCurve(accountId));
}

async function handleViolations(req: IncomingMessage, res: ServerResponse) {
  const accountId = await requireAccount(req);
  if (!accountId) return json(res, 401, { error: "unauthorized" });
  json(res, 200, await listViolations(accountId));
}

async function handlePlaceOrder(req: IncomingMessage, res: ServerResponse, engine: OrderEngine) {
  const accountId = await requireAccount(req);
  if (!accountId) return json(res, 401, { error: "unauthorized" });
  const body = await readJson<PlaceOrderInput>(req);
  if (!body?.symbol || !body.side || !body.type || !body.quantity) {
    return json(res, 400, { error: "symbol, side, type and quantity are required" });
  }
  const result = await engine.place(accountId, body);
  json(res, result.ok ? 201 : 400, result);
}

async function handleCancelOrder(url: URL, req: IncomingMessage, res: ServerResponse, engine: OrderEngine) {
  const accountId = await requireAccount(req);
  if (!accountId) return json(res, 401, { error: "unauthorized" });
  const orderId = url.pathname.split("/")[3]; // /api/orders/:id/cancel
  if (!orderId) return json(res, 400, { error: "order id is required" });
  const result = await engine.cancel(accountId, decodeURIComponent(orderId));
  json(res, result.ok ? 200 : 400, result);
}

async function handleClosePosition(req: IncomingMessage, res: ServerResponse, engine: OrderEngine) {
  const accountId = await requireAccount(req);
  if (!accountId) return json(res, 401, { error: "unauthorized" });
  const body = await readJson<{ symbol?: string }>(req);
  if (!body?.symbol) return json(res, 400, { error: "symbol is required" });
  const result = await engine.closePosition(accountId, body.symbol);
  json(res, result.ok ? 201 : 400, result);
}

/** Read and JSON-parse a request body (max 1 MB). */
async function readJson<T>(req: IncomingMessage): Promise<T | null> {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > 1_000_000) return null;
    chunks.push(chunk as Buffer);
  }
  if (chunks.length === 0) return null;
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8")) as T;
  } catch {
    return null;
  }
}

function json(res: ServerResponse, status: number, body: unknown) {
  res.writeHead(status, { "Content-Type": "application/json" });
  res.end(JSON.stringify(body));
}
