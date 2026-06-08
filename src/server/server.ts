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
} from "../trading/repository.js";
import type { AccountStream } from "../realtime/account-stream.js";
import type { OrderEngine, PlaceOrderInput } from "../trading/order-engine.js";

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
  if (url.pathname === "/api/equity-curve" && req.method === "GET") {
    return void handleEquityCurve(req, res);
  }

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
  json(res, 200, result);
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
