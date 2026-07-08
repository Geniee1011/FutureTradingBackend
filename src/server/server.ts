import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { WebSocketServer, type WebSocket } from "ws";
import type { MarketHub } from "../core/hub.js";
import { INSTRUMENTS, SYMBOLS } from "../instruments.js";
import type { AuthService } from "../auth/service.js";
import { bearerToken, verifyToken } from "../auth/jwt.js";
import { config, useDatabase } from "../config.js";
import {
  byoConfigured,
  clearUserDatabentoKey,
  getUserDatabentoKey,
  hasUserDatabentoKey,
  setUserDatabentoKey,
  validateDatabentoKey,
} from "../market-data/byo-repository.js";
import { fetchUserHistory, fetchUserQuote } from "../market-data/byo-data.js";
import { byoSessions } from "../market-data/byo-session.js";
import {
  createEvaluationAccount,
  getAccountDetail,
  getAccountIdByUserId,
  getEquityCurve,
  listOrders,
  listPositions,
  listTransactions,
  listViolations,
  requestAccountReset,
} from "../trading/repository.js";
import type { AccountStream } from "../realtime/account-stream.js";
import type { OrderEngine, PlaceOrderInput } from "../trading/order-engine.js";
import {
  adminAdjustBalance,
  adminGetTraderDetail,
  adminListAccounts,
  adminListActivity,
  adminListClosedPositions,
  adminListOpenPositions,
  adminListRules,
  adminListRuleTemplates,
  adminListTraders,
  adminListViolations,
  adminListPendingReviews,
  adminReviewDecision,
  adminAssignTier,
  adminResetAccount,
  adminSetAccountStatus,
  adminSetTraderStatus,
  adminUpdateRule,
  adminUpdateRuleTemplate,
  logActivity,
  type AdminAction,
} from "../trading/admin-repository.js";
import { analyticsOverall, analyticsTrader } from "../trading/analytics-repository.js";
import { listPhaseRules, createPhaseRule, updatePhaseRule, deletePhaseRule } from "../trading/phase-rules.js";
import { recomputeAllPhases } from "../trading/trader-stats.js";
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
  const http = createServer((req, res) => {
    // Catch any rejection from an async route handler (e.g. a transient DB ECONNRESET)
    // so it returns a 500 instead of becoming an unhandled rejection that crashes the
    // process. handleHttp returns the handler's promise (no longer fire-and-forget).
    Promise.resolve(handleHttp(req, res, hub, opts)).catch((err) => {
      console.error(`[http] ${req.method} ${req.url} failed:`, (err as Error).message);
      if (!res.headersSent) {
        try {
          res.writeHead(500, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ error: "internal server error" }));
        } catch {
          res.end();
        }
      }
    });
  });
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
    return handleLogin(req, res, opts.auth);
  }
  if (url.pathname === "/api/auth/register" && req.method === "POST") {
    return handleRegister(req, res, opts.auth);
  }
  if (url.pathname === "/api/auth/me" && req.method === "GET") {
    return handleMe(req, res, opts.auth);
  }
  if (url.pathname === "/api/auth/change-password" && req.method === "POST") {
    return handleChangePassword(req, res, opts.auth);
  }

  // --- Market-data connection (Model B / byo: each user's own Databento key) ---
  if (url.pathname === "/api/market-data/connection" && req.method === "GET") {
    return handleByoConnectionStatus(req, res);
  }
  if (url.pathname === "/api/market-data/connection" && req.method === "POST") {
    return handleByoConnect(req, res);
  }
  if (url.pathname === "/api/market-data/connection" && req.method === "DELETE") {
    return handleByoDisconnect(req, res);
  }

  // --- Trading (authenticated, DB-backed) ---
  if (url.pathname === "/api/positions" && req.method === "GET") {
    return handlePositions(req, res);
  }
  if (url.pathname === "/api/orders" && req.method === "GET") {
    return handleOrders(req, res);
  }
  if (url.pathname === "/api/orders" && req.method === "POST") {
    return handlePlaceOrder(req, res, opts.orderEngine);
  }
  if (/^\/api\/orders\/[^/]+\/cancel$/.test(url.pathname) && req.method === "POST") {
    return handleCancelOrder(url, req, res, opts.orderEngine);
  }
  if (/^\/api\/orders\/[^/]+\/modify$/.test(url.pathname) && req.method === "POST") {
    return handleModifyOrder(url, req, res, opts.orderEngine);
  }
  if (url.pathname === "/api/positions/close" && req.method === "POST") {
    return handleClosePosition(req, res, opts.orderEngine);
  }
  if (url.pathname === "/api/positions/bracket" && req.method === "POST") {
    return handlePositionBracket(req, res, opts.orderEngine);
  }
  if (url.pathname === "/api/account" && req.method === "GET") {
    return handleAccount(req, res);
  }
  if (url.pathname === "/api/account/request-reset" && req.method === "POST") {
    return handleRequestReset(req, res, opts.accountStream);
  }
  if (url.pathname === "/api/transactions" && req.method === "GET") {
    return handleTransactions(req, res);
  }
  if (url.pathname === "/api/violations" && req.method === "GET") {
    return handleViolations(req, res);
  }
  if (url.pathname === "/api/equity-curve" && req.method === "GET") {
    return handleEquityCurve(req, res);
  }

  // --- Admin (ADMIN role only) ---
  if (url.pathname === "/api/admin/traders" && req.method === "GET") return handleAdmin(req, res, adminListTraders);
  if (/^\/api\/admin\/traders\/[^/]+$/.test(url.pathname) && req.method === "GET")
    return handleAdmin(req, res, () => adminGetTraderDetail(url.pathname.split("/")[4]!));
  if (url.pathname === "/api/admin/accounts" && req.method === "GET") return handleAdmin(req, res, adminListAccounts);
  if (url.pathname === "/api/admin/activity" && req.method === "GET") return handleAdmin(req, res, () => adminListActivity(200));
  if (url.pathname === "/api/admin/violations" && req.method === "GET") return handleAdmin(req, res, () => adminListViolations(200));
  if (url.pathname === "/api/admin/positions" && req.method === "GET")
    return handleAdmin(req, res, async () => ({
      // Pass the live quote feed so each open lot is marked-to-market (stored unrealized is never live).
      open: await adminListOpenPositions((s) => opts.accountStream.getMarkPrice(s)),
      closed: await adminListClosedPositions(500),
    }));
  if (url.pathname === "/api/admin/reviews" && req.method === "GET") return handleAdmin(req, res, adminListPendingReviews);
  if (/^\/api\/admin\/reviews\/[^/]+$/.test(url.pathname) && req.method === "POST")
    return handleAdminReviewDecision(url, req, res, opts.accountStream);
  // Analytics (behavioural risk-phase dashboards).
  if (url.pathname === "/api/admin/analytics/overall" && req.method === "GET") return handleAdmin(req, res, analyticsOverall);
  if (/^\/api\/admin\/analytics\/trader\/[^/]+$/.test(url.pathname) && req.method === "GET")
    return handleAdmin(req, res, () => analyticsTrader(url.pathname.split("/")[5]!));
  // Phase rules CRUD (editable ruleset behind the phase engine).
  if (url.pathname === "/api/admin/phase-rules" && req.method === "GET") return handleAdmin(req, res, listPhaseRules);
  if (url.pathname === "/api/admin/phase-rules" && req.method === "POST")
    return handleAdminPhaseRule(url, req, res, opts.accountStream, "create");
  if (/^\/api\/admin\/phase-rules\/[^/]+$/.test(url.pathname) && (req.method === "PATCH" || req.method === "POST"))
    return handleAdminPhaseRule(url, req, res, opts.accountStream, "update");
  if (/^\/api\/admin\/phase-rules\/[^/]+$/.test(url.pathname) && req.method === "DELETE")
    return handleAdminPhaseRule(url, req, res, opts.accountStream, "delete");
  if (url.pathname === "/api/admin/rules" && req.method === "GET") return handleAdmin(req, res, adminListRules);
  if (url.pathname === "/api/admin/rule-templates" && req.method === "GET") return handleAdmin(req, res, adminListRuleTemplates);
  if (/^\/api\/admin\/rule-templates\/[^/]+$/.test(url.pathname) && req.method === "POST")
    return handleAdminRuleTemplateUpdate(url, req, res);
  if (/^\/api\/admin\/traders\/[^/]+\/status$/.test(url.pathname) && req.method === "POST")
    return handleAdminStatus(url, req, res, opts.accountStream, "trader");
  if (/^\/api\/admin\/traders\/[^/]+\/password$/.test(url.pathname) && req.method === "POST")
    return handleAdminResetPassword(url, req, res, opts.auth);
  if (/^\/api\/admin\/accounts\/[^/]+\/status$/.test(url.pathname) && req.method === "POST")
    return handleAdminStatus(url, req, res, opts.accountStream, "account");
  if (/^\/api\/admin\/accounts\/[^/]+\/(reset|adjust-balance|close-all|liquidate|cancel-orders|assign-tier)$/.test(url.pathname) && req.method === "POST")
    return handleAdminAccountAction(url, req, res, opts.orderEngine, opts.accountStream);
  if (/^\/api\/admin\/rules\/[^/]+$/.test(url.pathname) && req.method === "POST")
    return handleAdminRuleUpdate(url, req, res);

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
    // Model B: serve chart history from the requesting user's OWN Databento key.
    if (config.marketDataMode === "byo") return handleByoHistory(url, req, res);
    return handleHistory(url, res, hub);
  }
  if (url.pathname === "/api/market-data/quote" && req.method === "GET") {
    return handleByoQuote(url, req, res);
  }

  json(res, 404, { error: "not found" });
}

async function handleHistory(url: URL, res: ServerResponse, hub: MarketHub) {
  const symbol = url.searchParams.get("symbol");
  const resolution = Number(url.searchParams.get("resolution") ?? "60");
  // Cap generously: 1m charts request ~7 trading days (≈9,600 bars). Keep an upper
  // bound to guard against abuse, but well above the deepest legitimate request.
  const count = Math.min(Number(url.searchParams.get("count") ?? "240"), 12000);

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

/** Admin sets a new password for a trader (no current password needed). */
async function handleAdminResetPassword(url: URL, req: IncomingMessage, res: ServerResponse, auth: AuthService) {
  if (!requireAdmin(req)) return json(res, 403, { error: "admin access required" });
  const userId = url.pathname.split("/")[4]!; // /api/admin/traders/:id/password
  const body = await readJson<{ newPassword?: string }>(req);
  const newPassword = body?.newPassword ?? "";
  if (newPassword.length < 6) return json(res, 400, { error: "New password must be at least 6 characters." });
  const ok = await auth.adminResetPassword(userId, newPassword);
  if (!ok) return json(res, 404, { ok: false, error: "trader not found" });
  json(res, 200, { ok: true });
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
      case "assign-tier": {
        const body = await readJson<{ templateId?: string }>(req);
        const templateId = body?.templateId?.trim();
        if (!templateId) return json(res, 400, { error: "templateId is required" });
        const ok = await adminAssignTier(accountId, templateId);
        if (!ok) return json(res, 404, { ok: false, error: "account or tier not found" });
        result = { ok: true, templateId };
        break;
      }
      default:
        return json(res, 400, { error: "unknown action" });
    }
    await accountStream.refreshAccount(accountId).catch(() => {});
    // A reset zeroes equity back to the starting balance — clear the trailing drawdown
    // peak too (after the snapshot reload) so live drawdown recomputes to $0, not the
    // pre-reset high-water mark.
    if (action === "reset" || action === "assign-tier") accountStream.resetDrawdownPeak(accountId);
    accountStream.publishAdminUpdate({ kind: `account_${action}`, id: accountId });
    json(res, 200, result);
  } catch (err) {
    console.error(`[admin] account action ${action} failed:`, (err as Error).message);
    json(res, 500, { error: "action failed" });
  }
}

async function handleAdminPhaseRule(
  url: URL,
  req: IncomingMessage,
  res: ServerResponse,
  accountStream: AccountStream,
  op: "create" | "update" | "delete",
) {
  if (!requireAdmin(req)) return json(res, 403, { error: "admin access required" });
  try {
    if (op === "create") {
      const body = await readJson<Record<string, unknown>>(req);
      const result = await createPhaseRule({
        variable: String(body?.variable ?? ""),
        operator: String(body?.operator ?? ""),
        value: Number(body?.value),
        assignsPhase: Number(body?.assignsPhase),
        priority: body?.priority != null ? Number(body.priority) : undefined,
        active: body?.active != null ? Boolean(body.active) : undefined,
        notes: body?.notes != null ? String(body.notes) : null,
      });
      if ("error" in result) return json(res, 400, result);
      await recomputeAllPhases();
      accountStream.publishAdminUpdate({ kind: "phase_rules_changed" });
      return json(res, 200, result);
    }
    const ruleId = Number(url.pathname.split("/")[4]);
    if (!Number.isFinite(ruleId)) return json(res, 400, { error: "invalid rule id" });
    if (op === "delete") {
      const ok = await deletePhaseRule(ruleId);
      if (ok) { await recomputeAllPhases(); accountStream.publishAdminUpdate({ kind: "phase_rules_changed" }); }
      return json(res, ok ? 200 : 404, { ok });
    }
    // update (PATCH/POST): only the provided fields change.
    const body = await readJson<Record<string, unknown>>(req);
    const patch: Record<string, unknown> = {};
    if (body?.variable != null) patch.variable = String(body.variable);
    if (body?.operator != null) patch.operator = String(body.operator);
    if (body?.value != null) patch.value = Number(body.value);
    if (body?.assignsPhase != null) patch.assignsPhase = Number(body.assignsPhase);
    if (body?.priority != null) patch.priority = Number(body.priority);
    if (body?.active != null) patch.active = Boolean(body.active);
    if (body?.notes !== undefined) patch.notes = body.notes == null ? null : String(body.notes);
    const result = await updatePhaseRule(ruleId, patch);
    if (result && "error" in result) return json(res, 400, result);
    if (!result) return json(res, 404, { error: "rule not found or no fields to update" });
    await recomputeAllPhases();
    accountStream.publishAdminUpdate({ kind: "phase_rules_changed" });
    return json(res, 200, result);
  } catch (err) {
    console.error("[admin] phase-rule op failed:", (err as Error).message);
    return json(res, 500, { error: "phase-rule op failed" });
  }
}

async function handleAdminReviewDecision(url: URL, req: IncomingMessage, res: ServerResponse, accountStream: AccountStream) {
  if (!requireAdmin(req)) return json(res, 403, { error: "admin access required" });
  const accountId = url.pathname.split("/")[4]!; // /api/admin/reviews/:accountId
  const body = await readJson<{ decision?: string }>(req);
  const decision = body?.decision;
  if (decision !== "approve" && decision !== "disapprove")
    return json(res, 400, { error: "decision must be 'approve' or 'disapprove'" });
  try {
    const out = await adminReviewDecision(accountId, decision);
    if (!out.ok) return json(res, out.error === "account not found" ? 404 : 409, out);
    // Approve/disapprove both reset the account to a tier's starting state → reload the live
    // cache and clear the trailing-drawdown peak so it recomputes from the fresh equity.
    await accountStream.refreshAccount(accountId).catch(() => {});
    accountStream.resetDrawdownPeak(accountId);
    accountStream.publishAdminUpdate({ kind: `review_${decision}`, accountId });
    json(res, 200, out);
  } catch (err) {
    console.error(`[admin] review ${decision} failed:`, (err as Error).message);
    json(res, 500, { error: "review decision failed" });
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

async function handleAdminRuleTemplateUpdate(url: URL, req: IncomingMessage, res: ServerResponse) {
  if (!requireAdmin(req)) return json(res, 403, { error: "admin access required" });
  const id = url.pathname.split("/")[4]!; // /api/admin/rule-templates/:id
  const body = await readJson<Parameters<typeof adminUpdateRuleTemplate>[1]>(req);
  if (!body) return json(res, 400, { error: "body required" });
  const ok = await adminUpdateRuleTemplate(id, body);
  json(res, ok ? 200 : 400, { ok, error: ok ? undefined : "template not found or no valid fields" });
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

/** Resolve the authenticated user id from the bearer token, or null. */
function authedUserId(req: IncomingMessage): string | null {
  return verifyToken(bearerToken(req.headers.authorization) ?? "")?.sub ?? null;
}

/** Report the active market-data model + whether this user has a key connected. */
async function handleByoConnectionStatus(req: IncomingMessage, res: ServerResponse) {
  const mode = config.marketDataMode;
  const userId = authedUserId(req);
  let connected = false;
  if (mode === "byo" && userId && useDatabase) connected = await hasUserDatabentoKey(userId).catch(() => false);
  json(res, 200, { mode, configured: byoConfigured(), connected });
}

/** Validate + store this user's own Databento key (Model B). */
async function handleByoConnect(req: IncomingMessage, res: ServerResponse) {
  const userId = authedUserId(req);
  if (!userId) return json(res, 401, { error: "Not authenticated." });
  if (!useDatabase) return json(res, 400, { error: "Connecting an account requires the database." });
  if (!byoConfigured()) return json(res, 400, { error: "Server is not configured for per-user keys (MARKET_DATA_ENC_KEY unset)." });
  const body = await readJson<{ apiKey?: string }>(req);
  const apiKey = (body?.apiKey ?? "").trim();
  if (!apiKey) return json(res, 400, { error: "A Databento API key is required." });
  if (!(await validateDatabentoKey(apiKey))) {
    return json(res, 400, { error: "Databento rejected that API key. Check it and try again." });
  }
  await setUserDatabentoKey(userId, apiKey);
  byoSessions.drop(userId); // a replaced key must start a fresh Live session
  json(res, 200, { ok: true, connected: true });
}

/** Disconnect (remove) this user's stored Databento key. */
async function handleByoDisconnect(req: IncomingMessage, res: ServerResponse) {
  const userId = authedUserId(req);
  if (!userId) return json(res, 401, { error: "Not authenticated." });
  if (!useDatabase) return json(res, 400, { error: "Disconnecting requires the database." });
  await clearUserDatabentoKey(userId);
  byoSessions.drop(userId); // tear down their Live session
  json(res, 200, { ok: true, connected: false });
}

/** Resolve the caller's user id + own Databento key, or send the right gate/error
 *  response. Returns { userId, key }, or null after having written a response. */
async function requireUserKey(req: IncomingMessage, res: ServerResponse): Promise<{ userId: string; key: string } | null> {
  const userId = authedUserId(req);
  if (!userId) {
    json(res, 401, { error: "Not authenticated." });
    return null;
  }
  const key = await getUserDatabentoKey(userId).catch(() => null);
  if (!key) {
    // 412 + a stable code so the frontend can show the "Connect Databento" gate.
    json(res, 412, { error: "No Databento account connected.", code: "no_databento_key" });
    return null;
  }
  return { userId, key };
}

/** Model B: real-time chart history from the user's own Databento Live session. */
async function handleByoHistory(url: URL, req: IncomingMessage, res: ServerResponse) {
  const symbol = url.searchParams.get("symbol");
  const resolution = Number(url.searchParams.get("resolution") ?? "60");
  // Cap generously: 1m charts request ~7 trading days (≈9,600 bars). Keep an upper
  // bound to guard against abuse, but well above the deepest legitimate request.
  const count = Math.min(Number(url.searchParams.get("count") ?? "240"), 12000);
  if (!symbol || !SYMBOLS.includes(symbol)) return json(res, 400, { error: "invalid or missing symbol" });
  if (!Number.isFinite(resolution) || resolution <= 0) return json(res, 400, { error: "invalid resolution" });
  const auth = await requireUserKey(req, res);
  if (!auth) return;
  try {
    json(res, 200, await fetchUserHistory(auth.userId, auth.key, symbol, resolution, count));
  } catch (err) {
    console.error("[byo history] error:", (err as Error).message);
    json(res, 502, { error: "history fetch failed (check your Databento entitlements)" });
  }
}

/** Model B: latest real-time quote for a symbol from the user's own Live session. */
async function handleByoQuote(url: URL, req: IncomingMessage, res: ServerResponse) {
  const symbol = url.searchParams.get("symbol");
  if (!symbol || !SYMBOLS.includes(symbol)) return json(res, 400, { error: "invalid or missing symbol" });
  const auth = await requireUserKey(req, res);
  if (!auth) return;
  try {
    const quote = fetchUserQuote(auth.userId, auth.key, symbol);
    // 204: session is warming up (no trade received yet) — the next poll will have it.
    if (!quote) return json(res, 204, {});
    json(res, 200, quote);
  } catch (err) {
    console.error("[byo quote] error:", (err as Error).message);
    json(res, 502, { error: "quote fetch failed (check your Databento entitlements)" });
  }
}

/** Self-service password change for the authenticated user (any role). */
async function handleChangePassword(req: IncomingMessage, res: ServerResponse, auth: AuthService) {
  const payload = verifyToken(bearerToken(req.headers.authorization) ?? "");
  if (!payload) return json(res, 401, { error: "Not authenticated." });
  const body = await readJson<{ currentPassword?: string; newPassword?: string }>(req);
  const currentPassword = body?.currentPassword ?? "";
  const newPassword = body?.newPassword ?? "";
  if (!currentPassword || !newPassword) {
    return json(res, 400, { error: "Current and new password are required." });
  }
  if (newPassword.length < 6) {
    return json(res, 400, { error: "New password must be at least 6 characters." });
  }
  const result = await auth.changePassword(payload.sub, currentPassword, newPassword);
  if (!result.ok) return json(res, 400, { error: result.error ?? "Could not change password." });
  json(res, 200, { ok: true });
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

async function handleRequestReset(req: IncomingMessage, res: ServerResponse, accountStream: AccountStream) {
  const accountId = await requireAccount(req);
  if (!accountId) return json(res, 401, { error: "unauthorized" });
  const out = await requestAccountReset(accountId);
  if (!out.ok) return json(res, 409, out);
  await accountStream.refreshAccount(accountId).catch(() => {}); // push the requested state live
  json(res, 200, out);
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

async function handleModifyOrder(url: URL, req: IncomingMessage, res: ServerResponse, engine: OrderEngine) {
  const accountId = await requireAccount(req);
  if (!accountId) return json(res, 401, { error: "unauthorized" });
  const orderId = url.pathname.split("/")[3]; // /api/orders/:id/modify
  if (!orderId) return json(res, 400, { error: "order id is required" });
  const body = await readJson<{ price?: number | null; stopLoss?: number | null; takeProfit?: number | null; quantity?: number }>(req);
  if (!body) return json(res, 400, { error: "invalid body" });
  const result = await engine.modify(accountId, decodeURIComponent(orderId), body);
  json(res, result.ok ? 200 : 400, result);
}

async function handlePositionBracket(req: IncomingMessage, res: ServerResponse, engine: OrderEngine) {
  const accountId = await requireAccount(req);
  if (!accountId) return json(res, 401, { error: "unauthorized" });
  const body = await readJson<{ symbol?: string; stopLoss?: number | null; takeProfit?: number | null }>(req);
  if (!body?.symbol) return json(res, 400, { error: "symbol is required" });
  const result = await engine.setPositionBracket(accountId, body.symbol, { stopLoss: body.stopLoss, takeProfit: body.takeProfit });
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
