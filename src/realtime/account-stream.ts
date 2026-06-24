import type { WebSocket } from "ws";
import { verifyToken, type Role } from "../auth/jwt.js";
import { useDatabase } from "../config.js";
import type { MarketDataProvider } from "../providers/provider.js";
import { getMultiplier } from "../instruments.js";
import {
  getAccountIdByUserId,
  getAccountSnapshot,
  listPositions,
  type AccountSnapshot,
  type ApiPosition,
} from "../trading/repository.js";

/* ------------------------------------------------------------------ *
 * Real-time gateway for the authenticated channels:
 *   positions · account-updates · orders · admin-updates
 * (market-data is handled separately by MarketHub.)
 *
 * Protocol (client → server):
 *   { type: "auth", token }
 *   { type: "subscribe",   channel: "positions" }
 *   { type: "unsubscribe", channel: "positions" }
 *
 * Server → client (flat messages with a `type`, e.g.):
 *   { type: "position_update", symbol: "NQ", pnl: 245, ... }
 *   { type: "account_update",  equity, balance, unrealizedPnl, ... }
 *   { type: "order_update",    order: {...} }
 *   { type: "admin_update",    event: {...} }
 *
 * The positions/account bridge marks each account's DB positions against the
 * live quote feed every second and pushes updates — the precursor to the PnL
 * engine. orders/admin are published by the engines via publish*().
 * ------------------------------------------------------------------ */

const ACCOUNT_CHANNELS = new Set(["positions", "account-updates", "orders", "admin-updates"]);
const PUSH_MS = 1000;

interface ClientState {
  auth?: { userId: string; accountId: string | null; role: Role };
  channels: Set<string>;
}

interface CachedAccount {
  snapshot: AccountSnapshot;
  positions: ApiPosition[];
}

export class AccountStream {
  private clients = new Map<WebSocket, ClientState>();
  private quotes = new Map<string, number>(); // symbol → latest price
  private accounts = new Map<string, CachedAccount>(); // accountId → cached data
  private peaks = new Map<string, number>(); // accountId → trailing peak equity (for live drawdown)
  private timer: NodeJS.Timeout | null = null;
  private risk: { evaluate(accountId: string, equity: number): Promise<void> } | null = null;

  constructor(provider: MarketDataProvider) {
    provider.on("quote", (q) => this.quotes.set(q.symbol, q.price));
  }

  /** Wire the risk engine (set after construction to avoid a circular dependency). */
  setRiskEngine(risk: { evaluate(accountId: string, equity: number): Promise<void> }) {
    this.risk = risk;
  }

  start() {
    if (!this.timer) this.timer = setInterval(() => this.tick(), PUSH_MS);
  }

  stop() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  addClient(ws: WebSocket) {
    this.clients.set(ws, { channels: new Set() });
  }

  removeClient(ws: WebSocket) {
    this.clients.delete(ws);
  }

  async handleMessage(ws: WebSocket, raw: string) {
    let msg: { type?: string; token?: string; channel?: string };
    try {
      msg = JSON.parse(raw);
    } catch {
      return;
    }
    const st = this.clients.get(ws);
    if (!st) return;

    if (msg.type === "auth") {
      await this.authenticate(ws, st, msg.token ?? "");
      return;
    }
    if (msg.type === "subscribe" && msg.channel && ACCOUNT_CHANNELS.has(msg.channel)) {
      st.channels.add(msg.channel);
      this.pushAccount(ws, st); // immediate per-position marks + account
      if (msg.channel === "positions") this.pushPositionsSnapshot(ws, st); // full list incl. lifecycle
    }
    if (msg.type === "unsubscribe" && msg.channel) {
      st.channels.delete(msg.channel);
    }
  }

  private async authenticate(ws: WebSocket, st: ClientState, token: string) {
    const payload = verifyToken(token);
    if (!payload) {
      this.send(ws, { type: "auth_error", message: "invalid token" });
      return;
    }
    const accountId = useDatabase ? await getAccountIdByUserId(payload.sub) : null;
    st.auth = { userId: payload.sub, accountId, role: payload.role };
    if (accountId) await this.loadAccount(accountId);
    this.send(ws, { type: "auth_ok", userId: payload.sub, role: payload.role });
  }

  private async loadAccount(accountId: string) {
    const [snapshot, positions] = await Promise.all([
      getAccountSnapshot(accountId),
      listPositions(accountId),
    ]);
    if (snapshot) this.accounts.set(accountId, { snapshot, positions });
  }

  /** Reload an account's positions/balance (call after the order engine mutates it). */
  async refreshAccount(accountId: string) {
    await this.loadAccount(accountId);
    // A fill may have opened, resized, flipped, or CLOSED a position. Per-position
    // marks alone can't express a removal, so push the full list — subscribers
    // replace their book and the engine's changes show up live.
    this.broadcastPositions(accountId);
  }

  /**
   * Drop the trailing high-water mark so live drawdown is recomputed from the (reset)
   * equity — call after an admin reset. The peak otherwise ratchets only upward, so it
   * would keep reporting the pre-reset drawdown against the old high. Next tick re-seeds
   * it from the fresh equity (= starting balance), giving a $0 drawdown.
   */
  resetDrawdownPeak(accountId: string) {
    this.peaks.delete(accountId);
  }

  /** Current positions for an account, marked against the latest quotes. */
  private livePositions(acc: CachedAccount): ApiPosition[] {
    return acc.positions.map((p) => {
      const mark = this.quotes.get(p.symbol) ?? p.markPrice ?? p.avgPrice;
      const dir = p.side === "buy" ? 1 : -1;
      return {
        ...p,
        markPrice: round(mark, 4),
        unrealizedPnl: round((mark - p.avgPrice) * p.quantity * dir * getMultiplier(p.symbol), 2),
      };
    });
  }

  /** Push the full positions list to an account's `positions` subscribers. */
  broadcastPositions(accountId: string) {
    const acc = this.accounts.get(accountId);
    if (!acc) return;
    this.sendToAccountChannel(accountId, "positions", {
      type: "positions_snapshot",
      channel: "positions",
      positions: this.livePositions(acc),
    });
  }

  /** Send the current positions snapshot to a single just-subscribed client. */
  private pushPositionsSnapshot(ws: WebSocket, st: ClientState) {
    const accountId = st.auth?.accountId;
    if (!accountId || ws.readyState !== ws.OPEN) return;
    const acc = this.accounts.get(accountId);
    if (acc) this.send(ws, { type: "positions_snapshot", channel: "positions", positions: this.livePositions(acc) });
  }

  /** Latest known price for a symbol (used by the order engine to fill markets). */
  getMarkPrice(symbol: string): number | undefined {
    return this.quotes.get(symbol);
  }

  /**
   * Feed a mark from an EXTERNAL source (Model B / byo per-user live sessions).
   * In byo mode the shared provider streams nothing (no one subscribes to it),
   * so without this the order engine has no fill price and positions never mark.
   * A symbol's price is the same for every user, so a single global map is correct.
   */
  setExternalMark(symbol: string, price: number): void {
    if (price > 0) this.quotes.set(symbol, price);
  }

  private tick() {
    // Evaluate each known account against its rule once per tick, using LIVE equity
    // (cash + unrealized), and keep a trailing peak for live drawdown reporting.
    for (const [accountId, acc] of this.accounts) {
      const equity = acc.snapshot.balance + this.unrealizedFor(acc);
      this.peaks.set(accountId, Math.max(this.peaks.get(accountId) ?? equity, equity));
      if (this.risk) void this.risk.evaluate(accountId, equity).catch((e) => this.warnRisk(e));
    }
    for (const [ws, st] of this.clients) this.pushAccount(ws, st);
  }

  private riskErrAt = 0;
  /** Surface risk-evaluation failures (throttled) — e.g. the DB not being migrated. */
  private warnRisk(err: unknown) {
    const now = Date.now();
    if (now - this.riskErrAt < 30_000) return;
    this.riskErrAt = now;
    console.error("[risk] evaluation error (did you run `npm run db:migrate`?):", (err as Error).message);
  }

  /** Sum unrealized P&L across an account's positions at the latest quotes. */
  private unrealizedFor(acc: CachedAccount): number {
    let u = 0;
    for (const p of acc.positions) {
      const mark = this.quotes.get(p.symbol) ?? p.markPrice ?? p.avgPrice;
      u += (mark - p.avgPrice) * p.quantity * (p.side === "buy" ? 1 : -1) * getMultiplier(p.symbol);
    }
    return u;
  }

  /** Compute live PnL from cached positions + latest quotes and push to one client. */
  private pushAccount(ws: WebSocket, st: ClientState) {
    const accountId = st.auth?.accountId;
    if (!accountId || ws.readyState !== ws.OPEN) return;
    const acc = this.accounts.get(accountId);
    if (!acc) return;

    let unrealized = 0;
    const wantsPositions = st.channels.has("positions");

    for (const p of acc.positions) {
      const mark = this.quotes.get(p.symbol) ?? p.markPrice ?? p.avgPrice;
      const dir = p.side === "buy" ? 1 : -1;
      const pnl = (mark - p.avgPrice) * p.quantity * dir * getMultiplier(p.symbol);
      unrealized += pnl;
      if (wantsPositions) {
        this.send(ws, {
          type: "position_update",
          channel: "positions",
          symbol: p.symbol,
          side: p.side,
          quantity: p.quantity,
          avgPrice: p.avgPrice,
          markPrice: round(mark, 4),
          unrealizedPnl: round(pnl, 2),
          pnl: round(pnl, 2),
        });
      }
    }

    if (st.channels.has("account-updates")) {
      const equity = acc.snapshot.balance + unrealized;
      const peak = this.peaks.get(accountId) ?? equity;
      this.send(ws, {
        type: "account_update",
        channel: "account-updates",
        status: acc.snapshot.status, // ACTIVE → PASSED/FAILED reflects live after a breach/target
        statusReason: acc.snapshot.statusReason, // the specific breach detail (null when ACTIVE)
        balance: round(acc.snapshot.balance, 2),
        equity: round(equity, 2),
        unrealizedPnl: round(unrealized, 2),
        realizedPnlToday: round(acc.snapshot.realizedPnlToday, 2),
        // Equity-based day P&L (vs day-start equity) — exactly what the daily-loss limit checks.
        dailyPnl: round(equity - acc.snapshot.dayStartEquity, 2),
        totalPnl: round(equity - acc.snapshot.startingBalance, 2), // mark-to-market vs start
        drawdown: round(Math.max(0, peak - equity), 2),
      });
    }
  }

  /** Engine hook: push an order update to the owning account's subscribers. */
  publishOrderUpdate(accountId: string, order: unknown) {
    this.sendToAccountChannel(accountId, "orders", { type: "order_update", channel: "orders", order });
  }

  /** Engine hook: push an admin event to all admin subscribers. */
  publishAdminUpdate(event: unknown) {
    for (const [ws, st] of this.clients) {
      if (st.auth?.role === "ADMIN" && st.channels.has("admin-updates")) {
        this.send(ws, { type: "admin_update", channel: "admin-updates", event });
      }
    }
  }

  private sendToAccountChannel(accountId: string, channel: string, payload: object) {
    for (const [ws, st] of this.clients) {
      if (st.auth?.accountId === accountId && st.channels.has(channel)) this.send(ws, payload);
    }
  }

  private send(ws: WebSocket, payload: object) {
    if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(payload));
  }
}

function round(v: number, decimals: number): number {
  const f = 10 ** decimals;
  return Math.round(v * f) / f;
}
