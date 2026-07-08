import WebSocket from "ws";
import { BaseProvider, round, synthBook } from "./provider.js";
import { INSTRUMENTS, getInstrument } from "../instruments.js";
import type { Candle } from "../types.js";

/* ------------------------------------------------------------------ *
 * DxFeedProvider — real-time market data via dxFeed's dxLink WebSocket
 * protocol (https://demo.dxfeed.com/dxlink-ws for the public demo feed).
 *
 * ADDITIVE / ISOLATED: this file is selected only when MARKET_DATA_MODE=dxfeed.
 * It does NOT import or modify any Databento code — Databento (shared/byo) keeps
 * working exactly as before. Drop-in for the SHARED path: MarketHub fans its
 * quotes out to charts and AccountStream consumes the same quotes as marks.
 *
 * dxLink flow (JSON frames over one socket, multiplexed by `channel`):
 *   SETUP → (SETUP + AUTH_STATE) → AUTH? → (AUTH_STATE:AUTHORIZED)
 *     → CHANNEL_REQUEST(FEED) → CHANNEL_OPENED → FEED_SETUP → FEED_SUBSCRIPTION
 *     → FEED_DATA (live) …  + KEEPALIVE every 30s.
 * Live channel (1) carries Quote/Trade/Summary; a second channel (3) is used
 * for on-demand Candle history snapshots (getHistory).
 * ------------------------------------------------------------------ */

const KEEPALIVE_MS = 30_000; // server default timeout is 60s; ping at half
const BASE_BACKOFF_MS = 1_000;
const MAX_BACKOFF_MS = 15_000;
const PROTOCOL_VERSION = "1.0.0-tradingbackend";
// Book/quote emit throttles (dxFeed Quote can tick many times/sec).
const BOOK_THROTTLE_MS = 200;
// A candle-history snapshot is "done" once no new bars arrive for this long.
const HISTORY_IDLE_MS = 700;
const HISTORY_MAX_MS = 8_000;

const CHANNEL_FEED = 1; // live Quote/Trade/Summary
const CHANNEL_HIST = 3; // on-demand Candle snapshots

/**
 * Internal symbol → dxFeed symbol. Kept LOCAL to this provider so instruments.ts
 * (shared with the Databento path) stays untouched. These are CME continuous-
 * futures placeholders in dxFeed symbology — override per deploy with
 * DXFEED_SYMBOL_MAP (JSON, e.g. {"ES":"/ESU25:XCME"}). On the public demo feed
 * futures aren't entitled, so DXFEED_DEMO=1 swaps in liquid demo equities/FX to
 * validate the wiring end-to-end.
 */
const DEFAULT_SYMBOL_MAP: Record<string, string> = {
  ES: "/ES:XCME", MES: "/MES:XCME",
  NQ: "/NQ:XCME", MNQ: "/MNQ:XCME",
  YM: "/YM:XCBT", MYM: "/MYM:XCBT",
  CL: "/CL:XNYM", MCL: "/MCL:XNYM",
  GC: "/GC:XCEC", MGC: "/MGC:XCEC",
};

/** Demo-feed stand-ins so the plumbing can be exercised without futures entitlement. */
const DEMO_SYMBOL_MAP: Record<string, string> = {
  ES: "SPY", MES: "SPY", NQ: "QQQ", MNQ: "QQQ", YM: "DIA", MYM: "DIA",
  CL: "USO", MCL: "USO", GC: "GLD", MGC: "GLD",
};

interface SymState {
  price: number;
  bid: number;
  ask: number;
  dayOpen: number;
  prevClose: number;
  high: number;
  low: number;
  volume: number;
  havePrice: boolean;
}

interface HistPending {
  bars: Map<number, Candle>; // time(sec) → bar, deduped
  resolve: (bars: Candle[]) => void;
  idle: NodeJS.Timeout;
  cap: NodeJS.Timeout;
  precision: number;
}

export class DxFeedProvider extends BaseProvider {
  readonly name = "dxfeed";
  private ws: WebSocket | null = null;
  private running = false;
  private authorized = false;
  private retries = 0;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private keepaliveTimer: NodeJS.Timeout | null = null;

  private readonly state = new Map<string, SymState>();
  private readonly toDx = new Map<string, string>(); // internal → dxFeed symbol
  private readonly fromDx = new Map<string, string>(); // dxFeed symbol → internal
  private readonly channelOpen = new Map<number, boolean>();
  private readonly bookEmitAt = new Map<string, number>();
  // In-flight candle-history requests, keyed by the dxFeed candle symbol ("/ES:XCME{=1m}").
  private readonly hist = new Map<string, HistPending>();

  constructor(
    private readonly endpoint: string,
    private readonly token: string,
  ) {
    super();
    const isDemo = process.env.DXFEED_DEMO === "1" || /demo\.dxfeed\.com/i.test(endpoint);
    let override: Record<string, string> = {};
    if (process.env.DXFEED_SYMBOL_MAP) {
      try {
        override = JSON.parse(process.env.DXFEED_SYMBOL_MAP) as Record<string, string>;
      } catch {
        console.warn("[dxfeed] DXFEED_SYMBOL_MAP is not valid JSON — ignoring");
      }
    }
    const base = isDemo ? DEMO_SYMBOL_MAP : DEFAULT_SYMBOL_MAP;
    for (const inst of INSTRUMENTS) {
      const dx = override[inst.symbol] ?? base[inst.symbol];
      if (!dx) continue;
      this.toDx.set(inst.symbol, dx);
      this.fromDx.set(dx, inst.symbol);
      this.state.set(inst.symbol, {
        price: inst.simBase, bid: 0, ask: 0, dayOpen: 0, prevClose: 0,
        high: 0, low: 0, volume: 0, havePrice: false,
      });
    }
    if (isDemo) console.log("[dxfeed] DEMO symbol map active (equity/FX stand-ins for futures)");
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.connect();
  }

  stop(): void {
    this.running = false;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    if (this.keepaliveTimer) clearInterval(this.keepaliveTimer);
    this.reconnectTimer = this.keepaliveTimer = null;
    for (const p of this.hist.values()) {
      clearTimeout(p.idle);
      clearTimeout(p.cap);
      p.resolve([]);
    }
    this.hist.clear();
    this.ws?.close();
    this.ws = null;
  }

  // --- connection --------------------------------------------------

  private connect(): void {
    this.authorized = false;
    this.channelOpen.clear();
    const ws = new WebSocket(this.endpoint);
    this.ws = ws;
    ws.on("open", () => {
      console.log("[dxfeed] socket open →", this.endpoint);
      this.send({ type: "SETUP", channel: 0, version: PROTOCOL_VERSION, keepaliveTimeout: 60, acceptKeepaliveTimeout: 60 });
    });
    ws.on("message", (raw: WebSocket.RawData) => this.onMessage(raw));
    ws.on("error", (err) => console.warn("[dxfeed] socket error:", (err as Error).message));
    ws.on("close", (code) => {
      if (this.ws === ws) this.ws = null;
      if (this.keepaliveTimer) { clearInterval(this.keepaliveTimer); this.keepaliveTimer = null; }
      if (this.running) this.scheduleReconnect(code);
    });
  }

  private scheduleReconnect(code?: number): void {
    const delay = Math.min(BASE_BACKOFF_MS * 2 ** this.retries, MAX_BACKOFF_MS);
    this.retries += 1;
    console.warn(`[dxfeed] disconnected (code ${code ?? "?"}) — reconnecting in ${delay}ms`);
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = setTimeout(() => this.connect(), delay);
  }

  private send(msg: Record<string, unknown>): void {
    if (this.ws?.readyState === WebSocket.OPEN) this.ws.send(JSON.stringify(msg));
  }

  // --- protocol ----------------------------------------------------

  private onMessage(raw: WebSocket.RawData): void {
    let msg: Record<string, unknown>;
    try {
      msg = JSON.parse(raw.toString()) as Record<string, unknown>;
    } catch {
      return;
    }
    switch (msg.type) {
      case "SETUP":
        // Server acked SETUP; AUTH_STATE follows.
        break;
      case "AUTH_STATE":
        this.onAuthState(String(msg.state));
        break;
      case "CHANNEL_OPENED":
        this.onChannelOpened(Number(msg.channel));
        break;
      case "FEED_CONFIG":
        break; // server confirmed FEED_SETUP; subscriptions may proceed
      case "FEED_DATA":
        this.onFeedData(Number(msg.channel), msg.data as unknown[]);
        break;
      case "KEEPALIVE":
        break; // server heartbeat
      case "ERROR":
        console.warn(`[dxfeed] gateway error on channel ${msg.channel}: ${msg.error} — ${msg.message}`);
        break;
      default:
        break;
    }
  }

  private onAuthState(state: string): void {
    if (state === "UNAUTHORIZED") {
      // Real endpoints require a token; the public demo is open (goes straight to
      // AUTHORIZED). Only send AUTH when we actually have a token.
      if (this.token) this.send({ type: "AUTH", channel: 0, token: this.token });
      else console.warn("[dxfeed] server requires auth but DXFEED_TOKEN is unset — set it for a real endpoint");
      return;
    }
    if (state === "AUTHORIZED") {
      if (this.authorized) return;
      this.authorized = true;
      this.retries = 0;
      console.log("[dxfeed] authorized — opening feed channels");
      if (!this.keepaliveTimer) {
        this.keepaliveTimer = setInterval(() => this.send({ type: "KEEPALIVE", channel: 0 }), KEEPALIVE_MS);
      }
      // Open the live feed channel and the on-demand history channel.
      this.send({ type: "CHANNEL_REQUEST", channel: CHANNEL_FEED, service: "FEED", parameters: { contract: "AUTO" } });
      this.send({ type: "CHANNEL_REQUEST", channel: CHANNEL_HIST, service: "FEED", parameters: { contract: "HISTORY" } });
    }
  }

  private onChannelOpened(channel: number): void {
    this.channelOpen.set(channel, true);
    if (channel === CHANNEL_FEED) {
      this.send({
        type: "FEED_SETUP",
        channel: CHANNEL_FEED,
        acceptAggregationPeriod: 0.1,
        acceptDataFormat: "FULL",
        acceptEventFields: {
          Quote: ["eventType", "eventSymbol", "bidPrice", "askPrice"],
          Trade: ["eventType", "eventSymbol", "price", "size", "dayVolume"],
          Summary: ["eventType", "eventSymbol", "dayOpenPrice", "dayHighPrice", "dayLowPrice", "prevDayClosePrice"],
        },
      });
      // Subscribe every mapped instrument for Quote + Trade + Summary.
      const add = [...this.toDx.values()].flatMap((sym) => [
        { type: "Quote", symbol: sym },
        { type: "Trade", symbol: sym },
        { type: "Summary", symbol: sym },
      ]);
      this.send({ type: "FEED_SUBSCRIPTION", channel: CHANNEL_FEED, add });
      console.log(`[dxfeed] subscribed ${this.toDx.size} instruments (Quote/Trade/Summary)`);
    } else if (channel === CHANNEL_HIST) {
      this.send({
        type: "FEED_SETUP",
        channel: CHANNEL_HIST,
        acceptAggregationPeriod: 0,
        acceptDataFormat: "FULL",
        acceptEventFields: {
          Candle: ["eventType", "eventSymbol", "time", "open", "high", "low", "close", "volume"],
        },
      });
    }
  }

  // --- live data ---------------------------------------------------

  private onFeedData(channel: number, data: unknown[]): void {
    if (!Array.isArray(data)) return;
    for (const ev of data) {
      if (!ev || typeof ev !== "object") continue;
      const e = ev as Record<string, unknown>;
      if (channel === CHANNEL_HIST && e.eventType === "Candle") {
        this.onCandle(e);
        continue;
      }
      const symbol = this.fromDx.get(String(e.eventSymbol));
      if (!symbol) continue;
      const st = this.state.get(symbol);
      if (!st) continue;
      switch (e.eventType) {
        case "Trade":
          this.onTrade(symbol, st, e);
          break;
        case "Quote":
          this.onQuote(symbol, st, e);
          break;
        case "Summary":
          this.onSummary(symbol, st, e);
          break;
      }
    }
  }

  private onTrade(symbol: string, st: SymState, e: Record<string, unknown>): void {
    const price = num(e.price);
    if (!Number.isFinite(price) || price <= 0) return;
    const first = !st.havePrice;
    st.price = price;
    st.havePrice = true;
    st.volume = num(e.dayVolume) || st.volume;
    if (st.high) st.high = Math.max(st.high, price);
    if (st.low) st.low = Math.min(st.low, price);
    if (first) console.log(`[dxfeed] first trade ${symbol} @ ${price}`);
    this.emitQuote(symbol, st, num(e.size));
  }

  private onQuote(symbol: string, st: SymState, e: Record<string, unknown>): void {
    const bid = num(e.bidPrice);
    const ask = num(e.askPrice);
    if (bid > 0) st.bid = bid;
    if (ask > 0) st.ask = ask;
    // Before the first trade, use the mid as the price so charts/marks aren't $0.
    if (!st.havePrice && st.bid > 0 && st.ask > 0) st.price = (st.bid + st.ask) / 2;
    this.emitQuote(symbol, st, 0);
  }

  private onSummary(symbol: string, st: SymState, e: Record<string, unknown>): void {
    st.dayOpen = num(e.dayOpenPrice) || st.dayOpen;
    st.high = num(e.dayHighPrice) || st.high;
    st.low = num(e.dayLowPrice) || st.low;
    st.prevClose = num(e.prevDayClosePrice) || st.prevClose;
    this.emitQuote(symbol, st, 0);
  }

  private emitQuote(symbol: string, st: SymState, lastSize: number): void {
    const inst = getInstrument(symbol)!;
    const p = inst.pricePrecision;
    const spread = inst.tickSize;
    const bid = st.bid > 0 ? st.bid : st.price - spread;
    const ask = st.ask > 0 ? st.ask : st.price + spread;
    // Prefer prev-day close for the 24h change baseline; fall back to the day open.
    const base = st.prevClose > 0 ? st.prevClose : st.dayOpen;
    this.emit("quote", {
      symbol,
      price: round(st.price, p),
      bid: round(bid, p),
      ask: round(ask, p),
      change24h: base > 0 ? (st.price - base) / base : 0,
      high24h: round(st.high > 0 ? Math.max(st.high, st.price) : st.price, p),
      low24h: round(st.low > 0 ? Math.min(st.low, st.price) : st.price, p),
      volume24h: Math.round(st.volume),
      lastSize,
      ts: Date.now(),
    });
    this.maybeEmitBook(symbol, st, inst.tickSize, p);
  }

  /** dxFeed's Order/depth feed isn't wired yet — emit a synthetic ladder (like the
   *  Databento fallback) so the DOM is populated for watched symbols. */
  private maybeEmitBook(symbol: string, st: SymState, tickSize: number, precision: number): void {
    if (!this.bookSymbols.has(symbol) || !st.havePrice) return;
    const now = Date.now();
    if (now - (this.bookEmitAt.get(symbol) ?? 0) < BOOK_THROTTLE_MS) return;
    this.bookEmitAt.set(symbol, now);
    this.emit("orderbook", synthBook(symbol, st.price, tickSize, precision));
  }

  // --- history (Candle snapshots) ----------------------------------

  async getHistory(symbol: string, resolutionSec: number, count: number): Promise<Candle[]> {
    const inst = getInstrument(symbol);
    const dx = this.toDx.get(symbol);
    if (!inst || !dx || !this.channelOpen.get(CHANNEL_HIST)) return [];
    const candleSymbol = `${dx}{=${candlePeriod(resolutionSec)}}`;
    const existing = this.hist.get(candleSymbol);
    if (existing) return new Promise((res) => { const prev = existing.resolve; existing.resolve = (b) => { prev(b); res(b); }; });

    const fromTime = Date.now() - count * resolutionSec * 1000;
    return new Promise<Candle[]>((resolve) => {
      const pending: HistPending = {
        bars: new Map(),
        resolve,
        precision: inst.pricePrecision,
        idle: setTimeout(() => this.finishHistory(candleSymbol), HISTORY_IDLE_MS),
        cap: setTimeout(() => this.finishHistory(candleSymbol), HISTORY_MAX_MS),
      };
      this.hist.set(candleSymbol, pending);
      this.send({ type: "FEED_SUBSCRIPTION", channel: CHANNEL_HIST, add: [{ type: "Candle", symbol: candleSymbol, fromTime }] });
    }).then((bars) => bars.slice(-count));
  }

  private onCandle(e: Record<string, unknown>): void {
    const candleSymbol = String(e.eventSymbol);
    const pending = this.hist.get(candleSymbol);
    if (!pending) return;
    const timeSec = Math.floor(num(e.time) / 1000);
    const close = num(e.close);
    // dxFeed emits a synthetic snapshot-boundary event with NaN/empty OHLC (→ 0 here)
    // to mark the end of the history snapshot — skip it so only real bars are returned.
    if (!timeSec || close <= 0) return;
    const p = pending.precision;
    pending.bars.set(timeSec, {
      time: timeSec,
      open: round(num(e.open), p),
      high: round(num(e.high), p),
      low: round(num(e.low), p),
      close: round(close, p),
      volume: Math.max(0, Math.round(num(e.volume))),
    });
    // Reset the idle timer — snapshot is "complete" once bars stop flowing.
    clearTimeout(pending.idle);
    pending.idle = setTimeout(() => this.finishHistory(candleSymbol), HISTORY_IDLE_MS);
  }

  private finishHistory(candleSymbol: string): void {
    const pending = this.hist.get(candleSymbol);
    if (!pending) return;
    clearTimeout(pending.idle);
    clearTimeout(pending.cap);
    this.hist.delete(candleSymbol);
    // Stop the snapshot subscription so it doesn't keep streaming live candles.
    this.send({ type: "FEED_SUBSCRIPTION", channel: CHANNEL_HIST, remove: [{ type: "Candle", symbol: candleSymbol }] });
    const bars = [...pending.bars.values()].sort((a, b) => a.time - b.time);
    pending.resolve(bars);
  }
}

/** Coerce a dxFeed numeric field (may be number, numeric string, or "NaN") to a number. */
function num(v: unknown): number {
  const n = typeof v === "number" ? v : Number(v);
  return Number.isFinite(n) ? n : 0;
}

/** resolutionSec → dxFeed candle period string (60→"1m", 3600→"1h", 86400→"1d"). */
function candlePeriod(sec: number): string {
  if (sec % 86400 === 0) return `${sec / 86400}d`;
  if (sec % 3600 === 0) return `${sec / 3600}h`;
  return `${Math.max(1, Math.round(sec / 60))}m`;
}
