import net from "node:net";
import { createHash } from "node:crypto";
import { BaseProvider, round } from "./provider.js";
import { DatabentoClient } from "../databento/client.js";
import { DbnDecoder, type DecodedBook, type DecodedMapping, type DecodedRecord, type DecodedTrade } from "../databento/dbn.js";
import { aggregateCandles, fetchDailyStats, fetchHistory } from "./databento-shared.js";
import { INSTRUMENTS, getInstrument } from "../instruments.js";
import type { Candle } from "../types.js";

/* ------------------------------------------------------------------ *
 * DatabentoLiveProvider — true real-time via the Databento Live raw-TCP
 * gateway (CRAM auth + DBN binary stream). Streams `trades` for all roots and
 * emits quotes on every print. Hybrid: still uses the Historical HTTP client
 * for chart history (getHistory) and rolling-24h stats.
 *
 * Handshake (proven): connect → greeting + `cram=` → send auth → `success=` →
 * send subscription + start_session → DBN binary stream.
 * ------------------------------------------------------------------ */

const LIVE_PORT = 13000;
const DAILY_POLL_MS = 30_000;
const BASE_BACKOFF_MS = 1_000;
const MAX_BACKOFF_MS = 15_000;
// Coalesce book updates: mbp-10 can tick many times per millisecond on liquid
// contracts, but the UI only needs a fresh snapshot a few times per second.
const BOOK_THROTTLE_MS = 150;
// Rolling per-symbol live-bar buffer. The Historical API lags real-time (its
// dataset-end trails by minutes/hours), so chart history stops short of "now".
// We aggregate live trade prints into 1-minute bars and merge them into
// getHistory, closing that seam — and the buffer survives frontend reloads.
const LIVE_BAR_CAP = 1500; // ~25h of 1-minute bars per symbol
const HIST_CACHE_TTL_MS = 60_000; // re-fetch a symbol's historical backfill at most once/min

interface SymState {
  price: number;
  priceTs: number;
  dayOpen: number;
  high: number;
  low: number;
  volume: number;
  havePrice: boolean;
  haveStats: boolean;
}

export class DatabentoLiveProvider extends BaseProvider {
  readonly name = "databento-live";
  private readonly client: DatabentoClient;
  private readonly host: string;
  private socket: net.Socket | null = null;
  private decoder = new DbnDecoder();
  private state = new Map<string, SymState>();
  private idToSymbol = new Map<number, string>();
  private readonly dbSymToSymbol = new Map<string, string>(); // databentoSymbol → internal symbol
  private unmappedIds = new Set<number>(); // live ids we received trades for but couldn't map (diagnostic)
  private bookEmitAt = new Map<string, number>(); // per-symbol throttle clock
  private liveBars = new Map<string, Candle[]>(); // 1-min bars built from live trades
  // Historical-backfill cache, keyed `${symbol}:${resolutionSec}`. The Historical
  // HTTP hop is slow/rate-limited from some hosts (e.g. Railway), so we fetch it
  // ONCE in the background, cache it, and serve it instantly thereafter — the
  // chart never waits on (or is blanked by) a slow historical request.
  private histCache = new Map<string, { at: number; bars: Candle[] }>();
  private histInflight = new Set<string>();
  private streaming = false;
  private textBuf: Buffer = Buffer.alloc(0);
  private running = false;
  private retries = 0;
  private reconnectTimer: NodeJS.Timeout | null = null;
  private dailyTimer: NodeJS.Timeout | null = null;
  private failing = false;
  private lastErrorLogAt = 0;
  private lastSocketError: string | null = null;

  constructor(
    private readonly apiKey: string,
    private readonly dataset: string,
  ) {
    super();
    this.client = new DatabentoClient(apiKey, dataset);
    this.host = `${dataset.toLowerCase().replace(/\./g, "-")}.lsg.databento.com`;
    for (const inst of INSTRUMENTS) {
      this.state.set(inst.symbol, {
        price: inst.simBase, priceTs: 0, dayOpen: 0, high: 0, low: 0, volume: 0, havePrice: false, haveStats: false,
      });
      this.dbSymToSymbol.set(inst.databentoSymbol, inst.symbol);
    }
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    void this.resolveIds();
    void this.pollDaily();
    this.scheduleDaily();
    this.connect();
  }

  stop(): void {
    this.running = false;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    if (this.dailyTimer) clearTimeout(this.dailyTimer);
    this.reconnectTimer = this.dailyTimer = null;
    this.socket?.destroy();
    this.socket = null;
  }

  async getHistory(symbol: string, resolutionSec: number, count: number): Promise<Candle[]> {
    const inst = getInstrument(symbol);
    if (!inst) return [];
    const key = `${symbol}:${resolutionSec}`;
    const cached = this.histCache.get(key);
    // Warm (or refresh) the cache in the BACKGROUND — never block the chart
    // response on the slow Historical hop. The first call returns live-only; once
    // the background fetch lands, subsequent calls (the frontend re-polls) serve
    // full history. TTL 60s keeps it fresh without hammering the link.
    if (!cached || Date.now() - cached.at > HIST_CACHE_TTL_MS) {
      void this.refreshHistory(symbol, resolutionSec, count, inst);
    }
    const hist = cached?.bars ?? [];
    // The live buffer is minute-grained, so it can only extend resolutions >= 1m.
    const live = this.liveBars.get(symbol);
    if (resolutionSec < 60 || !live || live.length === 0) return hist;
    // Append live-built bars newer than the last Historical bar — this bridges
    // the publication-delay seam between Historical and the live stream.
    const lastHist = hist.length ? hist[hist.length - 1]!.time : 0;
    const tail = aggregateCandles(live, resolutionSec).filter((c) => c.time > lastHist);
    return tail.length ? [...hist, ...tail].slice(-count) : hist;
  }

  /**
   * Populate the historical-backfill cache for one symbol/resolution. Runs in the
   * background (callers don't await it), de-duped per key, with a generous timeout
   * — a slow Historical hop is acceptable here because it doesn't block the chart;
   * it just needs to land once, after which the cache serves it instantly.
   */
  private async refreshHistory(symbol: string, resolutionSec: number, count: number, inst: ReturnType<typeof getInstrument>): Promise<void> {
    if (!inst) return;
    const key = `${symbol}:${resolutionSec}`;
    if (this.histInflight.has(key)) return;
    this.histInflight.add(key);
    try {
      const bars = await fetchHistory(this.client, inst.databentoSymbol, resolutionSec, count, inst.pricePrecision, {
        retries: 1,
        timeoutMs: 60_000,
      });
      if (bars.length) {
        this.histCache.set(key, { at: Date.now(), bars });
        this.logRecovered();
      }
    } catch (err) {
      this.logError(err, `history ${symbol} ${resolutionSec}s (background backfill — chart shows live bars meanwhile)`);
    } finally {
      this.histInflight.delete(key);
    }
  }

  /** Resolve each root's current dated contract code (e.g. ES → ESM6). */
  async resolveContractCodes(): Promise<Record<string, string>> {
    try {
      const resolved = await this.client.resolveContracts(INSTRUMENTS.map((i) => i.databentoSymbol), Date.now());
      const out: Record<string, string> = {};
      for (const inst of INSTRUMENTS) {
        const code = resolved[inst.databentoSymbol];
        if (code) out[inst.symbol] = code;
      }
      return out;
    } catch {
      return {};
    }
  }

  // --- instrument id mapping ---------------------------------------

  private async resolveIds(attempt = 0): Promise<void> {
    // Without this id→symbol map, incoming live trades can't be decoded and are
    // dropped — so if the call fails/times out (e.g. slow hop to Databento), keep
    // retrying with backoff until the map is populated. Critical on cloud hosts.
    try {
      const map = await this.client.resolveInstrumentIds(INSTRUMENTS.map((i) => i.databentoSymbol), Date.now());
      for (const inst of INSTRUMENTS) {
        const id = map[inst.databentoSymbol];
        if (id) this.idToSymbol.set(id, inst.symbol);
      }
      if (this.idToSymbol.size > 0) {
        // The decisive line: once this prints, incoming live trades map to symbols
        // and the chart/quote stream in real time. Its ABSENCE means real-time is
        // broken at the symbology step (id map empty → every trade dropped).
        console.log(`[live] resolved ${this.idToSymbol.size} instrument ids — live trades now stream in real time`);
        return;
      }
    } catch (err) {
      this.logError(err, "symbology resolve (instrument-id map)");
    }
    if (this.idToSymbol.size === 0 && this.running && attempt < 8) {
      const delay = Math.min(2000 * 2 ** attempt, 30_000);
      setTimeout(() => void this.resolveIds(attempt + 1), delay);
    }
  }

  // --- live TCP connection -----------------------------------------

  private connect(): void {
    this.streaming = false;
    this.textBuf = Buffer.alloc(0);
    this.decoder.reset();
    this.lastSocketError = null;

    const socket = net.connect(LIVE_PORT, this.host);
    this.socket = socket;
    // NB: don't reset backoff on TCP `connect`. A session that connects and
    // authenticates but is dropped at/just-after subscription must still
    // escalate — otherwise the loop hammers every 1000ms forever. retries
    // resets only once a real DBN stream begins (see the `success=` branch).
    socket.on("data", (chunk) => this.onData(chunk));
    socket.on("error", (err) => {
      this.lastSocketError = (err as { message?: string }).message ?? "socket error";
    });
    socket.on("close", () => {
      if (this.socket === socket) this.socket = null;
      if (this.running) this.scheduleReconnect();
    });
  }

  private scheduleReconnect(): void {
    const delay = Math.min(BASE_BACKOFF_MS * 2 ** this.retries, MAX_BACKOFF_MS);
    this.retries += 1;
    const reason = this.lastSocketError
      ? `socket error: ${this.lastSocketError}`
      : this.streaming
        ? "gateway closed the stream (entitlement/idle?)"
        : "closed during handshake";
    // Reset the throttle so the disconnect reason is never swallowed, even in a
    // tight reconnect loop. (HTTP poll errors in logError still throttle.)
    this.failing = false;
    this.logError(new Error(`live disconnected (${reason}) — reconnecting in ${delay}ms`));
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.reconnectTimer = setTimeout(() => this.connect(), delay);
  }

  private onData(chunk: Buffer): void {
    if (this.streaming) {
      for (const r of this.decoder.feed(chunk)) this.onRecord(r);
      return;
    }
    this.textBuf = Buffer.concat([this.textBuf, chunk]);
    while (!this.streaming) {
      const nl = this.textBuf.indexOf(0x0a);
      if (nl < 0) break;
      const line = this.textBuf.subarray(0, nl).toString("utf8");
      this.textBuf = this.textBuf.subarray(nl + 1);
      this.handleLine(line);
    }
    // Anything left after start_session is the DBN stream.
    if (this.streaming && this.textBuf.length) {
      for (const r of this.decoder.feed(this.textBuf)) this.onRecord(r);
      this.textBuf = Buffer.alloc(0);
    }
  }

  private handleLine(line: string): void {
    if (line.startsWith("cram=")) {
      const challenge = line.slice(5).trim();
      const sha = createHash("sha256").update(`${challenge}|${this.apiKey}`).digest("hex");
      const response = `${sha}-${this.apiKey.slice(-5)}`;
      this.socket?.write(`auth=${response}|dataset=${this.dataset}|encoding=dbn|ts_out=0\n`);
    } else if (line.startsWith("success=")) {
      const symbols = INSTRUMENTS.map((i) => i.databentoSymbol).join(",");
      // No `start` field → live subscription from now (empty start is rejected).
      this.socket?.write(`schema=trades|stype_in=continuous|symbols=${symbols}\n`);
      // Real depth book: top-10 market-by-price. Each mbp-10 message is a full
      // top-10 snapshot, so no incremental book state has to be maintained.
      // NOTE: gated behind DATABENTO_MBP10 while we confirm whether this
      // subscription is what the gateway is silently closing (entitlement?).
      if (process.env.DATABENTO_MBP10 === "1") {
        this.socket?.write(`schema=mbp-10|stype_in=continuous|symbols=${symbols}\n`);
      }
      this.socket?.write(`start_session=0\n`);
      this.streaming = true;
      this.logRecovered();
      console.log("[live] authenticated — streaming trades + mbp-10 book for", INSTRUMENTS.length, "instruments");
    } else if (line.startsWith("error=")) {
      // Unconditional — a subscription/entitlement rejection must never be
      // swallowed by the error throttle.
      console.warn(`[live] gateway error: ${line.trim()}`);
      this.socket?.destroy();
    } else if (line.trim()) {
      // Any other control line (warnings, rejects) — surface it raw to diagnose
      // silent closes.
      console.warn(`[live] gateway: ${line.trim()}`);
    }
  }

  private onRecord(r: DecodedRecord): void {
    // Reset backoff only once real data flows — reaching `success=` isn't enough,
    // since the gateway can ack then immediately close (see entitlement loop).
    if (this.retries) this.retries = 0;
    if (r.kind === "trade") this.onTrade(r);
    else if (r.kind === "book") this.onBook(r);
    else this.onMapping(r);
  }

  /** Bind a LIVE instrument_id to one of our symbols from a gateway mapping record. */
  private onMapping(m: DecodedMapping): void {
    // The body holds the subscribed symbol(s) as null-padded C strings (e.g.
    // "ES.v.0"). Tokenize and match EXACTLY against our known databento symbols —
    // exact match avoids "ES.v.0" wrongly matching inside "MES.v.0".
    for (const token of m.symbolText.match(/[A-Za-z0-9._-]+/g) ?? []) {
      const symbol = this.dbSymToSymbol.get(token);
      if (!symbol) continue;
      if (this.idToSymbol.get(m.instrumentId) !== symbol) {
        this.idToSymbol.set(m.instrumentId, symbol);
        this.unmappedIds.delete(m.instrumentId);
        console.log(`[live] mapped live instrument ${m.instrumentId} → ${symbol} (${token})`);
      }
      return;
    }
  }

  private onTrade(t: DecodedTrade): void {
    const symbol = this.idToSymbol.get(t.instrumentId);
    if (!symbol) {
      // Trades ARE arriving but their live id isn't in our map — log the first
      // few distinct ids so this exact failure is visible (vs. "no trades at
      // all"). The mapping records above should populate the map and clear this.
      if (this.unmappedIds.size < 12 && !this.unmappedIds.has(t.instrumentId)) {
        this.unmappedIds.add(t.instrumentId);
        console.warn(`[live] trade for unmapped instrument ${t.instrumentId} (have ${this.idToSymbol.size} mappings) — dropped`);
      }
      return;
    }
    const st = this.state.get(symbol);
    if (!st) return;
    const firstForSymbol = !st.havePrice;
    st.price = t.price;
    st.priceTs = t.ts;
    st.havePrice = true;
    if (firstForSymbol) console.log(`[live] first live trade ${symbol} @ ${t.price}`);
    if (st.haveStats) {
      st.high = Math.max(st.high, t.price);
      st.low = Math.min(st.low, t.price);
    }
    this.recordLiveBar(symbol, t.price, t.size, t.ts);
    this.emitQuote(symbol, st, t.size);
  }

  /** Fold a trade print into the rolling 1-minute live-bar buffer for chart backfill. */
  private recordLiveBar(symbol: string, price: number, size: number, tsMs: number): void {
    if (!Number.isFinite(price) || tsMs <= 0) return;
    const p = getInstrument(symbol)!.pricePrecision;
    const px = round(price, p);
    const bucket = Math.floor(tsMs / 60_000) * 60; // minute-aligned, epoch SECONDS
    let bars = this.liveBars.get(symbol);
    if (!bars) {
      bars = [];
      this.liveBars.set(symbol, bars);
    }
    const last = bars[bars.length - 1];
    if (last && last.time === bucket) {
      last.high = Math.max(last.high, px);
      last.low = Math.min(last.low, px);
      last.close = px;
      last.volume += size;
    } else if (!last || bucket > last.time) {
      bars.push({ time: bucket, open: px, high: px, low: px, close: px, volume: size });
      if (bars.length > LIVE_BAR_CAP) bars.shift();
    }
    // Out-of-order prints older than the current bucket are ignored for bar-building.
  }

  // --- 24h stats (Historical HTTP) ---------------------------------

  private scheduleDaily(): void {
    const tick = async () => {
      await this.pollDaily();
      if (this.running) this.dailyTimer = setTimeout(tick, DAILY_POLL_MS);
    };
    this.dailyTimer = setTimeout(tick, DAILY_POLL_MS);
  }

  private async pollDaily(): Promise<void> {
    // Small worker pool, NOT 10-at-once: on a high-latency hop to Databento, ten
    // concurrent historical fetches saturate the link and starve the on-demand
    // chart-history request (which the user is actually waiting on). Cap to 3 so
    // there's always headroom for getHistory.
    const queue = [...INSTRUMENTS];
    const worker = async () => {
      for (;;) {
        const inst = queue.shift();
        if (!inst) return;
        const st = this.state.get(inst.symbol)!;
        try {
          const stats = await fetchDailyStats(this.client, inst.databentoSymbol);
          if (stats) {
            st.dayOpen = stats.dayOpen;
            st.high = Math.max(stats.high, st.havePrice ? st.price : stats.high);
            st.low = Math.min(stats.low, st.havePrice ? st.price : stats.low);
            st.volume = stats.volume;
            st.haveStats = true;
            if (!st.havePrice) st.price = stats.close;
            this.emitQuote(inst.symbol, st); // refresh change/high/low even without a trade
          }
        } catch (err) {
          this.logError(err, `24h-stats ${inst.symbol} (cosmetic — does not affect live price)`);
        }
      }
    };
    await Promise.all([worker(), worker(), worker()]);
  }

  private emitQuote(symbol: string, st: SymState, lastSize = 0): void {
    const inst = getInstrument(symbol)!;
    const p = inst.pricePrecision;
    const spread = inst.tickSize;
    this.emit("quote", {
      symbol,
      price: round(st.price, p),
      bid: round(st.price - spread, p),
      ask: round(st.price + spread, p),
      change24h: st.haveStats && st.dayOpen > 0 ? (st.price - st.dayOpen) / st.dayOpen : 0,
      high24h: round(st.haveStats ? Math.max(st.high, st.price) : st.price, p),
      low24h: round(st.haveStats ? Math.min(st.low, st.price) : st.price, p),
      volume24h: st.haveStats ? Math.round(st.volume) : 0,
      lastSize, // per-trade size so the UI can build live-bar volume (0 on stats refresh)
      ts: st.priceTs || Date.now(),
    });
  }

  /** Emit a real top-10 book from an mbp-10 snapshot, throttled and only for watched symbols. */
  private onBook(b: DecodedBook): void {
    const symbol = this.idToSymbol.get(b.instrumentId);
    if (!symbol || !this.bookSymbols.has(symbol)) return;
    const now = Date.now();
    if (now - (this.bookEmitAt.get(symbol) ?? 0) < BOOK_THROTTLE_MS) return;
    this.bookEmitAt.set(symbol, now);
    const p = getInstrument(symbol)!.pricePrecision;
    this.emit("orderbook", {
      symbol,
      bids: b.bids.map((l) => ({ price: round(l.price, p), size: l.size })),
      asks: b.asks.map((l) => ({ price: round(l.price, p), size: l.size })),
      ts: b.ts || now,
    });
  }

  private logError(err: unknown, context?: string): void {
    const now = Date.now();
    if (this.failing && now - this.lastErrorLogAt < 30_000) return;
    this.failing = true;
    this.lastErrorLogAt = now;
    const e = err as { message?: string; cause?: { code?: string } };
    const detail = e?.cause?.code ?? e?.message ?? "request failed";
    console.warn(`[live] ${context ? `${context}: ` : ""}${detail}`);
  }

  private logRecovered(): void {
    if (!this.failing) return;
    this.failing = false;
    console.log("[live] connection recovered");
  }
}
