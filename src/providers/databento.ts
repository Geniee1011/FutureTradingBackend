import { BaseProvider, round, synthBook } from "./provider.js";
import { DatabentoClient } from "../databento/client.js";
import { INSTRUMENTS, getInstrument, type Instrument } from "../instruments.js";
import type { Candle } from "../types.js";

interface SymState {
  price: number;
  priceTs: number; // epoch ms of the bar the price came from (NOT wall clock)
  dayOpen: number;
  high: number;
  low: number;
  volume: number;
  havePrice: boolean; // a real trade/1s-bar price has arrived
  haveStats: boolean; // rolling-24h stats (pollDaily) have arrived
}

const DAILY_POLL_MS = 30_000;
// Window of 1-second bars to scan for the latest price. Short enough to be fresh,
// wide enough to survive a quiet contract or minor clock skew.
const PRICE_LOOKBACK_MS = 90_000;

/**
 * Databento-backed provider using the Historical HTTP API:
 *  - fast loop polls the latest `ohlcv-1s` bar for each symbol → near-real-time price
 *  - slow loop polls `ohlcv-1h` over 24h → rolling open/high/low/volume
 *  - getHistory serves the chart from `ohlcv-*` (aggregating 1m for 5m/15m)
 *
 * For true tick-by-tick streaming, swap the fast loop for the Live raw-TCP
 * gateway (DBN + CRAM). The provider interface stays identical.
 */
export class DatabentoProvider extends BaseProvider {
  readonly name = "databento";
  private client: DatabentoClient;
  private state = new Map<string, SymState>();
  private quoteTimer: NodeJS.Timeout | null = null;
  private dailyTimer: NodeJS.Timeout | null = null;
  private failing = false;
  private lastErrorLogAt = 0;
  private lastErr: unknown = null;

  constructor(apiKey: string, dataset: string, private readonly quotePollMs: number) {
    super();
    this.client = new DatabentoClient(apiKey, dataset);
    for (const inst of INSTRUMENTS) {
      this.state.set(inst.symbol, {
        price: inst.simBase,
        priceTs: 0,
        dayOpen: 0,
        high: 0,
        low: 0,
        volume: 0,
        havePrice: false,
        haveStats: false,
      });
    }
  }

  start(): void {
    if (this.quoteTimer) return;
    // Start price polling immediately; the 24h stats (pollDaily) fill in
    // alongside so quotes flow within the first cycle instead of waiting on it.
    void this.pollDaily();
    void this.pollQuotes();
    this.quoteTimer = setInterval(() => void this.pollQuotes(), this.quotePollMs);
    this.dailyTimer = setInterval(() => void this.pollDaily(), DAILY_POLL_MS);
  }

  stop(): void {
    if (this.quoteTimer) clearInterval(this.quoteTimer);
    if (this.dailyTimer) clearInterval(this.dailyTimer);
    this.quoteTimer = this.dailyTimer = null;
  }

  /** Only the instruments clients are actually watching. */
  private activeInstruments(): Instrument[] {
    return INSTRUMENTS.filter((i) => this.quoteSymbols.has(i.symbol));
  }

  private async pollQuotes(): Promise<void> {
    const results = await Promise.all(this.activeInstruments().map((inst) => this.pollSymbolPrice(inst)));
    if (results.length === 0) return;
    // Recovered if anything succeeded; only flag an error if the whole cycle failed.
    if (results.every((ok) => !ok)) this.logError(this.lastErr);
    else this.logRecovered();
  }

  /** Resolve each root to its current dated contract code (e.g. ES → ESM6). */
  async resolveContractCodes(): Promise<Record<string, string>> {
    try {
      const end = await this.client.availableEnd("trades");
      const resolved = await this.client.resolveContracts(
        INSTRUMENTS.map((i) => i.databentoSymbol),
        end,
      );
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

  private async pollSymbolPrice(inst: Instrument): Promise<boolean> {
    const st = this.state.get(inst.symbol)!;
    let ok = true;
    try {
      // Latest 1-second bar close = freshest price (bounded record count).
      const bars = await this.client.ohlcv(
        inst.databentoSymbol,
        "ohlcv-1s",
        Date.now() - PRICE_LOOKBACK_MS,
        Date.now(),
      );
      const last = bars.at(-1);
      if (last && Number.isFinite(last.close)) {
        st.price = last.close;
        st.priceTs = last.ts * 1000; // bar's data time, so the chart aligns with history
        st.havePrice = true;
        // Extend the 24h range with the live print only once we have a baseline.
        if (st.haveStats) {
          st.high = Math.max(st.high, last.high);
          st.low = Math.min(st.low, last.low);
        }
      }
    } catch (err) {
      this.lastErr = err;
      ok = false;
    }
    this.emitQuote(inst, st);
    return ok;
  }

  /**
   * Refresh rolling-24h stats from hourly bars: open = 24h ago, high/low =
   * extremes, volume = sum. This avoids the partial-session skew of a single
   * ohlcv-1d bar (which made e.g. GC volume read far too low).
   */
  private async pollDaily(): Promise<void> {
    await Promise.all(
      this.activeInstruments().map(async (inst) => {
        const st = this.state.get(inst.symbol)!;
        try {
          const bars = await this.client.ohlcv(
            inst.databentoSymbol,
            "ohlcv-1h",
            Date.now() - 25 * 3600_000,
            Date.now(),
          );
          const window = bars.slice(-24); // last 24 hourly bars = rolling 24h
          if (window.length > 0) {
            st.dayOpen = window[0]!.open;
            st.high = Math.max(...window.map((b) => b.high));
            st.low = Math.min(...window.map((b) => b.low));
            st.volume = window.reduce((acc, b) => acc + b.volume, 0);
            st.haveStats = true;
            if (!st.havePrice) st.price = window.at(-1)!.close;
          }
        } catch (err) {
          this.logError(err);
        }
      }),
    );
  }

  private emitQuote(inst: Instrument, st: SymState): void {
    const p = inst.pricePrecision;
    const spread = inst.tickSize;
    this.emit("quote", {
      symbol: inst.symbol,
      price: round(st.price, p),
      bid: round(st.price - spread, p),
      ask: round(st.price + spread, p),
      // Until the 24h stats land, report neutral values rather than garbage
      // derived from the seed price.
      change24h: st.haveStats && st.dayOpen > 0 ? (st.price - st.dayOpen) / st.dayOpen : 0,
      high24h: round(st.haveStats ? Math.max(st.high, st.price) : st.price, p),
      low24h: round(st.haveStats ? Math.min(st.low, st.price) : st.price, p),
      volume24h: st.haveStats ? Math.round(st.volume) : 0,
      // Data time (when the print actually occurred), not wall clock — this is
      // what lets the chart's forming candle line up with the loaded history.
      ts: st.priceTs || Date.now(),
    });
    if (this.bookSymbols.has(inst.symbol)) {
      this.emit("orderbook", synthBook(inst.symbol, st.price, inst.tickSize, p));
    }
  }

  async getHistory(symbol: string, resolutionSec: number, count: number): Promise<Candle[]> {
    const inst = getInstrument(symbol);
    if (!inst) return [];
    const p = inst.pricePrecision;

    const native: Record<number, string> = { 1: "ohlcv-1s", 60: "ohlcv-1m", 3600: "ohlcv-1h", 86400: "ohlcv-1d" };
    const nativeSchema = native[resolutionSec];

    if (nativeSchema) {
      const startMs = Date.now() - count * resolutionSec * 1000 * 1.5 - 60_000;
      const bars = await this.client.ohlcv(inst.databentoSymbol, nativeSchema, startMs, Date.now());
      return bars.slice(-count).map((b) => toCandle(b, p));
    }

    // Aggregate 1m bars into the requested resolution (e.g. 5m, 15m).
    const startMs = Date.now() - count * resolutionSec * 1000 * 1.5 - 60_000;
    const minutes = await this.client.ohlcv(inst.databentoSymbol, "ohlcv-1m", startMs, Date.now());
    return aggregate(minutes, resolutionSec, p).slice(-count);
  }

  /** Log a request failure with its cause, at most once every 30s. */
  private logError(err: unknown): void {
    this.lastErr = err;
    const now = Date.now();
    if (this.failing && now - this.lastErrorLogAt < 30_000) return; // cooldown
    this.failing = true;
    this.lastErrorLogAt = now;
    const e = err as { message?: string; cause?: { code?: string; message?: string } };
    const cause = e?.cause?.code ?? e?.cause?.message ?? e?.message ?? "unknown";
    console.warn(`[databento] request failed (keeps retrying): ${cause}`);
  }

  /** Note that requests are succeeding again. */
  private logRecovered(): void {
    if (!this.failing) return;
    this.failing = false;
    console.log("[databento] connection recovered — requests succeeding again");
  }
}

function toCandle(b: { ts: number; open: number; high: number; low: number; close: number; volume: number }, p: number): Candle {
  return {
    time: b.ts,
    open: round(b.open, p),
    high: round(b.high, p),
    low: round(b.low, p),
    close: round(b.close, p),
    volume: b.volume,
  };
}

/** Aggregate 1-minute bars into resolutionSec buckets aligned to the epoch. */
function aggregate(
  minutes: { ts: number; open: number; high: number; low: number; close: number; volume: number }[],
  resolutionSec: number,
  precision: number,
): Candle[] {
  const buckets = new Map<number, Candle>();
  for (const m of minutes) {
    const bucket = m.ts - (m.ts % resolutionSec);
    const existing = buckets.get(bucket);
    if (!existing) {
      buckets.set(bucket, {
        time: bucket,
        open: round(m.open, precision),
        high: round(m.high, precision),
        low: round(m.low, precision),
        close: round(m.close, precision),
        volume: m.volume,
      });
    } else {
      existing.high = Math.max(existing.high, round(m.high, precision));
      existing.low = Math.min(existing.low, round(m.low, precision));
      existing.close = round(m.close, precision);
      existing.volume += m.volume;
    }
  }
  return [...buckets.values()].sort((a, b) => a.time - b.time);
}
