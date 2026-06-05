import { EventEmitter } from "node:events";
import type { Candle, OrderBook, Quote } from "../types.js";

/**
 * A market-data source. Emits `quote` (all instruments, continuously) and
 * `orderbook` (only for symbols passed to setBookSymbols). Exposes historical
 * candles for the chart. Swap implementations (Simulation ↔ Databento) without
 * touching the server.
 */
export interface MarketDataProvider extends EventEmitter {
  /** Begin streaming. */
  start(): void;
  /** Stop streaming and release resources. */
  stop(): void;
  /** Set which symbols to actively poll/stream quotes for (subscriber-driven). */
  setQuoteSymbols(symbols: Set<string>): void;
  /** Set which symbols should have a live order book emitted. */
  setBookSymbols(symbols: Set<string>): void;
  /** Historical OHLCV candles, newest last. resolutionSec e.g. 60, 300, 3600. */
  getHistory(symbol: string, resolutionSec: number, count: number): Promise<Candle[]>;
  /** Human-readable source name for /health and logs. */
  readonly name: string;

  on(event: "quote", listener: (q: Quote) => void): this;
  on(event: "orderbook", listener: (b: OrderBook) => void): this;
  emit(event: "quote", q: Quote): boolean;
  emit(event: "orderbook", b: OrderBook): boolean;
}

/** Shared base with the EventEmitter plumbing and book-symbol bookkeeping. */
export abstract class BaseProvider extends EventEmitter implements MarketDataProvider {
  abstract readonly name: string;
  protected quoteSymbols = new Set<string>();
  protected bookSymbols = new Set<string>();

  abstract start(): void;
  abstract stop(): void;
  abstract getHistory(symbol: string, resolutionSec: number, count: number): Promise<Candle[]>;

  setQuoteSymbols(symbols: Set<string>): void {
    this.quoteSymbols = new Set(symbols);
  }

  setBookSymbols(symbols: Set<string>): void {
    this.bookSymbols = new Set(symbols);
  }
}

/** Round a number to a fixed number of decimals. */
export function round(value: number, decimals: number): number {
  const f = 10 ** decimals;
  return Math.round(value * f) / f;
}

/**
 * Build a plausible depth ladder around a mid price. Real top-of-book depth
 * (Databento mbp-1/mbp-10) can replace this; until then it gives the UI a
 * populated book derived from the live price. Marked synthetic in the README.
 */
export function synthBook(
  symbol: string,
  mid: number,
  tickSize: number,
  pricePrecision: number,
): OrderBook {
  const levels = 12;
  const bids = Array.from({ length: levels }, (_, i) => ({
    price: round(mid - tickSize * (i + 1), pricePrecision),
    size: round(Math.random() * 40 + 1, 0),
  }));
  const asks = Array.from({ length: levels }, (_, i) => ({
    price: round(mid + tickSize * (i + 1), pricePrecision),
    size: round(Math.random() * 40 + 1, 0),
  }));
  return { symbol, bids, asks, ts: Date.now() };
}
