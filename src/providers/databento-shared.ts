import { HISTORY_RETRIES, HISTORY_TIMEOUT_MS } from "../databento/client.js";
import type { DatabentoClient, OhlcvRecord } from "../databento/client.js";
import { round } from "./provider.js";
import type { Candle } from "../types.js";

/* History + 24h-stats helpers shared by the polling and live Databento providers
   (both still use the Historical HTTP API for chart bars and rolling-24h stats). */

const NATIVE_SCHEMA: Record<number, string> = {
  1: "ohlcv-1s",
  60: "ohlcv-1m",
  3600: "ohlcv-1h",
  86400: "ohlcv-1d",
};

/**
 * Extra calendar lookback (~4 days) added on top of the exact bar span so the
 * window crosses weekends/holidays. Without it, an intraday request made on a
 * weekend lands entirely in the non-trading gap and returns zero bars — leaving
 * the chart blank. We over-fetch and then keep the most recent `count` bars.
 */
const GAP_PAD_MS = 4 * 24 * 3600_000;

export async function fetchHistory(
  client: DatabentoClient,
  dbSymbol: string,
  resolutionSec: number,
  count: number,
  precision: number,
  opts?: { retries?: number; timeoutMs?: number },
): Promise<Candle[]> {
  const startMs = Date.now() - count * resolutionSec * 1000 - GAP_PAD_MS;
  const native = NATIVE_SCHEMA[resolutionSec];
  // Heavy historical fetches need the longer budget — the default 10s/1-retry
  // isn't enough for Databento's cold-start latency (was silently timing out).
  // Callers warming a background cache pass an even longer timeout (slow hop is
  // fine when it doesn't block the chart response).
  const histOpts = {
    retries: opts?.retries ?? HISTORY_RETRIES,
    timeoutMs: opts?.timeoutMs ?? HISTORY_TIMEOUT_MS,
  };
  if (native) {
    const bars = await client.ohlcv(dbSymbol, native, startMs, Date.now(), undefined, histOpts);
    return bars.slice(-count).map((b) => toCandle(b, precision));
  }
  // Aggregate 1-minute bars for non-native resolutions (e.g. 5m, 15m).
  const minutes = await client.ohlcv(dbSymbol, "ohlcv-1m", startMs, Date.now(), undefined, histOpts);
  return aggregate(minutes, resolutionSec, precision).slice(-count);
}

export interface DailyStats {
  dayOpen: number;
  high: number;
  low: number;
  volume: number;
  close: number;
}

/**
 * Rolling 24h stats from hourly bars: open = 24h ago, high/low extremes, volume sum.
 * Uses a 5-day lookback (not 25h) so weekends/holidays — when the dataset's
 * available-end advances but no bars print — still resolve to the most recent
 * 24 *trading* hours (the last session) instead of an empty window.
 */
export async function fetchDailyStats(client: DatabentoClient, dbSymbol: string): Promise<DailyStats | null> {
  // No retries here: this is the cosmetic 24h-stats poll, re-run every 30s, so a
  // failed attempt self-heals on the next tick. Retrying 3× per symbol just piles
  // more load onto the rate/bandwidth-limited Historical link and starves the
  // user-facing chart-history fetch (which keeps its retries).
  const bars = await client.ohlcv(dbSymbol, "ohlcv-1h", Date.now() - 5 * 24 * 3600_000, Date.now(), undefined, {
    retries: 0,
    timeoutMs: HISTORY_TIMEOUT_MS,
  });
  const window = bars.slice(-24);
  if (window.length === 0) return null;
  return {
    dayOpen: window[0]!.open,
    high: Math.max(...window.map((b) => b.high)),
    low: Math.min(...window.map((b) => b.low)),
    volume: window.reduce((acc, b) => acc + b.volume, 0),
    close: window.at(-1)!.close,
  };
}

/**
 * Aggregate minute-grained candles (e.g. a live trade-built buffer) into
 * `resolutionSec` buckets. Identity for resolutions <= 60s. Used to merge the
 * live provider's rolling 1-minute bars into a higher-resolution history series.
 */
export function aggregateCandles(oneMin: Candle[], resolutionSec: number): Candle[] {
  if (resolutionSec <= 60) return oneMin.slice();
  const buckets = new Map<number, Candle>();
  for (const m of oneMin) {
    const bucket = m.time - (m.time % resolutionSec);
    const ex = buckets.get(bucket);
    if (!ex) {
      buckets.set(bucket, { ...m, time: bucket });
    } else {
      ex.high = Math.max(ex.high, m.high);
      ex.low = Math.min(ex.low, m.low);
      ex.close = m.close;
      ex.volume += m.volume;
    }
  }
  return [...buckets.values()].sort((a, b) => a.time - b.time);
}

function toCandle(b: OhlcvRecord, p: number): Candle {
  return {
    time: b.ts,
    open: round(b.open, p),
    high: round(b.high, p),
    low: round(b.low, p),
    close: round(b.close, p),
    volume: b.volume,
  };
}

function aggregate(minutes: OhlcvRecord[], resolutionSec: number, precision: number): Candle[] {
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
