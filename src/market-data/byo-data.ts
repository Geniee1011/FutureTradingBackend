import { byoSessions } from "./byo-session.js";
import type { Candle, Quote } from "../types.js";

/* Per-user (Model B) market-data fetches, served from each user's OWN real-time
   Databento LIVE session (see byo-session.ts). Data is sourced under that user's
   own entitlement and never shared — the property that keeps Model B out of
   redistribution. Real-time (not the delayed Historical API). */

/** Chart history (real-time: historical backfill + the user's live bars). */
export function fetchUserHistory(
  userId: string,
  apiKey: string,
  symbol: string,
  resolutionSec: number,
  count: number,
): Promise<Candle[]> {
  return byoSessions.history(userId, apiKey, symbol, resolutionSec, count);
}

/** Latest real-time quote for a symbol (full Quote incl. 24h stats), or null until warm. */
export function fetchUserQuote(userId: string, apiKey: string, symbol: string): Quote | null {
  return byoSessions.quote(userId, apiKey, symbol) ?? null;
}
