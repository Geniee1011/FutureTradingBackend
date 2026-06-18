import { DatabentoLiveProvider } from "../providers/databento-live.js";
import { config } from "../config.js";
import type { Candle, Quote } from "../types.js";

/* Per-user real-time market sessions (Model B). Each connected user gets their
   OWN Databento LIVE session (raw TCP / DBN) authenticated with THEIR key, so
   they receive real-time data under their own entitlement — never shared with
   another user (the no-redistribution property). Sessions are created lazily on
   first request and reaped after a period of inactivity to release the TCP
   connection. */

const IDLE_MS = 3 * 60_000; // tear down a user's Live session after 3 min idle
const REAP_MS = 60_000;

interface Session {
  provider: DatabentoLiveProvider;
  lastUsed: number;
}

class ByoSessionManager {
  private sessions = new Map<string, Session>();
  private reaper: NodeJS.Timeout | null = null;
  private markSink: ((symbol: string, price: number) => void) | null = null;

  /**
   * Register a sink that receives every live quote from every user's session, so
   * the execution engine has real marks in byo mode (it otherwise streams nothing
   * shared). A symbol's price is identical across users, so one global sink is fine.
   */
  setMarkSink(sink: (symbol: string, price: number) => void): void {
    this.markSink = sink;
  }

  /** Get (or lazily start) a user's Live session and mark it active. */
  private ensure(userId: string, apiKey: string): DatabentoLiveProvider {
    let s = this.sessions.get(userId);
    if (!s) {
      const provider = new DatabentoLiveProvider(apiKey, config.databento.dataset);
      // Forward this user's live prices to the shared mark sink (order fills + P&L).
      provider.on("quote", (q) => this.markSink?.(q.symbol, q.price));
      provider.start();
      s = { provider, lastUsed: Date.now() };
      this.sessions.set(userId, s);
      this.startReaper();
      console.log(`[byo] started Live session for user ${userId} (${this.sessions.size} active)`);
    } else {
      s.lastUsed = Date.now();
    }
    return s.provider;
  }

  /** Real-time chart history (historical backfill + the user's live bars). */
  history(userId: string, apiKey: string, symbol: string, resolutionSec: number, count: number): Promise<Candle[]> {
    return this.ensure(userId, apiKey).getHistory(symbol, resolutionSec, count);
  }

  /** Latest real-time quote for a symbol (in-memory snapshot from the live stream). */
  quote(userId: string, apiKey: string, symbol: string): Quote | undefined {
    return this.ensure(userId, apiKey).getQuoteSnapshot(symbol);
  }

  /** Tear down a user's session (on disconnect or key replacement). */
  drop(userId: string): void {
    const s = this.sessions.get(userId);
    if (!s) return;
    s.provider.stop();
    this.sessions.delete(userId);
    console.log(`[byo] stopped Live session for user ${userId}`);
  }

  private startReaper(): void {
    if (this.reaper) return;
    this.reaper = setInterval(() => {
      const now = Date.now();
      for (const [userId, s] of this.sessions) {
        if (now - s.lastUsed > IDLE_MS) {
          s.provider.stop();
          this.sessions.delete(userId);
          console.log(`[byo] reaped idle Live session for user ${userId}`);
        }
      }
      if (this.sessions.size === 0 && this.reaper) {
        clearInterval(this.reaper);
        this.reaper = null;
      }
    }, REAP_MS);
  }
}

export const byoSessions = new ByoSessionManager();
