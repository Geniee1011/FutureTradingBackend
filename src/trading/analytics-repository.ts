/* Aggregate reads for the admin Analytics page — Overall (across all traders)
 * and Per-Trader.
 *
 * The grouped charts are computed from the FULL closed-trade history by
 * RECONSTRUCTING each trade's context (losing streak, daily-loss consumed,
 * size deviation, and the resulting risk phase) as it was when the trade opened
 * — walking each account's trades in chronological order. This means the charts
 * work on ALL existing trades, not only those tagged with the go-forward
 * "at open" snapshot. (The snapshot columns remain the exact recorded value.)
 *
 * Reconstruction is an approximation of the live state (it sees only closed
 * trades, so e.g. daily-loss is realized-only), which is more than adequate for
 * "does win rate drop as risk rises?"-style questions. */

import { getPool } from "../db/pool.js";
import { sessionAnchor, etDateKey } from "./session-time.js";
import { parentInstrument, getTraderState, type TraderState } from "./trader-stats.js";
import { loadActiveRules, evaluatePhase } from "./phase-rules.js";

const num = (v: unknown): number => {
  const x = typeof v === "number" ? v : Number(v);
  return Number.isFinite(x) ? x : 0;
};
const round2 = (n: number) => Math.round(n * 100) / 100;

// --- per-trade reconstruction ------------------------------------

interface ReconTrade {
  accountId: string;
  id: string;
  symbol: string;
  side: string;
  quantity: number;
  entryPrice: number;
  exitPrice: number;
  realizedPnl: number;
  openedAt: number;
  closedAt: number;
  win: boolean;
  phase: number;
  consecLossesBefore: number;
  dailyLossPct: number;
  sizeDev: number;
}

/**
 * Rebuild each closed trade's at-open context from history. Trades are processed
 * per account in open-time order; running state (streak, same-day realized P&L,
 * trailing 7-day sizes) is captured BEFORE folding in each trade's own result.
 */
async function reconstructTrades(accountIds?: string[]): Promise<ReconTrade[]> {
  const pool = getPool();
  const rules = await loadActiveRules(pool);
  const params: unknown[] = [];
  let where = "";
  if (accountIds?.length) { where = `WHERE c."accountId" = ANY($1)`; params.push(accountIds); }
  const { rows } = await pool.query(
    `SELECT c."accountId", c."id", c."symbol", c."side", c."quantity", c."entryPrice", c."exitPrice",
            c."realizedPnl", c."openedAt", c."closedAt", COALESCE(r."maxDailyLoss", 0) AS "maxDailyLoss"
     FROM "ClosedPosition" c LEFT JOIN "Rule" r ON r."accountId" = c."accountId"
     ${where}
     ORDER BY c."accountId", c."openedAt" ASC, c."id" ASC`,
    params,
  );

  const out: ReconTrade[] = [];
  let curAcct = "";
  let streakLoss = 0, streakWin = 0;
  let dayKey = "", daySum = 0, dayCount = 0, dayWins = 0;
  let recent: { t: number; parent: string; qty: number }[] = [];

  for (const r of rows) {
    if (r.accountId !== curAcct) {
      curAcct = r.accountId;
      streakLoss = streakWin = 0; dayKey = ""; daySum = dayCount = dayWins = 0; recent = [];
    }
    const openedAt = new Date(r.openedAt).getTime();
    const closedAt = new Date(r.closedAt).getTime();
    const pnl = num(r.realizedPnl);
    const qty = num(r.quantity);
    const parent = parentInstrument(r.symbol);
    const maxDailyLoss = num(r.maxDailyLoss);

    const dk = etDateKey(new Date(openedAt));
    if (dk !== dayKey) { dayKey = dk; daySum = 0; dayCount = 0; dayWins = 0; }

    // Context BEFORE this trade's own result.
    const consecLossesBefore = streakLoss;
    const dailyLossPct = daySum < 0 && maxDailyLoss > 0 ? Math.min(100, (-daySum / maxDailyLoss) * 100) : 0;
    const cutoff = openedAt - 7 * 86_400_000;
    recent = recent.filter((s) => s.t >= cutoff);
    const sameParent = recent.filter((s) => s.parent === parent);
    const avg = sameParent.length ? sameParent.reduce((a, s) => a + s.qty, 0) / sameParent.length : 0;
    const sizeDev = avg > 0 ? qty / avg : 1;

    const phase = evaluatePhase(rules, {
      consecutive_losses: consecLossesBefore,
      consecutive_wins: streakWin,
      session_trade_count: dayCount,
      daily_loss_pct_consumed: dailyLossPct,
      session_pnl: daySum,
      session_win_rate: dayCount ? (dayWins / dayCount) * 100 : 0,
      time_in_session_minutes: 0,
      size_deviation_ratio: sizeDev,
      current_drawdown_consumed_pct: 0,
      current_challenge_pnl_pct: 0,
      challenge_day: 1,
      reset_count: 0,
      lifetime_win_rate: 0,
      lifetime_trade_count: 0,
    });

    const win = pnl > 0;
    out.push({
      accountId: r.accountId, id: String(r.id), symbol: r.symbol, side: r.side, quantity: qty,
      entryPrice: num(r.entryPrice), exitPrice: num(r.exitPrice), realizedPnl: pnl,
      openedAt, closedAt, win, phase, consecLossesBefore, dailyLossPct: round2(dailyLossPct), sizeDev: round2(sizeDev),
    });

    // Fold this trade's result into the running state.
    if (pnl > 0) { streakWin++; streakLoss = 0; }
    else if (pnl < 0) { streakLoss++; streakWin = 0; }
    else { streakLoss = 0; streakWin = 0; }
    daySum += pnl; dayCount++; if (win) dayWins++;
    recent.push({ t: openedAt, parent, qty });
  }
  return out;
}

// --- Overall ------------------------------------------------------

export interface Bucketed { label: string; n: number; winRate: number; }

export interface OverallAnalytics {
  avgPnlByPhase: { phase: number; avgPnl: number; n: number }[];
  winRateByConsecutiveLosses: Bucketed[];
  winRateByDailyLoss: Bucketed[];
  winRateBySizeDeviation: Bucketed[];
  lifetimeWinRateHistogram: { label: string; n: number }[];
  mostTradedInstruments: { symbol: string; n: number }[];
  shadowPnlCurve: { day: string; value: number }[];
  totalClosedTrades: number;
}

function winBuckets(trades: ReconTrade[], keyFn: (t: ReconTrade) => number, labels: string[]): Bucketed[] {
  const acc = labels.map(() => ({ n: 0, wins: 0 }));
  for (const t of trades) {
    const i = keyFn(t);
    if (i < 0 || i >= labels.length) continue;
    acc[i]!.n++;
    if (t.win) acc[i]!.wins++;
  }
  return labels.map((label, i) => ({ label, n: acc[i]!.n, winRate: acc[i]!.n ? round2((acc[i]!.wins / acc[i]!.n) * 100) : 0 }));
}

export async function analyticsOverall(): Promise<OverallAnalytics> {
  const recon = await reconstructTrades();

  // Avg P&L by (reconstructed) phase.
  const byPhase = new Map<number, { sum: number; n: number }>();
  for (const t of recon) {
    const e = byPhase.get(t.phase) ?? { sum: 0, n: 0 };
    e.sum += t.realizedPnl; e.n++;
    byPhase.set(t.phase, e);
  }
  const avgPnlByPhase = [1, 2, 3, 4].map((phase) => {
    const e = byPhase.get(phase);
    return { phase, avgPnl: e && e.n ? round2(e.sum / e.n) : 0, n: e?.n ?? 0 };
  });

  // Lifetime win-rate distribution across all traders (from full history).
  const byAcct = new Map<string, { n: number; wins: number }>();
  for (const t of recon) {
    const e = byAcct.get(t.accountId) ?? { n: 0, wins: 0 };
    e.n++; if (t.win) e.wins++;
    byAcct.set(t.accountId, e);
  }
  const histBins = Array<number>(10).fill(0);
  for (const e of byAcct.values()) {
    if (!e.n) continue;
    histBins[Math.min(9, Math.floor(((e.wins / e.n) * 100) / 10))]!++;
  }
  const lifetimeWinRateHistogram = histBins.map((n, i) => ({ label: `${i * 10}-${i * 10 + 10}%`, n }));

  // Most traded instruments.
  const bySym = new Map<string, number>();
  for (const t of recon) bySym.set(t.symbol, (bySym.get(t.symbol) ?? 0) + 1);
  const mostTradedInstruments = [...bySym.entries()].map(([symbol, n]) => ({ symbol, n })).sort((a, b) => b.n - a.n);

  // Cumulative shadow P&L (opposite side of every closed trade) by trading day.
  const byDay = new Map<string, number>();
  for (const t of recon) {
    const day = etDateKey(new Date(t.closedAt));
    byDay.set(day, (byDay.get(day) ?? 0) + -t.realizedPnl);
  }
  let cum = 0;
  const shadowPnlCurve = [...byDay.keys()].sort().map((day) => { cum += byDay.get(day)!; return { day, value: round2(cum) }; });

  return {
    avgPnlByPhase,
    winRateByConsecutiveLosses: winBuckets(recon, (t) => Math.min(t.consecLossesBefore, 4), ["0", "1", "2", "3", "4+"]),
    winRateByDailyLoss: winBuckets(recon, (t) => (t.dailyLossPct < 20 ? 0 : t.dailyLossPct < 40 ? 1 : t.dailyLossPct < 70 ? 2 : 3), ["0-20%", "20-40%", "40-70%", "70%+"]),
    winRateBySizeDeviation: winBuckets(recon, (t) => (t.sizeDev < 1 ? 0 : t.sizeDev < 1.5 ? 1 : t.sizeDev < 2.5 ? 2 : 3), ["<1x", "1-1.5x", "1.5-2.5x", ">2.5x"]),
    lifetimeWinRateHistogram,
    mostTradedInstruments,
    shadowPnlCurve,
    totalClosedTrades: recon.length,
  };
}

// --- Per-trader ---------------------------------------------------

export interface TraderTrade {
  id: string;
  symbol: string;
  side: string;
  quantity: number;
  entryPrice: number;
  exitPrice: number;
  realizedPnl: number;
  openedAt: number;
  closedAt: number;
  phaseAtOpen: number | null; // reconstructed phase for this trade
}

export interface TraderAnalytics {
  accountId: string;
  state: TraderState | null;
  tradesByPhase: { phase: number; n: number }[];
  sessionCurve: { time: number; value: number }[];
  trades: TraderTrade[];
}

async function resolveAccountId(idOrUserId: string): Promise<string | null> {
  const { rows } = await getPool().query<{ id: string }>(
    `SELECT "id" FROM "Account" WHERE "id" = $1 OR "userId" = $1 LIMIT 1`,
    [idOrUserId],
  );
  return rows[0]?.id ?? null;
}

export async function analyticsTrader(idOrUserId: string): Promise<TraderAnalytics | null> {
  const accountId = await resolveAccountId(idOrUserId);
  if (!accountId) return null;

  const recon = await reconstructTrades([accountId]);
  const state = await getTraderState(accountId);

  // Override the lifetime fields with values computed from the FULL history, so
  // accounts that only have pre-feature trades still show real lifetime stats.
  if (state && recon.length) {
    const wins = recon.filter((t) => t.win).length;
    state.lifetimeTradeCount = recon.length;
    state.lifetimeWinRate = round2((wins / recon.length) * 100);
    const wr = (parent: string) => {
      const g = recon.filter((t) => parentInstrument(t.symbol) === parent);
      return g.length ? round2((g.filter((t) => t.win).length / g.length) * 100) : 0;
    };
    state.lifetimeWinRateEs = wr("ES");
    state.lifetimeWinRateNq = wr("NQ");
    state.lifetimeWinRateGc = wr("GC");
    state.lifetimeWinRateCl = wr("CL");
    const winsArr = recon.filter((t) => t.realizedPnl > 0).map((t) => t.realizedPnl);
    const lossArr = recon.filter((t) => t.realizedPnl < 0).map((t) => t.realizedPnl);
    state.lifetimeAvgWin = winsArr.length ? round2(winsArr.reduce((a, b) => a + b, 0) / winsArr.length) : 0;
    state.lifetimeAvgLoss = lossArr.length ? round2(lossArr.reduce((a, b) => a + b, 0) / lossArr.length) : 0;
  }

  const phaseMap = new Map<number, number>();
  for (const t of recon) phaseMap.set(t.phase, (phaseMap.get(t.phase) ?? 0) + 1);
  const tradesByPhase = [1, 2, 3, 4].map((phase) => ({ phase, n: phaseMap.get(phase) ?? 0 }));

  const anchor = sessionAnchor().getTime();
  let cum = 0;
  const sessionCurve = recon
    .filter((t) => t.closedAt >= anchor)
    .sort((a, b) => a.closedAt - b.closedAt)
    .map((t) => { cum += t.realizedPnl; return { time: Math.floor(t.closedAt / 1000), value: round2(cum) }; });

  const trades = [...recon]
    .sort((a, b) => b.closedAt - a.closedAt)
    .slice(0, 300)
    .map((t) => ({
      id: t.id, symbol: t.symbol, side: t.side, quantity: t.quantity, entryPrice: t.entryPrice,
      exitPrice: t.exitPrice, realizedPnl: t.realizedPnl, openedAt: t.openedAt, closedAt: t.closedAt,
      phaseAtOpen: t.phase,
    }));

  return { accountId, state, tradesByPhase, sessionCurve, trades };
}
