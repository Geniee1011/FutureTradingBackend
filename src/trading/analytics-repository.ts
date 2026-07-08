/* Aggregate reads for the admin Analytics page — Overall (across all traders)
 * and Per-Trader. All grouped stats read the "at open" snapshot columns captured
 * on each ClosedPosition, so they reflect the trader's state when the trade was
 * opened. Pure reads; no mutation. */

import { getPool } from "../db/pool.js";
import { sessionAnchor } from "./session-time.js";
import { getTraderState, type TraderState } from "./trader-stats.js";

const num = (v: unknown): number => {
  const x = typeof v === "number" ? v : Number(v);
  return Number.isFinite(x) ? x : 0;
};

export interface Bucketed {
  label: string;
  n: number;
  winRate: number; // 0-100
}

export interface OverallAnalytics {
  avgPnlByPhase: { phase: number; avgPnl: number; n: number }[];
  winRateByConsecutiveLosses: Bucketed[];
  winRateByDailyLoss: Bucketed[];
  winRateBySizeDeviation: Bucketed[];
  lifetimeWinRateHistogram: { label: string; n: number }[];
  mostTradedInstruments: { symbol: string; n: number }[];
  shadowPnlCurve: { day: string; value: number }[]; // cumulative opposite-side P&L
  totalClosedTrades: number;
}

export async function analyticsOverall(): Promise<OverallAnalytics> {
  const pool = getPool();

  const [phasePnl, byLosses, byDaily, bySize, hist, instruments, shadow, total] = await Promise.all([
    pool.query(
      `SELECT "phaseAtOpen" AS phase, AVG("realizedPnl") AS avgpnl, COUNT(*)::int AS n
       FROM "ClosedPosition" WHERE "phaseAtOpen" IS NOT NULL GROUP BY "phaseAtOpen"`,
    ),
    pool.query(
      `SELECT LEAST("consecutiveLossesAtOpen", 4) AS bucket, COUNT(*)::int AS n,
              COUNT(*) FILTER (WHERE "realizedPnl" > 0)::int AS wins
       FROM "ClosedPosition" WHERE "consecutiveLossesAtOpen" IS NOT NULL GROUP BY bucket`,
    ),
    pool.query(
      `SELECT CASE WHEN "dailyLossPctAtOpen" < 20 THEN 0 WHEN "dailyLossPctAtOpen" < 40 THEN 1
                   WHEN "dailyLossPctAtOpen" < 70 THEN 2 ELSE 3 END AS bucket,
              COUNT(*)::int AS n, COUNT(*) FILTER (WHERE "realizedPnl" > 0)::int AS wins
       FROM "ClosedPosition" WHERE "dailyLossPctAtOpen" IS NOT NULL GROUP BY bucket`,
    ),
    pool.query(
      `SELECT CASE WHEN "sizeDeviationAtOpen" < 1 THEN 0 WHEN "sizeDeviationAtOpen" < 1.5 THEN 1
                   WHEN "sizeDeviationAtOpen" < 2.5 THEN 2 ELSE 3 END AS bucket,
              COUNT(*)::int AS n, COUNT(*) FILTER (WHERE "realizedPnl" > 0)::int AS wins
       FROM "ClosedPosition" WHERE "sizeDeviationAtOpen" IS NOT NULL GROUP BY bucket`,
    ),
    pool.query(
      `SELECT LEAST(FLOOR("lifetimeWinRate" / 10), 9)::int AS bucket, COUNT(*)::int AS n
       FROM "TraderStats" WHERE "lifetimeTradeCount" > 0 GROUP BY bucket`,
    ),
    pool.query(
      `SELECT "symbol", COUNT(*)::int AS n FROM "ClosedPosition" GROUP BY "symbol" ORDER BY n DESC`,
    ),
    pool.query(
      `SELECT to_char(date_trunc('day', "closedAt"), 'YYYY-MM-DD') AS day, SUM(-"realizedPnl") AS shadow
       FROM "ClosedPosition" GROUP BY day ORDER BY day`,
    ),
    pool.query(`SELECT COUNT(*)::int AS n FROM "ClosedPosition"`),
  ]);

  // avg P&L by phase (1-4, fill gaps).
  const phaseMap = new Map<number, { avgPnl: number; n: number }>();
  for (const r of phasePnl.rows) phaseMap.set(num(r.phase), { avgPnl: round2(num(r.avgpnl)), n: num(r.n) });
  const avgPnlByPhase = [1, 2, 3, 4].map((phase) => ({ phase, avgPnl: phaseMap.get(phase)?.avgPnl ?? 0, n: phaseMap.get(phase)?.n ?? 0 }));

  const bucketize = (rows: { bucket: unknown; n: unknown; wins: unknown }[], labels: string[]): Bucketed[] => {
    const m = new Map<number, { n: number; wins: number }>();
    for (const r of rows) m.set(num(r.bucket), { n: num(r.n), wins: num(r.wins) });
    return labels.map((label, i) => {
      const b = m.get(i) ?? { n: 0, wins: 0 };
      return { label, n: b.n, winRate: b.n > 0 ? round2((b.wins / b.n) * 100) : 0 };
    });
  };

  const histMap = new Map<number, number>();
  for (const r of hist.rows) histMap.set(num(r.bucket), num(r.n));
  const lifetimeWinRateHistogram = Array.from({ length: 10 }, (_, i) => ({
    label: `${i * 10}-${i * 10 + 10}%`,
    n: histMap.get(i) ?? 0,
  }));

  // Cumulative shadow P&L (opposite side of every closed trade), day by day.
  let cum = 0;
  const shadowPnlCurve = shadow.rows.map((r) => {
    cum += num(r.shadow);
    return { day: String(r.day), value: round2(cum) };
  });

  return {
    avgPnlByPhase,
    winRateByConsecutiveLosses: bucketize(byLosses.rows, ["0", "1", "2", "3", "4+"]),
    winRateByDailyLoss: bucketize(byDaily.rows, ["0-20%", "20-40%", "40-70%", "70%+"]),
    winRateBySizeDeviation: bucketize(bySize.rows, ["<1x", "1-1.5x", "1.5-2.5x", ">2.5x"]),
    lifetimeWinRateHistogram,
    mostTradedInstruments: instruments.rows.map((r) => ({ symbol: String(r.symbol), n: num(r.n) })),
    shadowPnlCurve,
    totalClosedTrades: num(total.rows[0]?.n),
  };
}

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
  phaseAtOpen: number | null;
}

export interface TraderAnalytics {
  accountId: string;
  state: TraderState | null;
  tradesByPhase: { phase: number; n: number }[];
  sessionCurve: { time: number; value: number }[]; // cumulative session P&L (epoch seconds)
  trades: TraderTrade[];
}

/** Resolve a trader's account id from either the account id or the user id. */
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
  const pool = getPool();
  const anchor = sessionAnchor();

  const [state, byPhase, curve, trades] = await Promise.all([
    getTraderState(accountId),
    pool.query(
      `SELECT "phaseAtOpen" AS phase, COUNT(*)::int AS n
       FROM "ClosedPosition" WHERE "accountId" = $1 AND "phaseAtOpen" IS NOT NULL GROUP BY phase`,
      [accountId],
    ),
    pool.query(
      `SELECT "closedAt", "realizedPnl" FROM "ClosedPosition"
       WHERE "accountId" = $1 AND "closedAt" >= $2 ORDER BY "closedAt" ASC`,
      [accountId, anchor],
    ),
    pool.query(
      `SELECT "id","symbol","side","quantity","entryPrice","exitPrice","realizedPnl","openedAt","closedAt","phaseAtOpen"
       FROM "ClosedPosition" WHERE "accountId" = $1 ORDER BY "closedAt" DESC LIMIT 300`,
      [accountId],
    ),
  ]);

  const phaseMap = new Map<number, number>();
  for (const r of byPhase.rows) phaseMap.set(num(r.phase), num(r.n));
  const tradesByPhase = [1, 2, 3, 4].map((phase) => ({ phase, n: phaseMap.get(phase) ?? 0 }));

  let cum = 0;
  const sessionCurve = curve.rows.map((r) => {
    cum += num(r.realizedPnl);
    return { time: Math.floor(new Date(r.closedAt).getTime() / 1000), value: round2(cum) };
  });

  return {
    accountId,
    state,
    tradesByPhase,
    sessionCurve,
    trades: trades.rows.map((r) => ({
      id: String(r.id),
      symbol: String(r.symbol),
      side: String(r.side),
      quantity: num(r.quantity),
      entryPrice: num(r.entryPrice),
      exitPrice: num(r.exitPrice),
      realizedPnl: num(r.realizedPnl),
      openedAt: new Date(r.openedAt).getTime(),
      closedAt: new Date(r.closedAt).getTime(),
      phaseAtOpen: r.phaseAtOpen == null ? null : num(r.phaseAtOpen),
    })),
  };
}

const round2 = (n: number) => Math.round(n * 100) / 100;
