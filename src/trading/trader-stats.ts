/* Per-trader analytics state: recompute lifetime / session / streak stats and
 * the behavioural risk phase on every trade event, and expose them for reads.
 *
 * - Lifetime + 7d-avg-size fields recompute from ClosedPosition on each close.
 * - Session fields (pnl, counts, win rate) recompute from ClosedPosition since
 *   the 9:30 ET session anchor; sessionTradeCount is a counter bumped on open.
 * - Session state resets lazily at the 9:30 ET boundary (see session-time.ts).
 * - Per-instrument win rate / 7d-avg-size roll micros into their parent
 *   (MES→ES, MNQ→NQ, MGC→GC, MCL→CL); ES/NQ/GC/CL only, YM in totals.
 * - The phase is (re)assigned by the editable PhaseRule engine. */

import type { Pool, PoolClient } from "pg";
import { getPool } from "../db/pool.js";
import { sessionAnchor, minutesInSession } from "./session-time.js";
import { loadActiveRules, evaluatePhase } from "./phase-rules.js";

type Db = Pool | PoolClient;

const PARENT: Record<string, string> = { MES: "ES", MNQ: "NQ", MGC: "GC", MCL: "CL", MYM: "YM" };
/** Micro → parent root (MES→ES); YM/MYM collapse to YM (not a tracked bucket). */
export function parentInstrument(symbol: string | null): string {
  if (!symbol) return "";
  return PARENT[symbol] ?? symbol;
}
/** The four instruments broken out in per-instrument stats. */
const TRACKED = ["ES", "NQ", "GC", "CL"] as const;
const TRACKED_SYMBOLS = ["ES", "MES", "NQ", "MNQ", "GC", "MGC", "CL", "MCL"];

const round2 = (n: number) => Math.round(n * 100) / 100;

/** The "at open" snapshot stamped on the lot and carried onto the closed trade. */
export interface OpenSnapshot {
  phaseAtOpen: number;
  scoreAtOpen: number | null;
  consecutiveLossesAtOpen: number;
  dailyLossPctAtOpen: number;
  sizeDeviationAtOpen: number;
}

/** The 14 variables the rules engine + analytics reference. */
export interface TraderVariables {
  consecutive_losses: number;
  consecutive_wins: number;
  session_trade_count: number;
  daily_loss_pct_consumed: number;
  session_pnl: number;
  session_win_rate: number;
  time_in_session_minutes: number;
  size_deviation_ratio: number;
  current_drawdown_consumed_pct: number;
  current_challenge_pnl_pct: number;
  challenge_day: number;
  reset_count: number;
  lifetime_win_rate: number;
  lifetime_trade_count: number;
}

interface Ctx {
  riskPhase: number;
  balance: number;
  startingBalance: number;
  drawdown: number;
  challengeStartedAt: string | null;
  maxDailyLoss: number;
  maxDrawdown: number;
  profitTarget: number;
  lifetimeWinRate: number;
  lifetimeWinRateEs: number;
  lifetimeWinRateNq: number;
  lifetimeWinRateGc: number;
  lifetimeWinRateCl: number;
  lifetimeAvgWin: number;
  lifetimeAvgLoss: number;
  lifetimeTradeCount: number;
  avgSize7dEs: number;
  avgSize7dNq: number;
  avgSize7dGc: number;
  avgSize7dCl: number;
  resetCount: number;
  sessionStartedAt: string | null;
  sessionPnl: number;
  sessionTradeCount: number;
  sessionClosedCount: number;
  sessionWinCount: number;
  consecutiveLosses: number;
  consecutiveWins: number;
  lastTradeResult: string | null;
  lastTradeAt: string | null;
  lastOpenSymbol: string | null;
  lastOpenSize: number;
}

const n = (v: unknown): number => {
  const x = typeof v === "number" ? v : Number(v);
  return Number.isFinite(x) ? x : 0;
};

async function loadContext(db: Db, accountId: string): Promise<Ctx | null> {
  const { rows } = await db.query(
    `SELECT a."riskPhase", a."balance", a."startingBalance", a."drawdown", a."challengeStartedAt",
            r."maxDailyLoss", r."maxDrawdown", r."profitTarget",
            s."lifetimeWinRate", s."lifetimeWinRateEs", s."lifetimeWinRateNq", s."lifetimeWinRateGc", s."lifetimeWinRateCl",
            s."lifetimeAvgWin", s."lifetimeAvgLoss", s."lifetimeTradeCount",
            s."avgSize7dEs", s."avgSize7dNq", s."avgSize7dGc", s."avgSize7dCl", s."resetCount",
            s."sessionStartedAt", s."sessionPnl", s."sessionTradeCount", s."sessionClosedCount", s."sessionWinCount",
            s."consecutiveLosses", s."consecutiveWins", s."lastTradeResult", s."lastTradeAt",
            s."lastOpenSymbol", s."lastOpenSize"
     FROM "Account" a
     LEFT JOIN "Rule" r ON r."accountId" = a."id"
     LEFT JOIN "TraderStats" s ON s."accountId" = a."id"
     WHERE a."id" = $1`,
    [accountId],
  );
  const r = rows[0];
  if (!r) return null;
  return {
    riskPhase: n(r.riskPhase) || 1,
    balance: n(r.balance),
    startingBalance: n(r.startingBalance),
    drawdown: n(r.drawdown),
    challengeStartedAt: r.challengeStartedAt ?? null,
    maxDailyLoss: n(r.maxDailyLoss),
    maxDrawdown: n(r.maxDrawdown),
    profitTarget: n(r.profitTarget),
    lifetimeWinRate: n(r.lifetimeWinRate),
    lifetimeWinRateEs: n(r.lifetimeWinRateEs),
    lifetimeWinRateNq: n(r.lifetimeWinRateNq),
    lifetimeWinRateGc: n(r.lifetimeWinRateGc),
    lifetimeWinRateCl: n(r.lifetimeWinRateCl),
    lifetimeAvgWin: n(r.lifetimeAvgWin),
    lifetimeAvgLoss: n(r.lifetimeAvgLoss),
    lifetimeTradeCount: n(r.lifetimeTradeCount),
    avgSize7dEs: n(r.avgSize7dEs),
    avgSize7dNq: n(r.avgSize7dNq),
    avgSize7dGc: n(r.avgSize7dGc),
    avgSize7dCl: n(r.avgSize7dCl),
    resetCount: n(r.resetCount),
    sessionStartedAt: r.sessionStartedAt ?? null,
    sessionPnl: n(r.sessionPnl),
    sessionTradeCount: n(r.sessionTradeCount),
    sessionClosedCount: n(r.sessionClosedCount),
    sessionWinCount: n(r.sessionWinCount),
    consecutiveLosses: n(r.consecutiveLosses),
    consecutiveWins: n(r.consecutiveWins),
    lastTradeResult: r.lastTradeResult ?? null,
    lastTradeAt: r.lastTradeAt ?? null,
    lastOpenSymbol: r.lastOpenSymbol ?? null,
    lastOpenSize: n(r.lastOpenSize),
  };
}

function avg7dForParent(ctx: Ctx, parent: string): number {
  switch (parent) {
    case "ES": return ctx.avgSize7dEs;
    case "NQ": return ctx.avgSize7dNq;
    case "GC": return ctx.avgSize7dGc;
    case "CL": return ctx.avgSize7dCl;
    default: return 0;
  }
}

function dailyLossPct(sessionPnl: number, maxDailyLoss: number): number {
  if (sessionPnl >= 0 || maxDailyLoss <= 0) return 0;
  return round2(Math.min(100, (Math.abs(sessionPnl) / maxDailyLoss) * 100));
}

function challengeDay(challengeStartedAt: string | null, now: Date): number {
  if (!challengeStartedAt) return 1;
  const start = new Date(challengeStartedAt).getTime();
  return Math.max(1, Math.floor((now.getTime() - start) / 86_400_000) + 1);
}

/** Build the 14 rule/analytics variables from a context (session fields as-is). */
function buildVariables(ctx: Ctx, now: Date): TraderVariables {
  const sessionWinRate = ctx.sessionClosedCount > 0 ? (ctx.sessionWinCount / ctx.sessionClosedCount) * 100 : 0;
  const avg = avg7dForParent(ctx, parentInstrument(ctx.lastOpenSymbol));
  const sizeDev = ctx.lastOpenSize > 0 ? (avg > 0 ? ctx.lastOpenSize / avg : 1) : 0;
  const ddPct = ctx.maxDrawdown > 0 ? Math.min(100, (ctx.drawdown / ctx.maxDrawdown) * 100) : 0;
  const chalPct = ctx.profitTarget > 0 ? ((ctx.balance - ctx.startingBalance) / ctx.profitTarget) * 100 : 0;
  return {
    consecutive_losses: ctx.consecutiveLosses,
    consecutive_wins: ctx.consecutiveWins,
    session_trade_count: ctx.sessionTradeCount,
    daily_loss_pct_consumed: dailyLossPct(ctx.sessionPnl, ctx.maxDailyLoss),
    session_pnl: round2(ctx.sessionPnl),
    session_win_rate: round2(sessionWinRate),
    time_in_session_minutes: minutesInSession(now),
    size_deviation_ratio: round2(sizeDev),
    current_drawdown_consumed_pct: round2(ddPct),
    current_challenge_pnl_pct: round2(chalPct),
    challenge_day: challengeDay(ctx.challengeStartedAt, now),
    reset_count: ctx.resetCount,
    lifetime_win_rate: round2(ctx.lifetimeWinRate),
    lifetime_trade_count: ctx.lifetimeTradeCount,
  };
}

/** Zero the session fields of a ctx if its anchor predates the current session. */
function applyPresentReset(ctx: Ctx, now: Date): Ctx {
  const anchor = sessionAnchor(now).getTime();
  const started = ctx.sessionStartedAt ? new Date(ctx.sessionStartedAt).getTime() : 0;
  if (started >= anchor) return ctx;
  return { ...ctx, sessionPnl: 0, sessionTradeCount: 0, sessionClosedCount: 0, sessionWinCount: 0, sessionStartedAt: new Date(anchor).toISOString() };
}

// --- write path (called inside the fill transaction) -------------

async function ensureRow(client: PoolClient, accountId: string): Promise<void> {
  await client.query(`INSERT INTO "TraderStats" ("accountId") VALUES ($1) ON CONFLICT ("accountId") DO NOTHING`, [accountId]);
}

/** Reset session counters if we've crossed the 9:30 ET boundary since last event. */
export async function resetSessionIfNeeded(client: PoolClient, accountId: string, now: Date): Promise<void> {
  await ensureRow(client, accountId);
  const anchor = sessionAnchor(now);
  await client.query(
    `UPDATE "TraderStats"
       SET "sessionPnl" = 0, "sessionTradeCount" = 0, "sessionClosedCount" = 0, "sessionWinCount" = 0,
           "sessionStartedAt" = $2, "updatedAt" = now()
     WHERE "accountId" = $1 AND ("sessionStartedAt" IS NULL OR "sessionStartedAt" < $2)`,
    [accountId, anchor],
  );
}

/** Compute the "at open" snapshot for a fill that opens/scales/flips a position. */
export async function buildOpenSnapshot(client: PoolClient, accountId: string, symbol: string, qty: number): Promise<OpenSnapshot> {
  const ctx = await loadContext(client, accountId);
  if (!ctx) return { phaseAtOpen: 1, scoreAtOpen: null, consecutiveLossesAtOpen: 0, dailyLossPctAtOpen: 0, sizeDeviationAtOpen: 0 };
  const avg = avg7dForParent(ctx, parentInstrument(symbol));
  const sizeDev = avg > 0 ? qty / avg : 1;
  return {
    phaseAtOpen: ctx.riskPhase || 1,
    scoreAtOpen: null,
    consecutiveLossesAtOpen: ctx.consecutiveLosses,
    dailyLossPctAtOpen: dailyLossPct(ctx.sessionPnl, ctx.maxDailyLoss),
    sizeDeviationAtOpen: round2(sizeDev),
  };
}

/** Record that a fill opened/increased exposure: bump session trade count + last-open. */
export async function markTradeOpened(client: PoolClient, accountId: string, symbol: string, qty: number, now: Date): Promise<void> {
  await resetSessionIfNeeded(client, accountId, now);
  await client.query(
    `UPDATE "TraderStats" SET "sessionTradeCount" = "sessionTradeCount" + 1,
            "lastOpenSymbol" = $2, "lastOpenSize" = $3, "updatedAt" = now() WHERE "accountId" = $1`,
    [accountId, symbol, qty],
  );
}

interface RecentTrade { realizedPnl: number; }

/** Recompute all derived stats from history, then re-assign the risk phase. */
export async function recomputeAndPhase(client: PoolClient, accountId: string, now: Date): Promise<number> {
  await resetSessionIfNeeded(client, accountId, now);
  const anchor = sessionAnchor(now);

  // Lifetime aggregates (all closed trades, all challenges).
  const life = await client.query(
    `SELECT COUNT(*)::int AS ntrades,
            COUNT(*) FILTER (WHERE "realizedPnl" > 0)::int AS wins,
            COALESCE(AVG("realizedPnl") FILTER (WHERE "realizedPnl" > 0), 0) AS avgwin,
            COALESCE(AVG("realizedPnl") FILTER (WHERE "realizedPnl" < 0), 0) AS avgloss
     FROM "ClosedPosition" WHERE "accountId" = $1`,
    [accountId],
  );
  const lifeRow = life.rows[0]!;
  const ntrades = n(lifeRow.ntrades);
  const lifeWinRate = ntrades > 0 ? (n(lifeRow.wins) / ntrades) * 100 : 0;

  // Per-instrument win rate (micros rolled into parent), ES/NQ/GC/CL.
  const perInst = await client.query(
    `SELECT (CASE WHEN "symbol" IN ('ES','MES') THEN 'ES'
                  WHEN "symbol" IN ('NQ','MNQ') THEN 'NQ'
                  WHEN "symbol" IN ('GC','MGC') THEN 'GC'
                  WHEN "symbol" IN ('CL','MCL') THEN 'CL' END) AS parent,
            COUNT(*)::int AS ntrades,
            COUNT(*) FILTER (WHERE "realizedPnl" > 0)::int AS wins
     FROM "ClosedPosition"
     WHERE "accountId" = $1 AND "symbol" = ANY($2)
     GROUP BY parent`,
    [accountId, TRACKED_SYMBOLS],
  );
  const winByInst: Record<string, number> = { ES: 0, NQ: 0, GC: 0, CL: 0 };
  for (const row of perInst.rows) {
    const c = n(row.ntrades);
    if (row.parent && c > 0) winByInst[row.parent] = (n(row.wins) / c) * 100;
  }

  // Rolling 7-day average contracts per instrument (micros rolled into parent).
  const size7d = await client.query(
    `SELECT (CASE WHEN "symbol" IN ('ES','MES') THEN 'ES'
                  WHEN "symbol" IN ('NQ','MNQ') THEN 'NQ'
                  WHEN "symbol" IN ('GC','MGC') THEN 'GC'
                  WHEN "symbol" IN ('CL','MCL') THEN 'CL' END) AS parent,
            COALESCE(AVG("quantity"), 0) AS avgsize
     FROM "ClosedPosition"
     WHERE "accountId" = $1 AND "symbol" = ANY($2) AND "closedAt" >= now() - interval '7 days'
     GROUP BY parent`,
    [accountId, TRACKED_SYMBOLS],
  );
  const avgByInst: Record<string, number> = { ES: 0, NQ: 0, GC: 0, CL: 0 };
  for (const row of size7d.rows) if (row.parent) avgByInst[row.parent] = n(row.avgsize);

  // Session aggregates (closed trades since the 9:30 ET anchor).
  const sess = await client.query(
    `SELECT COUNT(*)::int AS closed,
            COUNT(*) FILTER (WHERE "realizedPnl" > 0)::int AS wins,
            COALESCE(SUM("realizedPnl"), 0) AS pnl
     FROM "ClosedPosition" WHERE "accountId" = $1 AND "closedAt" >= $2`,
    [accountId, anchor],
  );
  const sessRow = sess.rows[0]!;

  // Consecutive streak + last-trade result from the most recent closed trades.
  const recent = await client.query<RecentTrade>(
    `SELECT "realizedPnl" FROM "ClosedPosition" WHERE "accountId" = $1 ORDER BY "closedAt" DESC, "id" DESC LIMIT 200`,
    [accountId],
  );
  // Count trailing losses / wins from the most recent trade backward. Only one of
  // the two can be non-zero; a breakeven (or the opposite result) ends the streak.
  const resultOf = (pnl: number) => (Math.abs(pnl) < 0.005 ? "breakeven" : pnl > 0 ? "win" : "loss");
  const results = recent.rows.map((row) => resultOf(n(row.realizedPnl)));
  const lastResult: string | null = results[0] ?? null;
  let consecLosses = 0;
  let consecWins = 0;
  for (const res of results) { if (res === "loss") consecLosses++; else break; }
  for (const res of results) { if (res === "win") consecWins++; else break; }

  await client.query(
    `UPDATE "TraderStats" SET
        "lifetimeWinRate" = $2, "lifetimeWinRateEs" = $3, "lifetimeWinRateNq" = $4, "lifetimeWinRateGc" = $5, "lifetimeWinRateCl" = $6,
        "lifetimeAvgWin" = $7, "lifetimeAvgLoss" = $8, "lifetimeTradeCount" = $9,
        "avgSize7dEs" = $10, "avgSize7dNq" = $11, "avgSize7dGc" = $12, "avgSize7dCl" = $13,
        "sessionPnl" = $14, "sessionClosedCount" = $15, "sessionWinCount" = $16,
        "consecutiveLosses" = $17, "consecutiveWins" = $18, "lastTradeResult" = $19,
        "lastTradeAt" = COALESCE((SELECT MAX("closedAt") FROM "ClosedPosition" WHERE "accountId" = $1), "lastTradeAt"),
        "sessionStartedAt" = $20, "updatedAt" = now()
     WHERE "accountId" = $1`,
    [
      accountId,
      round2(lifeWinRate), round2(winByInst.ES!), round2(winByInst.NQ!), round2(winByInst.GC!), round2(winByInst.CL!),
      round2(n(lifeRow.avgwin)), round2(n(lifeRow.avgloss)), ntrades,
      round2(avgByInst.ES!), round2(avgByInst.NQ!), round2(avgByInst.GC!), round2(avgByInst.CL!),
      round2(n(sessRow.pnl)), n(sessRow.closed), n(sessRow.wins),
      consecLosses, consecWins, lastResult,
      anchor,
    ],
  );

  // Re-assign the phase from the (now fresh) variables.
  const ctx = await loadContext(client, accountId);
  if (!ctx) return 1;
  const vars = buildVariables(ctx, now);
  const rules = await loadActiveRules(client);
  const phase = evaluatePhase(rules, vars as unknown as Record<string, number>);
  await client.query(`UPDATE "Account" SET "riskPhase" = $2, "updatedAt" = now() WHERE "id" = $1`, [accountId, phase]);
  return phase;
}

/** Increment the reset counter (called from adminResetAccount). */
export async function bumpResetCount(client: PoolClient, accountId: string): Promise<void> {
  await ensureRow(client, accountId);
  await client.query(`UPDATE "TraderStats" SET "resetCount" = "resetCount" + 1, "updatedAt" = now() WHERE "accountId" = $1`, [accountId]);
}

// --- read path (analytics endpoints) -----------------------------

export interface TraderState {
  riskPhase: number;
  variables: TraderVariables;
  lifetimeWinRate: number;
  lifetimeWinRateEs: number;
  lifetimeWinRateNq: number;
  lifetimeWinRateGc: number;
  lifetimeWinRateCl: number;
  lifetimeAvgWin: number;
  lifetimeAvgLoss: number;
  lifetimeTradeCount: number;
  sessionPnl: number;
  sessionTradeCount: number;
  sessionWinRate: number;
  lastTradeResult: string | null;
  minutesSinceLastTrade: number | null;
  resetCount: number;
}

/** Full per-trader state for the analytics Per-Trader view (present-reset applied). */
export async function getTraderState(accountId: string, now: Date = new Date()): Promise<TraderState | null> {
  const raw = await loadContext(getPool(), accountId);
  if (!raw) return null;
  const ctx = applyPresentReset(raw, now);
  const vars = buildVariables(ctx, now);
  const minsSince = ctx.lastTradeAt ? Math.max(0, Math.floor((now.getTime() - new Date(ctx.lastTradeAt).getTime()) / 60_000)) : null;
  return {
    riskPhase: ctx.riskPhase || 1,
    variables: vars,
    lifetimeWinRate: round2(ctx.lifetimeWinRate),
    lifetimeWinRateEs: round2(ctx.lifetimeWinRateEs),
    lifetimeWinRateNq: round2(ctx.lifetimeWinRateNq),
    lifetimeWinRateGc: round2(ctx.lifetimeWinRateGc),
    lifetimeWinRateCl: round2(ctx.lifetimeWinRateCl),
    lifetimeAvgWin: round2(ctx.lifetimeAvgWin),
    lifetimeAvgLoss: round2(ctx.lifetimeAvgLoss),
    lifetimeTradeCount: ctx.lifetimeTradeCount,
    sessionPnl: vars.session_pnl,
    sessionTradeCount: vars.session_trade_count,
    sessionWinRate: vars.session_win_rate,
    lastTradeResult: ctx.lastTradeResult,
    minutesSinceLastTrade: minsSince,
    resetCount: ctx.resetCount,
  };
}

/** Recompute EVERY account's phase (e.g. after an admin edits the ruleset). */
export async function recomputeAllPhases(): Promise<number> {
  const pool = getPool();
  const rules = await loadActiveRules(pool);
  const { rows } = await pool.query<{ id: string }>(`SELECT "id" FROM "Account"`);
  const now = new Date();
  let updated = 0;
  for (const row of rows) {
    const raw = await loadContext(pool, row.id);
    if (!raw) continue;
    const ctx = applyPresentReset(raw, now);
    const phase = evaluatePhase(rules, buildVariables(ctx, now) as unknown as Record<string, number>);
    if (phase !== raw.riskPhase) {
      await pool.query(`UPDATE "Account" SET "riskPhase" = $2, "updatedAt" = now() WHERE "id" = $1`, [row.id, phase]);
      updated++;
    }
  }
  return updated;
}
