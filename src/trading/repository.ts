import { getPool } from "../db/pool.js";
import { SYMBOLS } from "../instruments.js";

/** Default evaluation parameters for a newly registered trader. */
const STARTING_BALANCE = 50_000;
const DEFAULT_RULE = { maxDailyLoss: 2_500, maxDrawdown: 3_000, profitTarget: 6_000, maxContracts: 5 };
// Starting tier for a self-registered trader: the $50,000 Challenge Phase 1. Linking the
// account to this template (not just an ad-hoc rule) is what makes it show up as a proper
// "$50,000" tier in the admin "Account size / type" control instead of "Select a tier…".
const DEFAULT_TIER_ID = "c1_50k";

export interface ViolationRecord {
  id: string;
  ts: number;
  type: string;
  action: string;
  detail: string | null;
}

/** A trader's own rule violations (newest first) for the violation-status view. */
export async function listViolations(accountId: string): Promise<ViolationRecord[]> {
  const { rows } = await getPool().query(
    `SELECT "id","type","action","detail","createdAt" FROM "Violation"
     WHERE "accountId" = $1 AND "createdAt" >= (SELECT "challengeStartedAt" FROM "Account" WHERE "id" = $1)
     ORDER BY "createdAt" DESC LIMIT 100`,
    [accountId],
  );
  return rows.map((r) => ({
    id: r.id,
    ts: new Date(r.createdAt).getTime(),
    type: r.type,
    action: r.action,
    detail: r.detail ?? null,
  }));
}

/**
 * Provision a default evaluation account (Account + Rule + opening deposit) for a
 * user, atomically. Idempotent: does nothing if the user already has an account.
 */
export async function createEvaluationAccount(userId: string): Promise<void> {
  const client = await getPool().connect();
  try {
    await client.query("BEGIN");

    // Pull the starting tier so the account is linked to a real template (size +
    // full ruleset). Falls back to the ad-hoc DEFAULT_RULE if the template is absent.
    const tpl = (
      await client.query(
        `SELECT "phase","accountSize","maxDailyLoss","maxDrawdown","profitTarget","maxContracts",
                "minTradingDays","maxDailyProfitPct","maxRiskPerTrade","maxPositionUnits",
                "stopLossRequired","minHoldTimeSecs","overnightHoldsProhibited","weekendHoldsProhibited",
                "drawdownType","allowedInstruments"
         FROM "RuleTemplate" WHERE "id" = $1`,
        [DEFAULT_TIER_ID],
      )
    ).rows[0];

    const size = tpl ? Number(tpl.accountSize) : STARTING_BALANCE;
    const phase = tpl && tpl.phase !== "Challenge Phase 1" ? 2 : 1;

    const acc = await client.query<{ id: string }>(
      `INSERT INTO "Account" (
         "userId","ruleTemplateId","startingBalance","balance","equity","highestEquity",
         "dayStartEquity","dayStartAt","peakIntradayEquity","eodPeakEquity","challengePhase",
         "challengeStartedAt","status")
       VALUES ($1,$2,$3,$3,$3,$3,$3,CURRENT_DATE,$3,$3,$4,now(),'ACTIVE')
       ON CONFLICT ("userId") DO NOTHING
       RETURNING "id"`,
      [userId, tpl ? DEFAULT_TIER_ID : null, size, phase],
    );
    const accountId = acc.rows[0]?.id;
    if (accountId) {
      if (tpl) {
        // Copy the template's full ruleset onto the account (mirrors adminAssignTier).
        await client.query(
          `INSERT INTO "Rule" (
             "accountId","maxDailyLoss","maxDrawdown","profitTarget","maxContracts",
             "minTradingDays","maxDailyProfitPct","maxRiskPerTrade","maxPositionUnits",
             "stopLossRequired","minHoldTimeSecs","overnightHoldsProhibited","weekendHoldsProhibited",
             "drawdownType","allowedInstruments")
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
          [accountId, Number(tpl.maxDailyLoss), Number(tpl.maxDrawdown), Number(tpl.profitTarget), Number(tpl.maxContracts),
           Number(tpl.minTradingDays), Number(tpl.maxDailyProfitPct), Number(tpl.maxRiskPerTrade), Number(tpl.maxPositionUnits),
           Boolean(tpl.stopLossRequired), Number(tpl.minHoldTimeSecs), Boolean(tpl.overnightHoldsProhibited),
           Boolean(tpl.weekendHoldsProhibited), tpl.drawdownType, (tpl.allowedInstruments as string[] | null) ?? SYMBOLS],
        );
      } else {
        await client.query(
          `INSERT INTO "Rule" ("accountId","maxDailyLoss","maxDrawdown","profitTarget","maxContracts","allowedInstruments")
           VALUES ($1,$2,$3,$4,$5,$6)`,
          [accountId, DEFAULT_RULE.maxDailyLoss, DEFAULT_RULE.maxDrawdown, DEFAULT_RULE.profitTarget, DEFAULT_RULE.maxContracts, SYMBOLS],
        );
      }
      await client.query(
        `INSERT INTO "Transaction" ("accountId","type","amount","description")
         VALUES ($1,'DEPOSIT',$2,'Initial deposit')`,
        [accountId, size],
      );
    }
    await client.query("COMMIT");
  } catch (err) {
    await client.query("ROLLBACK");
    throw err;
  } finally {
    client.release();
  }
}

/* Reads trading data from Postgres and maps it to the shape the frontend
   expects (Position / Order in TradingApp/src/lib/types.ts). */

export async function getAccountIdByUserId(userId: string): Promise<string | null> {
  const { rows } = await getPool().query<{ id: string }>(`SELECT "id" FROM "Account" WHERE "userId" = $1`, [
    userId,
  ]);
  return rows[0]?.id ?? null;
}

export interface ApiAccount {
  accountId: string;
  currency: string;
  status: string;
  startingBalance: number;
  balance: number;
  equity: number;
  unrealizedPnl: number;
  realizedPnlToday: number;
  dailyPnl: number; // equity-based day P&L (vs day-start equity) — matches the enforced daily-loss limit
  totalPnl: number;
  drawdown: number;
  highestEquity: number;
  rule: {
    profitTarget: number; maxDailyLoss: number; maxDrawdown: number; maxContracts: number;
    maxRiskPerTrade: number; maxPositionUnits: number; stopLossRequired: boolean; minHoldTimeSecs: number;
  };
}

/** Full account + evaluation rule for the account page. */
export async function getAccountDetail(accountId: string): Promise<ApiAccount | null> {
  const { rows } = await getPool().query(
    `SELECT a."id", a."status", a."startingBalance", a."balance", a."equity",
            a."totalPnl", a."dailyPnl", a."drawdown", a."highestEquity", a."dayStartEquity",
            r."profitTarget", r."maxDailyLoss", r."maxDrawdown", r."maxContracts",
            r."maxRiskPerTrade", r."maxPositionUnits", r."stopLossRequired", r."minHoldTimeSecs"
     FROM "Account" a
     LEFT JOIN "Rule" r ON r."accountId" = a."id"
     WHERE a."id" = $1`,
    [accountId],
  );
  const r = rows[0];
  if (!r) return null;
  return {
    accountId: r.id,
    currency: "USD",
    status: r.status,
    startingBalance: Number(r.startingBalance),
    balance: Number(r.balance),
    equity: Number(r.equity),
    unrealizedPnl: 0, // overlaid live by the WS account-updates channel
    realizedPnlToday: Number(r.dailyPnl),
    // equity-based day P&L vs the day's starting equity — the figure the daily-loss limit uses.
    dailyPnl: Number(r.equity) - (r.dayStartEquity != null ? Number(r.dayStartEquity) : Number(r.startingBalance)),
    totalPnl: Number(r.totalPnl),
    drawdown: Number(r.drawdown),
    highestEquity: Number(r.highestEquity),
    rule: {
      profitTarget: Number(r.profitTarget ?? 0),
      maxDailyLoss: Number(r.maxDailyLoss ?? 0),
      maxDrawdown: Number(r.maxDrawdown ?? 0),
      maxContracts: Number(r.maxContracts ?? 0),
      maxRiskPerTrade: Number(r.maxRiskPerTrade ?? 0),
      maxPositionUnits: Number(r.maxPositionUnits ?? 0),
      stopLossRequired: Boolean(r.stopLossRequired ?? false),
      minHoldTimeSecs: Number(r.minHoldTimeSecs ?? 0),
    },
  };
}

export interface ApiTransaction {
  id: string;
  ts: number;
  type: string;
  amount: number;
  description: string;
}

/** Realized balance curve: cumulative transaction amounts over time (epoch seconds). */
export async function getEquityCurve(accountId: string): Promise<{ time: number; value: number }[]> {
  const { rows } = await getPool().query(
    `SELECT "amount","createdAt" FROM "Transaction"
     WHERE "accountId" = $1 AND "createdAt" >= (SELECT "challengeStartedAt" FROM "Account" WHERE "id" = $1)
     ORDER BY "createdAt" ASC`,
    [accountId],
  );
  // Map keyed by second so multiple txns in the same second collapse (chart needs unique times).
  const points = new Map<number, number>();
  let running = 0;
  for (const r of rows) {
    running += Number(r.amount);
    points.set(Math.floor(new Date(r.createdAt).getTime() / 1000), Math.round(running * 100) / 100);
  }
  const out = [...points.entries()].sort((a, b) => a[0] - b[0]).map(([time, value]) => ({ time, value }));
  // Extend the line to "now" at the latest balance.
  if (out.length) {
    const now = Math.floor(Date.now() / 1000);
    const last = out[out.length - 1]!;
    if (last.time < now) out.push({ time: now, value: last.value });
  }
  return out;
}

export async function listTransactions(accountId: string): Promise<ApiTransaction[]> {
  const { rows } = await getPool().query(
    `SELECT "id","type","amount","description","createdAt"
     FROM "Transaction"
     WHERE "accountId" = $1 AND "createdAt" >= (SELECT "challengeStartedAt" FROM "Account" WHERE "id" = $1)
     ORDER BY "createdAt" DESC`,
    [accountId],
  );
  return rows.map((r) => ({
    id: r.id,
    ts: new Date(r.createdAt).getTime(),
    type: (r.type as string).toLowerCase(),
    amount: Number(r.amount),
    description: r.description ?? "",
  }));
}

export interface AccountSnapshot {
  balance: number;
  startingBalance: number;
  realizedPnlToday: number;
  status: string;
  statusReason: string | null; // why a non-ACTIVE account is in that state (the breach detail)
  dayStartEquity: number; // equity at the start of the current trading day
  highestEquity: number; // persisted all-time high-water mark (basis for trailing drawdown)
  pendingReview: boolean; // reached profit target → awaiting admin Approve/Disapprove
  tradingPaused: boolean; // hit the daily-loss limit TODAY → positions closed, orders blocked until tomorrow
  tradingPausedReason: string | null; // the daily-limit breach detail
}

export async function getAccountSnapshot(accountId: string): Promise<AccountSnapshot | null> {
  const { rows } = await getPool().query(
    `SELECT "balance","startingBalance","dailyPnl","status","dayStartEquity","highestEquity","pendingReviewAt",
            ("tradingPausedAt" = CURRENT_DATE) AS "tradingPausedToday"
     FROM "Account" WHERE "id" = $1`,
    [accountId],
  );
  const r = rows[0];
  if (!r) return null;
  // For a non-ACTIVE account, surface the specific breach (e.g. "Drawdown $3,034
  // reached limit $3,000") so the UI can tell the trader exactly what happened.
  let statusReason: string | null = null;
  if (r.status !== "ACTIVE") {
    const v = await getPool().query(
      `SELECT "detail" FROM "Violation" WHERE "accountId" = $1 ORDER BY "createdAt" DESC LIMIT 1`,
      [accountId],
    );
    statusReason = (v.rows[0]?.detail as string | undefined) ?? null;
  }
  // A still-ACTIVE account can be paused for the day after hitting its daily-loss limit —
  // positions were liquidated and new orders are blocked until tomorrow. Surface the reason.
  const tradingPaused = r.status === "ACTIVE" && r.tradingPausedToday === true;
  let tradingPausedReason: string | null = null;
  if (tradingPaused) {
    const v = await getPool().query(
      `SELECT "detail" FROM "Violation" WHERE "accountId" = $1 AND "type" = 'DAILY_LOSS_EXCEEDED' ORDER BY "createdAt" DESC LIMIT 1`,
      [accountId],
    );
    tradingPausedReason = (v.rows[0]?.detail as string | undefined) ?? null;
  }
  return {
    balance: Number(r.balance),
    startingBalance: Number(r.startingBalance),
    realizedPnlToday: Number(r.dailyPnl),
    status: r.status,
    statusReason,
    dayStartEquity: r.dayStartEquity != null ? Number(r.dayStartEquity) : Number(r.startingBalance),
    highestEquity: r.highestEquity != null ? Number(r.highestEquity) : Number(r.startingBalance),
    pendingReview: r.pendingReviewAt != null,
    tradingPaused,
    tradingPausedReason,
  };
}

export interface ApiPosition {
  symbol: string;
  side: "buy" | "sell";
  quantity: number;
  avgPrice: number;
  markPrice: number; // placeholder; frontend overlays the live quote
  unrealizedPnl: number;
  realizedPnl: number;
}

export async function listPositions(accountId: string): Promise<ApiPosition[]> {
  const { rows } = await getPool().query(
    `SELECT "symbol","side","quantity","averagePrice","unrealizedPnl","realizedPnl"
     FROM "Position" WHERE "accountId" = $1 ORDER BY "symbol"`,
    [accountId],
  );
  return rows.map((r) => ({
    symbol: r.symbol,
    side: r.side === "LONG" ? "buy" : "sell",
    quantity: Number(r.quantity),
    avgPrice: Number(r.averagePrice),
    markPrice: Number(r.averagePrice),
    unrealizedPnl: Number(r.unrealizedPnl),
    realizedPnl: Number(r.realizedPnl),
  }));
}

const ORDER_STATUS: Record<string, string> = {
  PENDING: "open",
  FILLED: "filled",
  PARTIALLY_FILLED: "partial",
  CANCELLED: "cancelled",
  REJECTED: "rejected",
};

export interface ApiOrder {
  id: string;
  symbol: string;
  side: "buy" | "sell";
  type: "market" | "limit" | "stop";
  status: string;
  quantity: number;
  filledQuantity: number;
  price: number | null;
  avgFillPrice: number | null;
  timeInForce: string;
  createdAt: number;
  updatedAt: number;
  reason?: string;
  bracketRole?: "SL" | "TP"; // set when this order is a bracket exit leg (stop=SL, limit=TP)
  slPrice?: number | null; // bracket stop-loss attached to a working entry (until it fills)
  tpPrice?: number | null; // bracket take-profit attached to a working entry (until it fills)
}

export async function listOrders(accountId: string): Promise<ApiOrder[]> {
  const { rows } = await getPool().query(
    `SELECT "id","symbol","side","type","status","quantity","filledQuantity",
            "requestedPrice","fillPrice","reason","ocoGroupId","slPrice","tpPrice","createdAt","updatedAt"
     FROM "Order"
     WHERE "accountId" = $1 AND "createdAt" >= (SELECT "challengeStartedAt" FROM "Account" WHERE "id" = $1)
     ORDER BY "createdAt" DESC`,
    [accountId],
  );
  return rows.map((r) => {
    const type = r.type === "STOP_LIMIT" ? "stop" : ((r.type as string).toLowerCase() as "market" | "limit" | "stop");
    // A bracket exit leg carries an ocoGroupId; the stop is the SL, the limit the TP.
    const bracketRole = r.ocoGroupId ? (type === "stop" ? "SL" : type === "limit" ? "TP" : undefined) : undefined;
    return {
      id: r.id,
      symbol: r.symbol,
      side: (r.side as string).toLowerCase() as "buy" | "sell",
      type,
      status: ORDER_STATUS[r.status] ?? "open",
      quantity: Number(r.quantity),
      filledQuantity: Number(r.filledQuantity),
      price: r.requestedPrice != null ? Number(r.requestedPrice) : null,
      avgFillPrice: r.fillPrice != null ? Number(r.fillPrice) : null,
      timeInForce: "GTC",
      createdAt: new Date(r.createdAt).getTime(),
      updatedAt: new Date(r.updatedAt).getTime(),
      reason: r.reason ?? undefined,
      bracketRole,
      slPrice: r.slPrice != null ? Number(r.slPrice) : null,
      tpPrice: r.tpPrice != null ? Number(r.tpPrice) : null,
    } satisfies ApiOrder;
  });
}
