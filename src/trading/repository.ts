import { getPool } from "../db/pool.js";
import { SYMBOLS } from "../instruments.js";

/** Default evaluation parameters for a newly registered trader. */
const STARTING_BALANCE = 50_000;
const DEFAULT_RULE = { maxDailyLoss: 2_500, maxDrawdown: 3_000, profitTarget: 6_000, maxContracts: 5 };

/**
 * Provision a default evaluation account (Account + Rule + opening deposit) for a
 * user, atomically. Idempotent: does nothing if the user already has an account.
 */
export async function createEvaluationAccount(userId: string): Promise<void> {
  const client = await getPool().connect();
  try {
    await client.query("BEGIN");
    const acc = await client.query<{ id: string }>(
      `INSERT INTO "Account" ("userId","startingBalance","balance","equity","highestEquity","status")
       VALUES ($1,$2,$2,$2,$2,'ACTIVE')
       ON CONFLICT ("userId") DO NOTHING
       RETURNING "id"`,
      [userId, STARTING_BALANCE],
    );
    const accountId = acc.rows[0]?.id;
    if (accountId) {
      await client.query(
        `INSERT INTO "Rule" ("accountId","maxDailyLoss","maxDrawdown","profitTarget","maxContracts","allowedInstruments")
         VALUES ($1,$2,$3,$4,$5,$6)`,
        [accountId, DEFAULT_RULE.maxDailyLoss, DEFAULT_RULE.maxDrawdown, DEFAULT_RULE.profitTarget, DEFAULT_RULE.maxContracts, SYMBOLS],
      );
      await client.query(
        `INSERT INTO "Transaction" ("accountId","type","amount","description")
         VALUES ($1,'DEPOSIT',$2,'Initial deposit')`,
        [accountId, STARTING_BALANCE],
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
  totalPnl: number;
  drawdown: number;
  highestEquity: number;
  rule: { profitTarget: number; maxDailyLoss: number; maxDrawdown: number; maxContracts: number };
}

/** Full account + evaluation rule for the account page. */
export async function getAccountDetail(accountId: string): Promise<ApiAccount | null> {
  const { rows } = await getPool().query(
    `SELECT a."id", a."status", a."startingBalance", a."balance", a."equity",
            a."totalPnl", a."dailyPnl", a."drawdown", a."highestEquity",
            r."profitTarget", r."maxDailyLoss", r."maxDrawdown", r."maxContracts"
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
    totalPnl: Number(r.totalPnl),
    drawdown: Number(r.drawdown),
    highestEquity: Number(r.highestEquity),
    rule: {
      profitTarget: Number(r.profitTarget ?? 0),
      maxDailyLoss: Number(r.maxDailyLoss ?? 0),
      maxDrawdown: Number(r.maxDrawdown ?? 0),
      maxContracts: Number(r.maxContracts ?? 0),
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
    `SELECT "amount","createdAt" FROM "Transaction" WHERE "accountId" = $1 ORDER BY "createdAt" ASC`,
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
     FROM "Transaction" WHERE "accountId" = $1 ORDER BY "createdAt" DESC`,
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
}

export async function getAccountSnapshot(accountId: string): Promise<AccountSnapshot | null> {
  const { rows } = await getPool().query(
    `SELECT "balance","startingBalance","dailyPnl" FROM "Account" WHERE "id" = $1`,
    [accountId],
  );
  const r = rows[0];
  if (!r) return null;
  return {
    balance: Number(r.balance),
    startingBalance: Number(r.startingBalance),
    realizedPnlToday: Number(r.dailyPnl),
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
}

export async function listOrders(accountId: string): Promise<ApiOrder[]> {
  const { rows } = await getPool().query(
    `SELECT "id","symbol","side","type","status","quantity","filledQuantity",
            "requestedPrice","fillPrice","reason","createdAt","updatedAt"
     FROM "Order" WHERE "accountId" = $1 ORDER BY "createdAt" DESC`,
    [accountId],
  );
  return rows.map((r) => ({
    id: r.id,
    symbol: r.symbol,
    side: (r.side as string).toLowerCase() as "buy" | "sell",
    type: r.type === "STOP_LIMIT" ? "stop" : ((r.type as string).toLowerCase() as "market" | "limit" | "stop"),
    status: ORDER_STATUS[r.status] ?? "open",
    quantity: Number(r.quantity),
    filledQuantity: Number(r.filledQuantity),
    price: r.requestedPrice != null ? Number(r.requestedPrice) : null,
    avgFillPrice: r.fillPrice != null ? Number(r.fillPrice) : null,
    timeInForce: "GTC",
    createdAt: new Date(r.createdAt).getTime(),
    updatedAt: new Date(r.updatedAt).getTime(),
    reason: r.reason ?? undefined,
  }));
}
