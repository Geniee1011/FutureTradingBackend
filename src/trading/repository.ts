import { getPool } from "../db/pool.js";

/* Reads trading data from Postgres and maps it to the shape the frontend
   expects (Position / Order in TradingApp/src/lib/types.ts). */

export async function getAccountIdByUserId(userId: string): Promise<string | null> {
  const { rows } = await getPool().query<{ id: string }>(`SELECT "id" FROM "Account" WHERE "userId" = $1`, [
    userId,
  ]);
  return rows[0]?.id ?? null;
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
