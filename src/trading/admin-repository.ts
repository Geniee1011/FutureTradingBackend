import { getPool } from "../db/pool.js";
import { getMultiplier } from "../instruments.js";

/* Admin/CRM reads + mutations over the real DB. Shapes mirror the frontend
   types (TradingApp/src/lib/types.ts). Fields the schema doesn't track
   (country, kyc, tier, riskScore, leverage) are derived or placeholdered. */

export type AdminAction = "active" | "suspended"; // the only transitions the admin UI issues

type TraderStatus = "active" | "suspended" | "pending" | "closed";

export interface AdminTrader {
  id: string;
  name: string;
  email: string;
  country: string;
  status: TraderStatus;
  kyc: "verified" | "pending" | "rejected" | "unsubmitted";
  tier: "bronze" | "silver" | "gold" | "platinum";
  accountsCount: number;
  equity: number;
  pnl30d: number;
  riskScore: number;
  lastActive: number;
  createdAt: number;
}

export interface AdminAccountRow {
  id: string;
  traderId: string;
  traderName: string;
  type: "live" | "demo";
  currency: string;
  balance: number;
  equity: number;
  leverage: number;
  status: TraderStatus;
  openPositions: number;
  createdAt: number;
}

export interface AdminActivity {
  id: string;
  ts: number;
  actor: string;
  action: string;
  target: string;
  severity: "info" | "warning" | "critical";
  ip?: string;
  detail?: string;
}

export interface AdminRuleRow {
  accountId: string;
  traderName: string;
  email: string;
  maxDailyLoss: number;
  maxDrawdown: number;
  profitTarget: number;
  maxContracts: number;
  allowedInstruments: string[];
}

// --- derivations for fields the schema doesn't store ---
const ms = (t: Date | string | null) => (t ? new Date(t).getTime() : 0);
const tierFor = (equity: number) => (equity >= 100_000 ? "platinum" : equity >= 75_000 ? "gold" : equity >= 55_000 ? "silver" : "bronze");
function traderStatus(userStatus: string, accountStatus: string | null): TraderStatus {
  if (userStatus === "SUSPENDED") return "suspended";
  if (accountStatus === "FAILED") return "closed";
  if (userStatus === "PENDING") return "pending";
  return "active";
}
const accountStatusToTrader = (s: string): TraderStatus =>
  s === "SUSPENDED" ? "suspended" : s === "FAILED" ? "closed" : "active"; // ACTIVE/PASSED → active

const ACTION_LABEL: Record<string, string> = {
  USER_LOGIN: "logged in",
  ORDER_PLACEMENT: "placed order",
  ORDER_FILLED: "order filled",
  ORDER_CANCELLED: "cancelled order",
  ORDER_REJECTED: "order rejected",
  POSITION_OPENED: "opened position",
  POSITION_CLOSED: "closed position",
  RULE_VIOLATION: "rule violation",
  ACCOUNT_PASSED: "passed evaluation",
  ACCOUNT_SUSPENSION: "account suspended",
};
const SEVERITY: Record<string, "info" | "warning" | "critical"> = {
  RULE_VIOLATION: "critical",
  ACCOUNT_SUSPENSION: "critical",
  ORDER_REJECTED: "warning",
};

// ----------------------------- reads -----------------------------

export async function adminListTraders(): Promise<AdminTrader[]> {
  const { rows } = await getPool().query(
    `SELECT u."id", u."name", u."email", u."status" AS "userStatus", u."createdAt",
            a."id" AS "accountId", a."equity", a."totalPnl", a."drawdown", a."status" AS "accountStatus",
            r."maxDrawdown",
            (SELECT count(*)::int FROM "Position" p WHERE p."accountId" = a."id") AS "openPositions",
            (SELECT max(al."createdAt") FROM "ActivityLog" al WHERE al."accountId" = a."id") AS "lastActive"
     FROM "User" u
     LEFT JOIN "Account" a ON a."userId" = u."id"
     LEFT JOIN "Rule" r ON r."accountId" = a."id"
     WHERE u."role" = 'TRADER'
     ORDER BY u."createdAt" DESC`,
  );
  return rows.map((r) => {
    const equity = r.equity != null ? Number(r.equity) : 0;
    const maxDd = r.maxDrawdown != null ? Number(r.maxDrawdown) : 0;
    const drawdown = r.drawdown != null ? Number(r.drawdown) : 0;
    return {
      id: r.id,
      name: r.name ?? r.email,
      email: r.email,
      country: "—", // not tracked
      status: traderStatus(r.userStatus, r.accountStatus),
      kyc: "verified", // not tracked
      tier: tierFor(equity),
      accountsCount: r.accountId ? 1 : 0,
      equity,
      pnl30d: r.totalPnl != null ? Number(r.totalPnl) : 0, // approx: total realized (no 30d window)
      riskScore: maxDd > 0 ? Math.max(0, Math.min(100, Math.round((drawdown / maxDd) * 100))) : 0,
      lastActive: ms(r.lastActive) || ms(r.createdAt),
      createdAt: ms(r.createdAt),
    };
  });
}

export async function adminListAccounts(): Promise<AdminAccountRow[]> {
  const { rows } = await getPool().query(
    `SELECT a."id", a."userId", u."name", u."email", a."balance", a."equity", a."status", a."createdAt",
            (SELECT count(*)::int FROM "Position" p WHERE p."accountId" = a."id") AS "openPositions"
     FROM "Account" a JOIN "User" u ON u."id" = a."userId"
     ORDER BY a."createdAt" DESC`,
  );
  return rows.map((r) => ({
    id: r.id,
    traderId: r.userId,
    traderName: r.name ?? r.email,
    type: "demo", // all accounts are simulated evaluation accounts
    currency: "USD",
    balance: Number(r.balance),
    equity: Number(r.equity),
    leverage: 1, // futures sim — not tracked
    status: accountStatusToTrader(r.status),
    openPositions: r.openPositions,
    createdAt: ms(r.createdAt),
  }));
}

export async function adminListActivity(limit = 200): Promise<AdminActivity[]> {
  const { rows } = await getPool().query(
    `SELECT al."id", al."type", al."message", al."ip", al."createdAt",
            u."name" AS "traderName", u."email"
     FROM "ActivityLog" al
     LEFT JOIN "Account" a ON a."id" = al."accountId"
     LEFT JOIN "User" u ON u."id" = a."userId"
     ORDER BY al."createdAt" DESC
     LIMIT $1`,
    [limit],
  );
  return rows.map((r) => ({
    id: r.id,
    ts: ms(r.createdAt),
    actor: r.traderName ?? r.email ?? "system",
    action: ACTION_LABEL[r.type] ?? (r.type as string).toLowerCase().replace(/_/g, " "),
    target: r.message ?? "",
    severity: SEVERITY[r.type] ?? "info",
    ip: r.ip ?? undefined,
    detail: r.message ?? undefined,
  }));
}

export async function adminListRules(): Promise<AdminRuleRow[]> {
  const { rows } = await getPool().query(
    `SELECT a."id" AS "accountId", u."name" AS "traderName", u."email",
            r."maxDailyLoss", r."maxDrawdown", r."profitTarget", r."maxContracts", r."allowedInstruments"
     FROM "Account" a
     JOIN "User" u ON u."id" = a."userId"
     JOIN "Rule" r ON r."accountId" = a."id"
     ORDER BY u."name" NULLS LAST, u."email"`,
  );
  return rows.map((r) => ({
    accountId: r.accountId,
    traderName: r.traderName ?? r.email,
    email: r.email,
    maxDailyLoss: Number(r.maxDailyLoss),
    maxDrawdown: Number(r.maxDrawdown),
    profitTarget: Number(r.profitTarget),
    maxContracts: Number(r.maxContracts),
    allowedInstruments: (r.allowedInstruments as string[] | null) ?? [],
  }));
}

export interface AdminViolationRow {
  id: string;
  ts: number;
  traderId: string;
  traderName: string;
  accountId: string;
  type: string;
  action: string;
  detail: string | null;
}

/** All rule violations across accounts (newest first) for the admin Violations view. */
export async function adminListViolations(limit = 200): Promise<AdminViolationRow[]> {
  const { rows } = await getPool().query(
    `SELECT v."id", v."type", v."action", v."detail", v."createdAt", v."accountId",
            u."id" AS "traderId", u."name", u."email"
     FROM "Violation" v
     JOIN "Account" a ON a."id" = v."accountId"
     JOIN "User" u ON u."id" = a."userId"
     ORDER BY v."createdAt" DESC
     LIMIT $1`,
    [limit],
  );
  return rows.map((r) => ({
    id: r.id,
    ts: ms(r.createdAt),
    traderId: r.traderId,
    traderName: r.name ?? r.email,
    accountId: r.accountId,
    type: r.type,
    action: r.action,
    detail: r.detail ?? null,
  }));
}

export interface AdminOpenPosition {
  id: string;
  traderId: string;
  traderName: string;
  accountId: string;
  symbol: string;
  side: string; // LONG | SHORT
  quantity: number;
  averagePrice: number;
  realizedPnl: number;
  unrealizedPnl: number;
  openedAt: number;
  // Protective bracket levels (the position's open OCO exit legs), or null if none set.
  // Derived fresh per query, so a later add/edit by the trader shows on the next refresh.
  stopLoss: number | null;
  takeProfit: number | null;
}

export interface AdminClosedPosition {
  id: string;
  traderId: string;
  traderName: string;
  accountId: string;
  symbol: string;
  side: string;
  quantity: number;
  entryPrice: number;
  exitPrice: number;
  realizedPnl: number;
  openedAt: number;
  closedAt: number;
}

/** Every OPEN position across all accounts (admin-wide Positions view). */
/**
 * Live open positions for the admin CRM, one row per entry trade (lot).
 *
 * `markOf` supplies the current price per symbol (the WS quote feed) so each lot's
 * UNREALIZED P&L is marked-to-market here — the stored `Position.unrealizedPnl` is
 * never written live, so without this the admin's P&L column would sit at $0. Booked
 * (realized) P&L for the still-open position is split across its lots by size.
 */
export async function adminListOpenPositions(
  markOf?: (symbol: string) => number | undefined,
): Promise<AdminOpenPosition[]> {
  // The internal CRM lists every entry trade SEPARATELY, so we read per-trade lots
  // (not the netted Position line the trader dashboard shows). Each lot keeps its own
  // entry price + open time; the protective bracket is per-symbol (shared by the lots).
  const { rows } = await getPool().query(
    `SELECT l."id", l."symbol", l."side", l."quantity", l."entryPrice", l."openedAt", l."accountId",
            u."id" AS "traderId", u."name", u."email",
            p."quantity" AS "posQty", p."realizedPnl" AS "posRealized",
            -- Per-symbol protective bracket = the open OCO exit legs (SL=STOP, TP=LIMIT).
            -- One bracket per symbol (old legs are cancelled on replace), so LIMIT 1 is exact.
            (SELECT o."requestedPrice" FROM "Order" o
              WHERE o."accountId" = l."accountId" AND o."symbol" = l."symbol"
                AND o."status" = 'PENDING' AND o."ocoGroupId" IS NOT NULL AND o."type" = 'STOP'
              ORDER BY o."updatedAt" DESC LIMIT 1) AS "stopLoss",
            (SELECT o."requestedPrice" FROM "Order" o
              WHERE o."accountId" = l."accountId" AND o."symbol" = l."symbol"
                AND o."status" = 'PENDING' AND o."ocoGroupId" IS NOT NULL AND o."type" = 'LIMIT'
              ORDER BY o."updatedAt" DESC LIMIT 1) AS "takeProfit"
     FROM "PositionLot" l
     JOIN "Account" a ON a."id" = l."accountId"
     JOIN "User" u ON u."id" = a."userId"
     LEFT JOIN "Position" p ON p."accountId" = l."accountId" AND p."symbol" = l."symbol"
     ORDER BY l."openedAt" DESC`,
  );
  return rows.map((r) => {
    const lotQty = Number(r.quantity);
    const entry = Number(r.entryPrice);
    const dir = r.side === "LONG" ? 1 : -1;
    // Mark-to-market this lot against the live quote (same formula as the trader feed:
    // (mark − entry) × qty × direction × contract multiplier). No mark yet → $0.
    const mark = markOf?.(r.symbol);
    const unrealizedPnl =
      mark != null && mark > 0 ? Math.round((mark - entry) * lotQty * dir * getMultiplier(r.symbol) * 100) / 100 : 0;
    // Booked (realized) P&L of the still-open position, split across its lots by size so
    // the per-trade rows sum to the position total (a fresh, never-reduced trade → $0).
    const posQty = r.posQty != null ? Number(r.posQty) : lotQty;
    const posRealized = r.posRealized != null ? Number(r.posRealized) : 0;
    const realizedPnl = posQty > 0 ? Math.round(posRealized * (lotQty / posQty) * 100) / 100 : 0;
    return {
      id: r.id,
      traderId: r.traderId,
      traderName: r.name ?? r.email,
      accountId: r.accountId,
      symbol: r.symbol,
      side: r.side,
      quantity: lotQty,
      averagePrice: entry,
      realizedPnl,
      unrealizedPnl,
      openedAt: ms(r.openedAt),
      stopLoss: r.stopLoss != null ? Number(r.stopLoss) : null,
      takeProfit: r.takeProfit != null ? Number(r.takeProfit) : null,
    };
  });
}

/** Every CLOSED position across all accounts, newest first (admin-wide Positions view). */
export async function adminListClosedPositions(limit = 500): Promise<AdminClosedPosition[]> {
  const { rows } = await getPool().query(
    `SELECT c."id", c."symbol", c."side", c."quantity", c."entryPrice", c."exitPrice",
            c."realizedPnl", c."openedAt", c."closedAt", c."accountId",
            u."id" AS "traderId", u."name", u."email"
     FROM "ClosedPosition" c
     JOIN "Account" a ON a."id" = c."accountId"
     JOIN "User" u ON u."id" = a."userId"
     ORDER BY c."closedAt" DESC
     LIMIT $1`,
    [limit],
  );
  return rows.map((r) => ({
    id: r.id,
    traderId: r.traderId,
    traderName: r.name ?? r.email,
    accountId: r.accountId,
    symbol: r.symbol,
    side: r.side,
    quantity: Number(r.quantity),
    entryPrice: Number(r.entryPrice),
    exitPrice: Number(r.exitPrice),
    realizedPnl: Number(r.realizedPnl),
    openedAt: ms(r.openedAt),
    closedAt: ms(r.closedAt),
  }));
}

// --- single-trader detail (admin/traders/:id) ---

export interface AdminTraderAccount {
  id: string;
  startingBalance: number;
  balance: number;
  equity: number;
  dailyPnl: number;
  totalPnl: number;
  drawdown: number;
  highestEquity: number;
  status: string;
  currency: string;
  createdAt: number;
}
export interface AdminTraderRule {
  maxDailyLoss: number;
  maxDrawdown: number;
  profitTarget: number;
  maxContracts: number;
}
export interface AdminTraderPosition {
  symbol: string;
  side: "LONG" | "SHORT";
  quantity: number;
  averagePrice: number;
  unrealizedPnl: number;
  realizedPnl: number;
}
export interface AdminTraderOrder {
  id: string;
  symbol: string;
  side: string;
  type: string;
  quantity: number;
  filledQuantity: number;
  requestedPrice: number | null;
  fillPrice: number | null;
  status: string;
  reason: string | null;
  createdAt: number;
}
export interface AdminTraderViolation {
  id: string;
  ts: number;
  type: string;
  action: string;
  detail: string | null;
}
export interface AdminTraderDetail {
  trader: AdminTrader;
  account: AdminTraderAccount | null;
  rule: AdminTraderRule | null;
  positions: AdminTraderPosition[];
  orders: AdminTraderOrder[];
  violations: AdminTraderViolation[];
  activity: AdminActivity[];
}

/** Full management view for one trader: profile, account, rule, positions, orders, violations, activity. */
export async function adminGetTraderDetail(userId: string): Promise<AdminTraderDetail | null> {
  const pool = getPool();
  const { rows: base } = await pool.query(
    `SELECT u."id", u."name", u."email", u."status" AS "userStatus", u."createdAt",
            a."id" AS "accountId", a."startingBalance", a."balance", a."equity", a."dailyPnl",
            a."totalPnl", a."drawdown", a."highestEquity", a."status" AS "accountStatus",
            a."createdAt" AS "accountCreatedAt",
            r."maxDailyLoss", r."maxDrawdown", r."profitTarget", r."maxContracts"
     FROM "User" u
     LEFT JOIN "Account" a ON a."userId" = u."id"
     LEFT JOIN "Rule" r ON r."accountId" = a."id"
     WHERE u."id" = $1 AND u."role" = 'TRADER'`,
    [userId],
  );
  const b = base[0];
  if (!b) return null;

  const accountId = (b.accountId as string | null) ?? null;
  const equity = b.equity != null ? Number(b.equity) : 0;
  const drawdown = b.drawdown != null ? Number(b.drawdown) : 0;
  const maxDd = b.maxDrawdown != null ? Number(b.maxDrawdown) : 0;

  const trader: AdminTrader = {
    id: b.id,
    name: b.name ?? b.email,
    email: b.email,
    country: "—",
    status: traderStatus(b.userStatus, b.accountStatus),
    kyc: "verified",
    tier: tierFor(equity),
    accountsCount: accountId ? 1 : 0,
    equity,
    pnl30d: b.totalPnl != null ? Number(b.totalPnl) : 0,
    riskScore: maxDd > 0 ? Math.max(0, Math.min(100, Math.round((drawdown / maxDd) * 100))) : 0,
    lastActive: ms(b.createdAt),
    createdAt: ms(b.createdAt),
  };
  const account: AdminTraderAccount | null = accountId
    ? {
        id: accountId,
        startingBalance: Number(b.startingBalance),
        balance: Number(b.balance),
        equity,
        dailyPnl: Number(b.dailyPnl),
        totalPnl: Number(b.totalPnl),
        drawdown,
        highestEquity: Number(b.highestEquity),
        status: b.accountStatus,
        currency: "USD",
        createdAt: ms(b.accountCreatedAt),
      }
    : null;
  const rule: AdminTraderRule | null =
    accountId && b.maxDailyLoss != null
      ? {
          maxDailyLoss: Number(b.maxDailyLoss),
          maxDrawdown: Number(b.maxDrawdown),
          profitTarget: Number(b.profitTarget),
          maxContracts: Number(b.maxContracts),
        }
      : null;

  if (!accountId) {
    return { trader, account, rule, positions: [], orders: [], violations: [], activity: [] };
  }

  const [pos, ord, vio, act] = await Promise.all([
    pool.query(
      `SELECT "symbol","side","quantity","averagePrice","unrealizedPnl","realizedPnl"
       FROM "Position" WHERE "accountId" = $1 ORDER BY "symbol"`,
      [accountId],
    ),
    pool.query(
      `SELECT "id","symbol","side","type","quantity","filledQuantity","requestedPrice","fillPrice","status","reason","createdAt"
       FROM "Order" WHERE "accountId" = $1 ORDER BY "createdAt" DESC LIMIT 50`,
      [accountId],
    ),
    pool.query(
      `SELECT "id","type","action","detail","createdAt" FROM "Violation" WHERE "accountId" = $1 ORDER BY "createdAt" DESC LIMIT 50`,
      [accountId],
    ),
    pool.query(
      `SELECT "id","type","message","ip","createdAt" FROM "ActivityLog" WHERE "accountId" = $1 ORDER BY "createdAt" DESC LIMIT 50`,
      [accountId],
    ),
  ]);

  trader.lastActive = act.rows[0] ? ms(act.rows[0].createdAt) : ms(b.createdAt);

  return {
    trader,
    account,
    rule,
    positions: pos.rows.map((p) => ({
      symbol: p.symbol,
      side: p.side,
      quantity: p.quantity,
      averagePrice: Number(p.averagePrice),
      unrealizedPnl: Number(p.unrealizedPnl),
      realizedPnl: Number(p.realizedPnl),
    })),
    orders: ord.rows.map((o) => ({
      id: o.id,
      symbol: o.symbol,
      side: o.side,
      type: o.type,
      quantity: o.quantity,
      filledQuantity: o.filledQuantity,
      requestedPrice: o.requestedPrice != null ? Number(o.requestedPrice) : null,
      fillPrice: o.fillPrice != null ? Number(o.fillPrice) : null,
      status: o.status,
      reason: o.reason ?? null,
      createdAt: ms(o.createdAt),
    })),
    violations: vio.rows.map((v) => ({
      id: v.id,
      ts: ms(v.createdAt),
      type: v.type,
      action: v.action,
      detail: v.detail ?? null,
    })),
    activity: act.rows.map((r) => ({
      id: r.id,
      ts: ms(r.createdAt),
      actor: trader.name,
      action: ACTION_LABEL[r.type] ?? String(r.type).toLowerCase().replace(/_/g, " "),
      target: r.message ?? "",
      severity: SEVERITY[r.type] ?? "info",
      ip: r.ip ?? undefined,
      detail: r.message ?? undefined,
    })),
  };
}

// --------------------------- mutations ---------------------------

/** Suspend/activate a trader (User) and cascade to their account. Logs on suspend. */
export async function adminSetTraderStatus(userId: string, action: AdminAction): Promise<boolean> {
  const client = await getPool().connect();
  try {
    await client.query("BEGIN");
    const u = await client.query(`UPDATE "User" SET "status" = $2, "updatedAt" = now() WHERE "id" = $1 AND "role" = 'TRADER' RETURNING "id"`, [
      userId,
      action === "suspended" ? "SUSPENDED" : "ACTIVE",
    ]);
    if (u.rowCount === 0) {
      await client.query("ROLLBACK");
      return false;
    }
    const acc = await client.query<{ id: string }>(`SELECT "id" FROM "Account" WHERE "userId" = $1`, [userId]);
    const accountId = acc.rows[0]?.id;
    if (accountId) {
      if (action === "suspended") {
        await client.query(`UPDATE "Account" SET "status" = 'SUSPENDED', "updatedAt" = now() WHERE "id" = $1`, [accountId]);
        await logActivity(client, accountId, "ACCOUNT_SUSPENSION", "Trader suspended by admin");
      } else {
        // Reactivate only a suspended account — don't resurrect a FAILED/PASSED one.
        await client.query(`UPDATE "Account" SET "status" = 'ACTIVE', "updatedAt" = now() WHERE "id" = $1 AND "status" = 'SUSPENDED'`, [accountId]);
      }
    }
    await client.query("COMMIT");
    return true;
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

/** Freeze/unfreeze a single account. Logs on suspend. */
export async function adminSetAccountStatus(accountId: string, action: AdminAction): Promise<boolean> {
  const client = await getPool().connect();
  try {
    await client.query("BEGIN");
    let res;
    if (action === "suspended") {
      res = await client.query(`UPDATE "Account" SET "status" = 'SUSPENDED', "updatedAt" = now() WHERE "id" = $1 RETURNING "id"`, [accountId]);
      if (res.rowCount) await logActivity(client, accountId, "ACCOUNT_SUSPENSION", "Account frozen by admin");
    } else {
      res = await client.query(`UPDATE "Account" SET "status" = 'ACTIVE', "updatedAt" = now() WHERE "id" = $1 AND "status" = 'SUSPENDED' RETURNING "id"`, [accountId]);
    }
    await client.query("COMMIT");
    return (res.rowCount ?? 0) > 0;
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

/** Update an account's evaluation limits + allowed instruments (picked up live by the risk engine). */
export async function adminUpdateRule(
  accountId: string,
  fields: { maxDailyLoss?: number; maxDrawdown?: number; profitTarget?: number; maxContracts?: number; allowedInstruments?: string[] },
): Promise<boolean> {
  const cols: string[] = [];
  const vals: unknown[] = [accountId];
  for (const k of ["maxDailyLoss", "maxDrawdown", "profitTarget", "maxContracts"] as const) {
    const v = fields[k];
    if (v == null || !Number.isFinite(Number(v)) || Number(v) < 0) continue;
    cols.push(`"${k}" = $${vals.length + 1}`);
    vals.push(Number(v));
  }
  if (Array.isArray(fields.allowedInstruments)) {
    const list = fields.allowedInstruments.filter((s) => typeof s === "string");
    cols.push(`"allowedInstruments" = $${vals.length + 1}`);
    vals.push(list);
  }
  if (cols.length === 0) return false;
  const res = await getPool().query(`UPDATE "Rule" SET ${cols.join(", ")}, "updatedAt" = now() WHERE "accountId" = $1`, vals);
  return (res.rowCount ?? 0) > 0;
}

// ----------------------------- rule templates -----------------------------

export interface AdminRuleTemplate {
  id: string;
  label: string;
  phase: string;
  accountSize: number;
  sortOrder: number;
  maxDailyLoss: number;
  maxDrawdown: number;
  profitTarget: number;
  maxContracts: number;
  minTradingDays: number;
  maxDailyProfitPct: number;
  maxRiskPerTrade: number;
  maxPositionUnits: number;
  stopLossRequired: boolean;
  minHoldTimeSecs: number;
  overnightHoldsProhibited: boolean;
  weekendHoldsProhibited: boolean;
  allowedInstruments: string[];
  updatedAt: number;
}

/** All 9 global account-tier rule templates, ordered by sort position. */
export async function adminListRuleTemplates(): Promise<AdminRuleTemplate[]> {
  const { rows } = await getPool().query(
    `SELECT "id","label","phase","accountSize","sortOrder","maxDailyLoss","maxDrawdown",
            "profitTarget","maxContracts","minTradingDays","maxDailyProfitPct",
            "maxRiskPerTrade","maxPositionUnits","stopLossRequired","minHoldTimeSecs",
            "overnightHoldsProhibited","weekendHoldsProhibited","allowedInstruments","updatedAt"
     FROM "RuleTemplate" ORDER BY "sortOrder"`,
  );
  return rows.map((r) => ({
    id: r.id,
    label: r.label,
    phase: r.phase,
    accountSize: Number(r.accountSize),
    sortOrder: r.sortOrder,
    maxDailyLoss: Number(r.maxDailyLoss),
    maxDrawdown: Number(r.maxDrawdown),
    profitTarget: Number(r.profitTarget),
    maxContracts: Number(r.maxContracts),
    minTradingDays: Number(r.minTradingDays),
    maxDailyProfitPct: Number(r.maxDailyProfitPct),
    maxRiskPerTrade: Number(r.maxRiskPerTrade),
    maxPositionUnits: Number(r.maxPositionUnits),
    stopLossRequired: Boolean(r.stopLossRequired),
    minHoldTimeSecs: Number(r.minHoldTimeSecs),
    overnightHoldsProhibited: Boolean(r.overnightHoldsProhibited),
    weekendHoldsProhibited: Boolean(r.weekendHoldsProhibited),
    allowedInstruments: (r.allowedInstruments as string[] | null) ?? [],
    updatedAt: new Date(r.updatedAt).getTime(),
  }));
}

const BOOL_FIELDS = ["stopLossRequired", "overnightHoldsProhibited", "weekendHoldsProhibited"] as const;
const NUM_FIELDS  = ["maxDailyLoss", "maxDrawdown", "profitTarget", "maxContracts",
                     "minTradingDays", "maxDailyProfitPct", "maxRiskPerTrade", "maxPositionUnits",
                     "minHoldTimeSecs"] as const;
type TemplateFields = Partial<
  Record<typeof NUM_FIELDS[number], number> &
  Record<typeof BOOL_FIELDS[number], boolean> &
  { allowedInstruments: string[] }
>;

/** Update a template and cascade the same values to every linked account's Rule row. */
export async function adminUpdateRuleTemplate(id: string, fields: TemplateFields): Promise<boolean> {
  const cols: string[] = [];
  const vals: unknown[] = [id];

  for (const k of NUM_FIELDS) {
    const v = fields[k];
    if (v == null || !Number.isFinite(Number(v)) || Number(v) < 0) continue;
    cols.push(`"${k}" = $${vals.length + 1}`);
    vals.push(Number(v));
  }
  for (const k of BOOL_FIELDS) {
    if (fields[k] == null) continue;
    cols.push(`"${k}" = $${vals.length + 1}`);
    vals.push(Boolean(fields[k]));
  }
  if (Array.isArray(fields.allowedInstruments)) {
    const list = fields.allowedInstruments.filter((s) => typeof s === "string");
    cols.push(`"allowedInstruments" = $${vals.length + 1}`);
    vals.push(list);
  }
  if (cols.length === 0) return false;

  const res = await getPool().query(
    `UPDATE "RuleTemplate" SET ${cols.join(", ")}, "updatedAt" = now() WHERE "id" = $1`,
    vals,
  );
  if ((res.rowCount ?? 0) === 0) return false;

  // Cascade to every per-account Rule that belongs to this template.
  await getPool().query(
    `UPDATE "Rule" SET ${cols.join(", ")}, "updatedAt" = now()
     WHERE "accountId" IN (SELECT "id" FROM "Account" WHERE "ruleTemplateId" = $1)`,
    vals,
  );
  return true;
}

/** Reset an evaluation account to its day-1 state: wipe positions/orders/violations,
 *  reset the ledger to a single opening deposit, and restore balance/status. */
export async function adminResetAccount(accountId: string): Promise<boolean> {
  const client = await getPool().connect();
  try {
    await client.query("BEGIN");
    const acc = await client.query<{ startingBalance: string }>(
      `SELECT "startingBalance" FROM "Account" WHERE "id" = $1 FOR UPDATE`,
      [accountId],
    );
    const sb = acc.rows[0]?.startingBalance;
    if (sb == null) {
      await client.query("ROLLBACK");
      return false;
    }
    // Clear LIVE state so the new challenge starts flat: drop open positions and cancel
    // any resting orders (so they can't trigger in the new challenge). HISTORY — filled
    // orders, transactions, violations, closed positions, activity — is KEPT so admins can
    // still see every challenge. The trader's own views are scoped by challengeStartedAt.
    await client.query(`DELETE FROM "Position" WHERE "accountId" = $1`, [accountId]);
    await client.query(`DELETE FROM "PositionLot" WHERE "accountId" = $1`, [accountId]); // per-trade lots are live state too
    await client.query(
      `UPDATE "Order" SET "status" = 'CANCELLED', "reason" = 'Challenge reset', "updatedAt" = now()
       WHERE "accountId" = $1 AND "status" = 'PENDING'`,
      [accountId],
    );
    await client.query(`INSERT INTO "Transaction" ("accountId","type","amount","description") VALUES ($1,'DEPOSIT',$2,'Account reset — opening balance')`, [accountId, sb]);
    await client.query(
      `UPDATE "Account" SET "balance" = "startingBalance", "equity" = "startingBalance",
              "dailyPnl" = 0, "totalPnl" = 0, "drawdown" = 0, "highestEquity" = "startingBalance",
              "dayStartEquity" = "startingBalance", "dayStartAt" = CURRENT_DATE,
              "challengeStartedAt" = now(), "status" = 'ACTIVE', "updatedAt" = now() WHERE "id" = $1`,
      [accountId],
    );
    await client.query("COMMIT");
    return true;
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

/** Admin manual balance credit/debit. Records a ledger entry and bumps cash/equity. */
export async function adminAdjustBalance(accountId: string, amount: number): Promise<boolean> {
  const client = await getPool().connect();
  try {
    await client.query("BEGIN");
    const res = await client.query(
      `UPDATE "Account" SET "balance" = "balance" + $2, "equity" = "equity" + $2, "updatedAt" = now() WHERE "id" = $1 RETURNING "id"`,
      [accountId, amount],
    );
    if (res.rowCount === 0) {
      await client.query("ROLLBACK");
      return false;
    }
    const type = amount >= 0 ? "DEPOSIT" : "WITHDRAWAL";
    await client.query(`INSERT INTO "Transaction" ("accountId","type","amount","description") VALUES ($1,$2,$3,'Balance adjustment by admin')`, [accountId, type, amount]);
    await client.query("COMMIT");
    return true;
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

/** Generic activity insert (also reused for USER_LOGIN). */
export async function logActivity(
  client: { query: (sql: string, params: unknown[]) => Promise<unknown> },
  accountId: string,
  type: string,
  message: string,
  ip?: string,
): Promise<void> {
  await client.query(`INSERT INTO "ActivityLog" ("accountId","type","message","ip") VALUES ($1,$2,$3,$4)`, [accountId, type, message, ip ?? null]);
}
