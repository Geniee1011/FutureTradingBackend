import { getPool } from "../db/pool.js";

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
            r."maxDailyLoss", r."maxDrawdown", r."profitTarget", r."maxContracts"
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
  }));
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

/** Update an account's evaluation limits (picked up live by the risk engine). */
export async function adminUpdateRule(
  accountId: string,
  fields: { maxDailyLoss?: number; maxDrawdown?: number; profitTarget?: number; maxContracts?: number },
): Promise<boolean> {
  const cols: string[] = [];
  const vals: unknown[] = [accountId];
  for (const [k, v] of Object.entries(fields)) {
    if (v == null || !Number.isFinite(Number(v)) || Number(v) < 0) continue;
    cols.push(`"${k}" = $${vals.length + 1}`);
    vals.push(Number(v));
  }
  if (cols.length === 0) return false;
  const res = await getPool().query(`UPDATE "Rule" SET ${cols.join(", ")}, "updatedAt" = now() WHERE "accountId" = $1`, vals);
  return (res.rowCount ?? 0) > 0;
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
