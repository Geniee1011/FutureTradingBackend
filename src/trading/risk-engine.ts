import { getPool } from "../db/pool.js";
import type { OrderEngine } from "./order-engine.js";
import type { AccountStream } from "../realtime/account-stream.js";

/* ------------------------------------------------------------------ *
 * Risk / evaluation engine — enforces the prop-firm Rule limits.
 *
 * Driven by AccountStream's 1s tick with each account's LIVE equity
 * (cash balance + unrealized P&L), so breaches fire on unrealized moves,
 * not only on realized fills:
 *
 *   maxDailyLoss  — equity dropped >= limit from the day's starting equity
 *   maxDrawdown   — equity dropped >= limit from the trailing high-water mark
 *   profitTarget  — equity rose   >= target above the starting balance
 *
 * Breach → liquidate open positions, mark FAILED, record a Violation + logs.
 * Target → mark PASSED, log. Both are terminal: a non-ACTIVE account is no
 * longer evaluated and the order engine rejects its new orders.
 * ------------------------------------------------------------------ */

type ViolationType = "DAILY_LOSS_EXCEEDED" | "MAX_DRAWDOWN_BREACHED";

interface EvalRow {
  status: string;
  startingBalance: string;
  highestEquity: string;
  dayStartEquity: string | null;
  dayStartAt: Date | string | null;
  maxDailyLoss: string | null;
  maxDrawdown: string | null;
  profitTarget: string | null;
}

const round2 = (v: number) => Math.round(v * 100) / 100;
const dateStr = (d: Date | string) => new Date(d).toISOString().slice(0, 10);

export class RiskEngine {
  private processing = new Set<string>(); // accounts mid terminal-transition
  private last = new Map<string, { equity: number; day: string }>(); // skip idle re-evaluation

  constructor(
    private readonly orderEngine: OrderEngine,
    private readonly accountStream: AccountStream,
  ) {}

  /** Evaluate one account against its rule using live equity. Cheap + idempotent. */
  async evaluate(accountId: string, equity: number): Promise<void> {
    if (this.processing.has(accountId)) return;
    const today = new Date().toISOString().slice(0, 10);
    const prev = this.last.get(accountId);
    // Nothing material moved since the last tick → skip the DB round-trip entirely.
    if (prev && prev.day === today && Math.abs(equity - prev.equity) < 0.01) return;
    this.last.set(accountId, { equity, day: today });

    const { rows } = await getPool().query<EvalRow>(
      `SELECT a."status", a."startingBalance", a."highestEquity", a."dayStartEquity", a."dayStartAt",
              r."maxDailyLoss", r."maxDrawdown", r."profitTarget"
       FROM "Account" a LEFT JOIN "Rule" r ON r."accountId" = a."id"
       WHERE a."id" = $1`,
      [accountId],
    );
    const a = rows[0];
    if (!a || a.status !== "ACTIVE") return; // terminal accounts aren't evaluated

    const startingBalance = Number(a.startingBalance);
    // Roll (or initialize) the daily anchor at the date boundary.
    const sameDay = a.dayStartAt != null && dateStr(a.dayStartAt) === today;
    const rolled = !sameDay;
    const dayStartEquity = sameDay && a.dayStartEquity != null ? Number(a.dayStartEquity) : equity;

    const peak = Math.max(Number(a.highestEquity), equity); // trailing high-water mark
    const drawdown = round2(Math.max(0, peak - equity));
    const dailyLoss = round2(Math.max(0, dayStartEquity - equity));
    const profit = round2(equity - startingBalance);

    // Keep the live account fields fresh (also fixes equity/drawdown never being written).
    // On a new day, reset the realized daily figure so "today" doesn't accumulate forever.
    await getPool().query(
      `UPDATE "Account" SET "equity" = $2, "drawdown" = $3, "highestEquity" = $4,
              "dayStartEquity" = $5, "dayStartAt" = $6::date,
              "dailyPnl" = CASE WHEN $7 THEN 0 ELSE "dailyPnl" END, "updatedAt" = now()
       WHERE "id" = $1`,
      [accountId, round2(equity), drawdown, round2(peak), round2(dayStartEquity), today, rolled],
    );

    const maxDailyLoss = a.maxDailyLoss != null ? Number(a.maxDailyLoss) : 0;
    const maxDrawdown = a.maxDrawdown != null ? Number(a.maxDrawdown) : 0;
    const profitTarget = a.profitTarget != null ? Number(a.profitTarget) : 0;

    // Breach has priority over target.
    if (maxDailyLoss > 0 && dailyLoss >= maxDailyLoss) {
      await this.terminate(accountId, "FAILED", "DAILY_LOSS_EXCEEDED", `Daily loss $${dailyLoss} reached limit $${maxDailyLoss}`);
    } else if (maxDrawdown > 0 && drawdown >= maxDrawdown) {
      await this.terminate(accountId, "FAILED", "MAX_DRAWDOWN_BREACHED", `Drawdown $${drawdown} reached limit $${maxDrawdown}`);
    } else if (profitTarget > 0 && profit >= profitTarget) {
      await this.terminate(accountId, "PASSED", null, `Profit $${profit} reached target $${profitTarget}`);
    }
  }

  /** Apply a terminal outcome: liquidate (on fail), set status, record violation/logs, broadcast. */
  private async terminate(
    accountId: string,
    outcome: "FAILED" | "PASSED",
    violation: ViolationType | null,
    detail: string,
  ): Promise<void> {
    if (this.processing.has(accountId)) return;
    this.processing.add(accountId);
    try {
      // Flatten positions BEFORE locking the account FAILED — closePosition routes
      // through the order engine, which only fills while the account is ACTIVE.
      if (outcome === "FAILED") await this.liquidate(accountId);

      const client = await getPool().connect();
      try {
        await client.query("BEGIN");
        const lock = await client.query<{ status: string }>(`SELECT "status" FROM "Account" WHERE "id" = $1 FOR UPDATE`, [accountId]);
        if (lock.rows[0]?.status !== "ACTIVE") {
          await client.query("ROLLBACK");
          return; // already settled by a concurrent path
        }
        await client.query(`UPDATE "Account" SET "status" = $2, "updatedAt" = now() WHERE "id" = $1`, [accountId, outcome]);
        if (violation) {
          await client.query(`INSERT INTO "Violation" ("accountId","type","action","detail") VALUES ($1,$2,'SUSPEND_ACCOUNT',$3)`, [accountId, violation, detail]);
          await client.query(`INSERT INTO "ActivityLog" ("accountId","type","message") VALUES ($1,'RULE_VIOLATION',$2)`, [accountId, detail]);
          await client.query(`INSERT INTO "ActivityLog" ("accountId","type","message") VALUES ($1,'ACCOUNT_SUSPENSION','Account failed evaluation — trading disabled')`, [accountId]);
        } else {
          await client.query(`INSERT INTO "ActivityLog" ("accountId","type","message") VALUES ($1,'ACCOUNT_PASSED',$2)`, [accountId, detail]);
        }
        await client.query("COMMIT");
      } catch (e) {
        await client.query("ROLLBACK").catch(() => {});
        throw e;
      } finally {
        client.release();
      }

      console[outcome === "PASSED" ? "log" : "warn"](`[risk] account ${accountId} ${outcome} — ${detail}`);
      await this.accountStream.refreshAccount(accountId); // reload status/positions → pushed live
      this.accountStream.publishAdminUpdate({ kind: outcome, accountId, detail }); // notify admin dashboards
    } catch (err) {
      console.error("[risk] terminate failed:", (err as Error).message);
    } finally {
      this.processing.delete(accountId);
    }
  }

  /** Force-close every open position at market (used on failure). */
  private async liquidate(accountId: string): Promise<void> {
    const { rows } = await getPool().query<{ symbol: string }>(`SELECT "symbol" FROM "Position" WHERE "accountId" = $1`, [accountId]);
    for (const r of rows) {
      await this.orderEngine.closePosition(accountId, r.symbol).catch((e) => console.error("[risk] liquidate failed:", (e as Error).message));
    }
  }
}
