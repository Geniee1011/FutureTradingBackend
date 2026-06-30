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
 *                   → close all positions + pause trading for the rest of
 *                     the trading day (does NOT fail the challenge)
 *   maxDrawdown   — equity dropped >= limit from the trailing high-water mark
 *                   → FAIL the challenge (terminal)
 *   profitTarget  — equity rose >= target above the starting balance
 *                   AND minimum trading days met
 *                   → PASS the phase (terminal for Phase 2; auto-advance for Phase 1)
 * ------------------------------------------------------------------ */

type ViolationType = "DAILY_LOSS_EXCEEDED" | "MAX_DRAWDOWN_BREACHED";

interface EvalRow {
  status: string;
  challengePhase: number;
  ruleTemplateId: string | null;
  startingBalance: string;
  highestEquity: string;
  dayStartEquity: string | null;
  dayStartAt: Date | string | null;
  tradingPausedAt: Date | string | null;
  peakIntradayEquity: string | null;
  eodPeakEquity: string | null;
  maxDailyLoss: string | null;
  maxDrawdown: string | null;
  profitTarget: string | null;
  minTradingDays: number | null;
  drawdownType: string | null;
}

const round2 = (v: number) => Math.round(v * 100) / 100;
const dateStr = (d: Date | string) => new Date(d).toISOString().slice(0, 10);

// Account progression ladder: passing one tier's profit target auto-advances to the next.
// The 50k and 100k challenge paths merge into one shared funded scaling ladder. `f_1m` is the
// top — it has no successor, so it stays put (a target hit there is a payout reset, not a scale).
const NEXT_TIER: Record<string, string> = {
  c1_50k: "c2_50k",
  c2_50k: "f_50k",
  c1_100k: "c2_100k",
  c2_100k: "f_100k",
  f_50k: "f_100k",
  f_100k: "f_250k",
  f_250k: "f_500k",
  f_500k: "f_1m",
};

export class RiskEngine {
  private processing = new Set<string>(); // accounts mid terminal-transition
  private pauseProcessing = new Set<string>(); // accounts mid daily-limit pause
  private last = new Map<string, { equity: number; day: string }>(); // skip idle re-evaluation

  constructor(
    private readonly orderEngine: OrderEngine,
    private readonly accountStream: AccountStream,
  ) {}

  /** Evaluate one account against its rule using live equity. Cheap + idempotent. */
  async evaluate(accountId: string, equity: number): Promise<void> {
    if (this.processing.has(accountId) || this.pauseProcessing.has(accountId)) return;
    const today = new Date().toISOString().slice(0, 10);
    const prev = this.last.get(accountId);
    // Nothing material moved since the last tick → skip the DB round-trip entirely.
    if (prev && prev.day === today && Math.abs(equity - prev.equity) < 0.01) return;
    this.last.set(accountId, { equity, day: today });

    const { rows } = await getPool().query<EvalRow>(
      `SELECT a."status", a."challengePhase", a."ruleTemplateId", a."startingBalance", a."highestEquity",
              a."dayStartEquity", a."dayStartAt", a."tradingPausedAt",
              a."peakIntradayEquity", a."eodPeakEquity",
              r."maxDailyLoss", r."maxDrawdown", r."profitTarget", r."minTradingDays", r."drawdownType"
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

    const peak = Math.max(Number(a.highestEquity), equity); // all-time real-time high-water mark
    const dailyLoss = round2(Math.max(0, dayStartEquity - equity));
    const profit = round2(equity - startingBalance);

    const maxDailyLoss = a.maxDailyLoss != null ? Number(a.maxDailyLoss) : 0;
    const maxDrawdown  = a.maxDrawdown  != null ? Number(a.maxDrawdown)  : 0;
    const profitTarget = a.profitTarget != null ? Number(a.profitTarget) : 0;
    const minTradingDays = a.minTradingDays ?? 0;
    const drawdownType = a.drawdownType ?? "INTRADAY";

    // ---- EOD trailing drawdown state ----
    // The session's running intraday peak (highest equity reached today). On a day
    // rollover, the PRIOR session's intraday peak is banked into the EOD peak — this is
    // the "session close" snapshot — then the intraday peak restarts at the current equity.
    let eodPeak = a.eodPeakEquity != null ? Number(a.eodPeakEquity) : startingBalance;
    const priorIntraday = a.peakIntradayEquity != null ? Number(a.peakIntradayEquity) : startingBalance;
    let peakIntraday: number;
    if (rolled) {
      if (priorIntraday > eodPeak) eodPeak = priorIntraday; // session close: bank the day's high
      peakIntraday = equity; // new session — seed the intraday peak at the opening equity
    } else {
      peakIntraday = Math.max(priorIntraday, equity); // track the running high during the session
    }

    // The drawdown that matters depends on the account's drawdown style:
    //   INTRADAY → floor ratchets in real time off the all-time peak (challenge accounts)
    //   EOD      → floor is fixed for the day at (banked EOD peak − maxDrawdown) (funded accounts)
    const isEod = drawdownType === "EOD";
    const drawdownRef = isEod ? eodPeak : peak; // basis for the floor
    const drawdown = round2(Math.max(0, drawdownRef - equity));

    // Keep the live account fields fresh.
    // On a new day, reset the realized daily figure and clear the trading-pause flag.
    await getPool().query(
      `UPDATE "Account" SET "equity" = $2, "drawdown" = $3, "highestEquity" = $4,
              "dayStartEquity" = $5, "dayStartAt" = $6::date,
              "dailyPnl" = CASE WHEN $7 THEN 0 ELSE "dailyPnl" END,
              "tradingPausedAt" = CASE WHEN $7 THEN NULL ELSE "tradingPausedAt" END,
              "peakIntradayEquity" = $8, "eodPeakEquity" = $9,
              "updatedAt" = now()
       WHERE "id" = $1`,
      [accountId, round2(equity), drawdown, round2(peak), round2(dayStartEquity), today, rolled,
       round2(peakIntraday), round2(eodPeak)],
    );

    // Skip daily-limit re-evaluation if already paused today.
    const alreadyPaused = a.tradingPausedAt != null && dateStr(a.tradingPausedAt) === today;

    if (maxDailyLoss > 0 && dailyLoss >= maxDailyLoss && !alreadyPaused) {
      // Daily loss hit → close positions + pause for today. Challenge stays ACTIVE.
      await this.applyDailyLimitPause(accountId, dailyLoss, maxDailyLoss);
    } else if (maxDrawdown > 0 && drawdown >= maxDrawdown) {
      // Drawdown floor breached → FAIL the challenge (terminal). Floor basis differs by type.
      const floor = round2(drawdownRef - maxDrawdown);
      const label = isEod ? "EOD trailing drawdown" : "Trailing drawdown";
      await this.terminate(accountId, "FAILED", "MAX_DRAWDOWN_BREACHED",
        `${label}: equity $${round2(equity)} fell to/below floor $${floor} (limit $${maxDrawdown})`);
    } else if (profitTarget > 0 && profit >= profitTarget) {
      // Profit target hit — check additional conditions before passing/advancing.
      await this.checkAndPass(accountId, profit, profitTarget, minTradingDays, a.ruleTemplateId);
    }
  }

  /** Daily loss hit: liquidate, pause trading today, record violation — account stays ACTIVE. */
  private async applyDailyLimitPause(accountId: string, loss: number, limit: number): Promise<void> {
    if (this.pauseProcessing.has(accountId)) return;
    this.pauseProcessing.add(accountId);
    try {
      await this.liquidate(accountId);
      const today = new Date().toISOString().slice(0, 10);
      const detail = `Daily loss $${loss} reached limit $${limit} — positions closed, trading paused until tomorrow`;
      await getPool().query(
        `UPDATE "Account" SET "tradingPausedAt" = $2::date, "updatedAt" = now() WHERE "id" = $1`,
        [accountId, today],
      );
      await getPool().query(
        `INSERT INTO "Violation" ("accountId","type","action","detail") VALUES ($1,'DAILY_LOSS_EXCEEDED','LIQUIDATE_POSITION',$2)`,
        [accountId, detail],
      );
      await getPool().query(
        `INSERT INTO "ActivityLog" ("accountId","type","message") VALUES ($1,'RULE_VIOLATION',$2)`,
        [accountId, detail],
      );
      console.warn(`[risk] account ${accountId} daily limit — ${detail}`);
      await this.accountStream.refreshAccount(accountId);
      this.accountStream.publishAdminUpdate({ kind: "DAILY_LIMIT_HIT", accountId, detail });
    } catch (err) {
      console.error("[risk] daily-limit pause failed:", (err as Error).message);
    } finally {
      this.pauseProcessing.delete(accountId);
    }
  }

  /**
   * Profit target reached — verify minimum trading days, then advance up the tier ladder
   * (NEXT_TIER): c1→c2→funded, then funded scaling 50k→100k→250k→500k→1m, each step
   * triggered by hitting that tier's target. The top tier (f_1m) resets in place (a
   * payout) rather than stopping; an account on no/unknown tier falls back to PASSED.
   */
  private async checkAndPass(
    accountId: string,
    profit: number,
    target: number,
    minTradingDays: number,
    currentTier: string | null,
  ): Promise<void> {
    // Count distinct calendar days with at least one filled order in this challenge.
    if (minTradingDays > 0) {
      const { rows } = await getPool().query<{ days: string }>(
        `SELECT COUNT(DISTINCT DATE("updatedAt"))::text AS days
         FROM "Order"
         WHERE "accountId" = $1 AND "status" = 'FILLED'
           AND "updatedAt" >= (SELECT "challengeStartedAt" FROM "Account" WHERE "id" = $1)`,
        [accountId],
      );
      const tradingDays = Number(rows[0]?.days ?? 0);
      if (tradingDays < minTradingDays) {
        // Not enough trading days yet — keep the account active and wait.
        return;
      }
    }

    const detail = `Profit $${profit} reached target $${target}`;
    const next = currentTier ? NEXT_TIER[currentTier] : undefined;
    if (next) {
      // Climb to the next tier (challenge phase or a bigger funded account).
      await this.advanceToTier(accountId, next, `${detail} — advanced to ${next}`);
    } else if (currentTier === "f_1m") {
      // Top of the ladder — a target hit is a payout: reset the $1M account in place
      // so the trader keeps going rather than being stopped.
      await this.advanceToTier(accountId, "f_1m", `${detail} — payout, $1M account reset`);
    } else {
      // No recognized tier (custom/ad-hoc account) — preserve the old PASSED-for-review path.
      await this.terminate(accountId, "PASSED", null, detail);
    }
  }

  /**
   * Advance an account onto `templateId` (the next rung of NEXT_TIER): reset its stats and
   * size to that tier's starting state and apply its full ruleset, on the SAME account.
   * Used for every step — Phase 1→2, Phase 2→funded, and funded scaling — so size changes
   * (e.g. 50k→100k) flow through here too. A clean slate: positions/orders are cleared.
   */
  private async advanceToTier(accountId: string, templateId: string, detail: string): Promise<void> {
    if (this.processing.has(accountId)) return;
    this.processing.add(accountId);
    try {
      const client = await getPool().connect();
      try {
        await client.query("BEGIN");
        const lock = await client.query<{ status: string }>(
          `SELECT "status" FROM "Account" WHERE "id" = $1 FOR UPDATE`,
          [accountId],
        );
        if (lock.rows[0]?.status !== "ACTIVE") { await client.query("ROLLBACK"); return; }

        // The destination tier defines the new account size and challenge phase.
        const tpl = (await client.query<{ phase: string; accountSize: string }>(
          `SELECT "phase","accountSize" FROM "RuleTemplate" WHERE "id" = $1`,
          [templateId],
        )).rows[0];
        if (!tpl) { await client.query("ROLLBACK"); return; }
        const size = Number(tpl.accountSize);
        const phase = tpl.phase === "Challenge Phase 1" ? 1 : 2;

        // Reset the account to the destination tier's starting state (size + anchors + phase).
        await client.query(
          `UPDATE "Account"
           SET "ruleTemplateId" = $3, "startingBalance" = $2,
               "balance" = $2, "equity" = $2, "dailyPnl" = 0, "totalPnl" = 0,
               "drawdown" = 0, "highestEquity" = $2,
               "dayStartEquity" = $2, "dayStartAt" = CURRENT_DATE,
               "peakIntradayEquity" = $2, "eodPeakEquity" = $2,
               "tradingPausedAt" = NULL, "challengePhase" = $4,
               "challengeStartedAt" = now(), "status" = 'ACTIVE', "updatedAt" = now()
           WHERE "id" = $1`,
          [accountId, size, templateId, phase],
        );

        // Copy the destination tier's full ruleset onto the per-account Rule row.
        await client.query(
          `UPDATE "Rule" SET
             "maxDailyLoss"            = t."maxDailyLoss",
             "maxDrawdown"             = t."maxDrawdown",
             "profitTarget"            = t."profitTarget",
             "maxContracts"            = t."maxContracts",
             "minTradingDays"          = t."minTradingDays",
             "maxDailyProfitPct"       = t."maxDailyProfitPct",
             "maxRiskPerTrade"         = t."maxRiskPerTrade",
             "maxPositionUnits"        = t."maxPositionUnits",
             "stopLossRequired"        = t."stopLossRequired",
             "minHoldTimeSecs"         = t."minHoldTimeSecs",
             "overnightHoldsProhibited"= t."overnightHoldsProhibited",
             "weekendHoldsProhibited"  = t."weekendHoldsProhibited",
             "drawdownType"            = t."drawdownType",
             "allowedInstruments"      = t."allowedInstruments",
             "updatedAt"               = now()
           FROM "RuleTemplate" t
           WHERE "Rule"."accountId" = $1 AND t."id" = $2`,
          [accountId, templateId],
        );

        // Clear live state so the new tier starts clean.
        await client.query(`DELETE FROM "Position" WHERE "accountId" = $1`, [accountId]);
        await client.query(`DELETE FROM "PositionLot" WHERE "accountId" = $1`, [accountId]);
        await client.query(
          `UPDATE "Order" SET "status" = 'CANCELLED', "reason" = 'Tier advanced', "updatedAt" = now()
           WHERE "accountId" = $1 AND "status" = 'PENDING'`,
          [accountId],
        );

        await client.query(
          `INSERT INTO "ActivityLog" ("accountId","type","message") VALUES ($1,'ACCOUNT_PASSED',$2)`,
          [accountId, detail],
        );
        await client.query("COMMIT");
      } catch (e) {
        await client.query("ROLLBACK").catch(() => {});
        throw e;
      } finally {
        client.release();
      }

      console.log(`[risk] account ${accountId} advanced to ${templateId} — ${detail}`);
      await this.accountStream.refreshAccount(accountId);
      this.accountStream.publishAdminUpdate({ kind: "PHASE_ADVANCED", accountId, detail });
    } catch (err) {
      console.error("[risk] tier advance failed:", (err as Error).message);
    } finally {
      this.processing.delete(accountId);
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

  /** Force-close every open position at market (used on failure + daily-limit). */
  private async liquidate(accountId: string): Promise<void> {
    const { rows } = await getPool().query<{ symbol: string }>(`SELECT "symbol" FROM "Position" WHERE "accountId" = $1`, [accountId]);
    for (const r of rows) {
      // Risk liquidation must always run — bypass the market-hours gate.
      await this.orderEngine
        .closePosition(accountId, r.symbol, { bypassMarketHours: true })
        .catch((e) => console.error("[risk] liquidate failed:", (e as Error).message));
    }
  }
}
