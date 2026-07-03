import { getPool } from "../db/pool.js";
import { adminResetAccount } from "./admin-repository.js";
import { AUTO_RESET_HOURS } from "./repository.js";
import type { AccountStream } from "../realtime/account-stream.js";

/* Auto-reset sweeper: a FAILED account whose owner requested a reset is reset automatically
   AUTO_RESET_HOURS (12h) after the request. Runs on an interval; each pass resets every account
   past its window. adminResetAccount clears resetRequestedAt, so an account is reset once. */

const SWEEP_MS = 5 * 60_000; // check every 5 min — a few minutes' slack past the 12h mark is fine

export function startResetSweeper(accountStream: AccountStream): NodeJS.Timeout {
  const sweep = async () => {
    try {
      const { rows } = await getPool().query<{ id: string }>(
        `SELECT "id" FROM "Account"
         WHERE "status" = 'FAILED' AND "resetRequestedAt" IS NOT NULL
           AND "resetRequestedAt" <= now() - make_interval(hours => $1::int)`,
        [AUTO_RESET_HOURS],
      );
      for (const r of rows) {
        const ok = await adminResetAccount(r.id);
        if (!ok) continue;
        await accountStream.refreshAccount(r.id).catch(() => {});
        accountStream.resetDrawdownPeak(r.id); // fresh equity → recompute trailing drawdown from $0
        accountStream.publishAdminUpdate({ kind: "auto_reset", accountId: r.id });
        console.log(`[reset-sweeper] auto-reset account ${r.id} (${AUTO_RESET_HOURS}h after request)`);
      }
    } catch (err) {
      console.error("[reset-sweeper] sweep failed:", (err as Error).message);
    }
  };
  return setInterval(() => void sweep(), SWEEP_MS);
}
