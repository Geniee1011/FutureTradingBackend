import "dotenv/config";
import { getPool, closePool } from "./pool.js";
import { getMultiplier } from "../instruments.js";

/* ------------------------------------------------------------------ *
 * One-time backfill: repair historical closed trades whose PROFIT was
 * voided to $0.00 by the old minimum-hold-time rule.
 *
 * For every ClosedPosition that booked $0 even though its exit differed
 * from its entry in the trader's favour, this:
 *   1. recomputes the TRUE realized P&L  (exit − entry) × qty × dir × multiplier,
 *   2. updates the ClosedPosition row (ALL such rows — the blotter is all-time history),
 *   3. for rows in the account's CURRENT challenge only (closedAt ≥ challengeStartedAt),
 *      writes a matching TRADE ledger entry AND credits the live cash + P&L counters.
 *
 * The challenge scoping in (3) matters: an account can be reset many times (each reset
 * wipes the balance back to its starting value but KEEPS trade history). Crediting the
 * live balance for profits earned in an already-reset challenge would over-credit the
 * current one, so cash is only moved for trades that belong to the running challenge.
 *
 * DRY-RUN by default — prints what it would change and writes nothing.
 * Pass  --apply  to commit. Idempotent: a corrected row (realizedPnl ≠ 0) is
 * never touched again, so re-running is safe and never double-credits.
 *
 * Run against the TARGET database via DATABASE_URL, e.g. on Railway:
 *   railway run npm run db:backfill-realized            # dry run
 *   railway run npm run db:backfill-realized -- --apply # commit
 * ------------------------------------------------------------------ */

const APPLY = process.argv.includes("--apply");

interface ClosedRow {
  id: string;
  accountId: string;
  symbol: string;
  side: string; // LONG | SHORT
  quantity: number;
  entryPrice: string;
  exitPrice: string;
  closedAt: Date;
  challengeStartedAt: Date | null; // owning account's current-challenge anchor
}

async function main() {
  if (!process.env.DATABASE_URL) {
    console.error("DATABASE_URL is not set — point it at the DB you want to repair.");
    process.exit(1);
  }
  const pool = getPool();

  // Candidate rows: booked $0 but the price actually moved. We only correct rows whose
  // recomputed P&L is POSITIVE — that is exactly the set the old rule voided (it zeroed
  // fast profits only; losses always booked, and a genuine scratch trade recomputes to 0).
  const { rows } = await pool.query<ClosedRow>(
    `SELECT c."id", c."accountId", c."symbol", c."side", c."quantity", c."entryPrice", c."exitPrice",
            c."closedAt", a."challengeStartedAt"
     FROM "ClosedPosition" c JOIN "Account" a ON a."id" = c."accountId"
     WHERE c."realizedPnl" = 0 ORDER BY c."closedAt" ASC`,
  );

  const fixes = rows
    .map((r) => {
      const dir = r.side === "LONG" ? 1 : -1;
      const realized =
        Math.round((Number(r.exitPrice) - Number(r.entryPrice)) * Number(r.quantity) * dir * getMultiplier(r.symbol) * 100) / 100;
      // Cash (ledger + balance credit) moves only for the CURRENT challenge; the blotter row
      // is corrected regardless (all-time history). No anchor → treat as current.
      const current = r.challengeStartedAt == null || new Date(r.closedAt).getTime() >= new Date(r.challengeStartedAt).getTime();
      return { row: r, realized, current };
    })
    .filter((f) => f.realized > 0);

  if (fixes.length === 0) {
    console.log("No voided-profit rows found. Nothing to correct.");
    await closePool();
    return;
  }

  // Only CURRENT-challenge recoveries move cash; sum them per account for the balance credit.
  const byAccount = new Map<string, number>();
  for (const f of fixes.filter((f) => f.current))
    byAccount.set(f.row.accountId, Math.round(((byAccount.get(f.row.accountId) ?? 0) + f.realized) * 100) / 100);

  console.log(`Found ${fixes.length} voided-profit closed trade(s) to correct (blotter):\n`);
  for (const f of fixes) {
    const r = f.row;
    console.log(
      `  ${r.symbol} ${r.side} q${r.quantity}  ${Number(r.entryPrice)} -> ${Number(r.exitPrice)}   ` +
        `+$${f.realized.toFixed(2)}   ${new Date(r.closedAt).toISOString()}   ${f.current ? "[current → credits cash]" : "[prior challenge → blotter only]"}`,
    );
  }
  console.log("\nPer-account credit (current challenge only):");
  for (const [acct, sum] of byAccount) console.log(`  account ${acct}:  +$${sum.toFixed(2)}`);

  if (!APPLY) {
    console.log("\nDRY RUN — nothing written. Re-run with  --apply  to commit these changes.");
    await closePool();
    return;
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    for (const f of fixes) {
      // Blotter row is corrected for every voided trade (all-time history).
      await client.query(`UPDATE "ClosedPosition" SET "realizedPnl" = $2 WHERE "id" = $1`, [f.row.id, f.realized]);
      // Cash-ledger entry ONLY for current-challenge trades — a prior, reset challenge's
      // cash is gone, so we don't fabricate a ledger movement for it. Stamp it at the real
      // close time so it lands in the right place in the (challenge-scoped) history.
      if (f.current) {
        await client.query(
          `INSERT INTO "Transaction" ("accountId","type","amount","description","createdAt")
           VALUES ($1,'TRADE',$2,$3,$4)`,
          [f.row.accountId, f.realized, `Realized P&L · ${f.row.symbol} (recovered)`, f.row.closedAt],
        );
      }
    }
    // Credit each account by its recovered total. Lift the high-water mark too, so the trailing
    // drawdown isn't measured against a now-stale lower peak. (equity is recomputed live by the
    // account stream once it reloads; we set it here so the persisted column is correct meanwhile.)
    for (const [acct, sum] of byAccount) {
      await client.query(
        `UPDATE "Account" SET
           "balance" = "balance" + $2,
           "equity" = "equity" + $2,
           "totalPnl" = "totalPnl" + $2,
           "highestEquity" = GREATEST("highestEquity", "equity" + $2),
           "updatedAt" = now()
         WHERE "id" = $1`,
        [acct, sum],
      );
    }
    await client.query("COMMIT");
    console.log(`\n✓ Applied. Corrected ${fixes.length} trade(s) across ${byAccount.size} account(s).`);
    console.log("→ Restart / redeploy the backend so the live account cache reloads the new balances.");
  } catch (err) {
    await client.query("ROLLBACK").catch(() => {});
    throw err;
  } finally {
    client.release();
  }
  await closePool();
}

main().catch(async (err) => {
  console.error("Backfill failed:", (err as Error).message);
  await closePool();
  process.exit(1);
});
