import { getPool } from "../db/pool.js";
import { SYMBOLS, getMultiplier } from "../instruments.js";
import type { AccountStream } from "./account-stream.js";

/* Publishes the live quote map to the shared "MarketMark" table.
 *
 * The signal app runs as a SEPARATE service with no market-data key of its own,
 * but it already shares this database. Writing the marks here gives it the exact
 * same prices the admin Positions page and the trader chart mark against, with no
 * HTTP hop between the services and no extra configuration (TRADING_API_URL /
 * SERVICE_TOKEN) that can be forgotten or misconfigured in a deploy.
 *
 * One small upsert per second for ~10 symbols — negligible, and it makes the
 * cross-service P&L work by default rather than by correct setup. */

const PUBLISH_MS = 1000;

let timer: NodeJS.Timeout | null = null;
let publishing = false;
let ensured = false;
let lastWarning = "";

/**
 * Create the table if this deploy hasn't run `db:migrate` yet. Idempotent, and it
 * means a fresh trading backend publishes marks without a manual migration step —
 * otherwise the signal app's P&L silently stays blank until someone remembers.
 */
async function ensureTable(): Promise<void> {
  if (ensured) return;
  await getPool().query(
    `CREATE TABLE IF NOT EXISTS "MarketMark" (
       "symbol"     text PRIMARY KEY,
       "price"      numeric(18,6) NOT NULL,
       "multiplier" numeric(12,4) NOT NULL DEFAULT 1,
       "updatedAt"  timestamptz   NOT NULL DEFAULT now()
     )`,
  );
  ensured = true;
}

async function publish(accountStream: AccountStream): Promise<void> {
  if (publishing) return; // never let a slow write stack up behind the interval
  publishing = true;
  try {
    await ensureTable();
    const symbols: string[] = [];
    const prices: number[] = [];
    const multipliers: number[] = [];
    for (const symbol of SYMBOLS) {
      const price = accountStream.getMarkPrice(symbol);
      if (price == null || !(price > 0)) continue; // no quote yet — leave any previous row alone
      symbols.push(symbol);
      prices.push(price);
      multipliers.push(getMultiplier(symbol));
    }
    if (symbols.length === 0) return;
    // Single round-trip upsert for every symbol.
    await getPool().query(
      `INSERT INTO "MarketMark" ("symbol","price","multiplier","updatedAt")
       SELECT * FROM unnest($1::text[], $2::numeric[], $3::numeric[]) AS t(s,p,m), LATERAL (SELECT now()) AS n(ts)
       ON CONFLICT ("symbol") DO UPDATE
         SET "price" = EXCLUDED."price",
             "multiplier" = EXCLUDED."multiplier",
             "updatedAt" = EXCLUDED."updatedAt"`,
      [symbols, prices, multipliers],
    );
  } catch (err) {
    // Never let a publish failure disturb trading — the marks table is auxiliary.
    // Log each distinct cause once; this runs every second, so repeating would
    // drown the log in identical lines.
    const msg = (err as Error).message;
    if (msg !== lastWarning) {
      lastWarning = msg;
      console.warn("[marks] publish failed:", msg);
    }
  } finally {
    publishing = false;
  }
}

export function startMarkPublisher(accountStream: AccountStream): void {
  if (timer) return;
  timer = setInterval(() => void publish(accountStream), PUBLISH_MS);
  console.log("[marks] publishing live marks to the shared MarketMark table every 1s");
}

export function stopMarkPublisher(): void {
  if (timer) clearInterval(timer);
  timer = null;
}
