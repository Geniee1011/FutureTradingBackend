import "dotenv/config";
import { getPool, closePool } from "./pool.js";
import { hashPassword } from "../auth/password.js";

/* Seeds demo users + an evaluation account/rule for the trader.
   Idempotent (upserts on unique keys). Run via `npm run db:seed`. */

const USERS = [
  { email: "admin@demo.com", name: "Alex Admin", password: "demo", role: "ADMIN" },
  { email: "trader@demo.com", name: "Marvin Weiss", password: "demo", role: "TRADER" },
] as const;

async function main() {
  if (!process.env.DATABASE_URL) {
    console.error("DATABASE_URL is not set — nothing to seed.");
    process.exit(1);
  }
  const pool = getPool();

  for (const u of USERS) {
    await pool.query(
      `INSERT INTO "User" ("email","passwordHash","name","role")
       VALUES ($1,$2,$3,$4)
       ON CONFLICT ("email") DO UPDATE
         SET "passwordHash" = EXCLUDED."passwordHash",
             "name" = EXCLUDED."name",
             "role" = EXCLUDED."role",
             "updatedAt" = now()`,
      [u.email, hashPassword(u.password), u.name, u.role],
    );
  }

  // Evaluation account + rule for the trader (a $50k eval: $6k target, $2.5k daily
  // loss, $3k max drawdown, 5 contracts, all listed futures allowed).
  const { rows } = await pool.query<{ id: string }>(`SELECT "id" FROM "User" WHERE "email" = $1`, [
    "trader@demo.com",
  ]);
  const traderId = rows[0]?.id;
  if (traderId) {
    const acc = await pool.query<{ id: string }>(
      `INSERT INTO "Account" ("userId","startingBalance","balance","equity","highestEquity","status")
       VALUES ($1, 50000, 50000, 50000, 50000, 'ACTIVE')
       ON CONFLICT ("userId") DO UPDATE SET "updatedAt" = now()
       RETURNING "id"`,
      [traderId],
    );
    const accountId = acc.rows[0]!.id;
    await pool.query(
      `INSERT INTO "Rule" ("accountId","maxDailyLoss","maxDrawdown","profitTarget","maxContracts","allowedInstruments")
       VALUES ($1, 2500, 3000, 6000, 5, $2)
       ON CONFLICT ("accountId") DO UPDATE
         SET "maxDailyLoss" = EXCLUDED."maxDailyLoss",
             "maxDrawdown" = EXCLUDED."maxDrawdown",
             "profitTarget" = EXCLUDED."profitTarget",
             "maxContracts" = EXCLUDED."maxContracts",
             "allowedInstruments" = EXCLUDED."allowedInstruments",
             "updatedAt" = now()`,
      [accountId, ["ES", "MES", "NQ", "MNQ", "YM", "MYM", "CL", "MCL", "GC", "MGC"]],
    );

    // Demo positions + orders so the trade page shows DB-backed data
    // (until the order engine writes these itself). Reset, then insert.
    await pool.query(`DELETE FROM "Position" WHERE "accountId" = $1`, [accountId]);
    await pool.query(`DELETE FROM "Order" WHERE "accountId" = $1`, [accountId]);

    const positions = [
      { symbol: "ES", side: "LONG", qty: 2, avg: 7560.0 },
      { symbol: "NQ", side: "SHORT", qty: 1, avg: 30400.0 },
      { symbol: "CL", side: "LONG", qty: 3, avg: 92.5 },
      { symbol: "GC", side: "LONG", qty: 1, avg: 4480.0 },
    ];
    for (const p of positions) {
      await pool.query(
        `INSERT INTO "Position" ("accountId","symbol","side","quantity","averagePrice","realizedPnl")
         VALUES ($1,$2,$3,$4,$5,$6)`,
        [accountId, p.symbol, p.side, p.qty, p.avg, 0],
      );
    }

    const orders = [
      { symbol: "ES", side: "BUY", type: "LIMIT", status: "PENDING", qty: 1, filled: 0, req: 7500.0, fill: null },
      { symbol: "NQ", side: "SELL", type: "LIMIT", status: "PENDING", qty: 1, filled: 0, req: 30600.0, fill: null },
      { symbol: "GC", side: "BUY", type: "MARKET", status: "FILLED", qty: 1, filled: 1, req: null, fill: 4485.0 },
      { symbol: "CL", side: "SELL", type: "STOP", status: "PENDING", qty: 2, filled: 0, req: 90.0, fill: null },
    ];
    for (const o of orders) {
      await pool.query(
        `INSERT INTO "Order" ("accountId","symbol","side","type","status","quantity","filledQuantity","requestedPrice","fillPrice")
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
        [accountId, o.symbol, o.side, o.type, o.status, o.qty, o.filled, o.req, o.fill],
      );
    }
  }

  const { rows: count } = await pool.query<{ n: string }>(`SELECT count(*)::text AS n FROM "User"`);
  console.log(`✓ Seeded users + evaluation account. "User" table now has ${count[0]!.n} rows.`);
  await closePool();
}

main().catch(async (err) => {
  console.error("Seed failed:", err.message);
  await closePool();
  process.exit(1);
});
