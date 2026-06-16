import "dotenv/config";
import { WebSocket } from "ws";
import { getPool, closePool } from "./pool.js";
import { hashPassword } from "../auth/password.js";
import { getInstrument } from "../instruments.js";

/* Seeds demo users + an evaluation account/rule for the trader.
   Idempotent (upserts on unique keys). Run via `npm run db:seed`. */

const USERS = [
  { email: "admin@demo.com", name: "Alex Admin", password: "demo", role: "ADMIN" },
  { email: "trader@demo.com", name: "Marvin Weiss", password: "demo", role: "TRADER" },
] as const;

const MARK_SYMBOLS = ["ES", "NQ", "CL", "GC"] as const;

function roundTo(symbol: string, price: number): number {
  const f = 10 ** (getInstrument(symbol)?.pricePrecision ?? 2);
  return Math.round(price * f) / f;
}

/**
 * Best-effort: pull current marks from the running backend's WS (it sends a quote
 * snapshot on subscribe). Lets the demo positions/orders be priced near the LIVE
 * market so P&L stays modest and the account stays ACTIVE — instead of going
 * deeply under/over water against stale simBase levels. Falls back to simBase
 * (per-symbol) if the backend isn't reachable.
 */
async function fetchLiveMarks(symbols: readonly string[]): Promise<Record<string, number>> {
  const port = process.env.PORT ?? "8000";
  const marks: Record<string, number> = {};
  await new Promise<void>((resolve) => {
    let settled = false;
    const ws = new WebSocket(`ws://localhost:${port}/ws`);
    const finish = () => {
      if (settled) return;
      settled = true;
      try {
        ws.close();
      } catch {
        /* ignore */
      }
      resolve();
    };
    const timer = setTimeout(finish, 4000);
    ws.on("open", () => symbols.forEach((s) => ws.send(JSON.stringify({ type: "subscribe", channel: "quotes", symbol: s }))));
    ws.on("message", (raw) => {
      try {
        const msg = JSON.parse(String(raw)) as { type?: string; data?: { symbol: string; price: number } };
        if (msg.type === "quote" && msg.data && marks[msg.data.symbol] == null) {
          marks[msg.data.symbol] = msg.data.price;
          if (symbols.every((s) => marks[s] != null)) {
            clearTimeout(timer);
            finish();
          }
        }
      } catch {
        /* ignore */
      }
    });
    ws.on("error", () => {
      clearTimeout(timer);
      finish();
    });
  });
  return marks;
}

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
      `INSERT INTO "Account" ("userId","startingBalance","balance","equity","highestEquity","status","dayStartEquity","dayStartAt")
       VALUES ($1, 50000, 50000, 50000, 50000, 'ACTIVE', 50000, CURRENT_DATE)
       ON CONFLICT ("userId") DO UPDATE SET
         "startingBalance" = 50000, "balance" = 50000, "equity" = 50000, "highestEquity" = 50000,
         "dailyPnl" = 0, "totalPnl" = 0, "drawdown" = 0,
         "dayStartEquity" = 50000, "dayStartAt" = CURRENT_DATE,
         "status" = 'ACTIVE', "updatedAt" = now()
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

    // Price the demo positions near the LIVE mark (fetched above) so each one's
    // dollar P&L (× contract multiplier) is small and bounded — a mix of green/red
    // that keeps the account comfortably ACTIVE. Offsets are in price points.
    //
    // DRIFT-PROOFING: seed MICRO contracts (MES/MNQ/MCL/MGC, 1/10th the point value
    // of their E-mini parents) rather than full-size. The open book still marks live
    // off the parent's quote (micros trade at the same price level), but the dollar
    // swing is ~10× smaller — so an unattended demo account can't drift through the
    // $3,000 max-drawdown limit over hours of live market movement. `base` is the
    // symbol we pull the live mark from (micros aren't separately quoted server-side).
    const marks = await fetchLiveMarks(MARK_SYMBOLS);
    const markOf = (s: string) => marks[s] ?? getInstrument(s)!.simBase;
    const positions = [
      { symbol: "MES", base: "ES", side: "LONG", qty: 1, avg: roundTo("MES", markOf("ES") - 8) }, // MES $5/pt → ~+$40
      { symbol: "MNQ", base: "NQ", side: "SHORT", qty: 1, avg: roundTo("MNQ", markOf("NQ") + 25) }, // MNQ $2/pt → ~+$50
      { symbol: "MCL", base: "CL", side: "LONG", qty: 1, avg: roundTo("MCL", markOf("CL") + 0.2) }, // MCL $100/pt → ~-$20 (red)
      { symbol: "MGC", base: "GC", side: "LONG", qty: 1, avg: roundTo("MGC", markOf("GC") - 4) }, // MGC $10/pt → ~+$40
    ];
    for (const p of positions) {
      await pool.query(
        `INSERT INTO "Position" ("accountId","symbol","side","quantity","averagePrice","realizedPnl")
         VALUES ($1,$2,$3,$4,$5,$6)`,
        [accountId, p.symbol, p.side, p.qty, p.avg, 0],
      );
    }

    // Working orders priced clear of the live mark so the resting-order monitor
    // does NOT immediately trigger them (buy-limit below, sell-limit above,
    // sell-stop below). The MGC market order is historical (it opened the MGC pos).
    // Micro symbols match the seeded positions; priced off the parent's live mark.
    const orders = [
      { symbol: "MES", side: "BUY", type: "LIMIT", status: "PENDING", qty: 1, filled: 0, req: roundTo("MES", markOf("ES") - 60), fill: null },
      { symbol: "MNQ", side: "SELL", type: "LIMIT", status: "PENDING", qty: 1, filled: 0, req: roundTo("MNQ", markOf("NQ") + 120), fill: null },
      { symbol: "MGC", side: "BUY", type: "MARKET", status: "FILLED", qty: 1, filled: 1, req: null, fill: roundTo("MGC", markOf("GC") - 4) },
      { symbol: "MCL", side: "SELL", type: "STOP", status: "PENDING", qty: 1, filled: 0, req: roundTo("MCL", markOf("CL") - 3), fill: null },
    ];
    for (const o of orders) {
      await pool.query(
        `INSERT INTO "Order" ("accountId","symbol","side","type","status","quantity","filledQuantity","requestedPrice","fillPrice")
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
        [accountId, o.symbol, o.side, o.type, o.status, o.qty, o.filled, o.req, o.fill],
      );
    }

    // A few CLOSED positions (round-trip trades) so the admin Positions view has
    // history. Realized P&L = (exit−entry) for longs / (entry−exit) for shorts,
    // × qty × contract multiplier, rounded to cents. Idempotent: reset first.
    await pool.query(`DELETE FROM "ClosedPosition" WHERE "accountId" = $1`, [accountId]);
    const closedTrades = [
      { symbol: "MES", side: "LONG", qty: 1, entry: roundTo("MES", markOf("ES") - 12), exit: roundTo("MES", markOf("ES") - 2), dOpen: 5, dClose: 4 },
      { symbol: "MNQ", side: "SHORT", qty: 2, entry: roundTo("MNQ", markOf("NQ") + 30), exit: roundTo("MNQ", markOf("NQ") + 10), dOpen: 4, dClose: 3 },
      { symbol: "MCL", side: "LONG", qty: 1, entry: roundTo("MCL", markOf("CL") + 0.3), exit: roundTo("MCL", markOf("CL") + 0.1), dOpen: 3, dClose: 2 }, // red
      { symbol: "MGC", side: "LONG", qty: 1, entry: roundTo("MGC", markOf("GC") - 6), exit: roundTo("MGC", markOf("GC") - 1), dOpen: 2, dClose: 1 },
    ];
    for (const c of closedTrades) {
      const mult = getInstrument(c.symbol)!.multiplier;
      const realized = Math.round((c.side === "LONG" ? c.exit - c.entry : c.entry - c.exit) * c.qty * mult * 100) / 100;
      await pool.query(
        `INSERT INTO "ClosedPosition" ("accountId","symbol","side","quantity","entryPrice","exitPrice","realizedPnl","openedAt","closedAt")
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)`,
        [accountId, c.symbol, c.side, c.qty, c.entry, c.exit, realized,
         new Date(Date.now() - c.dOpen * 86_400_000), new Date(Date.now() - c.dClose * 86_400_000)],
      );
    }

    // Transaction history.
    await pool.query(`DELETE FROM "Transaction" WHERE "accountId" = $1`, [accountId]);
    const txns = [
      { type: "DEPOSIT", amount: 50000, desc: "Initial deposit", daysAgo: 30 },
      { type: "TRADE", amount: 1240.0, desc: "Realized P&L · MES", daysAgo: 3 },
      { type: "FEE", amount: -4.5, desc: "Trading commission", daysAgo: 3 },
      { type: "TRADE", amount: -380.5, desc: "Realized P&L · MNQ", daysAgo: 2 },
      { type: "FUNDING", amount: -12.2, desc: "Overnight funding", daysAgo: 1 },
      { type: "FEE", amount: -8.0, desc: "Trading commission", daysAgo: 0 },
    ];
    for (const t of txns) {
      await pool.query(
        `INSERT INTO "Transaction" ("accountId","type","amount","description","createdAt")
         VALUES ($1,$2,$3,$4,$5)`,
        [accountId, t.type, t.amount, t.desc, new Date(Date.now() - t.daysAgo * 86_400_000)],
      );
    }

    // Keep the account balance consistent with the transaction ledger so the
    // equity curve (cumulative transactions) ends at the shown balance.
    await pool.query(
      `UPDATE "Account" SET
         "balance" = sub.total, "equity" = sub.total,
         "highestEquity" = GREATEST("highestEquity", sub.total),
         "totalPnl" = sub.total - "startingBalance", "updatedAt" = now()
       FROM (SELECT COALESCE(SUM("amount"), 0) AS total FROM "Transaction" WHERE "accountId" = $1) sub
       WHERE "id" = $1`,
      [accountId],
    );
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
