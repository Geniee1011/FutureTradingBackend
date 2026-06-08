import type { PoolClient } from "pg";
import { getPool } from "../db/pool.js";
import { useDatabase } from "../config.js";
import { getInstrument } from "../instruments.js";
import type { AccountStream } from "../realtime/account-stream.js";

/* ------------------------------------------------------------------ *
 * Order engine — real, Postgres-backed order placement.
 *
 * Market orders fill immediately at the live mark price: writes an Order + Fill,
 * nets the Position (weighted-avg cost, realized P&L on reduce/close), and
 * updates the Account. Limit/stop orders are stored PENDING (triggering is a
 * future step). Pre-trade checks: allowed instruments + max contracts.
 * ------------------------------------------------------------------ */

export type ApiSide = "buy" | "sell";
export type ApiOrderType = "market" | "limit" | "stop";

export interface PlaceOrderInput {
  symbol: string;
  side: ApiSide;
  type: ApiOrderType;
  quantity: number;
  price?: number | null;
}

export interface PlaceResult {
  ok: boolean;
  error?: string;
  orderId?: string;
  status?: string;
  fillPrice?: number | null;
  realizedPnl?: number;
}

interface PositionRow {
  id: string;
  side: "LONG" | "SHORT";
  quantity: number;
  averagePrice: number;
}

interface WorkingOrder {
  id: string;
  accountId: string;
  symbol: string;
  side: "BUY" | "SELL";
  type: "LIMIT" | "STOP";
  quantity: number;
  requestedPrice: number;
}

// How often the resting-order monitor scans for triggers.
const WORKING_SCAN_MS = 1000;

export class OrderEngine {
  private monitorTimer: NodeJS.Timeout | null = null;
  private scanning = false; // guard against overlapping scans

  constructor(private readonly accountStream: AccountStream) {}

  /** Start the resting-order monitor (limit/stop triggering). No-op without a DB. */
  start(): void {
    if (!useDatabase || this.monitorTimer) return;
    this.monitorTimer = setInterval(() => void this.scanWorkingOrders(), WORKING_SCAN_MS);
  }

  stop(): void {
    if (this.monitorTimer) clearInterval(this.monitorTimer);
    this.monitorTimer = null;
  }

  async place(accountId: string, input: PlaceOrderInput): Promise<PlaceResult> {
    const inst = getInstrument(input.symbol);
    if (!inst) return { ok: false, error: "Unknown instrument." };
    if (!Number.isInteger(input.quantity) || input.quantity <= 0)
      return { ok: false, error: "Quantity must be a positive whole number of contracts." };
    if (input.type !== "market" && !(typeof input.price === "number" && input.price > 0))
      return { ok: false, error: "A valid price is required for limit/stop orders." };

    const mark = this.accountStream.getMarkPrice(input.symbol);
    if (input.type === "market" && !(mark && mark > 0))
      return { ok: false, error: "No live price available for this symbol right now." };

    const pool = getPool();
    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      // Pre-trade risk: allowed instruments + max contracts (resulting net size).
      const rule = await client.query<{ allowedInstruments: string[]; maxContracts: number; status: string }>(
        `SELECT r."allowedInstruments", r."maxContracts", a."status"
         FROM "Account" a LEFT JOIN "Rule" r ON r."accountId" = a."id"
         WHERE a."id" = $1 FOR UPDATE OF a`,
        [accountId],
      );
      const acc = rule.rows[0];
      if (acc?.status && acc.status !== "ACTIVE") {
        await client.query("ROLLBACK");
        return { ok: false, error: `Account is ${acc.status} — trading is disabled.` };
      }
      if (acc?.allowedInstruments?.length && !acc.allowedInstruments.includes(input.symbol)) {
        await client.query("ROLLBACK");
        return { ok: false, error: `${input.symbol} is not allowed on this account.` };
      }

      const isMarket = input.type === "market";
      const fillPrice = isMarket ? mark! : (input.price as number);

      // Limit/stop → store as a working order; no fill yet.
      if (!isMarket) {
        const ord = await client.query<{ id: string }>(
          `INSERT INTO "Order" ("accountId","symbol","side","type","quantity","requestedPrice","status")
           VALUES ($1,$2,$3,$4,$5,$6,'PENDING') RETURNING "id"`,
          [accountId, input.symbol, input.side.toUpperCase(), input.type.toUpperCase(), input.quantity, input.price],
        );
        await this.log(client, accountId, "ORDER_PLACEMENT", `${input.side} ${input.quantity} ${input.symbol} ${input.type} @ ${input.price}`);
        await client.query("COMMIT");
        const order = { id: ord.rows[0]!.id, status: "open" };
        this.accountStream.publishOrderUpdate(accountId, order);
        return { ok: true, orderId: order.id, status: "PENDING", fillPrice: null };
      }

      // Market order: check resulting position size against maxContracts.
      const existing = await this.getPosition(client, accountId, input.symbol);
      const netAfter = this.netSizeAfter(existing, input.side, input.quantity);
      if (acc?.maxContracts && netAfter > acc.maxContracts) {
        await client.query("ROLLBACK");
        return { ok: false, error: `Exceeds max position size (${acc.maxContracts} contracts).` };
      }

      // Fill it: order + fill + position + account.
      const ord = await client.query<{ id: string }>(
        `INSERT INTO "Order" ("accountId","symbol","side","type","quantity","filledQuantity","fillPrice","status")
         VALUES ($1,$2,$3,'MARKET',$4,$4,$5,'FILLED') RETURNING "id"`,
        [accountId, input.symbol, input.side.toUpperCase(), input.quantity, fillPrice],
      );
      const orderId = ord.rows[0]!.id;
      const realized = await this.settleFill(
        client, accountId, orderId, input.symbol, input.side, input.quantity, fillPrice, inst.pricePrecision,
      );
      await this.log(client, accountId, "ORDER_FILLED", `${input.side} ${input.quantity} ${input.symbol} @ ${fillPrice}`);

      await client.query("COMMIT");

      // Refresh the live bridge cache so position/account updates reflect the trade.
      await this.accountStream.refreshAccount(accountId);
      this.accountStream.publishOrderUpdate(accountId, { id: orderId, status: "filled", symbol: input.symbol });

      return { ok: true, orderId, status: "FILLED", fillPrice, realizedPnl: realized };
    } catch (err) {
      await client.query("ROLLBACK").catch(() => {});
      return { ok: false, error: (err as Error).message };
    } finally {
      client.release();
    }
  }

  /** Close a symbol's whole position with an opposing market order. */
  async closePosition(accountId: string, symbol: string): Promise<PlaceResult> {
    const pool = getPool();
    const { rows } = await pool.query<PositionRow>(
      `SELECT "id","side","quantity","averagePrice" FROM "Position" WHERE "accountId" = $1 AND "symbol" = $2`,
      [accountId, symbol],
    );
    const pos = rows[0];
    if (!pos) return { ok: false, error: "No open position to close." };
    const side: ApiSide = pos.side === "LONG" ? "sell" : "buy";
    return this.place(accountId, { symbol, side, type: "market", quantity: Number(pos.quantity) });
  }

  /** Cancel a working (PENDING) limit/stop order owned by this account. */
  async cancel(accountId: string, orderId: string): Promise<PlaceResult> {
    const client = await getPool().connect();
    try {
      await client.query("BEGIN");
      const lock = await client.query<{ status: string; accountId: string; symbol: string; side: string; type: string; quantity: number }>(
        `SELECT "status","accountId","symbol","side","type","quantity" FROM "Order" WHERE "id" = $1 FOR UPDATE`,
        [orderId],
      );
      const row = lock.rows[0];
      // Don't reveal another account's orders — same response as not found.
      if (!row || row.accountId !== accountId) {
        await client.query("ROLLBACK");
        return { ok: false, error: "Order not found." };
      }
      if (row.status !== "PENDING") {
        await client.query("ROLLBACK");
        return { ok: false, error: `Order is ${row.status.toLowerCase()} and can no longer be cancelled.` };
      }

      await client.query(
        `UPDATE "Order" SET "status" = 'CANCELLED', "reason" = 'Cancelled by user', "updatedAt" = now() WHERE "id" = $1`,
        [orderId],
      );
      await this.log(client, accountId, "ORDER_CANCELLED", `${row.side.toLowerCase()} ${row.quantity} ${row.symbol} ${row.type.toLowerCase()} cancelled`);
      await client.query("COMMIT");

      this.accountStream.publishOrderUpdate(accountId, { id: orderId, status: "cancelled", symbol: row.symbol });
      return { ok: true, orderId, status: "CANCELLED" };
    } catch (err) {
      await client.query("ROLLBACK").catch(() => {});
      return { ok: false, error: (err as Error).message };
    } finally {
      client.release();
    }
  }

  // --- helpers -----------------------------------------------------

  private async getPosition(client: PoolClient, accountId: string, symbol: string): Promise<PositionRow | null> {
    const { rows } = await client.query<PositionRow>(
      `SELECT "id","side","quantity","averagePrice" FROM "Position"
       WHERE "accountId" = $1 AND "symbol" = $2 FOR UPDATE`,
      [accountId, symbol],
    );
    const p = rows[0];
    return p ? { ...p, quantity: Number(p.quantity), averagePrice: Number(p.averagePrice) } : null;
  }

  private netSizeAfter(existing: PositionRow | null, side: ApiSide, qty: number): number {
    const cur = existing ? (existing.side === "LONG" ? 1 : -1) * existing.quantity : 0;
    const order = (side === "buy" ? 1 : -1) * qty;
    return Math.abs(cur + order);
  }

  /** Net the fill into the position; returns realized P&L. Writes Position changes. */
  private async applyToPosition(
    client: PoolClient,
    accountId: string,
    symbol: string,
    side: ApiSide,
    qty: number,
    fillPrice: number,
    precision: number,
  ): Promise<number> {
    const existing = await this.getPosition(client, accountId, symbol);
    const round = (v: number) => Math.round(v * 10 ** precision) / 10 ** precision;

    if (!existing) {
      await client.query(
        `INSERT INTO "Position" ("accountId","symbol","side","quantity","averagePrice")
         VALUES ($1,$2,$3,$4,$5)`,
        [accountId, symbol, side === "buy" ? "LONG" : "SHORT", qty, round(fillPrice)],
      );
      return 0;
    }

    const sameDir = (existing.side === "LONG" && side === "buy") || (existing.side === "SHORT" && side === "sell");
    const dir = existing.side === "LONG" ? 1 : -1;

    if (sameDir) {
      const newQty = existing.quantity + qty;
      const newAvg = round((existing.averagePrice * existing.quantity + fillPrice * qty) / newQty);
      await client.query(
        `UPDATE "Position" SET "quantity" = $2, "averagePrice" = $3, "updatedAt" = now() WHERE "id" = $1`,
        [existing.id, newQty, newAvg],
      );
      return 0;
    }

    // Opposing order: reduce, close, or flip.
    const closedQty = Math.min(qty, existing.quantity);
    const realized = round((fillPrice - existing.averagePrice) * closedQty * dir);

    if (qty < existing.quantity) {
      await client.query(`UPDATE "Position" SET "quantity" = $2, "realizedPnl" = "realizedPnl" + $3, "updatedAt" = now() WHERE "id" = $1`, [
        existing.id,
        existing.quantity - qty,
        realized,
      ]);
    } else if (qty === existing.quantity) {
      await client.query(`DELETE FROM "Position" WHERE "id" = $1`, [existing.id]);
    } else {
      // Flip to the opposite side with the remainder.
      await client.query(
        `UPDATE "Position" SET "side" = $2, "quantity" = $3, "averagePrice" = $4, "realizedPnl" = "realizedPnl" + $5, "updatedAt" = now() WHERE "id" = $1`,
        [existing.id, side === "buy" ? "LONG" : "SHORT", qty - existing.quantity, round(fillPrice), realized],
      );
    }
    return realized;
  }

  /** Write the Fill, net it into the position, and book realized P&L. Returns realized. */
  private async settleFill(
    client: PoolClient,
    accountId: string,
    orderId: string,
    symbol: string,
    side: ApiSide,
    qty: number,
    fillPrice: number,
    precision: number,
  ): Promise<number> {
    await client.query(`INSERT INTO "Fill" ("orderId","quantity","price") VALUES ($1,$2,$3)`, [orderId, qty, fillPrice]);
    const realized = await this.applyToPosition(client, accountId, symbol, side, qty, fillPrice, precision);
    if (realized !== 0) {
      await client.query(
        `UPDATE "Account" SET "balance" = "balance" + $2, "totalPnl" = "totalPnl" + $2,
                "dailyPnl" = "dailyPnl" + $2, "updatedAt" = now() WHERE "id" = $1`,
        [accountId, realized],
      );
      await client.query(
        `INSERT INTO "Transaction" ("accountId","type","amount","description") VALUES ($1,'TRADE',$2,$3)`,
        [accountId, realized, `Realized P&L · ${symbol}`],
      );
    }
    return realized;
  }

  // --- resting-order monitor (limit/stop triggering) ---------------

  /** Scan all working orders and fill any whose trigger the live price has crossed. */
  private async scanWorkingOrders(): Promise<void> {
    if (this.scanning) return; // skip if the previous scan is still running
    this.scanning = true;
    try {
      const { rows } = await getPool().query<WorkingOrder>(
        `SELECT "id","accountId","symbol","side","type","quantity","requestedPrice"
         FROM "Order" WHERE "status" = 'PENDING' AND "type" IN ('LIMIT','STOP')`,
      );
      for (const o of rows) {
        const mark = this.accountStream.getMarkPrice(o.symbol);
        if (!mark || mark <= 0) continue;
        if (this.isTriggered(o, mark)) {
          await this.fillWorkingOrder(o, mark).catch((e) =>
            console.error("[engine] trigger fill failed:", (e as Error).message),
          );
        }
      }
    } catch (err) {
      console.error("[engine] working-order scan failed:", (err as Error).message);
    } finally {
      this.scanning = false;
    }
  }

  /** Has the live mark crossed this order's trigger?
   *   limit buy fills at/below its price, limit sell at/above;
   *   stop buy fires at/above its price, stop sell at/below. */
  private isTriggered(o: WorkingOrder, mark: number): boolean {
    const px = Number(o.requestedPrice);
    if (!(px > 0)) return false;
    const buy = o.side === "BUY";
    return o.type === "LIMIT" ? (buy ? mark <= px : mark >= px) : buy ? mark >= px : mark <= px;
  }

  /** Fill a triggered working order in its own transaction (idempotent under concurrent scans). */
  private async fillWorkingOrder(o: WorkingOrder, mark: number): Promise<void> {
    const inst = getInstrument(o.symbol);
    if (!inst) return;
    const side: ApiSide = o.side === "BUY" ? "buy" : "sell";
    const qty = Number(o.quantity);
    // A limit fills at its limit price; a stop fills at the market that triggered it.
    const fillPrice = o.type === "LIMIT" ? Number(o.requestedPrice) : mark;

    const client = await getPool().connect();
    try {
      await client.query("BEGIN");

      // Lock the order and re-check it's still working — a concurrent scan or a
      // manual cancel may have already settled it.
      const lock = await client.query<{ status: string }>(`SELECT "status" FROM "Order" WHERE "id" = $1 FOR UPDATE`, [o.id]);
      if (lock.rows[0]?.status !== "PENDING") {
        await client.query("ROLLBACK");
        return;
      }

      const rule = await client.query<{ maxContracts: number; status: string }>(
        `SELECT r."maxContracts", a."status" FROM "Account" a
         LEFT JOIN "Rule" r ON r."accountId" = a."id" WHERE a."id" = $1 FOR UPDATE OF a`,
        [o.accountId],
      );
      const acc = rule.rows[0];

      // Account no longer tradable → reject the resting order instead of looping forever.
      if (acc?.status && acc.status !== "ACTIVE") {
        await this.rejectOrder(client, o, `account ${acc.status}`);
        await client.query("COMMIT");
        this.accountStream.publishOrderUpdate(o.accountId, { id: o.id, status: "rejected", symbol: o.symbol });
        return;
      }

      const existing = await this.getPosition(client, o.accountId, o.symbol);
      if (acc?.maxContracts && this.netSizeAfter(existing, side, qty) > acc.maxContracts) {
        await this.rejectOrder(client, o, `exceeds max ${acc.maxContracts} contracts`);
        await client.query("COMMIT");
        this.accountStream.publishOrderUpdate(o.accountId, { id: o.id, status: "rejected", symbol: o.symbol });
        return;
      }

      await client.query(
        `UPDATE "Order" SET "status" = 'FILLED', "filledQuantity" = $2, "fillPrice" = $3, "updatedAt" = now() WHERE "id" = $1`,
        [o.id, qty, fillPrice],
      );
      await this.settleFill(client, o.accountId, o.id, o.symbol, side, qty, fillPrice, inst.pricePrecision);
      await this.log(client, o.accountId, "ORDER_FILLED", `${side} ${qty} ${o.symbol} @ ${fillPrice} (${o.type.toLowerCase()})`);
      await client.query("COMMIT");

      await this.accountStream.refreshAccount(o.accountId);
      this.accountStream.publishOrderUpdate(o.accountId, { id: o.id, status: "filled", symbol: o.symbol });
    } catch (err) {
      await client.query("ROLLBACK").catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  }

  private async rejectOrder(client: PoolClient, o: WorkingOrder, reason: string) {
    await client.query(`UPDATE "Order" SET "status" = 'REJECTED', "reason" = $2, "updatedAt" = now() WHERE "id" = $1`, [o.id, reason]);
    await this.log(client, o.accountId, "ORDER_REJECTED", `${o.side} ${o.quantity} ${o.symbol} ${o.type.toLowerCase()} — ${reason}`);
  }

  private async log(client: PoolClient, accountId: string, type: string, message: string) {
    await client.query(`INSERT INTO "ActivityLog" ("accountId","type","message") VALUES ($1,$2,$3)`, [
      accountId,
      type,
      message,
    ]);
  }
}
