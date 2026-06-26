import { randomUUID } from "node:crypto";
import type { PoolClient } from "pg";
import { getPool } from "../db/pool.js";
import { useDatabase } from "../config.js";
import { getInstrument, getMultiplier, getMicroAlternative, miniEquivalent } from "../instruments.js";
import { isMarketOpen, MARKET_CLOSED_MESSAGE } from "./market-hours.js";
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
  /** Optional bracket: on entry fill, place an opposing stop (SL) / limit (TP), OCO-linked. */
  stopLoss?: number | null;
  takeProfit?: number | null;
}

/** Options for order placement; `bypassMarketHours` is for system/admin paths only. */
export interface PlaceOptions {
  bypassMarketHours?: boolean;
}

export interface PlaceResult {
  ok: boolean;
  error?: string;
  orderId?: string;
  status?: string;
  fillPrice?: number | null;
  realizedPnl?: number;
  /** Returned when the order is blocked by maxRiskPerTrade — shows the compliant micro alternative. */
  suggestion?: { symbol: string; quantity: number; risk: number };
}

interface PositionRow {
  id: string;
  side: "LONG" | "SHORT";
  quantity: number;
  averagePrice: number;
  openedAt?: Date;
}

interface WorkingOrder {
  id: string;
  accountId: string;
  symbol: string;
  side: "BUY" | "SELL";
  type: "LIMIT" | "STOP";
  quantity: number;
  requestedPrice: number;
  slPrice: number | null; // set on a bracket ENTRY order (creates exit legs on fill)
  tpPrice: number | null;
  ocoGroupId: string | null; // set on a bracket EXIT leg (one filling cancels its sibling)
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

  async place(accountId: string, input: PlaceOrderInput, opts?: PlaceOptions): Promise<PlaceResult> {
    const inst = getInstrument(input.symbol);
    if (!inst) return { ok: false, error: "Unknown instrument." };
    // Market-hours gate: no buying or selling while the exchange is closed.
    // Bypassed only for system/admin actions (risk liquidation, admin close-all).
    if (!opts?.bypassMarketHours && !isMarketOpen()) return { ok: false, error: MARKET_CLOSED_MESSAGE };
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

      // Pre-trade risk: allowed instruments + position limits + rule fields.
      const rule = await client.query<{
        allowedInstruments: string[];
        maxContracts: number;
        maxPositionUnits: number;
        maxRiskPerTrade: number;
        stopLossRequired: boolean;
        minHoldTimeSecs: number;
        status: string;
        tradingPausedAt: Date | null;
      }>(
        `SELECT r."allowedInstruments", r."maxContracts", r."maxPositionUnits",
                r."maxRiskPerTrade", r."stopLossRequired", r."minHoldTimeSecs",
                a."status", a."tradingPausedAt"
         FROM "Account" a LEFT JOIN "Rule" r ON r."accountId" = a."id"
         WHERE a."id" = $1 FOR UPDATE OF a`,
        [accountId],
      );
      const acc = rule.rows[0];
      if (acc?.status && acc.status !== "ACTIVE") {
        await client.query("ROLLBACK");
        return { ok: false, error: await disabledAccountMessage(accountId, acc.status) };
      }

      // Daily loss limit pause — clears automatically at the next day rollover.
      const today = new Date().toISOString().slice(0, 10);
      if (acc?.tradingPausedAt) {
        const pausedDate = new Date(acc.tradingPausedAt).toISOString().slice(0, 10);
        if (pausedDate === today) {
          await client.query("ROLLBACK");
          return { ok: false, error: "Daily loss limit reached. Trading is paused until the next trading day." };
        }
      }

      if (acc?.allowedInstruments?.length && !acc.allowedInstruments.includes(input.symbol)) {
        const detail = `${input.symbol} is not allowed on this account.`;
        await this.recordViolation(client, accountId, "RESTRICTED_INSTRUMENT", detail);
        await client.query("COMMIT"); // persist the violation (only SELECTs preceded it)
        return { ok: false, error: detail };
      }

      // Stop loss (and take profit) required before entering any position.
      const stopLossRequired = acc?.stopLossRequired ?? false;
      if (stopLossRequired && input.stopLoss == null) {
        await client.query("ROLLBACK");
        return { ok: false, error: "A stop loss is required before entering a position." };
      }
      if (stopLossRequired && input.takeProfit == null) {
        await client.query("ROLLBACK");
        return { ok: false, error: "A take profit is required before entering a position." };
      }

      const isMarket = input.type === "market";
      const fillPrice = isMarket ? mark! : (input.price as number);

      // Validate the bracket (SL/TP) against the entry reference price.
      const slPrice = input.stopLoss != null ? Number(input.stopLoss) : null;
      const tpPrice = input.takeProfit != null ? Number(input.takeProfit) : null;
      const bErr = bracketError(input.side, fillPrice, slPrice, tpPrice);
      if (bErr) {
        await client.query("ROLLBACK");
        return { ok: false, error: bErr };
      }

      // Max risk per trade: |entryPrice - slPrice| × qty × multiplier.
      // Only checked when a SL is provided (which is always the case when stopLossRequired).
      const maxRiskPerTrade = acc?.maxRiskPerTrade ?? 0;
      if (maxRiskPerTrade > 0 && slPrice != null) {
        const impliedRisk = Math.abs(fillPrice - slPrice) * input.quantity * inst.multiplier;
        if (impliedRisk > maxRiskPerTrade) {
          const suggestion = computeMicroSuggestion(input.symbol, fillPrice, slPrice, maxRiskPerTrade);
          await client.query("ROLLBACK");
          return {
            ok: false,
            error: `This order risks $${Math.round(impliedRisk)} — your limit is $${maxRiskPerTrade}.`,
            suggestion,
          };
        }
      }

      // Limit/stop → store as a working order; no fill yet. SL/TP ride along and
      // become exit legs when the entry fills (see fillWorkingOrder).
      if (!isMarket) {
        const ord = await client.query<{ id: string }>(
          `INSERT INTO "Order" ("accountId","symbol","side","type","quantity","requestedPrice","status","slPrice","tpPrice")
           VALUES ($1,$2,$3,$4,$5,$6,'PENDING',$7,$8) RETURNING "id"`,
          [accountId, input.symbol, input.side.toUpperCase(), input.type.toUpperCase(), input.quantity, input.price, slPrice, tpPrice],
        );
        await this.log(client, accountId, "ORDER_PLACEMENT", `${input.side} ${input.quantity} ${input.symbol} ${input.type} @ ${input.price}`);
        await client.query("COMMIT");
        const order = { id: ord.rows[0]!.id, status: "open" };
        this.accountStream.publishOrderUpdate(accountId, order);
        return { ok: true, orderId: order.id, status: "PENDING", fillPrice: null };
      }

      // Market order: check resulting cross-instrument position size (mini-equivalents).
      const maxPositionUnits = acc?.maxPositionUnits ?? 0;
      const existing = await this.getPosition(client, accountId, input.symbol);
      if (maxPositionUnits > 0) {
        const unitsAfter = await this.totalPositionUnitsAfter(client, accountId, input.symbol, input.side, input.quantity);
        if (unitsAfter > maxPositionUnits) {
          const detail = `Order would exceed the max position size (${maxPositionUnits} mini-equivalents across all instruments).`;
          await this.recordViolation(client, accountId, "CONTRACT_LIMIT_EXCEEDED", detail);
          await client.query("COMMIT"); // persist the violation
          return { ok: false, error: detail };
        }
      } else if (acc?.maxContracts) {
        const netAfter = this.netSizeAfter(existing, input.side, input.quantity);
        if (netAfter > acc.maxContracts) {
          const detail = `Order would exceed max position size (${acc.maxContracts} contracts).`;
          await this.recordViolation(client, accountId, "CONTRACT_LIMIT_EXCEEDED", detail);
          await client.query("COMMIT"); // persist the violation
          return { ok: false, error: detail };
        }
      }

      const minHoldTimeSecs = acc?.minHoldTimeSecs ?? 0;

      // Fill it: order + fill + position + account.
      const ord = await client.query<{ id: string }>(
        `INSERT INTO "Order" ("accountId","symbol","side","type","quantity","filledQuantity","fillPrice","status")
         VALUES ($1,$2,$3,'MARKET',$4,$4,$5,'FILLED') RETURNING "id"`,
        [accountId, input.symbol, input.side.toUpperCase(), input.quantity, fillPrice],
      );
      const orderId = ord.rows[0]!.id;
      const realized = await this.settleFill(
        client, accountId, orderId, input.symbol, input.side, input.quantity, fillPrice, inst.pricePrecision,
        minHoldTimeSecs,
      );
      await this.log(client, accountId, "ORDER_FILLED", `${input.side} ${input.quantity} ${input.symbol} @ ${fillPrice}`);

      // Attach the bracket: opposing stop (SL) + limit (TP), OCO-linked.
      if (slPrice != null || tpPrice != null) {
        await this.createBracketChildren(client, accountId, input.symbol, input.side, input.quantity, slPrice, tpPrice);
      }

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
  async closePosition(accountId: string, symbol: string, opts?: PlaceOptions): Promise<PlaceResult> {
    const pool = getPool();
    const { rows } = await pool.query<PositionRow>(
      `SELECT "id","side","quantity","averagePrice" FROM "Position" WHERE "accountId" = $1 AND "symbol" = $2`,
      [accountId, symbol],
    );
    const pos = rows[0];
    if (!pos) return { ok: false, error: "No open position to close." };
    const side: ApiSide = pos.side === "LONG" ? "sell" : "buy";
    const result = await this.place(accountId, { symbol, side, type: "market", quantity: Number(pos.quantity) }, opts);
    // Flat now → any resting SL/TP legs for this symbol are moot; cancel them.
    if (result.ok) await this.cancelBracketsForSymbol(accountId, symbol);
    return result;
  }

  /** Cancel any resting bracket (SL/TP) legs for an account's symbol. */
  private async cancelBracketsForSymbol(accountId: string, symbol: string): Promise<void> {
    const res = await getPool()
      .query(
        `UPDATE "Order" SET "status" = 'CANCELLED', "reason" = 'position closed', "updatedAt" = now()
         WHERE "accountId" = $1 AND "symbol" = $2 AND "status" = 'PENDING' AND "ocoGroupId" IS NOT NULL`,
        [accountId, symbol],
      )
      .catch(() => null);
    if (res?.rowCount) this.accountStream.publishOrderUpdate(accountId, { symbol, status: "cancelled" });
  }

  /** Create the opposing stop (SL) + limit (TP) exit legs for a just-opened position. */
  private async createBracketChildren(
    client: PoolClient,
    accountId: string,
    symbol: string,
    entrySide: ApiSide,
    qty: number,
    slPrice: number | null,
    tpPrice: number | null,
  ): Promise<void> {
    // One bracket per symbol — clear any existing resting legs first.
    await client.query(
      `UPDATE "Order" SET "status" = 'CANCELLED', "reason" = 'replaced by new bracket', "updatedAt" = now()
       WHERE "accountId" = $1 AND "symbol" = $2 AND "status" = 'PENDING' AND "ocoGroupId" IS NOT NULL`,
      [accountId, symbol],
    );
    const exitSide = entrySide === "buy" ? "SELL" : "BUY";
    const group = randomUUID();
    if (slPrice != null) {
      await client.query(
        `INSERT INTO "Order" ("accountId","symbol","side","type","quantity","requestedPrice","stopPrice","status","ocoGroupId")
         VALUES ($1,$2,$3,'STOP',$4,$5,$5,'PENDING',$6)`,
        [accountId, symbol, exitSide, qty, slPrice, group],
      );
    }
    if (tpPrice != null) {
      await client.query(
        `INSERT INTO "Order" ("accountId","symbol","side","type","quantity","requestedPrice","status","ocoGroupId")
         VALUES ($1,$2,$3,'LIMIT',$4,$5,'PENDING',$6)`,
        [accountId, symbol, exitSide, qty, tpPrice, group],
      );
    }
    const legs = [slPrice != null ? `SL ${slPrice}` : null, tpPrice != null ? `TP ${tpPrice}` : null].filter(Boolean).join(" · ");
    await this.log(client, accountId, "ORDER_PLACEMENT", `bracket ${exitSide.toLowerCase()} ${qty} ${symbol} (${legs})`);
  }

  /** Admin: flatten every open position at market. Returns how many were closed. */
  async closeAllPositions(accountId: string): Promise<number> {
    const { rows } = await getPool().query<{ symbol: string }>(`SELECT "symbol" FROM "Position" WHERE "accountId" = $1`, [accountId]);
    let closed = 0;
    for (const r of rows) {
      // Admin flatten must work regardless of session — bypass the market-hours gate.
      const res = await this.closePosition(accountId, r.symbol, { bypassMarketHours: true }).catch(() => ({ ok: false as const }));
      if (res.ok) closed += 1;
    }
    return closed;
  }

  /** Admin: cancel every working (PENDING) order. Returns how many were cancelled. */
  async cancelAllOrders(accountId: string): Promise<number> {
    const { rows } = await getPool().query<{ id: string }>(`SELECT "id" FROM "Order" WHERE "accountId" = $1 AND "status" = 'PENDING'`, [accountId]);
    let cancelled = 0;
    for (const r of rows) {
      const res = await this.cancel(accountId, r.id).catch(() => ({ ok: false as const }));
      if (res.ok) cancelled += 1;
    }
    return cancelled;
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

  /**
   * Modify a working (PENDING) order without filling it — used by the chart's
   * drag-to-edit. `price` moves the order's own level (entry, or a bracket exit
   * leg). `stopLoss`/`takeProfit` move the attached bracket on an ENTRY order
   * (pass null to remove a leg); they don't apply to an exit leg, which has no
   * bracket of its own. Each field is optional — only the dragged one is sent.
   */
  async modify(
    accountId: string,
    orderId: string,
    changes: { price?: number | null; stopLoss?: number | null; takeProfit?: number | null },
  ): Promise<PlaceResult> {
    const client = await getPool().connect();
    try {
      await client.query("BEGIN");
      const lock = await client.query<{
        status: string; accountId: string; symbol: string; side: string; type: string;
        requestedPrice: string | null; ocoGroupId: string | null; slPrice: string | null; tpPrice: string | null;
      }>(
        `SELECT "status","accountId","symbol","side","type","requestedPrice","ocoGroupId","slPrice","tpPrice"
         FROM "Order" WHERE "id" = $1 FOR UPDATE`,
        [orderId],
      );
      const row = lock.rows[0];
      if (!row || row.accountId !== accountId) {
        await client.query("ROLLBACK");
        return { ok: false, error: "Order not found." };
      }
      if (row.status !== "PENDING") {
        await client.query("ROLLBACK");
        return { ok: false, error: `Order is ${row.status.toLowerCase()} and can no longer be modified.` };
      }

      // The order's own price level (entry order, or a bracket exit leg).
      let newPrice = row.requestedPrice != null ? Number(row.requestedPrice) : null;
      if (changes.price !== undefined) {
        if (!(typeof changes.price === "number" && changes.price > 0)) {
          await client.query("ROLLBACK");
          return { ok: false, error: "A valid price is required." };
        }
        newPrice = changes.price;
      }

      const isExitLeg = row.ocoGroupId != null;
      const touchesBracket = changes.stopLoss !== undefined || changes.takeProfit !== undefined;
      if (touchesBracket && isExitLeg) {
        await client.query("ROLLBACK");
        return { ok: false, error: "This order has no attached bracket to modify." };
      }

      let newSl = row.slPrice != null ? Number(row.slPrice) : null;
      let newTp = row.tpPrice != null ? Number(row.tpPrice) : null;
      if (changes.stopLoss !== undefined) newSl = changes.stopLoss == null ? null : Number(changes.stopLoss);
      if (changes.takeProfit !== undefined) newTp = changes.takeProfit == null ? null : Number(changes.takeProfit);

      // Validate the bracket against the (new) entry price on an entry order.
      if (!isExitLeg && newPrice != null) {
        const bErr = bracketError(row.side.toLowerCase() as ApiSide, newPrice, newSl, newTp);
        if (bErr) {
          await client.query("ROLLBACK");
          return { ok: false, error: bErr };
        }
      }

      // A STOP order keeps its trigger in stopPrice too — keep it in sync on a move.
      const isStop = row.type === "STOP";
      await client.query(
        `UPDATE "Order"
           SET "requestedPrice" = $2,
               "stopPrice" = CASE WHEN $3 THEN $2 ELSE "stopPrice" END,
               "slPrice" = $4, "tpPrice" = $5, "updatedAt" = now()
         WHERE "id" = $1`,
        [orderId, newPrice, isStop, newSl, newTp],
      );
      await this.log(
        client, accountId, "ORDER_MODIFIED",
        `${row.side.toLowerCase()} ${row.symbol} ${row.type.toLowerCase()} → ${newPrice}${touchesBracket ? ` (SL ${newSl ?? "—"} / TP ${newTp ?? "—"})` : ""}`,
      );
      await client.query("COMMIT");

      this.accountStream.publishOrderUpdate(accountId, { id: orderId, status: "open", symbol: row.symbol });
      return { ok: true, orderId, status: "PENDING", fillPrice: null };
    } catch (err) {
      await client.query("ROLLBACK").catch(() => {});
      return { ok: false, error: (err as Error).message };
    } finally {
      client.release();
    }
  }

  /**
   * Attach / update / remove a protective bracket on an OPEN position (chart "+SL/+TP"
   * on the position line). Re-creates the OCO exit legs, preserving whichever leg isn't
   * being changed. `null` removes a leg; both null cancels the bracket entirely.
   */
  async setPositionBracket(
    accountId: string,
    symbol: string,
    changes: { stopLoss?: number | null; takeProfit?: number | null },
  ): Promise<PlaceResult> {
    const client = await getPool().connect();
    try {
      await client.query("BEGIN");
      const posRes = await client.query<{ side: string; quantity: number; averagePrice: string }>(
        `SELECT "side","quantity","averagePrice" FROM "Position" WHERE "accountId" = $1 AND "symbol" = $2 FOR UPDATE`,
        [accountId, symbol],
      );
      const pos = posRes.rows[0];
      if (!pos) {
        await client.query("ROLLBACK");
        return { ok: false, error: "No open position to attach a bracket to." };
      }
      const qty = Number(pos.quantity);
      const avg = Number(pos.averagePrice);
      const entrySide: ApiSide = pos.side === "LONG" ? "buy" : "sell"; // exit legs orient off this

      // Preserve the leg that isn't being changed.
      const legRes = await client.query<{ type: string; requestedPrice: string }>(
        `SELECT "type","requestedPrice" FROM "Order"
         WHERE "accountId" = $1 AND "symbol" = $2 AND "status" = 'PENDING' AND "ocoGroupId" IS NOT NULL`,
        [accountId, symbol],
      );
      let curSl: number | null = null;
      let curTp: number | null = null;
      for (const l of legRes.rows) {
        if (l.type === "STOP") curSl = Number(l.requestedPrice);
        else if (l.type === "LIMIT") curTp = Number(l.requestedPrice);
      }
      const newSl = changes.stopLoss !== undefined ? (changes.stopLoss == null ? null : Number(changes.stopLoss)) : curSl;
      const newTp = changes.takeProfit !== undefined ? (changes.takeProfit == null ? null : Number(changes.takeProfit)) : curTp;

      if (newSl == null && newTp == null) {
        await client.query(
          `UPDATE "Order" SET "status" = 'CANCELLED', "reason" = 'bracket removed', "updatedAt" = now()
           WHERE "accountId" = $1 AND "symbol" = $2 AND "status" = 'PENDING' AND "ocoGroupId" IS NOT NULL`,
          [accountId, symbol],
        );
        await client.query("COMMIT");
        this.accountStream.publishOrderUpdate(accountId, { symbol, status: "cancelled" });
        return { ok: true };
      }

      const bErr = bracketError(entrySide, avg, newSl, newTp);
      if (bErr) {
        await client.query("ROLLBACK");
        return { ok: false, error: bErr };
      }
      await this.createBracketChildren(client, accountId, symbol, entrySide, qty, newSl, newTp);
      await client.query("COMMIT");
      this.accountStream.publishOrderUpdate(accountId, { symbol, status: "open" });
      return { ok: true };
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
      `SELECT "id","side","quantity","averagePrice","openedAt" FROM "Position"
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

  /**
   * Sum of |position size × miniEquivalent| across ALL instruments for this account,
   * applying the net change that `side qty` would produce on `symbol`.
   * Used for cross-instrument position-size enforcement (maxPositionUnits).
   */
  private async totalPositionUnitsAfter(
    client: PoolClient,
    accountId: string,
    symbol: string,
    side: ApiSide,
    qty: number,
  ): Promise<number> {
    const { rows } = await client.query<{ symbol: string; side: string; quantity: number }>(
      `SELECT "symbol","side","quantity" FROM "Position" WHERE "accountId" = $1`,
      [accountId],
    );
    // Sum existing positions (excluding the one being changed, added back below).
    let total = 0;
    for (const p of rows) {
      if (p.symbol === symbol) continue; // handled separately
      total += Number(p.quantity) * miniEquivalent(p.symbol);
    }
    // Apply the net effect of the new order on this symbol.
    const existing = rows.find((p) => p.symbol === symbol);
    const curNet = existing ? (existing.side === "LONG" ? 1 : -1) * Number(existing.quantity) : 0;
    const orderNet = (side === "buy" ? 1 : -1) * qty;
    const newNet = Math.abs(curNet + orderNet);
    total += newNet * miniEquivalent(symbol);
    return total;
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
    minHoldTimeSecs = 0,
  ): Promise<number> {
    const existing = await this.getPosition(client, accountId, symbol);
    const round = (v: number) => Math.round(v * 10 ** precision) / 10 ** precision;

    if (!existing) {
      await client.query(
        `INSERT INTO "Position" ("accountId","symbol","side","quantity","averagePrice")
         VALUES ($1,$2,$3,$4,$5)`,
        [accountId, symbol, side === "buy" ? "LONG" : "SHORT", qty, round(fillPrice)],
      );
      // First lot of a brand-new position (admin CRM lists trades separately).
      await this.insertLot(client, accountId, symbol, side, qty, round(fillPrice));
      await this.log(client, accountId, "POSITION_OPENED", `${side === "buy" ? "long" : "short"} ${qty} ${symbol} @ ${round(fillPrice)}`);
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
      // A scale-in is a SEPARATE trade in the CRM — record its own lot (not merged
      // into the existing one), even though the netted Position line above averages them.
      await this.insertLot(client, accountId, symbol, side, qty, round(fillPrice));
      return 0;
    }

    // Opposing order: reduce, close, or flip. Close the oldest OPEN lots first (FIFO),
    // booking a SEPARATE ClosedPosition per lot — each at its own entry price + open time —
    // so the closed-trades log lists every entry trade individually instead of one blended
    // row. Realized P&L is summed across the lots closed (dollars: includes the contract
    // point value, ES=$50/pt, NQ=$20/pt, …, rounded to cents).
    const closedQty = Math.min(qty, existing.quantity);
    const realized = await this.closeLotsFifo(
      client, accountId, symbol, existing.side, closedQty, fillPrice,
      getMultiplier(symbol), existing.averagePrice, existing.openedAt,
      minHoldTimeSecs,
    );

    if (qty < existing.quantity) {
      await client.query(`UPDATE "Position" SET "quantity" = $2, "realizedPnl" = "realizedPnl" + $3, "updatedAt" = now() WHERE "id" = $1`, [
        existing.id,
        existing.quantity - qty,
        realized,
      ]);
    } else if (qty === existing.quantity) {
      await client.query(`DELETE FROM "Position" WHERE "id" = $1`, [existing.id]); // lots fully booked above
      await this.log(client, accountId, "POSITION_CLOSED", `${symbol} flat (realized ${realized})`);
    } else {
      // Flip: the old side is fully closed (its lots booked above); the remainder opens a
      // fresh lot on the new side.
      await client.query(
        `UPDATE "Position" SET "side" = $2, "quantity" = $3, "averagePrice" = $4, "realizedPnl" = "realizedPnl" + $5, "updatedAt" = now() WHERE "id" = $1`,
        [existing.id, side === "buy" ? "LONG" : "SHORT", qty - existing.quantity, round(fillPrice), realized],
      );
      await this.insertLot(client, accountId, symbol, side, qty - existing.quantity, round(fillPrice));
      await this.log(client, accountId, "POSITION_CLOSED", `${existing.side.toLowerCase()} ${symbol} closed (realized ${realized})`);
      await this.log(client, accountId, "POSITION_OPENED", `${side === "buy" ? "long" : "short"} ${qty - existing.quantity} ${symbol} @ ${round(fillPrice)}`);
    }
    return realized;
  }

  /** Record one open lot — a single entry trade — for the per-trade admin CRM view. */
  private async insertLot(
    client: PoolClient,
    accountId: string,
    symbol: string,
    side: ApiSide,
    qty: number,
    entryPrice: number,
  ): Promise<void> {
    await client.query(
      `INSERT INTO "PositionLot" ("accountId","symbol","side","quantity","entryPrice") VALUES ($1,$2,$3,$4,$5)`,
      [accountId, symbol, side === "buy" ? "LONG" : "SHORT", qty, entryPrice],
    );
  }

  /**
   * Close `closeQty` of a position against its OPEN lots, oldest first (FIFO), booking a
   * SEPARATE ClosedPosition per lot — each at its own entry price + open time — so the
   * closed-trades log lists every entry trade individually instead of one blended row.
   * Consumes (reduces/deletes) the lots and returns the total realized P&L (dollars).
   * `fallback*` cover the defensive case where lots don't fully back the position (e.g.
   * pre-lot-tracking drift) so a closed record is never dropped.
   */
  private async closeLotsFifo(
    client: PoolClient,
    accountId: string,
    symbol: string,
    side: "LONG" | "SHORT",
    closeQty: number,
    fillPrice: number,
    multiplier: number,
    fallbackEntry: number,
    fallbackOpenedAt: Date | undefined,
    minHoldTimeSecs = 0,
  ): Promise<number> {
    const dir = side === "LONG" ? 1 : -1;
    const now = Date.now();
    const realizedOf = (entry: number, q: number, openedAt: Date | undefined) => {
      const pnl = Math.round((fillPrice - entry) * q * dir * multiplier * 100) / 100;
      // Min-hold-time rule: if a trade is closed with profit before the minimum hold
      // time, the profit does not count. A loss is always real.
      if (minHoldTimeSecs > 0 && pnl > 0 && openedAt) {
        const heldMs = now - new Date(openedAt).getTime();
        if (heldMs < minHoldTimeSecs * 1000) return 0;
      }
      return pnl;
    };
    let remaining = closeQty;
    let total = 0;
    const { rows } = await client.query<{ id: string; quantity: number; entryPrice: string; openedAt: Date }>(
      `SELECT "id","quantity","entryPrice","openedAt" FROM "PositionLot"
       WHERE "accountId" = $1 AND "symbol" = $2 ORDER BY "openedAt" ASC, "id" ASC`,
      [accountId, symbol],
    );
    for (const lot of rows) {
      if (remaining <= 0) break;
      const lotQty = Number(lot.quantity);
      const take = Math.min(remaining, lotQty);
      const entry = Number(lot.entryPrice);
      const realized = realizedOf(entry, take, lot.openedAt);
      total += realized;
      // One closed-trade row per lot — its real entry price + open time, not the blend.
      await this.recordClosedPosition(client, accountId, symbol, side, take, entry, fillPrice, realized, lot.openedAt);
      if (take >= lotQty) {
        await client.query(`DELETE FROM "PositionLot" WHERE "id" = $1`, [lot.id]);
      } else {
        await client.query(`UPDATE "PositionLot" SET "quantity" = "quantity" - $2 WHERE "id" = $1`, [lot.id, take]);
      }
      remaining -= take;
    }
    // Defensive: lots didn't fully back the position — book the remainder at the position
    // average so the closed-trades log and realized P&L stay complete.
    if (remaining > 0) {
      const realized = realizedOf(fallbackEntry, remaining, fallbackOpenedAt);
      total += realized;
      await this.recordClosedPosition(client, accountId, symbol, side, remaining, fallbackEntry, fillPrice, realized, fallbackOpenedAt);
    }
    return Math.round(total * 100) / 100;
  }

  /** Append a closed-position record (one per realizing close/reduce/flip). */
  private async recordClosedPosition(
    client: PoolClient,
    accountId: string,
    symbol: string,
    side: "LONG" | "SHORT",
    quantity: number,
    entryPrice: number,
    exitPrice: number,
    realizedPnl: number,
    openedAt: Date | undefined,
  ): Promise<void> {
    await client.query(
      `INSERT INTO "ClosedPosition"
         ("accountId","symbol","side","quantity","entryPrice","exitPrice","realizedPnl","openedAt")
       VALUES ($1,$2,$3,$4,$5,$6,$7, COALESCE($8, now()))`,
      [accountId, symbol, side, quantity, entryPrice, exitPrice, realizedPnl, openedAt ?? null],
    );
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
    minHoldTimeSecs = 0,
  ): Promise<number> {
    await client.query(`INSERT INTO "Fill" ("orderId","quantity","price") VALUES ($1,$2,$3)`, [orderId, qty, fillPrice]);
    const realized = await this.applyToPosition(client, accountId, symbol, side, qty, fillPrice, precision, minHoldTimeSecs);
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
    if (!isMarketOpen()) return; // market closed → the mark is stale; don't trigger fills
    this.scanning = true;
    try {
      const { rows } = await getPool().query<WorkingOrder>(
        `SELECT "id","accountId","symbol","side","type","quantity","requestedPrice","slPrice","tpPrice","ocoGroupId"
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

      const rule = await client.query<{
        maxContracts: number;
        maxPositionUnits: number;
        minHoldTimeSecs: number;
        status: string;
        tradingPausedAt: Date | null;
      }>(
        `SELECT r."maxContracts", r."maxPositionUnits", r."minHoldTimeSecs",
                a."status", a."tradingPausedAt"
         FROM "Account" a LEFT JOIN "Rule" r ON r."accountId" = a."id" WHERE a."id" = $1 FOR UPDATE OF a`,
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

      // Daily loss limit pause active — reject (not cancel) so the order stays visible.
      const today = new Date().toISOString().slice(0, 10);
      if (acc?.tradingPausedAt) {
        const pausedDate = new Date(acc.tradingPausedAt).toISOString().slice(0, 10);
        if (pausedDate === today) {
          await this.rejectOrder(client, o, "daily loss limit: trading paused until tomorrow");
          await client.query("COMMIT");
          this.accountStream.publishOrderUpdate(o.accountId, { id: o.id, status: "rejected", symbol: o.symbol });
          return;
        }
      }

      const maxPositionUnits = acc?.maxPositionUnits ?? 0;
      if (maxPositionUnits > 0) {
        const unitsAfter = await this.totalPositionUnitsAfter(client, o.accountId, o.symbol, side, qty);
        if (unitsAfter > maxPositionUnits) {
          await this.rejectOrder(client, o, `exceeds max ${maxPositionUnits} mini-equivalent units`, "CONTRACT_LIMIT_EXCEEDED");
          await client.query("COMMIT");
          this.accountStream.publishOrderUpdate(o.accountId, { id: o.id, status: "rejected", symbol: o.symbol });
          return;
        }
      } else if (acc?.maxContracts) {
        const existing = await this.getPosition(client, o.accountId, o.symbol);
        if (this.netSizeAfter(existing, side, qty) > acc.maxContracts) {
          await this.rejectOrder(client, o, `exceeds max ${acc.maxContracts} contracts`, "CONTRACT_LIMIT_EXCEEDED");
          await client.query("COMMIT");
          this.accountStream.publishOrderUpdate(o.accountId, { id: o.id, status: "rejected", symbol: o.symbol });
          return;
        }
      }

      const minHoldTimeSecs = acc?.minHoldTimeSecs ?? 0;

      await client.query(
        `UPDATE "Order" SET "status" = 'FILLED', "filledQuantity" = $2, "fillPrice" = $3, "updatedAt" = now() WHERE "id" = $1`,
        [o.id, qty, fillPrice],
      );
      await this.settleFill(client, o.accountId, o.id, o.symbol, side, qty, fillPrice, inst.pricePrecision, minHoldTimeSecs);
      await this.log(client, o.accountId, "ORDER_FILLED", `${side} ${qty} ${o.symbol} @ ${fillPrice} (${o.type.toLowerCase()})`);

      // OCO: this leg filled → cancel its sibling. (A bracket exit leg has an ocoGroupId.)
      if (o.ocoGroupId) {
        await client.query(
          `UPDATE "Order" SET "status" = 'CANCELLED', "reason" = 'OCO: sibling filled', "updatedAt" = now()
           WHERE "ocoGroupId" = $1 AND "id" <> $2 AND "status" = 'PENDING'`,
          [o.ocoGroupId, o.id],
        );
      }
      // A bracket ENTRY (limit/stop with SL/TP) just filled → spawn its exit legs.
      const slPrice = o.slPrice != null ? Number(o.slPrice) : null;
      const tpPrice = o.tpPrice != null ? Number(o.tpPrice) : null;
      if (slPrice != null || tpPrice != null) {
        await this.createBracketChildren(client, o.accountId, o.symbol, side, qty, slPrice, tpPrice);
      }

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

  private async rejectOrder(client: PoolClient, o: WorkingOrder, reason: string, violationType?: string) {
    await client.query(`UPDATE "Order" SET "status" = 'REJECTED', "reason" = $2, "updatedAt" = now() WHERE "id" = $1`, [o.id, reason]);
    if (violationType) {
      await client.query(`INSERT INTO "Violation" ("accountId","type","action","detail") VALUES ($1,$2,'REJECT_ORDER',$3)`, [o.accountId, violationType, `${o.symbol} ${o.type.toLowerCase()}: ${reason}`]);
    }
    await this.log(client, o.accountId, "ORDER_REJECTED", `${o.side} ${o.quantity} ${o.symbol} ${o.type.toLowerCase()} — ${reason}`);
  }

  private async log(client: PoolClient, accountId: string, type: string, message: string) {
    await client.query(`INSERT INTO "ActivityLog" ("accountId","type","message") VALUES ($1,$2,$3)`, [
      accountId,
      type,
      message,
    ]);
  }

  /** Record a pre-trade rule rejection as a Violation + ORDER_REJECTED log (action = reject). */
  private async recordViolation(client: PoolClient, accountId: string, type: string, detail: string) {
    await client.query(`INSERT INTO "Violation" ("accountId","type","action","detail") VALUES ($1,$2,'REJECT_ORDER',$3)`, [accountId, type, detail]);
    await this.log(client, accountId, "ORDER_REJECTED", detail);
  }
}

/**
 * Build a specific "trading disabled" message for a non-ACTIVE account — includes
 * the actual breach (e.g. "Drawdown $3,034 reached limit $3,000") so the trader
 * understands WHY, instead of a bare "Account is FAILED".
 */
async function disabledAccountMessage(accountId: string, status: string): Promise<string> {
  let detail: string | null = null;
  try {
    const v = await getPool().query<{ detail: string }>(
      `SELECT "detail" FROM "Violation" WHERE "accountId" = $1 ORDER BY "createdAt" DESC LIMIT 1`,
      [accountId],
    );
    detail = v.rows[0]?.detail ?? null;
  } catch {
    /* fall back to the generic message */
  }
  if (status === "FAILED") {
    return detail
      ? `Account failed evaluation — ${detail}. Trading is disabled; contact your administrator to reset.`
      : "Account failed evaluation — trading is disabled; contact your administrator to reset.";
  }
  if (status === "PASSED") return "Account has passed the evaluation — trading is closed.";
  return `Account is ${status.toLowerCase()} — trading is disabled.`;
}

/**
 * Validate a bracket against the entry side/price. A protective stop sits adverse
 * to the entry and the target favourable: BUY → SL below / TP above; SELL → the
 * reverse. Returns an error string, or null if the bracket is valid (or absent).
 */
function bracketError(side: ApiSide, ref: number, sl: number | null, tp: number | null): string | null {
  const buy = side === "buy";
  if (sl != null) {
    if (!(sl > 0)) return "Stop loss must be a positive price.";
    if (buy ? sl >= ref : sl <= ref) return `Stop loss must be ${buy ? "below" : "above"} the entry price.`;
  }
  if (tp != null) {
    if (!(tp > 0)) return "Take profit must be a positive price.";
    if (buy ? tp <= ref : tp >= ref) return `Take profit must be ${buy ? "above" : "below"} the entry price.`;
  }
  return null;
}

/**
 * Compute the micro-contract alternative when a mini order exceeds maxRiskPerTrade.
 * Returns { symbol, quantity, risk } for the micro with the largest qty that fits,
 * or null if the symbol has no micro alternative or if even 1 micro exceeds the limit.
 */
function computeMicroSuggestion(
  symbol: string,
  fillPrice: number,
  slPrice: number,
  maxRisk: number,
): PlaceResult["suggestion"] {
  const microSymbol = getMicroAlternative(symbol);
  if (!microSymbol) return undefined;
  const distance = Math.abs(fillPrice - slPrice);
  if (distance === 0) return undefined;
  const microMultiplier = getMultiplier(microSymbol);
  const riskPer1 = distance * microMultiplier;
  if (riskPer1 <= 0) return undefined;
  const qty = Math.floor(maxRisk / riskPer1);
  if (qty <= 0) return undefined;
  return { symbol: microSymbol, quantity: qty, risk: Math.round(qty * riskPer1 * 100) / 100 };
}
