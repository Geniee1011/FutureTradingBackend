import { BaseProvider, round, synthBook } from "./provider.js";
import { INSTRUMENTS, getInstrument } from "../instruments.js";
import type { Candle } from "../types.js";

interface SymState {
  price: number;
  open: number;
  high: number;
  low: number;
  volume: number;
}

const TICK_MS = 1000;

/** Random-walk market simulator. Runs with no API key so the stack works out of the box. */
export class SimulationProvider extends BaseProvider {
  readonly name = "simulation";
  private state = new Map<string, SymState>();
  private timer: NodeJS.Timeout | null = null;

  constructor() {
    super();
    for (const inst of INSTRUMENTS) {
      this.state.set(inst.symbol, {
        price: inst.simBase,
        open: inst.simBase,
        high: inst.simBase,
        low: inst.simBase,
        volume: 0,
      });
    }
  }

  start(): void {
    if (this.timer) return;
    this.timer = setInterval(() => this.tick(), TICK_MS);
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }

  private tick(): void {
    for (const inst of INSTRUMENTS) {
      if (!this.quoteSymbols.has(inst.symbol)) continue; // only subscribed symbols
      const st = this.state.get(inst.symbol)!;
      const reversion = (inst.simBase - st.price) * 0.002;
      const shock = (Math.random() - 0.5) * inst.simBase * 0.0012;
      st.price = Math.max(st.price + reversion + shock, inst.simBase * 0.5);
      st.high = Math.max(st.high, st.price);
      st.low = Math.min(st.low, st.price);
      st.volume += Math.floor(Math.random() * 50);

      const spread = inst.tickSize;
      this.emit("quote", {
        symbol: inst.symbol,
        price: round(st.price, inst.pricePrecision),
        bid: round(st.price - spread, inst.pricePrecision),
        ask: round(st.price + spread, inst.pricePrecision),
        change24h: (st.price - st.open) / st.open,
        high24h: round(st.high, inst.pricePrecision),
        low24h: round(st.low, inst.pricePrecision),
        volume24h: st.volume,
        ts: Date.now(),
      });

      if (this.bookSymbols.has(inst.symbol)) {
        this.emit("orderbook", synthBook(inst.symbol, st.price, inst.tickSize, inst.pricePrecision));
      }
    }
  }

  async getHistory(symbol: string, resolutionSec: number, count: number): Promise<Candle[]> {
    const inst = getInstrument(symbol);
    const seed = inst?.simBase ?? 100;
    const now = Math.floor(Date.now() / 1000);
    const start = now - (now % resolutionSec) - (count - 1) * resolutionSec;
    const out: Candle[] = [];
    let price = seed * 0.97;
    const vol = seed * 0.004;
    for (let i = 0; i < count; i++) {
      const open = price;
      const close = Math.max(open + (Math.random() - 0.48) * vol, seed * 0.5);
      const high = Math.max(open, close) + Math.random() * vol * 0.5;
      const low = Math.min(open, close) - Math.random() * vol * 0.5;
      const p = inst?.pricePrecision ?? 2;
      out.push({
        time: start + i * resolutionSec,
        open: round(open, p),
        high: round(high, p),
        low: round(low, p),
        close: round(close, p),
        volume: Math.floor(Math.random() * 500 + 50),
      });
      price = close;
    }
    return out;
  }
}
