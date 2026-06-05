import type { WebSocket } from "ws";
import type { MarketDataProvider } from "../providers/provider.js";
import type { ClientMessage, ServerMessage } from "../types.js";
import { SYMBOLS } from "../instruments.js";

interface ClientState {
  quoteSymbols: Set<string>; // quote subscriptions (per symbol)
  books: Set<string>; // order-book symbols
}

/**
 * Central market hub: owns the provider, tracks per-client subscriptions, and
 * fans provider events out to interested clients. Aggregates order-book demand
 * so the provider only streams books someone is watching.
 */
export class MarketHub {
  private clients = new Map<WebSocket, ClientState>();

  constructor(private readonly provider: MarketDataProvider) {}

  start(): void {
    this.provider.on("quote", (q) =>
      this.broadcast({ type: "quote", data: q }, (c) => c.quoteSymbols.has(q.symbol)),
    );
    this.provider.on("orderbook", (b) =>
      this.broadcast({ type: "orderbook", data: b }, (c) => c.books.has(b.symbol)),
    );
    this.provider.start();
  }

  stop(): void {
    this.provider.stop();
  }

  addClient(ws: WebSocket): void {
    this.clients.set(ws, { quoteSymbols: new Set(), books: new Set() });
  }

  removeClient(ws: WebSocket): void {
    this.clients.delete(ws);
    this.recomputeQuotes();
    this.recomputeBooks();
  }

  handleMessage(ws: WebSocket, raw: string): void {
    let msg: ClientMessage;
    try {
      msg = JSON.parse(raw) as ClientMessage;
    } catch {
      return;
    }
    const state = this.clients.get(ws);
    if (!state) return;

    if (msg.type === "subscribe") {
      if (msg.channel === "quotes") {
        if (msg.symbol) state.quoteSymbols.add(msg.symbol);
        else for (const s of SYMBOLS) state.quoteSymbols.add(s); // no symbol = all
        this.recomputeQuotes();
      } else if (msg.channel === "orderbook" && msg.symbol) {
        state.books.add(msg.symbol);
        this.recomputeBooks();
      }
    } else if (msg.type === "unsubscribe") {
      if (msg.channel === "quotes") {
        if (msg.symbol) state.quoteSymbols.delete(msg.symbol);
        else state.quoteSymbols.clear();
        this.recomputeQuotes();
      } else if (msg.channel === "orderbook" && msg.symbol) {
        state.books.delete(msg.symbol);
        this.recomputeBooks();
      }
    }
  }

  getProvider(): MarketDataProvider {
    return this.provider;
  }

  clientCount(): number {
    return this.clients.size;
  }

  private recomputeQuotes(): void {
    const union = new Set<string>();
    for (const state of this.clients.values()) for (const s of state.quoteSymbols) union.add(s);
    this.provider.setQuoteSymbols(union);
  }

  private recomputeBooks(): void {
    const union = new Set<string>();
    for (const state of this.clients.values()) for (const s of state.books) union.add(s);
    this.provider.setBookSymbols(union);
  }

  private broadcast(msg: ServerMessage, want: (c: ClientState) => boolean): void {
    const payload = JSON.stringify(msg);
    for (const [ws, state] of this.clients) {
      if (want(state) && ws.readyState === ws.OPEN) ws.send(payload);
    }
  }
}
