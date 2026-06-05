/* Wire contracts shared with the frontend (TradingApp/src/lib/types.ts).
   Keep these in sync — the WebSocket and /api/history responses must match. */

export interface Quote {
  symbol: string;
  price: number;
  bid: number;
  ask: number;
  change24h: number; // fractional, e.g. 0.0123
  high24h: number;
  low24h: number;
  volume24h: number;
  ts: number; // epoch ms
}

export interface Candle {
  time: number; // epoch SECONDS (TradingView convention)
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface OrderBookLevel {
  price: number;
  size: number;
}

export interface OrderBook {
  symbol: string;
  bids: OrderBookLevel[];
  asks: OrderBookLevel[];
  ts: number;
}

export type ServerMessage =
  | { type: "quote"; data: Quote }
  | { type: "orderbook"; data: OrderBook };

export type ClientMessage =
  | { type: "subscribe"; channel: string; symbol?: string }
  | { type: "unsubscribe"; channel: string; symbol?: string };
