/* Tradable futures and the mapping from our internal root code to Databento
   volume-based continuous symbology (most-active contract, auto-rolling).

   We trade by ROOT (ES, MES, …). Databento `.v.0` resolves the root to the
   actual most-actively-traded dated contract (e.g. ES → ESM6 = June 2026) and
   rolls it automatically — see `DatabentoClient.resolveContracts` for turning
   that back into the human contract code shown in the UI.

   Roots cover E-mini + Micro equity indices, energy, and metals. */

export type Category = "Equity Index" | "Energy" | "Metals";

export interface Instrument {
  symbol: string; // internal/display root code
  databentoSymbol: string; // volume-based continuous (most-active)
  name: string;
  category: Category;
  pricePrecision: number;
  /** Fallback seed price for simulation mode only. */
  simBase: number;
  /** Minimum tick size, used to build a synthetic depth ladder. */
  tickSize: number;
  /**
   * Contract point value in USD — dollars of P&L per 1.00 of price movement, per
   * contract. P&L = (price - avg) × qty × multiplier. e.g. ES = $50/pt, NQ = $20/pt.
   * Without this, dollar P&L (and every dollar-based eval rule) is wrong.
   */
  multiplier: number;
}

export const INSTRUMENTS: Instrument[] = [
  { symbol: "ES", databentoSymbol: "ES.v.0", name: "E-mini S&P 500", category: "Equity Index", pricePrecision: 2, simBase: 7574, tickSize: 0.25, multiplier: 50 },
  { symbol: "MES", databentoSymbol: "MES.v.0", name: "Micro E-mini S&P 500", category: "Equity Index", pricePrecision: 2, simBase: 7574, tickSize: 0.25, multiplier: 5 },
  { symbol: "NQ", databentoSymbol: "NQ.v.0", name: "E-mini Nasdaq-100", category: "Equity Index", pricePrecision: 2, simBase: 30264, tickSize: 0.25, multiplier: 20 },
  { symbol: "MNQ", databentoSymbol: "MNQ.v.0", name: "Micro E-mini Nasdaq-100", category: "Equity Index", pricePrecision: 2, simBase: 30264, tickSize: 0.25, multiplier: 2 },
  { symbol: "YM", databentoSymbol: "YM.v.0", name: "E-mini Dow ($5)", category: "Equity Index", pricePrecision: 0, simBase: 47000, tickSize: 1, multiplier: 5 },
  { symbol: "MYM", databentoSymbol: "MYM.v.0", name: "Micro E-mini Dow ($0.50)", category: "Equity Index", pricePrecision: 0, simBase: 47000, tickSize: 1, multiplier: 0.5 },
  { symbol: "CL", databentoSymbol: "CL.v.0", name: "Crude Oil (WTI)", category: "Energy", pricePrecision: 2, simBase: 93, tickSize: 0.01, multiplier: 1000 },
  { symbol: "MCL", databentoSymbol: "MCL.v.0", name: "Micro Crude Oil", category: "Energy", pricePrecision: 2, simBase: 93, tickSize: 0.01, multiplier: 100 },
  { symbol: "GC", databentoSymbol: "GC.v.0", name: "Gold", category: "Metals", pricePrecision: 1, simBase: 4488, tickSize: 0.1, multiplier: 100 },
  { symbol: "MGC", databentoSymbol: "MGC.v.0", name: "Micro Gold", category: "Metals", pricePrecision: 1, simBase: 4488, tickSize: 0.1, multiplier: 10 },
];

export const SYMBOLS = INSTRUMENTS.map((i) => i.symbol);

const bySymbol = new Map(INSTRUMENTS.map((i) => [i.symbol, i]));
const byDatabento = new Map(INSTRUMENTS.map((i) => [i.databentoSymbol, i]));

export function getInstrument(symbol: string): Instrument | undefined {
  return bySymbol.get(symbol);
}

/** Contract point value in USD for a symbol (defaults to 1 for unknown symbols). */
export function getMultiplier(symbol: string): number {
  return bySymbol.get(symbol)?.multiplier ?? 1;
}

export function getByDatabentoSymbol(dbSymbol: string): Instrument | undefined {
  return byDatabento.get(dbSymbol);
}

/**
 * Mini-equivalent weight for cross-instrument position-size enforcement.
 * 1 mini = 1.0 unit; 1 micro = 0.1 unit (they are exactly 10× smaller).
 * A limit of 3.0 mini-equivalents equals "3 minis OR 30 micros (or any combo)".
 */
const MICROS = new Set(["MES", "MNQ", "MYM", "MCL", "MGC"]);
export function miniEquivalent(symbol: string): number {
  return MICROS.has(symbol) ? 0.1 : 1.0;
}

/**
 * Micro alternative for a mini (ES→MES, NQ→MNQ, …).
 * Returns null for micros (already the smallest) and unknown symbols.
 */
const MINI_TO_MICRO: Record<string, string> = {
  ES: "MES", NQ: "MNQ", YM: "MYM", CL: "MCL", GC: "MGC",
};
export function getMicroAlternative(symbol: string): string | null {
  return MINI_TO_MICRO[symbol] ?? null;
}
