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
}

export const INSTRUMENTS: Instrument[] = [
  { symbol: "ES", databentoSymbol: "ES.v.0", name: "E-mini S&P 500", category: "Equity Index", pricePrecision: 2, simBase: 7574, tickSize: 0.25 },
  { symbol: "MES", databentoSymbol: "MES.v.0", name: "Micro E-mini S&P 500", category: "Equity Index", pricePrecision: 2, simBase: 7574, tickSize: 0.25 },
  { symbol: "NQ", databentoSymbol: "NQ.v.0", name: "E-mini Nasdaq-100", category: "Equity Index", pricePrecision: 2, simBase: 30264, tickSize: 0.25 },
  { symbol: "MNQ", databentoSymbol: "MNQ.v.0", name: "Micro E-mini Nasdaq-100", category: "Equity Index", pricePrecision: 2, simBase: 30264, tickSize: 0.25 },
  { symbol: "YM", databentoSymbol: "YM.v.0", name: "E-mini Dow ($5)", category: "Equity Index", pricePrecision: 0, simBase: 47000, tickSize: 1 },
  { symbol: "MYM", databentoSymbol: "MYM.v.0", name: "Micro E-mini Dow ($0.50)", category: "Equity Index", pricePrecision: 0, simBase: 47000, tickSize: 1 },
  { symbol: "CL", databentoSymbol: "CL.v.0", name: "Crude Oil (WTI)", category: "Energy", pricePrecision: 2, simBase: 93, tickSize: 0.01 },
  { symbol: "MCL", databentoSymbol: "MCL.v.0", name: "Micro Crude Oil", category: "Energy", pricePrecision: 2, simBase: 93, tickSize: 0.01 },
  { symbol: "GC", databentoSymbol: "GC.v.0", name: "Gold", category: "Metals", pricePrecision: 1, simBase: 4488, tickSize: 0.1 },
  { symbol: "MGC", databentoSymbol: "MGC.v.0", name: "Micro Gold", category: "Metals", pricePrecision: 1, simBase: 4488, tickSize: 0.1 },
];

export const SYMBOLS = INSTRUMENTS.map((i) => i.symbol);

const bySymbol = new Map(INSTRUMENTS.map((i) => [i.symbol, i]));
const byDatabento = new Map(INSTRUMENTS.map((i) => [i.databentoSymbol, i]));

export function getInstrument(symbol: string): Instrument | undefined {
  return bySymbol.get(symbol);
}

export function getByDatabentoSymbol(dbSymbol: string): Instrument | undefined {
  return byDatabento.get(dbSymbol);
}
