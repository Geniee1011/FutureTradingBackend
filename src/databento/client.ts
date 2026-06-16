/* Thin client for Databento's Historical HTTP API (timeseries.get_range).

   Auth: HTTP Basic, API key as the username, empty password.
   Prices are fixed-point int64 scaled by 1e-9 (actual = raw / 1_000_000_000);
   the value 2^63-1 is Databento's "undefined" sentinel.
   Timestamps (ts_event) are nanoseconds since the UNIX epoch.

   Docs: https://databento.com/docs/api-reference-historical/timeseries/timeseries-get-range */

const HIST_BASE = "https://hist.databento.com/v0";
const PRICE_SCALE = 1e9;
const UNDEF_PRICE = "9223372036854775807"; // INT64_MAX sentinel
// Default per-request budget. Light calls (symbology, metadata, fast quote poll)
// keep this; heavy historical fetches pass a longer one — Databento's first
// timeseries.get_range for a dataset has notable cold-start latency.
const REQUEST_TIMEOUT_MS = 10_000;
export const HISTORY_TIMEOUT_MS = 30_000;
/** Retries for heavy historical fetches (vs. 1 for the fast quote poll). */
export const HISTORY_RETRIES = 2;

/** fetch with a timeout and one retry, so a transient network blip self-heals. */
async function fetchWithRetry(
  url: string,
  init: RequestInit,
  retries = 1,
  timeoutMs = REQUEST_TIMEOUT_MS,
): Promise<Response> {
  let lastErr: unknown;
  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      return await fetch(url, { ...init, signal: AbortSignal.timeout(timeoutMs) });
    } catch (err) {
      lastErr = err;
      if (attempt < retries) await new Promise((r) => setTimeout(r, 400));
    }
  }
  throw lastErr;
}

export interface OhlcvRecord {
  ts: number; // epoch seconds
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface TradeRecord {
  ts: number; // epoch ms
  price: number;
  size: number;
}

interface RawHeader {
  ts_event: string | number;
  instrument_id?: number;
}

const RANGE_TTL_MS = 5_000;

export class DatabentoClient {
  /** Cached per-schema available-end timestamps (ms), refreshed every RANGE_TTL_MS. */
  private rangeCache: { at: number; ends: Record<string, number> } | null = null;
  private rangeInflight: Promise<Record<string, number>> | null = null;

  constructor(
    private readonly apiKey: string,
    private readonly dataset: string,
  ) {}

  private authHeader(): string {
    // username = apiKey, password = empty
    return "Basic " + Buffer.from(`${this.apiKey}:`).toString("base64");
  }

  /**
   * Latest timestamp (ms) for which `schema` has data. Databento rejects queries
   * whose `end` exceeds this, so all windows must be clamped to it. The metadata
   * call is free and cached briefly.
   */
  async availableEnd(schema: string): Promise<number> {
    const ends = await this.datasetEnds();
    return ends[schema] ?? Date.now();
  }

  private async datasetEnds(): Promise<Record<string, number>> {
    if (this.rangeCache && Date.now() - this.rangeCache.at < RANGE_TTL_MS) return this.rangeCache.ends;
    if (this.rangeInflight) return this.rangeInflight;

    this.rangeInflight = (async () => {
      const res = await fetchWithRetry(
        `${HIST_BASE}/metadata.get_dataset_range?dataset=${this.dataset}`,
        { headers: { Authorization: this.authHeader() } },
        HISTORY_RETRIES,
        HISTORY_TIMEOUT_MS,
      );
      if (!res.ok) throw new Error(`Databento range ${res.status}`);
      const body = (await res.json()) as { schema?: Record<string, { start: string; end: string }> };
      const ends: Record<string, number> = {};
      for (const [k, v] of Object.entries(body.schema ?? {})) ends[k] = parseIsoNs(v.end);
      this.rangeCache = { at: Date.now(), ends };
      return ends;
    })().finally(() => {
      this.rangeInflight = null;
    });

    return this.rangeInflight;
  }

  /** Raw NDJSON request against timeseries.get_range. Returns parsed records. */
  private async getRange(params: {
    symbols: string;
    schema: string;
    start: string;
    end?: string;
    limit?: number;
    stypeIn?: string;
    retries?: number;
    timeoutMs?: number;
  }): Promise<Record<string, unknown>[]> {
    const qs = new URLSearchParams({
      dataset: this.dataset,
      symbols: params.symbols,
      schema: params.schema,
      stype_in: params.stypeIn ?? "continuous",
      start: params.start,
      encoding: "json",
    });
    if (params.end) qs.set("end", params.end);
    if (params.limit) qs.set("limit", String(params.limit));

    const res = await fetchWithRetry(
      `${HIST_BASE}/timeseries.get_range?${qs.toString()}`,
      { headers: { Authorization: this.authHeader() } },
      params.retries,
      params.timeoutMs,
    );

    if (!res.ok) {
      const body = await res.text().catch(() => "");
      throw new Error(`Databento ${res.status} ${res.statusText}: ${body.slice(0, 200)}`);
    }

    const text = await res.text();
    const out: Record<string, unknown>[] = [];
    for (const line of text.split("\n")) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      try {
        out.push(JSON.parse(trimmed));
      } catch {
        /* skip partial/non-JSON lines */
      }
    }
    return out;
  }

  /**
   * Resolve continuous symbols (e.g. `ES.v.0`) to their dated raw contract codes
   * (e.g. `ESM6`) as of `dateMs`. Databento has no direct continuous→raw_symbol
   * mapping, so this is a two-step resolve: continuous → instrument_id →
   * raw_symbol. Returns a map keyed by the continuous symbol. This is the
   * authoritative code — e.g. the volume-active gold contract may be GCQ6 (Aug),
   * not the calendar-front GCM6.
   */
  async resolveContracts(dbSymbols: string[], dateMs: number): Promise<Record<string, string>> {
    const ids = await this.symbologyResolve(dbSymbols, "continuous", "instrument_id", dateMs);
    const idList = Object.values(ids);
    if (idList.length === 0) return {};
    const raws = await this.symbologyResolve(idList, "instrument_id", "raw_symbol", dateMs);
    const out: Record<string, string> = {};
    for (const [continuous, id] of Object.entries(ids)) {
      const raw = raws[id];
      if (raw) out[continuous] = raw;
    }
    return out;
  }

  /** Resolve continuous symbols → numeric instrument_id (for live stream mapping). */
  async resolveInstrumentIds(dbSymbols: string[], dateMs: number): Promise<Record<string, number>> {
    const ids = await this.symbologyResolve(dbSymbols, "continuous", "instrument_id", dateMs);
    const out: Record<string, number> = {};
    for (const [sym, id] of Object.entries(ids)) {
      const n = Number(id);
      if (Number.isFinite(n)) out[sym] = n;
    }
    return out;
  }

  private async symbologyResolve(
    symbols: string[],
    stypeIn: string,
    stypeOut: string,
    dateMs: number,
  ): Promise<Record<string, string>> {
    // Databento rejects symbology windows whose start_date is on/after the
    // dataset's available end (case: data_start_date_after_available_end_date).
    // "Now" becomes exactly that edge the moment the clock ticks into the
    // dataset's end day, so step the lookup back to the last available day.
    let capMs = dateMs - 86_400_000; // fallback: yesterday
    try {
      capMs = (await this.availableEnd("trades")) - 86_400_000;
    } catch {
      /* keep the yesterday fallback */
    }
    const baseMs = Math.min(dateMs, capMs);
    const date = new Date(baseMs).toISOString().slice(0, 10);
    const next = new Date(baseMs + 86_400_000).toISOString().slice(0, 10);
    const qs = new URLSearchParams({
      dataset: this.dataset,
      symbols: symbols.join(","),
      stype_in: stypeIn,
      stype_out: stypeOut,
      start_date: date,
      end_date: next,
    });
    // Critical for the live feed: this maps instrument-ids → symbols so incoming
    // trades can be decoded. Use the longer budget (not the 10s default) — on a
    // higher-latency host (e.g. a cloud region far from Databento) the short
    // timeout fails, the id-map stays empty, and every live trade is dropped.
    const res = await fetchWithRetry(
      `${HIST_BASE}/symbology.resolve?${qs.toString()}`,
      { headers: { Authorization: this.authHeader() } },
      HISTORY_RETRIES,
      HISTORY_TIMEOUT_MS,
    );
    if (!res.ok) {
      // Surface the body + the requested window — a 422 here is almost always a
      // date/symbol-range rejection (e.g. end_date past the dataset's edge).
      const body = await res.text().catch(() => "");
      throw new Error(`Databento symbology ${res.status} [${date}..${next}]: ${body.slice(0, 200)}`);
    }
    const body = (await res.json()) as { result?: Record<string, { s: string }[]> };
    const out: Record<string, string> = {};
    for (const [key, arr] of Object.entries(body.result ?? {})) {
      const s = arr?.[arr.length - 1]?.s;
      if (s) out[key] = s;
    }
    return out;
  }

  /** Fetch OHLCV bars. schema must be ohlcv-1s | ohlcv-1m | ohlcv-1h | ohlcv-1d. */
  async ohlcv(
    dbSymbol: string,
    schema: string,
    startMs: number,
    endMs: number,
    limit?: number,
    opts?: { retries?: number; timeoutMs?: number },
  ): Promise<OhlcvRecord[]> {
    // Clamp the window to available data (shift back, preserving width) so the
    // request never exceeds the dataset end (which Databento rejects with 422).
    const availEnd = await this.availableEnd(schema);
    const width = endMs - startMs;
    const end = Math.min(endMs, availEnd);
    const start = end - width;

    const records = await this.getRange({
      symbols: dbSymbol,
      schema,
      start: new Date(start).toISOString(),
      end: new Date(end).toISOString(),
      limit,
      retries: opts?.retries,
      timeoutMs: opts?.timeoutMs,
    });
    const out: OhlcvRecord[] = [];
    for (const r of records) {
      const hd = r.hd as RawHeader | undefined;
      if (!hd) continue;
      out.push({
        ts: Math.floor(toNs(hd.ts_event) / 1e9),
        open: scalePrice(r.open),
        high: scalePrice(r.high),
        low: scalePrice(r.low),
        close: scalePrice(r.close),
        volume: Number(r.volume ?? 0),
      });
    }
    return out;
  }

  /** Latest trades in a recent window; caller takes the last for the freshest price. */
  async recentTrades(dbSymbol: string, sinceMs: number, limit = 50): Promise<TradeRecord[]> {
    const end = await this.availableEnd("trades");
    const start = Math.min(sinceMs, end - 60_000);
    const records = await this.getRange({
      symbols: dbSymbol,
      schema: "trades",
      start: new Date(start).toISOString(),
      end: new Date(end).toISOString(),
      limit,
    });
    const out: TradeRecord[] = [];
    for (const r of records) {
      const hd = r.hd as RawHeader | undefined;
      if (!hd) continue;
      const price = scalePrice(r.price);
      if (!Number.isFinite(price)) continue;
      out.push({ ts: Math.floor(toNs(hd.ts_event) / 1e6), price, size: Number(r.size ?? 0) });
    }
    return out;
  }
}

/** Convert a fixed-point price field (string|number) to a float, or NaN if undefined. */
function scalePrice(v: unknown): number {
  if (v == null) return NaN;
  const s = String(v);
  if (s === UNDEF_PRICE) return NaN;
  const n = Number(s);
  return Number.isFinite(n) ? n / PRICE_SCALE : NaN;
}

/** Parse an ISO-8601 timestamp with nanosecond precision (".000000000Z") to ms. */
function parseIsoNs(s: string): number {
  // Date.parse only handles ms precision — truncate the fractional part to 3 digits.
  return Date.parse(s.replace(/(\.\d{3})\d*(Z|[+-]\d{2}:?\d{2})$/, "$1$2"));
}

/** Normalize a ts_event (ns as string|number, or ISO string) to nanoseconds. */
function toNs(v: string | number): number {
  if (typeof v === "number") return v;
  if (/^\d+$/.test(v)) return Number(v);
  const ms = Date.parse(v);
  return Number.isFinite(ms) ? ms * 1e6 : 0;
}
