/* Minimal streaming decoder for Databento Binary Encoding (DBN).

   We parse two record types:
   - the metadata block (magic "DBN" + version + uint32 length) is skipped
   - each record's 16-byte header (length, rtype, publisher_id, instrument_id, ts_event)
   - `trades` records (rtype 0x00): price is an int64 (1e-9 fixed point) at offset 16
   - `mbp-10` records (rtype 0x0a): a full top-10 book snapshot — 10 BidAskPair
     levels (bid_px/ask_px int64, bid_sz/ask_sz uint32) starting at offset 48

   - `symbol mapping` records (rtype 0x16): the live gateway's authoritative
     instrument_id ↔ subscribed-symbol binding (emitted at session start / rolls)
   All other record types (system/heartbeat, error) are skipped by their length,
   so the decoder is robust across DBN versions — these field offsets are stable
   across v1/v2/v3.

   feed() is partial-safe: it buffers across chunks and only emits complete records. */

const RTYPE_TRADE = 0x00;
const RTYPE_MBP1 = 0x01; // top-of-book (best bid/ask) — one price level
const RTYPE_MBP10 = 0x0a;
const RTYPE_SYMBOL_MAPPING = 0x16; // live gateway → instrument_id ↔ subscribed symbol
const HEADER_BYTES = 16;
const PRICE_SCALE = 1e9;
const UNDEF_PRICE = 9223372036854775807n; // INT64_MAX sentinel = no price at this level

// mbp-10 layout after the 16-byte header: price(8)+size(4)+action(1)+side(1)+
// flags(1)+depth(1)+ts_recv(8)+ts_in_delta(4)+sequence(4) = 32 bytes, then the
// 10 levels. Each BidAskPair is 32 bytes: bid_px(8) ask_px(8) bid_sz(4) ask_sz(4) bid_ct(4) ask_ct(4).
const MBP10_LEVELS = 10;
const LEVELS_OFFSET = HEADER_BYTES + 32; // = 48
const LEVEL_BYTES = 32;
const MBP10_MIN_BYTES = LEVELS_OFFSET + MBP10_LEVELS * LEVEL_BYTES; // = 368
const MBP1_MIN_BYTES = LEVELS_OFFSET + LEVEL_BYTES; // = 80 — header + flds + one BidAskPair

export interface DecodedTrade {
  kind: "trade";
  instrumentId: number;
  price: number;
  size: number; // contracts in this print (uint32 at offset 24)
  ts: number; // epoch ms (from ts_event nanoseconds)
}

export interface BookLevel {
  price: number;
  size: number;
}

export interface DecodedBook {
  kind: "book";
  instrumentId: number;
  bids: BookLevel[]; // best first
  asks: BookLevel[]; // best first
  ts: number; // epoch ms
}

export interface DecodedMapping {
  kind: "mapping";
  instrumentId: number;
  symbolText: string; // printable ASCII of the symbol region; caller matches its known symbols
}

export type DecodedRecord = DecodedTrade | DecodedBook | DecodedMapping;

export class DbnDecoder {
  private buf: Buffer = Buffer.alloc(0);
  private metadataParsed = false;
  private version = 0;

  /** DBN version from the metadata header (0 until metadata is seen). */
  get dbnVersion(): number {
    return this.version;
  }

  feed(chunk: Buffer): DecodedRecord[] {
    this.buf = this.buf.length ? Buffer.concat([this.buf, chunk]) : chunk;
    const out: DecodedRecord[] = [];

    if (!this.metadataParsed) {
      if (this.buf.length < 8) return out; // need magic + length
      if (this.buf.subarray(0, 3).toString("ascii") !== "DBN") {
        throw new Error("not a DBN stream");
      }
      this.version = this.buf[3]!;
      const metaLen = this.buf.readUInt32LE(4);
      if (this.buf.length < 8 + metaLen) return out; // wait for full metadata
      this.buf = this.buf.subarray(8 + metaLen);
      this.metadataParsed = true;
    }

    while (this.buf.length >= 1) {
      const lengthWords = this.buf[0]!;
      const recBytes = lengthWords * 4;
      if (recBytes < HEADER_BYTES) {
        // Corrupt/unknown framing — drop the buffer to resync rather than loop.
        this.buf = Buffer.alloc(0);
        break;
      }
      if (this.buf.length < recBytes) break; // wait for the rest of the record

      const rtype = this.buf[1]!;
      if (rtype === RTYPE_TRADE) {
        out.push({
          kind: "trade",
          instrumentId: this.buf.readUInt32LE(4),
          ts: Number(this.buf.readBigUInt64LE(8) / 1_000_000n),
          price: Number(this.buf.readBigInt64LE(HEADER_BYTES)) / PRICE_SCALE,
          size: recBytes >= 28 ? this.buf.readUInt32LE(24) : 0,
        });
      } else if (rtype === RTYPE_MBP1 && recBytes >= MBP1_MIN_BYTES) {
        out.push(decodeTopOfBook(this.buf, 1));
      } else if (rtype === RTYPE_MBP10 && recBytes >= MBP10_MIN_BYTES) {
        out.push(decodeTopOfBook(this.buf, MBP10_LEVELS));
      } else if (rtype === RTYPE_SYMBOL_MAPPING) {
        // The gateway emits these at session start (and on rolls): they bind the
        // LIVE instrument_id to the symbol we subscribed by. The exact field
        // offsets shift across DBN versions, so we hand the caller the printable
        // ASCII of the record body and let it match the symbols it knows — robust
        // without version-specific struct math. This is the authoritative live
        // id→symbol map (the Historical symbology resolve can disagree on rolls).
        out.push({
          kind: "mapping",
          instrumentId: this.buf.readUInt32LE(4),
          symbolText: this.buf.subarray(HEADER_BYTES, recBytes).toString("latin1"),
        });
      }
      this.buf = this.buf.subarray(recBytes);
    }
    return out;
  }

  /** Reset for a fresh connection (new metadata header expected). */
  reset(): void {
    this.buf = Buffer.alloc(0);
    this.metadataParsed = false;
    this.version = 0;
  }
}

/** Decode an mbp-1 (levels=1) or mbp-10 (levels=10) record; level 0 is the BBO. */
function decodeTopOfBook(buf: Buffer, levels: number): DecodedBook {
  const instrumentId = buf.readUInt32LE(4);
  const ts = Number(buf.readBigUInt64LE(8) / 1_000_000n);
  const bids: BookLevel[] = [];
  const asks: BookLevel[] = [];
  for (let i = 0; i < levels; i++) {
    const o = LEVELS_OFFSET + i * LEVEL_BYTES;
    const bidPx = buf.readBigInt64LE(o);
    const askPx = buf.readBigInt64LE(o + 8);
    const bidSz = buf.readUInt32LE(o + 16);
    const askSz = buf.readUInt32LE(o + 20);
    // Skip empty/undefined levels so the book only carries real resting size.
    if (bidPx !== UNDEF_PRICE && bidSz > 0) bids.push({ price: Number(bidPx) / PRICE_SCALE, size: bidSz });
    if (askPx !== UNDEF_PRICE && askSz > 0) asks.push({ price: Number(askPx) / PRICE_SCALE, size: askSz });
  }
  return { kind: "book", instrumentId, bids, asks, ts };
}
