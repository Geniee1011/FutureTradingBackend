/* Minimal streaming decoder for Databento Binary Encoding (DBN).

   We parse two record types:
   - the metadata block (magic "DBN" + version + uint32 length) is skipped
   - each record's 16-byte header (length, rtype, publisher_id, instrument_id, ts_event)
   - `trades` records (rtype 0x00): price is an int64 (1e-9 fixed point) at offset 16
   - `mbp-10` records (rtype 0x0a): a full top-10 book snapshot — 10 BidAskPair
     levels (bid_px/ask_px int64, bid_sz/ask_sz uint32) starting at offset 48

   All other record types (symbol mapping, system/heartbeat, error) are skipped
   by their length, so the decoder is robust across DBN versions — these field
   offsets are stable across v1/v2/v3.

   feed() is partial-safe: it buffers across chunks and only emits complete records. */

const RTYPE_TRADE = 0x00;
const RTYPE_MBP10 = 0x0a;
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

export interface DecodedTrade {
  kind: "trade";
  instrumentId: number;
  price: number;
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

export type DecodedRecord = DecodedTrade | DecodedBook;

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
        });
      } else if (rtype === RTYPE_MBP10 && recBytes >= MBP10_MIN_BYTES) {
        out.push(decodeMbp10(this.buf));
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

/** Decode one complete mbp-10 record (caller guarantees buf holds >= MBP10_MIN_BYTES). */
function decodeMbp10(buf: Buffer): DecodedBook {
  const instrumentId = buf.readUInt32LE(4);
  const ts = Number(buf.readBigUInt64LE(8) / 1_000_000n);
  const bids: BookLevel[] = [];
  const asks: BookLevel[] = [];
  for (let i = 0; i < MBP10_LEVELS; i++) {
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
