/* Trading-session clock for the analytics layer.
 *
 * Session stats reset at 9:30am America/New_York (US equity open) every WEEKDAY.
 * `sessionAnchor(now)` returns the most recent 9:30 ET boundary <= now, clamped
 * back to Friday across the weekend (no reset Sat/Sun). DST-aware via Intl — no
 * external tz library. This is intentionally separate from the CME Globex
 * "trading day" (18:00 ET) that the risk engine uses for daily-loss resets. */

const ET = "America/New_York";
const SESSION_OPEN_MIN = 9 * 60 + 30; // 09:30 ET

interface EtParts {
  y: number;
  mo: number; // 1-12
  da: number;
  h: number;
  mi: number;
  s: number;
}

/** Wall-clock components of an instant in Eastern Time. */
function etParts(d: Date): EtParts {
  const fmt = new Intl.DateTimeFormat("en-US", {
    timeZone: ET,
    year: "numeric", month: "2-digit", day: "2-digit",
    hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false,
  });
  const p: Record<string, string> = {};
  for (const part of fmt.formatToParts(d)) p[part.type] = part.value;
  return {
    y: +p.year!, mo: +p.month!, da: +p.day!,
    h: p.hour === "24" ? 0 : +p.hour!, mi: +p.minute!, s: +p.second!,
  };
}

/** Offset of Eastern Time from UTC, in ms, at instant `d` (e.g. -4h in DST). */
function etOffsetMs(d: Date): number {
  const p = etParts(d);
  const asIfUtc = Date.UTC(p.y, p.mo - 1, p.da, p.h, p.mi, p.s);
  return asIfUtc - d.getTime();
}

/** Convert an ET wall-clock (y,mo,da 09:30:00) to the true UTC epoch, DST-safe. */
function etWallToEpoch(y: number, mo: number, da: number, h: number, mi: number): number {
  const wallAsUtc = Date.UTC(y, mo - 1, da, h, mi, 0);
  // Refine twice: the offset at the tentative instant may differ from `now`'s.
  let epoch = wallAsUtc - etOffsetMs(new Date(wallAsUtc));
  epoch = wallAsUtc - etOffsetMs(new Date(epoch));
  return epoch;
}

/**
 * The most recent 9:30 ET session-open at or before `now`, clamped to weekdays
 * (Sat→Fri, Sun→Fri). This is the anchor session stats reset against.
 */
export function sessionAnchor(now: Date = new Date()): Date {
  const p = etParts(now);
  // Start from today's ET date; if we're before 9:30 ET, step back a calendar day.
  let base = Date.UTC(p.y, p.mo - 1, p.da);
  if (p.h * 60 + p.mi < SESSION_OPEN_MIN) base -= 86_400_000;
  // Clamp across the weekend: an anchor landing on Sat/Sun rolls back to Friday.
  const dow = new Date(base).getUTCDay(); // 0=Sun … 6=Sat
  if (dow === 6) base -= 86_400_000; // Sat → Fri
  else if (dow === 0) base -= 2 * 86_400_000; // Sun → Fri
  const b = new Date(base);
  return new Date(etWallToEpoch(b.getUTCFullYear(), b.getUTCMonth() + 1, b.getUTCDate(), 9, 30));
}

/** Whole minutes elapsed since the current session opened (>= 0). */
export function minutesInSession(now: Date = new Date()): number {
  return Math.max(0, Math.floor((now.getTime() - sessionAnchor(now).getTime()) / 60_000));
}

/** Eastern-Time calendar day key (YYYY-MM-DD) — used to bucket trades by trading day. */
export function etDateKey(d: Date): string {
  return new Intl.DateTimeFormat("en-CA", { timeZone: ET, year: "numeric", month: "2-digit", day: "2-digit" }).format(d);
}
