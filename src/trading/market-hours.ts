/* ------------------------------------------------------------------ *
 * Market hours — is a CME-listed instrument tradable right now?
 *
 * Every instrument we list (CME equity index, NYMEX energy, COMEX metals)
 * trades on CME Globex, which shares one core weekly schedule:
 *   • Trading week:        Sunday 18:00 ET → Friday 17:00 ET
 *   • Daily maintenance:   closed 17:00–18:00 ET (Mon–Thu)
 * Outside that window the market is closed and no order may fill.
 *
 * All reasoning is done in America/New_York wall-clock time (DST-aware via
 * Intl), so it stays correct across the March/November clock changes without
 * any date arithmetic of our own. Per-product differences (e.g. metals/energy
 * settlement nuances) are deliberately not modelled — this is the unified
 * Globex session, which is what "is the market open?" means for the trader.
 * ------------------------------------------------------------------ */

const ET = "America/New_York";
const OPEN_MIN = 18 * 60; // 18:00 ET — Globex (re)opens for the next session
const CLOSE_MIN = 17 * 60; // 17:00 ET — daily maintenance halt / weekly close

/**
 * Full-day CME closures (holidays), in ET as YYYY-MM-DD. Only FULL closures are
 * listed — early-close ("shortened session") days are intentionally not modelled.
 * Extend this set each year from the CME holiday calendar.
 */
const HOLIDAYS = new Set<string>([
  // 2026
  "2026-01-01", // New Year's Day
  "2026-12-25", // Christmas Day
]);

interface EtParts {
  dow: number; // 0 = Sunday … 6 = Saturday, in ET
  minutes: number; // minutes since ET midnight
  date: string; // YYYY-MM-DD in ET
}

function etParts(now: Date): EtParts {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone: ET,
    weekday: "short",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(now);
  const get = (t: string) => parts.find((p) => p.type === t)?.value ?? "";
  const DOW: Record<string, number> = { Sun: 0, Mon: 1, Tue: 2, Wed: 3, Thu: 4, Fri: 5, Sat: 6 };
  const dow = DOW[get("weekday")] ?? 0;
  let hour = parseInt(get("hour"), 10);
  if (hour === 24) hour = 0; // some ICU builds emit "24" at midnight under hour12:false
  const minutes = hour * 60 + parseInt(get("minute"), 10);
  return { dow, minutes, date: `${get("year")}-${get("month")}-${get("day")}` };
}

/** True when the CME Globex session is open (and not a holiday) at `now`. */
export function isMarketOpen(now: Date = new Date()): boolean {
  const { dow, minutes, date } = etParts(now);
  if (HOLIDAYS.has(date)) return false;
  if (dow === 6) return false; // Saturday: closed all day
  if (dow === 0) return minutes >= OPEN_MIN; // Sunday: opens 18:00 ET
  if (dow === 5) return minutes < CLOSE_MIN; // Friday: closes 17:00 ET
  // Mon–Thu: open except the daily 17:00–18:00 ET maintenance halt.
  return minutes < CLOSE_MIN || minutes >= OPEN_MIN;
}

/** User-facing warning shown when an order is rejected because the market is closed. */
export const MARKET_CLOSED_MESSAGE = "Market is closed — you cannot place an order right now.";
