/* Account progression ladder. Passing one tier's profit target advances to the next.
   The 50k and 100k challenge paths merge into one shared funded scaling ladder. `f_1m`
   is the top — it has no successor, so hitting its target is a payout (reset in place),
   not a scale-up. Shared by the risk engine (detects the target) and the admin review
   flow (applies the approved advancement). */

export const NEXT_TIER: Record<string, string> = {
  c1_50k: "c2_50k",
  c2_50k: "f_50k",
  c1_100k: "c2_100k",
  c2_100k: "f_100k",
  f_50k: "f_100k",
  f_100k: "f_250k",
  f_250k: "f_500k",
  f_500k: "f_1m",
};

/** The tier an account advances to on approval, or null if it's at the top / untiered. */
export function nextTierFor(templateId: string | null): string | null {
  if (!templateId) return null;
  return NEXT_TIER[templateId] ?? null;
}

/** True once an account is on a funded tier (vs still in a challenge phase). */
export function isFundedTier(templateId: string | null): boolean {
  return !!templateId && templateId.startsWith("f_");
}
