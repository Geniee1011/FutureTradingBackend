/* Editable behavioural phase ruleset + evaluation engine.
 *
 * Rules live in the "PhaseRule" table so they can be changed from the admin
 * Analytics page without a code deploy. The engine reads the ACTIVE rules,
 * tests each against a trader's current variable values, and assigns the
 * HIGHEST matching phase (1-4); phase 1 if nothing matches. */

import type { Pool, PoolClient } from "pg";
import { getPool } from "../db/pool.js";

export type Db = Pool | PoolClient;

export const PHASE_OPERATORS = [">=", "<=", ">", "<", "="] as const;
export type PhaseOperator = (typeof PHASE_OPERATORS)[number];

/** Variables a rule may reference (all live on the trader record / derived). */
export const PHASE_VARIABLES = [
  "consecutive_losses",
  "consecutive_wins",
  "session_trade_count",
  "daily_loss_pct_consumed",
  "session_pnl",
  "session_win_rate",
  "time_in_session_minutes",
  "size_deviation_ratio",
  "current_drawdown_consumed_pct",
  "current_challenge_pnl_pct",
  "challenge_day",
  "reset_count",
  "lifetime_win_rate",
  "lifetime_trade_count",
] as const;
export type PhaseVariable = (typeof PHASE_VARIABLES)[number];

export interface PhaseRule {
  ruleId: number;
  variable: string;
  operator: string;
  value: number;
  assignsPhase: number;
  priority: number;
  active: boolean;
  notes: string | null;
}

interface PhaseRuleRaw {
  ruleId: number;
  variable: string;
  operator: string;
  value: string;
  assignsPhase: number;
  priority: number;
  active: boolean;
  notes: string | null;
}

function mapRule(r: PhaseRuleRaw): PhaseRule {
  return {
    ruleId: r.ruleId,
    variable: r.variable,
    operator: r.operator,
    value: Number(r.value),
    assignsPhase: r.assignsPhase,
    priority: r.priority,
    active: r.active,
    notes: r.notes,
  };
}

const SELECT = `SELECT "ruleId","variable","operator","value","assignsPhase","priority","active","notes" FROM "PhaseRule"`;

/** All active rules, ordered by priority (lower first) — for the engine. */
export async function loadActiveRules(db: Db): Promise<PhaseRule[]> {
  const { rows } = await db.query<PhaseRuleRaw>(`${SELECT} WHERE "active" = true ORDER BY "priority" ASC, "ruleId" ASC`);
  return rows.map(mapRule);
}

/** Every rule (active + disabled) for the admin panel. */
export async function listPhaseRules(): Promise<PhaseRule[]> {
  const { rows } = await getPool().query<PhaseRuleRaw>(`${SELECT} ORDER BY "priority" ASC, "ruleId" ASC`);
  return rows.map(mapRule);
}

function compare(a: number, op: string, b: number): boolean {
  switch (op) {
    case ">=": return a >= b;
    case "<=": return a <= b;
    case ">": return a > b;
    case "<": return a < b;
    case "=":
    case "==": return Math.abs(a - b) < 1e-9;
    default: return false;
  }
}

/**
 * Assign a phase from the ruleset: test every active rule against `vars`,
 * collect the phases whose rule matched, return the HIGHEST (default 1).
 */
export function evaluatePhase(rules: PhaseRule[], vars: Record<string, number>): number {
  let phase = 1;
  for (const r of rules) {
    if (!r.active) continue;
    const v = vars[r.variable];
    if (v == null || !Number.isFinite(v)) continue;
    if (compare(v, r.operator, r.value)) phase = Math.max(phase, r.assignsPhase);
  }
  return phase;
}

// --- admin CRUD --------------------------------------------------

export interface PhaseRuleInput {
  variable: string;
  operator: string;
  value: number;
  assignsPhase: number;
  priority?: number;
  active?: boolean;
  notes?: string | null;
}

function validate(input: Partial<PhaseRuleInput>): string | null {
  if (input.variable != null && !PHASE_VARIABLES.includes(input.variable as PhaseVariable)) return `unknown variable "${input.variable}"`;
  if (input.operator != null && !PHASE_OPERATORS.includes(input.operator as PhaseOperator)) return `invalid operator "${input.operator}"`;
  if (input.assignsPhase != null && (input.assignsPhase < 1 || input.assignsPhase > 4)) return "assignsPhase must be 1-4";
  if (input.value != null && !Number.isFinite(input.value)) return "value must be a number";
  return null;
}

export async function createPhaseRule(input: PhaseRuleInput): Promise<PhaseRule | { error: string }> {
  const err = validate(input);
  if (err) return { error: err };
  const { rows } = await getPool().query<PhaseRuleRaw>(
    `INSERT INTO "PhaseRule" ("variable","operator","value","assignsPhase","priority","active","notes")
     VALUES ($1,$2,$3,$4,$5,$6,$7)
     RETURNING "ruleId","variable","operator","value","assignsPhase","priority","active","notes"`,
    [input.variable, input.operator, input.value, input.assignsPhase, input.priority ?? 100, input.active ?? true, input.notes ?? null],
  );
  return mapRule(rows[0]!);
}

export async function updatePhaseRule(ruleId: number, patch: Partial<PhaseRuleInput>): Promise<PhaseRule | { error: string } | null> {
  const err = validate(patch);
  if (err) return { error: err };
  // Build a dynamic SET clause from only the provided fields.
  const cols: Record<string, unknown> = {
    variable: patch.variable, operator: patch.operator, value: patch.value,
    assignsPhase: patch.assignsPhase, priority: patch.priority, active: patch.active, notes: patch.notes,
  };
  const sets: string[] = [];
  const vals: unknown[] = [];
  let i = 1;
  for (const [k, v] of Object.entries(cols)) {
    if (v === undefined) continue;
    sets.push(`"${k}" = $${i++}`);
    vals.push(v);
  }
  if (!sets.length) return null;
  sets.push(`"updatedAt" = now()`);
  vals.push(ruleId);
  const { rows } = await getPool().query<PhaseRuleRaw>(
    `UPDATE "PhaseRule" SET ${sets.join(", ")} WHERE "ruleId" = $${i}
     RETURNING "ruleId","variable","operator","value","assignsPhase","priority","active","notes"`,
    vals,
  );
  return rows[0] ? mapRule(rows[0]) : null;
}

export async function deletePhaseRule(ruleId: number): Promise<boolean> {
  const { rowCount } = await getPool().query(`DELETE FROM "PhaseRule" WHERE "ruleId" = $1`, [ruleId]);
  return (rowCount ?? 0) > 0;
}
