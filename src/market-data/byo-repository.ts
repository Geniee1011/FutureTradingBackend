import { getPool } from "../db/pool.js";
import { config } from "../config.js";
import { DatabentoClient } from "../databento/client.js";
import { encryptSecret, decryptSecret } from "./secret-crypto.js";

/* Storage + validation for each user's OWN Databento API key (Model B / byo).
   Keys are encrypted at rest with MARKET_DATA_ENC_KEY and only ever decrypted
   server-side to make that user's own data requests — never returned to a client. */

/** True when the server is configured to store BYO keys (an encryption secret is set). */
export function byoConfigured(): boolean {
  return config.marketDataEncKey.length > 0;
}

/**
 * Validate a candidate Databento key with a cheap authenticated metadata call.
 * Returns true only if Databento accepts the key for our dataset.
 */
export async function validateDatabentoKey(apiKey: string): Promise<boolean> {
  if (!apiKey.trim()) return false;
  try {
    const client = new DatabentoClient(apiKey.trim(), config.databento.dataset);
    await client.availableEnd("trades"); // 401/403 → throws for a bad key
    return true;
  } catch {
    return false;
  }
}

/** Store (encrypted) a user's Databento key. Assumes it has already been validated. */
export async function setUserDatabentoKey(userId: string, apiKey: string): Promise<void> {
  const blob = encryptSecret(apiKey.trim(), config.marketDataEncKey);
  await getPool().query(`UPDATE "User" SET "databentoKeyEnc" = $1, "updatedAt" = now() WHERE "id" = $2`, [blob, userId]);
}

/** Remove a user's stored Databento key (disconnect). */
export async function clearUserDatabentoKey(userId: string): Promise<void> {
  await getPool().query(`UPDATE "User" SET "databentoKeyEnc" = NULL, "updatedAt" = now() WHERE "id" = $1`, [userId]);
}

/** Whether a user currently has a Databento key connected. */
export async function hasUserDatabentoKey(userId: string): Promise<boolean> {
  const { rows } = await getPool().query<{ has: boolean }>(
    `SELECT ("databentoKeyEnc" IS NOT NULL) AS has FROM "User" WHERE "id" = $1`,
    [userId],
  );
  return rows[0]?.has ?? false;
}

/** Decrypt and return a user's Databento key for server-side use, or null if none/undecryptable. */
export async function getUserDatabentoKey(userId: string): Promise<string | null> {
  const { rows } = await getPool().query<{ databentoKeyEnc: string | null }>(
    `SELECT "databentoKeyEnc" FROM "User" WHERE "id" = $1`,
    [userId],
  );
  const blob = rows[0]?.databentoKeyEnc;
  if (!blob) return null;
  return decryptSecret(blob, config.marketDataEncKey);
}
