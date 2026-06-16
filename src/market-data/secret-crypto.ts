import { createCipheriv, createDecipheriv, createHash, randomBytes } from "node:crypto";

/* Symmetric encryption for secrets stored at rest (each user's Databento API key
   under Model B). AES-256-GCM (authenticated); the 32-byte key is derived from
   the configured MARKET_DATA_ENC_KEY via SHA-256 so any passphrase length works.
   Stored format: "v1.<iv>.<tag>.<ciphertext>" (all base64). */

function keyBuf(secret: string): Buffer {
  return createHash("sha256").update(secret).digest(); // 32 bytes
}

export function encryptSecret(plaintext: string, secret: string): string {
  const iv = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", keyBuf(secret), iv);
  const enc = Buffer.concat([cipher.update(plaintext, "utf8"), cipher.final()]);
  const tag = cipher.getAuthTag();
  return `v1.${iv.toString("base64")}.${tag.toString("base64")}.${enc.toString("base64")}`;
}

/** Decrypt a blob produced by encryptSecret. Returns null on any tamper/format/key error. */
export function decryptSecret(blob: string, secret: string): string | null {
  try {
    const [v, ivB64, tagB64, encB64] = blob.split(".");
    if (v !== "v1" || !ivB64 || !tagB64 || !encB64) return null;
    const decipher = createDecipheriv("aes-256-gcm", keyBuf(secret), Buffer.from(ivB64, "base64"));
    decipher.setAuthTag(Buffer.from(tagB64, "base64"));
    return Buffer.concat([decipher.update(Buffer.from(encB64, "base64")), decipher.final()]).toString("utf8");
  } catch {
    return null;
  }
}
