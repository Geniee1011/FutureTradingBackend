import { getPool } from "../db/pool.js";
import { hashPassword } from "./password.js";
import { config } from "../config.js";

/* Provision an admin from env (ADMIN_EMAIL + ADMIN_PASSWORD) on startup — so a
   fresh deploy can have an admin without running the seed or a shell. Idempotent:
   creates the user if missing, promotes an existing user of that email to ADMIN,
   and otherwise does nothing. Never changes an existing user's password. */
export async function ensureBootstrapAdmin(): Promise<void> {
  const email = config.adminEmail.toLowerCase();
  const password = config.adminPassword;
  if (!email || !password) return; // not configured — skip

  const pool = getPool();
  const { rows } = await pool.query<{ id: string; role: string }>(
    `SELECT "id","role" FROM "User" WHERE "email" = $1`,
    [email],
  );

  if (rows[0]) {
    if (rows[0].role !== "ADMIN") {
      await pool.query(`UPDATE "User" SET "role" = 'ADMIN', "updatedAt" = now() WHERE "id" = $1`, [rows[0].id]);
      console.log(`[admin] promoted existing user ${email} to ADMIN`);
    } else {
      console.log(`[admin] bootstrap admin ${email} already present`);
    }
    return;
  }

  await pool.query(
    `INSERT INTO "User" ("email","passwordHash","name","role") VALUES ($1,$2,$3,'ADMIN')`,
    [email, hashPassword(password), "Administrator"],
  );
  console.log(`[admin] created bootstrap ADMIN ${email}`);
}
