import "dotenv/config";
import { readFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { getPool, closePool } from "./pool.js";

/* Applies schema.sql to the database (idempotent). Run via `npm run db:migrate`. */

const here = dirname(fileURLToPath(import.meta.url));

async function main() {
  if (!process.env.DATABASE_URL) {
    console.error("DATABASE_URL is not set — nothing to migrate.");
    process.exit(1);
  }
  const sql = readFileSync(join(here, "schema.sql"), "utf8");
  await getPool().query(sql);
  console.log("✓ Database schema applied (User, Account, Rule, Position, Order, Fill, Violation, ActivityLog).");
  await closePool();
}

main().catch(async (err) => {
  console.error("Migration failed:", err.message);
  await closePool();
  process.exit(1);
});
