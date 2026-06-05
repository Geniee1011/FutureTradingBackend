import "dotenv/config";
import pg from "pg";

/* PostgreSQL connection pool (pure-JS `pg` driver — runs on any CPU, unlike
   Prisma's native engine). Lazily created so the app can run in-memory when
   DATABASE_URL is unset. `pg` decodes percent-encoded passwords in the URL. */

let pool: pg.Pool | null = null;

export function getPool(): pg.Pool {
  if (!pool) {
    pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
  }
  return pool;
}

export async function closePool(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
  }
}
