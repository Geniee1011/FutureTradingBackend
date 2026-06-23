import "dotenv/config";
import pg from "pg";

/* PostgreSQL connection pool (pure-JS `pg` driver — runs on any CPU, unlike
   Prisma's native engine). Lazily created so the app can run in-memory when
   DATABASE_URL is unset. `pg` decodes percent-encoded passwords in the URL. */

let pool: pg.Pool | null = null;

export function getPool(): pg.Pool {
  if (!pool) {
    pool = new pg.Pool({ connectionString: process.env.DATABASE_URL });
    // CRITICAL: without this listener, an idle pooled connection dropped by Postgres
    // (ECONNRESET — server restart, idle timeout, network blip) is emitted as an
    // unhandled 'error' on the pool's EventEmitter, which THROWS and crashes the
    // process. Log it and move on; the pool discards the dead client and the next
    // query transparently opens a fresh one.
    pool.on("error", (err) => {
      console.warn(`[pg] idle client error (connection dropped, will reconnect): ${err.message}`);
    });
  }
  return pool;
}

export async function closePool(): Promise<void> {
  if (pool) {
    await pool.end();
    pool = null;
  }
}
