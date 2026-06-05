import { useDatabase } from "../config.js";
import { MemoryUserStore, type UserStore } from "./users.js";
import { PgUserStore } from "./pg-user-store.js";

/** Pick the user store based on configuration: Postgres if DATABASE_URL is set. */
export function createUserStore(): UserStore {
  if (useDatabase) {
    console.log("[auth] user store: PostgreSQL (pg)");
    return new PgUserStore();
  }
  console.log("[auth] user store: in-memory (no DATABASE_URL)");
  return new MemoryUserStore();
}
