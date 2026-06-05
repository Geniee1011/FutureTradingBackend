import { getPool } from "../db/pool.js";
import { hashPassword } from "./password.js";
import type { Role } from "./jwt.js";
import type { User, UserStore } from "./users.js";

interface UserRow {
  id: string;
  email: string;
  passwordHash: string;
  name: string | null;
  role: Role;
  status: User["status"];
}

const COLS = `"id","email","passwordHash","name","role","status"`;

/** PostgreSQL-backed user store (pure-JS `pg`). Reads/writes the "User" table. */
export class PgUserStore implements UserStore {
  async findByEmail(email: string): Promise<User | null> {
    const { rows } = await getPool().query<UserRow>(
      `SELECT ${COLS} FROM "User" WHERE "email" = $1`,
      [email.toLowerCase()],
    );
    return rows[0] ? this.map(rows[0]) : null;
  }

  async findById(id: string): Promise<User | null> {
    const { rows } = await getPool().query<UserRow>(`SELECT ${COLS} FROM "User" WHERE "id" = $1`, [id]);
    return rows[0] ? this.map(rows[0]) : null;
  }

  async create(input: { email: string; password: string; name: string; role?: Role }): Promise<User> {
    const { rows } = await getPool().query<UserRow>(
      `INSERT INTO "User" ("email","passwordHash","name","role")
       VALUES ($1,$2,$3,$4)
       RETURNING ${COLS}`,
      [input.email.toLowerCase(), hashPassword(input.password), input.name, input.role ?? "TRADER"],
    );
    return this.map(rows[0]!);
  }

  private map(r: UserRow): User {
    return {
      id: r.id,
      email: r.email,
      passwordHash: r.passwordHash,
      name: r.name ?? "",
      role: r.role,
      status: r.status,
    };
  }
}
