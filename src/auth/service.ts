import { signToken, verifyToken, type Role } from "./jwt.js";
import { verifyPassword } from "./password.js";
import { toPublicUser, type PublicUser, type UserStore } from "./users.js";

export interface AuthResult {
  token: string;
  user: PublicUser;
}

/** Authentication use-cases: login, token validation, registration. */
export class AuthService {
  constructor(private readonly users: UserStore) {}

  async login(email: string, password: string): Promise<AuthResult | null> {
    if (!email || !password) return null;
    const user = await this.users.findByEmail(email);
    // Same response whether the user is missing or the password is wrong.
    if (!user || !verifyPassword(password, user.passwordHash)) return null;
    if (user.status === "SUSPENDED") return null;
    return { token: signToken(this.payload(user.id, user.email, user.role)), user: toPublicUser(user) };
  }

  /** Resolve the user for a valid bearer token. */
  async me(token: string): Promise<PublicUser | null> {
    const payload = verifyToken(token);
    if (!payload) return null;
    const user = await this.users.findById(payload.sub);
    return user ? toPublicUser(user) : null;
  }

  async register(input: { email: string; password: string; name: string; role?: Role }): Promise<AuthResult> {
    const user = await this.users.create(input);
    return { token: signToken(this.payload(user.id, user.email, user.role)), user: toPublicUser(user) };
  }

  private payload(sub: string, email: string, role: Role) {
    return { sub, email, role };
  }
}
