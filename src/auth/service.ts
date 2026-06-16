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

  /** Self-service password change: verify the current password, then set a new one. */
  async changePassword(
    userId: string,
    currentPassword: string,
    newPassword: string,
  ): Promise<{ ok: boolean; error?: string }> {
    const user = await this.users.findById(userId);
    if (!user) return { ok: false, error: "User not found." };
    if (!verifyPassword(currentPassword, user.passwordHash)) {
      return { ok: false, error: "Current password is incorrect." };
    }
    await this.users.updatePassword(userId, newPassword);
    return { ok: true };
  }

  /** Admin override: set a user's password without knowing the current one. */
  async adminResetPassword(userId: string, newPassword: string): Promise<boolean> {
    return this.users.updatePassword(userId, newPassword);
  }

  private payload(sub: string, email: string, role: Role) {
    return { sub, email, role };
  }
}
