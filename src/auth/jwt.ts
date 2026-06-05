import jwt from "jsonwebtoken";
import { config } from "../config.js";

export type Role = "ADMIN" | "TRADER";

export interface JwtPayload {
  sub: string; // user id
  email: string;
  role: Role;
}

/** Sign a short-lived access token. */
export function signToken(payload: JwtPayload): string {
  return jwt.sign(payload, config.jwt.secret, { expiresIn: config.jwt.expiresInSec });
}

/** Verify a token; returns the payload or null if invalid/expired. */
export function verifyToken(token: string): JwtPayload | null {
  try {
    const decoded = jwt.verify(token, config.jwt.secret);
    if (typeof decoded === "string") return null;
    const { sub, email, role } = decoded as jwt.JwtPayload & JwtPayload;
    if (!sub || !email || !role) return null;
    return { sub, email, role };
  } catch {
    return null;
  }
}

/** Extract a bearer token from an Authorization header value. */
export function bearerToken(header: string | undefined): string | null {
  if (!header) return null;
  const [scheme, token] = header.split(" ");
  return scheme === "Bearer" && token ? token : null;
}
