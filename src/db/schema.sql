-- Trading Evaluation Platform schema (PostgreSQL).
-- Faithful 1:1 translation of prisma/schema.prisma — same table names (PascalCase)
-- and column names (camelCase), so it stays Prisma-compatible. Idempotent: safe
-- to run repeatedly. Applied by `npm run db:migrate` (src/db/migrate.ts via pg).

-- ----------------------------- Enums -----------------------------
DO $$ BEGIN CREATE TYPE "Role"            AS ENUM ('ADMIN','TRADER'); EXCEPTION WHEN duplicate_object THEN null; END $$;
DO $$ BEGIN CREATE TYPE "UserStatus"      AS ENUM ('ACTIVE','PENDING','SUSPENDED'); EXCEPTION WHEN duplicate_object THEN null; END $$;
DO $$ BEGIN CREATE TYPE "AccountStatus"   AS ENUM ('ACTIVE','PASSED','FAILED','SUSPENDED'); EXCEPTION WHEN duplicate_object THEN null; END $$;
DO $$ BEGIN CREATE TYPE "PositionSide"    AS ENUM ('LONG','SHORT'); EXCEPTION WHEN duplicate_object THEN null; END $$;
DO $$ BEGIN CREATE TYPE "OrderSide"       AS ENUM ('BUY','SELL'); EXCEPTION WHEN duplicate_object THEN null; END $$;
DO $$ BEGIN CREATE TYPE "OrderType"       AS ENUM ('MARKET','LIMIT','STOP','STOP_LIMIT'); EXCEPTION WHEN duplicate_object THEN null; END $$;
DO $$ BEGIN CREATE TYPE "OrderStatus"     AS ENUM ('PENDING','FILLED','PARTIALLY_FILLED','CANCELLED','REJECTED'); EXCEPTION WHEN duplicate_object THEN null; END $$;
DO $$ BEGIN CREATE TYPE "ViolationType"   AS ENUM ('DAILY_LOSS_EXCEEDED','MAX_DRAWDOWN_BREACHED','CONTRACT_LIMIT_EXCEEDED','RESTRICTED_INSTRUMENT'); EXCEPTION WHEN duplicate_object THEN null; END $$;
DO $$ BEGIN CREATE TYPE "ViolationAction" AS ENUM ('REJECT_ORDER','LIQUIDATE_POSITION','SUSPEND_ACCOUNT'); EXCEPTION WHEN duplicate_object THEN null; END $$;
DO $$ BEGIN CREATE TYPE "ActivityType"    AS ENUM ('USER_LOGIN','ORDER_PLACEMENT','ORDER_MODIFIED','ORDER_FILLED','ORDER_CANCELLED','ORDER_REJECTED','POSITION_OPENED','POSITION_CLOSED','RULE_VIOLATION','ACCOUNT_PASSED','ACCOUNT_SUSPENSION'); EXCEPTION WHEN duplicate_object THEN null; END $$;
DO $$ BEGIN CREATE TYPE "TransactionType" AS ENUM ('DEPOSIT','WITHDRAWAL','FEE','TRADE','FUNDING'); EXCEPTION WHEN duplicate_object THEN null; END $$;
-- Add enum values to pre-existing databases (CREATE TYPE above only fires on a fresh DB).
ALTER TYPE "ActivityType" ADD VALUE IF NOT EXISTS 'ORDER_MODIFIED';

-- ----------------------------- Tables ----------------------------
CREATE TABLE IF NOT EXISTS "User" (
  "id"           text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  "email"        text UNIQUE NOT NULL,
  "passwordHash" text NOT NULL,
  "name"         text,
  "role"         "Role" NOT NULL DEFAULT 'TRADER',
  "status"       "UserStatus" NOT NULL DEFAULT 'ACTIVE',
  "createdAt"    timestamptz NOT NULL DEFAULT now(),
  "updatedAt"    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS "User_role_status_idx" ON "User" ("role","status");

-- Model B (MARKET_DATA_MODE=byo): each user's OWN Databento API key, encrypted
-- at rest (AES-256-GCM via MARKET_DATA_ENC_KEY). NULL = not connected.
ALTER TABLE "User" ADD COLUMN IF NOT EXISTS "databentoKeyEnc" text;

CREATE TABLE IF NOT EXISTS "Account" (
  "id"              text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  "userId"          text UNIQUE NOT NULL REFERENCES "User"("id") ON DELETE CASCADE,
  "startingBalance" numeric(18,2) NOT NULL,
  "balance"         numeric(18,2) NOT NULL,
  "equity"          numeric(18,2) NOT NULL,
  "dailyPnl"        numeric(18,2) NOT NULL DEFAULT 0,
  "totalPnl"        numeric(18,2) NOT NULL DEFAULT 0,
  "drawdown"        numeric(18,2) NOT NULL DEFAULT 0,
  "highestEquity"   numeric(18,2) NOT NULL,
  "status"          "AccountStatus" NOT NULL DEFAULT 'ACTIVE',
  "createdAt"       timestamptz NOT NULL DEFAULT now(),
  "updatedAt"       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS "Account_status_idx" ON "Account" ("status");
-- Daily-loss anchor: equity at the start of the current trading day (UTC), and
-- the date it belongs to. The risk engine rolls these at the date boundary.
ALTER TABLE "Account" ADD COLUMN IF NOT EXISTS "dayStartEquity" numeric(18,2);
ALTER TABLE "Account" ADD COLUMN IF NOT EXISTS "dayStartAt"     date;
-- Per-challenge boundary. Trades/transactions before this belong to a previous (failed)
-- challenge: the TRADER's own history is scoped to >= this, while admins see everything.
-- Reset bumps it to now(); existing rows backfill to the account's creation date.
ALTER TABLE "Account" ADD COLUMN IF NOT EXISTS "challengeStartedAt" timestamptz;
UPDATE "Account" SET "challengeStartedAt" = "createdAt" WHERE "challengeStartedAt" IS NULL;
ALTER TABLE "Account" ALTER COLUMN "challengeStartedAt" SET DEFAULT now();

CREATE TABLE IF NOT EXISTS "Rule" (
  "id"                 text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  "accountId"          text UNIQUE NOT NULL REFERENCES "Account"("id") ON DELETE CASCADE,
  "maxDailyLoss"       numeric(18,2) NOT NULL,
  "maxDrawdown"        numeric(18,2) NOT NULL,
  "profitTarget"       numeric(18,2) NOT NULL,
  "maxContracts"       integer NOT NULL,
  "allowedInstruments" text[] NOT NULL DEFAULT '{}',
  "updatedAt"          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS "Position" (
  "id"            text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  "accountId"     text NOT NULL REFERENCES "Account"("id") ON DELETE CASCADE,
  "symbol"        text NOT NULL,
  "side"          "PositionSide" NOT NULL,
  "quantity"      integer NOT NULL,
  "averagePrice"  numeric(18,4) NOT NULL,
  "unrealizedPnl" numeric(18,2) NOT NULL DEFAULT 0,
  "realizedPnl"   numeric(18,2) NOT NULL DEFAULT 0,
  "openedAt"      timestamptz NOT NULL DEFAULT now(),
  "updatedAt"     timestamptz NOT NULL DEFAULT now(),
  UNIQUE ("accountId","symbol")
);

CREATE TABLE IF NOT EXISTS "Order" (
  "id"             text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  "accountId"      text NOT NULL REFERENCES "Account"("id") ON DELETE CASCADE,
  "symbol"         text NOT NULL,
  "side"           "OrderSide" NOT NULL,
  "type"           "OrderType" NOT NULL,
  "quantity"       integer NOT NULL,
  "filledQuantity" integer NOT NULL DEFAULT 0,
  "requestedPrice" numeric(18,4),
  "stopPrice"      numeric(18,4),
  "fillPrice"      numeric(18,4),
  "status"         "OrderStatus" NOT NULL DEFAULT 'PENDING',
  "reason"         text,
  "createdAt"      timestamptz NOT NULL DEFAULT now(),
  "updatedAt"      timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS "Order_accountId_status_idx" ON "Order" ("accountId","status");
CREATE INDEX IF NOT EXISTS "Order_symbol_status_idx" ON "Order" ("symbol","status");
-- Bracket (SL/TP) support: an entry order carries its stop-loss/take-profit prices;
-- the resulting exit legs share an ocoGroupId so one filling cancels the other (OCO).
ALTER TABLE "Order" ADD COLUMN IF NOT EXISTS "slPrice"    numeric(18,4);
ALTER TABLE "Order" ADD COLUMN IF NOT EXISTS "tpPrice"    numeric(18,4);
ALTER TABLE "Order" ADD COLUMN IF NOT EXISTS "ocoGroupId" text;
CREATE INDEX IF NOT EXISTS "Order_ocoGroupId_idx" ON "Order" ("ocoGroupId");

CREATE TABLE IF NOT EXISTS "Fill" (
  "id"        text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  "orderId"   text NOT NULL REFERENCES "Order"("id") ON DELETE CASCADE,
  "quantity"  integer NOT NULL,
  "price"     numeric(18,4) NOT NULL,
  "createdAt" timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS "Fill_orderId_idx" ON "Fill" ("orderId");

CREATE TABLE IF NOT EXISTS "Violation" (
  "id"        text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  "accountId" text NOT NULL REFERENCES "Account"("id") ON DELETE CASCADE,
  "type"      "ViolationType" NOT NULL,
  "action"    "ViolationAction" NOT NULL,
  "detail"    text,
  "createdAt" timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS "Violation_accountId_idx" ON "Violation" ("accountId");

CREATE TABLE IF NOT EXISTS "ActivityLog" (
  "id"        text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  "accountId" text REFERENCES "Account"("id") ON DELETE SET NULL,
  "type"      "ActivityType" NOT NULL,
  "message"   text NOT NULL,
  "metadata"  jsonb,
  "ip"        text,
  "createdAt" timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS "ActivityLog_accountId_createdAt_idx" ON "ActivityLog" ("accountId","createdAt");
CREATE INDEX IF NOT EXISTS "ActivityLog_type_idx" ON "ActivityLog" ("type");

CREATE TABLE IF NOT EXISTS "Transaction" (
  "id"          text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  "accountId"   text NOT NULL REFERENCES "Account"("id") ON DELETE CASCADE,
  "type"        "TransactionType" NOT NULL,
  "amount"      numeric(18,2) NOT NULL,
  "description" text,
  "createdAt"   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS "Transaction_accountId_createdAt_idx" ON "Transaction" ("accountId","createdAt");

-- A historical record of every closed (or partially closed) position. The live
-- "Position" row is deleted on full close, so this is the durable trade log the
-- admin "Positions" view reads for closed trades.
CREATE TABLE IF NOT EXISTS "ClosedPosition" (
  "id"          text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  "accountId"   text NOT NULL REFERENCES "Account"("id") ON DELETE CASCADE,
  "symbol"      text NOT NULL,
  "side"        "PositionSide" NOT NULL,
  "quantity"    integer NOT NULL,
  "entryPrice"  numeric(18,4) NOT NULL,
  "exitPrice"   numeric(18,4) NOT NULL,
  "realizedPnl" numeric(18,2) NOT NULL,
  "openedAt"    timestamptz NOT NULL,
  "closedAt"    timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS "ClosedPosition_accountId_closedAt_idx" ON "ClosedPosition" ("accountId","closedAt");
CREATE INDEX IF NOT EXISTS "ClosedPosition_closedAt_idx" ON "ClosedPosition" ("closedAt");

-- Per-trade OPEN lots. The live "Position" row nets every fill of a symbol into one
-- averaged line (what the trader dashboard shows). The internal admin CRM instead needs
-- each entry trade listed separately, so every entry fill records its own lot here;
-- opposing fills consume the oldest lots first (FIFO). Invariant: for one (account,symbol)
-- the lots are all the position's side and their quantities sum to the netted quantity.
CREATE TABLE IF NOT EXISTS "PositionLot" (
  "id"         text PRIMARY KEY DEFAULT gen_random_uuid()::text,
  "accountId"  text NOT NULL REFERENCES "Account"("id") ON DELETE CASCADE,
  "symbol"     text NOT NULL,
  "side"       "PositionSide" NOT NULL,
  "quantity"   integer NOT NULL,
  "entryPrice" numeric(18,4) NOT NULL,
  "openedAt"   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS "PositionLot_account_symbol_idx" ON "PositionLot" ("accountId","symbol","openedAt");

-- Backfill: existing open positions (pre-dating lot tracking) become a single lot each,
-- so they keep showing in the admin view. Only seeds symbols that have no lots yet.
INSERT INTO "PositionLot" ("accountId","symbol","side","quantity","entryPrice","openedAt")
SELECT p."accountId", p."symbol", p."side", p."quantity", p."averagePrice", p."openedAt"
FROM "Position" p
WHERE NOT EXISTS (
  SELECT 1 FROM "PositionLot" l WHERE l."accountId" = p."accountId" AND l."symbol" = p."symbol"
);
