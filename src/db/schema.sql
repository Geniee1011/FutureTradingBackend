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

-- ---- Extended per-account rule fields (added v2) ----
ALTER TABLE "Rule" ADD COLUMN IF NOT EXISTS "minTradingDays"           integer     NOT NULL DEFAULT 5;
ALTER TABLE "Rule" ADD COLUMN IF NOT EXISTS "maxDailyProfitPct"        numeric(5,2) NOT NULL DEFAULT 30;
ALTER TABLE "Rule" ADD COLUMN IF NOT EXISTS "maxRiskPerTrade"          numeric(18,2) NOT NULL DEFAULT 0;
ALTER TABLE "Rule" ADD COLUMN IF NOT EXISTS "maxPositionUnits"         numeric(5,1) NOT NULL DEFAULT 0;
ALTER TABLE "Rule" ADD COLUMN IF NOT EXISTS "stopLossRequired"         boolean     NOT NULL DEFAULT false;
ALTER TABLE "Rule" ADD COLUMN IF NOT EXISTS "minHoldTimeSecs"          integer     NOT NULL DEFAULT 0;
ALTER TABLE "Rule" ADD COLUMN IF NOT EXISTS "overnightHoldsProhibited" boolean     NOT NULL DEFAULT false;
ALTER TABLE "Rule" ADD COLUMN IF NOT EXISTS "weekendHoldsProhibited"   boolean     NOT NULL DEFAULT false;
-- Drawdown style: 'INTRADAY' = trailing floor ratchets in real time on unrealized P&L
-- (challenge accounts); 'EOD' = floor only updates once at session close, using the
-- highest intraday equity reached that day (funded accounts).
ALTER TABLE "Rule" ADD COLUMN IF NOT EXISTS "drawdownType"            text        NOT NULL DEFAULT 'INTRADAY';

-- Daily-limit pause (closes positions + blocks new orders for the rest of the trading day).
-- Does NOT fail the challenge — the account stays ACTIVE; the date clears automatically
-- at the next day boundary.
ALTER TABLE "Account" ADD COLUMN IF NOT EXISTS "tradingPausedAt" date;

-- Which evaluation phase (1 or 2) this account is currently in. Phase 1 → Phase 2
-- on auto-advance; Phase 2 PASSED goes to manual admin review for funded upgrade.
ALTER TABLE "Account" ADD COLUMN IF NOT EXISTS "challengePhase" smallint NOT NULL DEFAULT 1;

-- EOD trailing drawdown state.
-- peakIntradayEquity: highest equity (balance + unrealized) reached in the CURRENT
--   session; resets each day. Used to snapshot the EOD peak at session close.
-- eodPeakEquity: the banked end-of-day peak — the basis for the EOD floor
--   (floor = eodPeakEquity - maxDrawdown), fixed for the whole trading day.
ALTER TABLE "Account" ADD COLUMN IF NOT EXISTS "peakIntradayEquity" numeric(18,2);
ALTER TABLE "Account" ADD COLUMN IF NOT EXISTS "eodPeakEquity"      numeric(18,2);

-- Global rule templates: one row per account tier. Admin edits these and the
-- changes cascade to every Account.ruleTemplateId-linked per-account Rule row.
-- The risk engine still reads per-account Rule rows (no risk-engine change needed).
CREATE TABLE IF NOT EXISTS "RuleTemplate" (
  "id"                 text PRIMARY KEY,
  "label"              text NOT NULL,
  "phase"              text NOT NULL,
  "accountSize"        numeric(18,2) NOT NULL,
  "sortOrder"          integer NOT NULL DEFAULT 0,
  "maxDailyLoss"       numeric(18,2) NOT NULL,
  "maxDrawdown"        numeric(18,2) NOT NULL,
  "profitTarget"       numeric(18,2) NOT NULL DEFAULT 0,
  "maxContracts"       integer NOT NULL DEFAULT 3,
  "minTradingDays"     integer NOT NULL DEFAULT 5,
  "maxDailyProfitPct"  numeric(5,2) NOT NULL DEFAULT 30,
  "maxRiskPerTrade"    numeric(18,2) NOT NULL DEFAULT 0,
  "maxPositionUnits"   numeric(5,1) NOT NULL DEFAULT 0,
  "stopLossRequired"   boolean NOT NULL DEFAULT false,
  "minHoldTimeSecs"    integer NOT NULL DEFAULT 0,
  "overnightHoldsProhibited" boolean NOT NULL DEFAULT false,
  "weekendHoldsProhibited"   boolean NOT NULL DEFAULT false,
  "drawdownType"       text NOT NULL DEFAULT 'INTRADAY',
  "allowedInstruments" text[] NOT NULL DEFAULT '{}',
  "updatedAt"          timestamptz NOT NULL DEFAULT now()
);

-- ---- Extended RuleTemplate fields (mirrors Rule additions above) ----
ALTER TABLE "RuleTemplate" ADD COLUMN IF NOT EXISTS "minTradingDays"           integer     NOT NULL DEFAULT 5;
ALTER TABLE "RuleTemplate" ADD COLUMN IF NOT EXISTS "maxDailyProfitPct"        numeric(5,2) NOT NULL DEFAULT 30;
ALTER TABLE "RuleTemplate" ADD COLUMN IF NOT EXISTS "maxRiskPerTrade"          numeric(18,2) NOT NULL DEFAULT 0;
ALTER TABLE "RuleTemplate" ADD COLUMN IF NOT EXISTS "maxPositionUnits"         numeric(5,1) NOT NULL DEFAULT 0;
ALTER TABLE "RuleTemplate" ADD COLUMN IF NOT EXISTS "stopLossRequired"         boolean     NOT NULL DEFAULT false;
ALTER TABLE "RuleTemplate" ADD COLUMN IF NOT EXISTS "minHoldTimeSecs"          integer     NOT NULL DEFAULT 0;
ALTER TABLE "RuleTemplate" ADD COLUMN IF NOT EXISTS "overnightHoldsProhibited" boolean     NOT NULL DEFAULT false;
ALTER TABLE "RuleTemplate" ADD COLUMN IF NOT EXISTS "weekendHoldsProhibited"   boolean     NOT NULL DEFAULT false;
ALTER TABLE "RuleTemplate" ADD COLUMN IF NOT EXISTS "drawdownType"             text        NOT NULL DEFAULT 'INTRADAY';

-- Seed / refresh the 9 standard account tiers with spec-correct values.
-- DO UPDATE so a fresh deploy always applies the latest spec values; admins
-- can still override via the UI (values reset on next deploy — expected during setup).
INSERT INTO "RuleTemplate" (
  "id","label","phase","accountSize","sortOrder",
  "maxDailyLoss","maxDrawdown","profitTarget","maxContracts",
  "minTradingDays","maxDailyProfitPct","maxRiskPerTrade","maxPositionUnits",
  "stopLossRequired","minHoldTimeSecs","overnightHoldsProhibited","weekendHoldsProhibited","drawdownType"
) VALUES
--                                                         dly   dd    tgt  ctrs  days  pct   risk   units  sl    secs  ovnt  wknd  ddType
  ('c1_50k',  'Challenge Phase 1 — $50,000',  'Challenge Phase 1', 50000,    1, 1000,  2000,  1500, 3, 5, 30,  500,  3.0, true,  15, true,  true, 'INTRADAY'),
  ('c1_100k', 'Challenge Phase 1 — $100,000', 'Challenge Phase 1', 100000,   2, 2000,  4000,  3000, 3, 5, 30,  1000, 3.0, true,  15, true,  true, 'INTRADAY'),
  ('c2_50k',  'Challenge Phase 2 — $50,000',  'Challenge Phase 2', 50000,    3, 1000,  1500,  3000, 3, 5, 30,  500,  3.0, true,  15, true,  true, 'INTRADAY'),
  ('c2_100k', 'Challenge Phase 2 — $100,000', 'Challenge Phase 2', 100000,   4, 2000,  3000,  6000, 3, 5, 30,  1000, 3.0, true,  15, true,  true, 'INTRADAY'),
  ('f_50k',   'Funded — $50,000',             'Funded',            50000,    5, 1000,  2000,  5000,   5, 10, 30,  250,   5.0, true,  15, true,  true, 'EOD'),
  ('f_100k',  'Funded — $100,000',            'Funded',            100000,   6, 2000,  4000,  10000,  8, 10, 30,  500,   8.0, true,  15, true,  true, 'EOD'),
  ('f_250k',  'Funded — $250,000',            'Funded',            250000,   7, 5000,  10000, 25000, 15, 10, 30,  1250, 15.0, true,  15, true,  true, 'EOD'),
  ('f_500k',  'Funded — $500,000',            'Funded',            500000,   8, 10000, 20000, 50000, 20, 10, 30,  2500, 20.0, true,  15, true,  true, 'EOD'),
  ('f_1m',    'Funded — $1,000,000',          'Funded',            1000000,  9, 20000, 40000, 100000,30, 10, 30,  5000, 30.0, true,  15, true,  true, 'EOD')
ON CONFLICT ("id") DO UPDATE SET
  "maxDailyLoss"            = EXCLUDED."maxDailyLoss",
  "maxDrawdown"             = EXCLUDED."maxDrawdown",
  "profitTarget"            = EXCLUDED."profitTarget",
  "maxContracts"            = EXCLUDED."maxContracts",
  "minTradingDays"          = EXCLUDED."minTradingDays",
  "maxDailyProfitPct"       = EXCLUDED."maxDailyProfitPct",
  "maxRiskPerTrade"         = EXCLUDED."maxRiskPerTrade",
  "maxPositionUnits"        = EXCLUDED."maxPositionUnits",
  "stopLossRequired"        = EXCLUDED."stopLossRequired",
  "minHoldTimeSecs"         = EXCLUDED."minHoldTimeSecs",
  "overnightHoldsProhibited"= EXCLUDED."overnightHoldsProhibited",
  "weekendHoldsProhibited"  = EXCLUDED."weekendHoldsProhibited",
  "drawdownType"            = EXCLUDED."drawdownType";

-- Link each Account to the tier whose rules it inherits.
-- Set at account creation / upgrade; cascade propagates template edits to per-account Rule rows.
ALTER TABLE "Account" ADD COLUMN IF NOT EXISTS "ruleTemplateId" text REFERENCES "RuleTemplate"("id");
