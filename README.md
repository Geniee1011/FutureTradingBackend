# TradingBackend

Real-time **futures** market-data backend for the Trader Portal. Streams
**ES, NQ, CL, GC** to the frontend over WebSocket and serves chart history over
REST. Data comes from **Databento** (CME Globex, dataset `GLBX.MDP3`); with no
API key it runs a built-in **simulation** so the whole stack works offline.

Node + TypeScript · `ws` · Databento Historical HTTP API.

## Quick start

```bash
npm install
cp .env.example .env     # optional — add DATABENTO_API_KEY for live data
npm run dev              # http://localhost:8000  (tsx watch)
# or: npm run build && npm start
```

Then run the frontend (`TradingApp`) with `NEXT_PUBLIC_WS_URL=ws://localhost:8000/ws`
(already set in `TradingApp/.env.local`). Start this backend **first**, then the
frontend.

| Mode | Trigger | Behaviour |
| --- | --- | --- |
| **Simulation** | `DATABENTO_API_KEY` empty | Random-walk prices for ES/NQ/CL/GC. No account needed. |
| **Databento** | `DATABENTO_API_KEY` set | Real CME data via Databento Historical HTTP API. |

## Endpoints

| Endpoint | Description |
| --- | --- |
| `GET /health` | Status, active provider, symbols, connected clients. |
| `GET /api/history?symbol=ES&resolution=60&count=240` | OHLCV candles (epoch-seconds `time`). `resolution` in seconds. |
| `GET /api/instruments` | Instrument list with category + resolved dated contract code (e.g. `ES → ESM6`). |
| `POST /api/auth/login` | `{ email, password }` → `{ token, user }` (JWT). |
| `POST /api/auth/register` | `{ name, email, password }` → `{ token, user }`. |
| `GET /api/auth/me` | `Authorization: Bearer <token>` → `{ user }`. |
| `WS /ws` | Live stream. |

### Auth (JWT)

`POST /api/auth/login` verifies the password (scrypt via `node:crypto`, no native
deps) and returns a signed **HS256 JWT** plus the public user. Send it as
`Authorization: Bearer <token>` to protected endpoints; validate with
`/api/auth/me`. Config: `JWT_SECRET` (set in production) and `JWT_EXPIRES_IN_SEC`
(default 7 days).

Users live behind a `UserStore` interface (`src/auth/users.ts`): **`PgUserStore`
(PostgreSQL)** when `DATABASE_URL` is set, else a seeded in-memory store. Demo
users: `admin@demo.com` / `trader@demo.com`, password `demo`.

### Database (PostgreSQL)

```bash
# .env
DATABASE_URL="postgresql://user:pass@localhost:5432/FutureTradingApp"

npm run db:migrate   # create tables (User, Account, Rule, Order, Fill, …)
npm run db:seed      # insert demo users + a $50k evaluation account/rule
```

> **Why not Prisma?** The schema is authored in `prisma/schema.prisma`, but
> Prisma's prebuilt **native engine crashes with `Illegal instruction` (SIGILL)
> on this CPU** — it requires instruction-set extensions this hardware lacks, so
> `prisma migrate`/queries can't run here. We therefore use the pure-JS **`pg`**
> driver instead (`src/db/`), with SQL (`src/db/schema.sql`) translated 1:1 from
> the Prisma models — **same table/column names**, so it stays Prisma-compatible
> if you later run on a supported CPU. The data layer (`getPool`, `PgUserStore`)
> is all `pg`; nothing imports the Prisma client at runtime.

### Instruments & contract codes

Ten products (E-mini + Micro): **ES/MES, NQ/MNQ, YM/MYM, CL/MCL, GC/MGC**. We
trade by root and Databento `.v.0` resolves the most-active dated contract.
`/api/instruments` returns the human contract code via a **two-step symbology
resolve** (`continuous → instrument_id → raw_symbol`; the direct combo is
unsupported). This matters: the volume-active gold contract resolves to **GCQ6**
(Aug), *not* the calendar-front GCM6 — so the code shown always matches the data
served. In simulation mode (no key) codes fall back to a date-based approximation.

Quote polling is **subscriber-driven**: the provider only polls symbols a client
has actually subscribed to (per-symbol), so listing 10 instruments doesn't poll
all 10 — only the watchlist + selected symbol.

**WebSocket protocol** (matches `TradingApp/src/lib/types.ts`):

```jsonc
// client → server
{ "type": "subscribe",   "channel": "quotes" }
{ "type": "subscribe",   "channel": "orderbook", "symbol": "NQ" }
{ "type": "unsubscribe", "channel": "orderbook", "symbol": "NQ" }

// server → client
{ "type": "quote",     "data": { "symbol":"ES", "price":..., "bid":..., "ask":..., "change24h":..., "high24h":..., "low24h":..., "volume24h":..., "ts":... } }
{ "type": "orderbook", "data": { "symbol":"NQ", "bids":[...], "asks":[...], "ts":... } }
```

## How the Databento integration works

No official Databento **Node** SDK exists, so this uses the **Historical HTTP
API** (`hist.databento.com/v0/timeseries.get_range`, HTTP Basic with the API key
as username):

- **Quotes** — a fast loop (`QUOTE_POLL_MS`, default 1.5 s) reads the latest
  `ohlcv-1s` bar close for each symbol → near-real-time price; a 30 s loop sums
  `ohlcv-1h` over a rolling 24 h for open/high/low/volume (→ `change24h`). Until
  the 24 h stats land, quotes report neutral values (not seed-derived garbage).
- **History** — `getHistory` serves `ohlcv-1s|1m|1h|1d` directly and aggregates
  1-minute bars for 5 m / 15 m.
- **Symbology** — internal codes map to **volume-based** continuous contracts
  (`ES`→`ES.v.0`, …, `GC`→`GC.v.0`, `stype_in=continuous`) — the *most-actively-
  traded* month, rolled automatically. We use `.v.0` not calendar-front `.c.0`
  because for metals the nearest-expiry month is often illiquid (`GC.c.0` reports
  ~zero volume; `GC.v.0` tracks the real active contract). See `src/instruments.ts`.
- Prices are decoded from Databento fixed-point (`raw / 1e9`).

### Real-time note & upgrade path

Validated against live ES/NQ/CL/GC data. Two things to know:

- **Latency.** The Historical API is *not* real-time — it trails live by the
  dataset's ingestion lag plus your account's entitlement. Measured **~30 min**
  on this account/dataset (a key sign the account has **delayed**, not real-time,
  CME data). Query windows are **anchored to the dataset's available end** (free
  `metadata.get_dataset_range`, cached 5 s) — Databento returns HTTP 422 if `end`
  exceeds available data, so we clamp to it (`DatabentoClient.availableEnd`).
  Quotes carry the **data timestamp** (`priceTs`), not wall clock, so the chart's
  forming candle lines up with history instead of jumping ~30 min ahead.
  The chart updates as availability advances — but delayed. **For true real-time
  you need the Live API _and_ a real-time CME subscription** (see below).
- For **true tick-by-tick**, Databento's *Live* feed is a raw **TCP** gateway
  speaking **DBN** (binary) with **CRAM** auth — no WebSocket, no Node SDK, so
  it's a larger lift. The provider interface (`src/providers/provider.ts`) is
  designed for it: implement a `LiveProvider` with the same `start/stop/getHistory`
  surface and swap it in `src/index.ts`. Nothing else changes.

> On a cold start a symbol's 24h stats can take a poll cycle (up to ~30 s) to
> appear if its first hourly request is slow; the quote streams immediately with
> a real price and neutral 24h fields until then.

> Live CME data also requires a **CME market-data subscription** on your
> Databento account, in addition to the API key.

### Order book

`mbp-1`/`mbp-10` depth isn't polled yet; the book emitted for a watched symbol is
a **synthetic** ladder around the live price (`synthBook` in `provider.ts`) so the
UI is populated. Replace with real `mbp-10` records when you wire the depth feed.

## Project layout

```
src/
  index.ts              # boot: pick provider, start hub + server
  config.ts             # env (PORT, DATABENTO_API_KEY, …)
  instruments.ts        # ES/NQ/CL/GC + continuous symbology map
  types.ts              # wire contracts (shared shape with frontend)
  core/hub.ts           # client subscriptions + fan-out
  server/server.ts      # HTTP (history/health) + WebSocket (/ws) + CORS + heartbeat
  providers/
    provider.ts         # MarketDataProvider interface + base + synthBook
    simulation.ts       # zero-config random-walk feed
    databento.ts        # Databento Historical HTTP provider
  databento/client.ts   # timeseries.get_range client (auth, JSON, price scaling)
```

## Configuration

See `.env.example`. Key vars: `PORT` (8000), `CORS_ORIGIN`
(`http://localhost:3000`), `MARKET_DATA_MODE` (`shared`), `DATABENTO_API_KEY`,
`DATABENTO_DATASET` (`GLBX.MDP3`), `QUOTE_POLL_MS` (1500).

## Market-data models

`MARKET_DATA_MODE` selects how market data is delivered **and licensed**, so you
can switch models without code changes (just an env var + redeploy):

| Mode | Model | How it works | Licensing |
| --- | --- | --- | --- |
| `shared` *(default)* | **A** | One master `DATABENTO_API_KEY`; the backend fans the feed out to all users via the shared `MarketHub`. | Counts as **redistribution** → needs a redistribution/vendor license. |
| `byo` | **B** | Each user brings their **own** Databento account; the app streams only the data their own license covers. | No redistribution (per-user entitlements). |

The switch point lives in `buildProvider()` ([src/index.ts](src/index.ts)): `shared`
runs the current single-key path (Databento Live / Historical, or Simulation when
no key is set); `byo` is where the per-user provider slots in.

**Model B (`byo`) is implemented end-to-end** (historical/delayed MVP):

- Each user's Databento key is stored **encrypted at rest** (`databentoKeyEnc`
  column, AES-256-GCM via `MARKET_DATA_ENC_KEY`) and validated against Databento
  before saving.
- Chart **history** (`GET /api/history`) and the **polled latest price**
  (`GET /api/market-data/quote`) are served per-request with the **requesting
  user's own key** — never shared, so no redistribution.
- Connection endpoints: `GET/POST/DELETE /api/market-data/connection`.
- Frontend: users connect/disconnect via the avatar menu → **Databento account**;
  the chart shows a **"Connect your Databento account"** gate until they do.
- In `byo`, the shared provider carries no market data — it only backs the
  **simulated execution** engine (order fills / eval run on the simulation, while
  charts are the user's real data). Per-user fill pricing is a future follow-up.

> **Before switching a real deployment to `byo`**, get Databento's **written**
> confirmation that BYO-key access avoids the redistribution classification (see
> the licensing email in the project notes). Both models share the one
> `buildProvider()` seam, so toggling between them — or rolling back to `shared` —
> is a flag flip, not a code change. (Rolling back to `shared` re-introduces
> redistribution, so it requires holding the redistribution license then.)

## Deployment (Railway + Vercel)

Managed-PaaS topology: **backend + Postgres on Railway**, **frontend on Vercel**.
The backend must run as a **single always-on process** — it holds a persistent
Databento socket, in-memory engine state (quotes, account caches, the resting-order
monitor), and pooled Postgres connections — so it can't go serverless. The
`railway.json` in this repo pins it to **one replica** with a `/health` check.

### Backend → Railway

1. **New Project → Deploy from `FutureTradingBackend`.** Railway reads
   `railway.json` (Nixpacks build `npm run build`, start `npm run start`,
   healthcheck `/health`, 1 replica).
2. **Add Postgres:** *+ New → Database → PostgreSQL.* Railway injects
   `DATABASE_URL` into the service automatically.
3. **Set service variables** (Settings → Variables):

   | Variable | Value |
   | --- | --- |
   | `JWT_SECRET` | a long random string (**not** the dev default) |
   | `DATABENTO_API_KEY` | your Databento key |
   | `DATABENTO_LIVE` | `1` for the live feed, omit for delayed/historical |
   | `CORS_ORIGIN` | `https://<your-app>.vercel.app` (lock it down) |
   | `DATABENTO_DATASET` | `GLBX.MDP3` (default — optional) |

   **Do not set `PORT`** — Railway injects it and the server already reads
   `process.env.PORT`.
4. **Initialize the database once** (Railway shell or a one-off command):

   ```bash
   npm run db:migrate   # idempotent — safe to re-run
   npm run db:seed      # demo users + a $50k eval account (optional)
   ```

5. Your service URL is `https://<svc>.up.railway.app`; the WebSocket is
   **`wss://<svc>.up.railway.app/ws`**.

> **Optional zero-touch migrations.** To auto-apply the schema on every deploy,
> change `startCommand` in `railway.json` to `npm run db:migrate && npm run start`.
> Safe because the schema is fully idempotent (every `CREATE`/`ALTER` is guarded).
> Relies on `tsx` at runtime — keep dev dependencies installed (Nixpacks does by
> default).

### Frontend → Vercel

1. **New Project → import `FutureTradingApp`** (Next.js auto-detected; defaults are fine).
2. **Environment variable:**

   | Variable | Value |
   | --- | --- |
   | `NEXT_PUBLIC_WS_URL` | `wss://<svc>.up.railway.app/ws` |

   The app derives the REST base from this var (`ws→http`, strip `/ws`), so the
   backend must serve REST **and** WS on the **same host** (it does). Leave
   `NEXT_PUBLIC_TV_ADVANCED` unset unless you've added the licensed TradingView
   library.
3. **Deploy** → `https://<your-app>.vercel.app`.

### Deploy order & gotchas

1. **Backend + Postgres first** → copy the Railway URL → set `NEXT_PUBLIC_WS_URL`
   on Vercel → deploy the frontend.
2. **WSS is required.** Once the frontend is on HTTPS, the socket must be `wss://`
   or browsers block it as mixed content. Railway provides TLS automatically.
3. **Secrets live in the dashboards, never in git.** Both repos are public; `.env`
   (paid Databento key + JWT secret) must stay gitignored.
4. **Don't scale the backend past 1 replica** and don't use a tier that **sleeps
   on idle** — it holds the live feed and in-memory caches (`railway.json` pins
   `numReplicas: 1`). A cold start drops the Databento socket and wipes state.
5. **After re-seeding a running backend, restart the service** — the account-stream
   caches positions in memory at boot, so a reseed against a live process leaves
   stale equity/drawdown until restart.
6. **Node 20+ LTS.** Pin with a `.nvmrc` or `"engines": { "node": ">=20" }` in
   `package.json` if Railway selects an older default.
