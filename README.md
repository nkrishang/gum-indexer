# gum-indexer

Watches payment addresses for incoming ERC-20 stablecoin transfers, tells your webhook about each payment, and
retires the address once the confirmed total crosses a threshold. Rust, Postgres, QuickNode RPC, deployed on Railway.

Internal service: it has **no public domain and no authentication**. It lives in the same Railway project as
[gum-server](https://github.com/nkrishang/gum-server) and [gum-engine](https://github.com/nkrishang/gum-engine)
and is only reachable over the project's private network (`gum-indexer.railway.internal`).

Supported out of the box (config-only to extend):

| Chain | USDC | USDT | AUSD |
|---|---|---|---|
| Monad (143) | native | USDT0 | native |
| Arbitrum One (42161) | native | USDT0 | – |
| Base (8453) | native | – ¹ | – |

¹ Base has no Tether-issued USDT or USDT0, only a bridged token, so it is deliberately not offered.

## How it works

```
POST /v1/watches ──▶ Postgres (source of truth) ──▶ in-memory watch cache (hydrated at boot)

per chain, isolated and supervised:
  push path     WSS eth_subscribe(logs) filtered on OUR recipients        → payment.pending   (milliseconds)
  sweep path    eth_getLogs(cursor+1 ‥ head-confirmations), one DB tx      → payment.confirmed, threshold.reached,
                per chunk: confirm · orphan · count · retire · move cursor    payment.orphaned
  outbox        FOR UPDATE SKIP LOCKED → signed POST, ordered per watch, retried with backoff
```

* **WSS detects, `eth_getLogs` guarantees.** WebSocket subscriptions lose events on reconnect and can drop them
  silently, so they only ever produce `payment.pending`. Amounts are counted exclusively by the sweep, which reads
  the canonical chain at confirmation depth from a durable cursor. It runs when a pending payment reaches depth,
  when WSS (re)connects, at startup, and on a slow safety tick. A restart, a crash, an RPC outage or a missed
  notification therefore delay a payment; they cannot lose or double count it.
* **Only confirmed amounts count** toward `balance_threshold`, and only transfers after the watch was registered.
  Confirmation depth is per chain and deliberately shallow (Monad 3 blocks ≈ finalized, Arbitrum 12 ≈ 3 s,
  Base 3 ≈ 6 s) — not L1 finality.
* **Credit frugal.** Subscriptions are filtered on the recipient (`topics[2]`), in buckets of addresses, so QuickNode
  only ever notifies us about our own payments. A chain with no watches makes no log queries at all. Above
  `ws_targeted_max` watches the chain switches to a token-wide subscription or to polling (`large_scale_mode`).
  See [docs/runbook.md](docs/runbook.md#credits) for the cost model.
* **Deploy-safe.** Two instances may overlap: a Postgres advisory lock elects one ingester per chain, every write is
  idempotent, the outbox uses `SKIP LOCKED`, and watch registration is ordered against sweeps by a row lock.

## API

No authentication: the private network is the boundary (never give this service a public domain). Amounts are
**base-unit** integers
as strings (USDC has 6 decimals: `"2500000"` = 2.5 USDC).

```
POST /v1/watches
{ "payment_address": "0x…", "chain": "base", "token": "USDC",
  "balance_threshold": "5000000", "webhook_endpoint": "https://you.example/hooks/gum",
  "expires_at": "2026-10-01T00:00:00Z" }            # optional; default TTL 7 days (watch.default_ttl_secs)
```

`blockchain` / `erc20_token` are accepted as aliases for `chain` / `token`; `chain` may be a slug or chain id, `token`
a symbol or contract address from the registry. `201` created · `200` identical active watch already exists
(registration is idempotent) · `409` same address+token active with different parameters · `422` validation error ·
`503` chain still starting.

```
GET    /v1/watches/{id}     watch + its transfers
DELETE /v1/watches/{id}     cancel (idempotent)
GET    /v1/chains           config, tokens and live health per chain
GET    /v1/stats            payments indexed / volume / thresholds reached, per chain and token (durable)
GET    /healthz             liveness — independent of chain health on purpose
GET    /readyz              database reachable and every chain bootstrapped
GET    /metrics             Prometheus
```

### Webhooks

```json
{ "id": "01a0…", "type": "payment.confirmed", "created_at": "…", "sequence": 2,
  "watch": { "id": "…", "chain": "base", "chain_id": 8453, "token": "USDC", "token_address": "0x…",
             "payment_address": "0x…", "balance_threshold": "5000000", "confirmed_amount": "2500000",
             "status": "active" },
  "transfer": { "tx_hash": "0x…", "log_index": 12, "block_number": 123, "block_hash": "0x…",
                "from": "0x…", "amount": "2500000", "status": "confirmed" } }
```

| Type | Meaning |
|---|---|
| `payment.pending` | Seen at chain head. Not counted yet. May be absent if the push path missed it. |
| `payment.confirmed` | Canonical at confirmation depth. Counted; `watch.confirmed_amount` is the new total. |
| `payment.orphaned` | A previously announced pending transfer was reorged out. It was never counted. |
| `threshold.reached` | Confirmed total ≥ threshold. The watch is retired and the address no longer watched. |
| `watch.expired` | The watch hit its expiry first. |

Delivery is **at-least-once** and **ordered per watch** (`sequence`); dedupe on `id`. Respond `2xx` within 10 s.
Failures are retried with exponential backoff and jitter for 24 h, then marked dead (and logged).
Verify `X-Gum-Signature: t=<unix>,v1=<hex>` = `HMAC-SHA256(GUM_WEBHOOK__SECRET, "<t>.<raw body>")` and reject old
timestamps — see `webhook::sign::verify`. Endpoints must be public `https` URLs (private and loopback targets are
refused, at registration and again at DNS resolution), except hosts named in `webhook.host_allowlist`
(`GUM_WEBHOOK__HOST_ALLOWLIST='["gum-server.railway.internal"]'`), which may be private and reached over http.
That is how gum-server, in the same Railway project, is called back without leaving the private network.

## Local mode

Needs Docker, Rust and [Foundry](https://getfoundry.sh) (`anvil`).

```sh
docker compose up -d postgres
cargo run --features testkit --bin gum-devnet          # 3 Anvil chains + mock tokens + a webhook sink on :19000
cp .env.example .env && set -a && . ./.env && set +a
cargo run                                               # GUM_PROFILE=local
# register / pay: gum-devnet prints ready-made curl and `pay` commands
```

Ports 18545–18547 are used so an Anvil already running on 8545 is left alone.

## Tests and benchmarks

```sh
cargo test --all-features                # unit + database (#[sqlx::test]) + end-to-end on Anvil
cargo bench --all-features               # criterion: hot path, cache, decode
cargo run --release --features testkit --bin gum-bench -- --watches 100000 --payments 500 --noise 20
```

The end-to-end suite runs the real service against real chains and injects faults: crash + restart, reorgs, WSS
outage and silent gaps, webhook endpoint outage, RPC rate limits / 5xx / range caps, two overlapping instances,
one chain down next to a healthy one. `gum-bench` fails on any missed or duplicated payment.
Results: [docs/benchmarks.md](docs/benchmarks.md).

## Configuration

`config/default.toml` (chains, tokens, tuning) → `config/<GUM_PROFILE>.toml` → environment (`GUM_<SECTION>__<KEY>`).

| Variable | |
|---|---|
| `DATABASE_URL` | Postgres |
| `GUM_WEBHOOK__SECRET` | HMAC signing secret |
| `GUM_WEBHOOK__HOST_ALLOWLIST` | JSON array of private hosts that may receive webhooks, e.g. `["gum-server.railway.internal"]` |
| `GUM_CHAINS__<CHAIN>__HTTP_URL` / `__WS_URL` | QuickNode endpoints per enabled chain |
| `GUM_CHAINS__<CHAIN>__ENABLED=false` | run without a chain |
| `GUM_QUICKNODE__API_KEY` | optional; exports credits used / remaining |
| `PORT`, `RUST_LOG` | |

At startup each chain verifies `eth_chainId` and every token's `decimals()` against the config and refuses to ingest
on a mismatch. Adding a chain or token is a config change; run `gum-qn-probe limits` against the new endpoint first.

Deployment, alerts, credits and operations: [docs/runbook.md](docs/runbook.md).
