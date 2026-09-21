# Runbook

## Deploying on Railway

1. **Postgres**: add Railway's PostgreSQL to the project. Reference it from the service as
   `DATABASE_URL=${{Postgres.DATABASE_URL}}` (private network). Turn on volume backups.
2. **Service**: deploy this repo. Railway builds the `Dockerfile` (cargo-chef layers; code-only changes rebuild fast).
3. **Variables**: `GUM_API__KEYS`, `GUM_WEBHOOK__SECRET`, and per chain `GUM_CHAINS__MONAD__HTTP_URL`,
   `GUM_CHAINS__MONAD__WS_URL` (same for `ARBITRUM`, `BASE`). Optional `GUM_QUICKNODE__API_KEY`.
   A chain you have no endpoint for yet: `GUM_CHAINS__<CHAIN>__ENABLED=false`.
4. **Service settings** (`railway.toml` carries them; mirror them if the project uses Railway's TypeScript IaC —
   Railway has announced config-as-code files stop being read on 2026-12-01, check before relying on the file):

   | Setting | Value | Why |
   |---|---|---|
   | Healthcheck path | `/healthz` | Liveness only. It does not fail on chain/RPC trouble — a restart cannot fix that and would add downtime. |
   | Restart policy | on failure | |
   | Draining seconds | `30` | Default is 0 = SIGKILL straight after SIGTERM. We use the time to finish the in-flight sweep transaction and webhook posts and to release chain leadership. |
   | Overlap seconds | `0` | Overlap is safe (see below) but buys nothing. |
   | Replicas | `1` | More are safe; only one ingests per chain, all serve the API and dispatch webhooks. |
   | Volume | none | State lives in Postgres. A volume would forbid replicas and force downtime on every deploy. |

5. **Metrics**: Railway does not scrape application metrics. Deploy the "Grafana Stack" (or any Prometheus) template
   in the same project and scrape `http://<service>.railway.internal:8080/metrics` over the private network. Do not
   expose `/metrics` publicly if you can avoid it (it is unauthenticated); `/v1/*` is what needs the public domain.
6. **QuickNode hardening**: enable token/JWT auth on the endpoints. With Railway static outbound IPs (Pro) you can
   also IP-allow-list them.

The service binds `[::]:$PORT` (works on IPv6-only private networks), retries its first database connection (private
DNS can lag at boot), runs migrations itself, and logs JSON to stdout (Railway parses level + attributes).

### What happens during a deploy

New instance starts → healthcheck passes → Railway SIGTERMs the old one. While both run: only the holder of the
chain's Postgres advisory lock ingests; the other serves the API, and watches registered there reach the ingester via
`LISTEN/NOTIFY` (and, as a backstop, via the sweep's in-transaction delta load). On SIGTERM the old instance stops
accepting HTTP, finishes its sweep transaction, drains webhook posts and releases the locks; the new instance takes
over within ~2 s and catches up from the durable cursor. Nothing is lost or counted twice (covered by
`two_instances_never_double_count_and_fail_over` and `crash_and_restart_recovers_payments_made_during_downtime`).

## Before going live: measure what QuickNode does not document

```sh
cargo run --release --features testkit --bin gum-qn-probe -- limits  --http-url … --ws-url … --token <USDC on that chain>
cargo run --release --features testkit --bin gum-qn-probe -- billing --ws-url … --token <USDC> --seconds 300 --admin-key … --credits-per-call 20
```

* `limits` → set `ws_bucket_size`, `ws_targeted_max`, `max_log_range` per chain from its suggestion. Defaults are
  conservative (500 recipients per subscription, 5,000 watches in targeted mode, Monad range 100).
* `billing` → QuickNode documents two incompatible WSS billing models (per notification vs. metered per 0.1 MB), both
  only on Solana pages. The probe runs a token-wide subscription for a few minutes and diffs the account's usage.
  **Metered** → `large_scale_mode = "ws_firehose"`. **Per notification** → keep `large_scale_mode = "poll"` (the
  default). Recipient-filtered mode is cheap under either model, so this only matters above `ws_targeted_max`.

## Credits

Flat per call: 20 credits (Arbitrum, Base), 30 (Monad); range and result size do not matter. Per chain, per month:

| Component | Arbitrum / Base | Monad |
|---|---|---|
| Chain with no watches: health probe every 60 s | 0.86M | 1.30M |
| Chain with watches: safety sweep every 30 s (2 calls) | 3.5M | 5.2M |
| Per payment, targeted mode: 1 notification + confirm sweep (shared by payments close together) | ≈ 60 | ≈ 90 |
| Registration batch / bucket compaction: subscribe + unsubscribe | 40 | 60 |
| Poll mode (fallback or `large_scale_mode = "poll"`): `poll_interval_ms` 2000 / 3000 | +25.9M | +25.9M |

Everything idle ≈ 3M/month; three active chains in targeted mode ≈ 12M + payments; three chains polling ≈ 90M
(Build plan: 80M, then $0.62/M). Knobs: `safety_sweep_interval_ms`, `poll_interval_ms`, `idle_probe_interval_ms`,
`confirmations`, and the default watch TTL — an unpaid watch keeps its chain active, so expired watches matter.

Watch `gum_rpc_credits_estimated_total` (local estimate, by chain and method) against
`gum_quicknode_credits_used` / `_remaining` (Admin API, hourly). WSS notifications are not in the local estimate.

## Alerts worth having

| Alert | Expression (sketch) | Meaning |
|---|---|---|
| Chain down | `gum_chain_status == 0` for 2m | RPC failing or head not advancing: payments on that chain are not detected. They will be recovered when it returns. |
| Chain degraded | `gum_chain_status == 1` for 15m | Running on the poll fallback or intermittent RPC errors. |
| Nobody leads | `sum by (chain) (gum_chain_leader) == 0` for 1m | No instance holds the chain's lock. |
| Sweep lag | `gum_chain_head_block - gum_chain_confirmed_block` far above `confirmations` while watches > 0 | Sweeps failing or starved. |
| Push path leaking | `increase(gum_transfers_missed_by_push_total[1h]) > 0` outside restarts | WSS is dropping notifications; payments still confirm, `pending` is late or absent. |
| Webhooks stuck | `gum_outbox_oldest_age_seconds > 300` | A consumer endpoint is failing. |
| Webhooks dead | `increase(gum_outbox_dead[1h]) > 0` | Events given up on after 24 h. |
| Credits | `gum_quicknode_credits_remaining` below 30 / 15 / 5 % | |
| Restarts | `increase(gum_task_restarts_total[15m]) > 3` | An ingest task keeps failing. |
| Token event changed | `increase(gum_logs_anomalous_total[1h]) > 0` | A registry token emitted a Transfer with an unexpected shape (upgradeable proxies!). |

## Logs

State transitions and anomalies are logged; routine events are counted. Every error has a stable `error.kind`
(`rpc_rate_limited`, `rpc_auth`, `rpc_range_too_large`, `ws_disconnected`, `ws_lagged`, `push_missed_transfer`,
`cursor_moved`, `webhook_failed`, `webhook_dead`, `webhook_host_parked`, `config_chain_id_mismatch`,
`config_token_decimals_mismatch`, `leadership_lost`, `task_panic`, `db_*`, …). Repeating conditions are rate limited
(first occurrence, then a summary with a `suppressed` count every 30 s), which keeps an outage far below Railway's
500 lines/s cap. Per-transfer detail: `RUST_LOG=gum_indexer=debug`.

## Operations

* **Add a token / chain**: add it to `config/default.toml` (or env), run `gum-qn-probe limits`, deploy. The startup
  check refuses to ingest if `chainId` or `decimals()` disagree with the config.
* **Replay a dead webhook**: `UPDATE webhook_outbox SET status='pending', next_attempt_at=now() WHERE id='…';`
* **Inspect a watch**: `GET /v1/watches/{id}` lists its transfers with status `pending | confirmed | orphaned | ignored`
  (`ignored` = canonical, but the watch was already retired).
* **Accepted risk**: a reorg deeper than the chain's `confirmations` is not rolled back. Raise `confirmations` to
  trade latency for safety; Monad's 3 is already finalized.
* **`start_block`**: a watch counts transfers from the block after the latest head this process had seen at
  registration, never at or below the durable cursor. On a quiet chain that head can be up to one safety-sweep
  interval old, so a transfer made seconds *before* registration may count. Payment addresses are normally fresh,
  so this only ever errs in the payer's favour.
