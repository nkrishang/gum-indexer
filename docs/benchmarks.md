# Benchmarks

Apple Silicon laptop, release build, Postgres 17 in Docker (`fsync=off`, see `docker-compose.yml`), Anvil 1.5.1.
Reproduce with the commands shown; treat the numbers as a baseline for regressions, not as production figures.

## End to end — `gum-bench`

Real service, real chain, real Postgres, real HTTP webhooks. Anvil automines, so a transaction is mined the moment
it is submitted; latency is **transaction submitted → webhook received** and therefore an upper bound on
detection + persistence + signing + delivery. Every run asserts zero missed and zero duplicated payments.

`--payments 500`, 2 confirmations, expected block time 100 ms (the confirmed figures are dominated by that
configured depth delay, not by processing):

| Watches | Ingest mode | Boot incl. hydration | `payment.pending` p50 / p99 / max | `payment.confirmed` p50 / p99 | HTTP RPC calls per payment |
|---|---|---|---|---|---|
| 2,000 | `ws_targeted`, 100 payments/s | 49 ms | **3.6 / 9.2 / 20 ms** | 165 / 274 ms | 0.10 |
| 100,000 | `ws_firehose`, 50 payments/s + 1,000 unwatched transfers/s | 300 ms | 10.8 / 19.2 / 22 ms | 197 / 357 ms | 0.15 |
| 100,000 | `poll` (100 ms interval), same load | 297 ms | 72 / 133 / 141 ms | 247 / 423 ms | 0.30 |
| 1,000,000 | `ws_firehose`, same load | 1.75 s | 8.8 / 14.1 / 21 ms | 337 / 595 ms | 0.08 |

```sh
cargo run --release --features testkit --bin gum-bench -- --watches 2000 --mode ws_targeted --payments 500 --rate 100 --noise 0
cargo run --release --features testkit --bin gum-bench -- --watches 100000 --large-scale-mode ws_firehose --payments 500 --rate 50 --noise 20
cargo run --release --features testkit --bin gum-bench -- --watches 100000 --large-scale-mode poll --payments 500 --rate 50 --noise 20
cargo run --release --features testkit --bin gum-bench -- --watches 1000000 --large-scale-mode ws_firehose --payments 300 --rate 50 --noise 20
```

Notes
* Calls per payment fall as payments cluster, because one confirmation sweep covers every payment that reached
  depth. An isolated payment costs about 2 calls (`eth_blockNumber` + `eth_getLogs`).
* In poll mode some payments are confirmed by a sweep before the poller sees them, so they get `payment.confirmed`
  without a `payment.pending` (478 of 500 had one in the run above). By design.
* Pushing `--noise` to 100+ saturates Anvil's EVM, not the service; the latency then measures Anvil's queue.

Found by this benchmark and fixed: a lock-order deadlock between the head path and the sweep on a shared stats row
(the head path no longer touches any row shared between watches), and a sweep range that kept re-probing a
provider's block-range limit with billable rejected calls (the limit is now read from the error and remembered).

## Hot path — `cargo bench --all-features`

| Benchmark | Result |
|---|---|
| `match_log`, 10k logs vs 1k / 100k / 1M watches | 19.7 / 20.1 / 23.8 ns per log (≈ 42–50M logs/s per core) |
| lookup in 1M entries: papaya / dashmap / std `HashMap` | 18.8 / 12.9 / 17.5 ns |
| hydrate 100k watches into the cache | 11 ms |
| register one watch into a 1M-entry cache | 146 ns |
| decode a 10k-log `eth_getLogs` JSON response (alloy `Log`) | 8.6 ms (≈ 860 ns per log, 660 MiB/s) |

Decisions taken from these numbers
* Watchlist size is irrelevant to matching cost (1k → 1M watches: +20 %), so in-process recipient matching is the
  right large-scale strategy.
* dashmap is ~30 % faster than papaya single-threaded, but the map is 2 % of the per-log cost; papaya stays for its
  lock-free reads (no shard locks to hold across an `.await`, no reader/writer contention with registrations).
* JSON decoding dominates (40× the match). At stablecoin volumes that is still microseconds per second of chain
  time, and a 10k-log catch-up chunk decodes in under 10 ms, so a hand-written borrowed decoder is not worth its
  maintenance cost today. Revisit if `ws_firehose` is ever run on a token with > 10k transfers/s.
