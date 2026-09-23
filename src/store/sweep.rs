//! The two accounting transactions: `record_pending` (head path, best effort) and `apply_sweep`
//! (authoritative; the only place amounts are counted and the durable cursor advances).

use std::collections::{HashMap, HashSet};

use alloy::{
    primitives::{Address, B256},
    rpc::types::Log,
};
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use super::{Result, StatsDelta, StoreError, WatchRow, apply_stats, insert_event, to_numeric};
use crate::{
    cache::{WatchCache, WatchKey, WatchRef},
    events::{EventType, TransferView},
    ingest::matcher::{MatchedTransfer, match_log},
    registry::ChainSpec,
};

fn transfer_view(m: &MatchedTransfer, status: &str) -> TransferView {
    TransferView {
        tx_hash: m.tx_hash,
        log_index: m.log_index,
        block_number: m.block_number,
        block_hash: m.block_hash,
        from: m.from,
        amount: m.amount,
        status: status.to_string(),
    }
}

/// Locks the watch, bumps its event sequence and returns it — or `None` if it is no longer active.
async fn lock_active_watch(tx: &mut Transaction<'_, Postgres>, id: Uuid) -> Result<Option<WatchRow>> {
    let row = sqlx::query(
        "UPDATE watches SET next_event_seq = next_event_seq + 1 WHERE id = $1 AND status = 'active' RETURNING *",
    )
    .bind(id)
    .fetch_optional(&mut **tx)
    .await?;
    row.as_ref().map(WatchRow::from_row).transpose()
}

/// Records a transfer seen at chain head and queues `payment.pending`. Returns false when the transfer
/// was already known or the watch is no longer active (both are normal under replay).
pub async fn record_pending(pool: &PgPool, chain: &ChainSpec, m: &MatchedTransfer) -> Result<bool> {
    let mut tx = pool.begin().await?;
    let Some(watch) = lock_active_watch(&mut tx, m.watch.id).await? else {
        return Ok(false);
    };
    let inserted = sqlx::query(
        "INSERT INTO transfers (chain_id, block_hash, log_index, block_number, tx_hash, watch_id, from_address, amount, status)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending')
         ON CONFLICT (chain_id, block_hash, log_index) DO NOTHING RETURNING id",
    )
    .bind(chain.chain_id as i64)
    .bind(m.block_hash.as_slice())
    .bind(m.log_index as i64)
    .bind(m.block_number as i64)
    .bind(m.tx_hash.as_slice())
    .bind(watch.id)
    .bind(m.from.as_slice())
    .bind(to_numeric(m.amount))
    .fetch_optional(&mut *tx)
    .await?;
    if inserted.is_none() {
        return Ok(false); // dropping the transaction rolls the sequence bump back
    }
    // Deliberately touches no row shared between watches (such as stats): the head path must never queue
    // behind — or deadlock with — a sweep transaction that is accounting other watches.
    insert_event(&mut tx, chain, &watch, EventType::PaymentPending, Some(transfer_view(m, "pending"))).await?;
    tx.commit().await?;
    Ok(true)
}

#[derive(Debug, Default)]
pub struct SweepOutcome {
    /// Transfers newly counted in this chunk.
    pub confirmed: Vec<MatchedTransfer>,
    /// Of `confirmed`, how many had never been announced as pending (missed by the push path).
    pub confirmed_without_pending: usize,
    pub orphaned: usize,
    pub ignored: usize,
    /// Watches retired because the threshold was reached; already evicted from the cache.
    pub completed: Vec<(WatchKey, Uuid)>,
    /// Watches discovered by the in-transaction delta load; already inserted into the cache.
    pub discovered: Vec<WatchKey>,
    pub events: usize,
}

#[derive(Debug)]
pub enum SweepResult {
    Applied(SweepOutcome),
    /// The cursor is not where we read it: another instance swept concurrently. Nothing was written.
    CursorMoved {
        expected: u64,
        actual: u64,
    },
}

/// Counts one canonical transfer against its watch: records it as confirmed, adds it to the watch's
/// total, queues `payment.confirmed`, and retires the watch with `threshold.reached` once the
/// threshold is met. A no-op if the watch is no longer active or the transfer was already counted.
async fn count_confirmed(
    tx: &mut Transaction<'_, Postgres>,
    chain: &ChainSpec,
    m: MatchedTransfer,
    stats: &mut HashMap<Address, StatsDelta>,
    out: &mut SweepOutcome,
) -> Result<()> {
    let Some(watch) = lock_active_watch(tx, m.watch.id).await? else {
        // Watch retired (completed earlier in this very chunk, cancelled, expired). A pending row, if any,
        // is resolved by the sweep as `ignored`.
        return Ok(());
    };
    let upserted = sqlx::query(
        "INSERT INTO transfers (chain_id, block_hash, log_index, block_number, tx_hash, watch_id, from_address, amount, status, confirmed_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'confirmed', now())
         ON CONFLICT (chain_id, block_hash, log_index) DO UPDATE SET status = 'confirmed', confirmed_at = now()
             WHERE transfers.status = 'pending'
         RETURNING (xmax = 0) AS inserted",
    )
    .bind(chain.chain_id as i64)
    .bind(m.block_hash.as_slice())
    .bind(m.log_index as i64)
    .bind(m.block_number as i64)
    .bind(m.tx_hash.as_slice())
    .bind(watch.id)
    .bind(m.from.as_slice())
    .bind(to_numeric(m.amount))
    .fetch_optional(&mut **tx)
    .await?;
    let Some(upserted) = upserted else {
        // Already confirmed by an earlier (replayed) sweep: undo the speculative sequence bump.
        sqlx::query("UPDATE watches SET next_event_seq = next_event_seq - 1 WHERE id = $1")
            .bind(watch.id)
            .execute(&mut **tx)
            .await?;
        return Ok(());
    };
    if upserted.try_get::<bool, _>("inserted")? {
        out.confirmed_without_pending += 1;
    }

    let row = sqlx::query("UPDATE watches SET confirmed_amount = confirmed_amount + $2 WHERE id = $1 RETURNING *")
        .bind(watch.id)
        .bind(to_numeric(m.amount))
        .fetch_one(&mut **tx)
        .await?;
    let mut watch = WatchRow::from_row(&row)?;
    insert_event(tx, chain, &watch, EventType::PaymentConfirmed, Some(transfer_view(&m, "confirmed"))).await?;
    let delta = stats.entry(watch.token_address).or_default();
    delta.confirmed += 1;
    delta.volume += m.amount;
    out.events += 1;

    if watch.confirmed_amount >= watch.threshold {
        let row = sqlx::query(
            "UPDATE watches SET status = 'completed', completed_at = now(), next_event_seq = next_event_seq + 1
             WHERE id = $1 RETURNING *",
        )
        .bind(watch.id)
        .fetch_one(&mut **tx)
        .await?;
        watch = WatchRow::from_row(&row)?;
        insert_event(tx, chain, &watch, EventType::ThresholdReached, None).await?;
        stats.entry(watch.token_address).or_default().thresholds += 1;
        out.events += 1;
        out.completed.push((WatchKey::new(m.token_idx, &m.to), watch.id));
    }
    out.confirmed.push(m);
    Ok(())
}

/// A watch registered after blocks it cares about were swept (see `store::create_watch`): blocks
/// `[start_block, backfill_to]` still need scanning for it.
#[derive(Debug, Clone)]
pub struct PendingBackfill {
    pub id: Uuid,
    pub seq: i64,
    pub token_address: Address,
    pub payment_address: Address,
    pub start_block: u64,
    pub backfill_to: u64,
}

/// Up to `limit` watches of the chain awaiting a backfill, oldest first.
pub async fn pending_backfills(pool: &PgPool, chain_id: u64, limit: i64) -> Result<Vec<PendingBackfill>> {
    let rows = sqlx::query(
        "SELECT id, seq, token_address, payment_address, start_block, backfill_to FROM watches
         WHERE chain_id = $1 AND backfill_to IS NOT NULL ORDER BY seq LIMIT $2",
    )
    .bind(chain_id as i64)
    .bind(limit)
    .fetch_all(pool)
    .await?;
    rows.iter()
        .map(|row| {
            Ok(PendingBackfill {
                id: row.try_get("id")?,
                seq: row.try_get("seq")?,
                token_address: super::address(row, "token_address")?,
                payment_address: super::address(row, "payment_address")?,
                start_block: row.try_get::<i64, _>("start_block")? as u64,
                backfill_to: row.try_get::<i64, _>("backfill_to")? as u64,
            })
        })
        .collect()
}

/// Counts, for each watch in `backfills`, the transfers of its `[start_block, backfill_to]` found in
/// `logs` (every Transfer log of those blocks to those addresses), and marks the backfills done --
/// in one transaction, so a crash simply re-runs it and a re-run counts nothing twice. Blocks above
/// `backfill_to` are the sweeps' business and are ignored here. Counting goes through the same path
/// as a sweep, so events, totals and threshold crossings are identical.
pub async fn apply_backfill(
    pool: &PgPool,
    chain: &ChainSpec,
    cache: &WatchCache,
    backfills: &[PendingBackfill],
    logs: &[Log],
) -> Result<SweepOutcome> {
    let scope = WatchCache::new();
    let mut until: HashMap<Uuid, u64> = HashMap::new();
    for b in backfills {
        if let Some(token) = chain.token_by_address(&b.token_address) {
            scope.insert(
                WatchKey::new(token.idx, &b.payment_address),
                WatchRef { id: b.id, seq: b.seq, start_block: b.start_block },
            );
            until.insert(b.id, b.backfill_to);
        }
    }
    let mut matched: Vec<MatchedTransfer> = logs
        .iter()
        .filter(|l| !l.removed)
        .filter_map(|l| match_log(chain, &scope, l).ok())
        .filter(|m| until.get(&m.watch.id).is_some_and(|to| m.block_number <= *to))
        .collect();
    matched.sort_by_key(|m| (m.block_number, m.log_index));

    let mut tx = pool.begin().await?;
    let mut out = SweepOutcome::default();
    let mut stats: HashMap<Address, StatsDelta> = HashMap::new();
    for m in matched {
        count_confirmed(&mut tx, chain, m, &mut stats, &mut out).await?;
    }
    let ids: Vec<Uuid> = backfills.iter().map(|b| b.id).collect();
    sqlx::query("UPDATE watches SET backfill_to = NULL WHERE id = ANY($1)").bind(&ids).execute(&mut *tx).await?;
    let mut tokens: Vec<_> = stats.into_iter().collect();
    tokens.sort_unstable_by_key(|(token, _)| *token); // fixed lock order
    for (token, delta) in tokens {
        apply_stats(&mut tx, chain.chain_id, &token, &delta).await?;
    }
    tx.commit().await?;

    for (key, id) in &out.completed {
        cache.remove(key, *id);
    }
    Ok(out)
}

/// Applies the canonical logs of `[from, to]` and advances the cursor to `to`, atomically.
///
/// * `expected_cursor` – the cursor value the caller planned this chunk from; guards against split-brain.
/// * `logs` – every Transfer log of the registry tokens in `[from, to]` as returned by `eth_getLogs`.
///
/// Inside the transaction (cursor row held FOR UPDATE) we first load watches registered since the cache was
/// last synced and only then match, so a registration racing this sweep on any instance is never skipped.
pub async fn apply_sweep(
    pool: &PgPool,
    chain: &ChainSpec,
    cache: &WatchCache,
    expected_cursor: u64,
    to: u64,
    logs: &[Log],
) -> Result<SweepResult> {
    let mut tx = pool.begin().await?;
    let actual = sqlx::query("SELECT confirmed_block FROM chain_cursors WHERE chain_id = $1 FOR UPDATE")
        .bind(chain.chain_id as i64)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(StoreError::MissingCursor(chain.chain_id))?
        .try_get::<i64, _>(0)? as u64;
    if actual != expected_cursor {
        return Ok(SweepResult::CursorMoved { expected: expected_cursor, actual });
    }

    let mut out = SweepOutcome::default();
    // Per-token deltas, written once at the end of the transaction so the shared stats rows are locked briefly.
    let mut stats: HashMap<Address, StatsDelta> = HashMap::new();

    // Delta-load: gap-free because every registration assigns `seq` while holding the cursor row FOR SHARE.
    let rows = sqlx::query(
        "SELECT id, seq, token_address, payment_address, start_block FROM watches
         WHERE chain_id = $1 AND status = 'active' AND seq > $2 ORDER BY seq",
    )
    .bind(chain.chain_id as i64)
    .bind(cache.last_seq())
    .fetch_all(&mut *tx)
    .await?;
    for row in &rows {
        if let Some((key, watch)) = super::slim_entry(chain, row)?
            && cache.insert(key, watch)
        {
            out.discovered.push(key);
        }
    }

    let mut matched: Vec<MatchedTransfer> =
        logs.iter().filter(|l| !l.removed).filter_map(|l| match_log(chain, cache, l).ok()).collect();
    matched.sort_by_key(|m| (m.block_number, m.log_index));
    let canonical: HashSet<(B256, u64)> = matched.iter().map(|m| (m.block_hash, m.log_index)).collect();

    for m in matched {
        count_confirmed(&mut tx, chain, m, &mut stats, &mut out).await?;
    }

    // Resolve every pending transfer at or below `to` that was not confirmed above.
    let leftovers = sqlx::query(
        "SELECT t.id, t.block_hash, t.log_index, t.block_number, t.tx_hash, t.from_address, t.amount, t.watch_id
         FROM transfers t WHERE t.chain_id = $1 AND t.status = 'pending' AND t.block_number <= $2
         ORDER BY t.block_number, t.log_index",
    )
    .bind(chain.chain_id as i64)
    .bind(to as i64)
    .fetch_all(&mut *tx)
    .await?;
    if !leftovers.is_empty() {
        // Canonical-but-not-counted (watch retired) must not be reported as orphaned.
        let all_canonical: HashSet<(B256, u64)> =
            logs.iter().filter(|l| !l.removed).filter_map(|l| Some((l.block_hash?, l.log_index?))).collect();
        for row in &leftovers {
            let id: i64 = row.try_get("id")?;
            let block_hash = super::hash(row, "block_hash")?;
            let log_index = row.try_get::<i64, _>("log_index")? as u64;
            if canonical.contains(&(block_hash, log_index)) || all_canonical.contains(&(block_hash, log_index)) {
                sqlx::query("UPDATE transfers SET status = 'ignored' WHERE id = $1").bind(id).execute(&mut *tx).await?;
                out.ignored += 1;
                continue;
            }
            sqlx::query("UPDATE transfers SET status = 'orphaned' WHERE id = $1").bind(id).execute(&mut *tx).await?;
            out.orphaned += 1;
            let watch_id: Uuid = row.try_get("watch_id")?;
            if let Some(watch) = lock_active_watch(&mut tx, watch_id).await? {
                let view = TransferView {
                    tx_hash: super::hash(row, "tx_hash")?,
                    log_index,
                    block_number: row.try_get::<i64, _>("block_number")? as u64,
                    block_hash,
                    from: super::address(row, "from_address")?,
                    amount: super::amount(row, "amount")?,
                    status: "orphaned".into(),
                };
                insert_event(&mut tx, chain, &watch, EventType::PaymentOrphaned, Some(view)).await?;
                stats.entry(watch.token_address).or_default().orphaned += 1;
                out.events += 1;
            }
        }
    }

    let mut tokens: Vec<_> = stats.into_iter().collect();
    tokens.sort_unstable_by_key(|(token, _)| *token); // fixed lock order
    for (token, delta) in tokens {
        apply_stats(&mut tx, chain.chain_id, &token, &delta).await?;
    }
    sqlx::query("UPDATE chain_cursors SET confirmed_block = $2, updated_at = now() WHERE chain_id = $1")
        .bind(chain.chain_id as i64)
        .bind(to as i64)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;

    for (key, id) in &out.completed {
        cache.remove(key, *id);
    }
    Ok(SweepResult::Applied(out))
}

/// Lowest `start_block` among active watches; sweeps never need to look below it.
pub async fn min_active_start_block(pool: &PgPool, chain_id: u64) -> Result<Option<u64>> {
    let row = sqlx::query("SELECT min(start_block) FROM watches WHERE chain_id = $1 AND status = 'active'")
        .bind(chain_id as i64)
        .fetch_one(pool)
        .await?;
    Ok(row.try_get::<Option<i64>, _>(0)?.map(|v| v as u64))
}

/// Moves the cursor forward without scanning. Only legal when no active watch could have a transfer in the
/// skipped range, which is re-checked under the cursor lock. Returns the cursor after the call.
pub async fn fast_forward_cursor(pool: &PgPool, chain_id: u64, target: u64) -> Result<u64> {
    let mut tx = pool.begin().await?;
    let cursor = sqlx::query("SELECT confirmed_block FROM chain_cursors WHERE chain_id = $1 FOR UPDATE")
        .bind(chain_id as i64)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(StoreError::MissingCursor(chain_id))?
        .try_get::<i64, _>(0)? as u64;
    let min_start = sqlx::query("SELECT min(start_block) FROM watches WHERE chain_id = $1 AND status = 'active'")
        .bind(chain_id as i64)
        .fetch_one(&mut *tx)
        .await?
        .try_get::<Option<i64>, _>(0)?
        .map(|v| v as u64);
    let has_pending = sqlx::query("SELECT EXISTS (SELECT 1 FROM transfers WHERE chain_id = $1 AND status = 'pending')")
        .bind(chain_id as i64)
        .fetch_one(&mut *tx)
        .await?
        .try_get::<bool, _>(0)?;
    // Blocks below every active watch's start_block hold nothing we count.
    let ceiling = match min_start {
        Some(start) => start.saturating_sub(1),
        None => target,
    };
    let new_cursor = if has_pending { cursor } else { cursor.max(ceiling.min(target)) };
    if new_cursor != cursor {
        sqlx::query("UPDATE chain_cursors SET confirmed_block = $2, updated_at = now() WHERE chain_id = $1")
            .bind(chain_id as i64)
            .bind(new_cursor as i64)
            .execute(&mut *tx)
            .await?;
    }
    tx.commit().await?;
    Ok(new_cursor)
}
