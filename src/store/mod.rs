//! Postgres persistence. The database is the source of truth; every state change and the webhook events
//! describing it are written in one transaction, and every write is idempotent under replay.

pub mod outbox;
pub mod sweep;

use std::time::Duration;

use alloy::primitives::{Address, B256, U256};
use bigdecimal::{
    BigDecimal,
    num_bigint::{BigInt, Sign},
};
use chrono::{DateTime, Utc};
use futures_util::TryStreamExt;
use sqlx::{
    PgPool, Postgres, Row, Transaction,
    postgres::{PgPoolOptions, PgRow},
};
use uuid::Uuid;

use crate::{
    cache::{WatchKey, WatchRef},
    events::{EventPayload, EventType, TransferView, WatchView},
    registry::ChainSpec,
};

pub static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    #[error("migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    #[error("corrupt row: {0}")]
    Corrupt(String),
    #[error("chain {0} has no cursor row")]
    MissingCursor(u64),
}

impl StoreError {
    /// Stable identifier for logs and metrics.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Db(sqlx::Error::PoolTimedOut) => "db_pool_timeout",
            Self::Db(sqlx::Error::Io(_)) => "db_io",
            Self::Db(sqlx::Error::Database(_)) => "db_query",
            Self::Db(_) => "db_other",
            Self::Migrate(_) => "db_migrate",
            Self::Corrupt(_) => "db_corrupt_row",
            Self::MissingCursor(_) => "db_missing_cursor",
        }
    }
}

pub type Result<T> = std::result::Result<T, StoreError>;

/// Connects with retries: on Railway the private-network DNS name may not resolve for the first moments.
pub async fn connect(url: &str, max_connections: u32) -> Result<PgPool> {
    let mut delay = Duration::from_millis(250);
    let mut attempt = 0u32;
    loop {
        attempt += 1;
        let res = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(10))
            .connect(url)
            .await;
        match res {
            Ok(pool) => return Ok(pool),
            Err(e) if attempt < 8 => {
                tracing::warn!(error.kind = "db_connect", attempt, error = %e, "database not reachable yet, retrying");
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(5));
            }
            Err(e) => return Err(e.into()),
        }
    }
}

pub async fn migrate(pool: &PgPool) -> Result<()> {
    MIGRATOR.run(pool).await?;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// uint256 <-> NUMERIC(78,0)
// ---------------------------------------------------------------------------------------------

pub fn to_numeric(v: U256) -> BigDecimal {
    BigDecimal::new(BigInt::from_bytes_be(Sign::Plus, &v.to_be_bytes::<32>()), 0)
}

pub fn from_numeric(v: &BigDecimal) -> Result<U256> {
    let (int, scale) = v.with_scale(0).into_bigint_and_exponent();
    let (sign, bytes) = int.to_bytes_be();
    if scale != 0 || sign == Sign::Minus || bytes.len() > 32 {
        return Err(StoreError::Corrupt(format!("NUMERIC value {v} is not a uint256")));
    }
    Ok(U256::from_be_slice(&bytes))
}

fn address(row: &PgRow, col: &str) -> Result<Address> {
    let bytes: Vec<u8> = row.try_get(col)?;
    Address::try_from(bytes.as_slice()).map_err(|_| StoreError::Corrupt(format!("{col} is not 20 bytes")))
}

fn hash(row: &PgRow, col: &str) -> Result<B256> {
    let bytes: Vec<u8> = row.try_get(col)?;
    B256::try_from(bytes.as_slice()).map_err(|_| StoreError::Corrupt(format!("{col} is not 32 bytes")))
}

fn amount(row: &PgRow, col: &str) -> Result<U256> {
    from_numeric(&row.try_get::<BigDecimal, _>(col)?)
}

// ---------------------------------------------------------------------------------------------
// Watches
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct WatchRow {
    pub id: Uuid,
    pub seq: i64,
    pub chain_id: u64,
    pub token_address: Address,
    pub payment_address: Address,
    pub threshold: U256,
    pub confirmed_amount: U256,
    pub webhook_url: String,
    pub status: String,
    pub start_block: u64,
    /// Set while blocks `[start_block, backfill_to]`, swept before this watch existed, await a scan.
    pub backfill_to: Option<u64>,
    pub next_event_seq: i64,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
}

impl WatchRow {
    fn from_row(row: &PgRow) -> Result<Self> {
        Ok(Self {
            id: row.try_get("id")?,
            seq: row.try_get("seq")?,
            chain_id: row.try_get::<i64, _>("chain_id")? as u64,
            token_address: address(row, "token_address")?,
            payment_address: address(row, "payment_address")?,
            threshold: amount(row, "threshold")?,
            confirmed_amount: amount(row, "confirmed_amount")?,
            webhook_url: row.try_get("webhook_url")?,
            status: row.try_get("status")?,
            start_block: row.try_get::<i64, _>("start_block")? as u64,
            backfill_to: row.try_get::<Option<i64>, _>("backfill_to")?.map(|b| b as u64),
            next_event_seq: row.try_get("next_event_seq")?,
            created_at: row.try_get("created_at")?,
            completed_at: row.try_get("completed_at")?,
            expires_at: row.try_get("expires_at")?,
        })
    }

    pub fn view(&self, chain: &ChainSpec) -> WatchView {
        let token = chain.token_by_address(&self.token_address).map(|t| t.symbol).unwrap_or("UNKNOWN");
        WatchView {
            id: self.id,
            chain: chain.name.to_string(),
            chain_id: chain.chain_id,
            token: token.to_string(),
            token_address: self.token_address,
            payment_address: self.payment_address,
            balance_threshold: self.threshold,
            confirmed_amount: self.confirmed_amount,
            status: self.status.clone(),
        }
    }

    pub fn cache_entry(&self, chain: &ChainSpec) -> Option<(WatchKey, WatchRef)> {
        let token = chain.token_by_address(&self.token_address)?;
        Some((
            WatchKey::new(token.idx, &self.payment_address),
            WatchRef { id: self.id, seq: self.seq, start_block: self.start_block },
        ))
    }
}

#[derive(Debug, Clone)]
pub struct NewWatch {
    pub chain_id: u64,
    pub token_address: Address,
    pub payment_address: Address,
    pub threshold: U256,
    pub webhook_url: String,
    pub expires_at: Option<DateTime<Utc>>,
    /// Latest chain head known to this process. May be stale; staleness only moves `start_block` earlier.
    pub head_estimate: u64,
    /// How many blocks before the head payments may already have arrived (0: none). See `create_watch`.
    pub lookback_blocks: u64,
}

#[derive(Debug)]
pub enum CreateOutcome {
    Created(WatchRow),
    /// An identical active watch already exists (same threshold and webhook): registration is idempotent.
    Existing(WatchRow),
    /// An active watch exists on the same target with different parameters.
    Conflict(WatchRow),
}

/// Creates the cursor row for a chain if it has never been seen. Never moves an existing cursor.
pub async fn ensure_cursor(pool: &PgPool, chain_id: u64, head: u64) -> Result<u64> {
    let row = sqlx::query(
        "INSERT INTO chain_cursors (chain_id, confirmed_block) VALUES ($1, $2)
         ON CONFLICT (chain_id) DO UPDATE SET chain_id = EXCLUDED.chain_id
         RETURNING confirmed_block",
    )
    .bind(chain_id as i64)
    .bind(head as i64)
    .fetch_one(pool)
    .await?;
    Ok(row.try_get::<i64, _>(0)? as u64)
}

pub async fn cursor(pool: &PgPool, chain_id: u64) -> Result<u64> {
    let row = sqlx::query("SELECT confirmed_block FROM chain_cursors WHERE chain_id = $1")
        .bind(chain_id as i64)
        .fetch_optional(pool)
        .await?
        .ok_or(StoreError::MissingCursor(chain_id))?;
    Ok(row.try_get::<i64, _>(0)? as u64)
}

/// Registers a watch.
///
/// Correctness hinges on the cursor row lock: we hold it FOR SHARE while choosing `start_block` and inserting,
/// sweeps hold it FOR UPDATE while they delta-load watches and advance the cursor. Hence a watch can never
/// start at a block that a sweep (on any instance) has already passed without seeing it -- except on
/// purpose: a watch registered with a lookback (payments may predate the registration) can start at or
/// below the cursor, and then records `backfill_to = cursor`. The blocks `[start_block, cursor]` are
/// scanned once for it by the next sweep (`sweep::apply_backfill`), and everything above the cursor by
/// the sweeps as usual, so no block is missed or counted twice.
pub async fn create_watch(pool: &PgPool, new: &NewWatch) -> Result<CreateOutcome> {
    let mut tx = pool.begin().await?;
    let cursor = sqlx::query("SELECT confirmed_block FROM chain_cursors WHERE chain_id = $1 FOR SHARE")
        .bind(new.chain_id as i64)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or(StoreError::MissingCursor(new.chain_id))?
        .try_get::<i64, _>(0)? as u64;
    let start_block = start_block(cursor, new.head_estimate, new.lookback_blocks);
    let backfill_to = (start_block <= cursor).then_some(cursor);

    let inserted = sqlx::query(
        "INSERT INTO watches (id, chain_id, token_address, payment_address, threshold, webhook_url, start_block, expires_at, backfill_to)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
         ON CONFLICT (chain_id, token_address, payment_address) WHERE status = 'active' DO NOTHING
         RETURNING *",
    )
    .bind(Uuid::now_v7())
    .bind(new.chain_id as i64)
    .bind(new.token_address.as_slice())
    .bind(new.payment_address.as_slice())
    .bind(to_numeric(new.threshold))
    .bind(&new.webhook_url)
    .bind(start_block as i64)
    .bind(new.expires_at)
    .bind(backfill_to.map(|b| b as i64))
    .fetch_optional(&mut *tx)
    .await?;

    let outcome = match inserted {
        Some(row) => CreateOutcome::Created(WatchRow::from_row(&row)?),
        None => {
            let row = sqlx::query(
                "SELECT * FROM watches
                 WHERE chain_id = $1 AND token_address = $2 AND payment_address = $3 AND status = 'active'",
            )
            .bind(new.chain_id as i64)
            .bind(new.token_address.as_slice())
            .bind(new.payment_address.as_slice())
            .fetch_one(&mut *tx)
            .await?;
            let existing = WatchRow::from_row(&row)?;
            if existing.threshold == new.threshold && existing.webhook_url == new.webhook_url {
                CreateOutcome::Existing(existing)
            } else {
                CreateOutcome::Conflict(existing)
            }
        }
    };
    tx.commit().await?;
    Ok(outcome)
}

/// First block a new watch counts: the one after both the cursor and the head, or `lookback` blocks
/// before the head when payments may already have arrived.
pub fn start_block(cursor: u64, head_estimate: u64, lookback: u64) -> u64 {
    let base = cursor.max(head_estimate);
    if lookback == 0 { base + 1 } else { (base + 1).min(base.saturating_sub(lookback).max(1)) }
}

pub async fn get_watch(pool: &PgPool, id: Uuid) -> Result<Option<WatchRow>> {
    let row = sqlx::query("SELECT * FROM watches WHERE id = $1").bind(id).fetch_optional(pool).await?;
    row.as_ref().map(WatchRow::from_row).transpose()
}

/// Cancels an active watch. Returns the row if it was active.
pub async fn cancel_watch(pool: &PgPool, id: Uuid) -> Result<Option<WatchRow>> {
    let row = sqlx::query(
        "UPDATE watches SET status = 'cancelled', completed_at = now() WHERE id = $1 AND status = 'active' RETURNING *",
    )
    .bind(id)
    .fetch_optional(pool)
    .await?;
    row.as_ref().map(WatchRow::from_row).transpose()
}

/// Expires overdue watches and emits `watch.expired`. Safe to run concurrently on several instances.
pub async fn expire_watches(pool: &PgPool, chain: &ChainSpec, limit: i64) -> Result<Vec<WatchRow>> {
    let mut tx = pool.begin().await?;
    let rows = sqlx::query(
        "UPDATE watches SET status = 'expired', completed_at = now(), next_event_seq = next_event_seq + 1
         WHERE id IN (
             SELECT id FROM watches
             WHERE chain_id = $1 AND status = 'active' AND expires_at IS NOT NULL AND expires_at <= now()
             ORDER BY expires_at LIMIT $2 FOR UPDATE SKIP LOCKED)
         RETURNING *",
    )
    .bind(chain.chain_id as i64)
    .bind(limit)
    .fetch_all(&mut *tx)
    .await?;
    let mut expired = Vec::with_capacity(rows.len());
    for row in &rows {
        let watch = WatchRow::from_row(row)?;
        insert_event(&mut tx, chain, &watch, EventType::WatchExpired, None).await?;
        expired.push(watch);
    }
    tx.commit().await?;
    Ok(expired)
}

/// Streams every active watch of a chain into `sink` (boot hydration). Returns the number of rows.
pub async fn hydrate(pool: &PgPool, chain: &ChainSpec, mut sink: impl FnMut(WatchKey, WatchRef)) -> Result<usize> {
    let mut rows = sqlx::query(
        "SELECT id, seq, token_address, payment_address, start_block FROM watches
         WHERE chain_id = $1 AND status = 'active' ORDER BY seq",
    )
    .bind(chain.chain_id as i64)
    .fetch(pool);
    let mut n = 0;
    while let Some(row) = rows.try_next().await? {
        if let Some((key, watch)) = slim_entry(chain, &row)? {
            sink(key, watch);
            n += 1;
        }
    }
    Ok(n)
}

pub async fn load_watch_entry(pool: &PgPool, chain: &ChainSpec, id: Uuid) -> Result<Option<(WatchKey, WatchRef)>> {
    let row = sqlx::query(
        "SELECT id, seq, token_address, payment_address, start_block FROM watches
         WHERE id = $1 AND chain_id = $2 AND status = 'active'",
    )
    .bind(id)
    .bind(chain.chain_id as i64)
    .fetch_optional(pool)
    .await?;
    match row {
        Some(row) => slim_entry(chain, &row),
        None => Ok(None),
    }
}

fn slim_entry(chain: &ChainSpec, row: &PgRow) -> Result<Option<(WatchKey, WatchRef)>> {
    let token = address(row, "token_address")?;
    // A token removed from config leaves its watches dormant rather than breaking hydration.
    let Some(token) = chain.token_by_address(&token) else { return Ok(None) };
    Ok(Some((
        WatchKey::new(token.idx, &address(row, "payment_address")?),
        WatchRef {
            id: row.try_get("id")?,
            seq: row.try_get("seq")?,
            start_block: row.try_get::<i64, _>("start_block")? as u64,
        },
    )))
}

pub async fn active_watch_count(pool: &PgPool, chain_id: u64) -> Result<i64> {
    let row = sqlx::query("SELECT count(*) FROM watches WHERE chain_id = $1 AND status = 'active'")
        .bind(chain_id as i64)
        .fetch_one(pool)
        .await?;
    Ok(row.try_get(0)?)
}

// ---------------------------------------------------------------------------------------------
// Transfers (read side)
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct TransferRow {
    pub tx_hash: B256,
    pub log_index: u64,
    pub block_number: u64,
    pub block_hash: B256,
    pub from_address: Address,
    pub amount: U256,
    pub status: String,
    pub seen_at: DateTime<Utc>,
    pub confirmed_at: Option<DateTime<Utc>>,
}

impl TransferRow {
    fn from_row(row: &PgRow) -> Result<Self> {
        Ok(Self {
            tx_hash: hash(row, "tx_hash")?,
            log_index: row.try_get::<i64, _>("log_index")? as u64,
            block_number: row.try_get::<i64, _>("block_number")? as u64,
            block_hash: hash(row, "block_hash")?,
            from_address: address(row, "from_address")?,
            amount: amount(row, "amount")?,
            status: row.try_get("status")?,
            seen_at: row.try_get("seen_at")?,
            confirmed_at: row.try_get("confirmed_at")?,
        })
    }

    pub fn view(&self) -> TransferView {
        TransferView {
            tx_hash: self.tx_hash,
            log_index: self.log_index,
            block_number: self.block_number,
            block_hash: self.block_hash,
            from: self.from_address,
            amount: self.amount,
            status: self.status.clone(),
        }
    }
}

pub async fn transfers_for_watch(pool: &PgPool, watch_id: Uuid, limit: i64) -> Result<Vec<TransferRow>> {
    let rows = sqlx::query("SELECT * FROM transfers WHERE watch_id = $1 ORDER BY block_number, log_index LIMIT $2")
        .bind(watch_id)
        .bind(limit)
        .fetch_all(pool)
        .await?;
    rows.iter().map(TransferRow::from_row).collect()
}

/// Block numbers of transfers still awaiting confirmation (used to schedule sweeps after a restart).
pub async fn pending_blocks(pool: &PgPool, chain_id: u64) -> Result<Vec<u64>> {
    let rows = sqlx::query("SELECT DISTINCT block_number FROM transfers WHERE chain_id = $1 AND status = 'pending'")
        .bind(chain_id as i64)
        .fetch_all(pool)
        .await?;
    rows.iter().map(|r| Ok(r.try_get::<i64, _>(0)? as u64)).collect()
}

// ---------------------------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct StatsRow {
    pub chain_id: u64,
    pub token_address: Address,
    /// Transfers currently awaiting confirmation (live count, not a rollup).
    pub pending_count: i64,
    pub confirmed_count: i64,
    pub orphaned_count: i64,
    pub confirmed_volume: U256,
    pub thresholds_reached: i64,
    pub active_watches: i64,
}

pub async fn stats(pool: &PgPool) -> Result<Vec<StatsRow>> {
    let rows = sqlx::query(
        "SELECT k.chain_id, k.token_address,
                COALESCE(s.confirmed_count, 0) AS confirmed_count, COALESCE(s.orphaned_count, 0) AS orphaned_count,
                COALESCE(s.thresholds_reached, 0) AS thresholds_reached, COALESCE(s.confirmed_volume, 0) AS confirmed_volume,
                COALESCE(w.active, 0) AS active_watches, COALESCE(w.pending, 0) AS pending_count
         FROM (SELECT chain_id, token_address FROM stats_rollup
               UNION SELECT DISTINCT chain_id, token_address FROM watches WHERE status = 'active') k
         LEFT JOIN stats_rollup s USING (chain_id, token_address)
         LEFT JOIN (SELECT w.chain_id, w.token_address, count(*) AS active,
                           (SELECT count(*) FROM transfers t JOIN watches tw ON tw.id = t.watch_id
                             WHERE t.status = 'pending' AND tw.chain_id = w.chain_id AND tw.token_address = w.token_address) AS pending
                    FROM watches w WHERE w.status = 'active' GROUP BY 1, 2) w USING (chain_id, token_address)
         ORDER BY k.chain_id, k.token_address",
    )
    .fetch_all(pool)
    .await?;
    rows.iter()
        .map(|r| {
            Ok(StatsRow {
                chain_id: r.try_get::<i64, _>("chain_id")? as u64,
                token_address: address(r, "token_address")?,
                pending_count: r.try_get("pending_count")?,
                confirmed_count: r.try_get("confirmed_count")?,
                orphaned_count: r.try_get("orphaned_count")?,
                confirmed_volume: amount(r, "confirmed_volume")?,
                thresholds_reached: r.try_get("thresholds_reached")?,
                active_watches: r.try_get("active_watches")?,
            })
        })
        .collect()
}

#[derive(Debug, Default, Clone)]
pub(crate) struct StatsDelta {
    pub confirmed: i64,
    pub orphaned: i64,
    pub thresholds: i64,
    pub volume: U256,
}

pub(crate) async fn apply_stats(
    tx: &mut Transaction<'_, Postgres>,
    chain_id: u64,
    token: &Address,
    d: &StatsDelta,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO stats_rollup (chain_id, token_address, confirmed_count, orphaned_count, thresholds_reached, confirmed_volume)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (chain_id, token_address) DO UPDATE SET
             confirmed_count = stats_rollup.confirmed_count + EXCLUDED.confirmed_count,
             orphaned_count = stats_rollup.orphaned_count + EXCLUDED.orphaned_count,
             thresholds_reached = stats_rollup.thresholds_reached + EXCLUDED.thresholds_reached,
             confirmed_volume = stats_rollup.confirmed_volume + EXCLUDED.confirmed_volume",
    )
    .bind(chain_id as i64)
    .bind(token.as_slice())
    .bind(d.confirmed)
    .bind(d.orphaned)
    .bind(d.thresholds)
    .bind(to_numeric(d.volume))
    .execute(&mut **tx)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------------------------
// Outbox insert (shared by every state-changing transaction)
// ---------------------------------------------------------------------------------------------

/// Appends a webhook event for `watch`. The caller must already have bumped `next_event_seq` in this
/// transaction and pass the row as returned by that UPDATE; `watch.next_event_seq` is the event's sequence.
pub(crate) async fn insert_event(
    tx: &mut Transaction<'_, Postgres>,
    chain: &ChainSpec,
    watch: &WatchRow,
    event_type: EventType,
    transfer: Option<TransferView>,
) -> Result<Uuid> {
    let id = Uuid::now_v7();
    let payload = EventPayload {
        id,
        event_type,
        created_at: Utc::now(),
        sequence: watch.next_event_seq,
        watch: watch.view(chain),
        transfer,
    };
    let json = serde_json::to_value(&payload).map_err(|e| StoreError::Corrupt(e.to_string()))?;
    sqlx::query(
        "INSERT INTO webhook_outbox (id, watch_id, seq, event_type, url, payload) VALUES ($1, $2, $3, $4, $5, $6)",
    )
    .bind(id)
    .bind(watch.id)
    .bind(watch.next_event_seq)
    .bind(event_type.as_str())
    .bind(&watch.webhook_url)
    .bind(json)
    .execute(&mut **tx)
    .await?;
    Ok(id)
}

#[cfg(test)]
mod tests {
    #[test]
    fn start_block_covers_the_lookback_and_never_goes_below_one() {
        use super::start_block;
        // No lookback: the block after both the cursor and the head (unchanged behaviour).
        assert_eq!(start_block(100, 140, 0), 141);
        assert_eq!(start_block(100, 50, 0), 101, "a stale head estimate never starts below the cursor");
        // A lookback within the unswept blocks: no backfill needed (start stays above the cursor).
        assert_eq!(start_block(130, 140, 5), 135);
        // A lookback reaching swept blocks: starts at or below the cursor (the caller then backfills).
        assert_eq!(start_block(100, 105, 20), 85);
        assert_eq!(start_block(100, 105, 1_000), 1);
    }

    use super::*;

    #[test]
    fn uint256_roundtrips_through_numeric() {
        for v in
            [U256::ZERO, U256::from(1u64), U256::from(u64::MAX), U256::MAX, U256::from(10u64).pow(U256::from(77u64))]
        {
            let n = to_numeric(v);
            assert_eq!(n.to_string(), v.to_string());
            assert_eq!(from_numeric(&n).unwrap(), v);
        }
    }

    #[test]
    fn non_uint256_numerics_are_rejected() {
        assert!(from_numeric(&"-1".parse().unwrap()).is_err());
        let too_big: BigDecimal = format!("{}0", U256::MAX).parse().unwrap();
        assert!(from_numeric(&too_big).is_err());
    }
}
