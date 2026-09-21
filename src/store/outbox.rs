//! Outbox queue operations used by the webhook dispatcher.

use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use uuid::Uuid;

use super::Result;

#[derive(Debug, Clone)]
pub struct OutboxItem {
    pub id: Uuid,
    pub watch_id: Uuid,
    pub seq: i64,
    pub event_type: String,
    pub url: String,
    pub payload: serde_json::Value,
    /// Attempt number of the delivery being made now (1-based).
    pub attempts: i32,
    pub created_at: DateTime<Utc>,
}

/// Leases up to `limit` due events.
///
/// * `FOR UPDATE SKIP LOCKED` lets overlapping instances dispatch concurrently without double-sending.
/// * An event is only eligible when no earlier event of the same watch is still undelivered, which gives
///   per-watch ordering (`threshold.reached` can never overtake its `payment.confirmed`).
/// * The lease is just `next_attempt_at` pushed into the future: if we crash mid-delivery the event
///   becomes due again by itself.
pub async fn lease_due(pool: &PgPool, limit: i64, lease: Duration) -> Result<Vec<OutboxItem>> {
    let rows = sqlx::query(
        "UPDATE webhook_outbox o
         SET attempts = o.attempts + 1, next_attempt_at = now() + make_interval(secs => $2)
         WHERE o.id IN (
             SELECT c.id FROM webhook_outbox c
             WHERE c.status = 'pending' AND c.next_attempt_at <= now()
               AND NOT EXISTS (SELECT 1 FROM webhook_outbox e
                               WHERE e.watch_id = c.watch_id AND e.status = 'pending' AND e.seq < c.seq)
             ORDER BY c.next_attempt_at
             LIMIT $1
             FOR UPDATE SKIP LOCKED)
         RETURNING o.id, o.watch_id, o.seq, o.event_type, o.url, o.payload, o.attempts, o.created_at",
    )
    .bind(limit)
    .bind(lease.as_secs_f64())
    .fetch_all(pool)
    .await?;
    rows.iter()
        .map(|r| {
            Ok(OutboxItem {
                id: r.try_get("id")?,
                watch_id: r.try_get("watch_id")?,
                seq: r.try_get("seq")?,
                event_type: r.try_get("event_type")?,
                url: r.try_get("url")?,
                payload: r.try_get("payload")?,
                attempts: r.try_get("attempts")?,
                created_at: r.try_get("created_at")?,
            })
        })
        .collect()
}

pub async fn mark_delivered(pool: &PgPool, id: Uuid, status: u16) -> Result<()> {
    sqlx::query(
        "UPDATE webhook_outbox SET status = 'delivered', delivered_at = now(), last_status = $2, last_error = NULL
         WHERE id = $1",
    )
    .bind(id)
    .bind(status as i32)
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn mark_retry(pool: &PgPool, id: Uuid, delay: Duration, status: Option<u16>, error: &str) -> Result<()> {
    sqlx::query(
        "UPDATE webhook_outbox SET next_attempt_at = now() + make_interval(secs => $2), last_status = $3, last_error = $4
         WHERE id = $1 AND status = 'pending'",
    )
    .bind(id)
    .bind(delay.as_secs_f64())
    .bind(status.map(|s| s as i32))
    .bind(truncate(error))
    .execute(pool)
    .await?;
    Ok(())
}

/// Gives up on an event. Later events of the same watch become eligible.
pub async fn mark_dead(pool: &PgPool, id: Uuid, status: Option<u16>, error: &str) -> Result<()> {
    sqlx::query("UPDATE webhook_outbox SET status = 'dead', last_status = $2, last_error = $3 WHERE id = $1")
        .bind(id)
        .bind(status.map(|s| s as i32))
        .bind(truncate(error))
        .execute(pool)
        .await?;
    Ok(())
}

/// Returns a leased event to the queue without counting the attempt (used when a host is parked).
pub async fn release(pool: &PgPool, id: Uuid, delay: Duration) -> Result<()> {
    sqlx::query(
        "UPDATE webhook_outbox SET attempts = GREATEST(attempts - 1, 0), next_attempt_at = now() + make_interval(secs => $2)
         WHERE id = $1 AND status = 'pending'",
    )
    .bind(id)
    .bind(delay.as_secs_f64())
    .execute(pool)
    .await?;
    Ok(())
}

#[derive(Debug, Clone, Copy, Default)]
pub struct OutboxDepth {
    pub pending: i64,
    pub dead: i64,
    pub oldest_pending_age_secs: f64,
}

pub async fn depth(pool: &PgPool) -> Result<OutboxDepth> {
    let row = sqlx::query(
        "SELECT count(*) FILTER (WHERE status = 'pending') AS pending,
                count(*) FILTER (WHERE status = 'dead') AS dead,
                COALESCE(EXTRACT(EPOCH FROM now() - min(created_at) FILTER (WHERE status = 'pending')), 0)::float8 AS oldest
         FROM webhook_outbox WHERE status IN ('pending', 'dead')",
    )
    .fetch_one(pool)
    .await?;
    Ok(OutboxDepth {
        pending: row.try_get("pending")?,
        dead: row.try_get("dead")?,
        oldest_pending_age_secs: row.try_get("oldest")?,
    })
}

fn truncate(s: &str) -> String {
    s.chars().take(500).collect()
}
