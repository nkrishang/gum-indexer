//! Cross-instance watchlist propagation over Postgres LISTEN/NOTIFY.
//!
//! During a deploy two instances overlap: the API may run on one while another leads a chain. NOTIFY gets a new
//! watch into the leader's cache (and WSS buckets) within milliseconds. It is a latency optimisation only:
//! a lost notification is repaired by the sweeper's in-transaction delta load.

use std::{collections::HashMap, sync::Arc, time::Duration};

use sqlx::{PgPool, postgres::PgListener};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::{ChainRuntime, sleep_or_cancel};
use crate::store;

const CHANNEL: &str = "gum_watch";

pub async fn announce_added(pool: &PgPool, chain_id: u64, id: Uuid) {
    announce(pool, format!("add:{chain_id}:{id}")).await;
}

pub async fn announce_removed(pool: &PgPool, chain_id: u64, id: Uuid) {
    announce(pool, format!("remove:{chain_id}:{id}")).await;
}

async fn announce(pool: &PgPool, payload: String) {
    if let Err(e) = sqlx::query("SELECT pg_notify($1, $2)").bind(CHANNEL).bind(&payload).execute(pool).await {
        // Harmless: other instances learn about the watch at their next sweep.
        tracing::debug!(error = %e, payload, "pg_notify failed");
    }
}

pub async fn run(pool: PgPool, chains: HashMap<u64, Arc<ChainRuntime>>, cancel: CancellationToken) {
    let mut failing = false;
    while !cancel.is_cancelled() {
        let mut listener = match PgListener::connect_with(&pool).await {
            Ok(l) => l,
            Err(e) => {
                if !std::mem::replace(&mut failing, true) {
                    tracing::warn!(error.kind = "notify_listener", error = %e, "cannot start LISTEN; cross-instance watch propagation falls back to sweeps");
                }
                sleep_or_cancel(Duration::from_secs(5), &cancel).await;
                continue;
            }
        };
        if let Err(e) = listener.listen(CHANNEL).await {
            tracing::warn!(error.kind = "notify_listener", error = %e, "LISTEN failed");
            sleep_or_cancel(Duration::from_secs(5), &cancel).await;
            continue;
        }
        if std::mem::replace(&mut failing, false) {
            tracing::info!("LISTEN re-established");
        }
        loop {
            let notification = tokio::select! {
                _ = cancel.cancelled() => return,
                n = listener.recv() => n,
            };
            match notification {
                Ok(n) => handle(&pool, &chains, n.payload()).await,
                Err(e) => {
                    failing = true;
                    tracing::warn!(error.kind = "notify_listener", error = %e, "LISTEN connection lost; reconnecting");
                    break;
                }
            }
        }
    }
}

async fn handle(pool: &PgPool, chains: &HashMap<u64, Arc<ChainRuntime>>, payload: &str) {
    let mut parts = payload.splitn(3, ':');
    let (Some(op), Some(chain_id), Some(id)) = (parts.next(), parts.next(), parts.next()) else { return };
    let (Ok(chain_id), Ok(id)) = (chain_id.parse::<u64>(), id.parse::<Uuid>()) else { return };
    let Some(rt) = chains.get(&chain_id) else { return };
    match op {
        "add" => {
            if let Ok(Some((key, watch))) = store::load_watch_entry(pool, &rt.spec, id).await {
                rt.add_watch(key, watch);
            }
        }
        "remove" => {
            if let Ok(Some(row)) = store::get_watch(pool, id).await
                && let Some((key, _)) = row.cache_entry(&rt.spec)
            {
                rt.remove_watch(&key, id);
            }
        }
        _ => {}
    }
}
