//! Webhook delivery from the transactional outbox: at-least-once, ordered per watch, signed, with
//! exponential backoff + full jitter and per-host parking so a dead endpoint cannot monopolise workers.

pub mod sign;
pub mod target;

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use sqlx::PgPool;
use tokio::sync::{Notify, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::{
    config::WebhookConfig,
    store::outbox::{self, OutboxItem},
    telemetry::LogLimiter,
};

pub struct Dispatcher {
    pool: PgPool,
    cfg: WebhookConfig,
    client: reqwest::Client,
    wake: Arc<Notify>,
    permits: Arc<Semaphore>,
    hosts: Mutex<HashMap<String, HostState>>,
    limiter: LogLimiter,
}

#[derive(Debug, Default, Clone, Copy)]
struct HostState {
    consecutive_failures: u32,
    parked_until: Option<Instant>,
}

impl Dispatcher {
    pub fn new(pool: PgPool, cfg: WebhookConfig, wake: Arc<Notify>) -> Result<Arc<Self>, reqwest::Error> {
        let mut builder = reqwest::Client::builder()
            .user_agent(concat!("gum-indexer/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_millis(cfg.connect_timeout_ms))
            .timeout(Duration::from_millis(cfg.request_timeout_ms))
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(16)
            // A redirect could bounce a vetted public URL to an internal one.
            .redirect(reqwest::redirect::Policy::none());
        if !cfg.allow_insecure_targets {
            builder = builder.dns_resolver(Arc::new(target::PublicOnlyResolver { policy: cfg.target_policy() }));
        }
        Ok(Arc::new(Self {
            client: builder.build()?,
            permits: Arc::new(Semaphore::new(cfg.max_concurrency.max(1))),
            pool,
            cfg,
            wake,
            hosts: Mutex::new(HashMap::new()),
            limiter: LogLimiter::new(Duration::from_secs(30)),
        }))
    }

    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        let lease =
            Duration::from_millis(self.cfg.request_timeout_ms + self.cfg.connect_timeout_ms) + Duration::from_secs(5);
        let mut gauge_tick = tokio::time::interval(Duration::from_secs(15));
        loop {
            // Drain everything that is due, bounded by free workers.
            loop {
                let free = self.permits.available_permits();
                if free == 0 || cancel.is_cancelled() {
                    break;
                }
                let items = match outbox::lease_due(&self.pool, free as i64, lease).await {
                    Ok(items) => items,
                    Err(e) => {
                        if let Some(suppressed) = self.limiter.check("outbox_lease_failed") {
                            tracing::warn!(error.kind = e.kind(), error = %e, suppressed, "cannot read webhook outbox; deliveries are paused");
                        }
                        break;
                    }
                };
                if items.is_empty() {
                    break;
                }
                for item in items {
                    let permit = self.permits.clone().acquire_owned().await.expect("semaphore is never closed");
                    let this = self.clone();
                    tokio::spawn(async move {
                        this.deliver(item).await;
                        drop(permit);
                        // A delivered event may unblock the next one of the same watch.
                        this.wake.notify_one();
                    });
                }
            }
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = self.wake.notified() => {}
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                _ = gauge_tick.tick() => self.publish_depth().await,
            }
        }
        // Graceful shutdown: let in-flight deliveries finish (their leases protect them if we are killed anyway).
        let all = self.cfg.max_concurrency.max(1) as u32;
        let _ = tokio::time::timeout(Duration::from_secs(15), self.permits.acquire_many(all)).await;
    }

    async fn publish_depth(&self) {
        if let Ok(d) = outbox::depth(&self.pool).await {
            metrics::gauge!("gum_outbox_pending").set(d.pending as f64);
            metrics::gauge!("gum_outbox_dead").set(d.dead as f64);
            metrics::gauge!("gum_outbox_oldest_age_seconds").set(d.oldest_pending_age_secs);
        }
    }

    async fn deliver(&self, item: OutboxItem) {
        let event = leak_event(&item.event_type);
        let host = url::Url::parse(&item.url).ok().and_then(|u| u.host_str().map(str::to_owned)).unwrap_or_default();

        if let Some(wait) = self.parked_for(&host) {
            let _ = outbox::release(&self.pool, item.id, wait).await;
            return;
        }

        let body = match serde_json::to_vec(&item.payload) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(event_id = %item.id, error.kind = "webhook_payload", error = %e, "unserialisable payload; event dropped");
                let _ = outbox::mark_dead(&self.pool, item.id, None, "unserialisable payload").await;
                return;
            }
        };
        let now = chrono::Utc::now();
        let started = Instant::now();
        let result = self
            .client
            .post(&item.url)
            .header("content-type", "application/json")
            .header("x-gum-event-id", item.id.to_string())
            .header("x-gum-event-type", &item.event_type)
            .header("x-gum-delivery-attempt", item.attempts.to_string())
            .header("x-gum-signature", sign::signature_header(&self.cfg.secret, now.timestamp(), &body))
            .body(body)
            .send()
            .await;
        metrics::histogram!("gum_webhook_latency_seconds").record(started.elapsed().as_secs_f64());

        let (status, error) = match result {
            Ok(resp) if resp.status().is_success() => {
                self.host_ok(&host);
                metrics::counter!("gum_webhook_deliveries_total", "event" => event, "outcome" => "delivered")
                    .increment(1);
                metrics::histogram!("gum_webhook_end_to_end_seconds")
                    .record((now - item.created_at).to_std().unwrap_or_default().as_secs_f64());
                if let Err(e) = outbox::mark_delivered(&self.pool, item.id, resp.status().as_u16()).await {
                    // The consumer got it; the lease will expire and it will be sent again. At-least-once.
                    tracing::warn!(event_id = %item.id, error.kind = e.kind(), error = %e, "delivered but could not record it; event will be re-sent");
                }
                return;
            }
            Ok(resp) => (Some(resp.status().as_u16()), format!("HTTP {}", resp.status())),
            Err(e) => (None, describe(&e)),
        };

        self.host_failed(&host);
        let age = (now - item.created_at).to_std().unwrap_or_default();
        if age >= Duration::from_secs(self.cfg.max_age_secs) {
            metrics::counter!("gum_webhook_deliveries_total", "event" => event, "outcome" => "dead").increment(1);
            tracing::error!(
                event_id = %item.id, watch_id = %item.watch_id, event, host, attempts = item.attempts, last_error = %error,
                error.kind = "webhook_dead", "webhook undeliverable; giving up on this event"
            );
            let _ = outbox::mark_dead(&self.pool, item.id, status, &error).await;
            return;
        }
        metrics::counter!("gum_webhook_deliveries_total", "event" => event, "outcome" => "retry").increment(1);
        let delay = backoff(self.cfg.retry_base_ms, self.cfg.retry_cap_ms, item.attempts);
        if let Some(suppressed) = self.limiter.check("webhook_failed") {
            tracing::warn!(host, event, attempts = item.attempts, error = %error, retry_in_ms = delay.as_millis() as u64, suppressed,
                error.kind = "webhook_failed", "webhook delivery failed; will retry");
        }
        let _ = outbox::mark_retry(&self.pool, item.id, delay, status, &error).await;
    }

    fn parked_for(&self, host: &str) -> Option<Duration> {
        let hosts = self.hosts.lock().unwrap();
        let until = hosts.get(host)?.parked_until?;
        until.checked_duration_since(Instant::now())
    }

    fn host_ok(&self, host: &str) {
        let mut hosts = self.hosts.lock().unwrap();
        if let Some(state) = hosts.remove(host)
            && state.parked_until.is_some()
        {
            tracing::info!(host, "webhook host recovered");
        }
    }

    fn host_failed(&self, host: &str) {
        let mut hosts = self.hosts.lock().unwrap();
        let state = hosts.entry(host.to_owned()).or_default();
        state.consecutive_failures += 1;
        if state.consecutive_failures >= self.cfg.host_failure_threshold {
            let first = state.parked_until.is_none();
            state.parked_until = Some(Instant::now() + Duration::from_millis(self.cfg.host_park_ms));
            if first {
                tracing::warn!(
                    host,
                    failures = state.consecutive_failures,
                    park_ms = self.cfg.host_park_ms,
                    error.kind = "webhook_host_parked",
                    "webhook host keeps failing; pausing deliveries to it"
                );
            }
        }
    }
}

/// Full jitter: uniform in `[base, min(cap, base * 2^attempt)]`.
pub fn backoff(base_ms: u64, cap_ms: u64, attempt: i32) -> Duration {
    let exp = base_ms.saturating_mul(1u64 << attempt.clamp(0, 30) as u32).min(cap_ms).max(base_ms);
    Duration::from_millis(fastrand::u64(base_ms..=exp))
}

fn describe(e: &reqwest::Error) -> String {
    let kind = if e.is_timeout() {
        "timeout"
    } else if e.is_connect() {
        "connect"
    } else if e.is_redirect() {
        "redirect"
    } else {
        "request"
    };
    format!("{kind}: {e}")
}

fn leak_event(event_type: &str) -> &'static str {
    match event_type {
        "payment.pending" => "payment.pending",
        "payment.confirmed" => "payment.confirmed",
        "payment.orphaned" => "payment.orphaned",
        "threshold.reached" => "threshold.reached",
        "watch.expired" => "watch.expired",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_and_is_capped() {
        for _ in 0..200 {
            assert_eq!(backoff(1000, 60_000, 0), Duration::from_millis(1000));
            let third = backoff(1000, 60_000, 3).as_millis();
            assert!((1000..=8000).contains(&third), "{third}");
            let late = backoff(1000, 60_000, 25).as_millis();
            assert!((1000..=60_000).contains(&late), "{late}");
        }
    }
}
