//! Optional: polls the QuickNode Admin API so the real credit balance sits next to our local estimate.

use std::time::Duration;

use serde::Deserialize;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Deserialize)]
struct UsageEnvelope {
    data: Usage,
}

#[derive(Debug, Deserialize)]
struct Usage {
    credits_used: Option<f64>,
    credits_remaining: Option<f64>,
}

pub fn spawn(api_key: String, cancel: CancellationToken) {
    if api_key.is_empty() {
        return;
    }
    tokio::spawn(async move {
        let client = match reqwest::Client::builder().timeout(Duration::from_secs(15)).build() {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(error.kind = "quicknode_usage_client", error = %e, "cannot build HTTP client; credit gauges disabled");
                return;
            }
        };
        let mut tick = tokio::time::interval(Duration::from_secs(3600));
        let mut failing = false;
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tick.tick() => {}
            }
            let res = async {
                let resp = client
                    .get("https://api.quicknode.com/v0/usage/rpc")
                    .header("x-api-key", &api_key)
                    .send()
                    .await?
                    .error_for_status()?;
                resp.json::<UsageEnvelope>().await
            }
            .await;
            match res {
                Ok(env) => {
                    if let Some(v) = env.data.credits_used {
                        metrics::gauge!("gum_quicknode_credits_used").set(v);
                    }
                    if let Some(v) = env.data.credits_remaining {
                        metrics::gauge!("gum_quicknode_credits_remaining").set(v);
                    }
                    if failing {
                        tracing::info!("QuickNode usage polling recovered");
                        failing = false;
                    }
                }
                Err(e) if !failing => {
                    failing = true;
                    tracing::warn!(error.kind = "quicknode_usage", error = %e, "cannot read QuickNode usage; credit gauges are stale");
                }
                Err(_) => {}
            }
        }
    });
}
