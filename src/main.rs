use std::path::Path;

use gum_indexer::{app::App, config::Config, store, telemetry};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let json_logs = std::env::var("GUM_PROFILE").map_or(true, |p| p != "local");
    telemetry::init_logging(json_logs);

    let cfg = match Config::load(Path::new("config")) {
        Ok(cfg) => cfg,
        Err(e) => {
            tracing::error!(error.kind = "config_invalid", error = %e, "refusing to start");
            std::process::exit(2);
        }
    };
    let pool = store::connect(&cfg.database.url, cfg.database.max_connections).await?;
    let app = App::start(cfg, pool).await?;

    shutdown_signal().await;
    tracing::info!("shutdown signal received; draining");
    app.shutdown().await;
    tracing::info!("stopped");
    Ok(())
}

async fn shutdown_signal() {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler installs");
    tokio::select! {
        _ = term.recv() => {}
        _ = tokio::signal::ctrl_c() => {}
    }
}
