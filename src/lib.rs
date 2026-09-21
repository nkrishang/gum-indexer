//! gum-indexer: watches payment addresses for ERC-20 transfers and notifies webhooks.

pub mod api;
pub mod app;
pub mod cache;
pub mod config;
pub mod events;
pub mod ingest;
pub mod registry;
pub mod rpc;
pub mod store;
pub mod telemetry;
#[cfg(feature = "testkit")]
pub mod testkit;
pub mod webhook;
