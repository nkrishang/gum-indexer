//! Fault injection between the indexer and Anvil.
//!
//! * [`TcpProxy`]: transport-level faults for HTTP and WSS alike (sever connections, refuse new ones).
//! * [`RpcProxy`]: JSON-RPC-aware HTTP proxy (inject error responses per method, emulate provider range limits,
//!   count calls per method — which is also how tests and benchmarks measure credit usage).

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

pub struct TcpProxy {
    pub addr: SocketAddr,
    accepting: Arc<AtomicBool>,
    connections: Arc<Mutex<CancellationToken>>,
    task: tokio::task::JoinHandle<()>,
}

impl TcpProxy {
    pub async fn start(target_port: u16) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let addr = listener.local_addr().expect("proxy addr");
        let accepting = Arc::new(AtomicBool::new(true));
        let connections = Arc::new(Mutex::new(CancellationToken::new()));
        let (acc, conns) = (accepting.clone(), connections.clone());
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut inbound, _)) = listener.accept().await else { continue };
                if !acc.load(Ordering::Acquire) {
                    continue; // dropped immediately: connection reset for the client
                }
                let token = conns.lock().unwrap().child_token();
                tokio::spawn(async move {
                    let Ok(mut outbound) = TcpStream::connect(("127.0.0.1", target_port)).await else { return };
                    tokio::select! {
                        _ = token.cancelled() => {}
                        _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound) => {}
                    }
                });
            }
        });
        Self { addr, accepting, connections, task }
    }

    pub fn http_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn ws_url(&self) -> String {
        format!("ws://{}", self.addr)
    }

    /// Kills every open connection (clients may reconnect right away if still accepting).
    pub fn sever(&self) {
        let mut guard = self.connections.lock().unwrap();
        guard.cancel();
        *guard = CancellationToken::new();
    }

    /// Full outage: existing connections die and new ones are refused until `restore`.
    pub fn outage(&self) {
        self.accepting.store(false, Ordering::Release);
        self.sever();
    }

    pub fn restore(&self) {
        self.accepting.store(true, Ordering::Release);
    }
}

impl Drop for TcpProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Debug, Clone)]
pub enum Fault {
    Http(u16),
    Rpc { code: i64, message: String },
    Hang(Duration),
}

#[derive(Default)]
struct RpcProxyState {
    faults: HashMap<String, (u32, Fault)>,
    calls: HashMap<String, u64>,
    max_log_range: Option<u64>,
}

#[derive(Clone)]
struct RpcProxyShared {
    target: String,
    client: reqwest::Client,
    state: Arc<Mutex<RpcProxyState>>,
}

pub struct RpcProxy {
    pub addr: SocketAddr,
    shared: RpcProxyShared,
    task: tokio::task::JoinHandle<()>,
}

impl RpcProxy {
    pub async fn start(target_http_url: String) -> Self {
        let shared = RpcProxyShared { target: target_http_url, client: reqwest::Client::new(), state: Arc::default() };
        let app = Router::new().route("/", post(forward)).with_state(shared.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind rpc proxy");
        let addr = listener.local_addr().expect("rpc proxy addr");
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { addr, shared, task }
    }

    pub fn http_url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// The next `times` calls of `method` fail with `fault`.
    pub fn fail(&self, method: &str, times: u32, fault: Fault) {
        self.shared.state.lock().unwrap().faults.insert(method.to_owned(), (times, fault));
    }

    pub fn clear_faults(&self) {
        self.shared.state.lock().unwrap().faults.clear();
    }

    /// Emulates a provider that rejects `eth_getLogs` spans above `blocks` (QuickNode: 10,000; Monad: 100).
    pub fn limit_log_range(&self, blocks: u64) {
        self.shared.state.lock().unwrap().max_log_range = Some(blocks);
    }

    pub fn calls(&self, method: &str) -> u64 {
        self.shared.state.lock().unwrap().calls.get(method).copied().unwrap_or(0)
    }

    pub fn total_calls(&self) -> u64 {
        self.shared.state.lock().unwrap().calls.values().sum()
    }

    pub fn call_counts(&self) -> HashMap<String, u64> {
        self.shared.state.lock().unwrap().calls.clone()
    }
}

impl Drop for RpcProxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn rpc_error(id: &Value, code: i64, message: &str) -> Response {
    axum::Json(json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })).into_response()
}

fn block_param(v: Option<&Value>) -> Option<u64> {
    u64::from_str_radix(v?.as_str()?.strip_prefix("0x")?, 16).ok()
}

async fn forward(State(shared): State<RpcProxyShared>, body: Bytes) -> Response {
    let request: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let method = request.get("method").and_then(Value::as_str).unwrap_or("").to_owned();
    let id = request.get("id").cloned().unwrap_or(Value::Null);

    let fault = {
        let mut state = shared.state.lock().unwrap();
        *state.calls.entry(method.clone()).or_default() += 1;
        let fault = match state.faults.get_mut(&method) {
            Some((left, fault)) if *left > 0 => {
                *left -= 1;
                Some(fault.clone())
            }
            _ => None,
        };
        if fault.is_none()
            && method == "eth_getLogs"
            && let Some(max) = state.max_log_range
        {
            let filter = request.pointer("/params/0");
            let from = block_param(filter.and_then(|f| f.get("fromBlock")));
            let to = block_param(filter.and_then(|f| f.get("toBlock")));
            if let (Some(from), Some(to)) = (from, to)
                && to.saturating_sub(from) > max
            {
                return rpc_error(&id, -32602, &format!("eth_getLogs is limited to a {max} range"));
            }
        }
        fault
    };
    match fault {
        Some(Fault::Http(status)) => {
            return StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY).into_response();
        }
        Some(Fault::Rpc { code, message }) => return rpc_error(&id, code, &message),
        Some(Fault::Hang(d)) => tokio::time::sleep(d).await,
        None => {}
    }
    match shared.client.post(&shared.target).header("content-type", "application/json").body(body).send().await {
        Ok(resp) => {
            let status = StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let bytes = resp.bytes().await.unwrap_or_default();
            (status, [("content-type", "application/json")], bytes).into_response()
        }
        Err(_) => StatusCode::BAD_GATEWAY.into_response(),
    }
}
