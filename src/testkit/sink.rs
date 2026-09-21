//! A webhook receiver that records what it gets and can be told to misbehave.

use std::{
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::{Duration, Instant},
};

use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::post,
};

use crate::events::EventPayload;

#[derive(Debug, Clone)]
pub struct Received {
    pub at: Instant,
    pub payload: EventPayload,
    pub body: Bytes,
    pub signature: String,
    pub attempt: u32,
}

#[derive(Clone, Default)]
struct Shared {
    received: Arc<Mutex<Vec<Received>>>,
    fail_next: Arc<AtomicU32>,
    down: Arc<AtomicBool>,
    rejected: Arc<AtomicU32>,
}

pub struct WebhookSink {
    pub addr: SocketAddr,
    shared: Shared,
    task: tokio::task::JoinHandle<()>,
}

impl WebhookSink {
    pub async fn start() -> Self {
        Self::start_on(0).await
    }

    pub async fn start_on(port: u16) -> Self {
        let shared = Shared::default();
        let app = Router::new().route("/hook", post(receive)).with_state(shared.clone());
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await.expect("bind sink");
        let addr = listener.local_addr().expect("sink addr");
        let task = tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { addr, shared, task }
    }

    pub fn url(&self) -> String {
        format!("http://{}/hook", self.addr)
    }

    /// The next `n` deliveries are answered with HTTP 500.
    pub fn fail_next(&self, n: u32) {
        self.shared.fail_next.store(n, Ordering::Release);
    }

    /// While down, every delivery is answered with HTTP 503.
    pub fn set_down(&self, down: bool) {
        self.shared.down.store(down, Ordering::Release);
    }

    pub fn rejected(&self) -> u32 {
        self.shared.rejected.load(Ordering::Acquire)
    }

    pub fn received(&self) -> Vec<Received> {
        self.shared.received.lock().unwrap().clone()
    }

    pub fn event_types(&self) -> Vec<&'static str> {
        self.received().iter().map(|r| r.payload.event_type.as_str()).collect()
    }

    /// Waits until `done` holds for the received events; panics with what was received otherwise.
    pub async fn wait_for(&self, what: &str, timeout: Duration, done: impl Fn(&[Received]) -> bool) -> Vec<Received> {
        let deadline = Instant::now() + timeout;
        loop {
            let got = self.received();
            if done(&got) {
                return got;
            }
            if Instant::now() > deadline {
                let types: Vec<_> = got.iter().map(|r| (r.payload.event_type.as_str(), r.payload.sequence)).collect();
                panic!("timed out after {timeout:?} waiting for {what}; received so far: {types:?}");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub async fn wait_for_types(&self, expected: &[&str], timeout: Duration) -> Vec<Received> {
        self.wait_for(&format!("{expected:?}"), timeout, |got| {
            let mut types: Vec<_> = got.iter().map(|r| r.payload.event_type.as_str()).collect();
            let mut want = expected.to_vec();
            types.sort_unstable();
            want.sort_unstable();
            types == want
        })
        .await
    }
}

impl Drop for WebhookSink {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn receive(State(shared): State<Shared>, headers: HeaderMap, body: Bytes) -> StatusCode {
    if shared.down.load(Ordering::Acquire) {
        shared.rejected.fetch_add(1, Ordering::AcqRel);
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    if shared.fail_next.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1)).is_ok() {
        shared.rejected.fetch_add(1, Ordering::AcqRel);
        return StatusCode::INTERNAL_SERVER_ERROR;
    }
    let Ok(payload) = serde_json::from_slice::<EventPayload>(&body) else { return StatusCode::BAD_REQUEST };
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok()).unwrap_or_default().to_owned();
    shared.received.lock().unwrap().push(Received {
        at: Instant::now(),
        payload,
        body,
        signature: header("x-gum-signature"),
        attempt: header("x-gum-delivery-attempt").parse().unwrap_or(0),
    });
    StatusCode::OK
}
