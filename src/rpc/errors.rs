//! Classification of RPC failures into the handful of reactions the indexer has.
//! Codes follow QuickNode's error reference; message matching covers other providers and Anvil.

use alloy::transports::{RpcError, TransportError, TransportErrorKind};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcErrorClass {
    /// Per-second limit (HTTP 429, -32007, -32011): short backoff.
    RateLimited,
    /// Per-minute limit (-32008): back off for a minute.
    RateLimitedLong,
    /// Node hiccup, 5xx, connection reset, garbage body, timeout: retry with backoff.
    Transient,
    /// Block range / result set / response too large (HTTP 413, -32602 range, -32005, -32616): shrink the range.
    RangeTooLarge,
    /// The serving node is behind the requested `fromBlock` (load-balanced backends): treat as "nothing yet".
    AheadOfHead,
    /// 401/403: bad token, IP not allow-listed, endpoint disabled. Retrying cannot help; alert.
    Auth,
    /// Malformed request (-32600/-32601/-32602/-32603): a bug on our side. Never retried.
    Invalid,
}

impl RpcErrorClass {
    pub fn is_retryable(self) -> bool {
        matches!(self, Self::RateLimited | Self::RateLimitedLong | Self::Transient)
    }

    /// Stable identifier for logs (`error.kind`) and metrics (`outcome`).
    pub fn kind(self) -> &'static str {
        match self {
            Self::RateLimited => "rpc_rate_limited",
            Self::RateLimitedLong => "rpc_rate_limited_minute",
            Self::Transient => "rpc_transient",
            Self::RangeTooLarge => "rpc_range_too_large",
            Self::AheadOfHead => "rpc_node_behind",
            Self::Auth => "rpc_auth",
            Self::Invalid => "rpc_invalid_request",
        }
    }
}

pub fn classify(err: &TransportError) -> RpcErrorClass {
    match err {
        RpcError::ErrorResp(payload) => classify_code(payload.code, &payload.message),
        RpcError::Transport(TransportErrorKind::HttpError(http)) => classify_http(http.status, &http.body),
        RpcError::Transport(_) | RpcError::NullResp | RpcError::DeserError { .. } => RpcErrorClass::Transient,
        RpcError::SerError(_) | RpcError::UnsupportedFeature(_) | RpcError::LocalUsageError(_) => {
            RpcErrorClass::Invalid
        }
    }
}

pub fn classify_http(status: u16, body: &str) -> RpcErrorClass {
    match status {
        429 => RpcErrorClass::RateLimited,
        401 | 403 => RpcErrorClass::Auth,
        413 => RpcErrorClass::RangeTooLarge,
        500..=599 => RpcErrorClass::Transient,
        408 => RpcErrorClass::Transient,
        _ => {
            // Some gateways wrap a JSON-RPC error in a 4xx.
            if let Some(class) = classify_message(body) { class } else { RpcErrorClass::Invalid }
        }
    }
}

pub fn classify_code(code: i64, message: &str) -> RpcErrorClass {
    if let Some(class) = classify_message(message) {
        return class;
    }
    match code {
        -32007 | -32011 | 429 => RpcErrorClass::RateLimited,
        -32008 => RpcErrorClass::RateLimitedLong,
        -32005 => RpcErrorClass::RateLimited,
        -32616 => RpcErrorClass::RangeTooLarge,
        -32002..=-32000 => RpcErrorClass::Transient,
        -32603..=-32600 | -32611 | -32700 => RpcErrorClass::Invalid,
        _ => RpcErrorClass::Transient,
    }
}

fn classify_message(message: &str) -> Option<RpcErrorClass> {
    let m = message.to_ascii_lowercase();
    const AHEAD: [&str; 5] = [
        "beyond current head",
        "greater than toblock",
        "invalid block range",
        "fromblock is greater",
        "block range is invalid",
    ];
    const TOO_LARGE: [&str; 9] = [
        "limited to a",
        "block range",
        "exceeds limit",
        "exceeds the limit",
        "too large",
        "more than 10000",
        "response size",
        "too many results",
        "range is too",
    ];
    if AHEAD.iter().any(|p| m.contains(p)) {
        return Some(RpcErrorClass::AheadOfHead);
    }
    if TOO_LARGE.iter().any(|p| m.contains(p)) {
        return Some(RpcErrorClass::RangeTooLarge);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quicknode_codes() {
        assert_eq!(classify_code(-32007, "50/second request limit reached"), RpcErrorClass::RateLimited);
        assert_eq!(classify_code(-32008, "per-minute request limit"), RpcErrorClass::RateLimitedLong);
        assert_eq!(classify_code(-32011, "network connection issue"), RpcErrorClass::RateLimited);
        assert_eq!(classify_code(-32616, "response size exceeded"), RpcErrorClass::RangeTooLarge);
        assert_eq!(classify_code(-32000, "header not found"), RpcErrorClass::Transient);
        assert_eq!(classify_code(-32603, "invalid payload"), RpcErrorClass::Invalid);
        assert_eq!(classify_code(-32602, "missing 0x prefix"), RpcErrorClass::Invalid);
    }

    #[test]
    fn range_errors_are_recognised_by_message_across_providers() {
        for (code, msg) in [
            (-32602, "eth_getLogs is limited to a 10,000 range"),
            (-32614, "eth_getLogs is limited to a 100 range"),
            (-32005, "query returned more than 10000 results"),
            (-32000, "logs matched by query exceeds limit of 10000"),
            (-32000, "backend response too large"),
        ] {
            assert_eq!(classify_code(code, msg), RpcErrorClass::RangeTooLarge, "{msg}");
        }
    }

    #[test]
    fn node_behind_cursor_is_not_an_error_condition() {
        assert_eq!(classify_code(-32000, "block range extends beyond current head block"), RpcErrorClass::AheadOfHead);
        assert_eq!(classify_code(-32602, "invalid block range params"), RpcErrorClass::AheadOfHead);
    }

    #[test]
    fn http_statuses() {
        assert_eq!(classify_http(429, ""), RpcErrorClass::RateLimited);
        assert_eq!(classify_http(401, ""), RpcErrorClass::Auth);
        assert_eq!(classify_http(403, ""), RpcErrorClass::Auth);
        assert_eq!(classify_http(413, ""), RpcErrorClass::RangeTooLarge);
        assert_eq!(classify_http(502, "<html>bad gateway</html>"), RpcErrorClass::Transient);
        assert_eq!(classify_http(400, "bad request"), RpcErrorClass::Invalid);
        assert!(!RpcErrorClass::Auth.is_retryable() && !RpcErrorClass::Invalid.is_retryable());
        assert!(RpcErrorClass::Transient.is_retryable());
    }
}
