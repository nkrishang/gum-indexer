//! Webhook signatures: `X-Gum-Signature: t=<unix seconds>,v1=<hex hmac-sha256(secret, "<t>.<body>")>`.
//! Signing the timestamp together with the body lets consumers reject replays.

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

pub fn signature_header(secret: &str, timestamp: i64, body: &[u8]) -> String {
    format!("t={timestamp},v1={}", hex::encode(mac(secret, timestamp, body).finalize().into_bytes()))
}

/// Constant-time verification; `tolerance_secs` bounds how old a signed timestamp may be.
pub fn verify(secret: &str, header: &str, body: &[u8], now: i64, tolerance_secs: i64) -> bool {
    let mut timestamp = None;
    let mut digest = None;
    for part in header.split(',') {
        match part.trim().split_once('=') {
            Some(("t", v)) => timestamp = v.parse::<i64>().ok(),
            Some(("v1", v)) => digest = hex::decode(v).ok(),
            _ => {}
        }
    }
    let (Some(timestamp), Some(digest)) = (timestamp, digest) else { return false };
    if (now - timestamp).abs() > tolerance_secs {
        return false;
    }
    mac(secret, timestamp, body).verify_slice(&digest).is_ok()
}

fn mac(secret: &str, timestamp: i64, body: &[u8]) -> HmacSha256 {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    mac
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_tamper_detection() {
        let body = br#"{"id":"evt","type":"payment.confirmed"}"#;
        let header = signature_header("s3cret", 1_800_000_000, body);
        assert!(header.starts_with("t=1800000000,v1="));
        assert!(verify("s3cret", &header, body, 1_800_000_010, 300));
        assert!(!verify("other", &header, body, 1_800_000_010, 300), "wrong secret");
        assert!(!verify("s3cret", &header, b"{}", 1_800_000_010, 300), "tampered body");
        assert!(!verify("s3cret", &header, body, 1_800_001_000, 300), "replayed outside tolerance");
        assert!(!verify("s3cret", "garbage", body, 1_800_000_010, 300));
    }

    #[test]
    fn known_vector() {
        // printf '1.body' | openssl dgst -sha256 -hmac key
        assert_eq!(
            signature_header("key", 1, b"body"),
            "t=1,v1=91b5374b153842ad05b2c4eab9349b8321b14703165bd3fb8b034dfb8be98ae5"
        );
    }
}
