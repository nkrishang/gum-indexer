//! Webhook target validation. Callers choose the URL we POST to, so without checks the service could be
//! pointed at internal infrastructure (SSRF). Literal hosts are vetted at registration; DNS answers are vetted
//! at delivery time, which also covers DNS rebinding.

use std::net::{IpAddr, SocketAddr};

use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use url::{Host, Url};

pub fn validate(raw: &str, allow_insecure: bool) -> Result<Url, String> {
    let url = Url::parse(raw).map_err(|e| format!("not a valid URL: {e}"))?;
    if raw.len() > 2048 {
        return Err("URL is longer than 2048 characters".into());
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("credentials in the URL are not allowed".into());
    }
    match url.scheme() {
        "https" => {}
        "http" if allow_insecure => {}
        "http" => return Err("webhook_endpoint must use https".into()),
        other => return Err(format!("unsupported scheme {other}")),
    }
    let host = url.host().ok_or("URL has no host")?;
    if allow_insecure {
        return Ok(url);
    }
    match host {
        Host::Ipv4(ip) if !is_public(IpAddr::V4(ip)) => Err("webhook_endpoint must be a public address".into()),
        Host::Ipv6(ip) if !is_public(IpAddr::V6(ip)) => Err("webhook_endpoint must be a public address".into()),
        Host::Domain(d)
            if d.eq_ignore_ascii_case("localhost")
                || d.ends_with(".localhost")
                || d.ends_with(".internal")
                || d.ends_with(".local") =>
        {
            Err("webhook_endpoint must be a public address".into())
        }
        _ => Ok(url),
    }
}

pub fn is_public(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            let o = v4.octets();
            !(v4.is_private()
                || v4.is_loopback()
                || v4.is_link_local()
                || v4.is_broadcast()
                || v4.is_unspecified()
                || v4.is_documentation()
                || o[0] == 0
                || (o[0] == 100 && (64..128).contains(&o[1])) // CGNAT 100.64/10
                || (o[0] == 198 && (o[1] == 18 || o[1] == 19)) // benchmarking
                || o[0] >= 240)
        }
        IpAddr::V6(v6) => {
            if let Some(mapped) = v6.to_ipv4_mapped() {
                return is_public(IpAddr::V4(mapped));
            }
            let first = v6.segments()[0];
            !(v6.is_loopback()
                || v6.is_unspecified()
                || (first & 0xfe00) == 0xfc00 // unique local fc00::/7 (Railway private network lives here)
                || (first & 0xffc0) == 0xfe80 // link local
                || (first & 0xff00) == 0xff00) // multicast
        }
    }
}

/// Resolver that drops non-public answers; a name resolving only to private addresses fails to connect.
pub struct PublicOnlyResolver;

impl Resolve for PublicOnlyResolver {
    fn resolve(&self, name: Name) -> Resolving {
        Box::pin(async move {
            let host = name.as_str().to_owned();
            let resolved = tokio::net::lookup_host((host.as_str(), 0)).await?;
            let public: Vec<SocketAddr> = resolved.filter(|a| is_public(a.ip())).collect();
            if public.is_empty() {
                return Err(format!("{host} does not resolve to a public address").into());
            }
            Ok(Box::new(public.into_iter()) as Addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn production_rules() {
        assert!(validate("https://merchant.example/hooks", false).is_ok());
        for bad in [
            "http://merchant.example/hooks",
            "ftp://merchant.example",
            "https://localhost/hook",
            "https://127.0.0.1/hook",
            "https://10.1.2.3/hook",
            "https://169.254.169.254/latest/meta-data",
            "https://[::1]/hook",
            "https://[fd12::1]/hook",
            "https://[::ffff:10.0.0.1]/hook",
            "https://postgres.railway.internal/hook",
            "https://user:pw@merchant.example/hook",
            "not a url",
        ] {
            assert!(validate(bad, false).is_err(), "{bad} should be rejected");
        }
    }

    #[test]
    fn local_profile_allows_loopback_http() {
        assert!(validate("http://127.0.0.1:9000/hook", true).is_ok());
        assert!(validate("gopher://127.0.0.1", true).is_err());
    }
}
