//! Webhook target validation. Callers choose the URL we POST to, so without checks the service could be
//! pointed at internal infrastructure (SSRF). Literal hosts are vetted at registration; DNS answers are vetted
//! at delivery time, which also covers DNS rebinding.

use std::net::{IpAddr, SocketAddr};

use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use url::{Host, Url};

/// What a webhook target may be. Production keeps the defaults (public https only) and names the
/// consumers that live on the private network in `host_allowlist`; the local profile allows everything.
#[derive(Debug, Clone, Default)]
pub struct TargetPolicy {
    /// Accept http and private / loopback hosts everywhere (local development only).
    pub allow_insecure: bool,
    /// Hosts that may be private and reached over http, e.g. `gum-server.railway.internal`. Exact,
    /// case-insensitive host names; no wildcards, so the list stays an explicit inventory.
    pub host_allowlist: Vec<String>,
}

impl TargetPolicy {
    pub fn is_allowlisted(&self, host: &str) -> bool {
        self.host_allowlist.iter().any(|h| h.eq_ignore_ascii_case(host))
    }
}

pub fn validate(raw: &str, policy: &TargetPolicy) -> Result<Url, String> {
    let url = Url::parse(raw).map_err(|e| format!("not a valid URL: {e}"))?;
    if raw.len() > 2048 {
        return Err("URL is longer than 2048 characters".into());
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("credentials in the URL are not allowed".into());
    }
    let host = url.host().ok_or("URL has no host")?;
    let allowlisted = matches!(&host, Host::Domain(d) if policy.is_allowlisted(d));
    match url.scheme() {
        "https" => {}
        "http" if policy.allow_insecure || allowlisted => {}
        "http" => return Err("webhook_endpoint must use https".into()),
        other => return Err(format!("unsupported scheme {other}")),
    }
    if policy.allow_insecure || allowlisted {
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
/// Allowlisted hosts (the private-network consumers) keep every answer.
pub struct PublicOnlyResolver {
    pub policy: TargetPolicy,
}

impl Resolve for PublicOnlyResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let allowlisted = self.policy.is_allowlisted(name.as_str());
        Box::pin(async move {
            let host = name.as_str().to_owned();
            let resolved = tokio::net::lookup_host((host.as_str(), 0)).await?;
            let public: Vec<SocketAddr> = resolved.filter(|a| allowlisted || is_public(a.ip())).collect();
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

    fn production() -> TargetPolicy {
        TargetPolicy::default()
    }

    #[test]
    fn production_rules() {
        assert!(validate("https://merchant.example/hooks", &production()).is_ok());
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
            assert!(validate(bad, &production()).is_err(), "{bad} should be rejected");
        }
    }

    #[test]
    fn local_profile_allows_loopback_http() {
        let local = TargetPolicy { allow_insecure: true, host_allowlist: vec![] };
        assert!(validate("http://127.0.0.1:9000/hook", &local).is_ok());
        assert!(validate("gopher://127.0.0.1", &local).is_err());
    }

    #[test]
    fn allowlisted_private_hosts_are_accepted_over_http_and_nothing_else_changes() {
        let policy = TargetPolicy { allow_insecure: false, host_allowlist: vec!["gum-server.railway.internal".into()] };
        assert!(validate("http://gum-server.railway.internal:8080/v1/webhooks/indexer", &policy).is_ok());
        assert!(validate("http://GUM-SERVER.railway.internal:8080/x", &policy).is_ok(), "case-insensitive");
        assert!(validate("https://merchant.example/hooks", &policy).is_ok());
        assert!(validate("http://merchant.example/hooks", &policy).is_err(), "other hosts still need https");
        assert!(validate("http://postgres.railway.internal/hook", &policy).is_err(), "no wildcard on the suffix");
        assert!(validate("http://gum-server.railway.internal.evil.example/x", &policy).is_err(), "exact match only");
        assert!(validate("https://127.0.0.1/hook", &policy).is_err());
        assert!(policy.is_allowlisted("gum-server.railway.internal") && !policy.is_allowlisted("gum-server"));
    }
}
