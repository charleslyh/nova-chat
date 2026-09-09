//! SSRF defence in depth for reference URLs (SEC-6).
//!
//! Image and file parts may carry an `https` URL. The **execution side will fetch
//! it**, which makes this a second SSRF entry point next to peer forwarding.
//! Rejection therefore happens at the gateway boundary, before the URL is ever
//! stored or handed onwards.
//!
//! Scope: this is a *first* layer. It cannot defeat DNS rebinding on its own — a
//! hostname resolving to a private address at fetch time still needs to be blocked
//! by the fetching component. That responsibility is documented in
//! `docs/design/06-protocol-subset.md`.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use thiserror::Error;
use url::{Host, Url};

#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum UrlRejection {
    #[error("malformed url")]
    Malformed,
    /// Covers `data:` (inline binary smuggling), `file:`, `http:`, `gopher:` …
    #[error("scheme `{0}` is not allowed; only https is accepted")]
    SchemeNotAllowed(String),
    #[error("url has no host")]
    MissingHost,
    #[error("host resolves to a non-public address range")]
    PrivateAddress,
    #[error("credentials in url are not allowed")]
    EmbeddedCredentials,
    #[error("url exceeds {max} bytes")]
    TooLong { max: usize },
}

/// Hostnames that must never be fetched even before resolution.
const BLOCKED_HOST_SUFFIXES: &[&str] = &[
    "localhost",
    ".localhost",
    ".local",
    ".internal",
    ".intranet",
    ".corp",
    ".home",
    ".lan",
];

/// Reject anything that is not a plain public `https` URL.
///
/// `max_bytes` comes from [`crate::protocol::ProtocolLimits`] rather than a
/// constant here, so every input bound is reachable from one place.
pub fn ensure_public_https(raw: &str, max_bytes: usize) -> Result<(), UrlRejection> {
    if raw.len() > max_bytes {
        return Err(UrlRejection::TooLong { max: max_bytes });
    }

    let url = Url::parse(raw).map_err(|_| UrlRejection::Malformed)?;

    if url.scheme() != "https" {
        return Err(UrlRejection::SchemeNotAllowed(url.scheme().to_string()));
    }

    // `https://user:pass@host` can be used to confuse downstream parsers.
    if !url.username().is_empty() || url.password().is_some() {
        return Err(UrlRejection::EmbeddedCredentials);
    }

    match url.host() {
        None => Err(UrlRejection::MissingHost),
        Some(Host::Ipv4(v4)) if is_blocked_v4(v4) => Err(UrlRejection::PrivateAddress),
        Some(Host::Ipv6(v6)) if is_blocked_v6(v6) => Err(UrlRejection::PrivateAddress),
        Some(Host::Domain(name)) => {
            let lowered = name.to_ascii_lowercase();
            if BLOCKED_HOST_SUFFIXES
                .iter()
                .any(|s| lowered == *s || lowered.ends_with(s))
            {
                return Err(UrlRejection::PrivateAddress);
            }
            // A bare literal that `url` classified as a domain (e.g. "010.0.0.1")
            // still deserves an IP check.
            match lowered.parse::<IpAddr>() {
                Ok(ip) if is_blocked_ip(ip) => Err(UrlRejection::PrivateAddress),
                _ => Ok(()),
            }
        }
        Some(_) => Ok(()),
    }
}

pub fn is_blocked_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_blocked_v4(v4),
        IpAddr::V6(v6) => is_blocked_v6(v6),
    }
}

/// Everything that is not a routable, public unicast address, plus the ranges the
/// deployment targets reserve for internal fabric (9/11/21/30).
///
/// Written as two non-overlapping groups — what the standard library can already
/// answer, and what it cannot. The previous version asked `is_multicast`,
/// `is_broadcast` and `is_unspecified` and *also* matched `0`, `240..=255`
/// separately, so three rules covered the same addresses and none of them was
/// obviously redundant.
fn is_blocked_v4(ip: Ipv4Addr) -> bool {
    if ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_documentation()
    {
        return true;
    }
    let [a, b, ..] = ip.octets();
    match a {
        // 0.0.0.0/8: "this network", never a destination.
        0 => true,
        // Cloud-internal fabric ranges: routable in theory, never a legitimate
        // fetch target here.
        9 | 11 | 21 | 30 => true,
        // Carrier-grade NAT 100.64.0.0/10.
        100 => (64..=127).contains(&b),
        // Multicast (224/4), reserved (240/4) and broadcast — nothing below is
        // unicast, so one arm covers all three.
        224..=255 => true,
        _ => false,
    }
}

fn is_blocked_v6(ip: Ipv6Addr) -> bool {
    // IPv4-mapped / IPv4-compatible: unwrap and apply the v4 rules, so the two
    // families cannot disagree about the same address.
    if let Some(v4) = ip.to_ipv4() {
        return is_blocked_v4(v4);
    }
    if ip.is_loopback() || ip.is_unspecified() || ip.is_multicast() {
        return true;
    }
    let segments = ip.segments();
    // Unique local fc00::/7 and link-local fe80::/10.
    segments[0] & 0xfe00 == 0xfc00 || segments[0] & 0xffc0 == 0xfe80
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX: usize = 2048;

    fn check(raw: &str) -> Result<(), UrlRejection> {
        ensure_public_https(raw, MAX)
    }

    #[test]
    fn accepts_plain_public_https() {
        assert!(check("https://cdn.example.com/a.png").is_ok());
        // A genuinely routable public address. Note 203.0.113.0/24 (TEST-NET-3) is
        // *not* usable here — it is reserved for documentation and therefore
        // correctly blocked below.
        assert!(check("https://93.184.216.34/a.png").is_ok());
    }

    #[test]
    fn rejects_non_https_schemes() {
        // `data:` is how inline binary would sneak back in.
        for raw in [
            "data:image/png;base64,AAAA",
            "http://cdn.example.com/a.png",
            "file:///etc/passwd",
            "gopher://example.com/",
        ] {
            assert!(
                matches!(
                    check(raw),
                    Err(UrlRejection::SchemeNotAllowed(_)) | Err(UrlRejection::Malformed)
                ),
                "expected rejection for {raw}"
            );
        }
    }

    #[test]
    fn rejects_internal_ranges() {
        for raw in [
            "https://127.0.0.1/a",
            "https://10.1.2.3/a",
            "https://172.16.0.1/a",
            "https://172.31.255.254/a",
            "https://192.168.1.1/a",
            "https://169.254.169.254/latest/meta-data/",
            "https://9.1.2.3/a",
            "https://11.1.2.3/a",
            "https://21.1.2.3/a",
            "https://30.1.2.3/a",
            "https://100.64.0.1/a",
            "https://0.0.0.0/a",
            "https://255.255.255.255/a",
            "https://239.1.2.3/a",
            "https://[::1]/a",
            "https://[fd00::1]/a",
            "https://[fe80::1]/a",
            "https://localhost/a",
            "https://foo.internal/a",
            "https://svc.local/a",
        ] {
            assert_eq!(
                check(raw),
                Err(UrlRejection::PrivateAddress),
                "expected private-range rejection for {raw}"
            );
        }
    }

    #[test]
    fn an_ipv4_mapped_ipv6_address_gets_the_ipv4_rules() {
        // Otherwise the same address would be blocked in one notation and allowed
        // in the other.
        assert_eq!(check("https://[::ffff:10.0.0.1]/a"), Err(UrlRejection::PrivateAddress));
        assert!(is_blocked_ip("::ffff:127.0.0.1".parse().unwrap()));
    }

    #[test]
    fn rejects_public_ranges_adjacent_to_blocked_ones() {
        // 172.15 / 172.32 are public; only 172.16-31 is private.
        assert!(check("https://172.15.0.1/a").is_ok());
        assert!(check("https://172.32.0.1/a").is_ok());
        // 100.128 is outside CGNAT.
        assert!(check("https://100.128.0.1/a").is_ok());
        // 223 is the last unicast /8.
        assert!(check("https://223.1.2.3/a").is_ok());
    }

    #[test]
    fn rejects_reserved_documentation_ranges() {
        // Not "internal" in the routing sense, but never a legitimate fetch target
        // either.
        for raw in [
            "https://192.0.2.1/a",
            "https://203.0.113.1/a",
            "https://198.51.100.1/a",
        ] {
            assert_eq!(
                check(raw),
                Err(UrlRejection::PrivateAddress),
                "expected rejection for {raw}"
            );
        }
    }

    #[test]
    fn rejects_embedded_credentials_and_overlong() {
        assert_eq!(
            check("https://u:p@example.com/a"),
            Err(UrlRejection::EmbeddedCredentials)
        );
        let long = format!("https://example.com/{}", "a".repeat(MAX));
        assert_eq!(check(&long), Err(UrlRejection::TooLong { max: MAX }));
    }

    #[test]
    fn the_length_bound_is_the_configured_one() {
        // It used to be a constant here, unreachable from config while every other
        // input bound was operator-tunable.
        assert_eq!(
            ensure_public_https("https://example.com/aaaa", 8),
            Err(UrlRejection::TooLong { max: 8 })
        );
        assert!(ensure_public_https("https://example.com/aaaa", 4096).is_ok());
    }
}
