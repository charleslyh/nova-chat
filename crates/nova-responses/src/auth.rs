//! Authentication and tenant resolution.
//!
//! Two rules:
//!
//! 1. **Keys come from the environment** (SEC-4). The config file names the
//!    variable; the values never touch disk here.
//! 2. **Ownership mismatch reports "not found", not "forbidden"** (SEC-2).
//!    `403` on a foreign id confirms that the id exists, which turns the API
//!    into an id oracle.

use std::collections::HashMap;

use axum::http::HeaderMap;
use nova_responses_core::TenantId;
use subtle::ConstantTimeEq;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    Missing,
    Invalid,
}

pub struct KeyTable {
    /// api key -> tenant
    keys: HashMap<String, TenantId>,
    /// Admin endpoints require this key when set.
    admin_key: Option<String>,
}

impl KeyTable {
    /// Parse `key:tenant` pairs, comma or whitespace separated.
    pub fn parse(spec: &str) -> Result<Self, String> {
        let mut keys = HashMap::new();
        for entry in spec.split([',', ' ', '\n', '\t']).filter(|s| !s.is_empty()) {
            let (key, tenant) = entry
                .split_once(':')
                .ok_or_else(|| "api key entry must be `key:tenant`".to_string())?;
            if key.len() < 8 {
                return Err("api keys must be at least 8 characters".into());
            }
            let tenant = TenantId::parse(tenant).map_err(|e| e.to_string())?;
            keys.insert(key.to_string(), tenant);
        }
        Ok(Self {
            keys,
            admin_key: None,
        })
    }

    /// Load from the environment. An empty table is allowed only so local
    /// verification can run unauthenticated; production always sets keys.
    pub fn from_env(keys_env: &str, admin_env: &str) -> Result<Self, String> {
        let spec = std::env::var(keys_env).unwrap_or_default();
        let mut table = Self::parse(&spec)?;
        table.admin_key = std::env::var(admin_env).ok().filter(|v| !v.is_empty());
        Ok(table)
    }

    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    fn lookup(&self, key: &str) -> Option<&TenantId> {
        self.keys.get(key)
    }

    fn admin_key_matches(&self, presented: &str) -> bool {
        match &self.admin_key {
            // No admin key configured: admin endpoints stay open for local
            // verification. Deployments must set one.
            None => true,
            Some(expected) => {
                let equal: bool = expected.as_bytes().ct_eq(presented.as_bytes()).into();
                equal
            }
        }
    }

    /// Resolve the tenant for a request.
    pub fn resolve(&self, headers: &HeaderMap) -> Result<TenantId, AuthError> {
        let bearer = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(str::trim);

        match bearer {
            None => {
                if self.is_empty() {
                    // Unauthenticated local mode.
                    Ok(TenantId::parse("local").expect("static tenant is valid"))
                } else {
                    Err(AuthError::Missing)
                }
            }
            Some(key) => self.lookup(key).cloned().ok_or(AuthError::Invalid),
        }
    }

    pub fn authorize_admin(&self, headers: &HeaderMap) -> Result<(), AuthError> {
        let presented = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .map(str::trim)
            .unwrap_or_default();
        if self.admin_key_matches(presented) {
            Ok(())
        } else {
            Err(AuthError::Invalid)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table() -> KeyTable {
        KeyTable::parse("key-aaaaaaa:tenant-a,key-bbbbbbb:tenant-b").unwrap()
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        h
    }

    #[test]
    fn resolves_bearer_to_tenant() {
        let t = table();
        let tenant = t
            .resolve(&headers(&[("authorization", "Bearer key-aaaaaaa")]))
            .unwrap();
        assert_eq!(tenant.as_str(), "tenant-a");
    }

    #[test]
    fn rejects_unknown_and_missing_keys() {
        let t = table();
        assert_eq!(
            t.resolve(&headers(&[("authorization", "Bearer nope-nope")])),
            Err(AuthError::Invalid)
        );
        assert_eq!(t.resolve(&headers(&[])), Err(AuthError::Missing));
    }

    #[test]
    fn empty_table_allows_local_unauthenticated_use() {
        let t = KeyTable::parse("").unwrap();
        assert!(t.is_empty());
        assert_eq!(t.resolve(&headers(&[])).unwrap().as_str(), "local");
    }

    #[test]
    fn rejects_weak_or_malformed_key_specs() {
        assert!(KeyTable::parse("short:t").is_err());
        assert!(KeyTable::parse("key-aaaaaaa").is_err());
        assert!(KeyTable::parse("key-aaaaaaa:bad tenant").is_err());
    }

    #[test]
    fn admin_requires_key_when_configured() {
        let mut t = table();
        t.admin_key = Some("admin-secret".into());
        assert!(t.authorize_admin(&headers(&[])).is_err());
        assert!(t
            .authorize_admin(&headers(&[("authorization", "Bearer admin-secret")]))
            .is_ok());
    }
}
