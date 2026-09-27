//! Who is calling: a bearer token checked against an OIDC issuer's keys, or,
//! for local development only, the desktop's static token.

use std::collections::BTreeMap;
use std::io::Read;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, Instant};

use jsonwebtoken::jwk::{
    AlgorithmParameters, EllipticCurve, Jwk, JwkSet, KeyAlgorithm, KeyOperations, PublicKeyUse,
};
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};

/// The caller a token names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal {
    pub issuer: String,
    pub subject: String,
    pub tenant: String,
}

#[derive(Debug)]
pub enum AuthError {
    /// The token is missing, malformed, or not valid for this server (401).
    Unauthorized,
    /// The issuer's keys could not be read, so no token can be checked (503).
    Unavailable(String),
}

/// Checks a bearer token. It may block on the issuer's key set, so callers
/// run it off the async runtime.
pub trait Authenticator: Send + Sync {
    fn authenticate(&self, token: &str) -> Result<Principal, AuthError>;
}

const LEEWAY_SECS: u64 = 60;
const JWKS_TIMEOUT: Duration = Duration::from_secs(3);
/// An unknown kid, or a failed fetch, refetches the key set at most once per
/// interval.
const REFRESH_INTERVAL: Duration = Duration::from_secs(30);
/// A fetched key set is used for at most this long before it is refetched,
/// so a key the issuer removed stops verifying.
const MAX_AGE: Duration = Duration::from_secs(600);
const MAX_JWKS_BYTES: u64 = 1024 * 1024;
/// The static token the coworker desktop sends in local development.
pub const LOCAL_DEV_TOKEN: &str = "gol-gateway-local";

pub struct Missing;
pub struct Set;

enum KeySource {
    Url(String),
    Static(JwkSet),
}

struct Draft {
    issuer: Option<String>,
    audience: Option<String>,
    tenant_claim: Option<String>,
    keys: Option<KeySource>,
    refresh_interval: Duration,
    max_age: Duration,
}

/// The issuer, audience, tenant claim and key source. Each is required, and
/// `build` exists only once all four are set.
pub struct OidcConfig;

pub struct OidcConfigBuilder<I, A, T, K> {
    draft: Draft,
    _state: PhantomData<(I, A, T, K)>,
}

impl OidcConfig {
    pub fn builder() -> OidcConfigBuilder<Missing, Missing, Missing, Missing> {
        OidcConfigBuilder {
            draft: Draft {
                issuer: None,
                audience: None,
                tenant_claim: None,
                keys: None,
                refresh_interval: REFRESH_INTERVAL,
                max_age: MAX_AGE,
            },
            _state: PhantomData,
        }
    }
}

impl<I, A, T, K> OidcConfigBuilder<I, A, T, K> {
    fn retag<I2, A2, T2, K2>(self) -> OidcConfigBuilder<I2, A2, T2, K2> {
        OidcConfigBuilder {
            draft: self.draft,
            _state: PhantomData,
        }
    }

    /// How often an unknown kid, or a failed fetch, may refetch the key set
    /// (default 30 s).
    pub fn refresh_interval(mut self, interval: Duration) -> Self {
        self.draft.refresh_interval = interval;
        self
    }

    /// How long a fetched key set is used before it is refetched (default
    /// 10 min).
    pub fn max_age(mut self, age: Duration) -> Self {
        self.draft.max_age = age;
        self
    }
}

impl<A, T, K> OidcConfigBuilder<Missing, A, T, K> {
    pub fn issuer(mut self, issuer: impl Into<String>) -> OidcConfigBuilder<Set, A, T, K> {
        self.draft.issuer = Some(issuer.into());
        self.retag()
    }
}

impl<I, T, K> OidcConfigBuilder<I, Missing, T, K> {
    pub fn audience(mut self, audience: impl Into<String>) -> OidcConfigBuilder<I, Set, T, K> {
        self.draft.audience = Some(audience.into());
        self.retag()
    }
}

impl<I, A, K> OidcConfigBuilder<I, A, Missing, K> {
    /// The claim that names the caller's tenant.
    pub fn tenant_claim(mut self, claim: impl Into<String>) -> OidcConfigBuilder<I, A, Set, K> {
        self.draft.tenant_claim = Some(claim.into());
        self.retag()
    }
}

impl<I, A, T> OidcConfigBuilder<I, A, T, Missing> {
    /// Read the key set from the issuer's JWKS URL. Redirects are not
    /// followed; `auth_from_env` also requires https except on loopback.
    pub fn jwks_url(mut self, url: impl Into<String>) -> OidcConfigBuilder<I, A, T, Set> {
        self.draft.keys = Some(KeySource::Url(url.into()));
        self.retag()
    }

    /// Use a fixed key set, never fetched. For tests and air-gapped issuers.
    pub fn jwks_static(mut self, keys: JwkSet) -> OidcConfigBuilder<I, A, T, Set> {
        self.draft.keys = Some(KeySource::Static(keys));
        self.retag()
    }
}

impl OidcConfigBuilder<Set, Set, Set, Set> {
    pub fn build(self) -> OidcVerifier {
        let draft = self.draft;
        let (source, keys) = match draft.keys.expect("typestate recorded the keys") {
            KeySource::Url(url) => (
                Source::Url {
                    url,
                    agent: ureq::AgentBuilder::new()
                        .timeout(JWKS_TIMEOUT)
                        .redirects(0)
                        .build(),
                },
                Keys {
                    keys: Vec::new(),
                    loaded_at: None,
                },
            ),
            KeySource::Static(set) => (
                Source::Static,
                Keys {
                    keys: set.keys.into_iter().filter(usable).collect(),
                    loaded_at: None,
                },
            ),
        };
        OidcVerifier {
            issuer: draft.issuer.expect("typestate recorded the issuer"),
            audience: draft.audience.expect("typestate recorded the audience"),
            tenant_claim: draft.tenant_claim.expect("typestate recorded the claim"),
            refresh_interval: draft.refresh_interval,
            max_age: draft.max_age,
            source,
            keys: RwLock::new(keys),
            fetches: Mutex::new(Fetches {
                attempted: None,
                error: None,
                refreshed: None,
            }),
        }
    }
}

enum Source {
    Url { url: String, agent: ureq::Agent },
    Static,
}

struct Keys {
    keys: Vec<Jwk>,
    /// When the key set was fetched; `None` before the first fetch, and
    /// always for a static set.
    loaded_at: Option<Instant>,
}

/// Fetch bookkeeping. Its mutex is also the gate that lets one fetch run at
/// a time; lookups of a known kid never wait on it.
struct Fetches {
    /// When the last fetch was made, and why it failed, if it did.
    attempted: Option<Instant>,
    error: Option<String>,
    /// When an unknown kid last refetched a loaded key set. The first load
    /// does not count, so a kid published just after it is still found.
    refreshed: Option<Instant>,
}

/// Verifies RS256, ES256 and EdDSA tokens from one issuer: signature, `iss`,
/// `aud`, `exp` and `nbf` with 60 s leeway, and a string `sub` and tenant.
pub struct OidcVerifier {
    issuer: String,
    audience: String,
    tenant_claim: String,
    refresh_interval: Duration,
    max_age: Duration,
    source: Source,
    keys: RwLock<Keys>,
    fetches: Mutex<Fetches>,
}

impl OidcVerifier {
    /// The key for `kid`.
    ///
    /// A fresh key set that has the kid answers at once, without waiting on
    /// a fetch. Otherwise one fetch runs at a time: the first load; a
    /// refetch of a set older than `max_age`; or, for a kid the set lacks,
    /// a refetch at most once per `refresh_interval` (a rotated key). After
    /// a failed fetch the next waits `refresh_interval`, and a failed
    /// refetch keeps using the set already loaded.
    fn key(&self, kid: &str) -> Result<Jwk, AuthError> {
        let Source::Url { url, agent } = &self.source else {
            let keys = self.keys.read().unwrap_or_else(PoisonError::into_inner);
            return find(&keys.keys, kid).ok_or(AuthError::Unauthorized);
        };
        let fresh = |at: Option<Instant>| at.is_some_and(|at| at.elapsed() < self.max_age);
        let recent =
            |at: Option<Instant>| at.is_some_and(|at| at.elapsed() < self.refresh_interval);
        let lookup = || {
            let keys = self.keys.read().unwrap_or_else(PoisonError::into_inner);
            (keys.loaded_at, find(&keys.keys, kid))
        };
        let stale_key = match lookup() {
            (loaded_at, Some(key)) if fresh(loaded_at) => return Ok(key),
            (_, found) => found,
        };
        // A stale set that still has the kid serves it while another request
        // refetches, rather than waiting for that fetch.
        let mut fetches = match (stale_key, self.fetches.try_lock()) {
            (_, Ok(fetches)) => fetches,
            (_, Err(std::sync::TryLockError::Poisoned(poisoned))) => poisoned.into_inner(),
            (Some(key), Err(std::sync::TryLockError::WouldBlock)) => return Ok(key),
            (None, Err(std::sync::TryLockError::WouldBlock)) => {
                self.fetches.lock().unwrap_or_else(PoisonError::into_inner)
            }
        };
        // Another request may have fetched while this one waited.
        let (loaded_at, found) = lookup();
        if let (true, Some(key)) = (fresh(loaded_at), &found) {
            return Ok(key.clone());
        }
        let failing = fetches.error.is_some() && recent(fetches.attempted);
        // The first load waits only after a failure. A stale set waits a
        // refresh interval after any fetch; an unknown kid waits one after
        // the last unknown-kid refetch, and after a failure.
        let due = match (loaded_at, &found) {
            (None, _) => !failing,
            (Some(_), Some(_)) => !recent(fetches.attempted),
            (Some(_), None) => !failing && !recent(fetches.refreshed),
        };
        if !due {
            return match (found, failing) {
                (Some(key), _) => Ok(key),
                (None, true) => Err(AuthError::Unavailable(
                    fetches.error.clone().unwrap_or_default(),
                )),
                (None, false) => Err(AuthError::Unauthorized),
            };
        }
        fetches.attempted = Some(Instant::now());
        if loaded_at.is_some() && found.is_none() {
            fetches.refreshed = Some(Instant::now());
        }
        let fetched = fetch(agent, url);
        // The interval runs from when the fetch ended, so a fetch slower than
        // the interval is not followed at once by another.
        fetches.attempted = Some(Instant::now());
        if loaded_at.is_some() && found.is_none() {
            fetches.refreshed = fetches.attempted;
        }
        match fetched {
            Ok(set) => {
                fetches.error = None;
                let mut keys = self.keys.write().unwrap_or_else(PoisonError::into_inner);
                keys.keys = set;
                keys.loaded_at = Some(Instant::now());
                find(&keys.keys, kid).ok_or(AuthError::Unauthorized)
            }
            Err(error) => {
                fetches.error = Some(error.clone());
                found.ok_or(AuthError::Unavailable(error))
            }
        }
    }
}

fn find(keys: &[Jwk], kid: &str) -> Option<Jwk> {
    keys.iter()
        .find(|key| key.common.key_id.as_deref() == Some(kid))
        .cloned()
}

/// A key may verify signatures: its `use`, if any, is `sig`, and its
/// `key_ops`, if any, include `verify`.
fn usable(key: &Jwk) -> bool {
    let for_signing = match &key.common.public_key_use {
        None | Some(PublicKeyUse::Signature) => true,
        Some(_) => false,
    };
    let verifies = match &key.common.key_operations {
        None => true,
        Some(ops) => ops.iter().any(|op| matches!(op, KeyOperations::Verify)),
    };
    for_signing && verifies
}

/// The usable keys of a JWKS. A key this library cannot read (another curve
/// or algorithm) is skipped rather than failing the whole set.
fn parse_keys(body: &[u8]) -> Result<Vec<Jwk>, String> {
    #[derive(serde::Deserialize)]
    struct RawSet {
        keys: Vec<serde_json::Value>,
    }
    let raw: RawSet =
        serde_json::from_slice(body).map_err(|error| format!("jwks decode: {error}"))?;
    Ok(raw
        .keys
        .into_iter()
        .filter_map(|key| serde_json::from_value::<Jwk>(key).ok())
        .filter(usable)
        .collect())
}

fn fetch(agent: &ureq::Agent, url: &str) -> Result<Vec<Jwk>, String> {
    let response = agent
        .get(url)
        .call()
        .map_err(|error| format!("jwks fetch: {error}"))?;
    // Only a 200 is a key set; a redirect, which is not followed, is not.
    if response.status() != 200 {
        return Err(format!("jwks fetch: status {}", response.status()));
    }
    let mut body = Vec::new();
    response
        .into_reader()
        .take(MAX_JWKS_BYTES)
        .read_to_end(&mut body)
        .map_err(|error| format!("jwks read: {error}"))?;
    parse_keys(&body)
}

/// Whether `key` may verify `alg`: its type and curve must match, and its
/// own `alg`, when present, must be `alg`.
fn fits(key: &Jwk, alg: Algorithm) -> bool {
    let declared = match key.common.key_algorithm {
        None => true,
        Some(KeyAlgorithm::RS256) => alg == Algorithm::RS256,
        Some(KeyAlgorithm::ES256) => alg == Algorithm::ES256,
        Some(KeyAlgorithm::EdDSA) => alg == Algorithm::EdDSA,
        Some(_) => false,
    };
    let shape = match (&key.algorithm, alg) {
        (AlgorithmParameters::RSA(_), Algorithm::RS256) => true,
        (AlgorithmParameters::EllipticCurve(params), Algorithm::ES256) => {
            params.curve == EllipticCurve::P256
        }
        (AlgorithmParameters::OctetKeyPair(params), Algorithm::EdDSA) => {
            params.curve == EllipticCurve::Ed25519
        }
        _ => false,
    };
    declared && shape
}

fn claim(claims: &serde_json::Map<String, serde_json::Value>, name: &str) -> Option<String> {
    claims
        .get(name)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
}

impl Authenticator for OidcVerifier {
    fn authenticate(&self, token: &str) -> Result<Principal, AuthError> {
        let header = decode_header(token).map_err(|_| AuthError::Unauthorized)?;
        if !matches!(
            header.alg,
            Algorithm::RS256 | Algorithm::ES256 | Algorithm::EdDSA
        ) {
            return Err(AuthError::Unauthorized);
        }
        let kid = header.kid.ok_or(AuthError::Unauthorized)?;
        let key = self.key(&kid)?;
        if !fits(&key, header.alg) {
            return Err(AuthError::Unauthorized);
        }
        let key = DecodingKey::from_jwk(&key).map_err(|_| AuthError::Unauthorized)?;
        let mut validation = Validation::new(header.alg);
        validation.leeway = LEEWAY_SECS;
        validation.validate_nbf = true;
        validation.set_issuer(&[&self.issuer]);
        validation.set_audience(&[&self.audience]);
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        let data = decode::<serde_json::Map<String, serde_json::Value>>(token, &key, &validation)
            .map_err(|_| AuthError::Unauthorized)?;
        Ok(Principal {
            issuer: self.issuer.clone(),
            subject: claim(&data.claims, "sub").ok_or(AuthError::Unauthorized)?,
            tenant: claim(&data.claims, &self.tenant_claim).ok_or(AuthError::Unauthorized)?,
        })
    }
}

/// Local development only: accepts exactly the desktop's static token as one
/// fixed principal, and warns on every request. D6 replaces it with a real
/// token flow.
pub struct LocalDevAuthenticator;

impl Authenticator for LocalDevAuthenticator {
    fn authenticate(&self, token: &str) -> Result<Principal, AuthError> {
        if token != LOCAL_DEV_TOKEN {
            return Err(AuthError::Unauthorized);
        }
        eprintln!("gol: WARNING: GOL_AUTH=local-dev accepted the static token; never use it in production");
        Ok(Principal {
            issuer: "local-dev".to_string(),
            subject: "local-dev".to_string(),
            tenant: "local-dev".to_string(),
        })
    }
}

const OIDC_VARS: [&str; 4] = [
    "GOL_OIDC_ISSUER",
    "GOL_OIDC_AUDIENCE",
    "GOL_OIDC_JWKS_URL",
    "GOL_OIDC_TENANT_CLAIM",
];

/// The authenticator the environment selects. The server starts only with
/// every `GOL_OIDC_*` variable set, or with `GOL_AUTH=local-dev` and none of
/// them; anything else is an error naming what is missing.
pub fn auth_from_env(vars: &BTreeMap<String, String>) -> Result<Arc<dyn Authenticator>, String> {
    let get = |name: &str| vars.get(name).filter(|value| !value.is_empty());
    let set: Vec<&str> = OIDC_VARS
        .iter()
        .copied()
        .filter(|name| get(name).is_some())
        .collect();
    match get("GOL_AUTH").map(String::as_str) {
        Some("local-dev") if set.is_empty() => Ok(Arc::new(LocalDevAuthenticator)),
        Some("local-dev") => Err(format!(
            "GOL_AUTH=local-dev cannot be combined with {}",
            set.join(", ")
        )),
        Some(other) => Err(format!("GOL_AUTH must be local-dev or unset, not {other}")),
        None => {
            let missing: Vec<&str> = OIDC_VARS
                .iter()
                .copied()
                .filter(|name| get(name).is_none())
                .collect();
            if !missing.is_empty() {
                return Err(format!(
                    "set {} (or GOL_AUTH=local-dev for local development)",
                    missing.join(", ")
                ));
            }
            let value = |name: &str| get(name).cloned().unwrap_or_default();
            check_jwks_url(&value("GOL_OIDC_JWKS_URL"))?;
            Ok(Arc::new(
                OidcConfig::builder()
                    .issuer(value("GOL_OIDC_ISSUER"))
                    .audience(value("GOL_OIDC_AUDIENCE"))
                    .tenant_claim(value("GOL_OIDC_TENANT_CLAIM"))
                    .jwks_url(value("GOL_OIDC_JWKS_URL"))
                    .build(),
            ))
        }
    }
}

/// A JWKS URL must be https with a host, or http on 127.0.0.1, localhost or
/// [::1], so the key set cannot be swapped in transit.
fn check_jwks_url(url: &str) -> Result<(), String> {
    let refused = || {
        format!(
            "GOL_OIDC_JWKS_URL must be an https URL, or http on 127.0.0.1, localhost or [::1], not {url:?}"
        )
    };
    let parsed = url::Url::parse(url).map_err(|_| refused())?;
    let loopback = matches!(
        parsed.host(),
        Some(url::Host::Domain("localhost"))
            | Some(url::Host::Ipv4(std::net::Ipv4Addr::LOCALHOST))
            | Some(url::Host::Ipv6(std::net::Ipv6Addr::LOCALHOST))
    );
    let credentials = !parsed.username().is_empty() || parsed.password().is_some();
    match parsed.scheme() {
        "https" if parsed.host().is_some() && !credentials => Ok(()),
        "http" if loopback && !credentials => Ok(()),
        _ => Err(refused()),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    // The defaults the README documents.
    #[test]
    fn the_documented_defaults() {
        assert_eq!(super::LEEWAY_SECS, 60);
        assert_eq!(super::JWKS_TIMEOUT, Duration::from_secs(3));
        assert_eq!(super::REFRESH_INTERVAL, Duration::from_secs(30));
        assert_eq!(super::MAX_AGE, Duration::from_secs(600));
        assert_eq!(super::MAX_JWKS_BYTES, 1024 * 1024);
    }
}
