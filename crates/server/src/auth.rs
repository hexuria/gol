//! Who is calling: a bearer token checked against an OIDC issuer's keys, or,
//! for local development only, the desktop's static token.

use std::collections::BTreeMap;
use std::io::Read;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use jsonwebtoken::jwk::{AlgorithmParameters, EllipticCurve, Jwk, JwkSet, KeyAlgorithm};
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
/// An unknown kid refetches the key set at most once per interval.
const REFRESH_INTERVAL: Duration = Duration::from_secs(30);
const MAX_JWKS_BYTES: u64 = 1024 * 1024;
/// The static token the coworker desktop sends in local development.
pub const LOCAL_DEV_TOKEN: &str = "gol-gateway-local";

pub struct Missing;
pub struct Set;

enum KeySource {
    Url(String),
    Static(JwkSet),
}

#[derive(Default)]
struct Draft {
    issuer: Option<String>,
    audience: Option<String>,
    tenant_claim: Option<String>,
    keys: Option<KeySource>,
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
            draft: Draft::default(),
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
    /// Read the key set from the issuer's JWKS URL.
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
                    agent: ureq::AgentBuilder::new().timeout(JWKS_TIMEOUT).build(),
                },
                Vec::new(),
            ),
            KeySource::Static(set) => (Source::Static, set.keys),
        };
        let loaded = matches!(source, Source::Static);
        OidcVerifier {
            issuer: draft.issuer.expect("typestate recorded the issuer"),
            audience: draft.audience.expect("typestate recorded the audience"),
            tenant_claim: draft.tenant_claim.expect("typestate recorded the claim"),
            source,
            cache: Mutex::new(Cache {
                loaded,
                keys,
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

struct Cache {
    keys: Vec<Jwk>,
    /// Whether a fetch has succeeded; a static key set starts loaded.
    loaded: bool,
    /// When the last fetch was made, and why it failed, if it did.
    attempted: Option<Instant>,
    error: Option<String>,
    /// When an unknown kid last refetched a loaded key set.
    refreshed: Option<Instant>,
}

/// Verifies RS256, ES256 and EdDSA tokens from one issuer: signature, `iss`,
/// `aud`, `exp` and `nbf` with 60 s leeway, and a string `sub` and tenant.
pub struct OidcVerifier {
    issuer: String,
    audience: String,
    tenant_claim: String,
    source: Source,
    cache: Mutex<Cache>,
}

impl OidcVerifier {
    /// The key for `kid`. The key set is loaded on first use; after a failed
    /// fetch the next one waits a refresh interval, so a down issuer is not
    /// hammered. Once loaded, an unknown kid refetches the set at most once
    /// per interval (for a rotated key). The lock is held across the fetch,
    /// so concurrent requests wait for one fetch instead of each starting
    /// their own.
    fn key(&self, kid: &str) -> Result<Jwk, AuthError> {
        let mut cache = self
            .cache
            .lock()
            .map_err(|_| AuthError::Unavailable("key cache poisoned".to_string()))?;
        if let Some(key) = find(&cache.keys, kid) {
            return Ok(key);
        }
        let Source::Url { url, agent } = &self.source else {
            return Err(AuthError::Unauthorized);
        };
        let recent = |at: Option<Instant>| at.is_some_and(|at| at.elapsed() < REFRESH_INTERVAL);
        if let (Some(error), true) = (&cache.error, recent(cache.attempted)) {
            return Err(AuthError::Unavailable(error.clone()));
        }
        if cache.loaded {
            if recent(cache.refreshed) {
                return Err(AuthError::Unauthorized);
            }
            cache.refreshed = Some(Instant::now());
        }
        cache.attempted = Some(Instant::now());
        match fetch(agent, url) {
            Ok(set) => {
                cache.keys = set.keys;
                cache.loaded = true;
                cache.error = None;
                find(&cache.keys, kid).ok_or(AuthError::Unauthorized)
            }
            Err(error) => {
                cache.error = Some(error.clone());
                Err(AuthError::Unavailable(error))
            }
        }
    }
}

fn find(keys: &[Jwk], kid: &str) -> Option<Jwk> {
    keys.iter()
        .find(|key| key.common.key_id.as_deref() == Some(kid))
        .cloned()
}

fn fetch(agent: &ureq::Agent, url: &str) -> Result<JwkSet, String> {
    let response = agent
        .get(url)
        .call()
        .map_err(|error| format!("jwks fetch: {error}"))?;
    let mut body = Vec::new();
    response
        .into_reader()
        .take(MAX_JWKS_BYTES)
        .read_to_end(&mut body)
        .map_err(|error| format!("jwks read: {error}"))?;
    serde_json::from_slice(&body).map_err(|error| format!("jwks decode: {error}"))
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
        .filter(|value| !value.is_empty())
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
