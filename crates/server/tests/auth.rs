mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use common::{claims, eddsa, es256, jwks, rs256, sign, AUDIENCE, ISSUER, TENANT_CLAIM};
use protocol::RunId;
use server::{auth_from_env, router, Authenticator, InMemoryStore, OidcConfig};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Serves the router with `auth` and returns its base URL.
async fn serve(auth: Arc<dyn Authenticator>) -> String {
    let app = router(
        Arc::new(InMemoryStore::default()),
        "http://127.0.0.1:9",
        auth,
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("http://{addr}")
}

/// The status of reading a run that does not exist: 404 once authenticated.
async fn status(base: &str, token: &str) -> u16 {
    reqwest::Client::new()
        .get(format!("{base}/v1/runs/{}", RunId::new()))
        .header("authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap()
        .status()
        .as_u16()
}

fn with(mut claims: serde_json::Value, key: &str, value: serde_json::Value) -> serde_json::Value {
    claims[key] = value;
    claims
}

fn b64(value: &serde_json::Value) -> String {
    URL_SAFE_NO_PAD.encode(serde_json::to_vec(value).unwrap())
}

#[tokio::test]
async fn rs256_es256_and_eddsa_are_accepted() {
    let base = serve(common::authenticator()).await;
    for key in [rs256(), es256(), eddsa()] {
        assert_eq!(
            status(&base, &sign(key, &claims("u", "t"))).await,
            404,
            "{:?}",
            key.alg
        );
    }
}

#[tokio::test]
async fn garbage_token_401() {
    let base = serve(common::authenticator()).await;
    for token in ["garbage", "a.b.c", "", "Bearer"] {
        assert_eq!(status(&base, token).await, 401, "{token}");
    }
}

#[tokio::test]
async fn expired_401() {
    let base = serve(common::authenticator()).await;
    let now = common::now();
    let expired = with(claims("u", "t"), "exp", (now - 120).into());
    assert_eq!(status(&base, &sign(eddsa(), &expired)).await, 401);
    // Inside the 60 s leeway it is still accepted.
    let recent = with(claims("u", "t"), "exp", (now - 30).into());
    assert_eq!(status(&base, &sign(eddsa(), &recent)).await, 404);
}

#[tokio::test]
async fn not_yet_valid_401() {
    let base = serve(common::authenticator()).await;
    let future = with(claims("u", "t"), "nbf", (common::now() + 600).into());
    assert_eq!(status(&base, &sign(eddsa(), &future)).await, 401);
}

#[tokio::test]
async fn wrong_aud_401() {
    let base = serve(common::authenticator()).await;
    let token = sign(
        eddsa(),
        &with(claims("u", "t"), "aud", "someone-else".into()),
    );
    assert_eq!(status(&base, &token).await, 401);
}

#[tokio::test]
async fn wrong_iss_401() {
    let base = serve(common::authenticator()).await;
    let token = sign(
        eddsa(),
        &with(claims("u", "t"), "iss", "https://evil.test".into()),
    );
    assert_eq!(status(&base, &token).await, 401);
}

#[tokio::test]
async fn missing_subject_or_tenant_401() {
    let base = serve(common::authenticator()).await;
    for claim in ["sub", TENANT_CLAIM] {
        let mut body = claims("u", "t");
        body.as_object_mut().unwrap().remove(claim);
        assert_eq!(status(&base, &sign(eddsa(), &body)).await, 401, "{claim}");
    }
    let numeric = with(claims("u", "t"), TENANT_CLAIM, 7.into());
    assert_eq!(status(&base, &sign(eddsa(), &numeric)).await, 401);
}

#[tokio::test]
async fn alg_none_401() {
    let base = serve(common::authenticator()).await;
    let header = serde_json::json!({"alg": "none", "typ": "JWT", "kid": eddsa().kid});
    let token = format!("{}.{}.", b64(&header), b64(&claims("u", "t")));
    assert_eq!(status(&base, &token).await, 401);
}

// The classic confusion: HS256 keyed with the RSA public key the verifier
// would use for RS256.
#[tokio::test]
async fn hs256_confusion_401() {
    let base = serve(common::authenticator()).await;
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
    header.kid = Some(rs256().kid.to_string());
    for secret in [
        include_bytes!("fixtures/test-only-rs256.n").as_slice(),
        rs256().jwk["n"].as_str().unwrap().as_bytes(),
    ] {
        let key = jsonwebtoken::EncodingKey::from_secret(secret);
        let token = jsonwebtoken::encode(&header, &claims("u", "t"), &key).unwrap();
        assert_eq!(status(&base, &token).await, 401);
    }
}

// A key used with another key's algorithm is refused.
#[tokio::test]
async fn a_key_is_used_only_with_its_own_algorithm() {
    let base = serve(common::authenticator()).await;
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::EdDSA);
    header.kid = Some(es256().kid.to_string());
    let token = jsonwebtoken::encode(&header, &claims("u", "t"), &eddsa().encoding).unwrap();
    assert_eq!(status(&base, &token).await, 401);
}

async fn jwks_server(keys: serde_json::Value) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/jwks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(keys))
        .mount(&server)
        .await;
    server
}

fn remote(url: String) -> Arc<dyn Authenticator> {
    Arc::new(
        OidcConfig::builder()
            .issuer(ISSUER)
            .audience(AUDIENCE)
            .tenant_claim(TENANT_CLAIM)
            .jwks_url(url)
            .build(),
    )
}

async fn fetches(server: &MockServer) -> usize {
    server.received_requests().await.unwrap().len()
}

// The key set is fetched once and reused. An unknown kid refetches it once;
// a second unknown kid inside 30 s does not fetch again.
#[tokio::test]
async fn unknown_kid_refreshes_once() {
    let issuer = jwks_server(jwks(&[eddsa()])).await;
    let base = serve(remote(format!("{}/jwks", issuer.uri()))).await;
    let good = sign(eddsa(), &claims("u", "t"));
    assert_eq!(status(&base, &good).await, 404);
    assert_eq!(status(&base, &good).await, 404);
    assert_eq!(fetches(&issuer).await, 1);

    let unknown = sign(es256(), &claims("u", "t"));
    assert_eq!(status(&base, &unknown).await, 401);
    assert_eq!(fetches(&issuer).await, 2);
    assert_eq!(status(&base, &unknown).await, 401);
    assert_eq!(fetches(&issuer).await, 2);
    assert_eq!(status(&base, &good).await, 404);
    assert_eq!(fetches(&issuer).await, 2);
}

// An issuer that does not answer is a 503 within the 3 s timeout, not a hang.
#[tokio::test]
async fn jwks_timeout_503_not_hang() {
    let issuer = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/jwks"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(jwks(&[eddsa()]))
                .set_delay(Duration::from_secs(20)),
        )
        .mount(&issuer)
        .await;
    let base = serve(remote(format!("{}/jwks", issuer.uri()))).await;
    let started = Instant::now();
    assert_eq!(status(&base, &sign(eddsa(), &claims("u", "t"))).await, 503);
    assert!(
        started.elapsed() < Duration::from_secs(6),
        "{:?}",
        started.elapsed()
    );
}

#[tokio::test]
async fn a_jwks_error_is_503() {
    let issuer = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/jwks"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&issuer)
        .await;
    let base = serve(remote(format!("{}/jwks", issuer.uri()))).await;
    assert_eq!(status(&base, &sign(eddsa(), &claims("u", "t"))).await, 503);
}

fn env(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

const OIDC: [(&str, &str); 4] = [
    ("GOL_OIDC_ISSUER", ISSUER),
    ("GOL_OIDC_AUDIENCE", AUDIENCE),
    ("GOL_OIDC_JWKS_URL", "http://127.0.0.1:9/jwks"),
    ("GOL_OIDC_TENANT_CLAIM", TENANT_CLAIM),
];

// The server starts only with a complete GOL_OIDC_* set, or with the explicit
// local-dev mode and no GOL_OIDC_* at all.
#[test]
fn auth_from_env_refuses_to_start_without_oidc() {
    let error = auth_from_env(&env(&[])).err().unwrap();
    assert!(error.contains("GOL_OIDC_ISSUER"), "{error}");
    for (missing, _) in OIDC {
        let partial: Vec<_> = OIDC
            .iter()
            .copied()
            .filter(|(name, _)| *name != missing)
            .collect();
        let error = auth_from_env(&env(&partial)).err().unwrap();
        assert!(error.contains(missing), "{error}");
    }
    assert!(auth_from_env(&env(&OIDC)).is_ok());
    assert!(auth_from_env(&env(&[("GOL_AUTH", "local-dev")])).is_ok());
    let mut both = OIDC.to_vec();
    both.push(("GOL_AUTH", "local-dev"));
    let error = auth_from_env(&env(&both)).err().unwrap();
    assert!(error.contains("GOL_AUTH=local-dev"), "{error}");
    let error = auth_from_env(&env(&[("GOL_AUTH", "open")])).err().unwrap();
    assert!(error.contains("GOL_AUTH"), "{error}");
}

// Local-dev accepts exactly the desktop's static token, and nothing else.
#[tokio::test]
async fn local_dev_accepts_only_the_static_token() {
    let auth = auth_from_env(&env(&[("GOL_AUTH", "local-dev")])).unwrap();
    let base = serve(auth).await;
    assert_eq!(status(&base, "gol-gateway-local").await, 404);
    for token in [
        "gol-gateway-local2",
        "other",
        &sign(eddsa(), &claims("u", "t")),
    ] {
        assert_eq!(status(&base, token).await, 401, "{token}");
    }
}

// A key the issuer adds later (a rotation) is picked up by the refresh an
// unknown kid triggers.
#[tokio::test]
async fn a_rotated_key_is_picked_up() {
    let issuer = jwks_server(jwks(&[eddsa()])).await;
    let base = serve(remote(format!("{}/jwks", issuer.uri()))).await;
    assert_eq!(status(&base, &sign(eddsa(), &claims("u", "t"))).await, 404);
    issuer.reset().await;
    Mock::given(method("GET"))
        .and(path("/jwks"))
        .respond_with(ResponseTemplate::new(200).set_body_json(jwks(&[eddsa(), es256()])))
        .mount(&issuer)
        .await;
    assert_eq!(status(&base, &sign(es256(), &claims("u", "t"))).await, 404);
    assert_eq!(fetches(&issuer).await, 1);
}

// After a failed fetch, requests inside the refresh interval answer 503
// without fetching again, so a down issuer is not hammered.
#[tokio::test]
async fn a_failed_fetch_is_not_retried_at_once() {
    let issuer = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/jwks"))
        .respond_with(ResponseTemplate::new(500))
        .mount(&issuer)
        .await;
    let base = serve(remote(format!("{}/jwks", issuer.uri()))).await;
    for _ in 0..3 {
        assert_eq!(status(&base, &sign(eddsa(), &claims("u", "t"))).await, 503);
    }
    assert_eq!(fetches(&issuer).await, 1);
}

// An algorithm outside RS256, ES256 and EdDSA is refused before any key is
// looked up, so it cannot make the server fetch the issuer's keys.
#[tokio::test]
async fn a_disallowed_algorithm_never_fetches_keys() {
    let issuer = jwks_server(jwks(&[eddsa()])).await;
    let base = serve(remote(format!("{}/jwks", issuer.uri()))).await;
    for alg in [
        jsonwebtoken::Algorithm::RS384,
        jsonwebtoken::Algorithm::PS256,
    ] {
        let mut header = jsonwebtoken::Header::new(alg);
        header.kid = Some("unknown".to_string());
        let token = jsonwebtoken::encode(&header, &claims("u", "t"), &rs256().encoding).unwrap();
        assert_eq!(status(&base, &token).await, 401, "{alg:?}");
    }
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
    header.kid = Some("unknown".to_string());
    let key = jsonwebtoken::EncodingKey::from_secret(b"secret");
    let token = jsonwebtoken::encode(&header, &claims("u", "t"), &key).unwrap();
    assert_eq!(status(&base, &token).await, 401);
    assert_eq!(fetches(&issuer).await, 0);
}

// A key that declares another algorithm is not used for RS256, even though
// its type would verify it.
#[tokio::test]
async fn a_key_declared_for_another_algorithm_is_refused() {
    let mut declared = rs256().jwk.clone();
    declared["alg"] = "PS256".into();
    declared["kid"] = "rsa-ps256".into();
    let keys = serde_json::from_value(serde_json::json!({ "keys": [declared] })).unwrap();
    let auth: Arc<dyn Authenticator> = Arc::new(
        OidcConfig::builder()
            .issuer(ISSUER)
            .audience(AUDIENCE)
            .tenant_claim(TENANT_CLAIM)
            .jwks_static(keys)
            .build(),
    );
    let base = serve(auth).await;
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
    header.kid = Some("rsa-ps256".to_string());
    let token = jsonwebtoken::encode(&header, &claims("u", "t"), &rs256().encoding).unwrap();
    assert_eq!(status(&base, &token).await, 401);
}
