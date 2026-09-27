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
    // The leeway is 60 s: 55 s past exp is accepted, 65 s is not.
    let recent = with(claims("u", "t"), "exp", (now - 55).into());
    assert_eq!(status(&base, &sign(eddsa(), &recent)).await, 404);
    let past = with(claims("u", "t"), "exp", (now - 65).into());
    assert_eq!(status(&base, &sign(eddsa(), &past)).await, 401);
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
        // The fetch timeout is 3 s.
        started.elapsed() < Duration::from_millis(4500),
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

fn remote_with(url: String, refresh: Duration, max_age: Duration) -> Arc<dyn Authenticator> {
    Arc::new(
        OidcConfig::builder()
            .issuer(ISSUER)
            .audience(AUDIENCE)
            .tenant_claim(TENANT_CLAIM)
            .jwks_url(url)
            .refresh_interval(refresh)
            .max_age(max_age)
            .build(),
    )
}

fn static_keys(keys: serde_json::Value) -> Arc<dyn Authenticator> {
    Arc::new(
        OidcConfig::builder()
            .issuer(ISSUER)
            .audience(AUDIENCE)
            .tenant_claim(TENANT_CLAIM)
            .jwks_static(serde_json::from_value(keys).unwrap())
            .build(),
    )
}

async fn replace_jwks(server: &MockServer, response: ResponseTemplate) {
    server.reset().await;
    Mock::given(method("GET"))
        .and(path("/jwks"))
        .respond_with(response)
        .mount(server)
        .await;
}

async fn response(base: &str, token: &str) -> (u16, serde_json::Value) {
    let response = reqwest::Client::new()
        .get(format!("{base}/v1/runs/{}", RunId::new()))
        .header("authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    (response.status().as_u16(), response.json().await.unwrap())
}

#[tokio::test]
async fn required_claims_must_be_present_and_non_empty() {
    let base = serve(common::authenticator()).await;
    for claim in ["exp", "iss", "aud", "sub", TENANT_CLAIM] {
        let mut body = claims("u", "t");
        body.as_object_mut().unwrap().remove(claim);
        assert_eq!(status(&base, &sign(eddsa(), &body)).await, 401, "{claim}");
    }
    for (claim, value) in [("sub", ""), (TENANT_CLAIM, ""), (TENANT_CLAIM, "   ")] {
        let token = sign(eddsa(), &with(claims("u", "t"), claim, value.into()));
        assert_eq!(status(&base, &token).await, 401, "{claim}={value:?}");
    }
    // nbf is optional.
    let mut body = claims("u", "t");
    body.as_object_mut().unwrap().remove("nbf");
    assert_eq!(status(&base, &sign(eddsa(), &body)).await, 404);
}

#[tokio::test]
async fn errors_have_fixed_bodies() {
    let base = serve(common::authenticator()).await;
    assert_eq!(
        response(&base, "garbage").await,
        (401, serde_json::json!({"error": "unauthorized"}))
    );
    let issuer = MockServer::start().await;
    replace_jwks(
        &issuer,
        ResponseTemplate::new(500).set_body_string("secret detail"),
    )
    .await;
    let base = serve(remote(format!("{}/jwks", issuer.uri()))).await;
    assert_eq!(
        response(&base, &sign(eddsa(), &claims("u", "t"))).await,
        (
            503,
            serde_json::json!({"error": "identity provider unavailable"})
        )
    );
}

#[tokio::test]
async fn keys_without_alg_are_used_by_type() {
    let mut key = eddsa().jwk.clone();
    key.as_object_mut().unwrap().remove("alg");
    let base = serve(static_keys(serde_json::json!({ "keys": [key] }))).await;
    assert_eq!(status(&base, &sign(eddsa(), &claims("u", "t"))).await, 404);
}

#[tokio::test]
async fn unreadable_and_encryption_keys_are_skipped() {
    let mut enc = es256().jwk.clone();
    enc["use"] = "enc".into();
    let mut ops = rs256().jwk.clone();
    ops["key_ops"] = serde_json::json!(["encrypt"]);
    let unreadable = serde_json::json!({"kty": "OKP", "crv": "X25519", "kid": "x", "x": "AAAA"});
    // A signing key under the same kid as an encryption key listed first.
    let mut shadowed = es256().jwk.clone();
    shadowed["use"] = "enc".into();
    shadowed["kid"] = eddsa().kid.into();
    let issuer = jwks_server(serde_json::json!({
        "keys": [unreadable, enc.clone(), ops.clone(), shadowed.clone(), eddsa().jwk]
    }))
    .await;
    // A static set is typed, so it cannot hold the unreadable key.
    let readable = serde_json::json!({ "keys": [enc, ops, shadowed, eddsa().jwk] });
    for base in [
        serve(remote(format!("{}/jwks", issuer.uri()))).await,
        serve(static_keys(readable)).await,
    ] {
        assert_eq!(status(&base, &sign(eddsa(), &claims("u", "t"))).await, 404);
        assert_eq!(status(&base, &sign(es256(), &claims("u", "t"))).await, 401);
        assert_eq!(status(&base, &sign(rs256(), &claims("u", "t"))).await, 401);
    }
}

#[tokio::test]
async fn a_static_key_set_never_answers_503() {
    let base = serve(common::authenticator()).await;
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::EdDSA);
    header.kid = Some("unknown".to_string());
    let token = jsonwebtoken::encode(&header, &claims("u", "t"), &eddsa().encoding).unwrap();
    assert_eq!(status(&base, &token).await, 401);
}

// A JWKS up to 1 MiB is read; a larger one is refused.
#[tokio::test]
async fn the_jwks_is_read_up_to_one_mebibyte() {
    let padded = |bytes: usize| serde_json::json!({ "keys": [eddsa().jwk, {"kty": "none", "pad": "x".repeat(bytes)}] });
    let issuer = jwks_server(padded(300 * 1024)).await;
    let base = serve(remote(format!("{}/jwks", issuer.uri()))).await;
    assert_eq!(status(&base, &sign(eddsa(), &claims("u", "t"))).await, 404);
    let issuer = jwks_server(padded(1100 * 1024)).await;
    let base = serve(remote(format!("{}/jwks", issuer.uri()))).await;
    assert_eq!(status(&base, &sign(eddsa(), &claims("u", "t"))).await, 503);
}

// A key the issuer removed stops verifying once the set is older than its
// max age.
#[tokio::test]
async fn a_removed_key_stops_verifying_after_max_age() {
    let issuer = jwks_server(jwks(&[eddsa(), es256()])).await;
    let url = format!("{}/jwks", issuer.uri());
    let base = serve(remote_with(
        url,
        Duration::from_millis(100),
        Duration::from_millis(300),
    ))
    .await;
    assert_eq!(status(&base, &sign(es256(), &claims("u", "t"))).await, 404);
    replace_jwks(
        &issuer,
        ResponseTemplate::new(200).set_body_json(jwks(&[eddsa()])),
    )
    .await;
    assert_eq!(status(&base, &sign(es256(), &claims("u", "t"))).await, 404);
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(status(&base, &sign(es256(), &claims("u", "t"))).await, 401);
    assert_eq!(status(&base, &sign(eddsa(), &claims("u", "t"))).await, 404);
}

// A refetch that fails keeps the loaded keys: known kids keep working, and
// only a kid the set lacks gets 503.
#[tokio::test]
async fn a_failed_refetch_keeps_the_loaded_keys() {
    let issuer = jwks_server(jwks(&[eddsa()])).await;
    let url = format!("{}/jwks", issuer.uri());
    let base = serve(remote_with(
        url,
        Duration::from_millis(100),
        Duration::from_millis(300),
    ))
    .await;
    assert_eq!(status(&base, &sign(eddsa(), &claims("u", "t"))).await, 404);
    replace_jwks(&issuer, ResponseTemplate::new(500)).await;
    assert_eq!(status(&base, &sign(es256(), &claims("u", "t"))).await, 503);
    assert_eq!(status(&base, &sign(eddsa(), &claims("u", "t"))).await, 404);
    // Past max age the refetch fails too, and the loaded key still verifies.
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(status(&base, &sign(eddsa(), &claims("u", "t"))).await, 404);
    assert_eq!(fetches(&issuer).await, 2);
}

// The refresh interval bounds refetches, and a failure is retried once it
// has passed.
#[tokio::test]
async fn refetches_wait_for_the_refresh_interval() {
    let issuer = jwks_server(jwks(&[eddsa()])).await;
    let url = format!("{}/jwks", issuer.uri());
    let base = serve(remote_with(
        url,
        Duration::from_millis(300),
        Duration::from_secs(600),
    ))
    .await;
    let unknown = sign(es256(), &claims("u", "t"));
    // The first load, then one unknown-kid refetch, then none until the
    // interval passes.
    assert_eq!(status(&base, &unknown).await, 401);
    assert_eq!(status(&base, &unknown).await, 401);
    assert_eq!(status(&base, &unknown).await, 401);
    assert_eq!(fetches(&issuer).await, 2);
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(status(&base, &unknown).await, 401);
    assert_eq!(fetches(&issuer).await, 3);

    let down = MockServer::start().await;
    replace_jwks(&down, ResponseTemplate::new(500)).await;
    let url = format!("{}/jwks", down.uri());
    let base = serve(remote_with(
        url,
        Duration::from_millis(300),
        Duration::from_secs(600),
    ))
    .await;
    assert_eq!(status(&base, &sign(eddsa(), &claims("u", "t"))).await, 503);
    replace_jwks(
        &down,
        ResponseTemplate::new(200).set_body_json(jwks(&[eddsa()])),
    )
    .await;
    assert_eq!(status(&base, &sign(eddsa(), &claims("u", "t"))).await, 503);
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(status(&base, &sign(eddsa(), &claims("u", "t"))).await, 404);
    // The recovered fetch cleared the failure: an unknown kid is 401, not 503.
    assert_eq!(status(&base, &sign(es256(), &claims("u", "t"))).await, 401);
}

// Concurrent first requests share one fetch.
#[tokio::test]
async fn concurrent_first_loads_share_one_fetch() {
    let issuer = MockServer::start().await;
    replace_jwks(
        &issuer,
        ResponseTemplate::new(200)
            .set_body_json(jwks(&[eddsa()]))
            .set_delay(Duration::from_millis(300)),
    )
    .await;
    let base = serve(remote(format!("{}/jwks", issuer.uri()))).await;
    let token = sign(eddsa(), &claims("u", "t"));
    let requests: Vec<_> = (0..8)
        .map(|_| {
            let (base, token) = (base.clone(), token.clone());
            tokio::spawn(async move { status(&base, &token).await })
        })
        .collect();
    for request in requests {
        assert_eq!(request.await.unwrap(), 404);
    }
    assert_eq!(fetches(&issuer).await, 1);
}

// A known kid is answered while a slow refetch for an unknown kid runs.
#[tokio::test]
async fn a_known_kid_does_not_wait_for_a_refetch() {
    let issuer = jwks_server(jwks(&[eddsa()])).await;
    let base = serve(remote(format!("{}/jwks", issuer.uri()))).await;
    let known = sign(eddsa(), &claims("u", "t"));
    assert_eq!(status(&base, &known).await, 404);
    replace_jwks(
        &issuer,
        ResponseTemplate::new(200)
            .set_body_json(jwks(&[eddsa()]))
            .set_delay(Duration::from_secs(2)),
    )
    .await;
    let slow = {
        let (base, unknown) = (base.clone(), sign(es256(), &claims("u", "t")));
        tokio::spawn(async move { status(&base, &unknown).await })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    let started = Instant::now();
    assert_eq!(status(&base, &known).await, 404);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(slow.await.unwrap(), 401);
}

#[test]
fn the_jwks_url_must_be_https_or_loopback() {
    let with_url = |url: &str| {
        let mut vars: Vec<_> = OIDC
            .iter()
            .copied()
            .filter(|(k, _)| *k != "GOL_OIDC_JWKS_URL")
            .collect();
        vars.push(("GOL_OIDC_JWKS_URL", url));
        auth_from_env(&env(&vars))
    };
    for url in [
        "https://idp.example.com/jwks",
        "http://127.0.0.1:9/jwks",
        "http://localhost/jwks",
        "http://[::1]:8080/k",
    ] {
        assert!(with_url(url).is_ok(), "{url}");
    }
    for url in [
        "http://idp.example.com/jwks",
        "not a url",
        " ",
        "https://",
        "http://localhost.evil.test/jwks",
        "ftp://127.0.0.1/jwks",
        "http://127.0.0.2/jwks",
        "https://user:pw@idp.example.com/jwks",
        "http://user@localhost/jwks",
        "https://[/jwks",
    ] {
        let error = with_url(url).err().unwrap();
        assert!(error.contains("GOL_OIDC_JWKS_URL"), "{url}: {error}");
    }
}

#[test]
fn local_dev_is_refused_with_any_oidc_variable() {
    for (name, value) in OIDC {
        let error = auth_from_env(&env(&[("GOL_AUTH", "local-dev"), (name, value)]))
            .err()
            .unwrap();
        assert!(error.contains(name), "{error}");
    }
}

// A redirect from the JWKS URL is not followed.
#[tokio::test]
async fn a_jwks_redirect_is_not_followed() {
    let issuer = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/jwks"))
        // A redirect whose body is itself a valid key set is still refused.
        .respond_with(
            ResponseTemplate::new(302)
                .insert_header("location", "/moved")
                .set_body_json(jwks(&[eddsa()])),
        )
        .mount(&issuer)
        .await;
    Mock::given(method("GET"))
        .and(path("/moved"))
        .respond_with(ResponseTemplate::new(200).set_body_json(jwks(&[eddsa()])))
        .mount(&issuer)
        .await;
    let base = serve(remote(format!("{}/jwks", issuer.uri()))).await;
    assert_eq!(status(&base, &sign(eddsa(), &claims("u", "t"))).await, 503);
    assert_eq!(fetches(&issuer).await, 1);
}

proptest::proptest! {
    // The JWKS URL check never panics, and every URL it accepts parses as
    // https, or as http on a loopback host, with no credentials. The explicit
    // lists in the_jwks_url_must_be_https_or_loopback cover what it refuses.
    #[test]
    fn the_jwks_url_check_accepts_only_https_or_loopback(
        url in proptest::prop_oneof![
            proptest::arbitrary::any::<String>(),
            "(https?|HTTPS?|ftp)://[a-zA-Z0-9.:@%\\[\\]/ -]{0,24}",
            "http://(127\\.0\\.0\\.1|localhost|LOCALHOST|\\[::1\\]|127\\.0\\.0\\.2|127\\.1)(:[0-9]{0,6})?[/?#a-z]{0,8}",
        ]
    ) {
        let mut vars: Vec<(&str, &str)> =
            OIDC.iter().copied().filter(|(k, _)| *k != "GOL_OIDC_JWKS_URL").collect();
        vars.push(("GOL_OIDC_JWKS_URL", &url));
        if auth_from_env(&env(&vars)).is_ok() {
            let parsed = url::Url::parse(&url);
            proptest::prop_assert!(parsed.is_ok(), "{:?}", url);
            let parsed = parsed.unwrap();
            proptest::prop_assert!(parsed.username().is_empty() && parsed.password().is_none(), "{:?}", url);
            let loopback = match parsed.host() {
                Some(url::Host::Domain(name)) => name == "localhost",
                Some(url::Host::Ipv4(ip)) => ip.is_loopback() && ip == std::net::Ipv4Addr::LOCALHOST,
                Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
                None => false,
            };
            proptest::prop_assert!(
                (parsed.scheme() == "https" && parsed.host().is_some())
                    || (parsed.scheme() == "http" && loopback),
                "{:?}",
                url
            );
        }
    }
}

// Past max age, one request refetches; the others keep using the stale key
// set instead of waiting for that fetch.
#[tokio::test]
async fn a_stale_key_set_serves_while_one_request_refetches() {
    let issuer = jwks_server(jwks(&[eddsa()])).await;
    let url = format!("{}/jwks", issuer.uri());
    let base = serve(remote_with(
        url,
        Duration::from_millis(100),
        Duration::from_millis(300),
    ))
    .await;
    let known = sign(eddsa(), &claims("u", "t"));
    assert_eq!(status(&base, &known).await, 404);
    replace_jwks(
        &issuer,
        ResponseTemplate::new(200)
            .set_body_json(jwks(&[eddsa()]))
            .set_delay(Duration::from_secs(2)),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let refetching = {
        let (base, known) = (base.clone(), known.clone());
        tokio::spawn(async move { status(&base, &known).await })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    let started = Instant::now();
    assert_eq!(status(&base, &known).await, 404);
    assert!(
        started.elapsed() < Duration::from_secs(1),
        "{:?}",
        started.elapsed()
    );
    assert_eq!(refetching.await.unwrap(), 404);
}

// While the issuer fails, a stale set is refetched at most once per refresh
// interval, its keys keep verifying, and an unknown kid does not refetch.
#[tokio::test]
async fn a_failing_issuer_is_refetched_once_per_interval() {
    let issuer = jwks_server(jwks(&[eddsa()])).await;
    let url = format!("{}/jwks", issuer.uri());
    let base = serve(remote_with(
        url,
        Duration::from_millis(500),
        Duration::from_millis(100),
    ))
    .await;
    let known = sign(eddsa(), &claims("u", "t"));
    assert_eq!(status(&base, &known).await, 404);
    replace_jwks(&issuer, ResponseTemplate::new(500)).await;
    tokio::time::sleep(Duration::from_millis(600)).await;
    for _ in 0..4 {
        assert_eq!(status(&base, &known).await, 404);
    }
    assert_eq!(status(&base, &sign(es256(), &claims("u", "t"))).await, 503);
    assert_eq!(fetches(&issuer).await, 1);
}

#[test]
fn an_empty_oidc_variable_counts_as_unset() {
    let mut vars: Vec<_> = OIDC
        .iter()
        .copied()
        .filter(|(k, _)| *k != "GOL_OIDC_ISSUER")
        .collect();
    vars.push(("GOL_OIDC_ISSUER", ""));
    let error = auth_from_env(&env(&vars)).err().unwrap();
    assert!(error.contains("GOL_OIDC_ISSUER"), "{error}");
    assert!(auth_from_env(&env(&[("GOL_AUTH", "local-dev"), ("GOL_OIDC_ISSUER", "")])).is_ok());
}

// The refresh interval runs from when a fetch ends: with a fetch slower
// than the interval, a second unknown kid queued behind the first refetch
// does not refetch again at once.
#[tokio::test]
async fn the_refresh_interval_runs_from_the_end_of_a_fetch() {
    let issuer = jwks_server(jwks(&[eddsa()])).await;
    let url = format!("{}/jwks", issuer.uri());
    let base = serve(remote_with(
        url,
        Duration::from_millis(300),
        Duration::from_secs(600),
    ))
    .await;
    assert_eq!(status(&base, &sign(eddsa(), &claims("u", "t"))).await, 404);
    replace_jwks(
        &issuer,
        ResponseTemplate::new(200)
            .set_body_json(jwks(&[eddsa()]))
            .set_delay(Duration::from_millis(800)),
    )
    .await;
    let unknown = sign(es256(), &claims("u", "t"));
    let first = {
        let (base, unknown) = (base.clone(), unknown.clone());
        tokio::spawn(async move { status(&base, &unknown).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(status(&base, &unknown).await, 401);
    assert_eq!(first.await.unwrap(), 401);
    assert_eq!(fetches(&issuer).await, 1);
}
