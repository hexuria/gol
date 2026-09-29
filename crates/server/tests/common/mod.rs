//! Signs test tokens for the OIDC authenticator. Keys are made per process
//! (Ed25519, ES256) or read from the committed test-only RSA fixture.
#![allow(dead_code)]

pub mod queued;
pub mod redis_proxy;

use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use ring::rand::SystemRandom;
use ring::signature::{EcdsaKeyPair, Ed25519KeyPair, KeyPair, ECDSA_P256_SHA256_FIXED_SIGNING};
use server::{Authenticator, OidcConfig};

pub const ISSUER: &str = "https://issuer.test";
pub const AUDIENCE: &str = "gol-test";
pub const TENANT_CLAIM: &str = "tenant";

/// A signing key and the public JWK that verifies it.
pub struct TestKey {
    pub alg: Algorithm,
    pub kid: &'static str,
    pub encoding: EncodingKey,
    pub jwk: serde_json::Value,
}

pub fn eddsa() -> &'static TestKey {
    static KEY: OnceLock<TestKey> = OnceLock::new();
    KEY.get_or_init(|| {
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
        let pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap();
        TestKey {
            alg: Algorithm::EdDSA,
            kid: "test-eddsa",
            encoding: EncodingKey::from_ed_der(pkcs8.as_ref()),
            jwk: serde_json::json!({
                "kty": "OKP", "crv": "Ed25519", "kid": "test-eddsa", "alg": "EdDSA", "use": "sig",
                "x": URL_SAFE_NO_PAD.encode(pair.public_key().as_ref()),
            }),
        }
    })
}

pub fn es256() -> &'static TestKey {
    static KEY: OnceLock<TestKey> = OnceLock::new();
    KEY.get_or_init(|| {
        let rng = SystemRandom::new();
        let pkcs8 = EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &rng).unwrap();
        let pair = EcdsaKeyPair::from_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, pkcs8.as_ref(), &rng)
            .unwrap();
        // An uncompressed P-256 point: 0x04, then x and y, 32 bytes each.
        let point = pair.public_key().as_ref();
        TestKey {
            alg: Algorithm::ES256,
            kid: "test-es256",
            encoding: EncodingKey::from_ec_der(pkcs8.as_ref()),
            jwk: serde_json::json!({
                "kty": "EC", "crv": "P-256", "kid": "test-es256", "alg": "ES256", "use": "sig",
                "x": URL_SAFE_NO_PAD.encode(&point[1..33]),
                "y": URL_SAFE_NO_PAD.encode(&point[33..65]),
            }),
        }
    })
}

/// The committed test-only RSA key. It signs nothing outside these tests.
pub fn rs256() -> &'static TestKey {
    static KEY: OnceLock<TestKey> = OnceLock::new();
    KEY.get_or_init(|| TestKey {
        alg: Algorithm::RS256,
        kid: "test-rs256",
        encoding: EncodingKey::from_rsa_pem(include_bytes!("../fixtures/test-only-rs256.pem"))
            .unwrap(),
        jwk: serde_json::json!({
            "kty": "RSA", "kid": "test-rs256", "alg": "RS256", "use": "sig", "e": "AQAB",
            "n": include_str!("../fixtures/test-only-rs256.n").trim(),
        }),
    })
}

pub fn jwks(keys: &[&TestKey]) -> serde_json::Value {
    serde_json::json!({ "keys": keys.iter().map(|key| key.jwk.clone()).collect::<Vec<_>>() })
}

pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Claims valid for an hour, for `subject` in `tenant`.
pub fn claims(subject: &str, tenant: &str) -> serde_json::Value {
    serde_json::json!({
        "iss": ISSUER, "aud": AUDIENCE, "sub": subject, TENANT_CLAIM: tenant,
        "iat": now(), "nbf": now() - 1, "exp": now() + 3600,
    })
}

pub fn sign(key: &TestKey, claims: &serde_json::Value) -> String {
    let mut header = Header::new(key.alg);
    header.kid = Some(key.kid.to_string());
    jsonwebtoken::encode(&header, claims, &key.encoding).unwrap()
}

/// An authenticator over a static key set with all three test keys.
pub fn authenticator() -> Arc<dyn Authenticator> {
    let keys = serde_json::from_value(jwks(&[eddsa(), es256(), rs256()])).unwrap();
    Arc::new(
        OidcConfig::builder()
            .issuer(ISSUER)
            .audience(AUDIENCE)
            .tenant_claim(TENANT_CLAIM)
            .jwks_static(keys)
            .build(),
    )
}

/// A valid bearer header value for the default test user.
pub fn bearer() -> String {
    format!("Bearer {}", sign(eddsa(), &claims("user-1", "tenant-1")))
}

/// A valid bearer header value for `subject` in the default tenant.
pub fn bearer_for(subject: &str) -> String {
    format!("Bearer {}", sign(eddsa(), &claims(subject, "tenant-1")))
}
