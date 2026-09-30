//! T9: any headers and body a webhook sender posts are parsed or refused
//! without panicking; a request signed with the secret verifies exactly when
//! its event id and timestamp are of their forms (79A), and no longer once
//! its body changes.
#![no_main]

use libfuzzer_sys::fuzz_target;
use server::webhook::{is_event_id, parse_signature, parse_timestamp, plausible, verify, Request};

const SECRET: &str = "a fuzzing secret";

fn sign(event: &str, timestamp: &str, body: &[u8]) -> String {
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, SECRET.as_bytes());
    let mut message = format!("{event}.{timestamp}.").into_bytes();
    message.extend_from_slice(body);
    let tag = ring::hmac::sign(&key, &message);
    let hex: String = tag
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("v1={hex}")
}

fuzz_target!(|data: &[u8]| {
    // An event id, a timestamp, a signature and a body, split at the first
    // three NULs.
    let mut parts = data.splitn(4, |byte| *byte == 0);
    let (Some(event), Some(timestamp), Some(signature)) =
        (parts.next(), parts.next(), parts.next())
    else {
        return;
    };
    let body = parts.next().unwrap_or_default();
    let (Ok(event), Ok(timestamp), Ok(signature)) = (
        std::str::from_utf8(event),
        std::str::from_utf8(timestamp),
        std::str::from_utf8(signature),
    ) else {
        return;
    };
    // The raw headers: parsed or refused, never a panic.
    let now_s = parse_timestamp(timestamp).unwrap_or(0);
    let _ = parse_signature(signature);
    let _ = plausible(timestamp, signature, now_s);
    let raw = Request {
        event,
        timestamp,
        signature,
        body,
    };
    let _ = verify(SECRET, &raw, now_s);
    // Signed with the secret: it verifies exactly when its parts are of
    // their forms, and not with a changed body.
    let signed = sign(event, timestamp, body);
    let request = Request {
        signature: &signed,
        ..raw
    };
    let formed = is_event_id(event) && parse_timestamp(timestamp).is_some();
    assert_eq!(verify(SECRET, &request, now_s), formed);
    let mut changed = body.to_vec();
    changed.push(b'x');
    let tampered = Request {
        body: &changed,
        ..request
    };
    assert!(!verify(SECRET, &tampered, now_s));
});
