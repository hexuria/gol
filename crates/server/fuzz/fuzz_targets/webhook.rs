//! T9: any headers and body a webhook sender posts are parsed or refused
//! without panicking, and a request that checks carries a signature, a
//! timestamp and an event id of the shapes the parsers accept.
#![no_main]

use libfuzzer_sys::fuzz_target;
use server::webhook::{is_event_id, parse_signature, parse_timestamp, verify};

fuzz_target!(|data: &[u8]| {
    // Three header values and a body, split at the first three NULs.
    let mut parts = data.splitn(4, |byte| *byte == 0);
    let (Some(signature), Some(timestamp), Some(event)) =
        (parts.next(), parts.next(), parts.next())
    else {
        return;
    };
    let body = parts.next().unwrap_or_default();
    let (Ok(signature), Ok(timestamp), Ok(event)) = (
        std::str::from_utf8(signature),
        std::str::from_utf8(timestamp),
        std::str::from_utf8(event),
    ) else {
        return;
    };
    let _ = is_event_id(event);
    let now_s = parse_timestamp(timestamp).unwrap_or(0);
    if verify("a fuzzing secret", timestamp, signature, body, now_s) {
        assert!(parse_signature(signature).is_some());
        assert!(parse_timestamp(timestamp).is_some());
    }
});
