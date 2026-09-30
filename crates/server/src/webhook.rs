//! A webhook request's check (Phase 4.3, decisions 73A-76A): a signature of
//! the timestamp and the body under the trigger's secret, a timestamp within
//! `MAX_SKEW_S` of now, and the sender's event id. The parsers here read
//! headers a sender on the network writes.

/// How far a request's timestamp may be from the server's clock, in seconds.
pub const MAX_SKEW_S: i64 = 300;

/// The longest event id a sender may name.
pub const MAX_EVENT_ID: usize = 128;

/// The tag of an `X-Gol-Signature` header: `v1=` then 64 hex digits (either
/// case). Anything else is none.
pub fn parse_signature(header: &str) -> Option<[u8; 32]> {
    let hex = header.strip_prefix("v1=")?.as_bytes();
    if hex.len() != 64 {
        return None;
    }
    let mut tag = [0u8; 32];
    for (byte, [high, low]) in tag.iter_mut().zip(hex.as_chunks::<2>().0) {
        *byte = (hex_digit(*high)? << 4) | hex_digit(*low)?;
    }
    Some(tag)
}

fn hex_digit(digit: u8) -> Option<u8> {
    match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        b'A'..=b'F' => Some(digit - b'A' + 10),
        _ => None,
    }
}

/// The Unix seconds of an `X-Gol-Timestamp` header: 1 to 12 ASCII digits.
pub fn parse_timestamp(header: &str) -> Option<i64> {
    if header.is_empty() || header.len() > 12 || !header.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    header.parse().ok()
}

/// Whether an `X-Gol-Event` header is an event id: 1 to `MAX_EVENT_ID`
/// visible ASCII characters.
pub fn is_event_id(header: &str) -> bool {
    !header.is_empty()
        && header.len() <= MAX_EVENT_ID
        && header.bytes().all(|b| b.is_ascii_graphic())
}

/// Whether `signature` signs "`timestamp`.`body`" (the timestamp as sent)
/// under `secret`, in constant time, and the timestamp is within
/// `MAX_SKEW_S` of `now_s`.
pub fn verify(secret: &str, timestamp: &str, signature: &str, body: &[u8], now_s: i64) -> bool {
    let (Some(at), Some(tag)) = (parse_timestamp(timestamp), parse_signature(signature)) else {
        return false;
    };
    if (now_s - at).abs() > MAX_SKEW_S {
        return false;
    }
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
    let mut message = Vec::with_capacity(timestamp.len() + 1 + body.len());
    message.extend_from_slice(timestamp.as_bytes());
    message.push(b'.');
    message.extend_from_slice(body);
    ring::hmac::verify(&key, &message, &tag).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn sign(secret: &str, timestamp: &str, body: &[u8]) -> String {
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
        let mut message = format!("{timestamp}.").into_bytes();
        message.extend_from_slice(body);
        let tag = ring::hmac::sign(&key, &message);
        let hex: String = tag
            .as_ref()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        format!("v1={hex}")
    }

    #[test]
    fn a_signed_request_verifies_and_a_changed_one_does_not() {
        let good = sign("secret", "1000", b"{}");
        assert!(verify("secret", "1000", &good, b"{}", 1000));
        assert!(verify(
            "secret",
            "1000",
            &good.to_uppercase().replace("V1=", "v1="),
            b"{}",
            1000
        ));
        assert!(!verify("secret", "1000", &good, b"{ }", 1000), "body");
        assert!(!verify("other", "1000", &good, b"{}", 1000), "secret");
        assert!(!verify("secret", "1001", &good, b"{}", 1000), "timestamp");
        assert!(verify("secret", "1000", &good, b"{}", 1000 + MAX_SKEW_S));
        assert!(
            !verify("secret", "1000", &good, b"{}", 1001 + MAX_SKEW_S),
            "stale"
        );
        assert!(
            !verify("secret", "1000", &good, b"{}", 999 - MAX_SKEW_S),
            "future"
        );
        assert!(!verify(
            "secret",
            "01000",
            &sign("secret", "01000", b"{}"),
            b"{}",
            1000 + 1_000_000
        ));
    }

    /// What `parse_signature` accepts, said another way: exactly `v1=` and
    /// 64 characters, each a hex digit.
    fn signature_oracle(header: &str) -> bool {
        header.len() == 67
            && header.starts_with("v1=")
            && header[3..].chars().all(|c| c.is_ascii_hexdigit())
    }

    proptest! {
        // Any header: no panic, and accepted exactly in its form.
        #[test]
        fn any_signature_header_is_read_only_in_its_form(header in any::<String>()) {
            prop_assert_eq!(parse_signature(&header).is_some(), signature_oracle(&header));
        }

        #[test]
        fn a_formed_signature_reads_back(tag in proptest::array::uniform32(any::<u8>())) {
            let hex: String = tag.iter().map(|byte| format!("{byte:02x}")).collect();
            prop_assert_eq!(parse_signature(&format!("v1={hex}")), Some(tag));
        }

        #[test]
        fn any_timestamp_header_is_read_only_in_its_form(header in any::<String>()) {
            let oracle = (1..=12).contains(&header.len())
                && header.chars().all(|c| c.is_ascii_digit());
            prop_assert_eq!(parse_timestamp(&header).is_some(), oracle);
        }

        #[test]
        fn any_event_id_is_read_only_in_its_form(header in any::<String>()) {
            let oracle = (1..=MAX_EVENT_ID).contains(&header.len())
                && header.chars().all(|c| c.is_ascii_graphic());
            prop_assert_eq!(is_event_id(&header), oracle);
        }

        // A signature over any body verifies it, and no other body.
        #[test]
        fn a_signature_verifies_its_body_only(
            body in proptest::collection::vec(any::<u8>(), 0..256),
            other in proptest::collection::vec(any::<u8>(), 0..256),
        ) {
            let signature = sign("secret", "1000", &body);
            prop_assert!(verify("secret", "1000", &signature, &body, 1000));
            prop_assert_eq!(verify("secret", "1000", &signature, &other, 1000), other == body);
        }
    }
}
