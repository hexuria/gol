//! A webhook request's check (Phase 4.3, decisions 73A-76A and 79A): a
//! signature of the event id, the timestamp and the body under the
//! trigger's secret, a timestamp within `MAX_SKEW_S` of now, and the
//! sender's event id. The parsers here read headers a sender on the network
//! writes.

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

/// Whether a request's timestamp and signature are of their forms and the
/// timestamp is within `MAX_SKEW_S` of `now_s`: all that can be checked
/// before the trigger is looked up.
pub fn plausible(timestamp: &str, signature: &str, now_s: i64) -> bool {
    parse_signature(signature).is_some()
        && parse_timestamp(timestamp).is_some_and(|at| (now_s - at).abs() <= MAX_SKEW_S)
}

/// A request's check, as `verify` takes it.
#[derive(Clone, Copy, Debug)]
pub struct Request<'a> {
    pub event: &'a str,
    pub timestamp: &'a str,
    pub signature: &'a str,
    pub body: &'a [u8],
}

/// Whether the request's signature signs "`event`.`timestamp`.`body`" (each
/// as sent; 79A, so an event id cannot be changed on a captured request)
/// under `secret`, in constant time, its event id is one, and its timestamp
/// is within `MAX_SKEW_S` of `now_s`.
pub fn verify(secret: &str, request: &Request<'_>, now_s: i64) -> bool {
    let Request {
        event,
        timestamp,
        signature,
        body,
    } = *request;
    let Some(tag) = parse_signature(signature) else {
        return false;
    };
    if !is_event_id(event) || !plausible(timestamp, signature, now_s) {
        return false;
    }
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
    let mut message = Vec::with_capacity(event.len() + timestamp.len() + 2 + body.len());
    message.extend_from_slice(event.as_bytes());
    message.push(b'.');
    message.extend_from_slice(timestamp.as_bytes());
    message.push(b'.');
    message.extend_from_slice(body);
    ring::hmac::verify(&key, &message, &tag).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn sign(secret: &str, event: &str, timestamp: &str, body: &[u8]) -> String {
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
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

    /// `verify` of a request of these parts.
    fn check(
        secret: &str,
        event: &str,
        timestamp: &str,
        signature: &str,
        body: &[u8],
        now_s: i64,
    ) -> bool {
        let request = Request {
            event,
            timestamp,
            signature,
            body,
        };
        verify(secret, &request, now_s)
    }

    #[test]
    fn a_signed_request_verifies_and_a_changed_one_does_not() {
        let good = sign("secret", "evt-1", "1000", b"{}");
        assert!(check("secret", "evt-1", "1000", &good, b"{}", 1000));
        let upper = good.to_uppercase().replace("V1=", "v1=");
        assert!(check("secret", "evt-1", "1000", &upper, b"{}", 1000));
        assert!(
            !check("secret", "evt-1", "1000", &good, b"{ }", 1000),
            "body"
        );
        assert!(
            !check("other", "evt-1", "1000", &good, b"{}", 1000),
            "secret"
        );
        assert!(
            !check("secret", "evt-2", "1000", &good, b"{}", 1000),
            "event"
        );
        assert!(
            !check("secret", "evt-1", "1001", &good, b"{}", 1000),
            "timestamp"
        );
        assert!(check(
            "secret",
            "evt-1",
            "1000",
            &good,
            b"{}",
            1000 + MAX_SKEW_S
        ));
        assert!(
            !check("secret", "evt-1", "1000", &good, b"{}", 1001 + MAX_SKEW_S),
            "stale"
        );
        assert!(
            !check("secret", "evt-1", "1000", &good, b"{}", 999 - MAX_SKEW_S),
            "future"
        );
        // Each part is signed as sent: leading zeros and all.
        let zeros = sign("secret", "evt-1", "01000", b"{}");
        assert!(check("secret", "evt-1", "01000", &zeros, b"{}", 1000));
        assert!(
            !check("secret", "evt-1", "01000", &good, b"{}", 1000),
            "as sent"
        );
        // A signed request with an event id that is not one does not verify.
        let spaced = sign("secret", "evt 1", "1000", b"{}");
        assert!(!check("secret", "evt 1", "1000", &spaced, b"{}", 1000));
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
        fn any_signature_header_is_read_only_in_its_form(
            header in prop_oneof![any::<String>(), "(v1=|V1=|v2=)?[0-9a-fA-Fg]{62,66}"],
        ) {
            prop_assert_eq!(parse_signature(&header).is_some(), signature_oracle(&header));
        }

        #[test]
        fn a_formed_signature_reads_back(tag in proptest::array::uniform32(any::<u8>())) {
            let hex: String = tag.iter().map(|byte| format!("{byte:02x}")).collect();
            prop_assert_eq!(parse_signature(&format!("v1={hex}")), Some(tag));
        }

        #[test]
        fn any_timestamp_header_is_read_only_in_its_form(
            header in prop_oneof![any::<String>(), "[0-9]{0,14}", "[0-9 +-]{1,13}"],
        ) {
            let oracle = (1..=12).contains(&header.len())
                && header.chars().all(|c| c.is_ascii_digit());
            prop_assert_eq!(parse_timestamp(&header).is_some(), oracle);
        }

        #[test]
        fn any_event_id_is_read_only_in_its_form(
            header in prop_oneof![any::<String>(), "[ -~]{0,130}", "[!-~]{120,130}"],
        ) {
            let oracle = (1..=MAX_EVENT_ID).contains(&header.len())
                && header.chars().all(|c| c.is_ascii_graphic());
            prop_assert_eq!(is_event_id(&header), oracle);
        }

        // A signature over any event id verifies it, and no other.
        #[test]
        fn a_signature_verifies_its_event_only(
            event in "[!-~]{1,128}",
            other in "[!-~]{1,128}",
        ) {
            let signature = sign("secret", &event, "1000", b"{}");
            prop_assert!(check("secret", &event, "1000", &signature, b"{}", 1000), "signed");
            prop_assert_eq!(check("secret", &other, "1000", &signature, b"{}", 1000), other == event);
        }

        // A signature over any body verifies it, and no other body.
        #[test]
        fn a_signature_verifies_its_body_only(
            body in proptest::collection::vec(any::<u8>(), 0..256),
            other in proptest::collection::vec(any::<u8>(), 0..256),
        ) {
            let signature = sign("secret", "e", "1000", &body);
            prop_assert!(check("secret", "e", "1000", &signature, &body, 1000));
            prop_assert_eq!(check("secret", "e", "1000", &signature, &other, 1000), other == body);
        }

        // A signed request verifies exactly while its timestamp is within
        // the skew of now, either side, and so is plausible.
        #[test]
        fn a_signature_verifies_within_the_skew_only(
            at in 0i64..1_000_000_000_000,
            off in -2 * MAX_SKEW_S..=2 * MAX_SKEW_S,
        ) {
            let timestamp = at.to_string();
            let signature = sign("secret", "e", &timestamp, b"{}");
            let fresh = off.abs() <= MAX_SKEW_S;
            prop_assert_eq!(check("secret", "e", &timestamp, &signature, b"{}", at + off), fresh);
            prop_assert_eq!(plausible(&timestamp, &signature, at + off), fresh);
        }
    }
}
