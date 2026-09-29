use std::io::Read;
use std::time::Duration;

use serde_json::Value;

use crate::GatewayError;

pub trait HttpTransport {
    fn post_json(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: &Value,
    ) -> Result<Value, GatewayError>;
}

/// How much of a provider's error body an error keeps.
const ERROR_BODY_CHARS: usize = 300;

/// How long a model call may take before it fails (decision 24A).
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

/// HTTP over ureq, with a timeout and no retry: a failed call fails the run,
/// and a crashed one is performed again on resume. ureq 2 does not bound DNS
/// resolution, so a hung resolver can outlast the timeout.
#[derive(Clone, Debug)]
pub struct UreqTransport {
    agent: ureq::Agent,
}

impl UreqTransport {
    /// `timeout` bounds the connect and the whole call. Redirects are not
    /// followed: ureq would keep `x-api-key` and `x-goog-api-key` on the
    /// next request and return its body as the completion.
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            agent: ureq::AgentBuilder::new()
                .timeout(timeout)
                .timeout_connect(timeout)
                .redirects(0)
                .build(),
        }
    }
}

impl Default for UreqTransport {
    fn default() -> Self {
        Self::with_timeout(DEFAULT_TIMEOUT)
    }
}

impl HttpTransport for UreqTransport {
    fn post_json(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: &Value,
    ) -> Result<Value, GatewayError> {
        let mut request = self.agent.post(url);
        for (name, value) in headers {
            request = request.set(name, value);
        }
        let response = match request.send_json(body.clone()) {
            Ok(response) => response,
            Err(ureq::Error::Status(status, response)) => {
                return Err(status_error(url, status, response));
            }
            Err(other) => return Err(GatewayError::Transport(other.to_string())),
        };
        // ureq answers every status below 400 with Ok, so a redirect lands here.
        let status = response.status();
        if !(200..300).contains(&status) {
            return Err(status_error(url, status, response));
        }
        response
            .into_json()
            .map_err(|error| GatewayError::Malformed(error.to_string()))
    }
}

/// A response outside 200..300: its status and the start of its body, the
/// provider's own reason, read no further than that start needs.
fn status_error(url: &str, status: u16, response: ureq::Response) -> GatewayError {
    let mut head = Vec::new();
    let _ = response
        .into_reader()
        .take(ERROR_BODY_CHARS as u64 * 4)
        .read_to_end(&mut head);
    let detail: String = String::from_utf8_lossy(&head)
        .chars()
        .take(ERROR_BODY_CHARS)
        .collect();
    GatewayError::Transport(format!("{url}: status code {status}: {detail}"))
}
