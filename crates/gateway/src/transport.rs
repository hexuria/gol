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

/// How long a model call may take before it fails (decision 24A).
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(60);

/// HTTP over ureq, with a whole-call timeout and no retry: a failed call
/// fails the run, and a crashed one is performed again on resume.
#[derive(Clone, Debug)]
pub struct UreqTransport {
    agent: ureq::Agent,
}

impl UreqTransport {
    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            agent: ureq::AgentBuilder::new().timeout(timeout).build(),
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
        let response = request
            .send_json(body.clone())
            .map_err(|error| GatewayError::Transport(error.to_string()))?;
        response
            .into_json()
            .map_err(|error| GatewayError::Malformed(error.to_string()))
    }
}
