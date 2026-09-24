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

#[derive(Clone, Debug, Default)]
pub struct UreqTransport;

impl HttpTransport for UreqTransport {
    fn post_json(
        &self,
        url: &str,
        headers: &[(&str, &str)],
        body: &Value,
    ) -> Result<Value, GatewayError> {
        let mut request = ureq::post(url);
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
