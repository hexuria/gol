use std::marker::PhantomData;

use protocol::{CredentialSource, ModelMessage, ModelProvider, ModelRequest};

use crate::openai::{chat_body, parse_chat_completion};
use crate::{GatewayError, HttpTransport, UreqTransport};

#[derive(Clone, Copy, Debug, Default)]
pub struct Missing;

#[derive(Clone, Copy, Debug, Default)]
pub struct Set;

pub struct GatewayClient<P, C, T> {
    provider: Option<ModelProvider>,
    credential: Option<CredentialSource>,
    base_url: String,
    transport: T,
    _state: PhantomData<(P, C)>,
}

impl GatewayClient<Missing, Missing, UreqTransport> {
    pub fn new() -> Self {
        Self::with_transport(UreqTransport)
    }
}

impl Default for GatewayClient<Missing, Missing, UreqTransport> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> GatewayClient<Missing, Missing, T> {
    pub fn with_transport(transport: T) -> Self {
        Self {
            provider: None,
            credential: None,
            base_url: "https://api.openai.com".to_string(),
            transport,
            _state: PhantomData,
        }
    }
}

impl<P, C, T> GatewayClient<P, C, T> {
    fn retag<P2, C2>(self) -> GatewayClient<P2, C2, T> {
        GatewayClient {
            provider: self.provider,
            credential: self.credential,
            base_url: self.base_url,
            transport: self.transport,
            _state: PhantomData,
        }
    }

    pub fn base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }
}

impl<C, T> GatewayClient<Missing, C, T> {
    pub fn provider(mut self, provider: ModelProvider) -> GatewayClient<Set, C, T> {
        self.provider = Some(provider);
        self.retag()
    }
}

impl<P, T> GatewayClient<P, Missing, T> {
    pub fn credential(mut self, credential: CredentialSource) -> GatewayClient<P, Set, T> {
        self.credential = Some(credential);
        self.retag()
    }
}

impl<T: HttpTransport> GatewayClient<Set, Set, T> {
    pub fn send(&self, request: &ModelRequest) -> Result<ModelMessage, GatewayError> {
        let provider = self.provider.expect("typestate recorded the provider");
        let credential = self
            .credential
            .clone()
            .expect("typestate recorded the credential");
        if request.provider != provider {
            return Err(GatewayError::UnsupportedProvider(request.provider));
        }
        match provider {
            ModelProvider::OpenAI => self.openai(request, &credential),
            ModelProvider::Anthropic | ModelProvider::Gemini | ModelProvider::SystemOne => {
                Err(GatewayError::UnsupportedProvider(provider))
            }
        }
    }

    fn openai(
        &self,
        request: &ModelRequest,
        credential: &CredentialSource,
    ) -> Result<ModelMessage, GatewayError> {
        let url = format!(
            "{}/v1/chat/completions",
            self.base_url.trim_end_matches('/')
        );
        let body = chat_body(&request.model_name, &request.prompt);
        let response = match credential {
            CredentialSource::BringYourOwn { secret_ref } => {
                let headers = [
                    ("content-type", "application/json"),
                    ("x-secret-ref", secret_ref.as_str()),
                ];
                self.transport.post_json(&url, &headers, &body)?
            }
            CredentialSource::PlatformGateway => {
                let headers = [("content-type", "application/json")];
                self.transport.post_json(&url, &headers, &body)?
            }
        };
        parse_chat_completion(&response)
    }
}

pub trait ModelGateway {
    fn complete(&self, request: &ModelRequest) -> Result<ModelMessage, GatewayError>;
}

impl<T: HttpTransport> ModelGateway for GatewayClient<Set, Set, T> {
    fn complete(&self, request: &ModelRequest) -> Result<ModelMessage, GatewayError> {
        self.send(request)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    struct PanicTransport;

    impl HttpTransport for PanicTransport {
        fn post_json(
            &self,
            _url: &str,
            _headers: &[(&str, &str)],
            _body: &Value,
        ) -> Result<Value, GatewayError> {
            panic!("http was called");
        }
    }

    #[test]
    fn anthropic_is_unsupported() {
        let client = GatewayClient::with_transport(PanicTransport)
            .provider(ModelProvider::Anthropic)
            .credential(CredentialSource::PlatformGateway);
        let error = client
            .send(&ModelRequest {
                provider: ModelProvider::Anthropic,
                model_name: "claude".to_string(),
                prompt: "hi".to_string(),
            })
            .unwrap_err();
        assert!(matches!(
            error,
            GatewayError::UnsupportedProvider(ModelProvider::Anthropic)
        ));
    }
}
