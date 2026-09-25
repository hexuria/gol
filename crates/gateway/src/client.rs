use std::marker::PhantomData;

use protocol::{CredentialSource, ModelMessage, ModelProvider, ModelRequest};

use crate::openai::{chat_body, parse_chat_completion};
use crate::providers::{
    anthropic_body, gemini_body, parse_anthropic, parse_gemini, parse_system_one, system_one_body,
};
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
        if provider != ModelProvider::OpenAI {
            self.base_url = String::new();
        }
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
            ModelProvider::Anthropic => self.anthropic(request, &credential),
            ModelProvider::Gemini => self.gemini(request, &credential),
            ModelProvider::SystemOne => self.system_one(request, &credential),
        }
    }

    fn anthropic(
        &self,
        request: &ModelRequest,
        credential: &CredentialSource,
    ) -> Result<ModelMessage, GatewayError> {
        let url = format!("{}/v1/messages", self.base_url.trim_end_matches('/'));
        let body = anthropic_body(&request.model_name, &request.prompt);
        let response = match credential {
            CredentialSource::BringYourOwn { .. } => {
                let headers = [
                    ("content-type", "application/json"),
                    ("anthropic-version", "2023-06-01"),
                ];
                self.transport.post_json(&url, &headers, &body)?
            }
            CredentialSource::PlatformGateway => {
                let headers = [
                    ("content-type", "application/json"),
                    ("x-api-key", "platform"),
                    ("anthropic-version", "2023-06-01"),
                ];
                self.transport.post_json(&url, &headers, &body)?
            }
        };
        parse_anthropic(&response)
    }

    fn gemini(
        &self,
        request: &ModelRequest,
        credential: &CredentialSource,
    ) -> Result<ModelMessage, GatewayError> {
        let url = format!(
            "{}/v1beta/models/{}:generateContent",
            self.base_url.trim_end_matches('/'),
            request.model_name
        );
        let body = gemini_body(&request.prompt);
        let response = match credential {
            CredentialSource::BringYourOwn { .. } => {
                let headers = [("content-type", "application/json")];
                self.transport.post_json(&url, &headers, &body)?
            }
            CredentialSource::PlatformGateway => {
                let headers = [
                    ("content-type", "application/json"),
                    ("x-goog-api-key", "platform"),
                ];
                self.transport.post_json(&url, &headers, &body)?
            }
        };
        parse_gemini(&response)
    }

    fn system_one(
        &self,
        request: &ModelRequest,
        credential: &CredentialSource,
    ) -> Result<ModelMessage, GatewayError> {
        let url = format!(
            "{}/v1/chat/completions",
            self.base_url.trim_end_matches('/')
        );
        let body = system_one_body(&request.model_name, &request.prompt);
        let response = match credential {
            CredentialSource::BringYourOwn { .. } => {
                let headers = [("content-type", "application/json")];
                self.transport.post_json(&url, &headers, &body)?
            }
            CredentialSource::PlatformGateway => {
                let headers = [
                    ("content-type", "application/json"),
                    ("authorization", "Bearer platform"),
                ];
                self.transport.post_json(&url, &headers, &body)?
            }
        };
        parse_system_one(&response)
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
            CredentialSource::BringYourOwn { .. } => {
                let headers = [("content-type", "application/json")];
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
    use std::sync::{Arc, Mutex};

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

    struct RecordedPost {
        url: String,
        headers: Vec<(String, String)>,
    }

    struct RecordingTransport {
        posts: Arc<Mutex<Vec<RecordedPost>>>,
    }

    impl HttpTransport for RecordingTransport {
        fn post_json(
            &self,
            url: &str,
            headers: &[(&str, &str)],
            _body: &Value,
        ) -> Result<Value, GatewayError> {
            self.posts.lock().expect("recording").push(RecordedPost {
                url: url.to_string(),
                headers: headers
                    .iter()
                    .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
                    .collect(),
            });
            Ok(serde_json::json!({
                "content": [{ "text": "ok" }],
                "candidates": [{ "content": { "parts": [{ "text": "ok" }] } }],
                "choices": [{ "message": { "role": "assistant", "content": "ok" } }]
            }))
        }
    }

    fn send_recorded(
        provider: ModelProvider,
        credential: CredentialSource,
        base_url_first: Option<&str>,
    ) -> RecordedPost {
        let posts = Arc::new(Mutex::new(Vec::new()));
        let client = GatewayClient::with_transport(RecordingTransport {
            posts: Arc::clone(&posts),
        });
        let client = match base_url_first {
            Some(base_url) => client.base_url(base_url),
            None => client,
        };
        client
            .provider(provider)
            .credential(credential)
            .send(&ModelRequest {
                provider,
                model_name: "model".to_string(),
                prompt: "hi".to_string(),
            })
            .expect("recorded send");
        let mut posts = posts.lock().expect("recording");
        assert_eq!(posts.len(), 1, "{provider:?} posted once");
        posts.pop().expect("one post")
    }

    #[test]
    fn provider_mismatch_does_not_call_http() {
        let client = GatewayClient::with_transport(PanicTransport)
            .provider(ModelProvider::Anthropic)
            .credential(CredentialSource::PlatformGateway);
        let error = client
            .send(&ModelRequest {
                provider: ModelProvider::OpenAI,
                model_name: "gpt".to_string(),
                prompt: "hi".to_string(),
            })
            .unwrap_err();
        assert!(matches!(
            error,
            GatewayError::UnsupportedProvider(ModelProvider::OpenAI)
        ));
    }

    #[test]
    fn bring_your_own_header_omits_secret_ref() {
        let secret_ref = "jev-secret-ref";
        for provider in [
            ModelProvider::OpenAI,
            ModelProvider::Anthropic,
            ModelProvider::Gemini,
            ModelProvider::SystemOne,
        ] {
            let post = send_recorded(
                provider,
                CredentialSource::BringYourOwn {
                    secret_ref: secret_ref.to_string(),
                },
                None,
            );
            for (name, value) in &post.headers {
                assert!(
                    !value.contains(secret_ref),
                    "{provider:?} header {name} contained {secret_ref}: {value}"
                );
            }
        }
    }

    #[test]
    fn provider_drops_openai_host_for_other_providers() {
        let cases = [
            (ModelProvider::Anthropic, "/v1/messages"),
            (
                ModelProvider::Gemini,
                "/v1beta/models/model:generateContent",
            ),
            (ModelProvider::SystemOne, "/v1/chat/completions"),
        ];
        for (provider, suffix) in cases {
            let post = send_recorded(provider, CredentialSource::PlatformGateway, None);
            assert!(
                !post.url.contains("https://api.openai.com"),
                "{provider:?} kept the OpenAI host: {}",
                post.url
            );
            assert!(
                post.url.ends_with(suffix),
                "{provider:?} url {} does not end with {suffix}",
                post.url
            );
        }
    }

    #[test]
    fn base_url_before_provider_does_not_keep_the_mock() {
        let mock = "http://127.0.0.1:9";
        let cases = [
            (ModelProvider::Anthropic, "/v1/messages"),
            (
                ModelProvider::Gemini,
                "/v1beta/models/model:generateContent",
            ),
            (ModelProvider::SystemOne, "/v1/chat/completions"),
        ];
        for (provider, path) in cases {
            let post = send_recorded(provider, CredentialSource::PlatformGateway, Some(mock));
            assert_eq!(post.url, path, "{provider:?} kept {mock}");
        }
    }

    #[test]
    fn openai_default_host_stays_api_openai_com() {
        let post = send_recorded(
            ModelProvider::OpenAI,
            CredentialSource::PlatformGateway,
            None,
        );
        assert_eq!(post.url, "https://api.openai.com/v1/chat/completions");
    }
}
