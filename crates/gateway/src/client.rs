use std::marker::PhantomData;

use protocol::{CredentialSource, ModelProvider, ModelRequest};

use crate::openai::chat_body;
use crate::providers::{anthropic_body, gemini_body, parse, system_one_body};
use crate::{Completion, GatewayError, HttpTransport, UreqTransport};

#[derive(Clone, Copy, Debug, Default)]
pub struct Missing;

#[derive(Clone, Copy, Debug, Default)]
pub struct Set;

pub struct GatewayClient<P, C, T> {
    provider: Option<ModelProvider>,
    credential: Option<CredentialSource>,
    api_key: Option<String>,
    base_url: String,
    transport: T,
    _state: PhantomData<(P, C)>,
}

/// Where each provider is reached unless a base URL is configured. System
/// One has no public host, so it needs one (decision 21A).
fn default_base_url(provider: ModelProvider) -> &'static str {
    match provider {
        ModelProvider::OpenAI => "https://api.openai.com",
        ModelProvider::Anthropic => "https://api.anthropic.com",
        ModelProvider::Gemini => "https://generativelanguage.googleapis.com",
        ModelProvider::SystemOne => "",
    }
}

impl GatewayClient<Missing, Missing, UreqTransport> {
    pub fn new() -> Self {
        Self::with_transport(UreqTransport::default())
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
            api_key: None,
            base_url: default_base_url(ModelProvider::OpenAI).to_string(),
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
            api_key: self.api_key,
            base_url: self.base_url,
            transport: self.transport,
            _state: PhantomData,
        }
    }

    pub fn base_url(mut self, base_url: impl Into<String>) -> Self {
        self.base_url = base_url.into();
        self
    }

    /// The platform's API key for this client's provider (decision 21A).
    pub fn api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }
}

impl<C, T> GatewayClient<Missing, C, T> {
    /// Also resets the base URL to the provider's own host.
    pub fn provider(mut self, provider: ModelProvider) -> GatewayClient<Set, C, T> {
        self.provider = Some(provider);
        self.base_url = default_base_url(provider).to_string();
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
    /// Sends `request` to the provider with the platform's key for it. A
    /// bring-your-own credential, a missing key and a missing base URL are
    /// refused before any HTTP.
    pub fn send(&self, request: &ModelRequest) -> Result<Completion, GatewayError> {
        let provider = self.provider.expect("typestate recorded the provider");
        let credential = self
            .credential
            .as_ref()
            .expect("typestate recorded the credential");
        if request.provider != provider {
            return Err(GatewayError::UnsupportedProvider(request.provider));
        }
        let key = match credential {
            CredentialSource::BringYourOwn { .. } => return Err(GatewayError::BringYourOwn),
            CredentialSource::PlatformGateway => self
                .api_key
                .as_deref()
                .filter(|key| !key.is_empty())
                .ok_or(GatewayError::NotConfigured(provider))?,
        };
        let base = self.base_url.trim_end_matches('/');
        if base.is_empty() {
            return Err(GatewayError::NotConfigured(provider));
        }
        let json = ("content-type", "application/json");
        let response = match provider {
            ModelProvider::OpenAI | ModelProvider::SystemOne => {
                let body = match provider {
                    ModelProvider::SystemOne => {
                        system_one_body(&request.model_name, &request.prompt)
                    }
                    _ => chat_body(&request.model_name, &request.prompt),
                };
                let bearer = format!("Bearer {key}");
                self.transport.post_json(
                    &format!("{base}/v1/chat/completions"),
                    &[json, ("authorization", &bearer)],
                    &body,
                )?
            }
            ModelProvider::Anthropic => self.transport.post_json(
                &format!("{base}/v1/messages"),
                &[
                    json,
                    ("x-api-key", key),
                    ("anthropic-version", "2023-06-01"),
                ],
                &anthropic_body(&request.model_name, &request.prompt),
            )?,
            ModelProvider::Gemini => self.transport.post_json(
                &format!(
                    "{base}/v1beta/models/{}:generateContent",
                    request.model_name
                ),
                &[json, ("x-goog-api-key", key)],
                &gemini_body(&request.prompt),
            )?,
        };
        parse(provider, &response)
    }
}

pub trait ModelGateway {
    fn complete(&self, request: &ModelRequest) -> Result<Completion, GatewayError>;
}

impl<T: HttpTransport> ModelGateway for GatewayClient<Set, Set, T> {
    fn complete(&self, request: &ModelRequest) -> Result<Completion, GatewayError> {
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
    ) -> Result<RecordedPost, GatewayError> {
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
            .api_key("key-test")
            .send(&ModelRequest {
                provider,
                model_name: "model".to_string(),
                prompt: "hi".to_string(),
            })?;
        let mut posts = posts.lock().expect("recording");
        assert_eq!(posts.len(), 1, "{provider:?} posted once");
        Ok(posts.pop().expect("one post"))
    }

    #[test]
    fn provider_mismatch_does_not_call_http() {
        let client = GatewayClient::with_transport(PanicTransport)
            .provider(ModelProvider::Anthropic)
            .credential(CredentialSource::PlatformGateway)
            .api_key("key-test");
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

    // Decision 22A: a bring-your-own credential is refused before any HTTP,
    // so its secret reference is never sent anywhere.
    #[test]
    fn bring_your_own_is_refused_before_http() {
        for provider in [
            ModelProvider::OpenAI,
            ModelProvider::Anthropic,
            ModelProvider::Gemini,
            ModelProvider::SystemOne,
        ] {
            let error = GatewayClient::with_transport(PanicTransport)
                .provider(provider)
                .credential(CredentialSource::BringYourOwn {
                    secret_ref: "jev-secret-ref".to_string(),
                })
                .api_key("key-test")
                .base_url("http://127.0.0.1:9")
                .send(&ModelRequest {
                    provider,
                    model_name: "model".to_string(),
                    prompt: "hi".to_string(),
                })
                .unwrap_err();
            assert!(matches!(error, GatewayError::BringYourOwn), "{provider:?}");
        }
    }

    // Each provider has its own host; System One has none, so it needs a
    // configured base URL (decision 21A).
    #[test]
    fn provider_drops_openai_host_for_other_providers() {
        let cases = [
            (
                ModelProvider::Anthropic,
                "https://api.anthropic.com/v1/messages",
            ),
            (
                ModelProvider::Gemini,
                "https://generativelanguage.googleapis.com/v1beta/models/model:generateContent",
            ),
        ];
        for (provider, url) in cases {
            let post =
                send_recorded(provider, CredentialSource::PlatformGateway, None).expect("sent");
            assert_eq!(post.url, url, "{provider:?}");
        }
        assert!(matches!(
            send_recorded(
                ModelProvider::SystemOne,
                CredentialSource::PlatformGateway,
                None
            ),
            Err(GatewayError::NotConfigured(ModelProvider::SystemOne))
        ));
    }

    // Choosing the provider resets the base URL to its host.
    #[test]
    fn base_url_before_provider_does_not_keep_the_mock() {
        let mock = "http://127.0.0.1:9";
        let post = send_recorded(
            ModelProvider::Anthropic,
            CredentialSource::PlatformGateway,
            Some(mock),
        )
        .expect("sent");
        assert_eq!(post.url, "https://api.anthropic.com/v1/messages");
        assert!(matches!(
            send_recorded(
                ModelProvider::SystemOne,
                CredentialSource::PlatformGateway,
                Some(mock)
            ),
            Err(GatewayError::NotConfigured(ModelProvider::SystemOne))
        ));
    }

    #[test]
    fn openai_default_host_stays_api_openai_com() {
        let post = send_recorded(
            ModelProvider::OpenAI,
            CredentialSource::PlatformGateway,
            None,
        )
        .expect("sent");
        assert_eq!(post.url, "https://api.openai.com/v1/chat/completions");
        assert!(post
            .headers
            .contains(&("authorization".to_string(), "Bearer key-test".to_string())));
    }
}
