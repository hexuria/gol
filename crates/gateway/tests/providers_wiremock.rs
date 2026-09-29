//! D1: each provider is called with the platform's key for it, in the header
//! that provider reads, and its response is parsed into the message and the
//! tokens the call used. A provider with no key or base URL configured, and a
//! bring-your-own credential, are refused before any HTTP (decisions 21A,
//! 22A). A slow provider times out (24A). Each provider is mocked with
//! wiremock on its own path.
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use gateway::{GatewayClient, GatewayError, HttpTransport, ModelGateway, UreqTransport};
use protocol::{CredentialSource, MessageRole, ModelProvider, ModelRequest, Usage};
use serde_json::{json, Value};
use wiremock::matchers::{body_partial_json, header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn each_provider_sends_its_key_and_reports_usage() {
    for kind in PROVIDERS {
        let text = format!("{kind:?}-pong");
        let (route, (name, value), body) = case(kind, &text);
        let mock = provider(
            route,
            Some((name, &value)),
            ResponseTemplate::new(200).set_body_json(body),
        );
        let completion = GatewayClient::new()
            .provider(kind)
            .credential(CredentialSource::PlatformGateway)
            .api_key("key-test")
            .base_url(&mock.uri)
            .complete(&request(kind))
            .unwrap_or_else(|error| panic!("{kind:?}: {error}"));
        assert_eq!(completion.message.role, MessageRole::Assistant);
        assert_eq!(completion.message.text, text);
        assert_eq!(
            completion.usage,
            Some(Usage {
                input_tokens: 11,
                output_tokens: 7,
            }),
            "{kind:?}"
        );
    }
}

#[test]
fn an_http_error_is_a_transport_error() {
    for kind in PROVIDERS {
        let (route, _, _) = case(kind, "");
        let mock = provider(route, None, ResponseTemplate::new(500));
        let error = GatewayClient::new()
            .provider(kind)
            .credential(CredentialSource::PlatformGateway)
            .api_key("key-test")
            .base_url(&mock.uri)
            .complete(&request(kind))
            .expect_err("status error");
        assert!(matches!(error, GatewayError::Transport(_)), "{kind:?}");
    }
}

// A response that reports no usage parses with none.
#[test]
fn a_response_without_usage_has_none() {
    for kind in PROVIDERS {
        let (route, (name, value), mut body) = case(kind, "no usage");
        let object = body.as_object_mut().expect("object");
        object.remove("usage");
        object.remove("usageMetadata");
        let mock = provider(
            route,
            Some((name, &value)),
            ResponseTemplate::new(200).set_body_json(body),
        );
        let completion = GatewayClient::new()
            .provider(kind)
            .credential(CredentialSource::PlatformGateway)
            .api_key("key-test")
            .base_url(&mock.uri)
            .complete(&request(kind))
            .expect("completion");
        assert_eq!(completion.message.text, "no usage");
        assert_eq!(completion.usage, None, "{kind:?}");
    }
}

// Decision 21A: the platform pays only with a key it was given for the
// provider. Without one the call is refused before any HTTP.
#[test]
fn a_platform_call_without_a_key_is_refused() {
    for kind in PROVIDERS {
        let error = GatewayClient::with_transport(NoHttp)
            .provider(kind)
            .credential(CredentialSource::PlatformGateway)
            .base_url("http://127.0.0.1:9")
            .complete(&request(kind))
            .expect_err("no key");
        assert!(
            matches!(error, GatewayError::NotConfigured(provider) if provider == kind),
            "{kind:?}: {error}"
        );
        let error = GatewayClient::with_transport(NoHttp)
            .provider(kind)
            .credential(CredentialSource::PlatformGateway)
            .api_key("")
            .base_url("http://127.0.0.1:9")
            .complete(&request(kind))
            .expect_err("an empty key");
        assert!(matches!(error, GatewayError::NotConfigured(_)), "{kind:?}");
    }
}

// Decision 22A: the server cannot resolve a bring-your-own secret, so it
// sends nothing rather than a call without the caller's key.
#[test]
fn bring_your_own_is_refused_without_http() {
    for kind in PROVIDERS {
        let error = GatewayClient::with_transport(NoHttp)
            .provider(kind)
            .credential(CredentialSource::BringYourOwn {
                secret_ref: "jev-secret-ref".to_string(),
            })
            .api_key("key-test")
            .base_url("http://127.0.0.1:9")
            .complete(&request(kind))
            .expect_err("bring your own");
        assert!(matches!(error, GatewayError::BringYourOwn), "{kind:?}");
        assert!(!error.to_string().contains("jev-secret-ref"));
    }
}

// Decision 24A: a provider that does not answer within the timeout is a
// transport error, and the call does not wait for it.
#[test]
fn a_provider_that_does_not_answer_times_out() {
    let (route, (name, value), body) = case(ModelProvider::OpenAI, "late");
    let mock = provider(
        route,
        Some((name, &value)),
        ResponseTemplate::new(200)
            .set_body_json(body)
            .set_delay(Duration::from_secs(3)),
    );
    let started = Instant::now();
    let error =
        GatewayClient::with_transport(UreqTransport::with_timeout(Duration::from_millis(200)))
            .provider(ModelProvider::OpenAI)
            .credential(CredentialSource::PlatformGateway)
            .api_key("key-test")
            .base_url(&mock.uri)
            .complete(&request(ModelProvider::OpenAI))
            .expect_err("timed out");
    assert!(matches!(error, GatewayError::Transport(_)), "{error}");
    assert!(started.elapsed() < Duration::from_secs(2));
}

// Review of #70: a model name goes into the request (for Gemini, into the
// URL path), and whoever creates a run chooses it. A name that is not a
// plain model id is refused before any HTTP, so the platform's key is never
// sent to a path the caller picked.
#[test]
fn a_model_name_that_is_not_an_id_is_refused_before_http() {
    for kind in PROVIDERS {
        for name in [
            "../tunedModels/x",
            "x#",
            "x?key=1",
            "a/b",
            "",
            "x y",
            "x\ny",
        ] {
            let error = GatewayClient::with_transport(NoHttp)
                .provider(kind)
                .credential(CredentialSource::PlatformGateway)
                .api_key("key-test")
                .base_url("http://127.0.0.1:9")
                .complete(&ModelRequest {
                    provider: kind,
                    model_name: name.to_string(),
                    prompt: "ping".to_string(),
                })
                .expect_err("refused");
            assert!(
                matches!(error, GatewayError::InvalidModelName(_)),
                "{kind:?} {name:?}: {error}"
            );
        }
    }
}

// For Gemini the name is a path segment followed by `:generateContent`, so a
// colon in it would name another method.
#[test]
fn a_gemini_model_name_with_a_colon_is_refused() {
    let error = GatewayClient::with_transport(NoHttp)
        .provider(ModelProvider::Gemini)
        .credential(CredentialSource::PlatformGateway)
        .api_key("key-test")
        .complete(&ModelRequest {
            provider: ModelProvider::Gemini,
            model_name: "x:streamGenerateContent".to_string(),
            prompt: "ping".to_string(),
        })
        .expect_err("refused");
    assert!(
        matches!(error, GatewayError::InvalidModelName(_)),
        "{error}"
    );
}

// A key ureq would not put in a header (a line break, a space, anything
// outside visible ASCII) is refused before any HTTP, and the error does not
// repeat it.
#[test]
fn a_key_with_a_line_break_is_refused_before_http() {
    for kind in PROVIDERS {
        for key in [
            "key\r\nx-evil: 1",
            "key\n",
            "\rkey",
            "sk-live-\u{e9}-do-not-log",
            "sk live",
            "sk\tlive",
        ] {
            let error = GatewayClient::with_transport(NoHttp)
                .provider(kind)
                .credential(CredentialSource::PlatformGateway)
                .api_key(key)
                .base_url("http://127.0.0.1:9")
                .complete(&request(kind))
                .expect_err("refused");
            assert!(
                matches!(error, GatewayError::NotConfigured(_)),
                "{kind:?} {key:?}"
            );
            assert!(!error.to_string().contains("live"), "{error}");
        }
    }
}

// The allow-list keeps the ids real models have: Gemini's dotted names, and
// OpenAI fine-tunes with colons.
#[test]
fn a_dotted_gemini_id_and_a_colon_openai_id_are_sent() {
    let text = "ok";
    let (_, (name, value), body) = case(ModelProvider::Gemini, text);
    let gemini = provider(
        "/v1beta/models/gemini-2.0-flash:generateContent",
        Some((name, &value)),
        ResponseTemplate::new(200).set_body_json(body),
    );
    let completion = GatewayClient::new()
        .provider(ModelProvider::Gemini)
        .credential(CredentialSource::PlatformGateway)
        .api_key("key-test")
        .base_url(&gemini.uri)
        .complete(&ModelRequest {
            provider: ModelProvider::Gemini,
            model_name: "gemini-2.0-flash".to_string(),
            prompt: "ping".to_string(),
        })
        .expect("a dotted Gemini id");
    assert_eq!(completion.message.text, text);

    let fine_tune = "ft:gpt-4o-mini:org:custom:abc123";
    let (route, (name, value), body) = case(ModelProvider::OpenAI, text);
    let openai = provider_with_body(
        route,
        (name, &value),
        json!({"model": fine_tune}),
        ResponseTemplate::new(200).set_body_json(body),
    );
    let completion = GatewayClient::new()
        .provider(ModelProvider::OpenAI)
        .credential(CredentialSource::PlatformGateway)
        .api_key("key-test")
        .base_url(&openai.uri)
        .complete(&ModelRequest {
            provider: ModelProvider::OpenAI,
            model_name: fine_tune.to_string(),
            prompt: "ping".to_string(),
        })
        .expect("a colon OpenAI id");
    assert_eq!(completion.message.text, text);
}

// Review of #70: a redirect is not followed. ureq would keep x-api-key and
// x-goog-api-key on the next request and parse its body as the completion;
// a 3xx is a transport error, and the Location is never asked.
#[test]
fn a_redirect_is_not_followed() {
    for kind in [ModelProvider::Anthropic, ModelProvider::Gemini] {
        let (route, _, _) = case(kind, "");
        let mock = redirecting(route);
        let error = GatewayClient::new()
            .provider(kind)
            .credential(CredentialSource::PlatformGateway)
            .api_key("key-test")
            .base_url(&mock.uri)
            .complete(&request(kind))
            .expect_err("a redirect");
        assert!(
            matches!(error, GatewayError::Transport(_)),
            "{kind:?}: {error}"
        );
        assert_eq!(mock.collected(), 0, "{kind:?} followed the redirect");
    }
}

// A connect that does not complete is cut at the timeout too (192.0.2.1 is
// TEST-NET-1, which nothing answers).
#[test]
fn a_connect_that_hangs_is_cut_at_the_timeout() {
    let started = Instant::now();
    let error =
        GatewayClient::with_transport(UreqTransport::with_timeout(Duration::from_millis(300)))
            .provider(ModelProvider::OpenAI)
            .credential(CredentialSource::PlatformGateway)
            .api_key("key-test")
            .base_url("http://192.0.2.1:81")
            .complete(&request(ModelProvider::OpenAI))
            .expect_err("no answer");
    assert!(matches!(error, GatewayError::Transport(_)), "{error}");
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );
}

// A provider's error says why the call failed; its start is kept.
#[test]
fn a_providers_error_body_is_kept_in_the_error() {
    let (route, _, _) = case(ModelProvider::Anthropic, "");
    let mock = provider(
        route,
        None,
        ResponseTemplate::new(529).set_body_string("overloaded_error: try again later"),
    );
    let error = GatewayClient::new()
        .provider(ModelProvider::Anthropic)
        .credential(CredentialSource::PlatformGateway)
        .api_key("key-test")
        .base_url(&mock.uri)
        .complete(&request(ModelProvider::Anthropic))
        .expect_err("status error");
    let message = error.to_string();
    assert!(matches!(error, GatewayError::Transport(_)));
    assert!(message.contains("529"), "{message}");
    assert!(message.contains("overloaded_error"), "{message}");
    assert!(!message.contains("key-test"), "{message}");
}

/// A provider mocked on a thread of its own: requests to `route` carrying
/// `auth` get `response`. Returns the server's URI and a stop handle.
struct Provider {
    uri: String,
    stop: mpsc::Sender<()>,
    worker: Option<thread::JoinHandle<()>>,
}

impl Drop for Provider {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(worker) = self.worker.take() {
            worker.join().expect("mock thread");
        }
    }
}

fn provider(route: &str, auth: Option<(&str, &str)>, response: ResponseTemplate) -> Provider {
    let (uri_tx, uri_rx) = mpsc::channel();
    let (stop, stop_rx) = mpsc::channel::<()>();
    let route = route.to_string();
    let auth = auth.map(|(name, value)| (name.to_string(), value.to_string()));
    let worker = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        runtime.block_on(async move {
            let server = MockServer::start().await;
            let mut mock = Mock::given(method("POST")).and(path(route));
            if let Some((name, value)) = auth {
                mock = mock.and(header(name.as_str(), value.as_str()));
            }
            mock.respond_with(response).mount(&server).await;
            uri_tx.send(server.uri()).expect("uri");
            let _ = stop_rx.recv();
        });
    });
    Provider {
        uri: uri_rx.recv().expect("mock uri"),
        stop,
        worker: Some(worker),
    }
}

fn request(provider: ModelProvider) -> ModelRequest {
    ModelRequest {
        provider,
        model_name: "model-test".to_string(),
        prompt: "ping".to_string(),
    }
}

/// Each provider: its route, the header its key goes in, and a response
/// with `text` that used 11 input and 7 output tokens.
fn case(provider: ModelProvider, text: &str) -> (&'static str, (&'static str, String), Value) {
    match provider {
        ModelProvider::OpenAI | ModelProvider::SystemOne => (
            "/v1/chat/completions",
            ("authorization", "Bearer key-test".to_string()),
            json!({
                "choices": [{"message": {"role": "assistant", "content": text}}],
                "usage": {"prompt_tokens": 11, "completion_tokens": 7, "total_tokens": 18}
            }),
        ),
        ModelProvider::Anthropic => (
            "/v1/messages",
            ("x-api-key", "key-test".to_string()),
            json!({
                "content": [{"type": "text", "text": text}],
                "usage": {"input_tokens": 11, "output_tokens": 7}
            }),
        ),
        ModelProvider::Gemini => (
            "/v1beta/models/model-test:generateContent",
            ("x-goog-api-key", "key-test".to_string()),
            json!({
                "candidates": [{"content": {"parts": [{"text": text}]}}],
                "usageMetadata": {"promptTokenCount": 11, "candidatesTokenCount": 7}
            }),
        ),
    }
}

const PROVIDERS: [ModelProvider; 4] = [
    ModelProvider::OpenAI,
    ModelProvider::Anthropic,
    ModelProvider::Gemini,
    ModelProvider::SystemOne,
];

/// A transport that fails the test if it is called.
struct NoHttp;

impl HttpTransport for NoHttp {
    fn post_json(
        &self,
        url: &str,
        _headers: &[(&str, &str)],
        _body: &Value,
    ) -> Result<Value, GatewayError> {
        panic!("http was called: {url}");
    }
}

/// Like `provider`, and the request must also carry `body` (partially).
fn provider_with_body(
    route: &str,
    (name, value): (&str, &str),
    body: Value,
    response: ResponseTemplate,
) -> Provider {
    let (uri_tx, uri_rx) = mpsc::channel();
    let (stop, stop_rx) = mpsc::channel::<()>();
    let (route, name, value) = (route.to_string(), name.to_string(), value.to_string());
    let worker = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        runtime.block_on(async move {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path(route))
                .and(header(name.as_str(), value.as_str()))
                .and(body_partial_json(body))
                .respond_with(response)
                .mount(&server)
                .await;
            uri_tx.send(server.uri()).expect("uri");
            let _ = stop_rx.recv();
        });
    });
    Provider {
        uri: uri_rx.recv().expect("mock uri"),
        stop,
        worker: Some(worker),
    }
}

/// A provider whose `route` answers 302 to `/collect`, which counts the
/// requests it gets.
struct Redirecting {
    uri: String,
    collected: mpsc::Receiver<usize>,
    stop: mpsc::Sender<()>,
    worker: Option<thread::JoinHandle<()>>,
}

impl Redirecting {
    fn collected(&self) -> usize {
        let _ = self.stop.send(());
        self.collected.recv().expect("count")
    }
}

impl Drop for Redirecting {
    fn drop(&mut self) {
        let _ = self.stop.send(());
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

fn redirecting(route: &str) -> Redirecting {
    let (uri_tx, uri_rx) = mpsc::channel();
    let (count_tx, collected) = mpsc::channel();
    let (stop, stop_rx) = mpsc::channel::<()>();
    let route = route.to_string();
    let worker = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        runtime.block_on(async move {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path(route))
                .respond_with(
                    ResponseTemplate::new(302)
                        .insert_header("location", format!("{}/collect", server.uri())),
                )
                .mount(&server)
                .await;
            Mock::given(path("/collect"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "content": [{"type": "text", "text": "collected"}],
                    "candidates": [{"content": {"parts": [{"text": "collected"}]}}]
                })))
                .mount(&server)
                .await;
            uri_tx.send(server.uri()).expect("uri");
            let _ = stop_rx.recv();
            let collected = server
                .received_requests()
                .await
                .unwrap_or_default()
                .iter()
                .filter(|request| request.url.path() == "/collect")
                .count();
            let _ = count_tx.send(collected);
        });
    });
    Redirecting {
        uri: uri_rx.recv().expect("mock uri"),
        collected,
        stop,
        worker: Some(worker),
    }
}
