use std::sync::mpsc;
use std::thread;

use gateway::{GatewayClient, GatewayError, ModelGateway};
use protocol::{CredentialSource, MessageRole, ModelProvider, ModelRequest};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn anthropic_payload_maps_to_model_message() {
    let (uri_tx, uri_rx) = mpsc::channel();
    let (stop_tx, stop_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        runtime.block_on(async move {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/v1/messages"))
                .and(header("x-api-key", "test-key"))
                .and(header("anthropic-version", "2023-06-01"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "content": [{ "type": "text", "text": "anthropic-pong" }]
                })))
                .mount(&server)
                .await;
            uri_tx.send(server.uri()).expect("uri");
            let _ = stop_rx.recv();
        });
    });
    let client = GatewayClient::new()
        .provider(ModelProvider::Anthropic)
        .credential(CredentialSource::BringYourOwn {
            secret_ref: "test-key".to_string(),
        })
        .base_url(uri_rx.recv().expect("mock uri"));
    let message = client
        .complete(&ModelRequest {
            provider: ModelProvider::Anthropic,
            model_name: "claude-test".to_string(),
            prompt: "ping".to_string(),
        })
        .expect("mapped message");
    assert_eq!(message.role, MessageRole::Assistant);
    assert_eq!(message.text, "anthropic-pong");
    let _ = stop_tx.send(());
    worker.join().expect("mock thread");
}

#[test]
fn anthropic_http_error_is_gateway_error() {
    let (uri_tx, uri_rx) = mpsc::channel();
    let (stop_tx, stop_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        runtime.block_on(async move {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/v1/messages"))
                .respond_with(ResponseTemplate::new(500))
                .mount(&server)
                .await;
            uri_tx.send(server.uri()).expect("uri");
            let _ = stop_rx.recv();
        });
    });
    let client = GatewayClient::new()
        .provider(ModelProvider::Anthropic)
        .credential(CredentialSource::PlatformGateway)
        .base_url(uri_rx.recv().expect("mock uri"));
    let error = client
        .complete(&ModelRequest {
            provider: ModelProvider::Anthropic,
            model_name: "claude-test".to_string(),
            prompt: "ping".to_string(),
        })
        .expect_err("status error");
    assert!(matches!(error, GatewayError::Transport(_)));
    let _ = stop_tx.send(());
    worker.join().expect("mock thread");
}

#[test]
fn gemini_payload_maps_to_model_message() {
    let (uri_tx, uri_rx) = mpsc::channel();
    let (stop_tx, stop_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        runtime.block_on(async move {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/v1beta/models/gemini-test:generateContent"))
                .and(header("x-goog-api-key", "test-key"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "candidates": [{
                        "content": { "parts": [{ "text": "gemini-pong" }] }
                    }]
                })))
                .mount(&server)
                .await;
            uri_tx.send(server.uri()).expect("uri");
            let _ = stop_rx.recv();
        });
    });
    let client = GatewayClient::new()
        .provider(ModelProvider::Gemini)
        .credential(CredentialSource::BringYourOwn {
            secret_ref: "test-key".to_string(),
        })
        .base_url(uri_rx.recv().expect("mock uri"));
    let message = client
        .complete(&ModelRequest {
            provider: ModelProvider::Gemini,
            model_name: "gemini-test".to_string(),
            prompt: "ping".to_string(),
        })
        .expect("mapped message");
    assert_eq!(message.role, MessageRole::Assistant);
    assert_eq!(message.text, "gemini-pong");
    let _ = stop_tx.send(());
    worker.join().expect("mock thread");
}

#[test]
fn gemini_http_error_is_gateway_error() {
    let (uri_tx, uri_rx) = mpsc::channel();
    let (stop_tx, stop_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        runtime.block_on(async move {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/v1beta/models/gemini-test:generateContent"))
                .respond_with(ResponseTemplate::new(500))
                .mount(&server)
                .await;
            uri_tx.send(server.uri()).expect("uri");
            let _ = stop_rx.recv();
        });
    });
    let client = GatewayClient::new()
        .provider(ModelProvider::Gemini)
        .credential(CredentialSource::PlatformGateway)
        .base_url(uri_rx.recv().expect("mock uri"));
    let error = client
        .complete(&ModelRequest {
            provider: ModelProvider::Gemini,
            model_name: "gemini-test".to_string(),
            prompt: "ping".to_string(),
        })
        .expect_err("status error");
    assert!(matches!(error, GatewayError::Transport(_)));
    let _ = stop_tx.send(());
    worker.join().expect("mock thread");
}

#[test]
fn system_one_payload_maps_to_model_message() {
    let (uri_tx, uri_rx) = mpsc::channel();
    let (stop_tx, stop_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        runtime.block_on(async move {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/v1/chat/completions"))
                .and(header("authorization", "Bearer test-key"))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "choices": [{
                        "message": { "role": "assistant", "content": "systemone-pong" }
                    }]
                })))
                .mount(&server)
                .await;
            uri_tx.send(server.uri()).expect("uri");
            let _ = stop_rx.recv();
        });
    });
    let client = GatewayClient::new()
        .provider(ModelProvider::SystemOne)
        .credential(CredentialSource::BringYourOwn {
            secret_ref: "test-key".to_string(),
        })
        .base_url(uri_rx.recv().expect("mock uri"));
    let message = client
        .complete(&ModelRequest {
            provider: ModelProvider::SystemOne,
            model_name: "jev-test".to_string(),
            prompt: "ping".to_string(),
        })
        .expect("mapped message");
    assert_eq!(message.role, MessageRole::Assistant);
    assert_eq!(message.text, "systemone-pong");
    let _ = stop_tx.send(());
    worker.join().expect("mock thread");
}

#[test]
fn system_one_http_error_is_gateway_error() {
    let (uri_tx, uri_rx) = mpsc::channel();
    let (stop_tx, stop_rx) = mpsc::channel();
    let worker = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("tokio runtime");
        runtime.block_on(async move {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path("/v1/chat/completions"))
                .respond_with(ResponseTemplate::new(500))
                .mount(&server)
                .await;
            uri_tx.send(server.uri()).expect("uri");
            let _ = stop_rx.recv();
        });
    });
    let client = GatewayClient::new()
        .provider(ModelProvider::SystemOne)
        .credential(CredentialSource::PlatformGateway)
        .base_url(uri_rx.recv().expect("mock uri"));
    let error = client
        .complete(&ModelRequest {
            provider: ModelProvider::SystemOne,
            model_name: "jev-test".to_string(),
            prompt: "ping".to_string(),
        })
        .expect_err("status error");
    assert!(matches!(error, GatewayError::Transport(_)));
    let _ = stop_tx.send(());
    worker.join().expect("mock thread");
}
