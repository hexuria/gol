use std::sync::mpsc;
use std::thread;

use gateway::{GatewayClient, ModelGateway};
use protocol::{CredentialSource, MessageRole, ModelProvider, ModelRequest};
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn openai_payload_maps_to_model_message() {
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
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "choices": [{
                        "message": { "role": "assistant", "content": "pong" }
                    }]
                })))
                .mount(&server)
                .await;
            uri_tx.send(server.uri()).expect("uri");
            let _ = stop_rx.recv();
        });
    });

    let uri = uri_rx.recv().expect("mock uri");
    let client = GatewayClient::new()
        .provider(ModelProvider::OpenAI)
        .credential(CredentialSource::PlatformGateway)
        .base_url(uri);
    let message = client
        .complete(&ModelRequest {
            provider: ModelProvider::OpenAI,
            model_name: "gpt-test".to_string(),
            prompt: "ping".to_string(),
        })
        .expect("mapped message");
    assert_eq!(message.role, MessageRole::Assistant);
    assert_eq!(message.text, "pong");

    let _ = stop_tx.send(());
    worker.join().expect("mock thread");
}
