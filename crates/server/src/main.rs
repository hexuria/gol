use std::sync::Arc;

use server::{router_with_gateway, HttpGatewayPoster, InMemoryStore};

#[tokio::main]
async fn main() {
    let port = std::env::var("GOL_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(43123);
    let jev_base_url = std::env::var("TYPESAFE_BASE_URL")
        .unwrap_or_else(|_| "https://api.typesafe.ai".to_string());
    let app = router_with_gateway(
        Arc::new(InMemoryStore::default()),
        jev_base_url,
        Arc::new(HttpGatewayPoster::from_env()),
    );
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .expect("bind");
    println!("gol listening on http://127.0.0.1:{port}");
    axum::serve(listener, app).await.expect("serve");
}
