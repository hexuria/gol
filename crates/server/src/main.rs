use std::sync::Arc;

use server::{router, InMemoryStore, LocalEchoFactory};

#[tokio::main]
async fn main() {
    let port = std::env::var("GOL_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(43123);
    let app = router(
        Arc::new(InMemoryStore::default()),
        Arc::new(LocalEchoFactory),
    );
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .expect("bind");
    println!("gol listening on http://127.0.0.1:{port}");
    axum::serve(listener, app).await.expect("serve");
}
