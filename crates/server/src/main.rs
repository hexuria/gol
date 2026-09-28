#![forbid(unsafe_code)]
use std::sync::Arc;

use server::{
    auth_from_env, queue_from_env, router_with_memory, start_queue, stores_from_env,
    HttpGatewayPoster,
};

#[tokio::main]
async fn main() {
    // A variable that is not valid UTF-8 cannot configure gol; skip it rather
    // than panic.
    let env: std::collections::BTreeMap<String, String> = std::env::vars_os()
        .filter_map(|(key, value)| Some((key.into_string().ok()?, value.into_string().ok()?)))
        .collect();
    let auth = match auth_from_env(&env) {
        Ok(auth) => auth,
        Err(message) => {
            eprintln!("gol: refusing to start: {message}");
            std::process::exit(2);
        }
    };
    if env.get("GOL_AUTH").map(String::as_str) == Some("local-dev") {
        eprintln!(
            "gol: WARNING: GOL_AUTH=local-dev accepts a static token; never use it in production"
        );
    }
    let queue = match queue_from_env(&env) {
        Ok(queue) => queue,
        Err(message) => {
            eprintln!("gol: refusing to start: {message}");
            std::process::exit(2);
        }
    };
    // Postgres when GOL_DATABASE_URL is set. Connecting blocks, so it runs
    // off the async runtime.
    let stores = match tokio::task::spawn_blocking(move || stores_from_env(&env)).await {
        Ok(Ok(stores)) => stores,
        Ok(Err(message)) => {
            eprintln!("gol: refusing to start: {message}");
            std::process::exit(2);
        }
        Err(error) => {
            eprintln!("gol: refusing to start: {error}");
            std::process::exit(2);
        }
    };
    let port = std::env::var("GOL_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(43123);
    let jev_base_url = std::env::var("TYPESAFE_BASE_URL")
        .unwrap_or_else(|_| "https://api.typesafe.ai".to_string());
    // GOL_REDIS_URL queues runs, and worker threads in this process run
    // them (owner decision 1A for C4).
    if let Some(queue) = &queue {
        start_queue(
            queue,
            stores.runs.clone(),
            stores.memory.clone(),
            &jev_base_url,
        );
    }
    let app = router_with_memory(
        stores.runs,
        stores.memory,
        jev_base_url,
        queue.map(|queue| queue.redis_url),
        Arc::new(HttpGatewayPoster::from_env()),
        auth,
    );
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .expect("bind");
    println!("gol listening on http://127.0.0.1:{port}");
    axum::serve(listener, app).await.expect("serve");
}
