#![forbid(unsafe_code)]
use std::sync::Arc;

use server::{
    auth_from_env, queue_from_env, router_with_memory, start_queue, stores_from_env,
    HttpGatewayPoster, ModelsConfig, RedisRunQueue,
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
    // The platform's key per provider (D2). Without one, that provider's
    // model calls fail the run.
    let models = match ModelsConfig::from_env(&env) {
        Ok(models) => Arc::new(models),
        Err(message) => {
            eprintln!("gol: refusing to start: {message}");
            std::process::exit(2);
        }
    };
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
    // them (owner decision 1A for C4). Redis must answer before the server
    // takes a run.
    if let Some(queue) = &queue {
        let url = queue.redis_url.clone();
        let answered = tokio::task::spawn_blocking(move || RedisRunQueue::open(url).ping()).await;
        if let Err(message) = answered
            .map_err(|error| error.to_string())
            .and_then(|ping| ping)
        {
            eprintln!("gol: refusing to start: GOL_REDIS_URL: {message}");
            std::process::exit(2);
        }
    }
    let app = router_with_memory(
        stores.runs.clone(),
        stores.memory.clone(),
        jev_base_url.clone(),
        queue.as_ref().map(|queue| queue.redis_url.clone()),
        Arc::new(HttpGatewayPoster::from_env()),
        models.clone(),
        auth,
    );
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .expect("bind");
    // Workers start once the port is ours, so a server that cannot bind
    // claims no runs.
    if let Some(queue) = &queue {
        if let Err(message) = start_queue(
            queue,
            stores.runs,
            stores.memory,
            stores.messages,
            stores.outbox,
            &jev_base_url,
            models.clone(),
        ) {
            eprintln!("gol: refusing to start: queue workers: {message}");
            std::process::exit(2);
        }
    }
    println!("gol listening on http://127.0.0.1:{port}");
    axum::serve(listener, app).await.expect("serve");
}
