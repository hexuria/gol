use std::net::SocketAddr;

#[tokio::main]
async fn main() {
    let port = std::env::var("GOL_PROXY_PORT")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(43124);
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("bind proxy");
    println!("gol proxy listening on http://{addr}");
    axum::serve(listener, proxy::router())
        .await
        .expect("serve proxy");
}
