//! A TCP proxy in front of the test Redis that a test can turn against the
//! server: refuse new connections, and cut the ones it has.
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::{Arc, Mutex};

#[derive(Default)]
struct State {
    refusing: bool,
    open: Vec<TcpStream>,
}

#[derive(Clone)]
pub struct RedisProxy {
    port: u16,
    state: Arc<Mutex<State>>,
}

impl RedisProxy {
    /// A proxy that forwards to Redis on 127.0.0.1:6379 until told not to.
    pub fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let state = Arc::new(Mutex::new(State::default()));
        let shared = state.clone();
        std::thread::spawn(move || {
            for client in listener.incoming() {
                let Ok(client) = client else { continue };
                if shared.lock().expect("proxy").refusing {
                    continue;
                }
                let Ok(upstream) = TcpStream::connect("127.0.0.1:6379") else {
                    continue;
                };
                let mut state = shared.lock().expect("proxy");
                for stream in [&client, &upstream] {
                    state.open.push(stream.try_clone().expect("clone"));
                }
                pipe(
                    client.try_clone().expect("clone"),
                    upstream.try_clone().expect("clone"),
                );
                pipe(upstream, client);
            }
        });
        Self { port, state }
    }

    /// Redis database `db` through the proxy.
    pub fn url(&self, db: u8) -> String {
        format!("redis://127.0.0.1:{}/{db}", self.port)
    }

    /// Cuts every open connection; new ones still go through.
    pub fn cut(&self) {
        for stream in self.state.lock().expect("proxy").open.drain(..) {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }

    /// Refuses every new connection and cuts every open one: Redis is gone.
    pub fn go_down(&self) {
        let mut state = self.state.lock().expect("proxy");
        state.refusing = true;
        for stream in state.open.drain(..) {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }
}

fn pipe(mut from: TcpStream, mut to: TcpStream) {
    std::thread::spawn(move || {
        let mut buffer = [0u8; 16 * 1024];
        loop {
            match from.read(&mut buffer) {
                Ok(0) | Err(_) => break,
                Ok(read) => {
                    if to.write_all(&buffer[..read]).is_err() {
                        break;
                    }
                }
            }
        }
        let _ = to.shutdown(Shutdown::Write);
    });
}
