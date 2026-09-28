#![forbid(unsafe_code)]
mod client;
mod error;
mod openai;
mod providers;
mod transport;

pub use client::{GatewayClient, Missing, ModelGateway, Set};
pub use error::GatewayError;
pub use providers::{parse, Completion};
pub use transport::{HttpTransport, UreqTransport, DEFAULT_TIMEOUT};
