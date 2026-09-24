use std::fmt;

use protocol::ModelProvider;

#[derive(Debug)]
pub enum GatewayError {
    UnsupportedProvider(ModelProvider),
    Transport(String),
    Malformed(String),
}

impl fmt::Display for GatewayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedProvider(provider) => {
                write!(f, "provider is not implemented in this slice: {provider:?}")
            }
            Self::Transport(message) | Self::Malformed(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for GatewayError {}
