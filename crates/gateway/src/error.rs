use std::fmt;

use protocol::ModelProvider;

#[derive(Debug)]
pub enum GatewayError {
    UnsupportedProvider(ModelProvider),
    /// The platform has no API key, or no base URL, for this provider
    /// (decision 21A).
    NotConfigured(ModelProvider),
    /// A bring-your-own credential: the server cannot resolve its secret
    /// (decision 22A).
    BringYourOwn,
    /// A model name that is not a plain model id. It goes into the request,
    /// for Gemini into the URL path, so it is refused before any HTTP.
    InvalidModelName(String),
    Transport(String),
    Malformed(String),
}

impl fmt::Display for GatewayError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedProvider(provider) => {
                write!(f, "provider is not implemented in this slice: {provider:?}")
            }
            Self::NotConfigured(provider) => {
                write!(
                    f,
                    "no platform key or base URL is configured for {provider:?}"
                )
            }
            Self::InvalidModelName(name) => write!(f, "not a model id: {name:?}"),
            Self::BringYourOwn => {
                f.write_str("bring-your-own credentials are not supported for server runs")
            }
            Self::Transport(message) | Self::Malformed(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for GatewayError {}
