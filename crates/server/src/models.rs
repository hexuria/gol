//! The work model a run calls (D2): the gateway, with the platform's key for
//! the run's provider (decision 21A). A provider with no key, System One
//! without a base URL, and a bring-your-own credential (22A) fail the call,
//! and so the run, with a Dependency failure.
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use gateway::{GatewayClient, GatewayError, ModelGateway, Set, UreqTransport, DEFAULT_TIMEOUT};
use harness::ModelCompletion;
use protocol::{ModelMessage, ModelProvider, ModelRequest, RunSpec, Usage};

/// The longest model call `GOL_MODEL_TIMEOUT_SECS` may allow.
const MAX_TIMEOUT_SECS: u64 = 600;

/// The platform's key and base URL per provider, and the call timeout.
#[derive(Clone)]
pub struct ModelsConfig {
    keys: HashMap<ModelProvider, String>,
    base_urls: HashMap<ModelProvider, String>,
    timeout: Duration,
}

/// Names the providers that have a key, never the keys.
impl std::fmt::Debug for ModelsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut keyed: Vec<String> = self
            .keys
            .keys()
            .map(|provider| format!("{provider:?}"))
            .collect();
        keyed.sort();
        f.debug_struct("ModelsConfig")
            .field("keyed", &keyed)
            .field("base_urls", &self.base_urls)
            .field("timeout", &self.timeout)
            .finish()
    }
}

impl Default for ModelsConfig {
    /// No keys: every model call fails, as before D2.
    fn default() -> Self {
        Self {
            keys: HashMap::new(),
            base_urls: HashMap::new(),
            timeout: DEFAULT_TIMEOUT,
        }
    }
}

/// The name each provider's variables use: `GOL_<NAME>_API_KEY` and
/// `GOL_<NAME>_BASE_URL`.
const PROVIDERS: [(ModelProvider, &str); 4] = [
    (ModelProvider::OpenAI, "OPENAI"),
    (ModelProvider::Anthropic, "ANTHROPIC"),
    (ModelProvider::Gemini, "GEMINI"),
    (ModelProvider::SystemOne, "SYSTEMONE"),
];

impl ModelsConfig {
    /// `GOL_<PROVIDER>_API_KEY` and `GOL_<PROVIDER>_BASE_URL` for OpenAI,
    /// Anthropic, Gemini and System One (empty is unset), and
    /// `GOL_MODEL_TIMEOUT_SECS`, 1 to 600, 60 by default.
    /// Values are trimmed (a key read from a secret file keeps its trailing
    /// newline). A base URL must be `https://`, or `http://` to a local host.
    pub fn from_env(env: &BTreeMap<String, String>) -> Result<Self, String> {
        let set = |name: String| {
            env.get(&name)
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
        };
        let mut config = Self::default();
        for (provider, name) in PROVIDERS {
            if let Some(key) = set(format!("GOL_{name}_API_KEY")) {
                config.keys.insert(provider, key);
            }
            if let Some(url) = set(format!("GOL_{name}_BASE_URL")) {
                if !is_safe_base_url(&url) {
                    return Err(format!(
                        "GOL_{name}_BASE_URL must be https://, or http:// to a local host, not {url:?}"
                    ));
                }
                config.base_urls.insert(provider, url);
            }
        }
        if let Some(seconds) = set("GOL_MODEL_TIMEOUT_SECS".to_string()) {
            let seconds = seconds
                .parse::<u64>()
                .ok()
                .filter(|seconds| (1..=MAX_TIMEOUT_SECS).contains(seconds))
                .ok_or_else(|| {
                    format!(
                        "GOL_MODEL_TIMEOUT_SECS must be whole seconds from 1 to {MAX_TIMEOUT_SECS}, not {seconds:?}"
                    )
                })?;
            config.timeout = Duration::from_secs(seconds);
        }
        Ok(config)
    }

    pub fn with_key(mut self, provider: ModelProvider, key: impl Into<String>) -> Self {
        self.keys.insert(provider, key.into());
        self
    }

    pub fn with_base_url(mut self, provider: ModelProvider, url: impl Into<String>) -> Self {
        self.base_urls.insert(provider, url.into());
        self
    }

    pub fn api_key(&self, provider: ModelProvider) -> Option<&str> {
        self.keys.get(&provider).map(String::as_str)
    }

    pub fn base_url(&self, provider: ModelProvider) -> Option<&str> {
        self.base_urls.get(&provider).map(String::as_str)
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// The model `spec`'s run calls: its provider and credential, with the
    /// platform's key and base URL for that provider when configured.
    pub fn model_for(&self, spec: &RunSpec) -> GatewayModel {
        let provider = spec.work_model.provider;
        let mut client = GatewayClient::with_transport(UreqTransport::with_timeout(self.timeout))
            .provider(provider)
            .credential(spec.work_model.credential.clone());
        if let Some(key) = self.api_key(provider) {
            client = client.api_key(key);
        }
        if let Some(url) = self.base_url(provider) {
            client = client.base_url(url);
        }
        GatewayModel { provider, client }
    }
}

/// Whether `url` keeps the key off the network in the clear: `https://`, or
/// `http://` to this machine.
fn is_safe_base_url(url: &str) -> bool {
    if url.starts_with("https://") {
        return true;
    }
    let Some(rest) = url.strip_prefix("http://") else {
        return false;
    };
    let host = if rest.starts_with('[') {
        rest.split_inclusive(']').next().unwrap_or("")
    } else {
        rest.split([':', '/']).next().unwrap_or("")
    };
    matches!(host, "127.0.0.1" | "localhost" | "[::1]")
}

/// A run's work model over the gateway.
pub struct GatewayModel {
    provider: ModelProvider,
    client: GatewayClient<Set, Set, UreqTransport>,
}

/// What a failed call leaves in the run log, which the run's owner reads.
/// The gateway's own refusals are fixed words; a transport or parse error
/// may carry the URL and the provider's error body (which may repeat the
/// platform's key), so it goes to stderr and the log gets a fixed message.
fn run_log_reason(provider: ModelProvider, error: GatewayError) -> String {
    match error {
        GatewayError::NotConfigured(_)
        | GatewayError::BringYourOwn
        | GatewayError::UnsupportedProvider(_) => error.to_string(),
        GatewayError::InvalidModelName(_) => "the model name is not a model id".to_string(),
        GatewayError::Transport(_) | GatewayError::Malformed(_) => {
            eprintln!("gol: model call to {provider:?}: {error}");
            format!("model call to {provider:?} failed")
        }
    }
}

impl ModelCompletion for GatewayModel {
    fn complete(&self, request: &ModelRequest) -> Result<ModelMessage, String> {
        self.complete_with_usage(request)
            .map(|(message, _)| message)
    }

    fn complete_with_usage(
        &self,
        request: &ModelRequest,
    ) -> Result<(ModelMessage, Option<Usage>), String> {
        self.client
            .complete(request)
            .map(|completion| (completion.message, completion.usage))
            .map_err(|error| run_log_reason(self.provider, error))
    }
}
