//! T9: any bytes a provider sends back, as JSON, are parsed or refused by
//! every provider's parser without panicking.
#![no_main]

use libfuzzer_sys::fuzz_target;
use protocol::ModelProvider;

fuzz_target!(|data: &[u8]| {
    let Ok(body) = serde_json::from_slice::<serde_json::Value>(data) else {
        return;
    };
    for provider in [
        ModelProvider::OpenAI,
        ModelProvider::Anthropic,
        ModelProvider::Gemini,
        ModelProvider::SystemOne,
    ] {
        let _ = gateway::parse(provider, &body);
    }
});
