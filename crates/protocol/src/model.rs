use serde::{Deserialize, Serialize};

use crate::ModelProvider;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageRole {
    System,
    User,
    Assistant,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelMessage {
    pub role: MessageRole,
    pub text: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelRequest {
    pub provider: ModelProvider,
    pub model_name: String,
    pub prompt: String,
}
