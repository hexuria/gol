use protocol::{ModelMessage, ModelRequest};

pub trait ModelCompletion {
    fn complete(&self, request: &ModelRequest) -> Result<ModelMessage, String>;
}

pub struct UnavailableModel;

impl ModelCompletion for UnavailableModel {
    fn complete(&self, _request: &ModelRequest) -> Result<ModelMessage, String> {
        Err("no model gateway".to_string())
    }
}
