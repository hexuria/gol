use protocol::{ModelMessage, ModelRequest, Usage};

pub trait ModelCompletion {
    fn complete(&self, request: &ModelRequest) -> Result<ModelMessage, String>;

    /// The completion, with the tokens it used when the provider reported
    /// them. The driver records both on `ModelResponded` (D1).
    fn complete_with_usage(
        &self,
        request: &ModelRequest,
    ) -> Result<(ModelMessage, Option<Usage>), String> {
        self.complete(request).map(|message| (message, None))
    }
}

pub struct UnavailableModel;

impl ModelCompletion for UnavailableModel {
    fn complete(&self, _request: &ModelRequest) -> Result<ModelMessage, String> {
        Err("no model gateway".to_string())
    }
}
