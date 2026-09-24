use protocol::{Capability, ToolDescriptor, ToolId};

pub trait Tool {
    fn descriptor(&self) -> ToolDescriptor;
    fn call(&self, input: &str) -> String;
}

pub struct EchoTool;

impl EchoTool {
    pub fn descriptor() -> ToolDescriptor {
        ToolDescriptor {
            id: ToolId::new(),
            name: "echo".to_string(),
            description: "Returns the input text.".to_string(),
            input_schema: "{\"type\":\"string\"}".to_string(),
            output_schema: "{\"type\":\"string\"}".to_string(),
            required_capability: Capability::new("tool.echo"),
        }
    }
}

impl Tool for EchoTool {
    fn descriptor(&self) -> ToolDescriptor {
        Self::descriptor()
    }

    fn call(&self, input: &str) -> String {
        input.to_string()
    }
}
