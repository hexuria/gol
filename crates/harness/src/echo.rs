use protocol::{Capability, ToolDescriptor, ToolId};

pub trait Tool {
    fn descriptor(&self) -> ToolDescriptor;
    fn call(&self, input: &str) -> Result<String, String>;
}

pub struct EchoTool;

impl EchoTool {
    pub fn descriptor() -> ToolDescriptor {
        ToolDescriptor {
            id: ToolId::from_uuid(uuid::Uuid::from_u128(
                0x3b1c_9c0a_4e2d_4b7a_9c11_8a0e_5d2f_6b41,
            )),
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

    fn call(&self, input: &str) -> Result<String, String> {
        Ok(input.to_string())
    }
}
