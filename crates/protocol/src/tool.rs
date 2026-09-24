use serde::{Deserialize, Serialize};

use crate::{Capability, ToolId};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolDescriptor {
    pub id: ToolId,
    pub name: String,
    pub description: String,
    pub input_schema: String,
    pub output_schema: String,
    pub required_capability: Capability,
}
