use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyDecision {
    Allow,
    Deny { reason: String },
    RequireApproval,
    Modify,
    Limit,
    Redirect,
}
