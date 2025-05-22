pub mod conversion;
pub mod error;

use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// The ID of the trigger task which is globally unique.
pub type TriggerId = u64;

/// The severity level of the trigger.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum Severity {
    Critical,
    Warning,
    Info,
    Unknown,
}

impl Display for Severity {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Severity::Critical => write!(f, "critical"),
            Severity::Warning => write!(f, "warning"),
            Severity::Info => write!(f, "info"),
            Severity::Unknown => write!(f, "unknown"),
        }
    }
}

pub type Labels = HashMap<String, String>;
pub type Annotations = HashMap<String, String>;

/// Represents the metadata of a trigger task.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TriggerTaskMetadata {
    /// The trigger ID which is globally unique.
    pub trigger_id: TriggerId,
    /// The name of the trigger task.
    pub trigger_name: String,
    pub create_if_not_exists: bool,
    // TODO: maybe use logical plan instead of sql.
    /// The sql to be executed periodically.
    pub sql: String,
    pub channels: Vec<TriggerChannel>,
    /// The severity level of the trigger, include critical, warning, info and
    /// unknown.
    pub severity: Severity,
    /// The user-defined labels.
    pub labels: Labels,
    /// The user-defined annotations.
    pub annotations: Annotations,
    pub interval: Duration,
}

/// The available channels for sending trigger notifications.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TriggerChannel {
    AlertManagerWebhook(AlertManagerOptions),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AlertManagerOptions {
    /// The URL of the AlertManager API endpoint.
    ///
    /// e.g., "http://localhost:9093".
    pub url: String,
    /// The timeout duration for the HTTP request.
    ///
    /// The timeout is applied from when the request starts connecting until the
    /// response body has finished.
    pub timeout: Option<Duration>,
}

/// Formats trigger fully-qualified name.
pub fn format_full_trigger_name(catalog: &str, trigger: &str) -> String {
    format!("{catalog}.{trigger}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_full_trigger_name() {
        let catalog = "test_catalog";
        let trigger = "test_trigger";
        let full_name = format_full_trigger_name(catalog, trigger);
        assert_eq!(full_name, "test_catalog.test_trigger");
    }
}
