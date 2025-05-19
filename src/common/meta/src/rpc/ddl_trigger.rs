use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::time::Duration;

use api::v1::channel::Options as PbOptions;
use api::v1::meta::CreateTriggerTask as PbCreateTriggerTask;
use api::v1::{
    Channel as PbChannel, CreateTriggerExpr, CreateTriggerExpr as PbCreateTriggerExpr,
    Severity as PbSeverity,
};
use serde::{Deserialize, Serialize};

use crate::error;
use crate::error::Result;

// Create trigger
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTriggerTask {
    pub catalog_name: String,
    pub trigger_name: String,
    pub create_if_not_exists: bool,
    pub sql: String,
    pub channels: Vec<TriggerChannel>,
    pub severity: Severity,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
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

impl From<TriggerChannel> for PbOptions {
    fn from(_channel: TriggerChannel) -> Self {
        todo!()
    }
}

impl From<PbOptions> for TriggerChannel {
    fn from(channel: PbOptions) -> Self {
        match channel {
            PbOptions::AlertManagerOpts(opts) => {
                let url = opts.url;
                let timeout = if opts.timeout == 0 {
                    None
                } else {
                    Some(Duration::from_millis(opts.timeout))
                };
                TriggerChannel::AlertManagerWebhook(AlertManagerOptions { url, timeout })
            }
        }
    }
}

impl TryFrom<i32> for Severity {
    type Error = error::Error;

    fn try_from(severity: i32) -> Result<Self> {
        match severity {
            val if val == PbSeverity::Critical as i32 => Ok(Severity::Critical),
            val if val == PbSeverity::Warning as i32 => Ok(Severity::Warning),
            val if val == PbSeverity::Info as i32 => Ok(Severity::Info),
            val if val == PbSeverity::Unknown as i32 => Ok(Severity::Unknown),
            _ => error::UnsupportedSeveritySnafu { severity }.fail(),
        }
    }
}

impl TryFrom<PbCreateTriggerTask> for CreateTriggerTask {
    type Error = error::Error;

    fn try_from(task: PbCreateTriggerTask) -> Result<Self> {
        let PbCreateTriggerTask { create_trigger } = task;

        let create_trigger = create_trigger.unwrap();

        let PbCreateTriggerExpr {
            catalog_name,
            trigger_name,
            create_if_not_exists,
            sql,
            channels,
            severity,
            labels,
            annotations,
            interval,
        } = create_trigger;

        Ok(CreateTriggerTask {
            catalog_name,
            trigger_name,
            create_if_not_exists,
            sql,
            channels: channels
                .into_iter()
                .map(|c| TriggerChannel::from(c.options.unwrap()))
                .collect(),
            severity: severity.try_into()?,
            labels,
            annotations,
            interval: Duration::from_millis(interval),
        })
    }
}

impl From<CreateTriggerTask> for PbCreateTriggerTask {
    fn from(task: CreateTriggerTask) -> Self {
        let CreateTriggerTask {
            catalog_name,
            trigger_name,
            create_if_not_exists,
            sql,
            channels,
            severity,
            labels,
            annotations,
            interval,
        } = task;

        let channels = channels
            .into_iter()
            .map(|c| PbChannel {
                options: Some(c.into()),
            })
            .collect::<Vec<_>>();

        PbCreateTriggerTask {
            create_trigger: Some(CreateTriggerExpr {
                catalog_name,
                trigger_name,
                create_if_not_exists,
                sql,
                channels,
                severity: severity as i32,
                labels,
                annotations,
                interval: interval.as_millis() as u64,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use api::v1::channel::Options as PbOptions;
    use api::v1::AlertManagerOptions as PbAlertManagerOptions;

    use super::*;

    #[test]
    fn test_convert_channel() {
        let am = PbAlertManagerOptions {
            url: "http://localhost:9093".to_string(),
            timeout: 5000,
        };
        let channel = PbOptions::AlertManagerOpts(am);
        let trigger_channel: TriggerChannel = channel.into();

        match trigger_channel {
            TriggerChannel::AlertManagerWebhook(options) => {
                assert_eq!(options.url, "http://localhost:9093");
                assert_eq!(options.timeout, Some(Duration::from_millis(5000)));
            }
        }
    }

    #[test]
    fn test_convert_severity() {
        assert_eq!(
            Severity::try_from(PbSeverity::Critical as i32).unwrap(),
            Severity::Critical
        );
        assert_eq!(
            Severity::try_from(PbSeverity::Warning as i32).unwrap(),
            Severity::Warning
        );
        assert_eq!(
            Severity::try_from(PbSeverity::Info as i32).unwrap(),
            Severity::Info
        );
        assert_eq!(
            Severity::try_from(PbSeverity::Unknown as i32).unwrap(),
            Severity::Unknown
        );

        let invalid_severity = 999;
        assert!(Severity::try_from(invalid_severity).is_err());
    }

    // TODO(fys): add more unit tests
}
