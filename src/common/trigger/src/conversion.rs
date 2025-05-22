use std::time::Duration;

use greptime_ent_proto::v1::channel::Options as PbOptions;
use greptime_ent_proto::v1::Severity as PbSeverity;

use crate::{error, AlertManagerOptions, Severity, TriggerChannel};

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

    fn try_from(severity: i32) -> Result<Self, Self::Error> {
        match severity {
            val if val == PbSeverity::Critical as i32 => Ok(Severity::Critical),
            val if val == PbSeverity::Warning as i32 => Ok(Severity::Warning),
            val if val == PbSeverity::Info as i32 => Ok(Severity::Info),
            val if val == PbSeverity::Unknown as i32 => Ok(Severity::Unknown),
            _ => error::UnsupportedSeveritySnafu { severity }.fail(),
        }
    }
}

#[cfg(test)]
mod tests {
    use greptime_ent_proto::v1::channel::Options as PbOptions;
    use greptime_ent_proto::v1::AlertManagerOptions as PbAlertManagerOptions;

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
}
