use std::collections::HashMap;
use std::time::Duration;

use api::v1::meta::CreateTriggerTask as PbCreateTriggerTask;
use api::v1::{Channel as PbChannel, CreateTriggerExpr, CreateTriggerExpr as PbCreateTriggerExpr};
use common_error::ext::BoxedError;
use common_trigger::{Severity, TriggerChannel};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;

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
                .filter_map(|c| c.options)
                .map(|opt| opt.into())
                .collect(),
            severity: severity
                .try_into()
                .map_err(BoxedError::new)
                .context(error::ExternalSnafu)?,
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
