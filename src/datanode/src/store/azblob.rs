// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use common_telemetry::logging::info;
use object_store::services::Azblob as AzureBuilder;
use object_store::{util, ObjectStore};
use secrecy::ExposeSecret;
use snafu::prelude::*;

use crate::datanode::AzblobConfig;
use crate::error::{self, Result};

pub(crate) async fn new_azblob_object_store(azblob_config: &AzblobConfig) -> Result<ObjectStore> {
    let root = util::normalize_dir(&azblob_config.root);

    info!(
        "The azure storage container is: {}, root is: {}",
        azblob_config.container, &root
    );

    let mut builder = AzureBuilder::default();
    builder
        .root(&root)
        .container(&azblob_config.container)
        .endpoint(&azblob_config.endpoint)
        .account_name(azblob_config.account_name.expose_secret())
        .account_key(azblob_config.account_key.expose_secret());

    if let Some(token) = &azblob_config.sas_token {
        builder.sas_token(token);
    }

    Ok(ObjectStore::new(builder)
        .context(error::InitBackendSnafu)?
        .finish())
}
