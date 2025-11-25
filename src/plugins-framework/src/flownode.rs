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

use std::sync::Arc;

use catalog::CatalogManagerRef;
use common_error::ext::BoxedError;
use common_meta::FlownodeId;
use common_meta::kv_backend::KvBackendRef;
use flow::FrontendClient;

use crate::common::GrpcExtensionRef;

/// The extension point for flownode instance.
#[derive(Default)]
pub struct FlownodePlugins {
    pub grpc: Option<GrpcExtensionRef>,
}

/// The factory trait to create [`FlownodePlugins`].
pub trait FlownodePluginFactory {
    fn create(
        &self,
        ctx: FlownodePluginContext,
    ) -> impl Future<Output = Result<FlownodePlugins, BoxedError>> + Send;
}

/// Context provided to [`FlownodePluginFactory`] during plugin creation.
pub struct FlownodePluginContext {
    pub kv_backend: KvBackendRef,
    pub fe_client: Arc<FrontendClient>,
    pub flownode_id: FlownodeId,
    pub catalog_manager: CatalogManagerRef,
}

/// Default no-op implementation of [`FlownodePluginFactory`].
pub struct DefaultFlownodePluginFactory;

impl FlownodePluginFactory for DefaultFlownodePluginFactory {
    async fn create(&self, _: FlownodePluginContext) -> Result<FlownodePlugins, BoxedError> {
        Ok(FlownodePlugins::default())
    }
}
