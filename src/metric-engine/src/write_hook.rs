// Copyright 2023-2026 GrepTime Inc.
//
// This file is part of the GreptimeDB Enterprise Edition and is licensed under
// the GreptimeDB Enterprise License. You may not use this file except in
// compliance with that license. A copy of the license is available at the root
// of this repository in the file LICENSE-ENTERPRISE.
//
// Unless required by applicable law or agreed to in writing, this software is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied.

//! Enterprise extension at the Metric Engine online write boundary.

use std::sync::Arc;

use async_trait::async_trait;
use common_error::ext::BoxedError;
use store_api::region_engine::RegionEngine;
use store_api::region_request::{AffectedRows, RegionRequest};
use store_api::storage::RegionId;

/// Node-wide policy for Metric Engine online Put and BulkInserts.
///
/// Called after logical-to-physical conversion and before Mito writes. Deletes,
/// WAL replay and ordinary Mito table writes bypass this hook. Implementations
/// own validation before filtering, series extraction, policy and observability.
#[async_trait]
pub trait MetricWriteHook: Send + Sync {
    /// May transform the request; errors abort the write. A filtered BulkInserts
    /// must keep its RecordBatch and IPC payload consistent. The engine reference
    /// provides physical metadata; implementations must not submit writes here.
    ///
    /// Return `Some(original_rows)` to acknowledge intentional drops, or `None`
    /// to use Mito's affected rows. The override applies only on write success.
    async fn pre_write(
        &self,
        region_id: RegionId,
        request: &mut RegionRequest,
        mito: &dyn RegionEngine,
    ) -> Result<Option<AffectedRows>, BoxedError>;
}

/// Register this exact type in `Plugins` before constructing the datanode.
pub type MetricWriteHookRef = Arc<dyn MetricWriteHook>;
