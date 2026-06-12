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

use std::collections::HashSet;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use api::v1::SemanticType;
use common_telemetry::{debug, warn};
use datatypes::arrow::array::{ArrayRef, StructArray};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::ConcreteDataType;
use datatypes::vectors::Helper;
use index::inverted_index::create::InvertedIndexCreator;
use index::inverted_index::create::sort::external_sort::ExternalSorter;
use index::inverted_index::create::sort_create::SortIndexCreator;
use index::inverted_index::format::writer::InvertedIndexBlobWriter;
use index::target::IndexTarget;
use mito_codec::index::{IndexValueCodec, IndexValuesCodec};
use mito_codec::row_converter::{CompositeValues, SortField};
use puffin::puffin_manager::{PuffinWriter, PutOptions};
use snafu::{ResultExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, FileId};
use tokio::io::duplex;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

use crate::error::{
    BiErrorsSnafu, EncodeSnafu, IndexFinishSnafu, OperateAbortedIndexSnafu, PuffinAddBlobSnafu,
    PushIndexValueSnafu, Result,
};
use crate::read::Batch;
use crate::sst::index::intermediate::{
    IntermediateLocation, IntermediateManager, TempFileProvider,
};
use crate::sst::index::inverted_index::INDEX_BLOB_TYPE;
use crate::sst::index::puffin_manager::SstPuffinWriter;
use crate::sst::index::statistics::{ByteCount, RowCount, Statistics};
use crate::sst::index::{TYPE_INVERTED_INDEX, decode_primary_keys_with_counts};

/// The minimum memory usage threshold for one column.
const MIN_MEMORY_USAGE_THRESHOLD_PER_COLUMN: usize = 1024 * 1024; // 1MB

/// The buffer size for the pipe used to send index data to the puffin blob.
const PIPE_BUFFER_SIZE_FOR_SENDING_BLOB: usize = 8192;

#[derive(Debug, Clone)]
pub(crate) struct InvertedIndexTarget {
    pub target: IndexTarget,
    pub data_type: Option<ConcreteDataType>,
}

impl InvertedIndexTarget {
    pub(crate) fn column_id(column_id: ColumnId) -> Self {
        Self {
            target: IndexTarget::ColumnId(column_id),
            data_type: None,
        }
    }

    pub(crate) fn column_nested_path(
        column_id: ColumnId,
        path: Vec<String>,
        data_type: ConcreteDataType,
    ) -> Self {
        Self {
            target: IndexTarget::ColumnNestedPath { column_id, path },
            data_type: Some(data_type),
        }
    }

    fn column_id_value(&self) -> ColumnId {
        self.target.column_id()
    }

    fn target_key(&self) -> String {
        self.target.encode()
    }

    fn nested_path(&self) -> Option<&[String]> {
        match &self.target {
            IndexTarget::ColumnId(_) => None,
            IndexTarget::ColumnNestedPath { path, .. } => Some(path),
        }
    }
}

/// `InvertedIndexer` creates inverted index for SST files.
pub struct InvertedIndexer {
    /// The index creator.
    index_creator: Box<dyn InvertedIndexCreator>,
    /// The provider of intermediate files.
    temp_file_provider: Arc<TempFileProvider>,

    /// Codec for decoding primary keys.
    codec: IndexValuesCodec,
    /// Reusable buffer for encoding index values.
    value_buf: Vec<u8>,

    /// Statistics of index creation.
    stats: Statistics,
    /// Whether the index creation is aborted.
    aborted: bool,

    /// The memory usage of the index creator.
    memory_usage: Arc<AtomicUsize>,

    /// Root column and JSON nested path targets.
    indexed_targets: Vec<InvertedIndexTarget>,

    /// Region metadata for column lookups.
    metadata: RegionMetadataRef,
}

impl InvertedIndexer {
    /// Creates a new `InvertedIndexer`.
    /// Should ensure that the number of tag columns is greater than 0.
    pub fn new(
        sst_file_id: FileId,
        metadata: &RegionMetadataRef,
        intermediate_manager: IntermediateManager,
        memory_usage_threshold: Option<usize>,
        segment_row_count: NonZeroUsize,
        indexed_targets: Vec<InvertedIndexTarget>,
    ) -> Self {
        let temp_file_provider = Arc::new(TempFileProvider::new(
            IntermediateLocation::new(&metadata.region_id, &sst_file_id),
            intermediate_manager,
        ));

        let memory_usage = Arc::new(AtomicUsize::new(0));

        let sorter = ExternalSorter::factory(
            temp_file_provider.clone() as _,
            Some(MIN_MEMORY_USAGE_THRESHOLD_PER_COLUMN),
            memory_usage.clone(),
            memory_usage_threshold,
        );
        let index_creator = Box::new(SortIndexCreator::new(sorter, segment_row_count));

        let codec = IndexValuesCodec::from_tag_columns(
            metadata.primary_key_encoding,
            metadata.primary_key_columns(),
        );
        Self {
            codec,
            index_creator,
            temp_file_provider,
            value_buf: vec![],
            stats: Statistics::new(TYPE_INVERTED_INDEX),
            aborted: false,
            memory_usage,
            indexed_targets,
            metadata: metadata.clone(),
        }
    }

    /// Updates index with a batch of rows.
    /// Garbage will be cleaned up if failed to update.
    pub async fn update(&mut self, batch: &mut Batch) -> Result<()> {
        ensure!(!self.aborted, OperateAbortedIndexSnafu);

        if batch.is_empty() {
            return Ok(());
        }

        if let Err(update_err) = self.do_update(batch).await {
            // clean up garbage if failed to update
            if let Err(err) = self.do_cleanup().await {
                if cfg!(any(test, feature = "test")) {
                    panic!("Failed to clean up index creator, err: {err}",);
                } else {
                    warn!(err; "Failed to clean up index creator");
                }
            }
            return Err(update_err);
        }

        Ok(())
    }

    /// Updates the inverted index with the given flat format RecordBatch.
    pub async fn update_flat(&mut self, batch: &RecordBatch) -> Result<()> {
        ensure!(!self.aborted, OperateAbortedIndexSnafu);

        if batch.num_rows() == 0 {
            return Ok(());
        }

        self.do_update_flat(batch).await
    }

    async fn do_update_flat(&mut self, batch: &RecordBatch) -> Result<()> {
        {
            let mut guard = self.stats.record_update();
            guard.inc_row_count(batch.num_rows());
        }

        let is_sparse = self.metadata.primary_key_encoding == PrimaryKeyEncoding::Sparse;
        let mut decoded_pks: Option<Vec<(CompositeValues, usize)>> = None;

        let targets = self.indexed_targets.clone();
        for target in &targets {
            let col_id = target.column_id_value();
            let target_key = target.target_key();
            let Some(column_meta) = self.metadata.column_by_id(col_id) else {
                debug!(
                    "Column {} not found in the metadata during building inverted index",
                    col_id
                );
                continue;
            };
            let column_name = column_meta.column_schema.name.clone();
            if let Some(path) = target.nested_path() {
                self.push_json_path_index(
                    batch,
                    &column_name,
                    path,
                    target.data_type.as_ref(),
                    &target_key,
                )
                .await?;
            } else if let Some(column_array) = batch.column_by_name(&column_name) {
                // Convert Arrow array to VectorRef using Helper
                let vector = Helper::try_into_vector(column_array.clone())
                    .context(crate::error::ConvertVectorSnafu)?;
                let sort_field = SortField::new(vector.data_type());

                for row in 0..batch.num_rows() {
                    self.value_buf.clear();
                    let value_ref = vector.get_ref(row);

                    if value_ref.is_null() {
                        self.index_creator
                            .push_with_name(&target_key, None)
                            .await
                            .context(PushIndexValueSnafu)?;
                    } else {
                        IndexValueCodec::encode_nonnull_value(
                            value_ref,
                            &sort_field,
                            &mut self.value_buf,
                        )
                        .context(EncodeSnafu)?;
                        self.index_creator
                            .push_with_name(&target_key, Some(&self.value_buf))
                            .await
                            .context(PushIndexValueSnafu)?;
                    }
                }
            } else if is_sparse && column_meta.semantic_type == SemanticType::Tag {
                // Column not found in batch, tries to decode from primary keys for sparse encoding.
                if decoded_pks.is_none() {
                    decoded_pks = Some(decode_primary_keys_with_counts(batch, &self.codec)?);
                }

                let pk_values_with_counts = decoded_pks.as_ref().unwrap();
                let Some(col_info) = self.codec.pk_col_info(col_id) else {
                    debug!(
                        "Column {} not found in primary key during building bloom filter index",
                        column_name
                    );
                    continue;
                };
                let pk_index = col_info.idx;
                let field = &col_info.field;
                for (decoded, count) in pk_values_with_counts {
                    let value = match decoded {
                        CompositeValues::Dense(dense) => dense.get(pk_index).map(|v| &v.1),
                        CompositeValues::Sparse(sparse) => sparse.get(&col_id),
                    };

                    let elem = value
                        .filter(|v| !v.is_null())
                        .map(|v| {
                            self.value_buf.clear();
                            IndexValueCodec::encode_nonnull_value(
                                v.as_value_ref(),
                                field,
                                &mut self.value_buf,
                            )
                            .context(EncodeSnafu)?;
                            Ok(self.value_buf.as_slice())
                        })
                        .transpose()?;

                    self.index_creator
                        .push_with_name_n(&target_key, elem, *count)
                        .await
                        .context(PushIndexValueSnafu)?;
                }
            } else {
                debug!(
                    "Column {} not found in the batch during building inverted index",
                    col_id
                );
            }
        }

        Ok(())
    }

    async fn push_json_path_index(
        &mut self,
        batch: &RecordBatch,
        column_name: &str,
        path: &[String],
        data_type: Option<&ConcreteDataType>,
        target_key: &str,
    ) -> Result<()> {
        let Some(root) = batch.column_by_name(column_name) else {
            warn!(
                "JSON column {} not found in the batch during building inverted index for path {}, treat it as all null",
                column_name,
                path.join("."),
            );
            self.index_creator
                .push_with_name_n(target_key, None, batch.num_rows())
                .await
                .context(PushIndexValueSnafu)?;
            return Ok(());
        };

        let Some(resolved) = resolve_json_path_array(root.clone(), path) else {
            warn!(
                "Failed to resolve JSON path {} under column {} during building inverted index, treat it as all null",
                path.join("."),
                column_name,
            );
            self.index_creator
                .push_with_name_n(target_key, None, batch.num_rows())
                .await
                .context(PushIndexValueSnafu)?;
            return Ok(());
        };

        let vector = Helper::try_into_vector(resolved.leaf.clone())
            .context(crate::error::ConvertVectorSnafu)?;
        let sort_field = SortField::new(data_type.cloned().unwrap_or_else(|| vector.data_type()));

        for row in 0..batch.num_rows() {
            self.value_buf.clear();
            let value_ref = vector.get_ref(row);
            if value_ref.is_null() || resolved.parents.iter().any(|parent| parent.is_null(row)) {
                self.index_creator
                    .push_with_name(target_key, None)
                    .await
                    .context(PushIndexValueSnafu)?;
            } else {
                IndexValueCodec::encode_nonnull_value(value_ref, &sort_field, &mut self.value_buf)
                    .context(EncodeSnafu)?;
                self.index_creator
                    .push_with_name(target_key, Some(&self.value_buf))
                    .await
                    .context(PushIndexValueSnafu)?;
            }
        }

        Ok(())
    }

    /// Finishes index creation and cleans up garbage.
    /// Returns the number of rows and bytes written.
    pub async fn finish(
        &mut self,
        puffin_writer: &mut SstPuffinWriter,
    ) -> Result<(RowCount, ByteCount)> {
        ensure!(!self.aborted, OperateAbortedIndexSnafu);

        if self.stats.row_count() == 0 {
            // no IO is performed, no garbage to clean up, just return
            return Ok((0, 0));
        }

        let finish_res = self.do_finish(puffin_writer).await;
        // clean up garbage no matter finish successfully or not
        if let Err(err) = self.do_cleanup().await {
            if cfg!(any(test, feature = "test")) {
                panic!("Failed to clean up index creator, err: {err}",);
            } else {
                warn!(err; "Failed to clean up index creator");
            }
        }

        finish_res.map(|_| (self.stats.row_count(), self.stats.byte_count()))
    }

    /// Aborts index creation and clean up garbage.
    pub async fn abort(&mut self) -> Result<()> {
        if self.aborted {
            return Ok(());
        }
        self.aborted = true;

        self.do_cleanup().await
    }

    async fn do_update(&mut self, batch: &mut Batch) -> Result<()> {
        let n = batch.num_rows();
        {
            let mut guard = self.stats.record_update();
            guard.inc_row_count(n);
        }

        let targets = self.indexed_targets.clone();
        for target in &targets {
            if target.nested_path().is_some() {
                debug!(
                    "Skip JSON nested path target {} in non-flat inverted index update",
                    target.target_key()
                );
                continue;
            }

            let col_id = target.column_id_value();
            let target_key = target.target_key();
            match self.codec.pk_col_info(col_id) {
                // pk
                Some(col_info) => {
                    let pk_idx = col_info.idx;
                    let field = &col_info.field;
                    let value = batch
                        .pk_col_value(self.codec.decoder(), pk_idx, col_id)?
                        .filter(|v| !v.is_null())
                        .map(|v| {
                            self.value_buf.clear();
                            IndexValueCodec::encode_nonnull_value(
                                v.as_value_ref(),
                                field,
                                &mut self.value_buf,
                            )
                            .context(EncodeSnafu)?;
                            Ok(self.value_buf.as_slice())
                        })
                        .transpose()?;

                    self.index_creator
                        .push_with_name_n(&target_key, value, n)
                        .await
                        .context(PushIndexValueSnafu)?;
                }
                // fields
                None => {
                    let Some(values) = batch.field_col_value(col_id) else {
                        debug!(
                            "Column {} not found in the batch during building inverted index",
                            col_id
                        );
                        continue;
                    };
                    let sort_field = SortField::new(values.data.data_type());
                    for i in 0..n {
                        self.value_buf.clear();
                        let value = values.data.get_ref(i);
                        if value.is_null() {
                            self.index_creator
                                .push_with_name(&target_key, None)
                                .await
                                .context(PushIndexValueSnafu)?;
                        } else {
                            IndexValueCodec::encode_nonnull_value(
                                value,
                                &sort_field,
                                &mut self.value_buf,
                            )
                            .context(EncodeSnafu)?;
                            self.index_creator
                                .push_with_name(&target_key, Some(&self.value_buf))
                                .await
                                .context(PushIndexValueSnafu)?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Data flow of finishing index:
    ///
    /// ```text
    ///                               (In Memory Buffer)
    ///                                    ┌──────┐
    ///  ┌─────────────┐                   │ PIPE │
    ///  │             │ write index data  │      │
    ///  │ IndexWriter ├──────────────────►│ tx   │
    ///  │             │                   │      │
    ///  └─────────────┘                   │      │
    ///                  ┌─────────────────┤ rx   │
    ///  ┌─────────────┐ │ read as blob    └──────┘
    ///  │             │ │
    ///  │ PuffinWriter├─┤
    ///  │             │ │ copy to file    ┌──────┐
    ///  └─────────────┘ └────────────────►│ File │
    ///                                    └──────┘
    /// ```
    async fn do_finish(&mut self, puffin_writer: &mut SstPuffinWriter) -> Result<()> {
        let mut guard = self.stats.record_finish();

        let (tx, rx) = duplex(PIPE_BUFFER_SIZE_FOR_SENDING_BLOB);
        let mut index_writer = InvertedIndexBlobWriter::new(tx.compat_write());

        let (index_finish, puffin_add_blob) = futures::join!(
            // TODO(zhongzc): config bitmap type
            self.index_creator
                .finish(&mut index_writer, index::bitmap::BitmapType::Roaring),
            puffin_writer.put_blob(
                INDEX_BLOB_TYPE,
                rx.compat(),
                PutOptions::default(),
                Default::default(),
            )
        );

        match (
            puffin_add_blob.context(PuffinAddBlobSnafu),
            index_finish.context(IndexFinishSnafu),
        ) {
            (Err(e1), Err(e2)) => BiErrorsSnafu {
                first: Box::new(e1),
                second: Box::new(e2),
            }
            .fail()?,

            (Ok(_), e @ Err(_)) => e?,
            (e @ Err(_), Ok(_)) => e.map(|_| ())?,
            (Ok(written_bytes), Ok(_)) => {
                guard.inc_byte_count(written_bytes);
            }
        }

        Ok(())
    }

    async fn do_cleanup(&mut self) -> Result<()> {
        let _guard = self.stats.record_cleanup();

        self.temp_file_provider.cleanup().await
    }

    pub fn column_ids(&self) -> impl Iterator<Item = ColumnId> + '_ {
        self.indexed_targets
            .iter()
            .map(InvertedIndexTarget::column_id_value)
            .collect::<HashSet<_>>()
            .into_iter()
    }

    pub fn memory_usage(&self) -> usize {
        self.memory_usage.load(std::sync::atomic::Ordering::Relaxed)
    }
}

struct ResolvedJsonPathArray {
    leaf: ArrayRef,
    parents: Vec<ArrayRef>,
}

fn resolve_json_path_array(root: ArrayRef, path: &[String]) -> Option<ResolvedJsonPathArray> {
    let mut current = root;
    let mut parents = Vec::with_capacity(path.len());

    for segment in path {
        let struct_array = current.as_any().downcast_ref::<StructArray>()?;
        let field_index = struct_array
            .fields()
            .iter()
            .position(|field| field.name() == segment)?;
        parents.push(current.clone());
        current = struct_array.column(field_index).clone();
    }

    Some(ResolvedJsonPathArray {
        leaf: current,
        parents,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use api::v1::SemanticType;
    use datafusion_expr::{Expr as DfExpr, Operator, binary_expr, col, lit};
    use datatypes::arrow::array::{Int64Array, StringArray};
    use datatypes::arrow::buffer::{BooleanBuffer, NullBuffer};
    use datatypes::arrow::datatypes::{DataType, Field};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use datatypes::value::ValueRef;
    use datatypes::vectors::{UInt8Vector, UInt64Vector};
    use futures::future::BoxFuture;
    use mito_codec::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodecExt};
    use object_store::ObjectStore;
    use object_store::services::Memory;
    use puffin::puffin_manager::PuffinManager;
    use puffin::puffin_manager::cache::PuffinMetadataCache;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::region_request::PathType;
    use store_api::storage::RegionId;

    use super::*;
    use crate::access_layer::RegionFilePathFactory;
    use crate::cache::index::inverted_index::InvertedIndexCache;
    use crate::metrics::CACHE_BYTES;
    use crate::read::BatchColumn;
    use crate::sst::file::{RegionFileId, RegionIndexId};
    use crate::sst::index::inverted_index::applier::builder::InvertedIndexApplierBuilder;
    use crate::sst::index::puffin_manager::PuffinManagerFactory;

    fn string_path(path: &[&str]) -> Vec<String> {
        path.iter().map(|segment| (*segment).to_string()).collect()
    }

    #[test]
    fn test_resolve_json_path_array_nested_field() {
        let name: ArrayRef = Arc::new(StringArray::from(vec![Some("host-a"), Some("host-b")]));
        let host: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("name", DataType::Utf8, true)),
            name.clone(),
        )]));
        let root: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new(
                "host",
                DataType::Struct(vec![Field::new("name", DataType::Utf8, true)].into()),
                true,
            )),
            host,
        )]));

        let resolved = resolve_json_path_array(root, &string_path(&["host", "name"])).unwrap();

        assert_eq!(resolved.leaf.len(), 2);
        assert_eq!(resolved.parents.len(), 2);
        assert_eq!(
            resolved
                .leaf
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(1),
            "host-b"
        );
    }

    #[test]
    fn test_resolve_json_path_array_keeps_parent_nulls() {
        let value: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), Some(2)]));
        let root: ArrayRef = Arc::new(
            StructArray::try_new(
                vec![Field::new("value", DataType::Int64, true)].into(),
                vec![value],
                Some(NullBuffer::new(BooleanBuffer::from(vec![true, false]))),
            )
            .unwrap(),
        );

        let resolved = resolve_json_path_array(root, &string_path(&["value"])).unwrap();

        assert!(!resolved.leaf.is_null(1));
        assert!(resolved.parents.iter().any(|parent| parent.is_null(1)));
    }

    #[test]
    fn test_resolve_json_path_array_missing_field() {
        let root: ArrayRef = Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("value", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef,
        )]));

        assert!(resolve_json_path_array(root, &string_path(&["missing"])).is_none());
    }

    fn mock_object_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap().finish()
    }

    async fn new_intm_mgr(path: impl AsRef<str>) -> IntermediateManager {
        IntermediateManager::init_fs(path).await.unwrap()
    }

    fn mock_region_metadata() -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 2));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "tag_str",
                    ConcreteDataType::string_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "tag_i32",
                    ConcreteDataType::int32_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 3,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field_u64",
                    ConcreteDataType::uint64_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Field,
                column_id: 4,
            })
            .primary_key(vec![1, 2]);

        Arc::new(builder.build().unwrap())
    }

    fn new_batch(
        str_tag: impl AsRef<str>,
        i32_tag: impl Into<i32>,
        u64_field: impl IntoIterator<Item = u64>,
    ) -> Batch {
        let fields = vec![
            (0, SortField::new(ConcreteDataType::string_datatype())),
            (1, SortField::new(ConcreteDataType::int32_datatype())),
        ];
        let codec = DensePrimaryKeyCodec::with_fields(fields);
        let row: [ValueRef; 2] = [str_tag.as_ref().into(), i32_tag.into().into()];
        let primary_key = codec.encode(row.into_iter()).unwrap();

        let u64_field = BatchColumn {
            column_id: 4,
            data: Arc::new(UInt64Vector::from_iter_values(u64_field)),
        };
        let num_rows = u64_field.data.len();

        Batch::new(
            primary_key,
            Arc::new(UInt64Vector::from_iter_values(std::iter::repeat_n(
                0, num_rows,
            ))),
            Arc::new(UInt64Vector::from_iter_values(std::iter::repeat_n(
                0, num_rows,
            ))),
            Arc::new(UInt8Vector::from_iter_values(std::iter::repeat_n(
                1, num_rows,
            ))),
            vec![u64_field],
        )
        .unwrap()
    }

    async fn build_applier_factory(
        prefix: &str,
        rows: BTreeSet<(&'static str, i32, [u64; 2])>,
    ) -> impl Fn(DfExpr) -> BoxFuture<'static, Vec<usize>> {
        let (d, factory) = PuffinManagerFactory::new_for_test_async(prefix).await;
        let table_dir = "table0".to_string();
        let sst_file_id = FileId::random();
        let object_store = mock_object_store();
        let region_metadata = mock_region_metadata();
        let intm_mgr = new_intm_mgr(d.path().to_string_lossy()).await;
        let memory_threshold = None;
        let segment_row_count = 2;
        let indexed_column_ids = HashSet::from_iter([1, 2, 4]);
        let indexed_targets = indexed_column_ids
            .iter()
            .copied()
            .map(InvertedIndexTarget::column_id)
            .collect();

        let mut creator = InvertedIndexer::new(
            sst_file_id,
            &region_metadata,
            intm_mgr,
            memory_threshold,
            NonZeroUsize::new(segment_row_count).unwrap(),
            indexed_targets,
        );

        for (str_tag, i32_tag, u64_field) in &rows {
            let mut batch = new_batch(str_tag, *i32_tag, u64_field.iter().copied());
            creator.update(&mut batch).await.unwrap();
        }

        let puffin_manager = factory.build(
            object_store.clone(),
            RegionFilePathFactory::new(table_dir.clone(), PathType::Bare),
        );

        let sst_file_id = RegionFileId::new(region_metadata.region_id, sst_file_id);
        let index_id = RegionIndexId::new(sst_file_id, 0);
        let mut writer = puffin_manager.writer(&index_id).await.unwrap();
        let (row_count, _) = creator.finish(&mut writer).await.unwrap();
        assert_eq!(row_count, rows.len() * segment_row_count);
        writer.finish().await.unwrap();

        move |expr| {
            let _d = &d;
            let cache = Arc::new(InvertedIndexCache::new(10, 10, 100));
            let puffin_metadata_cache = Arc::new(PuffinMetadataCache::new(10, &CACHE_BYTES));
            let applier = InvertedIndexApplierBuilder::new(
                table_dir.clone(),
                PathType::Bare,
                object_store.clone(),
                &region_metadata,
                indexed_column_ids.clone(),
                factory.clone(),
            )
            .with_inverted_index_cache(Some(cache))
            .with_puffin_metadata_cache(Some(puffin_metadata_cache))
            .build(&[expr])
            .unwrap()
            .unwrap();
            let sst_metadata = Arc::new(region_metadata.clone());
            let plan = applier.plan_for_sst(&sst_metadata).unwrap().unwrap();
            Box::pin(async move {
                applier
                    .apply(index_id, None, &plan.index_applier, None)
                    .await
                    .unwrap()
                    .matched_segment_ids
                    .iter_ones()
                    .collect()
            })
        }
    }

    #[tokio::test]
    async fn test_create_and_query_get_key() {
        let rows = BTreeSet::from_iter([
            ("aaa", 1, [1, 2]),
            ("aaa", 2, [2, 3]),
            ("aaa", 3, [3, 4]),
            ("aab", 1, [4, 5]),
            ("aab", 2, [5, 6]),
            ("aab", 3, [6, 7]),
            ("abc", 1, [7, 8]),
            ("abc", 2, [8, 9]),
            ("abc", 3, [9, 10]),
        ]);

        let applier_factory = build_applier_factory("test_create_and_query_get_key_", rows).await;

        let expr = col("tag_str").eq(lit("aaa"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2]);

        let expr = col("tag_i32").eq(lit(2));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![1, 4, 7]);

        let expr = col("tag_str").eq(lit("aaa")).and(col("tag_i32").eq(lit(2)));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![1]);

        let expr = col("tag_str")
            .eq(lit("aaa"))
            .or(col("tag_str").eq(lit("abc")));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2, 6, 7, 8]);

        let expr = col("tag_str").in_list(vec![lit("aaa"), lit("abc")], false);
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2, 6, 7, 8]);

        let expr = col("field_u64").eq(lit(2u64));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1]);
    }

    #[tokio::test]
    async fn test_create_and_query_range() {
        let rows = BTreeSet::from_iter([
            ("aaa", 1, [1, 2]),
            ("aaa", 2, [2, 3]),
            ("aaa", 3, [3, 4]),
            ("aab", 1, [4, 5]),
            ("aab", 2, [5, 6]),
            ("aab", 3, [6, 7]),
            ("abc", 1, [7, 8]),
            ("abc", 2, [8, 9]),
            ("abc", 3, [9, 10]),
        ]);

        let applier_factory = build_applier_factory("test_create_and_query_range_", rows).await;

        let expr = col("tag_str").between(lit("aaa"), lit("aab"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2, 3, 4, 5]);

        let expr = col("tag_i32").between(lit(2), lit(3));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![1, 2, 4, 5, 7, 8]);

        let expr = col("tag_str").between(lit("aaa"), lit("aaa"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2]);

        let expr = col("tag_i32").between(lit(2), lit(2));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![1, 4, 7]);

        let expr = col("field_u64").between(lit(2u64), lit(5u64));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_create_and_query_comparison() {
        let rows = BTreeSet::from_iter([
            ("aaa", 1, [1, 2]),
            ("aaa", 2, [2, 3]),
            ("aaa", 3, [3, 4]),
            ("aab", 1, [4, 5]),
            ("aab", 2, [5, 6]),
            ("aab", 3, [6, 7]),
            ("abc", 1, [7, 8]),
            ("abc", 2, [8, 9]),
            ("abc", 3, [9, 10]),
        ]);

        let applier_factory =
            build_applier_factory("test_create_and_query_comparison_", rows).await;

        let expr = col("tag_str").lt(lit("aab"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2]);

        let expr = col("tag_i32").lt(lit(2));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 3, 6]);

        let expr = col("field_u64").lt(lit(2u64));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0]);

        let expr = col("tag_str").gt(lit("aab"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![6, 7, 8]);

        let expr = col("tag_i32").gt(lit(2));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![2, 5, 8]);

        let expr = col("field_u64").gt(lit(8u64));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![7, 8]);

        let expr = col("tag_str").lt_eq(lit("aab"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2, 3, 4, 5]);

        let expr = col("tag_i32").lt_eq(lit(2));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 3, 4, 6, 7]);

        let expr = col("field_u64").lt_eq(lit(2u64));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1]);

        let expr = col("tag_str").gt_eq(lit("aab"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![3, 4, 5, 6, 7, 8]);

        let expr = col("tag_i32").gt_eq(lit(2));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![1, 2, 4, 5, 7, 8]);

        let expr = col("field_u64").gt_eq(lit(8u64));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![6, 7, 8]);

        let expr = col("tag_str")
            .gt(lit("aaa"))
            .and(col("tag_str").lt(lit("abc")));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![3, 4, 5]);

        let expr = col("tag_i32").gt(lit(1)).and(col("tag_i32").lt(lit(3)));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![1, 4, 7]);

        let expr = col("field_u64")
            .gt(lit(2u64))
            .and(col("field_u64").lt(lit(9u64)));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![1, 2, 3, 4, 5, 6, 7]);
    }

    #[tokio::test]
    async fn test_create_and_query_regex() {
        let rows = BTreeSet::from_iter([
            ("aaa", 1, [1, 2]),
            ("aaa", 2, [2, 3]),
            ("aaa", 3, [3, 4]),
            ("aab", 1, [4, 5]),
            ("aab", 2, [5, 6]),
            ("aab", 3, [6, 7]),
            ("abc", 1, [7, 8]),
            ("abc", 2, [8, 9]),
            ("abc", 3, [9, 10]),
        ]);

        let applier_factory = build_applier_factory("test_create_and_query_regex_", rows).await;

        let expr = binary_expr(col("tag_str"), Operator::RegexMatch, lit(".*"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);

        let expr = binary_expr(col("tag_str"), Operator::RegexMatch, lit("a.*c"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![6, 7, 8]);

        let expr = binary_expr(col("tag_str"), Operator::RegexMatch, lit("a.*b$"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![3, 4, 5]);

        let expr = binary_expr(col("tag_str"), Operator::RegexMatch, lit("\\w"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2, 3, 4, 5, 6, 7, 8]);

        let expr = binary_expr(col("tag_str"), Operator::RegexMatch, lit("\\d"));
        let res = applier_factory(expr).await;
        assert!(res.is_empty());

        let expr = binary_expr(col("tag_str"), Operator::RegexMatch, lit("^aaa$"));
        let res = applier_factory(expr).await;
        assert_eq!(res, vec![0, 1, 2]);
    }
}
