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

use datafusion_common::HashMap;
use snafu::OptionExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, NestedPath};

use crate::error::{InvalidRequestSnafu, Result};

/// Logical columns to read from a region.
///
/// Read columns describe which logical columns and nested fields should be read
/// from storage. Each read column is identified by its [`ColumnId`],
/// which represents the root column in the storage schema.
///
/// Nested fields under the column are specified by [`NestedPath`] entries.
/// Each path includes the root column name as its first element.
///
/// For example, assume column id `9` corresponds to a root column named `j`
/// with nested fields:
///
/// ```text
/// j
/// ├── a
/// └── b
///     └── c
/// ```
///
/// The following SQL:
///
/// SELECT j.a, j.b.c FROM t
///
/// may produce read columns like:
///
/// ```text
/// ReadColumn {
///     column_id: 9,
///     nested_paths: [
///         ["j", "a"],
///         ["j", "b", "c"],
///     ]
/// }
/// ```
///
/// If `nested_paths` is empty, the whole column will be read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadColumns {
    cols: Vec<ReadColumn>,
}

impl ReadColumns {
    pub fn with_column_ids<I>(column_ids: I) -> Self
    where
        I: IntoIterator<Item = u32>,
    {
        let cols = column_ids
            .into_iter()
            .map(|col_id| ReadColumn::new(col_id, vec![]))
            .collect();
        ReadColumns { cols }
    }

    pub fn is_empty(&self) -> bool {
        self.cols.is_empty()
    }

    pub fn column_ids_iter(&self) -> impl Iterator<Item = ColumnId> + '_ {
        self.cols.iter().map(|column| column.column_id())
    }

    pub fn columns(&self) -> &[ReadColumn] {
        &self.cols
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadColumn {
    column_id: ColumnId,
    /// Nested filed paths under this column.
    /// Empty means reading the whole column.
    nested_paths: Vec<NestedPath>,
}

impl ReadColumn {
    pub fn new(column_id: ColumnId, nested_paths: Vec<NestedPath>) -> Self {
        Self {
            column_id,
            nested_paths,
        }
    }

    pub fn column_id(&self) -> ColumnId {
        self.column_id
    }

    pub fn nested_paths(&self) -> &[NestedPath] {
        &self.nested_paths
    }
}

/// Builds the final read columns.
///
/// `read_column_ids` determines which root columns to read and in what order.
/// Nested paths are attached to matching columns by column name.
pub fn build_read_columns(
    metadata: &RegionMetadataRef,
    nested_paths: &[NestedPath],
    read_col_ids: &[ColumnId],
) -> Result<ReadColumns> {
    let mut paths_by_col: HashMap<String, Vec<NestedPath>> = HashMap::new();
    for path in nested_paths {
        let Some((root_name, _)) = path.split_first() else {
            continue;
        };
        paths_by_col
            .entry(root_name.clone())
            .or_default()
            .push(path.clone());
    }

    let mut cols = Vec::with_capacity(read_col_ids.len());
    for col_id in read_col_ids {
        let column_id = *col_id;

        let col = metadata
            .column_by_id(column_id)
            .with_context(|| InvalidRequestSnafu {
                region_id: metadata.region_id,
                reason: format!("read column id {} does not exist in metadata", column_id),
            })?;

        let nested_paths = paths_by_col
            .get(&col.column_schema.name)
            .cloned()
            .unwrap_or_default();

        cols.push(ReadColumn {
            column_id,
            nested_paths,
        });
    }

    Ok(ReadColumns { cols })
}
