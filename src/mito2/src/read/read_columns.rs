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

use std::collections::BTreeMap;

use datafusion_common::HashMap;
use snafu::OptionExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, NestedPath, ProjectionInput};

use crate::error::{InvalidRequestSnafu, Result};
use crate::read::scan_region::PredicateGroup;

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
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReadColumns {
    cols: Vec<ReadColumn>,
}

impl ReadColumns {
    pub fn from_column_ids<I>(column_ids: I) -> Self
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

    pub fn column_ids(&self) -> Vec<ColumnId> {
        self.column_ids_iter().collect()
    }

    pub fn columns(&self) -> &[ReadColumn] {
        &self.cols
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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

pub fn merge_read_cols(a: ReadColumns, b: ReadColumns) -> ReadColumns {
    let mut merged = BTreeMap::<u32, Vec<NestedPath>>::new();

    for col in a.cols.into_iter().chain(b.cols) {
        if let Some(nested_paths) = merged.get_mut(&col.column_id) {
            if nested_paths.is_empty() || col.nested_paths.is_empty() {
                *nested_paths = vec![];
            } else {
                merge_nested_paths(nested_paths, col.nested_paths);
            }
            continue;
        }

        merged.insert(col.column_id, normalize_nested_paths(col.nested_paths));
    }

    ReadColumns {
        cols: merged
            .into_iter()
            .map(|(column_id, nested_paths)| ReadColumn {
                column_id,
                nested_paths,
            })
            .collect(),
    }
}

fn normalize_nested_paths(nested_paths: Vec<NestedPath>) -> Vec<NestedPath> {
    let mut normalized = Vec::with_capacity(nested_paths.len());
    merge_nested_paths(&mut normalized, nested_paths);
    normalized
}

fn merge_nested_paths(merged: &mut Vec<NestedPath>, incoming: Vec<NestedPath>) {
    for path in incoming {
        if merged
            .iter()
            .any(|existing| path.starts_with(existing.as_slice()))
        {
            continue;
        }

        merged.retain(|existing| !existing.starts_with(path.as_slice()));
        merged.push(path);
    }
}

/// Build [`ReadColumns`] from [`ProjectionInput`].
pub fn read_columns_from_projection(
    _projection: &ProjectionInput,
    _metadata: &RegionMetadataRef,
) -> ReadColumns {
    todo!()
}

/// Build [`ReadColumns`] from [`ProjectionInput`].
pub fn read_columns_from_predicate(
    _predicate: &PredicateGroup,
    _metadata: &RegionMetadataRef,
) -> ReadColumns {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nested_path(parts: &[&str]) -> NestedPath {
        parts.iter().map(|part| (*part).to_string()).collect()
    }

    #[test]
    fn test_merge_read_cols_with_only_root() {
        let a = ReadColumns {
            cols: vec![ReadColumn::new(3, vec![]), ReadColumn::new(1, vec![])],
        };
        let b = ReadColumns {
            cols: vec![ReadColumn::new(2, vec![])],
        };

        let merged = merge_read_cols(a, b);

        assert_eq!(
            merged,
            ReadColumns {
                cols: vec![
                    ReadColumn::new(1, vec![]),
                    ReadColumn::new(2, vec![]),
                    ReadColumn::new(3, vec![]),
                ],
            }
        );
    }

    #[test]
    fn test_merge_read_cols_with_nested_paths() {
        let a = ReadColumns {
            cols: vec![ReadColumn::new(1, vec![nested_path(&["j", "a"])])],
        };
        let b = ReadColumns {
            cols: vec![ReadColumn::new(
                1,
                vec![nested_path(&["j", "b"]), nested_path(&["j", "c"])],
            )],
        };

        let merged = merge_read_cols(a, b);

        assert_eq!(
            merged,
            ReadColumns {
                cols: vec![ReadColumn::new(
                    1,
                    vec![
                        nested_path(&["j", "a"]),
                        nested_path(&["j", "b"]),
                        nested_path(&["j", "c"]),
                    ],
                )],
            }
        );
    }

    #[test]
    fn test_merge_read_cols_with_column_override() {
        let a = ReadColumns {
            cols: vec![
                ReadColumn::new(1, vec![nested_path(&["j", "a"])]),
                ReadColumn::new(2, vec![nested_path(&["k", "b"])]),
            ],
        };
        let b = ReadColumns {
            cols: vec![
                ReadColumn::new(1, vec![]),
                ReadColumn::new(2, vec![nested_path(&["k", "b", "c"])]),
            ],
        };

        let merged = merge_read_cols(a, b);

        assert_eq!(
            merged,
            ReadColumns {
                cols: vec![
                    ReadColumn::new(1, vec![]),
                    ReadColumn::new(2, vec![nested_path(&["k", "b"])])
                ],
            }
        );
    }

    #[test]
    fn test_merge_read_cols_dedups_redundant_nested_paths() {
        let a = ReadColumns {
            cols: vec![ReadColumn::new(
                1,
                vec![
                    nested_path(&["j", "a", "b"]),
                    nested_path(&["j", "a"]),
                    nested_path(&["j", "a", "b", "c"]),
                ],
            )],
        };
        let b = ReadColumns {
            cols: vec![ReadColumn::new(1, vec![nested_path(&["j", "a"])])],
        };

        let merged = merge_read_cols(a, b);

        assert_eq!(
            merged,
            ReadColumns {
                cols: vec![ReadColumn::new(1, vec![nested_path(&["j", "a"])])],
            }
        );
    }
}
