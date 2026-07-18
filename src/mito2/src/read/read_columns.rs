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
use std::mem;

use store_api::storage::{ColumnId, NestedPath};

/// Logical columns to read from a region.
///
/// Read columns describe which logical columns and nested fields should be read
/// from storage. Each read column is identified by its [`ColumnId`],
/// which represents the root column in the storage schema.
///
/// Nested fields under the column are specified by [`NestedPath`] entries.
/// Each path is relative to the root column.
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
/// ReadColumn::new(9).with_nested_projection(NestedPathSet::new(vec![
///         vec!["a".to_string()],
///         vec!["b".to_string(), "c".to_string()],
///     ]))
/// ```
///
/// If [`ColumnProjection::Full`] is used, the whole column will be read.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct ReadColumns {
    pub cols: Vec<ReadColumn>,
}

impl ReadColumns {
    pub fn from_deduped_column_ids<I>(column_ids: I) -> Self
    where
        I: IntoIterator<Item = ColumnId>,
    {
        let cols = column_ids.into_iter().map(ReadColumn::new).collect();
        ReadColumns { cols }
    }

    pub fn is_empty(&self) -> bool {
        self.cols.is_empty()
    }

    pub fn column_ids_iter(&self) -> impl Iterator<Item = ColumnId> + '_ {
        self.cols.iter().map(|column| column.column_id)
    }

    pub fn column_ids(&self) -> Vec<ColumnId> {
        self.column_ids_iter().collect()
    }

    pub fn columns(&self) -> &[ReadColumn] {
        &self.cols
    }

    pub fn estimated_size(&self) -> usize {
        self.cols.capacity() * mem::size_of::<ReadColumn>()
            + self
                .cols
                .iter()
                .map(ReadColumn::estimated_size)
                .sum::<usize>()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ReadColumn {
    pub column_id: ColumnId,
    pub projection: ColumnProjection,
}

/// Projection requirement for a single read column.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ColumnProjection {
    /// Read the whole root column.
    Full,
    /// Read selected nested fields under the root column.
    Nested(NestedProjection),
}

/// Nested projection requirement for a single read column.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NestedProjection {
    /// Nested field paths under the root column.
    pub paths: NestedPathSet,
    /// How to handle requested paths missing from a parquet file schema.
    pub missing_path_policy: MissingPathPolicy,
}

/// Policy for handling requested nested paths missing from a parquet file schema.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MissingPathPolicy {
    /// Only read parquet leaves whose paths start with the requested nested paths.
    #[default]
    PrefixOnly,
    /// If a requested path is missing, read the nearest variant parent.
    FallbackToNearestVariantParent,
}

impl MissingPathPolicy {
    pub fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::FallbackToNearestVariantParent, _)
            | (_, Self::FallbackToNearestVariantParent) => Self::FallbackToNearestVariantParent,
            (Self::PrefixOnly, Self::PrefixOnly) => Self::PrefixOnly,
        }
    }
}

impl From<NestedReadStrategy> for MissingPathPolicy {
    fn from(strategy: NestedReadStrategy) -> Self {
        match strategy {
            NestedReadStrategy::Prefix => Self::PrefixOnly,
            NestedReadStrategy::FallbackToNearestVariantParent => {
                Self::FallbackToNearestVariantParent
            }
        }
    }
}

impl From<MissingPathPolicy> for NestedReadStrategy {
    fn from(policy: MissingPathPolicy) -> Self {
        match policy {
            MissingPathPolicy::PrefixOnly => Self::Prefix,
            MissingPathPolicy::FallbackToNearestVariantParent => {
                Self::FallbackToNearestVariantParent
            }
        }
    }
}

/// A normalized set of nested field paths.
///
/// The set keeps only the shortest prefix needed to cover requested paths. For
/// example, `["j", "a"]` covers `["j", "a", "b"]`, so only `["j", "a"]`
/// is retained.
#[derive(Debug, Default, Clone, PartialEq, Eq, Hash)]
pub struct NestedPathSet {
    paths: Vec<NestedPath>,
}

impl NestedPathSet {
    /// Creates a normalized path set from raw nested paths.
    pub fn new(paths: Vec<NestedPath>) -> Self {
        let mut set = Self {
            paths: Vec::with_capacity(paths.len()),
        };
        set.merge(paths);
        set
    }

    /// Inserts a nested path and keeps the set normalized.
    pub fn insert(&mut self, path: NestedPath) {
        if self
            .paths
            .iter()
            .any(|existing| path.starts_with(existing.as_slice()))
        {
            return;
        }

        self.paths
            .retain(|existing| !existing.starts_with(path.as_slice()));
        self.paths.push(path);
    }

    /// Merges raw nested paths into this set.
    pub fn merge(&mut self, paths: Vec<NestedPath>) {
        for path in paths {
            self.insert(path);
        }
    }

    /// Returns the normalized paths.
    pub fn paths(&self) -> &[NestedPath] {
        &self.paths
    }

    /// Consumes the set and returns the normalized paths.
    pub fn into_vec(self) -> Vec<NestedPath> {
        self.paths
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NestedReadStrategy {
    /// Read parquet leaves whose paths start with the requested nested paths.
    ///
    /// For example, requesting `j.a` may read `j.a.b` and `j.a.c` from parquet.
    /// If no leaf matches the requested prefix, the path stays missing.
    #[default]
    Prefix,
    /// If a requested path is missing, read the nearest variant parent.
    ///
    /// Useful for JSON schema evolution, e.g. read `j.a` when `j.a.b` is
    /// requested but only `j.a` exists as a variant value.
    FallbackToNearestVariantParent,
}

impl NestedReadStrategy {
    pub fn merge(self, other: Self) -> Self {
        match (self, other) {
            (Self::FallbackToNearestVariantParent, _)
            | (_, Self::FallbackToNearestVariantParent) => Self::FallbackToNearestVariantParent,
            (Self::Prefix, Self::Prefix) => Self::Prefix,
        }
    }
}

impl ReadColumn {
    pub fn new(column_id: ColumnId) -> Self {
        Self {
            column_id,
            projection: ColumnProjection::Full,
        }
    }

    pub fn with_nested_projection(mut self, paths: NestedPathSet) -> Self {
        self.projection = ColumnProjection::Nested(NestedProjection {
            paths,
            missing_path_policy: MissingPathPolicy::PrefixOnly,
        });
        self
    }

    pub fn with_missing_path_policy(mut self, missing_path_policy: MissingPathPolicy) -> Self {
        if let ColumnProjection::Nested(nested) = &mut self.projection {
            nested.missing_path_policy = missing_path_policy;
        }
        self
    }

    pub fn nested_paths(&self) -> &[NestedPath] {
        match &self.projection {
            ColumnProjection::Full => &[],
            ColumnProjection::Nested(nested) => nested.paths.paths(),
        }
    }

    pub fn nested_path_read_strategy(&self) -> NestedReadStrategy {
        match &self.projection {
            ColumnProjection::Full => NestedReadStrategy::Prefix,
            ColumnProjection::Nested(nested) => nested.missing_path_policy.into(),
        }
    }

    pub fn with_nested_path_read_strategy(
        mut self,
        nested_path_read_strategy: NestedReadStrategy,
    ) -> Self {
        if let ColumnProjection::Nested(nested) = &mut self.projection {
            nested.missing_path_policy = nested_path_read_strategy.into();
        }
        self
    }

    pub fn estimated_size(&self) -> usize {
        mem::size_of::<ColumnId>()
            + match &self.projection {
                ColumnProjection::Full => 0,
                ColumnProjection::Nested(nested) => {
                    nested.paths.paths.capacity() * mem::size_of::<NestedPath>()
                        + nested
                            .paths
                            .paths
                            .iter()
                            .map(|path| {
                                path.capacity() * mem::size_of::<String>()
                                    + path.iter().map(|node| node.capacity()).sum::<usize>()
                            })
                            .sum::<usize>()
                }
            }
    }
}

pub fn merge(a: ReadColumns, b: ReadColumns) -> ReadColumns {
    let mut merged = BTreeMap::<ColumnId, ColumnProjection>::new();

    for col in a.cols.into_iter().chain(b.cols) {
        if let Some(projection) = merged.get_mut(&col.column_id) {
            merge_column_projection(projection, col.projection);
            continue;
        }

        merged.insert(col.column_id, normalize_column_projection(col.projection));
    }

    ReadColumns {
        cols: merged
            .into_iter()
            .map(|(column_id, projection)| ReadColumn {
                column_id,
                projection,
            })
            .collect(),
    }
}

fn normalize_column_projection(projection: ColumnProjection) -> ColumnProjection {
    match projection {
        ColumnProjection::Full => ColumnProjection::Full,
        ColumnProjection::Nested(mut nested) => {
            nested.paths = NestedPathSet::new(nested.paths.into_vec());
            ColumnProjection::Nested(nested)
        }
    }
}

fn merge_column_projection(merged: &mut ColumnProjection, incoming: ColumnProjection) {
    match (&mut *merged, incoming) {
        (ColumnProjection::Full, _) | (_, ColumnProjection::Full) => {
            *merged = ColumnProjection::Full;
        }
        (ColumnProjection::Nested(merged), ColumnProjection::Nested(incoming)) => {
            merged.paths.merge(incoming.paths.into_vec());
            merged.missing_path_policy = merged
                .missing_path_policy
                .merge(incoming.missing_path_policy);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nested_path_set_parent_path_covers_children() {
        let set = NestedPathSet::new(vec![
            nested_path(&["a", "b"]),
            nested_path(&["a"]),
            nested_path(&["a", "c"]),
        ]);

        assert_eq!(set.paths(), &[nested_path(&["a"])]);
    }

    #[test]
    fn test_nested_path_set_skips_child_path_if_parent_exists() {
        let mut set = NestedPathSet::new(vec![nested_path(&["a"])]);

        set.insert(nested_path(&["a", "b"]));
        set.insert(nested_path(&["b"]));

        assert_eq!(set.paths(), &[nested_path(&["a"]), nested_path(&["b"])]);
    }

    fn nested_path(parts: &[&str]) -> NestedPath {
        parts.iter().map(|part| (*part).to_string()).collect()
    }
}
