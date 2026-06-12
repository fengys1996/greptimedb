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

mod between;
mod comparison;
mod eq_list;
mod in_list;
mod regex_match;

use std::collections::{BTreeMap, HashSet};

use arrow_schema::extension::ExtensionType;
use common_function::scalars::json::json_get::JsonGetWithType;
use common_telemetry::warn;
use datafusion_common::ScalarValue;
use datafusion_expr::{BinaryExpr, Expr, Operator};
use datatypes::data_type::ConcreteDataType;
use datatypes::extension::json::JsonExtensionType;
use datatypes::value::Value;
use index::inverted_index::search::predicate::Predicate;
use index::target::IndexTarget;
use mito_codec::index::IndexValueCodec;
use mito_codec::row_converter::SortField;
use object_store::ObjectStore;
use puffin::puffin_manager::cache::PuffinMetadataCacheRef;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadata;
use store_api::region_request::PathType;
use store_api::storage::ColumnId;

use crate::cache::file_cache::FileCacheRef;
use crate::cache::index::inverted_index::InvertedIndexCacheRef;
use crate::error::{ColumnNotFoundSnafu, ConvertValueSnafu, EncodeSnafu, Result};
use crate::sst::index::inverted_index::applier::InvertedIndexApplier;
use crate::sst::index::puffin_manager::PuffinManagerFactory;

/// Constructs an [`InvertedIndexApplier`] which applies predicates to SST files during scan.
pub(crate) struct InvertedIndexApplierBuilder<'a> {
    /// Directory of the table, required argument for constructing [`InvertedIndexApplier`].
    table_dir: String,

    /// Path type for generating file paths.
    path_type: PathType,

    /// Object store, required argument for constructing [`InvertedIndexApplier`].
    object_store: ObjectStore,

    /// File cache, required argument for constructing [`InvertedIndexApplier`].
    file_cache: Option<FileCacheRef>,

    /// Metadata of the region, used to get metadata like column type.
    metadata: &'a RegionMetadata,

    /// Column ids of the columns that are indexed.
    indexed_column_ids: HashSet<ColumnId>,

    /// Column ids ignored by inverted index config.
    ignored_column_ids: HashSet<ColumnId>,

    /// Stores predicates during traversal on the Expr tree.
    output: BTreeMap<IndexTarget, Vec<Predicate>>,

    /// The puffin manager factory.
    puffin_manager_factory: PuffinManagerFactory,

    /// Cache for inverted index.
    inverted_index_cache: Option<InvertedIndexCacheRef>,

    /// Cache for puffin metadata.
    puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
}

impl<'a> InvertedIndexApplierBuilder<'a> {
    /// Creates a new [`InvertedIndexApplierBuilder`].
    pub fn new(
        table_dir: String,
        path_type: PathType,
        object_store: ObjectStore,
        metadata: &'a RegionMetadata,
        indexed_column_ids: HashSet<ColumnId>,
        puffin_manager_factory: PuffinManagerFactory,
    ) -> Self {
        Self {
            table_dir,
            path_type,
            object_store,
            metadata,
            indexed_column_ids,
            ignored_column_ids: HashSet::default(),
            output: BTreeMap::default(),
            puffin_manager_factory,
            file_cache: None,
            inverted_index_cache: None,
            puffin_metadata_cache: None,
        }
    }

    /// Sets the file cache.
    pub fn with_file_cache(mut self, file_cache: Option<FileCacheRef>) -> Self {
        self.file_cache = file_cache;
        self
    }

    /// Sets the puffin metadata cache.
    pub fn with_puffin_metadata_cache(
        mut self,
        puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    ) -> Self {
        self.puffin_metadata_cache = puffin_metadata_cache;
        self
    }

    /// Sets the inverted index cache.
    pub fn with_inverted_index_cache(
        mut self,
        inverted_index_cache: Option<InvertedIndexCacheRef>,
    ) -> Self {
        self.inverted_index_cache = inverted_index_cache;
        self
    }

    /// Sets column ids ignored by inverted index config.
    pub fn with_ignored_column_ids(mut self, ignored_column_ids: HashSet<ColumnId>) -> Self {
        self.ignored_column_ids = ignored_column_ids;
        self
    }

    /// Consumes the builder to construct an [`InvertedIndexApplier`], optionally returned based on
    /// the expressions provided. If no predicates match, returns `None`.
    pub fn build(mut self, exprs: &[Expr]) -> Result<Option<InvertedIndexApplier>> {
        for expr in exprs {
            self.traverse_and_collect(expr);
        }

        if self.output.is_empty() {
            return Ok(None);
        }

        let expected_predicate_column_types = self.expected_predicate_column_types();

        Ok(Some(
            InvertedIndexApplier::new(
                self.table_dir,
                self.path_type,
                self.object_store,
                self.puffin_manager_factory,
                self.output,
                expected_predicate_column_types,
            )?
            .with_file_cache(self.file_cache)
            .with_puffin_metadata_cache(self.puffin_metadata_cache)
            .with_index_cache(self.inverted_index_cache),
        ))
    }

    /// Returns `(column_id, data_type)` pairs for predicate columns
    /// collected in `self.output`.
    ///
    /// The data types are resolved from the latest region manifest. Columns
    /// that no longer exist in the latest metadata are skipped.
    fn expected_predicate_column_types(&self) -> BTreeMap<IndexTarget, ConcreteDataType> {
        self.output
            .keys()
            .filter_map(|target| {
                let col = self.metadata.column_by_id(target.column_id())?;
                Some((target.clone(), col.column_schema.data_type.clone()))
            })
            .collect()
    }

    /// Recursively traverses expressions to collect predicates.
    /// Results are stored in `self.output`.
    fn traverse_and_collect(&mut self, expr: &Expr) {
        let res = match expr {
            Expr::Between(between) => self.collect_between(between),

            Expr::InList(in_list) => self.collect_inlist(in_list),
            Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
                Operator::And => {
                    self.traverse_and_collect(left);
                    self.traverse_and_collect(right);
                    Ok(())
                }
                Operator::Or => self.collect_or_eq_list(left, right),
                Operator::Eq => self.collect_eq(left, right),
                Operator::Lt | Operator::LtEq | Operator::Gt | Operator::GtEq => {
                    self.collect_comparison_expr(left, op, right)
                }
                Operator::RegexMatch => self.collect_regex_match(left, right),
                _ => Ok(()),
            },

            // TODO(zhongzc): support more expressions, e.g. IsNull, IsNotNull, ...
            _ => Ok(()),
        };

        if let Err(err) = res {
            warn!(err; "Failed to collect predicates, ignore it. expr: {expr}");
        }
    }

    /// Helper function to add a predicate to the output.
    fn add_predicate(&mut self, column_id: ColumnId, predicate: Predicate) {
        self.add_target_predicate(IndexTarget::ColumnId(column_id), predicate);
    }

    /// Helper function to add a predicate to a target.
    fn add_target_predicate(&mut self, target: IndexTarget, predicate: Predicate) {
        self.output.entry(target).or_default().push(predicate);
    }

    /// Helper function to get the indexed target and value type from a predicate expression.
    fn indexed_expr_target(&self, expr: &Expr) -> Result<Option<(IndexTarget, ConcreteDataType)>> {
        match expr {
            Expr::Column(column) => self
                .column_id_and_type(&column.name)
                .map(|column| column.map(|(id, ty)| (IndexTarget::ColumnId(id), ty))),
            Expr::ScalarFunction(func)
                if func.func.name().eq_ignore_ascii_case(JsonGetWithType::NAME) =>
            {
                self.json_get_target(func.args.as_slice())
            }
            _ => Ok(None),
        }
    }

    fn json_get_target(&self, args: &[Expr]) -> Result<Option<(IndexTarget, ConcreteDataType)>> {
        if args.len() != 2 && args.len() != 3 {
            return Ok(None);
        }

        let Some(root_column_name) = Self::column_name(&args[0]) else {
            return Ok(None);
        };
        let Some(path) = Self::literal_string(&args[1]) else {
            return Ok(None);
        };
        let Some(path) = parse_json_path(path) else {
            return Ok(None);
        };

        let column =
            self.metadata
                .column_by_name(root_column_name)
                .context(ColumnNotFoundSnafu {
                    column: root_column_name,
                })?;
        if self.ignored_column_ids.contains(&column.column_id) {
            return Ok(None);
        }

        let json_extension = match column.column_schema.extension_type::<JsonExtensionType>() {
            Ok(json_extension) => json_extension,
            Err(err) => {
                warn!(
                    err;
                    "Failed to parse JSON extension when building inverted index applier, column: {}",
                    root_column_name
                );
                return Ok(None);
            }
        };
        let Some(json_extension) = json_extension else {
            return Ok(None);
        };
        let settings = json_extension
            .metadata()
            .json_settings
            .clone()
            .unwrap_or_default();
        let Some(hint) = settings
            .type_hints
            .iter()
            .find(|hint| hint.inverted_index && hint.path == path)
        else {
            return Ok(None);
        };

        Ok(Some((
            IndexTarget::ColumnNestedPath {
                column_id: column.column_id,
                path,
            },
            hint.data_type.clone(),
        )))
    }

    /// Helper function to get the column id and the column type of a column.
    /// Returns `None` if the column is not a tag column or if the column is ignored.
    fn column_id_and_type(
        &self,
        column_name: &str,
    ) -> Result<Option<(ColumnId, ConcreteDataType)>> {
        let column = self
            .metadata
            .column_by_name(column_name)
            .context(ColumnNotFoundSnafu {
                column: column_name,
            })?;

        if !self.indexed_column_ids.contains(&column.column_id) {
            return Ok(None);
        }

        Ok(Some((
            column.column_id,
            column.column_schema.data_type.clone(),
        )))
    }

    /// Helper function to get a non-null literal.
    fn nonnull_lit(expr: &Expr) -> Option<&ScalarValue> {
        match expr {
            Expr::Literal(lit, _) if !lit.is_null() => Some(lit),
            _ => None,
        }
    }

    /// Helper function to get a string literal.
    fn literal_string(expr: &Expr) -> Option<&str> {
        match expr {
            Expr::Literal(lit, _) => lit.try_as_str().flatten(),
            _ => None,
        }
    }

    /// Helper function to get the column name of a column expression.
    fn column_name(expr: &Expr) -> Option<&str> {
        match expr {
            Expr::Column(column) => Some(&column.name),
            _ => None,
        }
    }

    /// Helper function to encode a literal into bytes.
    fn encode_lit(lit: &ScalarValue, data_type: ConcreteDataType) -> Result<Vec<u8>> {
        let value = Value::try_from(lit.clone()).context(ConvertValueSnafu)?;
        let mut bytes = vec![];
        let field = SortField::new(data_type);
        IndexValueCodec::encode_nonnull_value(value.as_value_ref(), &field, &mut bytes)
            .context(EncodeSnafu)?;
        Ok(bytes)
    }
}

fn parse_json_path(path: &str) -> Option<Vec<String>> {
    let path = path.split('.').map(ToString::to_string).collect::<Vec<_>>();
    (!path.is_empty() && path.iter().all(|segment| !segment.is_empty())).then_some(path)
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashSet};
    use std::sync::Arc;

    use api::v1::SemanticType;
    use common_function::scalars::json::json_get::JsonGetWithType;
    use common_function::scalars::udf::create_udf;
    use datafusion_common::Column;
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::{Between, Literal};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::extension::json::{JsonExtensionType, JsonMetadata};
    use datatypes::json::{JsonSettings, JsonTypeHint};
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema};
    use datatypes::types::json_type::{JsonNativeType, JsonObjectType};
    use index::inverted_index::search::predicate::{
        Bound, InListPredicate, Range, RangePredicate, RegexMatchPredicate,
    };
    use object_store::ObjectStore;
    use object_store::services::Memory;
    use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;

    pub(crate) fn test_region_metadata() -> RegionMetadata {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1234, 5678));
        let settings = JsonSettings::new(vec![
            JsonTypeHint {
                path: vec!["host".to_string(), "name".to_string()],
                data_type: ConcreteDataType::string_datatype(),
                nullable: true,
                default_constraint: None,
                inverted_index: true,
            },
            JsonTypeHint {
                path: vec!["host".to_string(), "region".to_string()],
                data_type: ConcreteDataType::string_datatype(),
                nullable: true,
                default_constraint: Some(ColumnDefaultConstraint::null_value()),
                inverted_index: false,
            },
        ]);
        let json_type = JsonNativeType::Object(JsonObjectType::from([(
            "host".to_string(),
            JsonNativeType::Object(JsonObjectType::from([
                ("name".to_string(), JsonNativeType::String),
                ("region".to_string(), JsonNativeType::String),
            ])),
        )]));
        let mut json_column = ColumnSchema::new("attrs", ConcreteDataType::json2(json_type), true);
        json_column
            .with_extension_type(&JsonExtensionType::new(Arc::new(JsonMetadata {
                json_settings: Some(settings),
            })))
            .unwrap();

        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("a", ConcreteDataType::string_datatype(), false),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("b", ConcreteDataType::int64_datatype(), false),
                semantic_type: SemanticType::Tag,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("c", ConcreteDataType::string_datatype(), false),
                semantic_type: SemanticType::Field,
                column_id: 3,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "d",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 4,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: json_column,
                semantic_type: SemanticType::Field,
                column_id: 5,
            })
            .primary_key(vec![1, 2]);
        builder.build().unwrap()
    }

    pub(crate) fn test_object_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap().finish()
    }

    pub(crate) fn tag_column() -> Expr {
        Expr::Column(Column::from_name("a"))
    }

    pub(crate) fn tag_column2() -> Expr {
        Expr::Column(Column::from_name("b"))
    }

    pub(crate) fn field_column() -> Expr {
        Expr::Column(Column::from_name("c"))
    }

    pub(crate) fn nonexistent_column() -> Expr {
        Expr::Column(Column::from_name("nonexistence"))
    }

    pub(crate) fn string_lit(s: impl Into<String>) -> Expr {
        s.into().lit()
    }

    pub(crate) fn int64_lit(i: impl Into<i64>) -> Expr {
        i.into().lit()
    }

    pub(crate) fn json_get_expr(path: &str) -> Expr {
        Expr::ScalarFunction(ScalarFunction::new_udf(
            Arc::new(create_udf(Arc::new(JsonGetWithType::default()))),
            vec![Expr::Column(Column::from_name("attrs")), string_lit(path)],
        ))
    }

    pub(crate) fn encoded_string(s: impl Into<String>) -> Vec<u8> {
        let mut bytes = vec![];
        IndexValueCodec::encode_nonnull_value(
            Value::from(s.into()).as_value_ref(),
            &SortField::new(ConcreteDataType::string_datatype()),
            &mut bytes,
        )
        .unwrap();
        bytes
    }

    pub(crate) fn encoded_int64(s: impl Into<i64>) -> Vec<u8> {
        let mut bytes = vec![];
        IndexValueCodec::encode_nonnull_value(
            Value::from(s.into()).as_value_ref(),
            &SortField::new(ConcreteDataType::int64_datatype()),
            &mut bytes,
        )
        .unwrap();
        bytes
    }

    #[test]
    fn test_collect_and_basic() {
        let (_d, factory) = PuffinManagerFactory::new_for_test_block("test_collect_and_basic_");

        let metadata = test_region_metadata();
        let mut builder = InvertedIndexApplierBuilder::new(
            "test".to_string(),
            PathType::Bare,
            test_object_store(),
            &metadata,
            HashSet::from_iter([1, 2, 3]),
            factory,
        );

        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(tag_column()),
                op: Operator::RegexMatch,
                right: Box::new(string_lit("bar")),
            })),
            op: Operator::And,
            right: Box::new(Expr::Between(Between {
                expr: Box::new(tag_column2()),
                negated: false,
                low: Box::new(int64_lit(123)),
                high: Box::new(int64_lit(456)),
            })),
        });

        builder.traverse_and_collect(&expr);
        let predicates = builder.output.get(&IndexTarget::ColumnId(1)).unwrap();
        assert_eq!(predicates.len(), 1);
        assert_eq!(
            predicates[0],
            Predicate::RegexMatch(RegexMatchPredicate {
                pattern: "bar".to_string()
            })
        );
        let predicates = builder.output.get(&IndexTarget::ColumnId(2)).unwrap();
        assert_eq!(predicates.len(), 1);
        assert_eq!(
            predicates[0],
            Predicate::Range(RangePredicate {
                range: Range {
                    lower: Some(Bound {
                        inclusive: true,
                        value: encoded_int64(123),
                    }),
                    upper: Some(Bound {
                        inclusive: true,
                        value: encoded_int64(456),
                    }),
                }
            })
        );
    }

    #[test]
    fn test_collect_json_get_eq() {
        let (_d, factory) = PuffinManagerFactory::new_for_test_block("test_collect_json_get_eq_");

        let metadata = test_region_metadata();
        let mut builder = InvertedIndexApplierBuilder::new(
            "test".to_string(),
            PathType::Bare,
            test_object_store(),
            &metadata,
            HashSet::from_iter([1, 2, 3]),
            factory,
        );

        builder
            .collect_eq(&json_get_expr("host.name"), &string_lit("greptime"))
            .unwrap();

        let target = IndexTarget::ColumnNestedPath {
            column_id: 5,
            path: vec!["host".to_string(), "name".to_string()],
        };
        let predicates = builder.output.get(&target).unwrap();
        assert_eq!(predicates.len(), 1);
        assert_eq!(
            predicates[0],
            Predicate::InList(InListPredicate {
                list: BTreeSet::from_iter([encoded_string("greptime")])
            })
        );
    }

    #[test]
    fn test_collect_json_get_ignores_non_indexed_hint() {
        let (_d, factory) =
            PuffinManagerFactory::new_for_test_block("test_collect_json_get_ignores_hint_");

        let metadata = test_region_metadata();
        let mut builder = InvertedIndexApplierBuilder::new(
            "test".to_string(),
            PathType::Bare,
            test_object_store(),
            &metadata,
            HashSet::from_iter([1, 2, 3]),
            factory,
        );

        builder
            .collect_eq(&json_get_expr("host.region"), &string_lit("us-west"))
            .unwrap();
        builder
            .collect_eq(&json_get_expr("host.missing"), &string_lit("us-west"))
            .unwrap();

        assert!(builder.output.is_empty());
    }
}
