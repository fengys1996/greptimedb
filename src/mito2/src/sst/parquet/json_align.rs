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

use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion_common::cast_column;
use datafusion_common::format::DEFAULT_CAST_OPTIONS;
use datatypes::arrow::array::{ArrayRef, new_null_array};
use datatypes::arrow::datatypes::{DataType, Field, FieldRef, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::extension::json::{JsonMetadata, is_json2_extension_type};
use datatypes::json::JsonSettings;
use datatypes::vectors::json::array::JsonArray;
use datatypes::vectors::json::json2_physical_data_type;
use futures::Stream;
use futures::stream::BoxStream;
use snafu::{ResultExt, ensure};

use crate::error::{
    CastColumnSnafu, DataTypeMismatchSnafu, NewRecordBatchSnafu, Result, UnexpectedSnafu,
};
use crate::sst::parquet::Json2TargetLayout;

pub(crate) type ProjectedRecordBatchStream = BoxStream<'static, Result<RecordBatch>>;

/// Specifies how JSON columns in a record batch are aligned.
///
/// Both modes use the output schema to restore missing root columns with null
/// arrays. Schema alignment produces logical query fields; rewriting produces
/// specified physical layouts, typically for compaction.
#[derive(Debug)]
pub(crate) enum JsonAlignTarget {
    /// Aligns columns to the logical fields in the output schema.
    AlignToSchema,
    /// Rewrites specified JSON columns to their target physical layouts.
    Rewrite {
        /// Target layouts keyed by root column name, not nested field path.
        columns: HashMap<String, Json2TargetLayout>,
    },
}

/// Logical JSON settings and the physical layout to use when rewriting a column.
#[derive(Debug)]
struct Json2LayoutRewriteSettings {
    /// Logical settings applied when re-encoding JSON values.
    logical_settings: JsonSettings,
    /// Settings defining the target physical layout.
    target_layout: JsonSettings,
}

impl TryFrom<&Json2TargetLayout> for Json2LayoutRewriteSettings {
    type Error = crate::error::Error;

    fn try_from(layout: &Json2TargetLayout) -> Result<Self> {
        let metadata =
            serde_json::from_str::<JsonMetadata>(&layout.extension_metadata).map_err(|e| {
                UnexpectedSnafu {
                    reason: format!("invalid JSON2 extension metadata: {e}"),
                }
                .build()
            })?;
        Ok(Self {
            logical_settings: metadata.into_json_settings(),
            target_layout: layout.target_layout.clone(),
        })
    }
}

/// Aligns projected batches to the expected output schema for nested projections.
///
/// Background
/// ----------
/// Nested projection may ask parquet to read leaves under a root column. If none
/// of the requested leaves exists in the current parquet file, parquet decoding
/// omits the whole root from the physical [`RecordBatch`].
///
/// In addition, after nested-path filtering, returned struct arrays may contain
/// only a subset of fields. The current output schema is not pruned by nested
/// paths, so physical struct fields can be a subset of the expected struct
/// fields, and their nested schema can differ from the expected output schema.
///
/// To keep projected batches schema-consistent before entering upper readers:
/// - Root-column presence alignment restores missing projected root columns by
///   inserting root-level null arrays.
/// - Nested struct alignment aligns struct arrays to the expected nested field
///   layout.
#[derive(derive_more::Debug)]
pub struct JsonSchemaAligner<S> {
    #[debug(skip)]
    inner: S,
    /// Output schema expected by the upper reader.
    output_schema: SchemaRef,
    /// Whether each projected root exists in the physical batch returned by
    /// parquet.
    projected_root_presence: Vec<bool>,
    /// Number of columns expected from the physical batch returned by parquet.
    expected_input_col_num: usize,
    /// Whether all projected roots are present and the stream can pass batches
    /// through.
    all_roots_present: bool,
    /// Parsed rewrite settings. `None` selects alignment to the output schema.
    rewrite_columns: Option<HashMap<String, Json2LayoutRewriteSettings>>,
    /// The cache for whether incoming batches already match output schema.
    is_schema_matched: Option<bool>,
}

impl<S> JsonSchemaAligner<S>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    /// Creates an aligner with a shared output schema and an explicit operation.
    /// Parses rewrite metadata once and validates layouts against output field types.
    pub(crate) fn new(
        inner: S,
        projected_root_presence: Vec<bool>,
        output_schema: SchemaRef,
        target: JsonAlignTarget,
    ) -> Result<JsonSchemaAligner<S>> {
        ensure!(
            projected_root_presence.len() == output_schema.fields().len(),
            UnexpectedSnafu {
                reason: format!(
                    "JsonSchemaAligner projected root presence len {} does not match output schema columns {}",
                    projected_root_presence.len(),
                    output_schema.fields().len()
                ),
            }
        );

        let rewrite_columns = if let JsonAlignTarget::Rewrite { columns } = target {
            let mut rewrite_columns = HashMap::with_capacity(columns.len());
            for (name, layout) in columns {
                let settings = Json2LayoutRewriteSettings::try_from(&layout)?;
                let field = output_schema.field_with_name(&name).map_err(|_| {
                    UnexpectedSnafu {
                        reason: format!(
                            "JSON2 rewrite column '{name}' is missing from output schema"
                        ),
                    }
                    .build()
                })?;
                ensure!(
                    is_json2_extension_type(field)
                        && field.data_type() == &json2_physical_data_type(&settings.target_layout),
                    UnexpectedSnafu {
                        reason: format!(
                            "JSON2 rewrite layout for column '{name}' does not match output field"
                        ),
                    }
                );
                rewrite_columns.insert(name, settings);
            }
            Some(rewrite_columns)
        } else {
            None
        };

        let expected_input_col_num = projected_root_presence
            .iter()
            .filter(|matched| **matched)
            .count();
        let all_roots_present = projected_root_presence.iter().all(|&m| m);
        Ok(JsonSchemaAligner {
            inner,
            output_schema,
            projected_root_presence,
            expected_input_col_num,
            all_roots_present,
            rewrite_columns,
            is_schema_matched: None,
        })
    }
}

impl<S> Stream for JsonSchemaAligner<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(rb))) => {
                let is_schema_matched = this.rewrite_columns.is_none()
                    && this.all_roots_present
                    && *this
                        .is_schema_matched
                        .get_or_insert_with(|| rb.schema() == this.output_schema);

                if is_schema_matched {
                    Poll::Ready(Some(Ok(rb)))
                } else {
                    Poll::Ready(Some(align_projected_batch(
                        rb,
                        &this.output_schema,
                        &this.projected_root_presence,
                        this.expected_input_col_num,
                        this.rewrite_columns.as_ref(),
                    )))
                }
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn align_projected_batch(
    rb: RecordBatch,
    output_schema: &SchemaRef,
    projected_root_presence: &[bool],
    expected_input_col_num: usize,
    rewrite_columns: Option<&HashMap<String, Json2LayoutRewriteSettings>>,
) -> Result<RecordBatch> {
    ensure!(
        rb.columns().len() == expected_input_col_num,
        UnexpectedSnafu {
            reason: format!(
                "JsonSchemaAligner expected {} input columns but got {}",
                expected_input_col_num,
                rb.columns().len()
            ),
        }
    );

    let mut cols = Vec::with_capacity(projected_root_presence.len());
    let mut idx = 0;
    let input_schema = rb.schema_ref();

    for (field, present) in output_schema.fields().iter().zip(projected_root_presence) {
        if !present {
            cols.push(new_null_array(field.data_type(), rb.num_rows()));
            continue;
        }

        cols.push(align_array(
            rb.column(idx),
            input_schema.field(idx),
            field,
            rewrite_columns.and_then(|columns| columns.get(field.name())),
        )?);
        idx += 1;
    }

    RecordBatch::try_new(output_schema.clone(), cols).context(NewRecordBatchSnafu)
}

fn align_array(
    source_array: &ArrayRef,
    source_field: &Field,
    target_field: &FieldRef,
    rewrite_settings: Option<&Json2LayoutRewriteSettings>,
) -> Result<ArrayRef> {
    if let Some(settings) = rewrite_settings {
        return JsonArray::from(source_array)
            .rewrite_to_v2(
                source_field,
                &settings.logical_settings,
                &settings.target_layout,
            )
            .context(DataTypeMismatchSnafu);
    }
    if source_array.data_type() == target_field.data_type() {
        return Ok(source_array.clone());
    }

    if is_json2_extension_type(target_field) {
        if is_json2_extension_type(source_field) {
            return JsonArray::from(source_array)
                .project_to_v2(source_field, target_field.data_type())
                .context(DataTypeMismatchSnafu);
        }
        return JsonArray::from(source_array)
            .project_to(target_field.data_type())
            .context(DataTypeMismatchSnafu);
    }

    if !matches!(target_field.data_type(), DataType::Struct(_)) {
        return Ok(source_array.clone());
    }

    cast_column(source_array, target_field.as_ref(), &DEFAULT_CAST_OPTIONS).context(CastColumnSnafu)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use datatypes::arrow::array::{
        Array, ArrayRef, BinaryArray, Int64Array, StringArray, StringViewArray, StructArray,
    };
    use datatypes::arrow::datatypes::{DataType, Field, Fields, Schema};
    use datatypes::extension::json::Json2ExtensionType;
    use datatypes::types::parse_string_to_jsonb;
    use futures::{StreamExt, stream};

    use super::*;

    #[test]
    fn test_aligner_resolves_json2_rewrite_settings()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let logical_settings = JsonSettings::default();
        let target_layout = JsonSettings::try_new(vec![], Some(0))?;
        let rewrite_targets = HashMap::from([(
            "j".to_string(),
            Json2TargetLayout {
                extension_metadata: serde_json::to_string(&JsonMetadata::new(
                    logical_settings.clone(),
                ))?,
                target_layout: target_layout.clone(),
            },
        )]);
        let aligner = JsonSchemaAligner::new(
            stream::empty::<Result<RecordBatch>>(),
            vec![false],
            schema([
                Field::new("j", json2_physical_data_type(&target_layout), true)
                    .with_extension_type(Json2ExtensionType::default()),
            ]),
            JsonAlignTarget::Rewrite {
                columns: rewrite_targets,
            },
        )?;
        let settings = &aligner.rewrite_columns.as_ref().unwrap()["j"];
        assert_eq!(logical_settings, settings.logical_settings);
        assert_eq!(target_layout, settings.target_layout);
        Ok(())
    }

    #[tokio::test]
    async fn test_aligner_with_all_projected_roots_match() {
        let output_schema = schema([
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let input = RecordBatch::try_new(
            output_schema.clone(),
            vec![int_array([1, 2, 3]), string_array(["x", "y", "z"])],
        )
        .unwrap();
        let stream = stream::iter([Ok(input.clone())]);

        let mut aligner = JsonSchemaAligner::new(
            stream,
            vec![true, true],
            output_schema.clone(),
            JsonAlignTarget::AlignToSchema,
        )
        .unwrap();
        let output = aligner.next().await.unwrap().unwrap();

        assert_eq!(input, output);
        assert!(aligner.next().await.is_none());
    }

    #[tokio::test]
    async fn test_aligner_with_fills_null_root_columns() {
        let input_schema = schema([Field::new("a", DataType::Int64, true)]);
        let output_schema = schema([
            Field::new("a", DataType::Int64, true),
            Field::new("missing", DataType::Utf8, true),
            Field::new("c", DataType::Int64, true),
        ]);
        let input = RecordBatch::try_new(input_schema, vec![int_array([10, 20])]).unwrap();
        let stream = stream::iter([Ok(input)]);

        let mut aligner = JsonSchemaAligner::new(
            stream,
            vec![true, false, false],
            output_schema.clone(),
            JsonAlignTarget::AlignToSchema,
        )
        .unwrap();
        let output = aligner.next().await.unwrap().unwrap();

        assert_eq!(output_schema, output.schema());
        assert_eq!(3, output.num_columns());
        assert_eq!(
            &[Some(10), Some(20)],
            output
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
                .as_slice()
        );
        assert_eq!(DataType::Utf8, *output.column(1).data_type());
        assert_eq!(output.num_rows(), output.column(1).null_count());
        assert_eq!(DataType::Int64, *output.column(2).data_type());
        assert_eq!(output.num_rows(), output.column(2).null_count());
    }

    #[tokio::test]
    async fn test_aligner_with_fills_missing_struct_root_column() {
        let input_schema = schema([Field::new("a", DataType::Int64, true)]);
        let struct_type = DataType::Struct(Fields::from(vec![
            Field::new("x", DataType::Int64, true),
            Field::new("y", DataType::Utf8, true),
        ]));
        let output_schema = schema([
            Field::new("a", DataType::Int64, true),
            Field::new("missing_struct", struct_type.clone(), true),
        ]);
        let input = RecordBatch::try_new(input_schema, vec![int_array([10, 20])]).unwrap();
        let stream = stream::iter([Ok(input)]);

        let mut aligner = JsonSchemaAligner::new(
            stream,
            vec![true, false],
            output_schema.clone(),
            JsonAlignTarget::AlignToSchema,
        )
        .unwrap();
        let output = aligner.next().await.unwrap().unwrap();

        assert_eq!(output_schema, output.schema());
        assert_eq!(2, output.num_columns());
        assert_eq!(struct_type, output.column(1).data_type().clone());
        assert_eq!(output.num_rows(), output.column(1).null_count());
    }

    #[tokio::test]
    async fn test_aligner_reject_projection_len_mismatch() {
        let output_schema = schema([Field::new("a", DataType::Int64, true)]);
        let stream = stream::iter([]);

        let err = match JsonSchemaAligner::new(
            stream,
            vec![true, false],
            output_schema,
            JsonAlignTarget::AlignToSchema,
        ) {
            Ok(_) => panic!("JsonSchemaAligner should reject projection length mismatch"),
            Err(err) => err,
        };

        assert!(
            err.to_string()
                .contains("projected root presence len 2 does not match output schema columns 1")
        );
    }

    #[tokio::test]
    async fn test_aligner_reject_with_input_column_mismatch() {
        let input_schema = schema([Field::new("a", DataType::Int64, true)]);
        let output_schema = schema([
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("missing", DataType::Int64, true),
        ]);
        let input = RecordBatch::try_new(input_schema, vec![int_array([1, 2])]).unwrap();
        let stream = stream::iter([Ok(input)]);

        let mut aligner = JsonSchemaAligner::new(
            stream,
            vec![true, true, false],
            output_schema,
            JsonAlignTarget::AlignToSchema,
        )
        .unwrap();
        let err = aligner.next().await.unwrap().unwrap_err();

        assert!(
            err.to_string()
                .contains("expected 2 input columns but got 1")
        );
    }

    #[tokio::test]
    async fn test_json_schema_aligner_aligns_struct_field() {
        let output_schema = schema([Field::new(
            "nested",
            DataType::Struct(Fields::from(vec![
                Field::new("x", DataType::Int64, true),
                Field::new("y", DataType::Utf8, true),
            ])),
            true,
        )]);
        let input = RecordBatch::try_new(
            schema([Field::new(
                "nested",
                DataType::Struct(Fields::from(vec![Field::new("x", DataType::Int64, true)])),
                true,
            )]),
            vec![Arc::new(StructArray::from(vec![(
                Arc::new(Field::new("x", DataType::Int64, true)),
                int_array([1, 2]),
            )]))],
        )
        .unwrap();

        let mut aligner = JsonSchemaAligner::new(
            stream::iter([Ok(input)]),
            vec![true],
            output_schema.clone(),
            JsonAlignTarget::AlignToSchema,
        )
        .unwrap();
        let output = aligner.next().await.unwrap().unwrap();

        assert_eq!(output_schema, output.schema());
        let nested = output
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(2, nested.columns().len());
        assert_eq!(2, nested.column(1).null_count());
    }

    #[tokio::test]
    async fn test_json_schema_aligner_decodes_variant_to_struct() {
        let source_values = [
            Some(parse_string_to_jsonb("1").unwrap()),
            Some(parse_string_to_jsonb(r#"{"b":2}"#).unwrap()),
            None,
        ];
        let source = Arc::new(BinaryArray::from_iter(
            source_values.iter().map(|value| value.as_deref()),
        )) as ArrayRef;
        let input_fields = Fields::from(vec![Arc::new(Field::new("a", DataType::Binary, true))]);
        let input = RecordBatch::try_new(
            schema([Field::new(
                "j",
                DataType::Struct(input_fields.clone()),
                true,
            )]),
            vec![Arc::new(StructArray::new(input_fields, vec![source], None))],
        )
        .unwrap();

        let output_schema = schema([Field::new(
            "j",
            DataType::Struct(Fields::from(vec![Arc::new(Field::new(
                "a",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("b", DataType::UInt64, true)),
                    Arc::new(Field::new("c", DataType::Utf8View, true)),
                ])),
                true,
            ))])),
            true,
        )
        .with_extension_type(Json2ExtensionType::default())]);
        let mut aligner = JsonSchemaAligner::new(
            stream::iter([Ok(input)]),
            vec![true],
            output_schema.clone(),
            JsonAlignTarget::AlignToSchema,
        )
        .unwrap();
        let output = aligner.next().await.unwrap().unwrap();

        assert_eq!(output_schema, output.schema());
        let j = output
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let a = j.column(0).as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            &[None, Some(2), None],
            a.column(0)
                .as_any()
                .downcast_ref::<datatypes::arrow::array::UInt64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
                .as_slice()
        );
        assert_eq!(
            &[None, None, None],
            a.column(1)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
                .as_slice()
        );
    }

    #[tokio::test]
    async fn test_json_schema_aligner_preserves_struct_siblings() {
        let source_values = [
            Some(parse_string_to_jsonb(r#"{"x":1}"#).unwrap()),
            Some(parse_string_to_jsonb(r#"{"x":2}"#).unwrap()),
        ];
        let source = Arc::new(BinaryArray::from_iter(
            source_values.iter().map(|value| value.as_deref()),
        )) as ArrayRef;
        let c = Arc::new(Int64Array::from_iter_values([10, 20])) as ArrayRef;

        let a_fields = Fields::from(vec![
            Arc::new(Field::new("b", DataType::Binary, true)),
            Arc::new(Field::new("c", DataType::Int64, true)),
        ]);
        let input_fields = Fields::from(vec![Arc::new(Field::new(
            "a",
            DataType::Struct(a_fields.clone()),
            true,
        ))]);
        let input = RecordBatch::try_new(
            schema([Field::new(
                "j",
                DataType::Struct(input_fields.clone()),
                true,
            )]),
            vec![Arc::new(StructArray::new(
                input_fields,
                vec![Arc::new(StructArray::new(a_fields, vec![source, c], None))],
                None,
            ))],
        )
        .unwrap();

        let output_schema = schema([Field::new(
            "j",
            DataType::Struct(Fields::from(vec![Arc::new(Field::new(
                "a",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new(
                        "b",
                        DataType::Struct(Fields::from(vec![Arc::new(Field::new(
                            "x",
                            DataType::Int64,
                            true,
                        ))])),
                        true,
                    )),
                    Arc::new(Field::new("c", DataType::Int64, true)),
                ])),
                true,
            ))])),
            true,
        )
        .with_extension_type(Json2ExtensionType::default())]);
        let mut aligner = JsonSchemaAligner::new(
            stream::iter([Ok(input)]),
            vec![true],
            output_schema.clone(),
            JsonAlignTarget::AlignToSchema,
        )
        .unwrap();
        let output = aligner.next().await.unwrap().unwrap();

        assert_eq!(output_schema, output.schema());
        let j = output
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let a = j.column(0).as_any().downcast_ref::<StructArray>().unwrap();
        let b = a.column(0).as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(
            &[Some(1), Some(2)],
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
                .as_slice()
        );
        assert_eq!(
            &[Some(10), Some(20)],
            a.column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
                .as_slice()
        );
    }

    #[tokio::test]
    async fn test_rewrite_multiple_columns_and_fill_missing_roots() {
        let logical_settings = JsonSettings::default();
        let target_layout = JsonSettings::try_new(vec![], Some(0)).unwrap();
        let target_type = json2_physical_data_type(&target_layout);
        let output_schema = schema([
            Field::new("j", target_type.clone(), true)
                .with_extension_type(Json2ExtensionType::default()),
            Field::new("missing", target_type.clone(), true)
                .with_extension_type(Json2ExtensionType::default()),
            Field::new("k", target_type.clone(), true)
                .with_extension_type(Json2ExtensionType::default()),
            Field::new("a", DataType::Int64, true),
        ]);
        let values = [Some(parse_string_to_jsonb(r#"{"x":1}"#).unwrap()), None];
        let source = Arc::new(BinaryArray::from_iter(
            values.iter().map(|value| value.as_deref()),
        )) as ArrayRef;
        let source_field = Field::new("j", DataType::Binary, true)
            .with_extension_type(Json2ExtensionType::default());
        let expected = JsonArray::from(&source)
            .rewrite_to_v2(&source_field, &logical_settings, &target_layout)
            .unwrap();
        let input = RecordBatch::try_new(
            schema([
                source_field,
                Field::new("k", DataType::Binary, true)
                    .with_extension_type(Json2ExtensionType::default()),
                Field::new("a", DataType::Int64, true),
            ]),
            vec![source.clone(), source, int_array([10, 20])],
        )
        .unwrap();
        let columns = ["j", "missing", "k"]
            .into_iter()
            .map(|name| {
                (
                    name.to_string(),
                    Json2TargetLayout {
                        extension_metadata: serde_json::to_string(&JsonMetadata::new(
                            logical_settings.clone(),
                        ))
                        .unwrap(),
                        target_layout: target_layout.clone(),
                    },
                )
            })
            .collect();
        let mut aligner = JsonSchemaAligner::new(
            stream::iter([Ok(input)]),
            vec![true, false, true, true],
            output_schema.clone(),
            JsonAlignTarget::Rewrite { columns },
        )
        .unwrap();
        let output = aligner.next().await.unwrap().unwrap();
        assert_eq!(output_schema, output.schema());
        assert_eq!(expected.as_ref(), output.column(0).as_ref());
        assert_eq!(expected.as_ref(), output.column(2).as_ref());
        assert_eq!(&target_type, output.column(1).data_type());
        assert_eq!(2, output.column(1).null_count());
        assert_eq!(int_array([10, 20]).as_ref(), output.column(3).as_ref());
    }

    #[test]
    fn test_rewrite_rejects_mismatched_output_layout() {
        let columns = HashMap::from([(
            "j".to_string(),
            Json2TargetLayout {
                extension_metadata: serde_json::to_string(&JsonMetadata::new(
                    JsonSettings::default(),
                ))
                .unwrap(),
                target_layout: JsonSettings::try_new(vec![], Some(0)).unwrap(),
            },
        )]);
        let result = JsonSchemaAligner::new(
            stream::empty::<Result<RecordBatch>>(),
            vec![false],
            schema([Field::new("j", DataType::Binary, true)
                .with_extension_type(Json2ExtensionType::default())]),
            JsonAlignTarget::Rewrite { columns },
        );
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("does not match output field")
        );
    }

    fn schema(fields: impl IntoIterator<Item = Field>) -> SchemaRef {
        Arc::new(Schema::new(fields.into_iter().collect::<Vec<_>>()))
    }

    fn int_array(values: impl IntoIterator<Item = i64>) -> ArrayRef {
        Arc::new(Int64Array::from_iter_values(values))
    }

    fn string_array(values: impl IntoIterator<Item = &'static str>) -> ArrayRef {
        Arc::new(StringArray::from_iter_values(values))
    }
}
