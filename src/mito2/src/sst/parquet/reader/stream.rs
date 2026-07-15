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
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion_common::cast_column;
use datafusion_common::format::DEFAULT_CAST_OPTIONS;
use datatypes::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
    StringViewBuilder, StructArray, UInt64Builder, make_array, new_null_array,
};
use datatypes::arrow::datatypes::{DataType, FieldRef, Schema, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::types::jsonb_to_serde_json;
use futures::Stream;
use futures::stream::BoxStream;
use serde_json::Value as JsonValue;
use snafu::{ResultExt, ensure};

use crate::error::{CastColumnSnafu, NewRecordBatchSnafu, Result, UnexpectedSnafu};
use crate::sst::parquet::read_columns::{NestedPathFallback, ParquetNestedPath};

/// Projects binary JSON fallback columns back to the expected nested schema.
///
/// Parquet nested projection may fall back from a requested typed path, such as
/// `j.a.b`, to a binary ancestor path, such as `j.a`. This stream wrapper
/// consumes the fallback metadata produced by projection planning, extracts the
/// requested JSON subpath, and rewrites the affected root column to match the
/// expected schema before [`NestedSchemaAligner`] fills missing roots.
#[derive(derive_more::Debug)]
pub struct NestedJsonFallbackProjector<S> {
    #[debug(skip)]
    inner: S,
    output_schema: SchemaRef,
    physical_schema: SchemaRef,
    projected_root_presence: Vec<bool>,
    fallback: Vec<NestedPathFallback>,
    expected_input_col_num: usize,
}

impl<S> NestedJsonFallbackProjector<S>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    pub fn new(
        inner: S,
        projected_root_presence: Vec<bool>,
        output_schema: SchemaRef,
        fallback: Vec<NestedPathFallback>,
    ) -> Result<Self> {
        ensure!(
            projected_root_presence.len() == output_schema.fields().len(),
            UnexpectedSnafu {
                reason: format!(
                    "NestedJsonFallbackProjector projected root presence len {} does not match output schema columns {}",
                    projected_root_presence.len(),
                    output_schema.fields().len()
                ),
            }
        );

        let expected_input_col_num = projected_root_presence
            .iter()
            .filter(|matched| **matched)
            .count();
        let physical_schema = Arc::new(Schema::new(
            output_schema
                .fields()
                .iter()
                .zip(&projected_root_presence)
                .filter_map(|(field, present)| present.then_some(field.as_ref().clone()))
                .collect::<Vec<_>>(),
        ));

        Ok(Self {
            inner,
            output_schema,
            physical_schema,
            projected_root_presence,
            fallback,
            expected_input_col_num,
        })
    }
}

impl<S> Stream for NestedJsonFallbackProjector<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(rb))) => Poll::Ready(Some(project_json_fallback_batch(
                rb,
                &this.output_schema,
                &this.physical_schema,
                &this.projected_root_presence,
                &this.fallback,
                this.expected_input_col_num,
            ))),
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
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
pub struct NestedSchemaAligner<S> {
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
    /// The cache for whether incoming batches already match output schema.
    is_schema_matched: Option<bool>,
}

pub(crate) type ProjectedRecordBatchStream = BoxStream<'static, Result<RecordBatch>>;

fn project_json_fallback_batch(
    rb: RecordBatch,
    output_schema: &SchemaRef,
    physical_schema: &SchemaRef,
    projected_root_presence: &[bool],
    fallback: &[NestedPathFallback],
    expected_input_col_num: usize,
) -> Result<RecordBatch> {
    ensure!(
        rb.columns().len() == expected_input_col_num,
        UnexpectedSnafu {
            reason: format!(
                "NestedJsonFallbackProjector expected {} input columns but got {}",
                expected_input_col_num,
                rb.columns().len()
            ),
        }
    );

    let mut columns = rb.columns().to_vec();
    let mut fallback_by_root = HashMap::<usize, Vec<&NestedPathFallback>>::new();
    for fallback in fallback {
        fallback_by_root
            .entry(fallback.output_root_index)
            .or_default()
            .push(fallback);
    }

    for (output_root_index, fallback) in fallback_by_root {
        ensure!(
            output_root_index < projected_root_presence.len(),
            UnexpectedSnafu {
                reason: format!(
                    "NestedJsonFallbackProjector output root index {} out of range {}",
                    output_root_index,
                    projected_root_presence.len()
                ),
            }
        );
        ensure!(
            projected_root_presence[output_root_index],
            UnexpectedSnafu {
                reason: format!(
                    "NestedJsonFallbackProjector fallback root {} is not physically present",
                    output_root_index
                ),
            }
        );

        let physical_index = projected_root_presence[..output_root_index]
            .iter()
            .filter(|present| **present)
            .count();
        let root_field = output_schema.fields()[output_root_index].clone();
        let root_array = columns[physical_index].clone();
        let mut replacements = HashMap::new();

        for fallback in fallback {
            let target_field =
                field_by_path(&root_field, &fallback.requested_path[1..]).ok_or_else(|| {
                    UnexpectedSnafu {
                        reason: format!(
                            "NestedJsonFallbackProjector cannot find requested path {:?} in output schema",
                            fallback.requested_path
                        ),
                    }
                    .build()
                })?;
            let physical_array = array_by_path(&root_array, &fallback.physical_path[1..]);
            let replacement = match physical_array {
                Some(physical_array) => extract_jsonb_path(
                    physical_array,
                    &fallback.requested_path[fallback.physical_path.len()..],
                    target_field.data_type(),
                ),
                None => new_null_array(target_field.data_type(), rb.num_rows()),
            };
            replacements.insert(fallback.requested_path.clone(), replacement);
        }

        columns[physical_index] = build_array_with_replacements(
            Some(root_array.as_ref()),
            &root_field,
            vec![root_field.name().clone()],
            rb.num_rows(),
            &replacements,
        )?;
    }

    RecordBatch::try_new(physical_schema.clone(), columns).context(NewRecordBatchSnafu)
}

fn field_by_path(field: &FieldRef, path: &[String]) -> Option<FieldRef> {
    if path.is_empty() {
        return Some(field.clone());
    }

    let DataType::Struct(fields) = field.data_type() else {
        return None;
    };
    let child = fields.iter().find(|field| field.name() == &path[0])?;
    field_by_path(child, &path[1..])
}

fn array_by_path(array: &ArrayRef, path: &[String]) -> Option<ArrayRef> {
    if path.is_empty() {
        return Some(array.clone());
    }

    let struct_array = array.as_any().downcast_ref::<StructArray>()?;
    let child = struct_array.column_by_name(&path[0])?;
    array_by_path(child, &path[1..])
}

fn build_array_with_replacements(
    physical_array: Option<&dyn Array>,
    expected_field: &FieldRef,
    current_path: ParquetNestedPath,
    num_rows: usize,
    replacements: &HashMap<ParquetNestedPath, ArrayRef>,
) -> Result<ArrayRef> {
    if let Some(replacement) = replacements.get(&current_path) {
        return Ok(replacement.clone());
    }

    let DataType::Struct(expected_fields) = expected_field.data_type() else {
        return build_leaf_array(physical_array, expected_field, num_rows);
    };

    let physical_struct =
        physical_array.and_then(|array| array.as_any().downcast_ref::<StructArray>());
    let arrays = expected_fields
        .iter()
        .map(|child_field| {
            let child_physical_array =
                physical_struct.and_then(|array| array.column_by_name(child_field.name()));
            let mut child_path = current_path.clone();
            child_path.push(child_field.name().clone());
            build_array_with_replacements(
                child_physical_array.map(|array| array.as_ref()),
                child_field,
                child_path,
                num_rows,
                replacements,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    let fields = expected_fields.iter().cloned().collect::<Vec<_>>();
    Ok(Arc::new(StructArray::from(
        fields.into_iter().zip(arrays).collect::<Vec<_>>(),
    )))
}

fn build_leaf_array(
    physical_array: Option<&dyn Array>,
    expected_field: &FieldRef,
    num_rows: usize,
) -> Result<ArrayRef> {
    let Some(physical_array) = physical_array else {
        return Ok(new_null_array(expected_field.data_type(), num_rows));
    };

    let physical_array = make_array(physical_array.to_data());
    if physical_array.data_type() == expected_field.data_type() {
        return Ok(physical_array);
    }

    cast_column(
        &physical_array,
        expected_field.as_ref(),
        &DEFAULT_CAST_OPTIONS,
    )
    .or_else(|_| Ok(new_null_array(expected_field.data_type(), num_rows)))
}

fn extract_jsonb_path(array: ArrayRef, suffix: &[String], data_type: &DataType) -> ArrayRef {
    let Some(binary_array) = array.as_any().downcast_ref::<BinaryArray>() else {
        return new_null_array(data_type, array.len());
    };

    match data_type {
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for row in 0..binary_array.len() {
                append_jsonb_string(binary_array, row, suffix, &mut builder);
            }
            Arc::new(builder.finish())
        }
        DataType::Utf8View => {
            let mut builder = StringViewBuilder::new();
            for row in 0..binary_array.len() {
                let value = extract_json_value(binary_array, row, suffix).map(|value| {
                    value
                        .as_str()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| value.to_string())
                });
                builder.append_option(value);
            }
            Arc::new(builder.finish())
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::new();
            for row in 0..binary_array.len() {
                let value =
                    extract_json_value(binary_array, row, suffix).and_then(|value| value.as_i64());
                builder.append_option(value);
            }
            Arc::new(builder.finish())
        }
        DataType::UInt64 => {
            let mut builder = UInt64Builder::new();
            for row in 0..binary_array.len() {
                let value =
                    extract_json_value(binary_array, row, suffix).and_then(|value| value.as_u64());
                builder.append_option(value);
            }
            Arc::new(builder.finish())
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::new();
            for row in 0..binary_array.len() {
                let value =
                    extract_json_value(binary_array, row, suffix).and_then(|value| value.as_f64());
                builder.append_option(value);
            }
            Arc::new(builder.finish())
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for row in 0..binary_array.len() {
                let value =
                    extract_json_value(binary_array, row, suffix).and_then(|value| value.as_bool());
                builder.append_option(value);
            }
            Arc::new(builder.finish())
        }
        DataType::Binary => {
            let values = (0..binary_array.len())
                .map(|row| extract_json_value(binary_array, row, suffix))
                .map(|value| value.map(|value| value.to_string().into_bytes()));
            Arc::new(BinaryArray::from_iter(values))
        }
        _ => new_null_array(data_type, binary_array.len()),
    }
}

fn append_jsonb_string(
    binary_array: &BinaryArray,
    row: usize,
    suffix: &[String],
    builder: &mut StringBuilder,
) {
    let value = extract_json_value(binary_array, row, suffix).map(|value| {
        value
            .as_str()
            .map(ToString::to_string)
            .unwrap_or_else(|| value.to_string())
    });
    builder.append_option(value);
}

fn extract_json_value(
    binary_array: &BinaryArray,
    row: usize,
    suffix: &[String],
) -> Option<JsonValue> {
    if binary_array.is_null(row) {
        return None;
    }

    let mut value = jsonb_to_serde_json(binary_array.value(row)).ok()?;
    if suffix.is_empty() {
        return Some(value);
    }

    for segment in suffix {
        value = value.get(segment)?.clone();
    }
    Some(value)
}

impl<S> NestedSchemaAligner<S>
where
    S: Stream<Item = Result<RecordBatch>>,
{
    pub fn new(
        inner: S,
        projected_root_presence: Vec<bool>,
        output_schema: SchemaRef,
    ) -> Result<NestedSchemaAligner<S>> {
        ensure!(
            projected_root_presence.len() == output_schema.fields().len(),
            UnexpectedSnafu {
                reason: format!(
                    "NestedSchemaAligner projected root presence len {} does not match output schema columns {}",
                    projected_root_presence.len(),
                    output_schema.fields().len()
                ),
            }
        );

        let expected_input_col_num = projected_root_presence
            .iter()
            .filter(|matched| **matched)
            .count();
        let all_roots_present = projected_root_presence.iter().all(|&m| m);

        Ok(NestedSchemaAligner {
            inner,
            output_schema,
            projected_root_presence,
            expected_input_col_num,
            all_roots_present,
            is_schema_matched: None,
        })
    }
}

impl<S> Stream for NestedSchemaAligner<S>
where
    S: Stream<Item = Result<RecordBatch>> + Unpin,
{
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(rb))) => {
                let rb = if this.all_roots_present {
                    rb
                } else {
                    fill_missing_cols(
                        rb,
                        &this.output_schema,
                        &this.projected_root_presence,
                        this.expected_input_col_num,
                    )?
                };

                let is_schema_matched = *this
                    .is_schema_matched
                    .get_or_insert_with(|| rb.schema() == this.output_schema);

                if is_schema_matched {
                    Poll::Ready(Some(Ok(rb)))
                } else {
                    Poll::Ready(Some(align_batch_to_schema(rb, &this.output_schema)))
                }
            }
            Poll::Ready(Some(Err(err))) => Poll::Ready(Some(Err(err))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

fn fill_missing_cols(
    rb: RecordBatch,
    output_schema: &SchemaRef,
    projected_root_matches: &[bool],
    expected_input_col_num: usize,
) -> Result<RecordBatch> {
    ensure!(
        rb.columns().len() == expected_input_col_num,
        UnexpectedSnafu {
            reason: format!(
                "NestedSchemaAligner expected {} input columns but got {}",
                expected_input_col_num,
                rb.columns().len()
            ),
        }
    );

    let mut cols = Vec::with_capacity(projected_root_matches.len());
    let mut idx = 0;

    for (field, matched) in output_schema.fields().iter().zip(projected_root_matches) {
        if *matched {
            cols.push(rb.column(idx).clone());
            idx += 1;
        } else {
            cols.push(new_null_array(field.data_type(), rb.num_rows()));
        }
    }

    RecordBatch::try_new(output_schema.clone(), cols).context(NewRecordBatchSnafu)
}

fn align_batch_to_schema(rb: RecordBatch, output_schema: &SchemaRef) -> Result<RecordBatch> {
    ensure!(
        rb.num_columns() == output_schema.fields().len(),
        UnexpectedSnafu {
            reason: format!(
                "NestedSchemaAligner expected {} columns but got {}",
                output_schema.fields().len(),
                rb.num_columns()
            ),
        }
    );

    let columns = rb
        .columns()
        .iter()
        .zip(output_schema.fields())
        .map(|(array, field)| align_array(array, field))
        .collect::<Result<Vec<_>>>()?;

    RecordBatch::try_new(output_schema.clone(), columns).context(NewRecordBatchSnafu)
}

fn align_array(array: &ArrayRef, field: &FieldRef) -> Result<ArrayRef> {
    if array.data_type() == field.data_type() {
        return Ok(array.clone());
    }

    if !matches!(field.data_type(), DataType::Struct(_)) {
        return Ok(array.clone());
    }

    cast_column(array, field.as_ref(), &DEFAULT_CAST_OPTIONS).context(CastColumnSnafu)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::{
        Array, ArrayRef, BinaryArray, Int64Array, StringArray, StringViewArray, StructArray,
    };
    use datatypes::arrow::datatypes::{DataType, Field, Fields, Schema};
    use datatypes::types::parse_string_to_jsonb;
    use futures::{StreamExt, stream};

    use super::*;
    use crate::sst::parquet::read_columns::NestedPathFallback;

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

        let mut aligner =
            NestedSchemaAligner::new(stream, vec![true, true], output_schema.clone()).unwrap();
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

        let mut aligner =
            NestedSchemaAligner::new(stream, vec![true, false, false], output_schema.clone())
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

        let mut aligner =
            NestedSchemaAligner::new(stream, vec![true, false], output_schema.clone()).unwrap();
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

        let err = match NestedSchemaAligner::new(stream, vec![true, false], output_schema) {
            Ok(_) => panic!("NestedSchemaAligner should reject projection length mismatch"),
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

        let mut aligner =
            NestedSchemaAligner::new(stream, vec![true, true, false], output_schema).unwrap();
        let err = aligner.next().await.unwrap().unwrap_err();

        assert!(
            err.to_string()
                .contains("expected 2 input columns but got 1")
        );
    }

    #[tokio::test]
    async fn test_nested_schema_aligner_aligns_struct_field() {
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

        let mut aligner =
            NestedSchemaAligner::new(stream::iter([Ok(input)]), vec![true], output_schema.clone())
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
    async fn test_json_fallback_projector_extracts_binary_root() {
        let physical_schema = schema([Field::new("j", DataType::Binary, true)]);
        let output_schema = schema([Field::new(
            "j",
            DataType::Struct(Fields::from(vec![Field::new(
                "a",
                DataType::Struct(Fields::from(vec![Field::new("b", DataType::Utf8, true)])),
                true,
            )])),
            true,
        )]);
        let json = parse_string_to_jsonb(r#"{"a":{"b":"x"}}"#).unwrap();
        let input = RecordBatch::try_new(
            physical_schema,
            vec![Arc::new(BinaryArray::from_iter_values([json]))],
        )
        .unwrap();
        let fallback = vec![NestedPathFallback {
            output_root_index: 0,
            requested_path: vec!["j".to_string(), "a".to_string(), "b".to_string()],
            physical_path: vec!["j".to_string()],
        }];

        let mut projector = NestedJsonFallbackProjector::new(
            stream::iter([Ok(input)]),
            vec![true],
            output_schema.clone(),
            fallback,
        )
        .unwrap();
        let output = projector.next().await.unwrap().unwrap();

        assert_eq!(output_schema, output.schema());
        let root = output
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let a = root.column_by_name("a").unwrap();
        let a = a.as_any().downcast_ref::<StructArray>().unwrap();
        let b = a.column_by_name("b").unwrap();
        let b = b.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(Some("x"), b.iter().next().unwrap());
    }

    #[tokio::test]
    async fn test_json_fallback_projector_extracts_binary_child_path() {
        let physical_field = Arc::new(Field::new(
            "j",
            DataType::Struct(Fields::from(vec![Field::new("a", DataType::Binary, true)])),
            true,
        ));
        let physical_schema = Arc::new(Schema::new(vec![physical_field.clone()]));
        let output_schema = schema([Field::new(
            "j",
            DataType::Struct(Fields::from(vec![Field::new(
                "a",
                DataType::Struct(Fields::from(vec![Field::new(
                    "b",
                    DataType::Utf8View,
                    true,
                )])),
                true,
            )])),
            true,
        )]);
        let json = parse_string_to_jsonb(r#"{"b":"x"}"#).unwrap();
        let input = RecordBatch::try_new(
            physical_schema,
            vec![Arc::new(StructArray::from(vec![(
                Arc::new(Field::new("a", DataType::Binary, true)),
                Arc::new(BinaryArray::from_iter_values([json])) as ArrayRef,
            )]))],
        )
        .unwrap();
        let fallback = vec![NestedPathFallback {
            output_root_index: 0,
            requested_path: vec!["j".to_string(), "a".to_string(), "b".to_string()],
            physical_path: vec!["j".to_string(), "a".to_string()],
        }];

        let mut projector = NestedJsonFallbackProjector::new(
            stream::iter([Ok(input)]),
            vec![true],
            output_schema.clone(),
            fallback,
        )
        .unwrap();
        let output = projector.next().await.unwrap().unwrap();

        assert_eq!(output_schema, output.schema());
        let root = output
            .column(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let a = root.column_by_name("a").unwrap();
        let a = a.as_any().downcast_ref::<StructArray>().unwrap();
        let b = a.column_by_name("b").unwrap();
        assert_eq!(DataType::Utf8View, b.data_type().clone());
        assert_eq!(
            "x",
            b.as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap()
                .value(0)
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
