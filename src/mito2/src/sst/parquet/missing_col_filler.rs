use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datatypes::arrow::array::{ArrayRef, new_null_array};
use datatypes::arrow::datatypes::{Field, SchemaRef};
use datatypes::arrow::record_batch::RecordBatch;
use futures::{Stream, ready};
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use snafu::{IntoError, ResultExt};

use crate::error::{NewRecordBatchSnafu, ReadParquetSnafu, Result};
use crate::sst::parquet::async_reader::SstAsyncFileReader;

/// Fills missing projected root columns and restores a normalized batch layout.
pub struct MissingColFiller {
    inner: ParquetRecordBatchStream<SstAsyncFileReader>,
    file_path: String,
    output_schema: SchemaRef,
    present_col_mappings: Vec<PresentColMapping>,
    missing_cols: Vec<MissingRootCol>,
}

#[derive(Clone)]
pub struct MissingColFillPlan {
    pub output_schema: SchemaRef,
    pub present_col_mappings: Vec<PresentColMapping>,
    pub missing_cols: Vec<MissingRootCol>,
}

impl MissingColFiller {
    pub fn new(
        inner: ParquetRecordBatchStream<SstAsyncFileReader>,
        file_path: String,
        output_schema: SchemaRef,
        present_col_mappings: Vec<PresentColMapping>,
        missing_cols: Vec<MissingRootCol>,
    ) -> Self {
        Self {
            inner,
            file_path,
            output_schema,
            present_col_mappings,
            missing_cols,
        }
    }
}

impl Stream for MissingColFiller {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match ready!(Pin::new(&mut this.inner).poll_next(ctx)) {
            Some(Ok(batch)) => Poll::Ready(Some(fill_missing_columns(
                batch,
                &this.output_schema,
                &this.present_col_mappings,
                &this.missing_cols,
            ))),
            Some(Err(err)) => Poll::Ready(Some(Err(ReadParquetSnafu {
                path: this.file_path.clone(),
            }
            .into_error(err)))),
            None => Poll::Ready(None),
        }
    }
}

/// Maps an existing input column to its expected position in the normalized batch.
#[derive(Clone)]
pub struct PresentColMapping {
    pub source_idx: usize,
    pub expected_pos: usize,
}

/// A projected root column missing from the physical parquet batch.
#[derive(Clone)]
pub struct MissingRootCol {
    pub expected_pos: usize,
    pub field: Arc<Field>,
}

pub(crate) fn fill_missing_columns(
    batch: RecordBatch,
    output_schema: &SchemaRef,
    present_col_mappings: &[PresentColMapping],
    missing_cols: &[MissingRootCol],
) -> Result<RecordBatch> {
    if present_col_mappings.is_empty() && missing_cols.is_empty() {
        return Ok(batch);
    }

    let mut columns: Vec<Option<ArrayRef>> = vec![None; output_schema.fields().len()];

    for mapping in present_col_mappings {
        columns[mapping.expected_pos] = Some(batch.column(mapping.source_idx).clone());
    }

    for missing in missing_cols {
        columns[missing.expected_pos] =
            Some(new_null_array(missing.field.data_type(), batch.num_rows()));
    }

    let columns = columns
        .into_iter()
        .enumerate()
        .map(|(idx, col)| {
            col.unwrap_or_else(|| {
                new_null_array(output_schema.field(idx).data_type(), batch.num_rows())
            })
        })
        .collect::<Vec<_>>();

    RecordBatch::try_new(output_schema.clone(), columns).context(NewRecordBatchSnafu)
}

#[cfg(test)]
mod tests {
    use datatypes::arrow::array::{Array, Int64Array, StringArray};
    use datatypes::arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn test_fill_missing_columns_inserts_null_column_and_reorders() {
        let input_schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("k", DataType::Int64, true)),
            Arc::new(Field::new("ts", DataType::Int64, false)),
        ]));
        let input_batch = RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 20])),
                Arc::new(Int64Array::from(vec![100, 200])),
            ],
        )
        .unwrap();

        let output_schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("j", DataType::Utf8, true)),
            Arc::new(Field::new("k", DataType::Int64, true)),
            Arc::new(Field::new("ts", DataType::Int64, false)),
        ]));

        let output = fill_missing_columns(
            input_batch,
            &output_schema,
            &[
                PresentColMapping {
                    source_idx: 0,
                    expected_pos: 1,
                },
                PresentColMapping {
                    source_idx: 1,
                    expected_pos: 2,
                },
            ],
            &[MissingRootCol {
                expected_pos: 0,
                field: Arc::new(Field::new("j", DataType::Utf8, true)),
            }],
        )
        .unwrap();

        let j = output
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(2, j.len());
        assert_eq!(2, j.null_count());

        let k = output
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(&Int64Array::from(vec![10, 20]), k);

        let ts = output
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(&Int64Array::from(vec![100, 200]), ts);
    }

    #[test]
    fn test_fill_missing_columns_only_reorders_present_columns() {
        let input_schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("b", DataType::Int64, true)),
            Arc::new(Field::new("a", DataType::Int64, true)),
        ]));
        let input_batch = RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(Int64Array::from(vec![2, 4])),
                Arc::new(Int64Array::from(vec![1, 3])),
            ],
        )
        .unwrap();

        let output_schema = Arc::new(Schema::new(vec![
            Arc::new(Field::new("a", DataType::Int64, true)),
            Arc::new(Field::new("b", DataType::Int64, true)),
        ]));

        let output = fill_missing_columns(
            input_batch,
            &output_schema,
            &[
                PresentColMapping {
                    source_idx: 0,
                    expected_pos: 1,
                },
                PresentColMapping {
                    source_idx: 1,
                    expected_pos: 0,
                },
            ],
            &[],
        )
        .unwrap();

        let a = output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(&Int64Array::from(vec![1, 3]), a);

        let b = output
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(&Int64Array::from(vec![2, 4]), b);
    }
}
