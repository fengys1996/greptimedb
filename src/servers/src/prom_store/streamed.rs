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

//! Bounded Prometheus Remote Read framing and chunk encoding.

use api::prom_store::remote::histogram::{Count, ZeroCount};
use api::prom_store::remote::{Chunk, ChunkedReadResponse, ChunkedSeries, Histogram, Sample};
use bytes::Bytes;
use common_recordbatch::{RecordBatches, SendableRecordBatchStream};
use datafusion::dataframe::DataFrame;
use datafusion::prelude::{ident, lit, when};
use futures::TryStreamExt;
use prost::Message;
use snafu::ResultExt;

use crate::error::{self, Result};
use crate::prom_store;

/// Maximum number of scalar samples encoded into one chunk.
const CHUNK_SAMPLES: usize = 120;
/// Maximum protobuf payload accepted by this encoder, including labels.
const MAX_FRAME_BYTES: usize = 1024 * 1024;

/// Orders rows by their emitted labels, then timestamp. At each label name,
/// an ended label set sorts before a present label, which sorts before a later
/// label. Comparing these small ranks and the original values is equivalent to
/// lexicographically comparing the variable-length emitted label sets, without
/// allocating a serialized label set for every sample. NULL labels are omitted;
/// empty strings remain present, matching sampled Remote Read.
/// Built-in expressions remain serializable for distributed execution.
pub fn sort_dataframe(
    dataframe: DataFrame,
    table: &str,
    timestamp: &str,
    value: &str,
) -> Result<DataFrame> {
    let mut labels = dataframe
        .schema()
        .fields()
        .iter()
        .filter(|field| field.name() != timestamp && field.name() != value)
        .map(|field| (field.name().clone(), ident(field.name())))
        .collect::<Vec<_>>();
    labels.push((prom_store::METRIC_NAME_LABEL.to_string(), lit(table)));
    labels.sort_by(|a, b| a.0.cmp(&b.0));
    let mut keys = Vec::with_capacity(labels.len() * 2 + 1);
    for (index, (_, value)) in labels.iter().enumerate() {
        let later_present = labels[index + 1..]
            .iter()
            .map(|(_, value)| value.clone().is_not_null())
            .reduce(|left, right| left.or(right))
            .unwrap_or_else(|| lit(false));
        let rank = when(value.clone().is_not_null(), lit(1u8))
            .when(later_present, lit(2u8))
            .otherwise(lit(0u8))
            .context(error::DataFrameSnafu)?;
        keys.push(rank.sort(true, true));
        keys.push(value.clone().sort(true, true));
    }
    keys.push(ident(timestamp).sort(true, true));
    dataframe.sort(keys).context(error::DataFrameSnafu)
}

/// Consumes an ordered query lazily. At most one input batch and a bounded row
/// slice are converted at a time; network backpressure stops polling the query.
pub fn encode_stream(
    mut input: SendableRecordBatchStream,
    table: String,
    timestamp: String,
    value: String,
    query_index: i64,
) -> futures::stream::BoxStream<'static, Result<Bytes>> {
    Box::pin(async_stream::try_stream! {
        while let Some(batch) = input.try_next().await.context(error::CollectRecordbatchSnafu)? {
            for offset in (0..batch.num_rows()).step_by(CHUNK_SAMPLES) {
                let slice = batch.slice(offset, CHUNK_SAMPLES.min(batch.num_rows() - offset))
                    .context(error::CollectRecordbatchSnafu)?;
                let batches = RecordBatches::try_new(slice.schema.clone(), vec![slice])
                    .context(error::CollectRecordbatchSnafu)?;
                let mut series = prom_store::recordbatches_to_timeseries(&table, &timestamp, &value, batches)?;
                for series in &mut series {
                    series.labels.sort_by(|a, b| a.name.cmp(&b.name));
                }
                series.sort_by(|a, b| a.labels.iter().map(|l| (&l.name, &l.value))
                    .cmp(b.labels.iter().map(|l| (&l.name, &l.value))));
                for series in series {
                    for samples in series.samples.chunks(CHUNK_SAMPLES) {
                        let chunk = Chunk {
                            min_time_ms: samples[0].timestamp,
                            max_time_ms: samples[samples.len() - 1].timestamp,
                            r#type: 1,
                            data: encode_xor(samples),
                        };
                        yield frame(query_index, series.labels.clone(), chunk)?;
                    }
                    for histogram in &series.histograms {
                        yield frame(query_index, series.labels.clone(), encode_histogram(histogram))?;
                    }
                }
            }
        }
    })
}

fn frame(
    query_index: i64,
    labels: Vec<api::prom_store::remote::Label>,
    chunk: Chunk,
) -> Result<Bytes> {
    let response = ChunkedReadResponse {
        query_index,
        chunked_series: vec![ChunkedSeries {
            labels,
            chunks: vec![chunk],
        }],
    };
    snafu::ensure!(
        response.encoded_len() <= MAX_FRAME_BYTES,
        error::InvalidPromRemoteReadQueryResultSnafu {
            msg: "a Remote Read chunk with its labels exceeds the 1 MiB frame limit",
        }
    );
    let payload = response.encode_to_vec();
    let mut framed = Vec::with_capacity(payload.len() + 14);
    prost::encoding::encode_varint(payload.len() as u64, &mut framed);
    framed.extend_from_slice(&crc32c::crc32c(&payload).to_be_bytes());
    framed.extend_from_slice(&payload);
    Ok(Bytes::from(framed))
}

fn encode_xor(samples: &[Sample]) -> Vec<u8> {
    let mut bits = Bits::new(samples.len() as u16, None);
    let mut previous_time = 0i64;
    let mut previous_delta = 0i64;
    let mut previous_value = 0u64;
    let mut window = (u32::MAX, 0);
    for (index, sample) in samples.iter().enumerate() {
        let delta = sample.timestamp.wrapping_sub(previous_time);
        match index {
            0 => {
                bits.uvarint(((sample.timestamp as u64) << 1) ^ ((sample.timestamp >> 63) as u64));
                bits.write(sample.value.to_bits(), 64);
            }
            1 => {
                bits.uvarint(delta as u64);
                bits.xor(sample.value.to_bits(), previous_value, &mut window);
            }
            _ => {
                let dod = delta.wrapping_sub(previous_delta);
                if dod == 0 {
                    bits.write(0, 1);
                } else if let Some((prefix, width, count)) = [(2, 14, 2), (6, 17, 3), (14, 20, 4)]
                    .into_iter()
                    .find(|(_, width, _)| in_range(dod, *width))
                {
                    bits.write(prefix, count);
                    bits.write(dod as u64, width);
                } else {
                    bits.write(15, 4);
                    bits.write(dod as u64, 64);
                }
                bits.xor(sample.value.to_bits(), previous_value, &mut window);
            }
        }
        previous_time = sample.timestamp;
        previous_delta = if index == 0 { 0 } else { delta };
        previous_value = sample.value.to_bits();
    }
    bits.bytes
}

/// Single-sample histogram chunks avoid layout recoding and preserve reset hints.
/// Scalar chunks use temporal compression; histogram chunk coalescing is independent.
fn encode_histogram(histogram: &Histogram) -> Chunk {
    let float = matches!(histogram.count, Some(Count::CountFloat(_)));
    let header = match histogram.reset_hint {
        1 => 0x80,
        2 => 0x40,
        3 => 0xc0,
        _ => 0,
    };
    let mut bits = Bits::new(1, Some(header));
    if histogram.zero_threshold == 0.0 {
        bits.write(0, 8);
    } else {
        bits.write(255, 8);
        bits.write(histogram.zero_threshold.to_bits(), 64);
    }
    bits.varbit(histogram.schema as i64 as u64, true);
    for spans in [&histogram.positive_spans, &histogram.negative_spans] {
        bits.varbit(spans.len() as u64, false);
        for span in spans {
            bits.varbit(span.length as u64, false);
            bits.varbit(span.offset as i64 as u64, true);
        }
    }
    if histogram.schema == -53 {
        bits.varbit(histogram.custom_values.len() as u64, false);
        for bound in &histogram.custom_values {
            bits.write(0, 1);
            bits.write(bound.to_bits(), 64);
        }
    }
    bits.varbit(histogram.timestamp as u64, true);
    if float {
        let count = match histogram.count {
            Some(Count::CountFloat(v)) => v,
            _ => 0.0,
        };
        let zero = match histogram.zero_count {
            Some(ZeroCount::ZeroCountFloat(v)) => v,
            _ => 0.0,
        };
        bits.write(count.to_bits(), 64);
        bits.write(zero.to_bits(), 64);
        bits.write(histogram.sum.to_bits(), 64);
        for value in histogram
            .positive_counts
            .iter()
            .chain(&histogram.negative_counts)
        {
            bits.write(value.to_bits(), 64);
        }
    } else {
        let count = match histogram.count {
            Some(Count::CountInt(v)) => v,
            _ => 0,
        };
        let zero = match histogram.zero_count {
            Some(ZeroCount::ZeroCountInt(v)) => v,
            _ => 0,
        };
        bits.varbit(count, false);
        bits.varbit(zero, false);
        bits.write(histogram.sum.to_bits(), 64);
        for value in histogram
            .positive_deltas
            .iter()
            .chain(&histogram.negative_deltas)
        {
            bits.varbit(*value as u64, true);
        }
    }
    Chunk {
        min_time_ms: histogram.timestamp,
        max_time_ms: histogram.timestamp,
        r#type: if float { 3 } else { 2 },
        data: bits.bytes,
    }
}

struct Bits {
    bytes: Vec<u8>,
    used: u8,
}

impl Bits {
    fn new(count: u16, header: Option<u8>) -> Self {
        let mut bytes = count.to_be_bytes().to_vec();
        bytes.extend(header);
        Self { bytes, used: 0 }
    }

    fn write(&mut self, value: u64, mut width: u32) {
        while width > 0 {
            if self.used == 0 {
                self.bytes.push(0);
            }
            let available = 8 - self.used;
            let take = width.min(u32::from(available));
            width -= take;
            let part = ((value >> width) & ((1 << take) - 1)) as u8;
            let index = self.bytes.len() - 1;
            self.bytes[index] |= part << (u32::from(available) - take);
            self.used = (self.used + take as u8) % 8;
        }
    }

    fn uvarint(&mut self, mut value: u64) {
        while value >= 128 {
            self.write((value as u8 | 128) as u64, 8);
            value >>= 7;
        }
        self.write(value, 8);
    }

    fn varbit(&mut self, value: u64, signed: bool) {
        if value == 0 {
            self.write(0, 1);
            return;
        }
        for (index, width) in [3, 6, 9, 12, 18, 25, 56].into_iter().enumerate() {
            if (signed && in_range(value as i64, width)) || (!signed && value < (1u64 << width)) {
                self.write((1u64 << (index + 2)) - 2, index as u32 + 2);
                self.write(value, width);
                return;
            }
        }
        self.write(255, 8);
        self.write(value, 64);
    }

    fn xor(&mut self, value: u64, previous: u64, window: &mut (u32, u32)) {
        let xor = value ^ previous;
        if xor == 0 {
            self.write(0, 1);
            return;
        }
        self.write(1, 1);
        let leading = xor.leading_zeros().min(31);
        let trailing = xor.trailing_zeros();
        if leading >= window.0 && trailing >= window.1 {
            self.write(0, 1);
            self.write(xor >> window.1, 64 - window.0 - window.1);
        } else {
            self.write(1, 1);
            self.write(leading as u64, 5);
            let significant = 64 - leading - trailing;
            self.write((significant % 64) as u64, 6);
            self.write(xor >> trailing, significant);
            *window = (leading, trailing);
        }
    }
}

fn in_range(value: i64, width: u32) -> bool {
    let half = 1i64 << (width - 1);
    value >= -(half - 1) && value <= half
}

#[cfg(test)]
mod tests {
    use super::*;
    use api::prom_store::remote::{BucketSpan, Label};

    // Frozen output of github.com/prometheus/prometheus v0.51.0 chunkenc.
    // Includes negative time, delta-of-delta width changes, stale NaN, and infinity.
    #[test]
    fn xor_matches_prometheus() {
        let samples = [-1000, 0, 1000, 2001, 500000, 500001]
            .into_iter()
            .zip([
                1.0,
                1.0,
                2.0,
                -0.5,
                f64::from_bits(0x7ff0000000000002),
                f64::INFINITY,
            ])
            .map(|(timestamp, value)| Sample { timestamp, value })
            .collect::<Vec<_>>();
        assert_eq!(
            hex::encode(encode_xor(&samples)),
            "0006cf0f3ff0000000000000e8073097ffe0007017fff9e559b07f8020000000000007a19aca0000000000000002"
        );
    }

    #[test]
    fn histograms_match_prometheus() {
        let integer = Histogram {
            timestamp: 1234,
            schema: 0,
            sum: 5.25,
            count: Some(Count::CountInt(9007199254740993)),
            zero_count: Some(ZeroCount::ZeroCountInt(2)),
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 2,
            }],
            positive_deltas: vec![3, -1],
            ..Default::default()
        };
        assert_eq!(
            hex::encode(encode_histogram(&integer).data),
            "000100004647934bf8800000000000064802a0000000000013b8"
        );
        let float = Histogram {
            count: Some(Count::CountFloat(7.5)),
            zero_count: Some(ZeroCount::ZeroCountFloat(0.5)),
            positive_counts: vec![3.5, 4.0],
            positive_deltas: vec![],
            reset_hint: 3,
            ..integer
        };
        assert_eq!(
            hex::encode(encode_histogram(&float).data),
            "0001c000464793490078000000000000ff80000000000001005400000000000100300000000000010040000000000000"
        );
    }

    #[test]
    fn frame_length_checksum_and_limit() {
        let chunk = Chunk {
            min_time_ms: 1,
            max_time_ms: 1,
            r#type: 1,
            data: encode_xor(&[Sample {
                timestamp: 1,
                value: 2.0,
            }]),
        };
        let data = frame(3, vec![], chunk.clone()).unwrap();
        let mut input = data.as_ref();
        let length = prost::encoding::decode_varint(&mut input).unwrap() as usize;
        assert_eq!(length + 4, input.len());
        assert_eq!(&crc32c::crc32c(&input[4..]).to_be_bytes(), &input[..4]);
        let decoded = ChunkedReadResponse::decode(&input[4..]).unwrap();
        assert_eq!(decoded.query_index, 3);
        assert_eq!(decoded.chunked_series[0].chunks[0], chunk);
        assert!(
            frame(
                0,
                vec![Label {
                    name: "host".into(),
                    value: "x".repeat(MAX_FRAME_BYTES)
                }],
                chunk
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn sort_matches_emitted_labels_and_time() {
        use datafusion::prelude::SessionContext;
        let ctx = SessionContext::new();
        let input = ctx
            .sql(
                "SELECT * FROM (VALUES \
            ('z', NULL, 30, 1.0), \
            (NULL, 'a', 10, 1.0), \
            ('z', NULL, 10, 1.0), \
            (NULL, NULL, 1, 1.0), \
            ('', NULL, 2, 1.0), \
            ('z!', NULL, 3, 1.0)) AS t(host, zone, ts, val)",
            )
            .await
            .unwrap();
        let sorted = sort_dataframe(input, "metric", "ts", "val")
            .unwrap()
            .collect()
            .await
            .unwrap();
        use arrow::array::AsArray;
        use arrow::datatypes::Int64Type;
        let times = sorted
            .iter()
            .flat_map(|b| {
                b.column(2)
                    .as_primitive::<Int64Type>()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        assert_eq!(times, [1, 2, 10, 30, 3, 10]);
    }
    #[tokio::test]
    async fn stream_is_lazy_backpressured_and_propagates_errors() {
        use common_recordbatch::{RecordBatch, RecordBatchStreamWrapper};
        use datatypes::{
            data_type::ConcreteDataType,
            schema::{ColumnSchema, Schema},
            vectors::{Float64Vector, TimestampMillisecondVector, VectorRef},
        };
        use futures::StreamExt;
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };
        let schema = Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            ColumnSchema::new("val", ConcreteDataType::float64_datatype(), false),
        ]));
        let batch = RecordBatch::new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondVector::from_vec((0..240).collect())) as VectorRef,
                Arc::new(Float64Vector::from_vec(vec![1.0; 240])) as VectorRef,
            ],
        )
        .unwrap();
        let polls = Arc::new(AtomicUsize::new(0));
        let count = polls.clone();
        let source = futures::stream::poll_fn(move |_| {
            let index = count.fetch_add(1, Ordering::SeqCst);
            std::task::Poll::Ready(Some(if index == 0 {
                Ok(batch.clone())
            } else {
                common_recordbatch::error::CreateRecordBatchesSnafu {
                    reason: "injected read failure",
                }
                .fail()
            }))
        });
        let input = Box::pin(RecordBatchStreamWrapper::new(schema, source));
        let mut output = encode_stream(input, "metric".into(), "ts".into(), "val".into(), 2);
        assert_eq!(polls.load(Ordering::SeqCst), 0);
        for _ in 0..2 {
            let frame = output.next().await.unwrap().unwrap();
            let mut frame = frame.as_ref();
            prost::encoding::decode_varint(&mut frame).unwrap();
            let response = ChunkedReadResponse::decode(&frame[4..]).unwrap();
            assert_eq!(response.query_index, 2);
            assert_eq!(
                u16::from_be_bytes(
                    response.chunked_series[0].chunks[0].data[..2]
                        .try_into()
                        .unwrap()
                ),
                120
            );
            assert_eq!(polls.load(Ordering::SeqCst), 1);
        }
        assert!(output.next().await.unwrap().is_err());
        assert!(output.next().await.is_none());
        assert_eq!(polls.load(Ordering::SeqCst), 2);
    }
    #[tokio::test]
    async fn sort_matches_label_comparator_for_all_null_patterns() {
        use arrow::array::{ArrayRef, AsArray, Float64Array, Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Int64Type, Schema};
        use arrow::record_batch::RecordBatch;
        use datafusion::prelude::SessionContext;
        use std::sync::Arc;
        let choices = [None, Some(""), Some("a!"), Some("中")];
        let mut columns = [Vec::new(), Vec::new(), Vec::new()];
        let mut expected = Vec::new();
        for a in choices {
            for b in choices {
                for c in choices {
                    let mut labels = vec![("__name__", "metric")];
                    for (index, (name, value)) in
                        [("A", a), ("host", b), ("z", c)].into_iter().enumerate()
                    {
                        columns[index].push(value);
                        if let Some(value) = value {
                            labels.push((name, value));
                        }
                    }
                    labels.sort();
                    expected.push((labels, expected.len() as i64));
                }
            }
        }
        let rows = expected.len();
        expected.sort();
        let schema = Arc::new(Schema::new(vec![
            Field::new("A", DataType::Utf8, true),
            Field::new("host", DataType::Utf8, true),
            Field::new("z", DataType::Utf8, true),
            Field::new("ts", DataType::Int64, false),
            Field::new("val", DataType::Float64, false),
        ]));
        let mut arrays = columns
            .into_iter()
            .map(|values| Arc::new(StringArray::from(values)) as ArrayRef)
            .collect::<Vec<_>>();
        arrays.push(Arc::new(Int64Array::from_iter_values(0..rows as i64)));
        arrays.push(Arc::new(Float64Array::from(vec![1.0; rows])));
        let input = SessionContext::new()
            .read_batch(RecordBatch::try_new(schema, arrays).unwrap())
            .unwrap();
        let output = sort_dataframe(input, "metric", "ts", "val")
            .unwrap()
            .collect()
            .await
            .unwrap();
        let actual = output
            .iter()
            .flat_map(|b| {
                b.column(3)
                    .as_primitive::<Int64Type>()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            expected.into_iter().map(|(_, row)| row).collect::<Vec<_>>()
        );
    }
    #[tokio::test]
    async fn dropping_response_releases_unpolled_query() {
        use common_recordbatch::{RecordBatch, RecordBatchStreamWrapper};
        use datatypes::schema::Schema;
        use std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        };
        struct QueryGuard(Arc<AtomicBool>);
        impl Drop for QueryGuard {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = QueryGuard(dropped.clone());
        let source = futures::stream::poll_fn(move |_| {
            let _ = &guard;
            std::task::Poll::<Option<common_recordbatch::error::Result<RecordBatch>>>::Pending
        });
        let input = Box::pin(RecordBatchStreamWrapper::new(
            Arc::new(Schema::new(vec![])),
            source,
        ));
        let output = encode_stream(input, "metric".into(), "ts".into(), "val".into(), 0);
        assert!(!dropped.load(Ordering::SeqCst));
        drop(output);
        assert!(dropped.load(Ordering::SeqCst));
    }
}
