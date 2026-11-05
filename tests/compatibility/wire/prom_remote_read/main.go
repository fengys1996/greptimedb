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

package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"hash/crc32"
	"io"
	"math"
	"net/http"
)

func main() {
	endpoint := flag.String("url", "", "Remote Read endpoint; empty emits frozen codec fixtures")
	metric := flag.String("metric", "bench", "metric table")
	end := flag.Int64("end", 1000000000, "inclusive end timestamp in milliseconds")
	flag.Parse()
	if *endpoint != "" {
		verify(*endpoint, *metric, *end)
		return
	}

	c := chunkenc.NewXORChunk()
	a, _ := c.Appender()
	for i, t := range []int64{-1000, 0, 1000, 2001, 500000, 500001} {
		a.Append(t, []float64{1, 1, 2, -0.5, math.Float64frombits(0x7ff0000000000002), math.Inf(1)}[i])
	}
	fmt.Printf("xor %x\n", c.Bytes())
	h := &histogram.Histogram{Count: 9007199254740993, ZeroCount: 2, Sum: 5.25, Schema: 0, PositiveSpans: []histogram.Span{{Offset: 0, Length: 2}}, PositiveBuckets: []int64{3, -1}}
	hc := chunkenc.NewHistogramChunk()
	ha, _ := hc.Appender()
	_, _, _, err := ha.AppendHistogram(nil, 1234, h, false)
	if err != nil {
		panic(err)
	}
	fmt.Printf("hist %x\n", hc.Bytes())
	fh := &histogram.FloatHistogram{CounterResetHint: histogram.GaugeType, Count: 7.5, ZeroCount: 0.5, Sum: 5.25, Schema: 0, PositiveSpans: []histogram.Span{{Offset: 0, Length: 2}}, PositiveBuckets: []float64{3.5, 4}}
	fc := chunkenc.NewFloatHistogramChunk()
	fa, _ := fc.Appender()
	_, _, _, err = fa.AppendFloatHistogram(nil, 1234, fh, false)
	if err != nil {
		panic(err)
	}
	fmt.Printf("float %x\n", fc.Bytes())
}

// verify checks frame checksums and uses Prometheus's own chunk decoders.
func verify(endpoint, metric string, end int64) {
	request := &prompb.ReadRequest{Queries: []*prompb.Query{{StartTimestampMs: 0, EndTimestampMs: end, Matchers: []*prompb.LabelMatcher{{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: metric}}}}, AcceptedResponseTypes: []prompb.ReadRequest_ResponseType{prompb.ReadRequest_STREAMED_XOR_CHUNKS}}
	raw, err := request.Marshal()
	if err != nil {
		panic(err)
	}
	response, err := http.Post(endpoint, "application/x-protobuf", bytes.NewReader(snappy.Encode(nil, raw)))
	if err != nil {
		panic(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		body, _ := io.ReadAll(response.Body)
		panic(string(body))
	}
	reader := bufio.NewReader(response.Body)
	var previous labels.Labels
	var lastTime int64
	frames, samples := 0, 0
	var sum float64
	for {
		var frame prompb.ChunkedReadResponse
		size, err := binary.ReadUvarint(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			panic(err)
		}
		if size > 1024*1024 {
			panic("frame exceeds limit")
		}
		var checksum uint32
		if err := binary.Read(reader, binary.BigEndian, &checksum); err != nil {
			panic(err)
		}
		payload := make([]byte, size)
		if _, err := io.ReadFull(reader, payload); err != nil {
			panic(err)
		}
		if crc32.Checksum(payload, crc32.MakeTable(crc32.Castagnoli)) != checksum {
			panic("checksum mismatch")
		}
		if err := frame.Unmarshal(payload); err != nil {
			panic(err)
		}

		if frame.QueryIndex != 0 {
			panic("unexpected query index")
		}
		frames++
		for _, series := range frame.ChunkedSeries {
			current := make(labels.Labels, len(series.Labels))
			for i, l := range series.Labels {
				current[i] = labels.Label{Name: l.Name, Value: l.Value}
				if i > 0 && current[i-1].Name >= l.Name {
					panic("labels are not sorted")
				}
			}
			if previous != nil && labels.Compare(previous, current) > 0 {
				panic("series order decreased")
			}
			if previous == nil || labels.Compare(previous, current) != 0 {
				lastTime = math.MinInt64
			}
			previous = current
			for _, chunk := range series.Chunks {
				decoded, err := chunkenc.FromData(chunkenc.Encoding(chunk.Type), chunk.Data)
				if err != nil {
					panic(err)
				}
				iter := decoded.Iterator(nil)
				for kind := iter.Next(); kind != chunkenc.ValNone; kind = iter.Next() {
					t := iter.AtT()
					if kind == chunkenc.ValFloat {
						_, value := iter.At()
						sum += value
					}
					if t < lastTime {
						panic("sample time decreased")
					}
					if t < chunk.MinTimeMs || t > chunk.MaxTimeMs {
						panic("sample outside chunk bounds")
					}
					lastTime = t
					samples++
				}
				if err := iter.Err(); err != nil {
					panic(err)
				}
			}
		}
	}
	fmt.Printf("verified frames=%d samples=%d sum=%g\n", frames, samples, sum)
}
