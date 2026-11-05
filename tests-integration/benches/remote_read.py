# Copyright 2023 Greptime Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Compare Remote Read server CPU/RSS on Linux, draining responses without buffering.

Use an isolated server. Supply --pid for that server, --table for an existing
metric table and --end-ms for its inclusive upper timestamp. This measures the
server process (not the Python client). Alternate formats to reduce order bias.
Example: python remote_read.py --pid 123 --table bench --end-ms 10000000
"""

import argparse
import http.client
import json
import os
from pathlib import Path
import threading
import time
import urllib.parse


def varint(value):
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def field(number, value):
    if isinstance(value, int):
        return varint(number << 3) + varint(value)
    return varint((number << 3) | 2) + varint(len(value)) + value


def request(table, end, streamed):
    matcher = field(2, b"__name__") + field(3, table.encode())
    query = field(2, end) + field(3, matcher)
    proto = field(1, query) + field(2, int(streamed))
    # A raw Snappy block containing one literal (request sizes are small).
    size = len(proto) - 1
    if size < 60:
        literal = bytes([size << 2])
    else:
        width = (size.bit_length() + 7) // 8
        literal = bytes([(59 + width) << 2]) + size.to_bytes(width, "little")
    return varint(len(proto)) + literal + proto


def process_stats(pid):
    root = Path(f"/proc/{pid}")
    fields = (root / "stat").read_text().rsplit(")", 1)[1].split()
    cpu = (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
    rss = int((root / "statm").read_text().split()[1]) * os.sysconf("SC_PAGE_SIZE")
    return cpu, rss


def measure(args, streamed):
    url = urllib.parse.urlsplit(args.url)
    conn = http.client.HTTPConnection(url.hostname, url.port, timeout=600)
    stop = threading.Event()
    before_cpu, before_rss = process_stats(args.pid)
    peak = [before_rss]

    def sample():
        while not stop.wait(0.01):
            peak[0] = max(peak[0], process_stats(args.pid)[1])

    monitor = threading.Thread(target=sample)
    monitor.start()
    start = time.perf_counter()
    try:
        conn.request("POST", (url.path or "/v1/prometheus/read") + ("?" + url.query if url.query else ""),
                     request(args.table, args.end_ms, streamed),
                     {"Content-Type": "application/x-protobuf", "Content-Encoding": "snappy"})
        response = conn.getresponse()
        if response.status != 200:
            raise RuntimeError((response.status, response.read(4096)))
        content_type = response.getheader("Content-Type", "")
        if streamed != ("x-streamed-protobuf" in content_type):
            raise RuntimeError(f"unexpected negotiated type: {content_type}")
        first = response.read(1)
        first_byte = time.perf_counter() - start
        size = len(first)
        while data := response.read(64 * 1024):
            size += len(data)
            if args.slow_ms:
                time.sleep(args.slow_ms / 1000)
        elapsed = time.perf_counter() - start
        after_cpu, after_rss = process_stats(args.pid)
        peak[0] = max(peak[0], after_rss)
        return {"mode": "streamed" if streamed else "samples", "table": args.table,
                "cpu_seconds": after_cpu - before_cpu, "elapsed_seconds": elapsed,
                "first_byte_seconds": first_byte, "wire_bytes": size,
                "baseline_rss_bytes": before_rss, "peak_rss_bytes": peak[0],
                "rss_increase_bytes": peak[0] - before_rss}
    finally:
        stop.set()
        monitor.join()
        conn.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://127.0.0.1:14000/v1/prometheus/read")
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--end-ms", type=int, required=True)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--slow-ms", type=float, default=0)
    parser.add_argument("--mode", choices=["both", "samples", "streamed"], default="both")
    args = parser.parse_args()
    for repeat in range(args.repeats):
        modes = [False, True] if repeat % 2 == 0 else [True, False]
        if args.mode != "both":
            modes = [args.mode == "streamed"]
        for streamed in modes:
            print(json.dumps(measure(args, streamed)), flush=True)


if __name__ == "__main__":
    main()
