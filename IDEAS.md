# Ideas

## Copy compressed HTTP responses directly to S3

Avoid local decompression and zstd recompression by storing the compressed YT
HTTP response (e.g. gzip). Benchmark CPU usage, throughput, and object size.
Trade-off: handle HTTP completion errors and retries ourselves; initially restart
an interrupted table, later consider independent row ranges. Keep the SDK for
metadata and snapshot management.
