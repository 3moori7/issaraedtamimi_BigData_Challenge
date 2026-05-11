# Scaling Analysis: News Pulse Pipeline

## Which step would break first if input grew 1000×?

The **ingester** would break first. Currently fetching ~150 headlines per 30-second batch, a 1000× increase means ~150,000 headlines per batch. The network I/O bottleneck dominates—RSS parsing and HTTP requests to four feeds would become network-bound. A single thread serially fetching feeds creates severe latency.

## Spark feature to fix it: Adaptive Query Execution (AQE) + Partitioned Ingestion

Deploy **parallel ingester threads** (one per feed) with exponential backoff. Partition incoming files by feed source. On the Spark side, enable **Adaptive Query Execution** (`spark.sql.adaptive.enabled=true`) and switch to **micro-batching intervals** (default 500ms) to handle micro-batches efficiently. For massive ingest, migrate to **Kafka** as a buffer layer instead of file-source streaming, enabling horizontal scaling and fault tolerance beyond single-machine limits.
