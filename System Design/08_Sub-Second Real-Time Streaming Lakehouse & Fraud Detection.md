# Scenario 8: Sub-Second Real-Time Streaming Lakehouse & Fraud Detection

This scenario covers building an ultra-low-latency, event-driven streaming lakehouse capable of processing continuous clickstreams and payment events. It combines **AWS Kinesis / Azure Event Hubs**, **Databricks Structured Streaming** (with RocksDB stateful stream-stream processing), and **Snowpipe Streaming** to achieve sub-second analytical ingestion and real-time fraud scoring without relying on traditional micro-batch file staging.

---

## 1. System Requirements & Latency Budgets

- **Throughput:** $100{,}000\text{--}250{,}000\text{ events/sec}$ peak ($15\text{--}30\text{ MB/sec}$).
- **End-to-End Latency Target:**
- **Operational Fraud Alerting:** $< 1\text{ second}$ (alert published back to payment gateway).
- **Warehouse Ingestion SLA:** $< 5\text{ seconds}$ from event emission to queryable state in Snowflake.

- **Stateful Windows:** 10-minute sliding windows tracking user transaction velocity, geo-velocity (impossible travel speed), and IP anomaly scores.
- **Fault Tolerance:** End-to-end **exactly-once** processing semantics; zero lost transactions on worker node crash.

```
+------------------------------------+--------------------------+
| Pipeline Stage                     | Latency Budget           |
+------------------------------------+--------------------------+
| Ingestion & Buffer (Kinesis/Hubs)  | 50 - 150 ms              |
| Complex Event Processing (Spark)   | 400 - 800 ms             |
| Fraud Scoring & Alert Emit         | 50 - 100 ms              |
| Warehouse Flush (Snowpipe Stream)  | 1,000 - 3,000 ms         |
+------------------------------------+--------------------------+
| Total End-to-End SLA               | < 5.0 Seconds            |
+------------------------------------+--------------------------+

```

---

## 2. End-to-End Architecture

```
[ Mobile / Web App / POS Terminals ]
                 │
                 ▼
[ Ingestion Layer: AWS Kinesis Data Streams / Azure Event Hubs ]
                 │
                 ├───────────────────────────────────────────────────────┐
                 │                                                       │
                 ▼ (Real-Time Fraud Evaluation)                          ▼ (Low-Cost Raw Archive)
┌─────────────────────────────────────────────────────────┐   ┌───────────────────────────┐
│ Databricks Structured Streaming Engine                  │   │ S3 / ADLS Gen2 Bucket     │
│  - Engine: Spark Structured Streaming (Trigger: Continuous)││ (Raw Dump via Firehose)   │
│  - State Management: RocksDB State Store                │   └───────────────────────────┘
│  - Stateful Join: Transactions ⟕ Clickstream/Geo        │
│  - ML Inference: ONNX / MLflow Fraud Model              │
└───────────────┬─────────────────────────┬───────────────┘
                │                         │
  (Flagged / High Risk > 0.85)            │ (Enriched Valid Stream)
                │                         │
                ▼                         ▼
┌───────────────────────────────┐ ┌────────────────────────────────────────┐
│ Amazon SNS / SQS Alert Emitter│ │ Snowflake Ingestion Path               │
│ -> Payment Gateway Webhook    │ │  - Snowpipe Streaming SDK (Java/Python)│
│ -> Immediate Transaction Lock │ │  - Bypasses S3 intermediate files      │
└───────────────────────────────┘ └──────────────────┬─────────────────────┘
                                                     │
                                                     ▼
                                  ┌────────────────────────────────────────┐
                                  │ Snowflake Real-Time Layer              │
                                  │  - Staging: Ingest-Only Streaming Table│
                                  │  - Dynamic Tables (1-minute refresh lag│
                                  │  - Operational BI & Anomaly Dashboards │
                                  └────────────────────────────────────────┘

```

---

## 3. Stateful Stream Processing: Databricks & RocksDB

Default in-memory state stores in Spark crash with Out-Of-Memory (OOM) errors when keeping millions of keys over sliding windows. Use the **RocksDB StateStore Provider**, which offloads checkpointed state to off-heap native memory and local SSDs.

### PySpark Stateful Stream-Stream Join & Anomaly Detection

```python
from pyspark.sql import functions as F
from pyspark.sql.types import *

# 1. Configure RocksDB State Store
spark.conf.set(
    "spark.sql.streaming.stateStore.providerClass",
    "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
)

# 2. Read Ingestion Streams with Watermarking
transactions_stream = (
    spark.readStream.format("kinesis")
    .option("streamName", "prod-payment-events")
    .option("region", "us-east-1")
    .option("initialPosition", "LATEST")
    .load()
    .select(F.from_json(F.col("data").cast("string"), transaction_schema).alias("tx"))
    .select("tx.*")
    # Watermark handles late-arriving events up to 2 minutes
    .withWatermark("event_time", "2 minutes")
)

clickstream_stream = (
    spark.readStream.format("kinesis")
    .option("streamName", "prod-clickstream-events")
    .option("region", "us-east-1")
    .option("initialPosition", "LATEST")
    .load()
    .select(F.from_json(F.col("data").cast("string"), click_schema).alias("click"))
    .select("click.*")
    .withWatermark("click_time", "2 minutes")
)

# 3. Stateful Stream-Stream Interval Join (Verify user was active within 5 mins of payment)
enriched_stream = transactions_stream.join(
    clickstream_stream,
    F.expr("""
        tx.user_id = click.user_id AND
        click_time BETWEEN event_time - INTERVAL 5 MINUTES AND event_time
    """),
    joinType="leftOuter"
)

# 4. Sliding Window Anomaly Detection (Velocity Check)
# Flags users attempting > 5 transactions in a 3-minute sliding window
velocity_check = (
    enriched_stream
    .groupBy(
        F.window("event_time", "3 minutes", "30 seconds"),
        F.col("tx.user_id")
    )
    .agg(
        F.count("transaction_id").alias("tx_count_3m"),
        F.sum("amount").alias("total_amount_3m"),
        F.collect_set("ip_country").alias("distinct_countries")
    )
    .withColumn(
        "is_geo_anomaly",
        F.size("distinct_countries") > 1 # Impossible travel in 3 minutes
    )
)

```

---

## 4. Sub-Second Warehouse Ingestion: Snowpipe Streaming

Traditional Snowpipe polls an S3 stage and writes micro-batched Parquet files, creating a latency floor of $60\text{--}120\text{ seconds}$.

**Snowpipe Streaming** uses a low-level SDK that connects directly to the Snowflake internal load balancer via gRPC/HTTP2, writing raw serialized rows into internal memory buffers (channel blobs). Data becomes queryable in Snowflake within $1\text{--}3\text{ seconds}$.

```
Traditional Snowpipe (Latency: 60 - 120s):
[ Spark Stream ] ──(Write File)──► [ S3 Bucket ] ──(SQS Event)──► [ Snowpipe Copy ] ──► [ Snowflake ]

Snowpipe Streaming SDK (Latency: 1 - 3s):
[ Spark Stream / Kafka Sink ] ──(Streaming SDK Direct Stream Insert)──► [ Snowflake Channels ]

```

### Snowpipe Streaming Java/Scala Integration (Inside Spark `foreachBatch` Sink)

```java
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.InsertValidationResponse;

public class SnowflakeDirectStreamSink {
    private SnowflakeStreamingIngestChannel channel;

    public void initChannel() {
        SnowflakeStreamingIngestClient client = SnowflakeStreamingIngestClientFactory
            .builder("CLIENT_PROD_FRAUD_STREAM")
            .setProperties(snowflakeProperties)
            .build();

        OpenChannelRequest request = OpenChannelRequest.builder("FRAUD_EVENTS_CHANNEL")
            .setDBName("PROD_LAKEHOUSE")
            .setSchemaName("STREAMING_INGEST")
            .setTableName("STG_REALTIME_PAYMENTS")
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

        this.channel = client.openChannel(request);
    }

    public void streamInsert(Map<String, Object> event, String offsetToken) {
        // offsetToken acts as an atomic checkpoint inside Snowflake for idempotency
        InsertValidationResponse response = channel.insertRow(event, offsetToken);
        if (response.hasErrors()) {
            throw new RuntimeException("Insert row failed: " + response.getInsertErrors().toString());
        }
    }
}

```

---

## 5. Warehouse Transformation: Snowflake Dynamic Tables

Instead of running continuous, resource-draining tasks or complex merge statements, configure **Dynamic Tables**. They continuously track streaming ingestion tables and maintain materialized views based on a declared **Target Lag**.

```
[ STG_REALTIME_PAYMENTS (Raw Ingestion) ]
                    │
                    ▼ (Continuous Refresh, Target Lag = 1 Minute)
[ DT_HIGH_RISK_ACCOUNTS (Dynamic Table) ]
                    │
                    ▼
[ BI Dashboards / SecOps Real-Time Alerting ]

```

### Dynamic Table DDL

```sql
CREATE OR REPLACE DYNAMIC TABLE prod_lakehouse.analytics.dt_fraud_monitoring
  TARGET_LAG = '1 MINUTE'
  WAREHOUSE = 'STREAMING_SERVING_WH'
AS
SELECT
    user_id,
    COUNT(transaction_id) AS velocity_10m,
    SUM(amount) AS total_amount_10m,
    MAX(fraud_risk_score) AS peak_risk_score,
    ARRAY_UNIQUE_AGG(ip_country) AS origin_countries,
    MAX(event_time) AS last_event_timestamp
FROM prod_lakehouse.streaming_ingest.stg_realtime_payments
WHERE event_time >= DATEADD('minute', -10, CURRENT_TIMESTAMP())
GROUP BY user_id
HAVING velocity_10m >= 5 OR peak_risk_score >= 0.85;

```

---

## 6. End-to-End Exactly-Once Semantics

Maintaining exactly-once guarantees across streaming ingestion engines, cloud object stores, and the data warehouse requires coordinated checkpointing and offset tokens:

1. **Upstream Commit Coordination:** Kinesis/Event Hubs acts as the append-only commit log with partition sequence numbers.
2. **Spark Checkpointing:** The Databricks structured streaming engine maintains atomic transaction state in `s3://lakehouse-checkpoints/fraud_processing/`.
3. **Snowflake Channel Offset Tokens:**

- Snowpipe Streaming stores an engine-provided `offsetToken` (the partition sequence ID) alongside the channel metadata inside Snowflake.
- If a Spark worker crashes mid-batch, the restarted executor calls `channel.getLatestCommittedOffsetToken()`.
- Replay starts **strictly** from the uncommitted token, discarding duplicated rows at the Snowflake channel boundary before they commit.

---

## 7. Failure Modes & Mitigations

| Failure Scenario                  | Real-World Impact                                                                                       | Engineering Mitigation                                                                                                                                                    |
| --------------------------------- | ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Kinesis Hot Sharding**          | A single user/bot spams requests, overwhelming a single stream shard and causing consumer lag to surge. | Compute partition keys using a hash of `(user_id, hour)` or add random salt bits to high-frequency keys; scale shards using Kinesis On-Demand or dynamic shard splitters. |
| **RocksDB State Disk Bloat**      | High volume of late-arriving events forces state retention to expand, filling worker NVMe storage.      | Enforce aggressive **watermarking** (`withWatermark("event_time", "2 minutes")`). Spark drops state entries immediately once the watermark passes.                        |
| **Snowpipe Channel Invalidation** | Network drop or cluster restart causes Snowflake channel state to desynchronize.                        | Re-open the channel via `OpenChannelRequest`. The Snowflake client SDK refreshes authentication credentials and re-aligns channel offset markers automatically.           |
| **Poison-Pill Payloads**          | Malformed JSON fails parser, halting the streaming query.                                               | Use `from_json` with permissive mode; route unparseable rows into a `_corrupt_record` column and redirect them downstream to an S3 quarantine prefix.                     |
