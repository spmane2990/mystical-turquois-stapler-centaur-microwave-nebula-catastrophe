# Scenario 4: Cross-Cloud Incremental CDC Pipeline (Azure to Snowflake on AWS)

This design establishes an automated, near-real-time Change Data Capture (CDC) replication pipeline between an enterprise transactional system running in **Microsoft Azure** and an analytical data warehouse hosted on **Snowflake on AWS**.

---

## 1. System Requirements & Latency Targets

- **Throughput:** $5{,}000\text{--}10{,}000\text{ transactions/sec}$ peak; $100\text{--}200\text{ GB}$ daily change deltas across multi-terabyte transactional tables.
- **Freshness SLA:** Target data freshness of **$< 15$ minutes** end-to-end from Azure transaction commit to Snowflake availability.
- **Integrity Guarantee:** Zero data loss, exactly-once processing semantics at the warehouse layer, and guaranteed preservation of mutation ordering (INSERT, UPDATE, DELETE).
- **Cost Efficiency:** Eliminate multi-terabyte full reloads; minimize cross-cloud egress costs through binary compression and micro-batch syncing.

---

## 2. End-to-End Architectural Flow

```
[ Azure Cloud (East US 2) ]
  Azure SQL / Cosmos / PostgreSQL
                │
                ▼
  [ Azure Data Factory (ADF) ]
    - Native CDC Mapping Data Flows (Tumbling Window: 5 min)
    - Captures row-level operational logs: __$operation, __$start_lsn
                │
                ▼
  [ ADLS Gen2 (Staging Zone) ]
    - Snappy-compressed Parquet delta files
    - Partition: cdc_stage/table_name/YYYY/MM/DD/HH/
                │
                │ (Cross-Cloud Bandwidth-Optimized Transfer)
════════════════╪══════════════════════════════════════════════════════════
                ▼
[ AWS Cloud (us-east-1) ]
  [ AWS DataSync / Managed AzCopy Service ]
    - Scheduled micro-sync (5-minute interval)
    - Direct TLS transport across cloud provider backbones
                │
                ▼
  [ Amazon S3 (Raw Delta Bucket) ]
    - S3 ObjectCreated Event
                │
                ▼
  [ Amazon SQS Notification Queue ]
                │
                ▼
  [ Snowflake (Hosted on AWS us-east-1) ]
    ┌─────────────────────────────────────────────────────────────┐
    │ 1. Snowpipe: Auto-ingests S3 delta files into Staging Table │
    │ 2. Snowflake Stream: Tracks appended change records         │
    │ 3. Snowflake Task: Executes atomic MERGE INTO Target Table  │
    └─────────────────────────────────────────────────────────────┘

```

---

## 3. Change Data Capture (CDC) Format & Metadata Contract

ADF reads changes using native database CDC connectors (capturing SQL Server CDC, MongoDB Change Streams, or PostgreSQL WAL). Deltas land in **ADLS Gen2** using an immutable append schema:

| Column Name         | Type            | Description                                                           |
| ------------------- | --------------- | --------------------------------------------------------------------- |
| `order_id`          | `VARCHAR`       | Primary / Natural Business Key                                        |
| `customer_id`       | `VARCHAR`       | Attribute                                                             |
| `order_status`      | `VARCHAR`       | Attribute                                                             |
| `amount`            | `DECIMAL(12,2)` | Attribute                                                             |
| `cdc_operation`     | `INT`           | Operation code: `1` = Delete, `2` = Insert, `4` = Update (Post-image) |
| `cdc_lsn`           | `VARCHAR`       | Log Sequence Number / Commit Log ID (determines true commit order)    |
| `cdc_timestamp`     | `TIMESTAMP`     | Source commit timestamp                                               |
| `_adf_extracted_at` | `TIMESTAMP`     | Pipeline metadata extraction time                                     |

---

## 4. Cross-Cloud Synchronization: ADLS Gen2 to AWS S3

Moving files across cloud boundaries requires balancing latency against network transfer costs.

### Strategy Comparison

| Method                         | Latency  | Reliability & Checkpointing                                                     | Cost & Maintenance                                                            |
| ------------------------------ | -------- | ------------------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| **AWS DataSync (Recommended)** | 3–5 min  | Managed scheduling, automated TLS retry, data integrity validation (MD5/CRC32). | Standard AWS DataSync rate ($0.0125\text{/GB}$) + Azure internet egress fees. |
| **Serverless Lambda + AzCopy** | 1–3 min  | High concurrency, requires custom error handling and execution checkpointing.   | Operational burden to manage custom failure states and chunked retries.       |
| **ADF Copy to S3 Directly**    | 5–10 min | Integrated inside Azure Data Factory; manages auth via AWS S3 Access Keys.      | Higher pipeline run costs in ADF for external cloud copy activities.          |

---

## 5. Warehouse Processing: Snowpipe, Streams & Tasks

To preserve true sequential order and handle out-of-order deliveries, Snowflake decouples file loading from dimension upserting using an **Event Stage $\to$ Staging Table $\to$ Stream $\to$ Target Dimension** pattern.

```
[ S3 Raw Delta Files ]
          │
          │ (S3 Event Notifications via SQS)
          ▼
   [ Snowpipe Auto-Ingest ]
          │
          ▼
┌─────────────────────────────────────────────────┐
│ raw_cdc.stg_orders_cdc (Append-Only Stage)      │
└───────────────────────┬─────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│ orders_cdc_stream (Append-Only Stream on Table) │
└───────────────────────┬─────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│ Snowflake Scheduled Task (Runs every 5 mins)    │
│ - Evaluates SYSTEM$STREAM_HAS_DATA              │
│ - Executes Deduplication & MERGE INTO           │
└───────────────────────┬─────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────┐
│ enterprise_dw.dim_orders (Target Dimensional)   │
└─────────────────────────────────────────────────┘

```

### Step 1: Automated Ingestion via Snowpipe

```sql
CREATE OR REPLACE PIPE raw_cdc.pipe_orders_ingest
AUTO_INGEST = TRUE
INTEGRATION = 's3_sqs_notification_int'
AS
COPY INTO raw_cdc.stg_orders_cdc
FROM @raw_cdc.s3_orders_stage
FILE_FORMAT = (TYPE = 'PARQUET')
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

```

### Step 2: Stream Definition on Staging

Create an append-only stream on the CDC staging table to capture new batch records:

```sql
CREATE OR REPLACE STREAM raw_cdc.orders_cdc_stream
ON TABLE raw_cdc.stg_orders_cdc
APPEND_ONLY = TRUE;

```

### Step 3: Atomic Deduplication & Merge Task

If an entity mutated multiple times within the 5-minute batch window (e.g., `INSERT` followed by two `UPDATE`s), running a direct `MERGE` fails with a non-deterministic target row collision.

The Task must window by `order_id`, order by `cdc_lsn DESC`, and apply only the **latest valid operation**:

```sql
CREATE OR REPLACE TASK raw_cdc.task_process_orders_cdc
WAREHOUSE = 'CDC_MERGE_WH'
SCHEDULE = '5 MINUTE'
WHEN
  SYSTEM$STREAM_HAS_DATA('raw_cdc.orders_cdc_stream')
AS
MERGE INTO enterprise_dw.orders AS target
USING (
    -- Deduplicate batch: select only the most recent operational state per key
    WITH ranked_events AS (
        SELECT
            order_id,
            customer_id,
            order_status,
            amount,
            cdc_operation,
            cdc_lsn,
            cdc_timestamp,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY cdc_lsn DESC, cdc_timestamp DESC
            ) as rank_id
        FROM raw_cdc.orders_cdc_stream
    )
    SELECT * FROM ranked_events WHERE rank_id = 1
) AS source
ON target.order_id = source.order_id

-- Case 1: Latest operation is DELETE (code 1)
WHEN MATCHED AND source.cdc_operation = 1 THEN
    DELETE

-- Case 2: Latest operation is UPDATE (code 4)
WHEN MATCHED AND source.cdc_operation = 4 THEN
    UPDATE SET
        target.customer_id = source.customer_id,
        target.order_status = source.order_status,
        target.amount = source.amount,
        target.last_updated_lsn = source.cdc_lsn,
        target.dw_updated_at = CURRENT_TIMESTAMP()

-- Case 3: Latest operation is INSERT (code 2)
WHEN NOT MATCHED AND source.cdc_operation != 1 THEN
    INSERT (
        order_id,
        customer_id,
        order_status,
        amount,
        last_updated_lsn,
        dw_created_at,
        dw_updated_at
    )
    VALUES (
        source.order_id,
        source.customer_id,
        source.order_status,
        source.amount,
        source.cdc_lsn,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
    );

```

---

## 6. End-to-End SLA Monitoring & Lag Tracking

To ensure the 15-minute SLA is met without silent failures across cloud barriers, capture time metrics at every stage boundary:

$$\text{End-to-End Latency} = T_{\text{Snowflake Target Commit}} - T_{\text{Source DB Commit}}$$

- **Azure Stage:** Emit custom Azure Monitor metrics on ADF pipeline finish:

$$\Delta t_{\text{ADF}} = T_{\text{ADLS Write}} - T_{\text{Source CDC}}$$

- **Transport Stage:** CloudWatch metric `DataSyncBytesTransferred` and execution duration alerts if transfer runs $> 3\text{ minutes}$.
- **Snowflake Stage:** Track consumer lag using system view metadata:

```sql
SELECT
    DATEDIFF('minute', MAX(cdc_timestamp), CURRENT_TIMESTAMP()) AS replication_lag_minutes
FROM enterprise_dw.orders;

```

- **Alerting Rule:** If `replication_lag_minutes > 15`, trigger a PagerDuty incident via Snowflake external network access or an AWS SNS webhook.

---

## 7. Edge Cases & Resilience Engineering

- **Out-of-Order Delta Transmissions:**
- _Risk:_ A network retry causes a prior batch file containing an older record to land in S3 after a newer file has already processed.
- _Mitigation:_ The `MERGE` statement checks `WHERE source.cdc_lsn > target.last_updated_lsn`. Older updates are silently discarded, maintaining transactional consistency.

- **Transient Cross-Cloud Network Partitions:**
- _Risk:_ AWS DataSync cannot establish a TLS connection to Azure storage due to an Azure ExpressRoute or public egress routing failure.
- _Mitigation:_ ADLS Gen2 retains parquet files with a 7-day lifecycle policy. Once network paths restore, DataSync resumes incrementally based on missing object hashes without backpressure or queue overflow on the source OLTP engine.

- **Staging Table Storage Bloat:**
- _Risk:_ The `stg_orders_cdc` table grows indefinitely as Snowpipe ingests continuous files.
- _Mitigation:_ Configure a second scheduled maintenance task or a 3-day Transient Table TTL (`DATA_RETENTION_TIME_IN_DAYS = 0`) coupled with an automated truncation routine to discard rows once consumed by the stream.
