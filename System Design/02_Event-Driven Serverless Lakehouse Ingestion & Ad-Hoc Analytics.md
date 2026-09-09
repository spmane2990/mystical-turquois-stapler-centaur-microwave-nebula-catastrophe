# Scenario 2: Event-Driven Serverless Lakehouse Ingestion & Ad-Hoc Analytics

This design handles high-frequency semi-structured payloads (e.g., IoT telemetry, clickstreams, or partner webhooks) landing in **Amazon S3**, transforming raw files into an optimized columnar Lakehouse accessible via **Amazon Athena** and **Snowflake External Tables** without long-running compute clusters.

---

## 1. Requirements & System Constraints

- **Ingestion Profile:** Thousands of small JSON/GZIP files ($10\text{--}100\text{ KB}$ each) arriving every minute ($50\text{M+} \text{ events/day}$).
- **Query Freshness SLA:** Data queryable via serverless SQL within **$< 5$ minutes** of landing.
- **Cost & Performance Goals:** Eliminate 24/7 idle cluster costs; resolve the **"Small File Problem"** to prevent Athena planning timeouts and runaway S3 GET request costs.
- **Access Patterns:**
- Ad-hoc exploratory SQL and BI dashboards via **Amazon Athena**.
- Financial/reporting analytics via **Snowflake External / Iceberg Tables** pointing to the same S3 files without data duplication.

---

## 2. End-to-End Serverless Architecture

```
[ IoT / Webhook Producers ]
             │
             ▼
[ S3 Raw Landing Bucket ] (Partition: raw/year=YYYY/month=MM/day=DD/hour=HH/)
             │
             │ (ObjectCreated Notification)
             ▼
[ Amazon EventBridge ] ──(Dead Letter Queue: SQS)
             │
             ▼
[ AWS Lambda (Validation & Staging) ]
   - Payload schema validation
   - Bad records routed to Quarantine (S3 DLQ)
   - Writes micro-batched Parquet to S3 Staging
             │
             ▼
[ Micro-Batch Orchestration: AWS Glue ETL / Athena OPTIMIZE ]
   - Scheduled every 15-30 mins via EventBridge / MWAA
   - Compaction: Merges small Parquet files into 256 MB - 512 MB blocks
   - Registers partitions in AWS Glue Data Catalog
             │
             ▼
[ S3 Curated Lakehouse Layer (Parquet / Apache Iceberg) ]
             │
      ┌──────┴─────────────────────────────────┐
      ▼                                        ▼
[ Amazon Athena ]                   [ Snowflake External Layer ]
  - Partition Projection              - External Volumes / Storage Integration
  - Workgroup Quota Limits            - External Tables or Iceberg Tables (Glue Catalog)

```

---

## 3. Storage Layering & File Compaction Strategy

Querying uncompacted small files degrades Athena performance because each file requires separate S3 metadata listings, GET requests, and HTTP handshakes.

| Storage Zone             | S3 Path Format                           | Format & Compression              | Target File Size                        | Retention / Lifecycle                     |
| ------------------------ | ---------------------------------------- | --------------------------------- | --------------------------------------- | ----------------------------------------- |
| **Landing**              | `s3://lake-raw/landing/YYYY/MM/DD/HH/`   | Raw JSON / Gzip                   | Varies ($10\text{ KB} - 200\text{ KB}$) | Expire / Delete after 7 days              |
| **Quarantine (DLQ)**     | `s3://lake-raw/quarantine/err_code=.../` | JSON + Metadata error headers     | Original size                           | Transition to Glacier after 30 days       |
| **Staging**              | `s3://lake-curated/staging/YYYY/MM/DD/`  | Snappy-compressed Parquet         | Micro-batch ($5\text{--}20\text{ MB}$)  | Delete after compaction                   |
| **Curated (Production)** | `s3://lake-curated/telemetry/date=.../`  | ZSTD-compressed Parquet / Iceberg | **$256\text{ MB} - 512\text{ MB}$**     | Retain indefinitely (Intelligent-Tiering) |

---

## 4. Lambda Pre-Validation & Ingestion

To prevent concurrent Lambda execution explosion, do not trigger Lambda directly on individual S3 events. Instead, batch S3 events via **Amazon SQS** or buffer high-throughput streams through **Amazon Kinesis Data Firehose**.

If using direct S3 $\to$ EventBridge $\to$ SQS $\to$ Lambda:

```python
import json
import boto3
import os
from datetime import datetime

s3_client = boto3.client('s3')
QUARANTINE_BUCKET = os.environ['QUARANTINE_BUCKET']

REQUIRED_FIELDS = {"device_id", "timestamp", "payload_version", "metric_value"}

def lambda_handler(event, context):
    valid_records = []

    for record in event['Records']:
        # Parse S3 notification details
        body = json.loads(record['body'])
        for s3_record in body.get('Records', []):
            bucket = s3_record['s3']['bucket']['name']
            key = s3_record['s3']['object']['key']

            # Fetch raw object
            response = s3_client.get_object(Bucket=bucket, Key=key)
            lines = response['Body'].read().decode('utf-8').splitlines()

            for line in lines:
                try:
                    payload = json.loads(line)
                    # Schema contract enforcement
                    if not REQUIRED_FIELDS.issubset(payload.keys()):
                        raise ValueError(f"Missing required fields: {REQUIRED_FIELDS - payload.keys()}")

                    # Attach processing audit attributes
                    payload['_ingested_at'] = datetime.utcnow().isoformat()
                    valid_records.append(payload)

                except Exception as err:
                    # Quarantine poison-pill records
                    s3_client.put_object(
                        Bucket=QUARANTINE_BUCKET,
                        Key=f"quarantine/err_date={datetime.utcnow().strftime('%Y-%m-%d')}/{key.split('/')[-1]}.bad",
                        Body=json.dumps({"error": str(err), "raw_payload": line})
                    )

    # Process or emit batch to staging buffer
    return {"status": "SUCCESS", "valid_count": len(valid_records)}

```

---

## 5. Amazon Athena: Partition Projection & Compaction

Traditional Athena tables require running `MSCK REPAIR TABLE` or execution of Glue Crawlers to register newly added partition folders. Under high write frequencies, both approaches break: crawlers add latency and repair statements scan the whole S3 hierarchy.

**Partition Projection** bypasses metadata lookups entirely by calculating partition paths programmatically based on configured ranges.

### Athena DDL with Partition Projection

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS telemetry_lakehouse.device_telemetry (
    device_id STRING,
    payload_version STRING,
    metric_value DOUBLE,
    _ingested_at TIMESTAMP
)
PARTITIONED BY (
    event_date STRING,
    region STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://lake-curated/telemetry/'
TBLPROPERTIES (
    'has_encrypted_data'='false',
    'projection.enabled'='true',
    -- Date projection configuration
    'projection.event_date.type'='date',
    'projection.event_date.range'='2025/01/01,NOW',
    'projection.event_date.format'='yyyy/MM/dd',
    'projection.event_date.interval'='1',
    'projection.event_date.interval.unit'='DAYS',
    -- Region enum projection
    'projection.region.type'='enum',
    'projection.region.values'='us-east-1,us-west-2,eu-central-1,ap-south-1',
    -- Storage location template mapping
    'storage.location.template'='s3://lake-curated/telemetry/event_date=${event_date}/region=${region}/'
);

```

### Serverless File Compaction (AWS Glue Job / Athena Iceberg)

To compact small files into large columnar blocks without running dedicated EMR clusters:

- **If Hive/Parquet:** Run a periodic serverless Glue Python shell or Spark ETL job grouping small files via:

```python
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame,
    connection_type="s3",
    connection_options={"path": "s3://lake-curated/telemetry/", "partitionKeys": ["event_date", "region"]},
    format="parquet",
    format_options={"compression": "SNAPPY", "useGlueParquetWriter": True}
)

```

- **If using Apache Iceberg tables:** Execute the Athena native compaction command:

```sql
OPTIMIZE telemetry_lakehouse.device_telemetry_iceberg REWRITE DATA USING BIN_PACK
WHERE event_date >= current_date - interval '1' day;

```

---

## 6. Snowflake Integration: External & Iceberg Tables

Expose the curated S3 data to Snowflake without moving data or creating secondary copies.

### Option A: Standard Snowflake External Table

```sql
-- 1. Create External Stage pointing to AWS S3
CREATE OR REPLACE STAGE telemetry_lake_stage
  URL = 's3://lake-curated/telemetry/'
  STORAGE_INTEGRATION = s3_lake_storage_int
  FILE_FORMAT = (TYPE = PARQUET);

-- 2. Define External Table with Auto-Refresh enabled
CREATE OR REPLACE EXTERNAL TABLE analytics_db.public.ext_device_telemetry (
  device_id VARCHAR AS (Value:device_id::VARCHAR),
  metric_value FLOAT AS (Value:metric_value::FLOAT),
  _ingested_at TIMESTAMP AS (Value:_ingested_at::TIMESTAMP),
  event_date DATE AS (TO_DATE(SPLIT_PART(metadata$filename, '/', 3), 'YYYY-MM-DD'))
)
PARTITION BY (event_date)
LOCATION = @telemetry_lake_stage
AUTO_REFRESH = TRUE
FILE_FORMAT = (TYPE = PARQUET);

```

### Option B: Snowflake Iceberg Table (External Glue Metastore)

For superior read performance and ACID snapshot isolation, point Snowflake directly to the **AWS Glue Data Catalog** backing Iceberg tables:

```sql
CREATE OR REPLACE ICEBERG TABLE analytics_db.public.iceberg_device_telemetry
  EXTERNAL_VOLUME = 's3_lakehouse_ext_vol'
  CATALOG = 'aws_glue_catalog_integration'
  CATALOG_TABLE_NAME = 'device_telemetry_iceberg';

```

---

## 7. Operational Guardrails & Failure Modes

- **S3 API Throttling (HTTP 503 `SlowDown`):**
- _Cause:_ Writing thousands of small files into a single prefix exceeds S3 limits ($3{,}500\text{ PUT/POST/DELETE}$ per second per prefix).
- _Mitigation:_ Distribute write paths using hash prefixes or date-hour structures (`s3://bucket/{hash}/...`), and buffer writes through SQS/Kinesis.

- **Athena Query Cost Runaway:**
- _Cause:_ Analysts executing unbounded `SELECT *` scans across full history.
- _Mitigation:_ Enforce **Workgroup Query Limits** (e.g., auto-cancel queries exceeding $10\text{ GB}$ scanned) and mandate partition filters (`event_date`) via Athena pre-execution hooks.

- **Schema Drift / Malformed Payloads:**
- _Cause:_ Upstream firmware pushes untyped fields, breaking downstream Parquet serialization.
- _Mitigation:_ The Lambda validation boundary intercepts schema violations before files hit curated storage, isolating them into the quarantine path with CloudWatch alarms alert triggers.
