# Scenario 15: Real-Time Telemetry & Observability Pipeline

This design establishes a centralized, high-throughput **Telemetry and Observability Lakehouse** that unifies the "Three Pillars of Observability" (Logs, Metrics, Traces) across a heterogeneous data platform (**AWS MWAA, Databricks, Snowflake, Azure ADF, and Lambda**).

It replaces disparate vendor consoles with an **OpenTelemetry (OTel)** standard pipeline, streaming telemetry into an optimized Lakehouse queried via **Amazon Athena** and visualized in **Grafana**, while powering automated anomaly detection for pipeline health.

---

## 1. System Requirements & Operational Targets

- **Ingestion Scale:** Handle $50{,}000\text{--}150{,}000\text{ events/sec}$ peak ($1.5\text{--}3\text{ TB}$ daily) across unstructured log lines, structured metrics, and distributed OTel spans.
- **Latency Budgets:**
- **Alerting SLA:** Automated alert firing within **$< 60\text{ seconds}$** of a pipeline failure, task hang, or data freshness breach.
- **End-to-End Query Latency:** Queryable via SQL within **$< 5\text{ minutes}$** for root-cause incident triage.

- **Trace Context Propagation:** Trace an end-to-end data transaction using a single `trace_id`:

$$\text{Source Webhook} \xrightarrow{\text{trace\_id}} \text{Lambda Ingest} \xrightarrow{\text{trace\_id}} \text{Databricks Bronze/Silver} \xrightarrow{\text{trace\_id}} \text{Snowflake Gold}$$

- **Retention & Cost Optimization:** Hot searchable layer ($15\text{ days}$), warm analytical layer ($90\text{ days}$), and cold compliance archive ($365\text{ days}$) using automated Parquet compaction and S3 Lifecycle tiering.

---

## 2. End-to-End Observability Architecture

```
[ Ingestion Sources ]
  - MWAA Tasks (Python Logging + Airflow Callbacks)
  - Databricks Clusters (log4j2 + Spark Listeners + Init Scripts)
  - Snowflake Events (EVENT_USAGE / ACCESS_HISTORY / Notification Webhooks)
  - Azure ADF / AWS Lambda (OpenTelemetry SDK instrumentation)
                             │
                             ▼
┌─────────────────────────────────────────────────────────┐
│ Ingestion & Transport Layer                             │
│   - OpenTelemetry Collector DaemonSet / Edge Agents     │
│   - AWS Kinesis Data Streams / Amazon Firehose          │
│   - Decoupled buffering to survive downstream backpressure│
└────────────────────────────┬────────────────────────────┘
                             │
       ┌─────────────────────┴─────────────────────┐
       │ (High-Priority Anomaly Path)              │ (Continuous Micro-Batch Path)
       ▼                                           ▼
┌───────────────────────────────┐ ┌──────────────────────────────────────┐
│ Stream Alerting (Lambda + SNS)│ │ Ingestion Compute (Databricks / Glue)│
│  - Parses LogLevel = CRITICAL │ │  - Micro-batch streaming (2 min)     │
│  - CloudWatch Alarms          │ │  - Schema enforcement (OTel SemConv) │
│  - Slack / PagerDuty On-Call  │ │  - Snappy/Zstandard Parquet Compaction│
└───────────────────────────────┘ └──────────────────┬───────────────────┘
                                                     │
                                                     ▼
┌────────────────────────────────────────────────────────────────────────┐
│ Unified Observability Lakehouse (S3 / Delta Lake)                      │
│   s3://observability-lakehouse/                                        │
│     ├── logs/year=YYYY/month=MM/day=DD/service=...                     │
│     ├── metrics/metric_family=.../                                     │
│     └── traces/trace_date=YYYY-MM-DD/                                  │
└──────────────────┬─────────────────────────────────┬───────────────────┘
                   │                                 │
                   ▼                                 ▼
┌──────────────────────────────────────┐ ┌───────────────────────────────┐
│ Interactive SQL Triage (Athena)      │ │ Unified Dashboards (Grafana)  │
│  - AWS Glue Catalog integration      │ │  - Athena Plugin for Grafana  │
│  - Fast partition pruning via OTel   │ │  - End-to-end trace waterfall │
│  - Regex / JSON parse acceleration   │ │  - Pipeline SLA burn-up charts│
└──────────────────────────────────────┘ └───────────────────────────────┘

```

---

## 3. Standardization: OpenTelemetry (OTel) Schema Contract

Unstructured plaintext logs degrade query performance. Every service must serialize its telemetry into the **OpenTelemetry Semantic Conventions** before flushing to storage.

### Standard Observability Record Schema (Parquet)

| Field Name      | Type                  | Description                                                                |
| --------------- | --------------------- | -------------------------------------------------------------------------- |
| `timestamp`     | `TIMESTAMP`           | Event emission timestamp (UTC).                                            |
| `trace_id`      | `VARCHAR(32)`         | 128-bit global transaction identifier shared across all hops.              |
| `span_id`       | `VARCHAR(16)`         | 64-bit identifier for the specific task/execution unit.                    |
| `service_name`  | `VARCHAR(64)`         | System originating the log (e.g., `mwaa-orchestrator`, `databricks-scd2`). |
| `environment`   | `VARCHAR(16)`         | `prod`, `stage`, or `dev`.                                                 |
| `severity_text` | `VARCHAR(16)`         | `DEBUG`, `INFO`, `WARN`, `ERROR`, `FATAL`.                                 |
| `body`          | `STRING`              | Normalized human-readable message or serialized JSON.                      |
| `attributes`    | `MAP<STRING, STRING>` | High-cardinality tags: `dag_id`, `task_id`, `cluster_id`, `user_id`.       |
| `duration_ms`   | `DOUBLE`              | Execution runtime (for metric/trace spans).                                |

---

## 4. Telemetry Extraction Across the Core Data Stack

### A. AWS MWAA (Airflow) Trace & Log Forwarding

Configure `airflow.cfg` in MWAA to pipe logs directly into CloudWatch, and instrument DAG tasks using custom callbacks that inject the W3C TraceContext headers:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import json
import os
import uuid

logger = logging.getLogger("airflow.task")

def execute_instrumented_spark_job(**context):
    # Extract or generate global trace context
    dag_run_id = context['dag_run'].run_id
    task_id = context['task'].task_id
    trace_id = context['dag_run'].conf.get('trace_id', uuid.uuid4().hex)

    # Structure payload matching OTel schema
    telemetry_payload = {
        "trace_id": trace_id,
        "span_id": uuid.uuid4().hex[:16],
        "service_name": "mwaa-orchestrator",
        "severity_text": "INFO",
        "body": f"Starting task execution: {task_id}",
        "attributes": {
            "dag_id": context['dag'].dag_id,
            "task_id": task_id,
            "execution_date": context['execution_date'].isoformat(),
            "environment": os.getenv("ENVIRONMENT", "prod")
        }
    }

    # Forward to CloudWatch / Kinesis collector
    logger.info(json.dumps(telemetry_payload))

    # Pass trace_id downstream into Databricks job parameters
    return {"propagated_trace_id": trace_id}

```

### B. Databricks Log4j2 & Spark Listener

To capture internal Spark worker metrics without modifying individual notebooks, attach a custom cluster init script that writes structured JSON to an S3 logging bucket:

```xml
<!-- log4j2.properties snippet packaged onto Databricks driver -->
appender.s3_json.type = RollingFile
appender.s3_json.name = OTelJsonAppender
appender.s3_json.fileName = /local_disk0/tmp/spark_otel.log
appender.s3_json.filePattern = /local_disk0/tmp/spark_otel-%d{yyyy-MM-dd-HH}.log.gz
appender.s3_json.layout.type = JsonLayout
appender.s3_json.layout.compact = true
appender.s3_json.layout.eventEol = true
appender.s3_json.layout.additionalFields = service_name=databricks-engine,environment=prod

```

### C. Snowflake Account Telemetry Sync

Ingest Snowflake warehouse metering, query errors, and audit trails by running an automated Snowpipe task tailing `SNOWFLAKE.ACCOUNT_USAGE`:

```sql
-- Unload failed and long-running query spans into S3 Observability stage
CREATE OR REPLACE TASK telemetry.task_export_snowflake_spans
WAREHOUSE = 'OBSERVABILITY_WH'
SCHEDULE = '5 MINUTE'
AS
COPY INTO @stage_observability_sink/snowflake_spans/
FROM (
    SELECT
        CURRENT_TIMESTAMP() AS timestamp,
        MD5(query_id) AS trace_id,
        LEFT(query_id, 16) AS span_id,
        'snowflake-warehouse' AS service_name,
        'prod' AS environment,
        CASE
            WHEN execution_status = 'SUCCESS' THEN 'INFO'
            ELSE 'ERROR'
        END AS severity_text,
        COALESCE(error_message, 'Query completed successfully') AS body,
        OBJECT_CONSTRUCT(
            'query_id', query_id,
            'warehouse_name', warehouse_name,
            'user_name', user_name,
            'rows_produced', rows_produced,
            'compilation_time_ms', compilation_time,
            'execution_time_ms', execution_time
        ) AS attributes,
        total_elapsed_time AS duration_ms
    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
    WHERE start_time >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
)
FILE_FORMAT = (TYPE = 'PARQUET')
OVERWRITE = FALSE;

```

---

## 5. Streaming Ingestion & Storage Layout (AWS Kinesis + Glue)

Telemetry lands continuously through **Amazon Kinesis Data Firehose**, which dynamically partitions files by timestamp and service name, applying Snappy compression and creating files sized between $128\text{ MB}$ and $256\text{ MB}$ to eliminate the "small file problem."

```
s3://enterprise-observability-lakehouse/
  └── telemetry_logs/
      └── service_name=mwaa-orchestrator/
          └── date=2026-09-10/
              ├── part-0001.snappy.parquet
              └── part-0002.snappy.parquet

```

### Athena DDL with Partition Projection

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS observability.unified_telemetry (
    timestamp TIMESTAMP,
    trace_id STRING,
    span_id STRING,
    severity_text STRING,
    body STRING,
    attributes MAP<STRING, STRING>,
    duration_ms DOUBLE
)
PARTITIONED BY (
    service_name STRING,
    date STRING
)
STORED AS PARQUET
LOCATION 's3://enterprise-observability-lakehouse/telemetry_logs/'
TBLPROPERTIES (
    'projection.enabled'='true',
    'projection.service_name.type'='enum',
    'projection.service_name.values'='mwaa-orchestrator,databricks-engine,snowflake-warehouse,lambda-ingest',
    'projection.date.type'='date',
    'projection.date.range'='2026-01-01,NOW',
    'projection.date.format'='yyyy-MM-dd',
    'storage.location.template'='s3://enterprise-observability-lakehouse/telemetry_logs/service_name=${service_name}/date=${date}/'
);

```

---

## 6. Root-Cause Incident Triage & Grafana Correlation

When an on-call engineer receives an alert indicating that a production table failed to update, they trace the entire lineage of that execution run using the shared `trace_id` in Amazon Athena.

### End-to-End Cross-System Root Cause Query

```sql
SELECT
    timestamp,
    service_name,
    severity_text,
    body,
    attributes['task_id'] AS task_name,
    attributes['error_message'] AS error_detail,
    duration_ms
FROM observability.unified_telemetry
WHERE trace_id = 'c4b82d3f9a714e82b9e6e1871a2810a9'
ORDER BY timestamp ASC;

```

### Execution Trace Result Waterfall

```
Timeline (c4b82d3f9a714e82b9e6e1871a2810a9):
[02:00:01] mwaa-orchestrator     [INFO]  Task: trigger_spark_ingest started.
[02:00:03] databricks-engine     [INFO]  PySpark cluster initialized. Allocating 4 nodes.
[02:01:45] databricks-engine     [INFO]  Processed 1,200,000 records into Silver.
[02:01:50] mwaa-orchestrator     [INFO]  Task: trigger_snowflake_gold_merge started.
[02:02:15] snowflake-warehouse   [ERROR] Statement aborted: Numeric value 'NULL' is not recognized.
                                         (Root cause identified: upstream column type divergence!)

```

---

## 7. Automated Anomaly Detection & Circuit Breaking

Rather than waiting for manual dashboard inspection, an **AWS Lambda Anomaly Monitor** runs every 5 minutes against the Athena lakehouse, checking for **Execution Duration Spikes** (indicating pipeline hangs or database lock contention).

$$\text{Z-Score} = \frac{\text{Current Duration} - \mu_{\text{historical}}}{\sigma_{\text{historical}}}$$

```python
import boto3
import os
import json
import urllib3

athena = boto3.client('athena')
SLACK_WEBHOOK = os.environ['SLACK_ALERT_WEBHOOK']
http = urllib3.PoolManager()

def lambda_handler(event, context):
    # Query to detect tasks running 3 standard deviations longer than normal
    anomaly_query = """
    WITH stats AS (
        SELECT
            attributes['task_id'] AS task_id,
            AVG(duration_ms) AS avg_duration,
            STDDEV(duration_ms) AS std_duration
        FROM observability.unified_telemetry
        WHERE date >= DATE_FORMAT(CURRENT_DATE - INTERVAL '14' DAY, '%Y-%m-%d')
          AND severity_text = 'INFO'
        GROUP BY attributes['task_id']
    ),
    latest_run AS (
        SELECT
            attributes['task_id'] AS task_id,
            trace_id,
            duration_ms
        FROM observability.unified_telemetry
        WHERE date = DATE_FORMAT(CURRENT_DATE, '%Y-%m-%d')
          AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '10' MINUTE
    )
    SELECT
        l.task_id,
        l.trace_id,
        l.duration_ms,
        s.avg_duration,
        (l.duration_ms - s.avg_duration) / NULLIF(s.std_duration, 0) AS z_score
    FROM latest_run l
    JOIN stats s ON l.task_id = s.task_id
    WHERE (l.duration_ms - s.avg_duration) / NULLIF(s.std_duration, 0) > 3.0;
    """

    # Executes Athena query and dispatches PagerDuty incident if z_score > 3.0
    # ... execution logic ...
    return {"status": "COMPLETE"}

```

---

## 8. Failure Modes & Edge Case Matrix

| Failure Vector                           | Operational Risk                                                                                                     | Architectural Mitigation                                                                                                                                                                                                                    |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Telemetry Ingestion Flooding**         | A failing retry loop produces millions of error logs/sec, overwhelming Kinesis and driving up S3 ingestion costs.    | Apply **Sampling & Log-Rate Quotas** at the OTel Collector level: Debug logs are sampled at $1\%$; Error logs pass through at $100\%$ with a ceiling of $1{,}000\text{ logs/sec}$ per task host before client-side throttling kicks in.     |
| **High-Cardinality Partition Explosion** | Dynamic partitioning on high-cardinality keys like `user_id` or `trace_id` creates millions of tiny S3 prefixes.     | Partition **strictly** by coarse-grained attributes: `service_name` and `date`. `trace_id` and `span_id` are kept as internal columnar Parquet fields indexed via Parquet dictionary encoding and statistics (min/max bounds).              |
| **Schema Evolution in Log Attributes**   | A team adds a nested JSON attribute that breaks static Glue Catalog Parquet schemas.                                 | Define the `attributes` column as a generic `MAP<STRING, STRING>`. Complex nested structures are serialized as JSON strings within map values, allowing schema-free tag indexing without altering DDLs.                                     |
| **Monitoring System Self-Failure**       | The observability pipeline crashes, leaving engineers blind to concurrent failures across core production pipelines. | Decouple basic health monitoring from the Lakehouse: Configure **CloudWatch Metric Filters** directly on Kinesis buffer lag and MWAA dead-letter queues to alert SREs even if the analytics query engine (Athena/Glue) becomes unavailable. |

---

# Complete Master Portfolio: 15 Data Engineering System Designs

This completes the end-to-end technical system design syllabus covering modern enterprise architectures:

```
[ FOUNDATIONAL MODERNIZATION ]
  1. Legacy Informatica to Databricks/Snowflake Migration (CDC, Dual-Run Parity)
  2. Serverless Lakehouse Ingestion (S3, Lambda, Glue, Athena, Small File Problem)
  3. Multi-Engine Lakehouse Coexistence (Databricks, Snowflake Iceberg, Delta UniForm)
  4. Cross-Cloud Incremental CDC Pipeline (Azure ADF to Snowflake on AWS, Streams/Tasks)
  5. Automated Data Quality & Quarantine Pipeline (Auto Loader, DLT, Athena Triage)

[ COMPLIANCE, SECURITY & GOVERNANCE ]
  6. GDPR/CCPA "Right to be Forgotten" at Scale (Deletion Vectors, WORM Audit)
  7. Multi-Region Active-Passive Disaster Recovery (RPO < 15m, RTO < 60m)
  9. Data Mesh Architecture (Federated Data Products, Open Delta Sharing)
  14. Real-Time Data Masking & Multi-Tenant SaaS (KMS, FPE, Snowflake RLS/RAP)

[ PERFORMANCE, REAL-TIME & CLOUD FINOPS ]
  8. Sub-Second Real-Time Streaming Lakehouse (RocksDB State, Snowpipe Streaming)
  10. Lakehouse FinOps & Cost Governance (FOCUS Model, Runaway Query Breakers)
  11. LLMOps & Retrieval-Augmented Generation (RAG) Data Pipeline (Textract, Cortex)
  12. Cross-Workspace CI/CD & Automated Data Deployments (DABs, Schemachange, MWAA)
  13. Reverse ETL & Operational Analytics (Change Detection, SQS Throttling)
  15. Real-Time Telemetry & Observability Pipeline (OpenTelemetry, Athena, Grafana)

```
