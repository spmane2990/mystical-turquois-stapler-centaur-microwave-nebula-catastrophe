# Scenario 5: Automated Data Quality, Quarantine & Metadata Governance Pipeline

This design establishes a resilient Lakehouse ingestion platform handling high-velocity, untrusted external vendor data. It prevents silent warehouse corruption, handles unexpected schema drift without pipeline failure, isolates bad data into a queryable quarantine zone, and enforces strict data contracts.

---

## 1. System Requirements & Failure Scenarios

- **Throughput:** $100\text{k--}500\text{k}$ incoming events/minute landed as JSON, CSV, and Parquet on **Amazon S3**.
- **Zero Pipeline Crashes:** Upstream schema changes (e.g., unexpected new fields, casing switches, type migrations) must not abort ingestion jobs.
- **Strict Quality Gates:** Rows failing critical constraints (null primary keys, invalid business codes, out-of-range metrics) must be split atomically into a **Quarantine Zone** without blocking valid rows.
- **Triage & Remediation SLA:** Data stewards must have sub-minute, zero-copy SQL access to quarantined records with error diagnostic codes via **Amazon Athena**.
- **Automated Escalation:** If the quarantine rate exceeds 2% of any incoming batch, trigger an alert via **AWS Lambda** to PagerDuty/Slack.

---

## 2. End-to-End Architectural Blueprint

```
[ External Vendors / Third Parties ]
                 │
                 ▼
[ S3 Raw Landing Bucket ] (Unvalidated, schema-fluid files)
                 │
                 │ (CloudFiles incremental discovery)
                 ▼
[ Databricks Auto Loader (Ingestion Engine) ]
   - Schema Inference & Schema Evolution (addNewColumns)
   - Rescued Data Column (_rescued_data) for type-mismatch capture
                 │
                 ▼
[ Databricks Delta Live Tables (DLT) & Great Expectations ]
   ┌────────────────────────────────────────────────────────┐
   │ Bronze Delta Table: Schema preserved + audit metadata  │
   └───────────────────────────┬────────────────────────────┘
                               │
               (Evaluate Expectation Rulesets)
                               │
          ┌────────────────────┴────────────────────┐
          │                                         │
   [ Passed Rows ]                           [ Failed Rows ]
          │                                         │
          ▼                                         ▼
┌───────────────────────┐             ┌───────────────────────────────────┐
│ Silver Delta Table    │             │ Quarantine Delta / S3 Export Zone │
│ Clean, typed dataset  │             │ - Row content                     │
│ ready for analytics   │             │ - Failed constraint name          │
└───────────────────────┘             │ - Ingestion timestamp             │
                                      └─────────────────┬─────────────────┘
                                                        │
                         ┌──────────────────────────────┴──────────────────────────────┐
                         ▼                                                             ▼
         [ AWS Glue Data Catalog & Athena ]                             [ AWS Lambda Alert Engine ]
           Data stewards query bad rows via SQL                           - Monitors quarantine breach (>2%)
           SELECT * FROM quarantine WHERE rule='pk_null';                 - Pushes alert to Slack/PagerDuty

```

---

## 3. Ingestion Resilience: Auto Loader & Rescued Data

Standard Spark pipelines fail when a string arrives in a numeric field or an unmapped column appears. Databricks **Auto Loader** (`cloudFiles`) avoids this by persisting raw state, inferring schemas dynamically, and sequestering parse failures into `_rescued_data`.

### Auto Loader Configuration with Rescued Data

```python
from pyspark.sql import functions as F

bronze_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "s3://lakehouse-checkpoints/vendor_schema/")
    # Automatically add newly introduced columns to table schema
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    # Route corrupt/type-mismatched fields into a JSON text column instead of dropping them
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .load("s3://lakehouse-raw-landing/vendor_records/")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

(
    bronze_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "s3://lakehouse-checkpoints/bronze_write/")
    .toTable("enterprise_bronze.vendor_raw_feed")
)

```

- **How `_rescued_data` works:** If an incoming record contains `{"user_id": "ABC"}` for an integer column, the value is not cast to `NULL` (which causes silent corruption). Instead, `user_id` becomes `NULL`, and `_rescued_data` stores `{"user_id": "ABC"}` along with the parse error message.

---

## 4. Multi-Track Quarantine Pattern via Delta Live Tables

Delta Live Tables (DLT) supports continuous data quality expectations (`@dlt.expect`). To implement a quarantine pattern (splitting failed records rather than halting the pipeline), define parallel materialized views:

```python
import dlt
from pyspark.sql import functions as F

# Base expectation rules
RULES = {
    "valid_record_id": "record_id IS NOT NULL",
    "valid_transaction_amount": "transaction_amount >= 0.0",
    "known_vendor_code": "vendor_code IN ('VND_01', 'VND_02', 'VND_03')",
    "clean_rescued_data": "_rescued_data IS NULL"
}

# Construct boolean condition for all rules passing
all_rules_pass_expr = " AND ".join([f"({rule})" for rule in RULES.values()])

# 1. SILVER: Valid records that passed 100% of expectations
@dlt.table(
    name="vendor_feed_silver",
    comment="Cleansed vendor records passing all data contracts"
)
def vendor_feed_silver():
    return (
        dlt.read_stream("enterprise_bronze.vendor_raw_feed")
        .filter(all_rules_pass_expr)
    )

# 2. QUARANTINE: Failed records tagged with diagnostic reason codes
@dlt.table(
    name="vendor_feed_quarantine",
    comment="Quarantined vendor records for steward audit and remediation"
)
def vendor_feed_quarantine():
    bronze_df = dlt.read_stream("enterprise_bronze.vendor_raw_feed")

    # Filter for any row that failed at least one rule
    failed_df = bronze_df.filter(f"NOT ({all_rules_pass_expr})")

    # Tag failing records with the specific failed rule names
    quarantine_tagged = failed_df.withColumn(
        "failure_reasons",
        F.concat_ws(", ",
            *[F.when(~F.expr(condition), F.lit(name)).otherwise(F.lit(None))
              for name, condition in RULES.items()]
        )
    )
    return quarantine_tagged

```

---

## 5. Instant Triage via AWS Glue & Amazon Athena

To give non-Spark analytics teams and data stewards instant visibility into quarantine dumps, the quarantine Delta/Parquet table is cataloged in the **AWS Glue Data Catalog**.

```
[ Databricks Quarantine Delta Table ]
                 │
                 │ (External Table pointing to S3 location)
                 ▼
     [ AWS Glue Data Catalog ]
                 │
                 ▼
         [ Amazon Athena ]

```

### Steward Ad-Hoc SQL Query in Athena

```sql
SELECT
    failure_reasons,
    vendor_code,
    _source_file,
    _rescued_data,
    COUNT(*) AS total_failed_rows
FROM "lakehouse_catalog"."governance"."vendor_feed_quarantine"
WHERE CAST(_ingested_at AS DATE) = CURRENT_DATE
GROUP BY failure_reasons, vendor_code, _source_file, _rescued_data
ORDER BY total_failed_rows DESC;

```

This lets governance teams identify whether a specific vendor changed their file headers or sent corrupted records, without needing access to Databricks clusters.

---

## 6. Automated SLA Monitoring & Alerting (AWS Lambda)

A scheduled AWS Lambda function runs every 15 minutes to calculate the **Quarantine Failure Ratio**:

$$\text{Quarantine Ratio} = \frac{\text{Quarantined Rows}}{\text{Bronze Total Rows Ingested}} \times 100$$

If this metric exceeds **2%**, it halts dependent downstream Gold pipeline DAGs and triggers an alert with diagnostics.

```python
import json
import os
import boto3
import urllib3

athena = boto3.client('athena')
SLACK_WEBHOOK_URL = os.environ['SLACK_WEBHOOK_URL']
http = urllib3.PoolManager()

def lambda_handler(event, context):
    query = """
    WITH batch_counts AS (
        SELECT
            (SELECT count(*) FROM "governance"."vendor_feed_silver"
             WHERE _ingested_at >= now() - interval '15' minute) AS valid_count,
            (SELECT count(*) FROM "governance"."vendor_feed_quarantine"
             WHERE _ingested_at >= now() - interval '15' minute) AS quarantine_count
    )
    SELECT
        valid_count,
        quarantine_count,
        CAST(quarantine_count AS DOUBLE) / NULLIF(valid_count + quarantine_count, 0) * 100.0 AS failure_pct
    FROM batch_counts;
    """

    # Execute query against Athena (simplified execution wrapper)
    exec_id = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': 'governance'},
        ResultConfiguration={'OutputLocation': 's3://lakehouse-query-results/athena_alerts/'}
    )['QueryExecutionId']

    # After query completes (polled), parse result
    # If failure_pct > 2.0:
    failure_pct = 4.8  # Example evaluated value
    if failure_pct > 2.0:
        alert_payload = {
            "text": f":rotating_light: *CRITICAL: Data Quality Gate Breach* :rotating_light:\n"
                    f"*Failure Rate:* `{failure_pct:.2f}%` (Threshold: 2.0%)\n"
                    f"*Action:* Downstream Gold Mart DAGs halted. Inspect via Athena."
        }
        http.request('POST', SLACK_WEBHOOK_URL, body=json.dumps(alert_payload), headers={'Content-Type': 'application/json'})

    return {"status": "COMPLETE", "failure_pct": failure_pct}

```

---

## 7. Automated Backfill & Remediation Flow

When corrupt records in the quarantine layer are resolved (e.g., the vendor supplies the missing mapping table or a parsing rule is updated), apply this reconciliation pattern:

```
[ Data Steward Remediation ]
              │
              │ (Update mapping / ETL logic)
              ▼
[ PySpark Re-processing Job ]
  - Reads from vendor_feed_quarantine for specific failure_reasons
  - Applies updated transformations / default fill values
              │
              ▼
[ Silver Layer Atomic Upsert ]
  - MERGE INTO enterprise_silver.vendor_feed USING remediated_data
              │
              ▼
[ Quarantine Table Purge / Flag ]
  - UPDATE vendor_feed_quarantine SET is_remediated = TRUE

```

This prevents duplicate insertions while maintaining complete audit trails across the Bronze, Silver, and Quarantine boundaries.

---

## 8. Quality & Governance Architectural Summary

| Vector                     | Production Failure Mode                                               | Architecture Solution                                                                                                     |
| -------------------------- | --------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| **Schema Evolution**       | Vendor adds unexpected columns; pipeline crashes on write.            | `cloudFiles.schemaEvolutionMode = "addNewColumns"` automatically updates the Delta table.                                 |
| **Type Mismatch**          | Vendor sends strings in numeric fields; standard Spark yields `NULL`. | `cloudFiles.rescuedDataColumn` captures invalid types in raw JSON without dropping data.                                  |
| **Constraint Violation**   | Dirty records pollute analytics and corrupt executive dashboards.     | Parallel DLT tables split data into Silver and Quarantine based on boolean validation rules.                              |
| **Operational Visibility** | Pipeline quietly drops data without notifying downstream teams.       | Glue Catalog + Athena provide instant SQL triage; Lambda tracks quarantine rates and alerts when thresholds are breached. |

---

### All 5 Scenarios Completed

1. **Legacy Informatica to Databricks/Snowflake Migration** (Architecture, CDC, Dual-Run Reconciliation)
2. **Serverless Lakehouse Ingestion** (S3, Lambda, Glue, Athena, Small File Problem)
3. **Multi-Engine Lakehouse Coexistence** (Databricks, Snowflake Iceberg Tables, Delta UniForm, MWAA)
4. **Cross-Cloud Incremental CDC Pipeline** (Azure to Snowflake on AWS, Snowpipe, Streams & Tasks)
5. **Automated Data Quality & Quarantine Pipeline** (Auto Loader, DLT, Great Expectations, Athena, Lambda)
