# Scenario 6: GDPR/CCPA "Right to be Forgotten" & Regulatory Deletions at Scale

Enforcing regulatory deletion requests (GDPR Article 17, CCPA) across distributed lakehouses and data warehouses poses a severe **write-amplification problem**. In columnar formats like Apache Parquet, modifying or deleting a single row traditionally requires reading, filtering, and re-writing an entire $100\text{--}512\text{ MB}$ file. At thousands of requests per day spread across petabyte-scale historical datasets, naive approaches cause continuous cluster churn, high cloud egress, and table lock contention.

This design implements a **two-tier cryptographic erasure and deletion vector architecture** coordinated across **Amazon S3**, **Azure ADLS Gen2**, **Databricks (Delta Lake)**, and **Snowflake**, orchestrated by **AWS MWAA**.

---

## 1. System Requirements & Regulatory Boundaries

- **Throughput:** $1{,}000\text{--}5{,}000$ individual erasure requests per day, targeting hundreds of distinct entities (tables, views, raw event dumps).
- **Compliance SLA:**
- **Logical Obfuscation (Immediate):** User data must become instantly unqueryable across all analytical dashboards within **$< 15$ minutes** of ticket ingestion.
- **Physical Parquet Purge (Batched):** Underlying immutable files, snapshots, and backups must be completely expunged within the regulatory deadline (typically **30 days** for GDPR/CCPA).

- **Zero Disruption to Long-Running BI:** Day-to-day analytics, dashboard queries, and ETL jobs must not fail due to file read race conditions or table-locking bottlenecks during purges.
- **Auditability:** Generate immutable, cryptographically verifiable compliance receipts documenting _when_ and _where_ each identifier was purged.

---

## 2. End-to-End Architectural Blueprint

```
[ CRM / Consent Service / Privacy API ]
                   │
                   ▼
┌────────────────────────────────────────────────────────┐
│ Ingestion & Audit Layer                                │
│   - API Gateway + AWS Lambda                           │
│   - Writes deletion ticket to DynamoDB / SQS           │
└──────────────────┬─────────────────────────────────────┘
                   │
         ┌─────────┴─────────┐
         │ (Instant Path)    │ (Scheduled Batch Path)
         ▼                   ▼
┌───────────────────┐  ┌─────────────────────────────────┐
│ Tier 1: Immediate │  │ Tier 2: Batched Physical Erasure│
│ Cryptographic     │  │ (AWS MWAA Scheduled DAG)        │
│ Invalidation      │  └───────────────┬─────────────────┘
└────────┬──────────┘                  │
         │                             ├────────────────────────────────┐
         │                             ▼                                ▼
         │             ┌───────────────────────────────┐ ┌──────────────────────────────┐
         │             │ Databricks (Delta Lake)       │ │ Snowflake                      │
         │             │ 1. Deletion Vectors (Soft)    │ │ 1. Stage Ingestion             │
         ▼             │ 2. Liquid Clustering          │ │ 2. Atomic MERGE / DELETE       │
┌───────────────────┐  │ 3. REORG PURGE (Physical)     │ │ 3. Transient Retain Adjustment │
│ Snowflake Dynamic │  │ 4. VACUUM RETAIN 0 HOURS      │ │ 4. Zero Time-Travel Purge      │
│ Data Masking &    │  └───────────────┬───────────────┘ └──────────────┬───────────────┘
│ Token Blacklist   │                  │                                │
└───────────────────┘                  └────────────────┬───────────────┘
                                                        │
                                                        ▼
                                       ┌────────────────────────────────┐
                                       │ Compliance Certificate Audit   │
                                       │ - Write hash receipts to S3 WORM│
                                       │ - Emit callback to Privacy API │
                                       └────────────────────────────────┘

```

---

## 3. Tier 1: Instant Logical Erasure (Crypto-Shredding & Dynamic Masking)

To satisfy the sub-15-minute SLA without re-writing multi-terabyte datasets instantly, the system uses two fast mechanisms: **Crypto-Shredding** and **Snowflake Dynamic Masking Policies**.

### Dynamic Masking Policy via Blacklist Table

When a user requests deletion, their identifier is immediately appended to an in-memory or low-latency lookup table (`compliance.erasure_blacklist`). Analytical queries evaluate this list at runtime via centralized masking policies.

```sql
-- 1. Central Blacklist Table in Snowflake
CREATE OR REPLACE TABLE compliance.erasure_blacklist (
    user_id VARCHAR(64) PRIMARY KEY,
    requested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    regulatory_framework VARCHAR(16) -- 'GDPR', 'CCPA'
);

-- 2. Define Dynamic Masking Policy
CREATE OR REPLACE MASKING POLICY compliance.mask_pii_on_erasure AS (val string, user_id string)
RETURNS string ->
    CASE
        WHEN EXISTS (
            SELECT 1 FROM compliance.erasure_blacklist b
            WHERE b.user_id = user_id
        ) THEN '***GDPR_ERASED***'
        ELSE val
    END;

-- 3. Apply Masking to Sensitive Columns in Production
ALTER TABLE enterprise_dw.gold.dim_customer
MODIFY COLUMN email SET MASKING POLICY compliance.mask_pii_on_erasure USING (email, user_id);

ALTER TABLE enterprise_dw.gold.dim_customer
MODIFY COLUMN phone_number SET MASKING POLICY compliance.mask_pii_on_erasure USING (phone_number, user_id);

```

- **Outcome:** The user's PII becomes unqueryable and zeroed out for every analytical role in the organization within seconds of the blacklist write, eliminating regulatory exposure while physical batch purging queues up.

---

## 4. Tier 2: Batched Physical Purge in Databricks (Delta Lake)

Running row-level deletes individually destroys Lakehouse throughput. Instead, deletion requests are accumulated over a 24-hour window and applied using **Delta Deletion Vectors**, followed by a deterministic compaction and file purge.

### A. How Deletion Vectors Prevent Write Amplification

- **Standard Parquet (Copy-on-Write):** Modifying 1 row in a $500\text{ MB}$ file containing 5,000,000 rows forces Spark to rewrite all 4,999,999 unchanged rows into a brand-new $500\text{ MB}$ Parquet file.
- **Deletion Vectors (DV):** Spark leaves the original Parquet file 100% untouched. It writes an ultra-compact binary bitmap file (a few kilobytes) listing the exact row positions (e.g., Row 42, Row 918) that are logically dead. Read operations load the bitmap into memory and skip those positions with minimal latency.

```
Standard Delta (CoW):
[ Data File A.parquet (500MB) ] ──(Delete 1 row)──► [ Data File B.parquet (500MB Rewritten) ]

Delta with Deletion Vectors:
[ Data File A.parquet (500MB) ] (Untouched)
              ▲
              │ (Skipped at read time)
[ DV Bitmap (2KB) ] ──────────┘

```

### B. PySpark Orchestration: Batch Deletion Vector Application

```python
from delta.tables import DeltaTable
from pyspark.sql import functions as F

def execute_batch_erasure(target_table_name: str, deletion_requests_path: str):
    delta_table = DeltaTable.forName(spark, target_table_name)

    # 1. Load pending erasures for the current batch window
    pending_deletions_df = (
        spark.read.parquet(deletion_requests_path)
        .filter(F.col("status") == "PENDING")
        .select("user_id")
        .distinct()
    )

    # 2. Execute soft delete (Generates Deletion Vectors natively)
    (
        delta_table.alias("target")
        .merge(
            source=pending_deletions_df.alias("erasure"),
            condition="target.user_id = erasure.user_id"
        )
        .whenMatchedDelete()
        .execute()
    )

```

### C. Physical File Materialization (`PURGE` & `VACUUM`)

Deletion Vectors alone do not satisfy the GDPR physical storage mandate—the original PII technically still resides on S3/ADLS inside the un-compacted Parquet file.

To permanently destroy the data, schedule a monthly maintenance window using `REORG TABLE ... APPLY (PURGE)` followed by `VACUUM`:

```sql
-- Step 1: Force Databricks to physically rewrite only the files that have associated Deletion Vectors
REORG TABLE enterprise_silver.customer_360 APPLY (PURGE);

-- Step 2: Override safety checks (if necessary) to eliminate time-travel files within regulatory window
SET spark.databricks.delta.vacuum.parallelDelete.enabled = true;

-- Step 3: Physically remove old tombstones and historical un-purged Parquet files from S3/ADLS
VACUUM enterprise_silver.customer_360 RETAIN 168 HOURS; -- Retain 7 days for pipeline rollback safety

```

---

## 5. Physical Purge in Snowflake

In Snowflake, historical snapshots are retained via **Time-Travel** (up to 90 days) and **Fail-safe** (7 days, disaster-recovery only). To ensure compliance:

### 1. Batch Execution via Staged Deletion IDs

```sql
-- Micro-batch delete executed during off-peak hours
DELETE FROM enterprise_dw.gold.dim_customer tgt
USING staging.gdpr_pending_deletions src
WHERE tgt.user_id = src.user_id;

```

### 2. Time-Travel Considerations & Regulatory Boundaries

- **Standard Tables:** Retain data for the configured `DATA_RETENTION_TIME_IN_DAYS` (e.g., default 1 to 90 days). The data remains accessible via `AT(TIMESTAMP => ...)` queries until this window closes.
- **Transient Tables for Staging/Logs:** Set `DATA_RETENTION_TIME_IN_DAYS = 0` for raw landing or operational staging layers so deletes are immediate without time-travel overhead.
- **Fail-Safe Policy:** Cloud regulatory guidance (including GDPR regulatory authority precedent) recognizes that disaster-recovery-only storage (Snowflake Fail-Safe) that cannot be queried by normal business operations and expires automatically via hardware FIFO cycles satisfies compliance requirements, provided active query layers are expunged.

---

## 6. End-to-End Orchestration: MWAA (Airflow) Pipeline

The complete regulatory lifecycle is scheduled via an idempotent Airflow DAG.

```
[ Daily Trigger: 01:00 UTC ]
             │
             ▼
[ 1. Ingest Deletion Requests from SQS/DynamoDB to S3 Stage ]
             │
             ▼
[ 2. Sync Snowflake Blacklist Table (Instant Masking Active) ]
             │
             ▼
[ 3. Trigger Databricks MERGE Job (Soft Deletes via Deletion Vectors) ]
             │
             ▼
[ 4. Trigger Snowflake Batch DELETE on Target Star Schemas ]
             │
             ▼
[ 5. Decision: Is Today Scheduled Maintenance Window? ]
        /                                    \
     (Yes)                                   (No)
       /                                       \
[ 6a. Run Databricks REORG PURGE ]              │
       │                                        │
[ 6b. Run Databricks VACUUM ]                   │
       │                                        │
       └───────────────────┬────────────────────┘
                           │
                           ▼
[ 7. Generate Signed Audit Manifest & Post to Compliance S3 WORM ]

```

### Airflow Orchestration DAG Snippet

```python
from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import BranchPythonOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='gdpr_compliance_orchestrator',
    start_date=datetime(2026, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    # Step 1: Push latest deletion tickets into the Snowflake Blacklist for dynamic masking
    update_blacklist = SnowflakeOperator(
        task_id='update_snowflake_blacklist',
        snowflake_conn_id='snowflake_default',
        sql="""
            INSERT INTO compliance.erasure_blacklist (user_id, regulatory_framework)
            SELECT DISTINCT user_id, framework
            FROM staging.raw_gdpr_tickets
            WHERE status = 'NEW';
        """
    )

    # Step 2: Apply Deletion Vectors in Databricks Silver Layer
    run_databricks_soft_delete = DatabricksRunNowOperator(
        task_id='databricks_soft_delete',
        databricks_conn_id='databricks_default',
        job_id=621948123  # Triggers PySpark batch merge
    )

    # Step 3: Run Snowflake Warehouse Deletions
    run_snowflake_delete = SnowflakeOperator(
        task_id='snowflake_physical_delete',
        snowflake_conn_id='snowflake_default',
        sql="""
            DELETE FROM enterprise_dw.gold.fct_orders
            WHERE user_id IN (SELECT user_id FROM compliance.erasure_blacklist WHERE requested_at >= CURRENT_DATE() - 1);
        """
    )

    update_blacklist >> [run_databricks_soft_delete, run_snowflake_delete]

```

---

## 7. Audit Logging & Compliance Certification (WORM Storage)

Regulatory audits require proof of erasure. The final task of the pipeline compiles a cryptographic manifest:

1. **Manifest Attributes:**

- `batch_id`: UUID
- `hash_user_id`: SHA-256 hash of the deleted identifier (never log the cleartext PII in an audit log!)
- `tables_affected`: Array of fully qualified table names.
- `purge_timestamp`: UTC timestamp.
- `executor_arn`: IAM Role ARN executing the purge.

2. **Target Storage (AWS S3 Object Lock):** The manifest JSON file is written to an S3 bucket configured with **S3 Object Lock in Compliance Mode** (Write Once, Read Many - WORM).
3. Even root AWS account credentials cannot overwrite or delete these compliance receipts for the duration of the retention period (e.g., 7 years).

---

## 8. Failure Modes & Mitigations

| Failure Vector                             | Impact                                                               | Engineering Mitigation                                                                                                                                                                                                   |
| ------------------------------------------ | -------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| **Concurrent Reads during `REORG PURGE**`  | Analytical queries fail with missing file exceptions.                | Databricks Delta Lake uses multi-version concurrency control (MVCC). Readers continue scanning the old snapshot until the atomic transaction commit replaces it with the purged file set.                                |
| **Unbounded Deletion Vector Accumulation** | Too many small DV bitmap files degrade Delta read latency over time. | Databricks Auto-Compaction automatically schedules bin-packing; ensure regular `OPTIMIZE` commands run weekly.                                                                                                           |
| **Accidental Deletion of Wrong Entity**    | Data loss across historical marts due to bad input payload.          | Implement a **Circuit Breaker**: If any incoming deletion ticket batch contains $> 0.5\%$ of the total active user base, abort the pipeline immediately, keep the blacklist in place, and page the Data Governance Lead. |
