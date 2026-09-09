# Scenario 1: Legacy Informatica/DataStage Migration to Databricks & Snowflake

This scenario addresses migrating a legacy, mission-critical on-premises **Informatica PowerCenter / IBM DataStage** footprint (backed by Oracle/DB2/Teradata) into a modern cloud-native architecture powered by **Azure Data Factory (ADF)**, **Databricks (PySpark/Delta Lake)**, and **Snowflake**, orchestrated end-to-end by **AWS MWAA (Managed Airflow)**.

---

## 1. System Requirements & Migration Goals

- **Source Scale:** $20\text{+ TB}$ historical baseline; $300\text{--}500\text{ GB}$ daily incremental deltas across $1{,}500+$ legacy mapping jobs.
- **SLA Compression:** Reduce the legacy daily batch window from **8 hours** to **$< 90$ minutes**.
- **Zero Downtime Dual-Run:** Maintain parallel on-premises and cloud runs for a 30-day reconciliation window before decommissioning legacy hardware.
- **Functional Scope:**
- Refactor heavy server-bound Informatica transformations: Router, Aggregator, Dynamic/Static Lookup caches, and SCD Type 2 tracking.
- Implement automated data reconciliation (row-count parity, null checks, numerical hash checks) running at batch close.

---

## 2. End-to-End Architecture

```
[ On-Prem Data Center ]
  Oracle / DB2 / Teradata
            │
            │ (Encrypted ExpressRoute / VPN via Self-Hosted Integration Runtime)
            ▼
[ Ingestion & Staging Layer (Azure ADLS Gen2 / AWS S3) ]
  ADF / Glue Ingestion ──► Landing/Raw Storage (Compressed Parquet)
                                    │
                                    ▼
[ Processing Layer (Databricks / Delta Lake) ]
  ┌──────────────────────────────────────────────────────────┐
  │ Bronze: Raw Append + Audit Columns                       │
  │ Silver: Cleansed, De-duped, SCD Type 2 Merge via PySpark │
  └────────────────────────┬─────────────────────────────────┘
                           │
       ┌───────────────────┴───────────────────┐
       │ (High-Volume Feature Store / ML)      │ (Optimized Parquet Export / S3 Stage)
       ▼                                       ▼
[ Databricks Lakehouse ]              [ Enterprise DW (Snowflake) ]
                                        - Snowpipe / COPY INTO
                                        - Storage Integration
                                        - Gold Marts (Dimensional)
                                                       │
                                                       ▼
                                      [ Reconciliation Engine ]
                                        - AWS MWAA Airflow DAGs
                                        - Automated Drift & Hash Parity

```

---

## 3. Storage Layering: Medallion Architecture Mapping

| Layer             | Target Engine  | Storage Format            | Function                                                                                      |
| ----------------- | -------------- | ------------------------- | --------------------------------------------------------------------------------------------- |
| **Landing / Raw** | ADLS Gen2 / S3 | Compressed Parquet / Gzip | Direct staging from on-prem via ADF. Preserves raw source formats.                            |
| **Bronze**        | Databricks     | Delta Lake (Append-Only)  | Schema enforced; metadata fields added (`_ingested_at`, `_source_file`, `_batch_id`).         |
| **Silver**        | Databricks     | Delta Lake                | Cleaned, deduplicated, standardized types. Informatica Lookup/Join equivalents executed here. |
| **Gold**          | Snowflake      | Native Tables / Hybrid    | Star schema (Fact/Dimension), aggregated reporting marts, data-sharing consumers.             |

---

## 4. Informatica Component Refactoring Matrix

Informatica uses single-node, memory-bound caching engines; PySpark on Databricks executes these over distributed memory blocks.

| Legacy Informatica Transform               | PySpark / Delta Equivalent                                       | Production Implementation Detail                                                                              |
| ------------------------------------------ | ---------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| **Source Qualifier / Filter**              | `.filter()` / `.where()`                                         | Push down predicate filters to storage using partition pruning (`col("date") >= ...`).                        |
| **Lookup Transformation (Static/Dynamic)** | `broadcast(lookup_df)` or Hash Join                              | Use broadcast join if lookup table is $< 2\text{ GB}$ (`broadcast(dim_df)`). Avoids network shuffle.          |
| **Router**                                 | `.withColumn()` with `.when().otherwise()` or partitioned writes | Route conditionally using column expressions or split using cached DataFrames to avoid multi-pass scanning.   |
| **Aggregator**                             | `.groupBy().agg()`                                               | Ensure join/group keys avoid data skew. Use `spark.sql.adaptive.enabled = true` (AQE) to coalesce partitions. |
| **Update Strategy (DD_UPDATE / INSERT)**   | `DeltaTable.merge()`                                             | Execute vectorized ACID upserts directly on the target Delta/Snowflake table.                                 |

---

## 5. Technical Implementation: The SCD Type 2 Pattern

Informatica pipelines traditionally managed Slowly Changing Dimensions (SCD Type 2) using sequential Lookup $\to$ Expression $\to$ Router $\to$ Update Strategy steps. In PySpark on Databricks, replace this with a single, distributed atomic `MERGE` operation.

### PySpark & Delta Lake SCD2 Implementation

```python
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def process_scd2(target_delta_table_path, incoming_df, business_keys, tracked_columns):
    target_table = DeltaTable.forPath(spark, target_delta_table_path)
    target_df = target_table.toDF()

    # 1. Deduplicate incoming batch by business key (keep latest record)
    window_spec = Window.partitionBy(business_keys).orderBy(F.col("src_updated_at").desc())
    deduped_incoming = (
        incoming_df.withColumn("rank", F.row_number().over(window_spec))
        .filter(F.col("rank") == 1)
        .drop("rank")
    )

    # 2. Identify records where tracked attributes changed
    join_cond = [deduped_incoming[k] == target_df[k] for k in business_keys]
    join_cond.append(target_df["is_current"] == True)

    joined_df = deduped_incoming.alias("src").join(target_df.alias("tgt"), on=join_cond, how="left")

    # Generate hash to quickly compare tracked attributes
    src_hash = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(f"src.{c}"), F.lit("NULL")) for c in tracked_columns]), 256)
    tgt_hash = F.sha2(F.concat_ws("||", *[F.coalesce(F.col(f"tgt.{c}"), F.lit("NULL")) for c in tracked_columns]), 256)

    # Records that need old row closed and new row inserted
    changed_records = joined_df.filter(
        (F.col("tgt.is_current") == True) & (src_hash != tgt_hash)
    ).select("src.*")

    # 3. Prepare staging union: brand new records + mutated records (null merge key to force insert)
    staged_updates = (
        deduped_incoming.selectExpr("*", "NULL as merge_key")
        .unionByName(changed_records.withColumn("merge_key", F.concat_ws("||", *business_keys)))
    )

    # 4. Atomic Delta MERGE execution
    merge_condition = (
        "F.concat_ws('||', *[f'tgt.{k}' for k in business_keys]) = staged.merge_key AND tgt.is_current = true"
    )

    (
        target_table.alias("tgt")
        .merge(
            source=staged_updates.alias("staged"),
            condition=f"F.concat_ws('||', *[tgt[k] for k in business_keys]) = staged.merge_key AND tgt.is_current = true",
        )
        .whenMatchedUpdate(
            set={
                "is_current": F.lit(False),
                "end_date": F.col("staged.src_updated_at"),
            }
        )
        .whenNotMatchedInsert(
            values={
                **{c: F.col(f"staged.{c}") for c in staged_updates.columns if c != "merge_key"},
                "is_current": F.lit(True),
                "start_date": F.col("staged.src_updated_at"),
                "end_date": F.lit(None).cast("timestamp"),
            }
        )
        .execute()
    )

```

---

## 6. Snowflake Ingestion & External Stage Integration

Once PySpark processes the Silver layer, export the Gold layer to S3 or ADLS Gen2 as compressed Parquet files. Snowflake consumes this data through zero-overhead serverless ingestion.

### 1. Configure Storage Integration (AWS S3 Example)

```sql
CREATE OR REPLACE STORAGE INTEGRATION s3_gold_stage_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake_read_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://enterprise-data-lake-gold/');

```

### 2. Idempotent Ingestion via COPY INTO with Pattern Matching

```sql
CREATE OR REPLACE STAGE gold_layer_stage
  STORAGE_INTEGRATION = s3_gold_stage_int
  URL = 's3://enterprise-data-lake-gold/sales_fact/'
  FILE_FORMAT = (TYPE = 'PARQUET' COMPRESSION = 'SNAPPY');

COPY INTO enterprise_dw.gold.sales_fact
FROM (
  SELECT
    $1:sales_id::INT,
    $1:customer_key::INT,
    $1:amount::NUMBER(12,2),
    $1:transaction_ts::TIMESTAMP_NTZ,
    CURRENT_TIMESTAMP() AS dw_loaded_at
  FROM @gold_layer_stage
)
PATTERN = '.*batch_.*[.]parquet'
ON_ERROR = 'ABORT_STATEMENT';

```

---

## 7. Dual-Run Reconciliation Framework (MWAA Airflow)

To guarantee exact numerical parity between legacy Informatica and the new Databricks $\to$ Snowflake pipeline, orchestrate a dual-run verification DAG in **AWS MWAA**.

```
[ Scheduled Trigger (Daily 02:00 UTC) ]
                 │
      ┌──────────┴──────────┐
      ▼                     ▼
[ Run Informatica Job ]   [ Trigger ADF + Databricks Pipeline ]
      │                     │
      ▼                     ▼
[ Write to Oracle DW ]    [ Write to Snowflake Gold ]
      │                     │
      └──────────┬──────────┘
                 ▼
    [ Run Validation Operator ]
      - Row Count Equality
      - Primary Key Difference Sets (A - B == 0)
      - Numerical Sums & Hash Checksums
                 │
        ┌────────┴────────┐
     (Pass)             (Fail)
        ▼                 ▼
[ Flag Batch Ready ]   [ PagerDuty Alert + Retain Staging Dump ]

```

### Automated Reconciliation SQL (Executed by Airflow)

```sql
WITH legacy_metrics AS (
    SELECT
        COUNT(*) AS total_rows,
        SUM(amount) AS total_val,
        BIT_XOR(HASH(customer_id, amount, status)) AS checksum
    FROM oracle_dw.sales_fact
    WHERE load_date = CURRENT_DATE()
),
cloud_metrics AS (
    SELECT
        COUNT(*) AS total_rows,
        SUM(amount) AS total_val,
        BIT_XOR(HASH(customer_id, amount, status)) AS checksum
    FROM enterprise_dw.gold.sales_fact
    WHERE CAST(dw_loaded_at AS DATE) = CURRENT_DATE()
)
SELECT
    CASE
        WHEN l.total_rows != c.total_rows THEN 'ROW_COUNT_MISMATCH'
        WHEN ABS(l.total_val - c.total_val) > 0.01 THEN 'VALUE_VARIANCE_DETECTED'
        WHEN l.checksum != c.checksum THEN 'COLUMN_HASH_MISMATCH'
        ELSE 'PASSED'
    END AS reconciliation_status,
    l.total_rows AS legacy_rows,
    c.total_rows AS cloud_rows,
    l.total_val AS legacy_sum,
    c.total_val AS cloud_sum
FROM legacy_metrics l
CROSS JOIN cloud_metrics c;

```

---

## 8. Failure Modes & Mitigations

- **Shuffle Spill in PySpark During Massive Lookups:**
- _Failure:_ Joining a 500M row fact table with a 50M row customer lookup exhausts worker local disk space.
- _Mitigation:_ Enable Adaptive Query Execution (`spark.sql.adaptive.skewJoin.enabled = true`) and ensure tables are partitioned on identical join keys before executing the merge.

- **Snowflake Warehouse Auto-Suspend Latency During Bursts:**
- _Failure:_ Loading numerous small files with `COPY INTO` causes compute costs to skyrocket without utilizing full node capacity.
- _Mitigation:_ Size stage files to between $100\text{ MB}$ and $250\text{ MB}$ in Databricks before writing to the target bucket, maximizing the efficiency of multi-threaded Snowflake ingestion threads.

- **On-Premise Network Saturation via SHIR:**
- _Failure:_ Transferring raw tables locks the client’s core IP network, throttling daily operational systems.
- _Mitigation:_ Configure ADF pipelines with **Data Integration Unit (DIU)** ceilings and schedule non-critical bulk historical extractions during the on-premise off-peak window (23:00–05:00 local time).
