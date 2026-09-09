# Scenario 3: Multi-Engine Enterprise Lakehouse (Databricks + Snowflake Coexistence)

This design addresses enterprise data architectures where **Databricks** serves as the heavy-duty compute, data science, and complex ETL engine, while **Snowflake** acts as the high-concurrency BI, semantic, and enterprise reporting layer.

Instead of maintaining dual pipelines that write to both storage accounts and inflate cross-cloud egress and storage fees, this architecture leverages **open table formats (Apache Iceberg / Delta UniForm)** over **Amazon S3 / Azure ADLS Gen2**, enabling Snowflake to query Databricks-managed tables zero-copy.

---

## 1. System Requirements & Architecture Blueprint

- **Single Source of Truth:** Data files (Parquet) are written once to cloud object storage (S3 or ADLS Gen2) by Databricks.
- **Zero Duplicate Storage:** Eliminate multi-terabyte data duplication and warehouse loading fees inside Snowflake.
- **ACID & Snapshot Isolation:** Support high-frequency updates, deletes, and time-travel across both engines without read conflicts.
- **Pipeline Orchestration:** **AWS MWAA (Airflow)** coordinates job sequencing: ETL execution, table compaction/vacuuming, and metadata catalog refreshes.

```
                    [ Upstream Sources / Raw Event Stream ]
                                      │
                                      ▼
             [ Databricks (Compute Engine / Delta Lake + UniForm) ]
               - Bronze / Silver: Heavy PySpark Cleansing
               - Gold: Aggregations & Business Marts
               - Auto-generates Iceberg Metadata (.metadata.json)
                                      │
                                      │ (Single Write to Open Storage)
                                      ▼
            [ Unified Cloud Storage: Amazon S3 or Azure ADLS Gen2 ]
              - Underlying Columnar Data: snappy/zstd Parquet
              - Delta Log: _delta_log/
              - Iceberg Metadata: metadata/*.metadata.json
                                      │
                ┌─────────────────────┴─────────────────────┐
                │                                           │
                ▼                                           ▼
      [ Unity Catalog ]                         [ AWS Glue / REST Catalog ]
     (Iceberg REST Endpoint)                     (External Catalog Synced)
                │                                           │
                └─────────────────────┬─────────────────────┘
                                      │ (Zero-Copy Catalog Reference)
                                      ▼
                        [ Snowflake (Serving & BI) ]
                          - External Volumes (S3/ADLS Auth)
                          - Catalog Integrations (Glue / REST / Delta Direct)
                          - Iceberg Tables (Local warehouse compute)
                                      ▲
                                      │
                     [ Orchestrator: AWS MWAA (Airflow) ]
                       - Databricks Run Submit Operator
                       - Delta OPTIMIZE & VACUUM Maintenance
                       - Snowflake ALTER ICEBERG TABLE REFRESH

```

---

## 2. Table Format Interoperability Patterns

There are two primary architectures for querying Databricks-produced storage from Snowflake:

| Strategy                                | Mechanism                                                                                         | Snowflake Catalog Integration                                                             | Trade-offs                                                                                       |
| --------------------------------------- | ------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------ |
| **A. Databricks UniForm (Recommended)** | Delta writes underlying Parquet and asynchronously emits Iceberg metadata (`.metadata.json`).     | Point Snowflake to Unity Catalog's **Iceberg REST Catalog** or **AWS Glue Data Catalog**. | Native Iceberg performance in Snowflake. Slight lag (seconds) while Iceberg metadata generates.  |
| **B. Snowflake Delta Direct**           | Snowflake natively parses the `_delta_log/` directly without needing external Iceberg generation. | `CATALOG_SOURCE = OBJECT_STORE`, `TABLE_FORMAT = DELTA`.                                  | Zero configuration in Databricks; Snowflake parses JSON Delta logs on each query planning phase. |

---

## 3. Storage & Interoperability Setup

### Step 1: Enable Delta UniForm in Databricks

When writing the Gold serving layer in Databricks, configure the table properties to automatically generate Iceberg metadata on every commit:

```sql
-- Databricks SQL / PySpark DDL
CREATE TABLE enterprise_gold.finance.monthly_revenue (
    account_id STRING,
    billing_period DATE,
    gross_revenue DECIMAL(18, 2),
    churn_risk_score DOUBLE,
    last_updated_at TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.universalFormat.enabledFormats' = 'iceberg',
    'delta.columnMapping.mode' = 'name'
)
LOCATION 's3://enterprise-lakehouse-gold/finance/monthly_revenue/';

```

### Step 2: Establish AWS IAM Trust for Snowflake External Volume

Snowflake authenticates via an IAM Role using external IDs to read Parquet data directly from the S3 bucket.

```sql
-- Run as ACCOUNTADMIN in Snowflake
CREATE OR REPLACE EXTERNAL VOLUME exvol_lakehouse_gold
   STORAGE_LOCATIONS =
      (
         (
            NAME = 's3-us-east-1-gold'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://enterprise-lakehouse-gold/'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/SnowflakeLakehouseReaderRole'
         )
      );

-- Run DESC EXTERNAL VOLUME to retrieve STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID
DESC EXTERNAL VOLUME exvol_lakehouse_gold;

```

### Step 3: Configure Catalog Integration & Iceberg Table in Snowflake

Point Snowflake to the **AWS Glue Data Catalog** (which is registered as an external catalog target from Databricks/S3):

```sql
-- 1. Integrate with AWS Glue Catalog
CREATE OR REPLACE CATALOG INTEGRATION glue_catalog_int
  CATALOG_SOURCE = GLUE
  CATALOG_NAMESPACE = 'finance'
  TABLE_FORMAT = ICEBERG
  GLUE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/SnowflakeGlueCatalogRole'
  GLUE_CATALOG_ID = '123456789012'
  GLUE_REGION = 'us-east-1'
  ENABLED = TRUE;

-- 2. Create the zero-copy Iceberg Table
CREATE OR REPLACE ICEBERG TABLE analytics_dw.gold.monthly_revenue
  EXTERNAL_VOLUME = 'exvol_lakehouse_gold'
  CATALOG = 'glue_catalog_int'
  CATALOG_TABLE_NAME = 'monthly_revenue'
  AUTO_REFRESH = FALSE; -- Refreshed deterministically by Airflow

```

---

## 4. End-to-End Orchestration Pipeline (AWS MWAA)

To prevent dirty reads and ensure snapshot isolation, the Airflow DAG coordinates the lifecycle: Databricks writes $\to$ Databricks compacts $\to$ Snowflake refreshes metadata.

```
+--------------------------------------------------------------+
| 1. Airflow: Trigger Databricks Gold Transformation Pipeline  |
+--------------------------------------------------------------+
                               |
                               v
+--------------------------------------------------------------+
| 2. Airflow: Run Delta Maintenance (OPTIMIZE + VACUUM)        |
+--------------------------------------------------------------+
                               |
                               v
+--------------------------------------------------------------+
| 3. Airflow: Sync AWS Glue Catalog with Latest Delta Commit   |
+--------------------------------------------------------------+
                               |
                               v
+--------------------------------------------------------------+
| 4. Airflow: Trigger Snowflake Metadata Refresh via SQL Hook  |
+--------------------------------------------------------------+

```

### MWAA Airflow DAG Implementation (`multi_engine_lakehouse_dag.py`)

```python
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='lakehouse_databricks_to_snowflake_sync',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='0 3 * * *', # 03:00 AM UTC daily
    catchup=False,
) as dag:

    # 1. Trigger Databricks Gold Layer ETL Job
    run_databricks_etl = DatabricksRunNowOperator(
        task_id='databricks_gold_etl',
        databricks_conn_id='databricks_default',
        job_id=4598213749
    )

    # 2. Trigger Table Maintenance (Compaction) in Databricks
    run_databricks_optimize = DatabricksRunNowOperator(
        task_id='databricks_optimize_table',
        databricks_conn_id='databricks_default',
        job_id=8921734912 # Job executing: OPTIMIZE monthly_revenue ZORDER BY (billing_period)
    )

    # 3. Deterministically Refresh Iceberg Metadata in Snowflake
    refresh_snowflake_iceberg = SnowflakeOperator(
        task_id='refresh_snowflake_iceberg_table',
        snowflake_conn_id='snowflake_default',
        sql="""
            ALTER ICEBERG TABLE analytics_dw.gold.monthly_revenue REFRESH;
        """
    )

    run_databricks_etl >> run_databricks_optimize >> refresh_snowflake_iceberg

```

---

## 5. ACID Concurrency & Snapshot Consistency

When multiple engines interact with the same underlying data lakehouse, strict concurrency guarantees are required:

- **Read-Committed Snapshot Isolation:**
- Snowflake queries bind to a specific Iceberg snapshot version (e.g., `snapshot-id: 849204812`).
- If Databricks executes a long-running `MERGE` or batch append while Snowflake users run executive dashboards, Snowflake continues reading from the committed snapshot state until `ALTER ICEBERG TABLE ... REFRESH` is executed. There is no read lock, and readers never block writers.

- **Deletion Vectors & Compatibility:**
- Databricks Delta uses **Deletion Vectors** (soft-deletes inside separate bitmap files) for low-latency row-level mutations.
- When using UniForm to publish to Iceberg clients, configure Databricks Runtime 14.3 LTS+ with `REORG TABLE ... APPLY (PURGE)` to materialize true positional delete files that standard external Iceberg readers can parse without errors.

- **VACUUM Rules across Engine Boundaries:**
- Executing `VACUUM` in Databricks permanently purges Parquet files older than the retention threshold (default: 7 days).
- If Snowflake maintains long-running analytical queries or time-travel references beyond the retention period, queries will fail with `FileNotFoundException`.
- **Rule:** Synchronize the Databricks table property `spark.databricks.delta.vacuum.parallelDelete.enabled` retention period with Snowflake's Iceberg time-travel window requirements (e.g., keep both at a minimum of 7 days).

---

## 6. Failure Modes & Mitigations

- **UniForm Async Conversion Lag:**
- _Failure:_ Databricks completes writing the Delta commit, but Airflow triggers the Snowflake refresh before the asynchronous Iceberg metadata writer finishes flushing `vN.metadata.json`.
- _Mitigation:_ Explicitly call `CALL sys.generate_iceberg_metadata(table => 'monthly_revenue')` in Databricks to enforce **synchronous** metadata conversion prior to alerting Airflow.

- **Cloud Egress Traps (Multi-Cloud / Cross-Region):**
- _Failure:_ The S3/ADLS Lakehouse bucket is in `us-east-1`, but Snowflake compute warehouses run in `us-west-2` or Azure. Every Snowflake scan incurs cross-region egress bandwidth fees.
- _Mitigation:_ External volumes **must** reside in the same cloud provider and exact cloud region as the Snowflake account deployment.

- **Schema Drift Out of Sync:**
- _Failure:_ Databricks evolves schema with an altered column data type, breaking Snowflake queries.
- _Mitigation:_ When using Delta UniForm, Snowflake automatically detects added columns upon running `ALTER ... REFRESH`. However, dropping or re-casting column types requires coordinated metadata synchronization and schema validation steps in Airflow before triggering the catalog refresh.
