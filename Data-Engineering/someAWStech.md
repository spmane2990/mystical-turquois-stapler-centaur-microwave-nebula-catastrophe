## Amazon S3

Amazon S3 is an object store organized into buckets containing keys, objects, and metadata. Unlike a POSIX filesystem, it is a flat namespace with zero rename semantics. Renaming a "folder" is an $O(N)$ copy-and-delete cascade of every nested object.

### Architectural Foundations & Mechanics

- **Strong Read-After-Write Consistency:** S3 provides atomic read-after-write consistency for `PUT`, `POST`, and `DELETE` requests of objects, as well as bucket metadata listings. An immediate `GET` or `LIST` after an overwrite or delete reflects the latest mutation across all AWS regions.
- **Partitioning Mechanics & Request Throttling:** S3 scales request bandwidth automatically by prefix. Each distinct partition prefix supports:
- **3,500 `PUT`/`POST`/`DELETE` requests per second.**
- **5,500 `GET`/`HEAD` requests per second.**
- _Optimization Pattern:_ Avoid monolithic root paths for parallel ingestion. Do not land thousands of concurrent files into `s3://bucket/data/*.parquet`. Distribute high-throughput ingestion using high-cardinality prefixes:
  `s3://bucket/data/hash_prefix=a1b2/year=2026/month=09/data.parquet`

- **Storage Tiers & Compaction Trade-offs:**
- _Standard:_ Frequent access, no retrieval fees, zero minimum storage duration.
- _Intelligent-Tiering:_ Automates transitions across Frequent, Infrequent, and Archive Instant access tiers using monitoring logic. Includes a monthly monitoring/automation charge per 1,000 objects. **Avoid for micro-files under 128 KB**; it introduces net-negative cost overhead.
- _Glacier Instant Retrieval:_ Millisecond access, lower storage cost, high data retrieval charge per GB ($0.03/GB). Target for compliance-bound, cold raw data.
- _Small File Anti-Pattern:_ 1 million 1 KB files cost $0.023/month in storage, but incur $5.00 in S3 `PUT` costs, balloon Glue crawler durations, degrade Spark driver memory during partition discovery, and increase Snowflake stage scanning overhead. Standardize target file sizes to **128 MB – 512 MB**.

### Production Implementation

#### Multi-Tier S3 Lifecycle Configuration

Transitions raw landing data to Glacier Instant Retrieval after 30 days and permanently purges non-current object versions after 14 days.

```json
{
  "Rules": [
    {
      "ID": "PruneLandingAndTierToGlacier",
      "Filter": {
        "Prefix": "raw/landing/"
      },
      "Status": "Enabled",
      "Transitions": [
        {
          "Days": 30,
          "StorageClass": "GLACIER_IR"
        }
      ],
      "NoncurrentVersionExpiration": {
        "NoncurrentDays": 14
      },
      "AbortIncompleteMultipartUpload": {
        "DaysAfterInitiation": 7
      }
    }
  ]
}
```

---

## AWS IAM

AWS Identity and Access Management (IAM) controls authentication (who is verified) and authorization (what actions are permitted) across AWS API boundaries.

### Architectural Foundations & Mechanics

- **Policy Evaluation Logic:** Default Deny $\to$ Explicit Allow $\to$ Explicit Deny (which overrides any Allow).
- **Cross-Account Trust Boundaries & The Confused Deputy Problem:** When delegating third-party or multi-tenant system access (e.g., Snowflake, Databricks, external SaaS ETL), a malicious actor could leverage the third-party's AWS role to query your bucket if authorization relies solely on target Account IDs. Prevent this using strict `sts:ExternalId` matching.
- **Permission Boundaries:** IAM entities can be constrained by an administrative boundary policy, establishing the maximum permissions an identity can hold regardless of inline or attached policies.
- **Session Management & STS:** Machine-to-machine ETL leverages AWS Security Token Service (STS) to assume roles (`sts:AssumeRole`), yielding temporary credentials (`AccessKeyId`, `SecretAccessKey`, `SessionToken`) that expire within 15 minutes to 12 hours.

### Production Implementation

#### IAM Role Trust & Access Policy for Snowflake External Stages

Enforces cross-account validation using an external ID, TLS 1.2+, and minimum S3 read/write permissions.

```json
// Trust Policy attached to IAM Role: SnowflakeDWHExecutionRole
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "SnowflakeCrossAccountTrust",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::444455556666:user/snowflake-service-user"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "MYORGANIZATION_SF_STAGE_ID_78910"
        }
      }
    }
  ]
}
```

```json
// Identity-Based Policy attached to SnowflakeDWHExecutionRole
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowS3OperationsOnCuratedBucket",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::prod-analytics-lake-us-east-1",
        "arn:aws:s3:::prod-analytics-lake-us-east-1/curated/*"
      ],
      "Condition": {
        "NumericGreaterThanEquals": {
          "s3:TlsVersion": 1.2
        }
      }
    }
  ]
}
```

---

## AWS Lake Formation

Lake Formation is an orchestration and governance overlay for S3 and the AWS Glue Data Catalog. It abstracts low-level S3 IAM bucket policies into database-style Access Control Lists (ACLs).

### Architectural Foundations & Mechanics

- **IAM vs. Lake Formation Permissions:**
- _IAM Access:_ Principals require direct `s3:GetObject` and `glue:GetTable` API allowances. Coarse-grained (prefix or table level).
- _Lake Formation Model:_ The storage location is registered with Lake Formation (`lakeformation:RegisterResource`). Lake Formation assumes its own service-linked role to access S3. Principals need only `lakeformation:GetDataAccess` and Lake Formation permissions (`SELECT`, `DESCRIBE`). Direct IAM bucket access is completely stripped from users.

- **Credential Vending Machine:** When an engine (Athena, EMR, Glue) queries a table, it calls Lake Formation, which evaluates policy and vends temporary, scoped S3 credentials back to the engine. The client engine accesses S3 directly using these ephemeral keys.
- **Fine-Grained Access Control (FGAC):** Lake Formation enforces column-level restrictions (projection filtering), row-level security (SQL predicate filtering applied at query time), and cell-level masking (hashing, nulling, or redaction) without modifying physical Parquet files.
- **Hybrid Access Mode:** Allows legacy workloads with direct IAM access to function alongside new LF-governed policies on the same tables. To fully secure a catalog asset, you must revoke the default `IAM_ALLOWED_PRINCIPALS` permission group from the table.

### Production Implementation

#### Revoking Legacy IAM and Granting Row/Column Redaction via AWS CLI

```bash
# 1. Revoke default legacy IAM fall-through on curated table
aws lakeformation revoke-permissions \
  --principal '{"DataLakePrincipalIdentifier": "IAM_ALLOWED_PRINCIPALS"}' \
  --resource '{"Table": {"DatabaseName": "financial_dw", "Name": "wire_transactions"}}' \
  --permissions '["ALL"]'

# 2. Grant Restricted Access: Drop PII columns (ssn, recipient_account) and filter row predicates
aws lakeformation grant-permissions \
  --principal '{"DataLakePrincipalIdentifier": "arn:aws:iam::111122223333:role/DataAnalystTier1"}' \
  --resource '{
    "DataCellsFilter": {
      "TableCatalogId": "111122223333",
      "DatabaseName": "financial_dw",
      "TableName": "wire_transactions",
      "Name": "us_transactions_no_pii",
      "RowFilter": {"FilterExpression": "country_code = 'USA' AND transaction_amount < 10000"},
      "ColumnWildcard": {
        "ExcludedColumnNames": ["ssn", "recipient_account", "internal_routing_notes"]
      }
    }
  }' \
  --permissions '["SELECT"]'

```

---

## AWS Glue

AWS Glue provides a serverless metadata repository (Glue Data Catalog) and distributed compute engines (Spark, Streaming, Ray, Python Shell) to run heavy ETL without managing cluster infrastructure.

### Architectural Foundations & Mechanics

- **Glue Data Catalog:** A managed Hive Metastore (HMS) equivalent. Stores schema definitions, partition schemas, SerDe information, and data locations. External compute engines (Snowflake, Presto, Athena, Spark) use the Catalog for query compilation.
- **Crawlers vs. Push-Metadata Ingestion:**
- _Crawlers:_ Asynchronously infer schema via built-in classifiers by scanning a sample of S3 files. They are costly, non-deterministic (schema drift can break types), and slow over deep partition hierarchies.
- _Push Pattern:_ Modern pipelines bypass crawlers. Spark jobs or Step Functions write schemas explicitly into the Glue Catalog via the `GlueClient` API (`CreateTable`, `BatchCreatePartition`) or `saveAsTable()` operations.

- **DPU (Data Processing Unit) Sizing:**
- 1 DPU = 4 vCPUs and 16 GB memory.
- _Worker Types:_
- `G.1X` (1 DPU = 1 executor, 8 dynamic partitions target): Optimized for memory-intensive batch jobs.
- `G.2X` (2 DPUs = 1 executor, higher memory/compute density): Ideal for complex transforms, skew mitigation, and large shuffles.
- `G.4X` / `G.8X`: High-scale caching and deep machine learning workloads.

- **Job Bookmarks:** Glue tracks state across executions by maintaining processed file metadata in an internal key-value store, preventing duplicate processing of append-only S3 sources.
- **Iceberg Native Engine:** Glue natively writes and optimizes Apache Iceberg tables, managing snapshots, metadata pointers, and data compactions directly.

### Production Implementation

#### Production-Grade Glue PySpark ETL (Glue 4.0 / Spark 3.3+)

Reads multi-source JSON logs, cleanses data, eliminates shuffle partitions, applies programmatic date partitioning, writes snappy-compressed Parquet, and injects partitions directly into the Data Catalog.

```python
import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, upper, lit
from pyspark.sql.types import LongType, DoubleType

# Resolve input parameters
args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'INPUT_S3_PATH', 'OUTPUT_S3_PATH', 'DATABASE_NAME', 'TABLE_NAME']
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Configure Spark for optimized partitioning and catalog sync
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")

# Read payload using Glue dynamic frame to leverage Job Bookmarks
datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [args['INPUT_S3_PATH']],
        "recurse": True
    },
    format="json",
    transformation_ctx="raw_transaction_bookmark"
)

df = datasource.toDF()

# Transformations & schema enforcement
transformed_df = df.filter(col("transaction_id").isNotNull()) \
    .withColumn("amount", col("amount").cast(DoubleType())) \
    .withColumn("customer_id", col("customer_id").cast(LongType())) \
    .withColumn("status", upper(col("status"))) \
    .withColumn("tx_date", to_date(col("timestamp"))) \
    .dropDuplicates(["transaction_id"])

# Repartition dynamically to control output file sizes (avoids small files)
# Number of partitions tuned based on output data size: target 256MB per file
transformed_df = transformed_df.repartition(col("tx_date"))

# Write directly to S3 partitioned by date, registering with Glue Catalog
transformed_df.write \
    .mode("overwrite") \
    .format("parquet") \
    .partitionBy("tx_date") \
    .option("path", args['OUTPUT_S3_PATH']) \
    .saveAsTable(f"{args['DATABASE_NAME']}.{args['TABLE_NAME']}")

job.commit()

```

---

## Amazon Athena

Amazon Athena is an interactive query engine built on open-source Trino and Presto, running serverless SQL queries directly over data in S3.

### Architectural Foundations & Mechanics

- **Serverless Query Execution Engine:** Scales workers dynamically per query execution. Compiles SQL statements into distributed execution DAGs (Directed Acyclic Graphs), assigning tasks across transient worker fleets.
- **Cost Topology:** Standard pricing charges **$5.00 per TB of data scanned**. Queries reading uncompressed CSV/JSON pay a severe penalty because the entire file must be scanned off disk. Columnar Parquet with Snappy/ZSTD compression reduces scanned bytes by 80–95%, directly cutting query costs.
- **Partition Pruning:** Requires queries to leverage partition columns in the `WHERE` clause. Scanning `year=2026/month=09` skips all other object prefixes, minimizing S3 read limits and costs.
- **Partition Projection:** Replaces the need for frequent catalog partition indexing (`MSCK REPAIR TABLE` or crawler updates). Computes partition locations deterministically via predefined regex/range rules in catalog table properties, avoiding metadata retrieval bottlenecks.
- **Athena Workgroups:** Enforce governance boundaries, including per-query data scan limits (which immediately cancel runaway queries), query metrics collection, and isolated S3 output result locations.

### Production Implementation

#### CTAS Compaction Script with Partition Projection Table

```sql
-- 1. Table with Partition Projection (Zero Crawlers Required)
CREATE EXTERNAL TABLE IF NOT EXISTS lake_analytics.network_logs (
    event_id STRING,
    source_ip STRING,
    destination_ip STRING,
    bytes_transferred BIGINT,
    action STRING
)
PARTITIONED BY (log_date STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION 's3://prod-analytics-lake-us-east-1/raw/network_logs/'
TBLPROPERTIES (
    'has_encrypted_data'='false',
    'projection.enabled'='true',
    'projection.log_date.type'='date',
    'projection.log_date.range'='2025/01/01,NOW',
    'projection.log_date.format'='yyyy/MM/dd',
    'projection.log_date.interval'='1',
    'projection.log_date.interval.unit'='DAYS',
    'storage.location.template'='s3://prod-analytics-lake-us-east-1/raw/network_logs/${log_date}/'
);

-- 2. Compaction CTAS: Merges daily small files into compressed 256MB Parquet chunks
CREATE TABLE lake_analytics.network_logs_compacted_2026_09_10
WITH (
    format = 'PARQUET',
    parquet_compression = 'SNAPPY',
    external_location = 's3://prod-analytics-lake-us-east-1/curated/network_logs/log_date=2026-09-10/'
) AS
SELECT
    event_id,
    source_ip,
    destination_ip,
    bytes_transferred,
    action
FROM lake_analytics.network_logs
WHERE log_date = '2026/09/10';

```

---

## AWS Step Functions

AWS Step Functions coordinates distributed microservices, big data pipelines, and serverless compute using deterministic finite-state machines.

### Architectural Foundations & Mechanics

- **Workflow Types:**
- _Standard Workflows:_ Exactly-once execution semantics, maximum run time up to 1 year, full visual audit history and state recovery. Essential for batch ETL, long-running Glue tasks, and multi-step data pipelines.
- _Express Workflows:_ At-least-once execution semantics, 5-minute maximum run time, high-throughput execution (100,000+ executions/sec). Ideal for high-rate streaming ingestion and fast REST microservice orchestration.

- **Service Integrations Patterns:**
- _Request-Response:_ Invokes an API and advances state immediately (e.g., triggering a Lambda or starting an async job without waiting).
- _Run a Job (`.sync`):_ Pushes task execution down to a native AWS engine (Glue, Athena, EMR) and pauses the state machine until that external job returns a success or failure status code.
- _Wait-for-Task-Token (`.waitForTaskToken`):_ Pauses the state machine indefinitely until an external process callbacks using the generated token. Used for manual human reviews or third-party webhooks (e.g., dbt Cloud job callbacks).

- **Error Handling & Resiliency:** Features built-in `Retry` backoffs with jitter and `Catch` routes that handle specific runtime exceptions (like `Glue.ConcurrentRunsExceededException` or `States.Timeout`) without requiring custom orchestration code.

### Production Implementation

#### Complete Production ASL (Amazon States Language) Definition

Orchestrates a Glue Spark job, runs an Athena validation query, and triggers an SNS alert on job failure.

```json
{
  "Comment": "Production Orchestration: Glue Spark -> Athena Quality Gate",
  "StartAt": "TriggerGlueIngestion",
  "States": {
    "TriggerGlueIngestion": {
      "Type": "Task",
      "Resource": "arn:aws:states:::glue:startJobRun.sync",
      "Parameters": {
        "JobName": "DailySalesJob",
        "Arguments": {
          "--DATABASE_NAME": "financial_dw",
          "--TABLE_NAME": "wire_transactions",
          "--OUTPUT_S3_PATH": "s3://prod-analytics-lake-us-east-1/curated/wire_transactions/"
        }
      },
      "Retry": [
        {
          "ErrorEquals": [
            "Glue.ConcurrentRunsExceededException",
            "Glue.ResourceNumberLimitExceededException"
          ],
          "IntervalSeconds": 60,
          "MaxAttempts": 3,
          "BackoffRate": 2.0
        }
      ],
      "Catch": [
        {
          "ErrorEquals": ["States.ALL"],
          "ResultPath": "$.errorInfo",
          "Next": "PipelineFailureAlert"
        }
      ],
      "Next": "RunDataValidationQuery"
    },
    "RunDataValidationQuery": {
      "Type": "Task",
      "Resource": "arn:aws:states:::athena:startQueryExecution.sync",
      "Parameters": {
        "QueryString": "SELECT COUNT(*) FROM financial_dw.wire_transactions WHERE tx_date = CURRENT_DATE AND transaction_id IS NULL",
        "WorkGroup": "primary",
        "ResultConfiguration": {
          "OutputLocation": "s3://prod-analytics-lake-us-east-1/athena-audit-results/"
        }
      },
      "ResultPath": "$.validationResult",
      "Next": "EvaluateQualityOutput"
    },
    "EvaluateQualityOutput": {
      "Type": "Choice",
      "Choices": [
        {
          "Variable": "$.validationResult.QueryExecution.Status.State",
          "StringEquals": "SUCCEEDED",
          "Next": "PipelineCompleteSuccess"
        }
      ],
      "Default": "PipelineFailureAlert"
    },
    "PipelineCompleteSuccess": {
      "Type": "Succeed"
    },
    "PipelineFailureAlert": {
      "Type": "Task",
      "Resource": "arn:aws:states:::sns:publish",
      "Parameters": {
        "TopicArn": "arn:aws:sns:us-east-1:111122223333:data-ops-pipeline-failures",
        "Subject": "ETL PIPELINE ERROR: Failure in Glue or Athena Validation Step",
        "Message.$": "$.errorInfo"
      },
      "End": true
    }
  }
}
```

---

## Amazon MWAA (Managed Workflows for Apache Airflow)

MWAA provides a managed Apache Airflow deployment on AWS, handling scheduler provisioning, worker auto-scaling, metadata database patching, and integrated security.

### Architectural Foundations & Mechanics

- **Architecture & Topology:** MWAA runs in an AWS-managed VPC, communicating with your environment via VPC Endpoints (PrivateLink). It separates the Web Server, PostgreSQL Metadata DB, and Celery Executor Workers.
- **Worker Auto-Scaling:** MWAA provisions Fargate tasks dynamically based on the `QueuedTasks` CloudWatch metric. If `QueuedTasks > 0`, it provisions additional workers up to the configured `max_workers` threshold.
- **Dependency Management & Constraints:** Define packages in `requirements.txt`.
- _Best Practice:_ Always append the Airflow-provided constraint file. Missing constraints can trigger pip resolution failures during worker autoscaling, bricking dynamic worker spin-ups.

- **Execution Isolation Anti-Pattern:** Never run heavy data processing (such as Pandas data frame transformations or raw file merges) directly inside the Airflow worker memory space. The MWAA worker should act solely as an orchestration control plane, dispatching compute workloads to external engines like Glue, EMR, Athena, or Snowflake.

### Production Implementation

#### Production DAG: S3 Extraction -> Glue Job -> Snowflake Loading & Transformation

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email': ['dataops@enterprise.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='mwaa_s3_glue_to_snowflake_pipeline_v1',
    default_args=DEFAULT_ARGS,
    description='Polls S3 landing zone, executes Glue transformation, loads into Snowflake, and merges.',
    schedule_interval='0 4 * * *', # Daily at 04:00 AM UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['production', 'finance', 'snowflake']
) as dag:

    # 1. Wait for landing files to be delivered into S3
    wait_for_raw_s3_data = S3KeySensor(
        task_id='wait_for_raw_s3_data',
        bucket_name='prod-analytics-lake-us-east-1',
        bucket_key='raw/landing/sales_feed_*.json',
        wildcard_match=True,
        aws_conn_id='aws_default',
        timeout=60 * 60 * 2, # 2 hours
        poke_interval=120,    # Check every 2 minutes
        mode='reschedule'     # Releases Airflow worker slot during sleep
    )

    # 2. Trigger AWS Glue Serverless Spark Job
    run_glue_cleansing = GlueJobOperator(
        task_id='run_glue_cleansing',
        job_name='DailySalesJob',
        region_name='us-east-1',
        aws_conn_id='aws_default',
        script_args={
            '--INPUT_S3_PATH': 's3://prod-analytics-lake-us-east-1/raw/landing/',
            '--OUTPUT_S3_PATH': 's3://prod-analytics-lake-us-east-1/curated/sales/'
        },
        wait_for_completion=True
    )

    # 3. Load from S3 into Snowflake Staging Table
    copy_s3_to_snowflake = SnowflakeOperator(
        task_id='copy_s3_to_snowflake',
        snowflake_conn_id='snowflake_production_conn',
        sql="""
            COPY INTO PROD_DWH.STAGING.TRANSACTIONS_STAGE
            FROM @PROD_DWH.EXTERNAL_STAGES.S3_CURATED_LAKE_STAGE/sales/
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            PURGE = FALSE
            ON_ERROR = 'ABORT_STATEMENT';
        """
    )

    # 4. Atomic Merge Staging data into Target Analytics Production Table
    merge_snowflake_final = SnowflakeOperator(
        task_id='merge_snowflake_final',
        snowflake_conn_id='snowflake_production_conn',
        sql="""
            MERGE INTO PROD_DWH.ANALYTICS.FACT_TRANSACTIONS tgt
            USING PROD_DWH.STAGING.TRANSACTIONS_STAGE src
            ON tgt.transaction_id = src.transaction_id
            WHEN MATCHED AND src.status = 'CANCELLED' THEN
                DELETE
            WHEN MATCHED THEN
                UPDATE SET tgt.amount = src.amount, tgt.updated_at = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (transaction_id, customer_id, amount, status, tx_date)
                VALUES (src.transaction_id, src.customer_id, src.amount, src.status, src.tx_date);

            -- Empty staging table to prepare for subsequent cycle
            TRUNCATE TABLE PROD_DWH.STAGING.TRANSACTIONS_STAGE;
        """
    )

    # Define orchestration graph
    wait_for_raw_s3_data >> run_glue_cleansing >> copy_s3_to_snowflake >> merge_snowflake_final

```

---

## Snowflake Interoperability

Snowflake connects to AWS data lakes through two primary patterns: ingestion stages for physical loading, and external tables or Iceberg tables for in-place querying.

```
+-----------------------------------------------------------------------------------+
|                               AWS ACCOUNT (DATA LAKE)                             |
|                                                                                   |
|  +------------------+          +-----------------------+      +----------------+  |
|  | S3 Curated Layer |  <----   | Lake Formation / Glue | <--- | IAM Trust Role |  |
|  +------------------+          +-----------------------+      +----------------+  |
+-----------^------------------------------------------------------------^----------+
            |                                                            |
            | (Read via Presigned / Delegated Access)                    | AssumeRole
            |                                                            |
+-----------+------------------------------------------------------------+----------+
|           |                 SNOWFLAKE VIRTUAL WAREHOUSE                |          |
|  +------------------+                                      +-------------------+  |
|  |  EXTERNAL STAGE  | <=================================== | STORAGE / CATALOG |  |
|  |  (Direct S3)     |                                      | INTEGRATION       |  |
|  +--------+---------+                                      +-------------------+  |
|           |                                                                       |
|      COPY INTO (ELT)                                                              |
|           |                                                                       |
|           v                                                                       |
|  +------------------+          +------------------------+                         |
|  | Internal Tables  |          | Apache Iceberg Tables  |                         |
|  | (Snowflake Marts)|          | (Zero-Copy Open Format)|                         |
|  +------------------+          +------------------------+                         |
+-----------------------------------------------------------------------------------+

```

### Storage Integration Deep-Dive

- Avoid setting access keys directly in Snowflake credentials (`AWS_KEY_ID`, `AWS_SECRET_KEY`). This anti-pattern prevents credential rotation, violates compliance standards, and fails basic least-privilege checks.
- Use a **Snowflake Storage Integration**: an explicit Snowflake object that encapsulates external cloud storage authentication via an AWS IAM Role.
- Once created, Snowflake generates a unique user ARN (`STORAGE_AWS_IAM_USER_ARN`) and external ID (`STORAGE_AWS_EXTERNAL_ID`). You bind these into your target AWS IAM Role Trust Policy to establish secure cross-account identity delegation.

### Apache Iceberg Architectural Pattern

Snowflake can manage Iceberg tables directly or register them as external entities using an AWS Glue Data Catalog integration:

- **Snowflake-Managed Tables:** Snowflake writes data directly to your customer S3 bucket using Parquet and manages the Iceberg metadata JSON, snapshot files, and manifest lists.
- **External (Glue-Managed) Tables:** AWS Glue Spark jobs write the Iceberg table and catalog metadata into S3. Snowflake queries it read-only via a **Catalog Integration** without duplicating or loading the underlying data.

### Production Implementation

#### 1. Configure Snowflake Storage Integration & External Stage

```sql
-- Step 1: Instantiate the Storage Integration within Snowflake
CREATE OR REPLACE STORAGE INTEGRATION s3_lake_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::111122223333:role/SnowflakeDWHExecutionRole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://prod-analytics-lake-us-east-1/curated/');

-- Step 2: Extract the identity generated by Snowflake for cross-account configuration
DESC INTEGRATION s3_lake_integration;
-- RECORD THE FOLLOWING GENERATED OUTPUTS:
-- 1. STORAGE_AWS_IAM_USER_ARN (e.g., 'arn:aws:iam::123456789012:user/ab12-snowflake-engine')
-- 2. STORAGE_AWS_EXTERNAL_ID   (e.g., 'MYORGANIZATION_SF_STAGE_ID_78910')
-- Update these exact values in the AWS IAM Role trust policy statement shown earlier.

-- Step 3: Define file format and point external stage to the S3 bucket location
CREATE OR REPLACE FILE FORMAT PROD_DWH.COMMON.PARQUET_FORMAT
  TYPE = 'PARQUET'
  COMPRESSION = 'SNAPPY';

CREATE OR REPLACE STAGE PROD_DWH.EXTERNAL_STAGES.S3_CURATED_LAKE_STAGE
  STORAGE_INTEGRATION = s3_lake_integration
  URL = 's3://prod-analytics-lake-us-east-1/curated/'
  FILE_FORMAT = PROD_DWH.COMMON.PARQUET_FORMAT;

```

#### 2. Query In-Place Using Snowflake Iceberg External Catalog

```sql
-- Create an external catalog integration pointing to the AWS Glue Data Catalog
CREATE OR REPLACE CATALOG INTEGRATION glue_catalog_integration
  CATALOG_SOURCE = 'GLUE'
  CATALOG_NAMESPACE = 'financial_dw'
  TABLE_FORMAT = 'ICEBERG'
  GLUE_AWS_ROLE_ARN = 'arn:aws:iam::111122223333:role/SnowflakeDWHExecutionRole'
  GLUE_CATALOG_ID = '111122223333'
  ENABLED = TRUE;

-- Create the Iceberg Table inside Snowflake that queries S3 data via Glue metadata
CREATE OR REPLACE ICEBERG TABLE PROD_DWH.ANALYTICS.ICEBERG_WIRE_TRANSACTIONS
  EXTERNAL_VOLUME = 's3_lake_integration'
  CATALOG = 'glue_catalog_integration'
  CATALOG_TABLE_NAME = 'wire_transactions';

-- Query the Iceberg table directly using Snowflake virtual warehouse compute
SELECT
    tx_date,
    COUNT(*) as total_txs,
    SUM(amount) as total_volume
FROM PROD_DWH.ANALYTICS.ICEBERG_WIRE_TRANSACTIONS
WHERE tx_date >= DATEADD('day', -30, CURRENT_DATE())
GROUP BY tx_date
ORDER BY tx_date DESC;

```

---

## Troubleshooting & Debugging

| Service            | Failure Signature / Error Code                               | Root Cause Mechanics                                                                                                                                              | Production Mitigation Strategy                                                                                                                              |
| ------------------ | ------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **S3**             | `503 Slow Down`                                              | Exceeded prefix throttling limits (3,500 `PUT` or 5,500 `GET` per second per partition).                                                                          | Implement exponential backoff retry algorithms; introduce deterministic partition hashing into deep prefixes.                                               |
| **IAM**            | `403 Access Denied: AssumeRole`                              | Mismatch in `sts:ExternalId`, missing `sts:AssumeRole` in target trust document, or permission boundary clipping.                                                 | Run `aws sts decode-authorization-message` to extract decoded permission failures; verify role trust policies against integration definitions.              |
| **Lake Formation** | `400 EntityNotFound: Data Cells Filter` or empty result sets | Principal granted permission in Lake Formation, but the underlying table still contains `IAM_ALLOWED_PRINCIPALS`, or the row filter predicate evaluates to false. | Ensure legacy IAM catalog grants are fully revoked; confirm the service executing the query uses temporary credentials vended by Lake Formation.            |
| **Glue**           | `Exit code 1: OOM / Java Heap Space`                         | Spark Driver or Executor Out-Of-Memory caused by partition skew, huge file ingestion, or insufficient executor memory allocations.                                | Upgrade worker types from `G.1X` to `G.2X`; tune `spark.sql.shuffle.partitions`; inject dynamic salt keys into skewed join or group-by columns.             |
| **Athena**         | `Query exhausted resources at this scale factor`             | Deep non-indexed scans, excessive cross-joins, or queries operating on hundreds of thousands of micro-files.                                                      | Enforce partition pruning in the `WHERE` clause; replace dynamic crawlers with partition projection; run CTAS compaction scripts on target S3 prefixes.     |
| **Step Functions** | `States.TaskFailed: Glue.ConcurrentRunsExceededException`    | The state machine triggered more concurrent Glue runs than the maximum concurrency limit configured for the job.                                                  | Add an exponential retry block matching `Glue.ConcurrentRunsExceededException` directly in the Amazon States Language (ASL) task definition.                |
| **MWAA**           | `Scheduler unresponsiveness / Worker eviction`               | Heavy data processing (e.g., in-memory Pandas data transformations) executed directly inside Airflow tasks, crashing worker containers.                           | Move transformations out of the DAG process; use MWAA solely as an orchestrator that delegates execution to Glue or Snowflake tasks via standard providers. |
| **Snowflake**      | `Access Denied (403)` on `COPY INTO` or Stage creation       | Snowflake storage integration trust mismatch, expired STS session tokens, or an S3 bucket policy explicitly denying access.                                       | Run `DESC INTEGRATION <name>` and verify that `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` match the AWS IAM Role Trust Policy conditions.      |
