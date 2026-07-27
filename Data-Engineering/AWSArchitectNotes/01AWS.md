# AWS Data Engineering Architecture Manual

_A Production-Grade Architectural Guide for Principal Data Engineers, Enterprise Architects & Cloud Solution Leads_

---

## 1. High-Level Enterprise Data Lakehouse Architecture

Modern AWS data architectures leverage the **Data Lakehouse pattern**, decoupling storage from compute while unifying ACID transactions, object storage, and specialized query engines.

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                              AWS DATA LAKEHOUSE ARCHITECTURE                           │
│                                                                                        │
│   INGESTION TIER             STORAGE / GOVERNANCE TIER        ANALYTICS & COMPUTE TIER │
│                                                                                        │
│   ┌──────────────────┐       ┌──────────────────────┐        ┌─────────────────────┐   │
│   │ Amazon Kinesis / ├──────►│  S3 Data Lake        ├───────►│ Amazon EMR          │   │
│   │ MSK (Streaming)  │       │  (Bronze / Silver /  │        │ (Apache Spark/Iceberg)│   │
│   └──────────────────┘       │   Gold Buckets)      │        └─────────────────────┘   │
│                              └──────────┬───────────┘                                  │
│   ┌──────────────────┐                  │                    ┌─────────────────────┐   │
│   │ AWS AppFlow /    ├──────────────────┼───────────────────►│ Amazon Redshift     │   │
│   │ DMS (Batch/CDC)  │                  │                    │ (RA3 Serverless)    │   │
│   └──────────────────┘       ┌──────────┴───────────┐        └─────────────────────┘   │
│                              │ AWS Glue Data Catalog│                                  │
│                              │ & AWS Lake Formation │        ┌─────────────────────┐   │
│                              │ (Governance / Fine RLS)───────►│ Amazon Athena       │   │
│                              └──────────────────────┘        │ (Trino Engine)      │   │
│                                                              └─────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────────────┘

```

### Architectural Principles

1. **Separation of Compute and Storage:** Decouple analytical workloads using S3 as the central storage layer, allowing EMR, Athena, and Redshift RA3 instances to scale independently.
2. **Open Table Formats:** Standardize on open storage formats (**Apache Iceberg**, **Delta Lake**) over raw Parquet to enable ACID transactions, time travel, schema evolution, and concurrent write isolation.
3. **Unified Governance:** Enforce security, data lineage, and column/row-level permissions centrally via **AWS Lake Formation** and **AWS Glue Data Catalog** rather than configuring permissions within individual query engines.

---

## 2. Ingestion Tier Engineering

Selecting the right ingestion pipeline requires balancing data velocity, system complexity, and cost profiles.

```
                                  INGESTION DESIGN DECISION TREE
                                                │
                                                ▼
                                   Is source streaming or CDC?
                                                │
                       ┌────────────────────────┴────────────────────────┐
                       │ YES                                             │ NO
                       ▼                                                 ▼
        Real-time latency < 1 second?                       Micro-batch or bulk ingestion?
           ┌───────────┴───────────┐                         ┌───────────┴───────────┐
           │ YES                   │ NO                      │ MICRO-BATCH           │ BULK / CDC
           ▼                       ▼                         ▼                       ▼
      Amazon MSK           Kinesis Data Streams           AWS Glue / EMR          AWS DMS / AppFlow
    (Kafka Cluster)        & Firehose (S3 Sink)          (Spark Ingestion)       (Database CDC)

```

### Ingestion Component Comparison Matrix

| Technology                       | Latency           | Target Scale                          | Key Architectural Trade-off                                                              |
| -------------------------------- | ----------------- | ------------------------------------- | ---------------------------------------------------------------------------------------- |
| **Amazon Kinesis Data Streams**  | Sub-second        | $10\text{ MB/s}$ per shard (scalable) | Managed & serverless; constrained by shard partitioning limits compared to Kafka.        |
| **Amazon MSK (Kafka)**           | Sub-10ms          | Very high ($100\text{k+ msg/s}$)      | Full Kafka API compatibility; requires managing brokers, memory, and zoo/kraft topology. |
| **AWS DMS (Database Migration)** | Near real-time    | Terabytes                             | Low-impact CDC replication; target schema generation can be rigid with LOB types.        |
| **AWS AppFlow**                  | Scheduled / Event | SaaS API Limits                       | No-code integration for Salesforce/ServiceNow; payload file sizing requires tuning.      |

---

## 3. Storage Layer & Table Format Mechanics

### 3.1 Lakehouse Storage Tiering (Medallion Architecture)

```
RAW SOURCES ────► [ BRONZE BUCKET ] ────► [ SILVER BUCKET ] ────► [ GOLD BUCKET ]
                  Raw landing zone        Cleaned, schema-bound     Aggregated, business-
                  (JSON, CSV, Avro)       Apache Iceberg tables     ready data marts

```

### 3.2 Apache Iceberg on Amazon S3

Apache Iceberg eliminates historical S3 file-listing latency bottlenecks (`S3 ListObjects` $O(N)$ overhead) by tracking files using hierarchical metadata JSON/Avro structures.

```
                              ICEBERG SPECIFICATION TOPOLOGY
                                            │
                                            ▼
                                 ┌────────────────────┐
                                 │   Catalog Pointer  │
                                 └──────────┬─────────┘
                                            │
                                            ▼
                                 ┌────────────────────┐
                                 │   vN.metadata.json │
                                 └──────────┬─────────┘
                                            │
                                            ▼
                                 ┌────────────────────┐
                                 │   Manifest List    │
                                 └──────────┬─────────┘
                                            │
                                            ▼
                                 ┌────────────────────┐
                                 │   Manifest File    │
                                 └──────────┬─────────┘
                                            │
                                            ▼
                                 ┌────────────────────┐
                                 │ Data Files (.parquet)
                                 └────────────────────┘

```

#### Production PySpark Script: Writing to Iceberg on AWS EMR / Glue

```python
from pyspark.sql import SparkSession

# Initialize Spark Session with AWS Glue Catalog and Apache Iceberg Extensions
spark = SparkSession.builder \
    .appName("IcebergLakehouseIngest") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://prod-lakehouse-silver-bucket/iceberg/") \
    .getOrCreate()

# Read incoming micro-batch stream
df_incoming = spark.read.json("s3://prod-lakehouse-bronze-bucket/raw_transactions/")

# Upsert (MERGE INTO) target Iceberg Table
df_incoming.createOrReplaceTempView("src_transactions")

spark.sql("""
    MERGE INTO glue_catalog.financial_db.fact_transactions t
    USING src_transactions s
    ON t.transaction_id = s.transaction_id
    WHEN MATCHED THEN
        UPDATE SET t.amount = s.amount, t.status = s.status, t.updated_at = s.updated_at
    WHEN NOT MATCHED THEN
        INSERT (transaction_id, customer_id, amount, status, updated_at)
        VALUES (s.transaction_id, s.customer_id, s.amount, s.status, s.updated_at)
""")

```

---

## 4. Processing & Compute Layer

```
                             PROCESSING COMPUTE SELECTION
                                          │
                                          ▼
                             Analytical Workload Pattern?
                                          │
                   ┌──────────────────────┴──────────────────────┐
                   │ INTERACTIVE / AD-HOC                        │ BATCH / ETL / ML
                   ▼                                             ▼
        Amazon Athena (Trino)                          Dataset Volume Profile?
    • Zero server management                                     │
    • Pay per TB scanned                            ┌────────────┴────────────┐
    • Partition pruning mandatory                   │ < 10 TB                 │ > 10 TB
                                                    ▼                         ▼
                                                AWS Glue              Amazon EMR (EKS/EC2)
                                             (Serverless Spark)      (Custom clusters/Spot)

```

### 4.1 Processing Engine Comparison

- **AWS Glue 4.0/5.0:** Serverless Spark runtime optimized for fast startup times (DPUs — Data Processing Units). Ideal for scheduled ETL workloads and pipeline automation.
- **Amazon EMR (Elastic MapReduce):** Provisioned EC2 clusters, EMR Serverless, or EMR on EKS. Best for high-scale processing ($>10\text{ TB}$) requiring fine-grained control over instance types, memory tuning, and custom Spark configurations.
- **Amazon Athena:** Serverless query engine built on Trino/Presto. Designed for ad-hoc analytical queries directly over S3 data lakes without managing infrastructure.

---

## 5. Amazon Redshift Data Warehousing

Amazon Redshift RA3 nodes decouple compute from storage by leveraging **Redshift Managed Storage (RMS)** backed by high-speed NVMe drives and S3.

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                              REDSHIFT RA3 & DATA SHARING                               │
│                                                                                        │
│   PRODUCER CLUSTER             REDSHIFT MANAGED STORAGE (RMS)    CONSUMER CLUSTER      │
│   ┌─────────────────────┐      ┌──────────────────────────┐      ┌──────────────────┐  │
│   │ RA3 Compute Nodes   ├─────►│ Distributed S3 Tier      │◄─────┤ RA3 Compute Nodes│  │
│   │ (ETL & Ingestion)   │      │ (Transactional Storage)  │      │ (BI & Analytics) │  │
│   └─────────────────────┘      └──────────────────────────┘      └──────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────────────────┘

```

### Production Optimization & DDL Mechanics

#### Redshift Table DDL with Compound Keys and Auto Distribution

```sql
CREATE TABLE dw_prod.fact_sales (
    sales_id BIGINT NOT NULL ENCODE az64,
    customer_id INTEGER NOT NULL ENCODE az64,
    store_id SMALLINT NOT NULL ENCODE az64,
    transaction_time TIMESTAMP NOT NULL ENCODE az64,
    gross_amount NUMERIC(18,4) ENCODE az64,
    PRIMARY KEY (sales_id)
)
DISTSTYLE KEY
DISTKEY (customer_id)
COMPOUND SORTKEY (transaction_time, customer_id);

```

#### Key Redshift Engineering Rules

1. **Redshift Data Sharing:** Share live data across isolated Redshift clusters without copying or running complex ETL export pipelines.
2. **Auto WLM & Concurrency Scaling:** Configure Workload Management (WLM) with memory isolation for long-running ETL queries versus fast interactive BI dashboards.
3. **Redshift Spectrum vs. Local Tables:** Use local RA3 tables for high-frequency $80/20$ analytical joins; route historical archive queries ($>3\text{ years}$) to S3 via Redshift Spectrum external tables.

---

## 6. Security, Governance & Orchestration

### 6.1 Unified Data Governance with AWS Lake Formation

AWS Lake Formation simplifies security management by replacing traditional IAM bucket policies with granular access controls.

```
                                LAKE FORMATION GOVERNANCE
                                            │
                 ┌──────────────────────────┼──────────────────────────┐
                 ▼                          ▼                          ▼
      TABLE-LEVEL PERMISSIONS    COLUMN-LEVEL PERMISSIONS     ROW-LEVEL FILTERING
      Grants SELECT / ALTER      Filters out PII columns      Applies predicate logic
      access per role            (e.g., SSN, credit cards)    (e.g., Region = 'EU')

```

### 6.2 Pipeline Orchestration: AWS Step Functions vs. Apache Airflow (MWAA)

- **AWS Step Functions:** Serverless, event-driven orchestration natively integrated with AWS services (Glue, EMR, Athena, Lambda). Excellent for deterministic state machine workflows with low maintenance overhead.
- **Amazon MWAA (Managed Workflows for Apache Airflow):** Highly scalable, code-centric DAG framework. Essential for complex cross-cloud workflows, dynamic task generation, and teams with existing Python/Airflow codebases.

---

## 7. Cost Optimization & Operational Playbook

### 1. Amazon S3 Storage Lifecycle Rules

Transition Bronze raw files to **S3 Glacier Flexible Retrieval** or **Glacier Deep Archive** after 90 days. Configure S3 Intelligent-Tiering for dynamic usage patterns.

### 2. Spot Instance Engineering on EMR

Build EMR clusters using **Instance Fleets** combining multiple EC2 instance families (e.g., `m5.xlarge`, `m6g.xlarge`, `r5.xlarge`). Allocate **Spot Instances for Task Nodes** (stateless computation) and **On-Demand/Savings Plans for Primary/Core Nodes** (HDFS state holders).

### 3. Athena Cost Control via Workgroups

Enforce strict query data scan caps per query and per workgroup using **Athena Workgroups**:

```bash
aws athena update-workgroup \
    --workgroup "BI_Analytics_Workgroup" \
    --configuration-updates "BytesScannedCutoffPerQuery=107374182400" # 100 GB Limit

```

---
