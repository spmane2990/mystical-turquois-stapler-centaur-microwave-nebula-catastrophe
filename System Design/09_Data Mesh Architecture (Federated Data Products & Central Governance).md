# Scenario 9: Data Mesh Architecture (Federated Data Products & Central Governance)

This design addresses transitioning a centralized, bottlenecked data team into a decentralized **Data Mesh**. In this topology, business units build, maintain, and publish their own **Data Products** autonomously—such as a **Marketing Domain** operating on **Azure Databricks** and a **Finance Domain** operating on **Snowflake on AWS**—while a federated corporate governance layer enforces compliance, access control, and cross-domain data sharing without copying data.

---

## 1. Core Data Mesh Principles & Platform Contracts

- **Domain Ownership:** Domains own the end-to-end lifecycle (ingestion, transformation, CI/CD, and quality) of their data products.
- **Data as a Product (DaaP):** Tables are not internal dumping grounds; they have documented SLOs, uptime SLAs, explicit schemas, and backward-compatibility guarantees.
- **Self-Serve Data Platform:** Central data infrastructure provides automated landing zones, IAM scaffolding, compute blueprints, and telemetry templates.
- **Federated Computational Governance:** Central governance defines global security, masking, and classification policies (e.g., PII tags), enforced dynamically at query time across clouds.

```
┌──────────────────────────────────────┐     ┌──────────────────────────────────────┐
│ MARKETING DOMAIN                     │     │ FINANCE DOMAIN                       │
│ Infra: Azure Databricks + ADLS Gen2  │     │ Infra: Snowflake on AWS              │
│ Product: "Customer Lifetime Value"   │     │ Product: "Daily Billed Revenue"      │
└──────────────────┬───────────────────┘     └──────────────────┬───────────────────┘
                   │                                            │
                   ▼                                            ▼
      [ Unity Catalog / Delta Sharing ]        [ Snowflake Secure Data Sharing ]
                   │                                            │
                   └─────────────────────┬──────────────────────┘
                                         ▼
           ┌──────────────────────────────────────────────────────────┐
           │ Central Governance & Interoperability Layer              │
           │  - Federated Identity: Azure Entra ID ↔ AWS IAM          │
           │  - Cross-Domain Sharing: Open Delta Sharing Protocol     │
           │  - Global Orchestration & Contracts: AWS MWAA            │
           │  - Global Catalog & Discovery: AWS Glue / Unity Catalog  │
           └──────────────────────────────────────────────────────────┘

```

---

## 2. Cross-Domain Zero-Copy Sharing Mechanics

Cross-cloud, cross-engine data sharing must avoid periodic batch exports, SFTP drops, or duplicate data synchronization jobs.

```
[ Domain A: Azure Databricks (Producer) ]
   Delta Lake Table on ADLS Gen2
                 │
                 ▼
   [ Unity Catalog Delta Sharing Server ]
                 │
                 │ (Open Delta Sharing Protocol over HTTPS)
                 │ - Short-lived pre-signed Parquet URLs
                 │ - Zero Data Movement / Compute Decoupled
                 ▼
[ Domain B: Snowflake on AWS (Consumer) ]
   External Iceberg / Delta Stage ──► Local Warehouse Query Execution

```

### Protocol Comparison

| Vector                    | Delta Sharing (Open Protocol)                                                           | Snowflake Secure Data Sharing                                                                |
| ------------------------- | --------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| **Vendor Portability**    | Open-source standard; any client (Python, Spark, Snowflake, PowerBI) can consume.       | Native to Snowflake; fastest when both producer and consumer run Snowflake.                  |
| **Compute Billing**       | Reader pays for their own compute; producer only incurs storage egress bandwidth costs. | Reader pays for their own virtual warehouse; zero compute charged to provider.               |
| **Cross-Cloud Mechanics** | Unity Catalog serves short-lived SAS/pre-signed URLs direct to underlying Parquet.      | Employs Snowflake Cross-Cloud Auto-Fulfillment (replicates metadata/data behind the scenes). |

---

## 3. Data Product Definition: Producer Domain (Azure Databricks)

The Marketing Domain manages customer lifetime value (`clv_features`) and exposes it as a certified data product via **Delta Sharing**.

### 1. Tagging & Data Contract Definition in Unity Catalog

```sql
-- Create and certify the domain-owned data product
CREATE SCHEMA IF NOT EXISTS marketing_domain.customer_analytics
  COMMENT 'Data Product: Certified Marketing Attribution and Feature Store';

CREATE TABLE marketing_domain.customer_analytics.clv_features (
    customer_id STRING COMMENT 'Unique global customer UUID',
    segment STRING COMMENT 'Churn tier: HIGH, MEDIUM, LOW',
    predicted_ltv_usd DECIMAL(12,2) COMMENT '365-day forward projected revenue',
    last_activity_date DATE COMMENT 'Audit watermark for incremental reads'
)
USING DELTA
TBLPROPERTIES (
    'delta.enableDeletionVectors' = 'true',
    'dataproduct.owner' = 'marketing-data-eng@enterprise.com',
    'dataproduct.tier' = 'gold-certified',
    'dataproduct.sla_freshness' = '06:00 UTC'
);

-- Tag sensitive columns for global governance
ALTER TABLE marketing_domain.customer_analytics.clv_features
ALTER COLUMN customer_id SET TAGS ('PII' = 'PSEUDONYMIZED');

```

### 2. Share Authorization via Delta Sharing

```sql
-- Create an open Delta Share exposed to downstream domains
CREATE SHARE marketing_clv_product_share;

ALTER SHARE marketing_clv_product_share ADD TABLE marketing_domain.customer_analytics.clv_features
  AS enterprise_public.marketing_clv;

-- Grant access to the consumer recipient (Finance Domain Snowflake instance)
CREATE RECIPIENT snowflake_finance_consumer
IDENTIFIER USING 's3://snowflake-recipient-keys/finance_token.json';

GRANT SELECT ON SHARE marketing_clv_product_share TO RECIPIENT snowflake_finance_consumer;

```

---

## 4. Consuming the Data Product: Consumer Domain (Snowflake on AWS)

The Finance Domain joins marketing attribution directly against daily ERP billing tables in Snowflake **zero-copy**.

### 1. Register Delta Share in Snowflake

```sql
-- Configure external Delta Sharing integration
CREATE OR REPLACE SECURITY INTEGRATION delta_sharing_marketing
  TYPE = EXTERNAL_OAUTH
  ENABLED = TRUE;

-- Snowflake queries the Delta Sharing REST server directly
CREATE OR REPLACE TABLE finance_dw.raw_external.clv_product_feed
  USING DELTA
  LOCATION = 'deltasharing://marketing_unity_catalog/marketing_clv_product_share/enterprise_public.marketing_clv';

```

### 2. Cross-Domain Federated Consumption (Star-Schema Join)

```sql
-- Finance Analyst joins internal ERP with external Marketing product
CREATE OR REPLACE SECURE VIEW finance_dw.marts.fct_daily_revenue_attribution AS
SELECT
    f.invoice_id,
    f.account_id,
    f.billed_amount_usd,
    m.segment AS marketing_segment,
    m.predicted_ltv_usd,
    f.billing_timestamp
FROM finance_dw.gold.fct_billed_invoices f
INNER JOIN finance_dw.raw_external.clv_product_feed m
    ON f.customer_uuid = m.customer_id;

```

---

## 5. Federated Orchestration & Data Contract Verification (MWAA)

In a Data Mesh, a central pipeline failure should not bring down domains. Instead, **MWAA** manages cross-domain dependencies using **Contract Testing & Sensor DAGs**, rather than monolithic giant DAGs.

```
[ Marketing Domain Airflow / Pipeline ]
  1. Ingest Raw Web Events
  2. Compute CLV Features
  3. Validate Data Contract (pytest / Great Expectations)
  4. Write Completion Manifest to S3: s3://mesh-contracts/marketing/clv/latest.json
                 │
                 ▼
[ Central MWAA Dependency Bus ]
  - Sensor: S3KeySensor waits for manifest
  - Evaluates schema version: contract_version == "2.1"
                 │
                 ▼
[ Finance Domain Pipeline Trigger ]
  1. Trigger Snowflake Warehouse
  2. Refresh External Delta Share References
  3. Materialize Daily Revenue Marts

```

### Cross-Domain Airflow Contract Sensor (`central_mesh_contract_bus.py`)

```python
from airflow import DAG
from airflow.sensors.s3 import S3KeySensor
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import json
import boto3

default_args = {
    'owner': 'central-governance',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
}

def verify_data_contract(**context):
    s3_client = boto3.client('s3')
    obj = s3_client.get_object(
        Bucket='enterprise-mesh-governance',
        Key='contracts/marketing_clv/latest_manifest.json'
    )
    manifest = json.loads(obj['Body'].read().decode('utf-8'))

    # Enforce Contract SLOs
    assert manifest['sla_met'] is True, "Producer violated freshness SLA!"
    assert manifest['row_count'] > 0, "Producer generated empty dataset!"
    assert manifest['schema_version'] == '2.0', "Contract version drift detected!"

with DAG(
    dag_id='mesh_marketing_to_finance_contract_coordinator',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval='0 6 * * *', # 06:00 AM UTC
    catchup=False
) as dag:

    # 1. Non-invasive sensor waiting for producer signal
    wait_for_producer_completion = S3KeySensor(
        task_id='wait_for_marketing_clv_manifest',
        bucket_name='enterprise-mesh-governance',
        bucket_key='contracts/marketing_clv/latest_manifest.json',
        poke_interval=120, # Check every 2 minutes
        timeout=3600      # 1-hour timeout window
    )

    # 2. Automated Contract & SLA Verification
    verify_contract = PythonOperator(
        task_id='verify_contract_assertions',
        python_callable=verify_data_contract,
        provide_context=True
    )

    # 3. Notify downstream consumer domain
    trigger_consumer_pipeline = SnowflakeOperator(
        task_id='trigger_finance_downstream_refresh',
        snowflake_conn_id='snowflake_finance_wh',
        sql="""
            ALTER TABLE finance_dw.raw_external.clv_product_feed REFRESH;
        """
    )

    wait_for_producer_completion >> verify_contract >> trigger_consumer_pipeline

```

---

## 6. Federated Governance, Access Control & Lineage

A decentralized mesh introduces data discoverability and security risks if governance is applied inconsistently.

```
┌─────────────────────────────────────────────────────────────┐
│ Federated Catalog (AWS Glue + Purview / Unity Catalog Link) │
│  - Automated crawler indexes certified Domain Schemas       │
│  - Cross-domain data lineage tracking                       │
└──────────────────────────────┬──────────────────────────────┘
                               │
            ┌──────────────────┴──────────────────┐
            ▼                                     ▼
┌───────────────────────────────┐ ┌───────────────────────────────────┐
│ Global Identity Scaffolding   │ │ ABAC (Attribute-Based Access)     │
│ - Azure Entra ID groups       │ │ - Reads PII metadata tag          │
│ - Federates to AWS IAM Roles  │ │ - Dynamically applies SHA-256     │
│   via OIDC / SAML 2.0         │ │   masking to consumer queries     │
└───────────────────────────────┘ └───────────────────────────────────┘

```

- **Attribute-Based Access Control (ABAC):** Rather than creating brittle role-to-table access lists (`GRANT SELECT ON ... TO ROLE_MARKETING`), central governance deploys global tag policies. Any column tagged with `PII=CONFIDENTIAL` across Databricks or Snowflake undergoes dynamic cryptographic hashing unless the user holds a `COMPLIANCE_OFFICER` Entra ID claim.
- **Decentralized Data Quality Responsibility:** The data producer domain is responsible for data quality. If data quality fails (tested via Great Expectations in the producer pipeline), the producer's manifest file is not published to S3, cleanly preventing bad data from entering the central mesh.

---

## 7. Failure Modes & Mitigations

| Failure Vector               | Production Impact                                                                                                | Mitigation Strategy                                                                                                                                                                                                                                             |
| ---------------------------- | ---------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Breaking Schema Drift**    | Producer alters or drops a column, breaking downstream consumer dashboards.                                      | **Semantic Versioning & Data Contracts:** Schemas must adhere to a contractual minor/major version (`v1.x`, `v2.x`). Breaking changes require creating a separate versioned endpoint/table (`clv_features_v2`) and deprecating `v1` over a 90-day grace period. |
| **Cross-Cloud Egress Shock** | A consumer executes un-pruned full table scans on a producer's Delta Share, driving up cross-cloud network fees. | Producers enforce mandatory partition pruning predicates on the Delta Share (e.g., `WHERE last_activity_date >= CURRENT_DATE - 30`). Egress cost monitoring tags track consumption volume per recipient ID.                                                     |
| **Domain SLA Breach**        | Marketing fails to produce their data product on time, locking downstream Finance pipelines.                     | **Circuit Breaking:** The MWAA S3 Key Sensor times out safely after 60 minutes, triggers an alert to the producer's on-call team, and allows the Finance pipeline to fall back on the previous day's snapshot without failing customer invoicing.               |
