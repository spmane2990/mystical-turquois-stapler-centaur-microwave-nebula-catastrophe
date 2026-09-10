# Enterprise ETL/ELT Implementation Lifecycle: Architecture, Operations & Delivery Handbook

**Author:** Somnath Mane  
**Role:** Senior Data Engineer / Lead Data Architect  
**Scope:** Complete Enterprise Platform Lifecycle (Cloud Data Warehousing, Modern Lakehouse, Disaster Recovery, Archival, FinOps, Observability, and Governance)  
**Date:** September 2026  

---

## Table of Contents
1. [Lifecycle Overview & Complete Phase Progression](#1-lifecycle-overview--complete-phase-progression)
2. [Phase 1: Discovery, Source Assessment & Data Profiling](#2-phase-1-discovery-source-assessment--data-profiling)
3. [Phase 2: High-Level Design (HLD) Specification](#3-phase-2-high-level-design-hld-specification)
4. [Phase 3: Data Modeling, Keys & Schema Architecture](#4-phase-3-data-modeling-keys--schema-architecture)
5. [Phase 4: Low-Level Design (LLD) Specification](#5-phase-4-low-level-design-lld-specification)
6. [Phase 5: Source-to-Target Mapping (STTM) Standards](#6-phase-5-source-to-target-mapping-sttm-standards)
7. [Phase 6: Data Governance, Security & Lineage Framework](#7-phase-6-data-governance-security--lineage-framework)
8. [Phase 7: Development, Build & Database DevOps (CI/CD)](#8-phase-7-development-build--database-devops-cicd)
9. [Phase 8: Quality Assurance, Automated Reconciliation & Testing](#9-phase-8-quality-assurance-automated-reconciliation--testing)
10. [Phase 9: Disaster Recovery (DR) & Business Continuity Planning (BCP)](#10-phase-9-disaster-recovery-dr--business-continuity-planning-bcp)
11. [Phase 10: Information Lifecycle Management (ILM), Data Archiving & Purging](#11-phase-10-information-lifecycle-management-ilm-data-archiving--purging)
12. [Phase 11: Data Observability, SLA Monitoring & FinOps Cost Governance](#12-phase-11-data-observability-sla-monitoring--finops-cost-governance)
13. [Phase 12: Production Cutover, Historical Backfill & Hypercare](#13-phase-12-production-cutover-historical-backfill--hypercare)
14. [Enterprise Artifact Checklists, RACI & Runbook Templates](#14-enterprise-artifact-checklists-raci--runbook-templates)

---

## 1. Lifecycle Overview & Complete Phase Progression

Delivering a mission-critical cloud analytics ecosystem requires crossing traditional engineering boundaries into disaster recovery, automated lifecycle pruning, financial telemetry, and security governance.

```
+-------------------------------------------------------------------------------------------------------------------------+
|                                    END-TO-END ENTERPRISE DATA LIFECYCLE                                                 |
+-------------------------------------------------------------------------------------------------------------------------+
|                                                                                                                         |
|  [ 1. Discovery & Profiling ]                                                                                           |
|          |  Schema audits, volume analysis, frequency, API boundaries, CDC viability                                    |
|          v                                                                                                              |
|  [ 2. High-Level Design (HLD) ]                                                                                         |
|          |  Storage tiers, MPP engines (Snowflake/Databricks), event streaming, networking, VPC peering                |
|          v                                                                                                              |
|  [ 3. Data Modeling & Schema Design ]                                                                                   |
|          |  Conceptual -> Logical -> Physical, Kimball Star, Data Vault 2.0, erwin schemas, Hash Keys                   |
|          v                                                                                                              |
|  [ 4. Low-Level Design (LLD) ]                                                                                          |
|          |  DAG-level tasks, state management, exponential backoff, dead-letter queues (DLQ), cluster sizing            |
|          v                                                                                                              |
|  [ 5. Source-to-Target Mapping (STTM) ]                                                                                 |
|          |  Grain definition, transform rules, SCD2 hashdiffs, default surrogate handling                               |
|          v                                                                                                              |
|  [ 6. Data Governance, Security & Lineage ]                                                                            |
|          |  Cataloging, column-level lineage, ABAC/RBAC, dynamic masking (CLS), row access policies (RLS)              |
|          v                                                                                                              |
|  [ 7. Development, Build & CI/CD ]                                                                                      |
|          |  IaC (Terraform), PySpark/dbt code, Database-as-Code (Liquibase/Flyway), containerization                       |
|          v                                                                                                              |
|  [ 8. Testing & Dual-Run Reconciliation ]                                                                              |
|          |  Unit tests, volumetric stress tests, automated SQL reconciliation (row counts & financial checksums)       |
|          v                                                                                                              |
|  [ 9. Disaster Recovery (DR) & Business Continuity ]                                                                    |
|          |  RPO/RTO SLAs, cross-region failover, Snowflake Account Replication, Delta Lake replication, failback drills   |
|          v                                                                                                              |
|  [ 10. Data Archiving, Tiering & Purging (ILM) ]                                                                       |
|          |  S3 Glacier lifecycle rules, cold data partitioning, regulatory compliance (GDPR/HIPAA/SOX), shredding      |
|          v                                                                                                              |
|  [ 11. Data Observability & FinOps Governance ]                                                                        |
|          |  Pipeline freshness, anomaly detection, warehouse auto-suspend, credit quotas, tag-based cost allocation    |
|          v                                                                                                              |
|  [ 12. Cutover, Historical Backfill & Hypercare ]                                                                      |
|             Parallelized chunk loading, incremental CDC sync, BI repointing, 24/7 hypercare, runbook handoff            |
+-------------------------------------------------------------------------------------------------------------------------+
```

---

## 2. Phase 1: Discovery, Source Assessment & Data Profiling

Source inspection ensures source data anomalies are resolved before physical schemas are written.

### 2.1 Extraction Archetypes & Source Taxonomy
* **Log-Based CDC:** Captures uncommitted operational states, updates, and hard deletes with near-zero overhead on the operational source database (e.g., Debezium, AWS DMS, Qlik Replicate, Oracle GoldenGate).
* **High-Watermark Querying:** Queries source tables via `WHERE LAST_MODIFIED_TS >= :checkpoint_watermark`. Fails to capture hard deletes without soft-delete audit flags (`IS_DELETED = TRUE`).
* **REST / GraphQL APIs:** Webhook ingestion or scheduled pollers. Must implement token rotation, client-side pagination, rate-limit throttling (HTTP 429 backoff), and payload JSON unnesting.
* **Bulk File Feeds (SFTP/S3):** Event-triggered file drops (S3 EventBridge notifications to SQS/Lambda) or scheduled batch directory scans.

### 2.2 Enterprise Data Profiling Matrix

| Profiling Domain | What It Evaluates | Real-World Anomaly / Risk | Engineering Action / Remediation Pattern |
| :--- | :--- | :--- | :--- |
| **Volume & Burst Velocity** | File/table byte sizes, daily record growth, peak burst rate. | Month-end close spikes pipeline memory, triggering Out-of-Memory (OOM) failures. | Implement dynamic compute cluster autoscaling and bounded chunking. |
| **Completeness & Nullability** | Percent null per column; mandatory operational attributes. | Null values in foreign keys or mandatory business codes (`Customer_ID`, `Store_ID`). | Discard to dead-letter queue (DLQ) or route to surrogate key `-1 = 'Unknown'`. |
| **Uniqueness & Cardinality** | Unique row checks; potential composite primary keys. | Composite natural keys collide due to operational soft deletes or re-used IDs. | Use window functions (`ROW_NUMBER() OVER (PARTITION BY id ORDER BY update_ts DESC)`). |
| **Domain & Schema Drift** | Enum adherence, string lengths, nested schemas. | Column data types change dynamically without notice (e.g., String into Numeric). | Enable schema validation gates in Silver layers; configure schema evolution rules. |
| **Temporal Integrity** | Timestamp min/max distributions, future/past skew. | Timestamps set to epoch zero (`1970-01-01`) or invalid future dates (`2999-12-31`). | Validate against temporal bounds; replace corrupt dates with NULL or default dates. |
| **Referential Integrity** | Parent-child joins across independent operational tables. | Order line records referencing non-existent products in inventory. | Treat as early-arriving facts: assign placeholder surrogate key `-2 = 'Late Arriving'`. |

---

## 3. Phase 2: High-Level Design (HLD) Specification

The HLD establishes the architectural blueprint, security perimeter, network isolation, and technology stack.

### 3.1 Architectural Paradigms: Modern Lakehouse (ELT) vs. Legacy ETL

```
 Traditional In-Flight ETL (Legacy)
 [ Sources ] ===> [ Standalone ETL Appliance (PWC / DataStage) ] ===> [ Monolithic RDBMS (Teradata / Oracle) ]
  - Heavy transformation compute outside the target database
  - Costly proprietary appliance licensing
  - Storage tightly coupled with compute capacity

 Modern Decoupled Cloud ELT / Lakehouse (Cloud Native)
 [ Sources ] ===> [ Object Storage (S3 / ADLS) ] ===> [ Cloud MPP / Spark (Snowflake / Databricks) ] ===> [ Curated Marts ]
  - Raw immutable files retained for total auditability
  - Decoupled storage and compute: compute scales dynamically to zero when idle
  - Push-down processing: transformations execute inside distributed MPP query engines
```

### 3.2 Core Architectural Pillars in the HLD
1. **Network Topology & Private Isolation:**
   * Isolation within AWS VPC / Azure VNet environments.
   * No data egress to the public internet: connectivity via AWS PrivateLink, Azure Private Endpoints, and direct VPN peering.
2. **Medallion Storage Tiering (Object Store & Warehouse):**
   * **Bronze (Raw / Ingestion):** Immutable raw files (Parquet, JSON, Avro) preserving original payloads, append-only, retained under lifecycle management.
   * **Silver (Cleansed / Conformed):** De-duplicated, schema-validated, conformed tables storing historical versioning (SCD Type 2) in Delta Lake or Snowflake schemas.
   * **Gold (Curated / Business Marts):** Dimensional Star Schemas and Aggregated Marts optimized for executive BI (Power BI, Tableau) and feature stores.
3. **Compute Decoupling & Orchestration Architecture:**
   * Workflow orchestration via Amazon MWAA (Apache Airflow), Azure Data Factory, or Step Functions.
   * Dynamic compute provisioning: multi-cluster Snowflake virtual warehouses or serverless Spark pools sizing dynamically according to load.

---

## 4. Phase 3: Data Modeling, Keys & Schema Architecture

A robust data model abstracts complex physical source schemas into governed, performant structures.

### 4.1 Key Architecture Reference
* **Natural / Business Key:** The real-world operational identifier (e.g., `Patient_MRN`, `Account_UUID`). Volatile and subject to business changes; never use as analytical join keys.
* **Surrogate Key:** System-generated, non-business key (e.g., auto-incrementing integer or 64-character deterministic SHA-256 hash). Insulates analytical models from source drift and enables SCD2 tracking.
* **Degenerate Dimension Key:** Operational identifiers stored directly on the fact table without a dedicated dimension entity (e.g., `Order_Number`, `Invoice_ID`, `Transaction_Ref`).

### 4.2 Kimball Dimensional Modeling Patterns
* **Transaction Fact:** Discrete operational events recorded at atomic granularity (e.g., retail scan, credit card swipe).
* **Periodic Snapshot Fact:** Uniform captures of continuous statuses over predefined intervals (e.g., monthly bank balances, daily inventory snapshots).
* **Accumulating Snapshot Fact:** Tracks state transitions across a predictable multi-stage lifecycle (e.g., order fulfillment, patient healthcare claim journeys).
* **Slowly Changing Dimensions (SCD):**
  * **Type 1:** Overwrite (no historical tracking).
  * **Type 2:** Split history by inserting new rows with `START_DATE`, `END_DATE`, `IS_CURRENT`, and `RECORD_HASH`.
  * **Type 6:** Hybrid (SCD Type 1 + 2 + 3) where the current value is back-propagated across all historical slices.

### 4.3 Data Modeling Tooling: erwin Data Modeler
* **Logical-to-Physical Derivation:** Derive relational and dimensional physical schemas from a normalized 3NF enterprise logical model.
* **Forward / Reverse Engineering:** Reverse engineer legacy on-prem schemas via JDBC/ODBC; forward engineer cloud-optimized DDL (Snowflake/Delta Lake).
* **Complete Compare:** Automatically evaluate schema drift between Git DDL definitions and production database catalogs to generate zero-downtime alter scripts.

---

## 5. Phase 4: Low-Level Design (LLD) Specification

The Low-Level Design defines module-level DAG structures, runtime execution profiles, partition boundaries, and failure isolation.

### 5.1 DAG Execution & Task Dependencies
```
  [ S3 Event Trigger / Cron ]
             |
             v
  [ Task 1: Check Upstream Dependencies & Validate Manifest ]
             |
             v
  [ Task 2: Ingest Raw Payload to Bronze Landing Table ]
             |
             v
  [ Task 3: Schema Conformance & Silver Deduplication ]
             |
             +------------------------------+
             |                              | (On Failure)
             v                              v
  [ Task 4: Gold SCD2 Merge & Mart Refresh ] [ Route to DLQ & Send PagerDuty Alert ]
             |
             v
  [ Task 5: Automated Reconciliation Audit & Watermark Update ]
```

### 5.2 Idempotency & Fault Tolerance Patterns
* **Idempotent Ingestion:** Re-running a pipeline for the same time window must yield identical downstream states without generating duplicate rows. Accomplished via atomic `MERGE INTO`, partition-level overwrite (`INSERT OVERWRITE`), or transactional stage swaps.
* **Dynamic Chunking & Watermarking:** Persist pipeline run state in a governed table:
  ```sql
  CREATE TABLE CONTROL.ETL_PIPELINE_WATERMARK (
      PIPELINE_NAME      VARCHAR(128) NOT NULL,
      SOURCE_ENTITY      VARCHAR(128) NOT NULL,
      LAST_WATERMARK_VAL TIMESTAMP_NTZ NOT NULL,
      ROWS_PROCESSED     BIGINT NOT NULL,
      EXECUTION_STATUS   VARCHAR(32) NOT NULL,
      UPDATED_TIMESTAMP  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      PRIMARY KEY (PIPELINE_NAME, SOURCE_ENTITY)
  );
  ```

---

## 6. Phase 5: Source-to-Target Mapping (STTM) Standards

The STTM document bridges technical transformation logic with business functional requirements.

### 6.1 Production STTM Matrix

```
+-------------------------------------------------------------------------------------------------------------------------+
|                                  SOURCE-TO-TARGET MAPPING SPECIFICATION                                                 |
+-------------------------------------------------------------------------------------------------------------------------+
| Interface ID: INT_ORDER_LINE_001                     Target Entity: DW_ANALYTICS.FACT_SALES_TRANSACTION                 |
| Source System: SAP S/4HANA                           Load Pattern: Incremental Micro-Batch / Merge                      |
| Grain: One row per sales order line item             SCD Type: N/A (Transaction Fact)                                   |
+-------------------------------------------------------------------------------------------------------------------------+
```

| Target Column | Target Data Type | Null? | Key Role | Source Table | Source Column | Transformation Logic & Derivation Rules | Default Value |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `SALES_KEY` | `NUMBER(38,0)` | No | PK | Staging | N/A | `AUTOINCREMENT START 1 INCREMENT 1` | Generated |
| `DATE_KEY` | `NUMBER(8,0)` | No | FK | `VBAK` | `ERDAT` | Convert `'YYYYMMDD'` string to Integer key. Lookup `DIM_DATE`. | `-1` |
| `CUSTOMER_KEY` | `NUMBER(38,0)` | No | FK | `VBAK` | `KUNNR` | Lookup `DIM_CUSTOMER.CUSTOMER_KEY` WHERE `SRC.KUNNR = CUSTOMER_ID` AND `IS_CURRENT = TRUE`. | `-1` (Unknown) |
| `PRODUCT_KEY` | `NUMBER(38,0)` | No | FK | `VBAP` | `MATNR` | Lookup `DIM_PRODUCT.PRODUCT_KEY` WHERE `SKU_CODE = LTRIM(SRC.MATNR, '0')`. | `-1` (Unknown) |
| `ORDER_ID` | `VARCHAR(64)` | No | Degenerate | `VBAK` | `VBELN` | Trim whitespace. | `'NA'` |
| `LINE_NUMBER` | `NUMBER(5,0)` | No | Degenerate | `VBAP` | `POSNR` | Cast to integer. | `1` |
| `NET_AMT` | `NUMBER(18,2)` | No | Measure | `VBAP` | `NETWR` | Cast to Decimal(18,2). Converted to Base USD using `FX_RATES`. | `0.00` |
| `STATUS_FLAG` | `VARCHAR(16)` | No | Attribute | `VBAK` | `GBSTK` | Standardize codes: `'O' -> 'Open'`, `'C' -> 'Closed'`, `'X' -> 'Cancelled'`. | `'Unknown'` |
| `DW_INSERT_TS`| `TIMESTAMP_NTZ`| No | Metadata | System | N/A | `CURRENT_TIMESTAMP()` | System Time |
| `DW_BATCH_ID` | `VARCHAR(64)` | No | Metadata | Airflow | N/A | Ingestion DAG execution run ID. | `'MANUAL'` |

---

## 7. Phase 6: Data Governance, Security & Lineage Framework

Data governance ensures compliance with data protection laws (GDPR, HIPAA, CCPA) and establishes analytical trust.

```
                     Data Governance Execution Layers
                     
  [ Data Catalog & Glossary ] ---> [ Automated Lineage ] ---> [ PII/PHI Discovery ]
               |                             |                          |
               v                             v                          v
  [ Role-Based Access (RBAC) ] --> [ Dynamic Masking (CLS) ] -> [ Row Security (RLS) ]
```

### 7.1 Security Policy Implementation (Snowflake Dynamic Masking & RLS)

```sql
-- Dynamic Column-Level Security (CLS) Masking Policy
CREATE OR REPLACE MASKING POLICY SEC_POLICIES.MASK_EMAIL AS (VAL STRING) RETURNS STRING ->
  CASE 
    WHEN CURRENT_ROLE() IN ('SECURITY_OFFICER', 'COMPLIANCE_LEAD') THEN VAL
    WHEN CURRENT_ROLE() IN ('ANALYST_ROLE') THEN REGEXP_REPLACE(VAL, '(?i)^(.{2})(.*)(@.*)$', '\1****\3')
    ELSE '***RESTRICTED_PII***'
  END;

ALTER TABLE DW_ANALYTICS.DIM_CUSTOMER MODIFY COLUMN EMAIL SET MASKING POLICY SEC_POLICIES.MASK_EMAIL;

-- Dynamic Row-Level Security (RLS) Access Policy
CREATE OR REPLACE ROW ACCESS POLICY SEC_POLICIES.ROW_RESTRICT_TERRITORY
AS (STORE_KEY NUMBER) RETURNS BOOLEAN ->
  CURRENT_ROLE() IN ('EXECUTIVE_ADMIN')
  OR EXISTS (
      SELECT 1 FROM SECURITY.USER_TERRITORY_MAPPING M
      JOIN DW_ANALYTICS.DIM_STORE S ON S.TERRITORY_CODE = M.TERRITORY_CODE
      WHERE M.USER_NAME = CURRENT_USER() AND S.STORE_KEY = STORE_KEY
  );

ALTER TABLE DW_ANALYTICS.FACT_SALES_TRANSACTION ADD ROW ACCESS POLICY SEC_POLICIES.ROW_RESTRICT_TERRITORY ON (STORE_KEY);
```

### 7.2 End-to-End Column Lineage & Data Contracts
* **Data Contracts:** JSON-schema agreements defined between software engineering microservices and data platform pipelines. Prevents upstream engineers from changing data types or dropping attributes without breaking CI/CD tests.
* **Automated Lineage Harvesting:** Extract lineage via OpenLineage, dbt docs, or Snowflake Access History metadata to trace metrics from consumption dashboards back to raw source tables.

---

## 8. Phase 7: Development, Build & Database DevOps (CI/CD)

Modern data engineering treats pipelines, database objects, and cloud infrastructure as code.

```
 Developer Commit ---> GitHub Actions / Jenkins ---> Terraform Plan/Apply ---> Liquibase Migration ---> dbt Run / Test
```

### 8.1 Database-as-Code Best Practices
* **Zero Manual DDL:** All schema mutations (tables, views, stored procedures, clustering keys) are committed as versioned Liquibase or Flyway changelogs.
* **Automated Linting & Unit Testing:** GitHub Actions triggers `SQLFluff` linting, `black` Python formatting, and `pytest` mock data assertions on every pull request.
* **Zero-Copy Clone Staging Testing:** Snowflake zero-copy clones create ephemeral production environments for pull request validation, terminating them post-merge to save cost.

---

## 9. Phase 8: Quality Assurance, Automated Reconciliation & Testing

Verification must validate mathematical correctness and eliminate silent pipeline drift.

```sql
-- Production Automated Checksum & Count Reconciliation Script
WITH SRC_AGG AS (
    SELECT 
        POSTING_DATE::DATE AS TX_DATE,
        COUNT(*) AS SRC_COUNT,
        ROUND(SUM(AMOUNT), 2) AS SRC_AMOUNT
    FROM STAGE.STG_FINANCIAL_TRANSACTIONS
    GROUP BY 1
),
TGT_AGG AS (
    SELECT 
        D.CALENDAR_DATE AS TX_DATE,
        COUNT(*) AS TGT_COUNT,
        ROUND(SUM(F.NET_AMT), 2) AS TGT_AMOUNT
    FROM DW_ANALYTICS.FACT_SALES_TRANSACTION F
    JOIN DW_ANALYTICS.DIM_DATE D ON F.DATE_KEY = D.DATE_KEY
    GROUP BY 1
)
SELECT 
    COALESCE(S.TX_DATE, T.TX_DATE) AS RECON_DATE,
    S.SRC_COUNT, T.TGT_COUNT,
    (T.TGT_COUNT - S.SRC_COUNT) AS COUNT_DELTA,
    S.SRC_AMOUNT, T.TGT_AMOUNT,
    (T.TGT_AMOUNT - S.SRC_AMOUNT) AS AMOUNT_DELTA,
    CASE 
        WHEN S.SRC_COUNT = T.TGT_COUNT AND ABS(S.SRC_AMOUNT - T.TGT_AMOUNT) < 0.01 THEN 'BALANCED'
        ELSE 'RECONCILIATION_ALERT'
    END AS RECON_STATUS
FROM SRC_AGG S
FULL OUTER JOIN TGT_AGG T ON S.TX_DATE = T.TX_DATE
WHERE RECON_STATUS = 'RECONCILIATION_ALERT';
```

---

## 10. Phase 9: Disaster Recovery (DR) & Business Continuity Planning (BCP)

Disaster Recovery defines how the enterprise platform withstands catastrophic cloud outages, regional disasters, or ransomware events.

```
                      Disaster Recovery Deployment Models
                      
  Active-Passive (Cold Standby)           Active-Passive (Warm Standby)          Active-Active (Dual Multi-Region)
 +-----------------------------+         +-----------------------------+        +--------------------------------+
 | Primary: Full Production    |         | Primary: Full Production    |        | Region A: Running Ingestion    |
 | Secondary: Infrastructure   |         | Secondary: Replicated Data  |        | Region B: Running Ingestion    |
 | as Code ready (Terraform)   |         | Compute scales on failover  |        | Continuous Cross-Sync          |
 | RTO: 4 - 8 Hours            |         | RTO: 30 - 60 Minutes        |        | RTO: < 5 Minutes               |
 | RPO: 4 - 12 Hours           |         | RPO: < 15 Minutes           |        | RPO: Near Zero                 |
 +-----------------------------+         +-----------------------------+        +--------------------------------+
```

### 10.1 Key Metrics: RPO vs. RTO
* **Recovery Point Objective (RPO):** The maximum acceptable age of data loss following an incident (e.g., *RPO = 15 minutes* means the business can tolerate losing the most recent 15 minutes of incremental data).
* **Recovery Time Objective (RTO):** The maximum allowable duration between system failure and full operational recovery (e.g., *RTO = 1 hour* means systems must be live and processing queries within 60 minutes).

### 10.2 Technical Implementation Across Cloud Platforms
1. **Cross-Region Storage Replication:**
   * **AWS S3 Cross-Region Replication (CRR):** Automatically replicates raw Bronze and Silver Parquet files from `us-east-1` (Primary) to `us-west-2` (DR) with KMS key alias translation.
   * **Delta Lake Deep Cloning:** Executes scheduled cross-region deep clones:
     ```sql
     CREATE OR REPLACE TABLE delta.`s3://dr-backup-bucket/delta/fact_sales`
     DEEP CLONE delta.`s3://prod-primary-bucket/delta/fact_sales`;
     ```
2. **Cloud Warehouse Database Replication (Snowflake Failover Groups):**
   * Link accounts across regions/clouds to synchronize database schemas, tables, views, role grants, and users:
     ```sql
     -- Primary Account: Create Failover Group
     CREATE FAILOVER GROUP ENTERPRISE_FAILOVER_GROUP
       OBJECT_TYPES = USERS, ROLES, DATABASES
       ALLOWED_DATABASES = DW_ANALYTICS, DW_GOVERNANCE
       ALLOWED_ACCOUNTS = myorg.dr_account_west;

     -- Secondary DR Account: Replicate and Refresh
     ALTER FAILOVER GROUP ENTERPRISE_FAILOVER_GROUP REFRESH;

     -- Failover Event (Promote DR Account to Primary)
     ALTER FAILOVER GROUP ENTERPRISE_FAILOVER_GROUP PRIMARY;
     ```
3. **Orchestrator & Ingestion Failover:**
   * Infrastructure as Code (Terraform) maintains active-passive configurations of MWAA Airflow DAGs in the DR region. DAGs remain paused in DR until failover is declared.
4. **DR Drills & Automated Simulation:**
   * Execute scheduled, non-disruptive DR simulations quarterly: temporarily redirect BI DNS routing (Route 53 / Azure Traffic Manager) to the DR warehouse to validate dashboard stability.

---

## 11. Phase 10: Information Lifecycle Management (ILM), Data Archiving & Purging

Unconstrained data growth degrades query execution performance, inflates cloud storage costs, and increases legal and regulatory discovery liabilities.

```
                    Data Lifecycle & Storage Tiering
                    
   Active Tier (0 - 90 Days)            Warm Tier (91 - 365 Days)           Cold Archive (1 - 7+ Years)
  +---------------------------+        +--------------------------+        +---------------------------+
  | - Cloud MPP (Snowflake)   | -----> | - Low-Cost Object Storage| -----> | - Deep Archive (Glacier)  |
  | - High-Performance NVMe   |        |   (S3 Standard-IA / ADLS)|        | - Parquet / Compressed ZSTD|
  | - Full Interactive Query  |        | - External Tables/Athena |        | - Compliance & Legal Hold |
  +---------------------------+        +--------------------------+        +---------------------------+
                                                                                         |
                                                                                         v
                                                                            [ Automated Data Shredding ]
                                                                            [ GDPR Right-to-be-Forgotten ]
```

### 11.1 Tiering Policies & Engine Offloading
1. **Object Storage Lifecycle Configuration (AWS S3):**
   * Days 0–90: S3 Standard (frequent analytical reads).
   * Days 91–365: S3 Standard-Infrequent Access (S3 Standard-IA).
   * Days 366–2555 (Year 1 to 7): S3 Glacier Flexible Archive.
   * Beyond Year 7: S3 Glacier Deep Archive or automatic expiration.
2. **Partition Eviction in Cloud Warehouses:**
   * Use an automated stored procedure to export partitions older than 18 months to external S3 stages in Parquet format, unlinking them from active compute warehouses:
     ```sql
     -- Export and Prune Old Fact Partitions
     COPY INTO @DW_STAGE.COLD_ARCHIVE/fact_sales/year=2023/
     FROM (
         SELECT * FROM DW_ANALYTICS.FACT_SALES_TRANSACTION 
         WHERE DATE_KEY < 20240101
     )
     FILE_FORMAT = (TYPE = 'PARQUET', COMPRESSION = 'SNAPPY');

     DELETE FROM DW_ANALYTICS.FACT_SALES_TRANSACTION WHERE DATE_KEY < 20240101;
     ```
3. **Querying Historical Archives via External Tables:**
   * Query offloaded data seamlessly using Snowflake External Tables or AWS Athena over Iceberg metadata without loading files into internal warehouse storage.

### 11.2 Regulatory Compliance & Data Purging (GDPR / CCPA / HIPAA)
* **Right to Be Forgotten (GDPR Article 17):**
  * In append-only lakehouses, deleting a specific customer record requires rewriting parquet files.
  * *Solution*: Maintain a centralized "Forgotten Customers" lookup table. Apply dynamic masking or row-level views in Gold layers that filter out these IDs instantaneously. Batch physical file rewrites into scheduled weekend maintenance compaction tasks (`VACUUM` / rewrite).
* **Cryptographic Erasure (Crypto-Shredding):**
  * Store sensitive PII attributes encrypted with unique per-customer encryption keys managed in AWS KMS. When a customer exercises their right to erasure, destroy their distinct KMS key, instantly rendering all historical ciphertext permanently unreadable across all cold backups and active tables.

---

## 12. Phase 11: Data Observability, SLA Monitoring & FinOps Cost Governance

Enterprise systems must maintain predictable operational costs and alert engineers to silent data failures.

### 12.1 The 5 Pillars of Data Observability

```
                                  5 Pillars of Data Observability
  +------------------+------------------+------------------+------------------+------------------+
  |    Freshness     |      Volume      |     Quality      |      Schema      |     Lineage      |
  | Pipeline runtime | Unexpected drops | Value domain,    | Unexpected field | Root-cause tracing|
  | & SLA compliance | or sudden spikes | null rate anomalies| drops / alterations| to downstream BI |
  +------------------+------------------+------------------+------------------+------------------+
```

* **Freshness & SLA Monitoring:** Track pipeline execution duration against defined business delivery windows (e.g., if `MAX(DW_LOAD_TIMESTAMP)` is older than 6 hours, page the on-call engineer).
* **Volume Anomaly Detection:** Flag batch inputs deviating by more than 3 standard deviations from a rolling 30-day mean.
* **Observability Tooling:** Integrate platforms like Monte Carlo, Acceldata, or open-source automated dbt test suites with Slack and PagerDuty.

### 12.2 FinOps: Cloud Data Platform Cost Optimization
1. **Compute Warehouse Auto-Suspend & Auto-Resume:**
   * Enforce aggressive auto-suspend timers (e.g., 60 seconds) on analytical and ETL virtual warehouses to prevent idle compute burn.
2. **Resource Monitors & Hard Quotas:**
   * Configure Snowflake Resource Monitors to suspend warehouses automatically if budget allocations exceed 100% of the allocated monthly credit quota.
3. **Cluster Auto-Scaling Boundaries:**
   * Enforce strict maximum bounds on multi-cluster warehouses (e.g., `MIN_CLUSTERS = 1, MAX_CLUSTERS = 3`) to eliminate run-away query concurrency billing.
4. **Cost Allocation Tagging:**
   * Enforce mandatory tags on all cloud resources (`Environment`, `Cost_Center`, `Project`, `Owner`) across Terraform, Snowflake objects, and Airflow DAGs to enable chargeback accounting across business units.

---

## 13. Phase 12: Production Cutover, Historical Backfill & Hypercare

Transitioning mission-critical platforms to production requires structured execution to prevent system downtime.

```
                   Production Cutover Timeline
                   
  [ T - 30 Days ] Validate Production Infrastructure (IaC) & Connectivity
  [ T - 14 Days ] Begin Historical Backfill (Cold Historical Batches)
  [ T - 48 Hours] Delta CDC Ingestion Sync (Catch-up to Real-Time)
  [ T - 24 Hours] Dual-Run Execution (Old Platform vs. New Platform Side-by-Side)
  [ Day 0: Go-Live] Switch Downstream BI DNS & Repoint Semantic Models
  [ T + 14 Days ] Hypercare: Daily Standups, SLA Verification & Operations Handover
```

### 13.1 Historical Backfill Mechanics
* Break massive historical loads (e.g., 5–10 years of operational history) into bounded, discrete micro-batches (e.g., monthly chunks).
* Temporarily scale compute warehouses up (e.g., scale from Medium to 2X-Large in Snowflake) to execute parallel chunk loading, scaling down immediately afterward.
* Temporarily drop or disable non-essential foreign keys, search optimization services, and automated clustering keys during the backfill to prevent write overhead, re-enabling them prior to cutover.

### 13.2 Hypercare Protocol
* Establish a 24/7 hypercare engineering rotation for 2–4 weeks post-go-live.
* Daily morning reconciliation review: execute cross-system row count, null-check, and financial balance audits prior to business hours.
* Track MTTR (Mean Time to Resolution) and log all production anomalies into a formal handover register.

---

## 14. Enterprise Artifact Checklists, RACI & Runbook Templates

### 14.1 Comprehensive RACI Delivery Matrix

| Implementation Phase & Artifact | Responsible (R) | Accountable (A) | Consulted (C) | Informed (I) |
| :--- | :--- | :--- | :--- | :--- |
| **Source Assessment & Profiling Report** | Senior Data Engineer | Lead Data Architect | Source System DBA | Analytics Leads |
| **High-Level Design (HLD)** | Lead Data Architect | Principal Enterprise Architect | CISO, Security Lead, Network Team | Project Leadership |
| **Data Models (Conceptual / Logical / Physical)**| Data Modeler / Architect | Lead Data Architect | BI Lead, Data Stewards | Analytics Team |
| **Low-Level Design (LLD)** | Senior Data Engineer | Lead Data Architect | Platform Engineers | Operations / L2 Support |
| **Source-to-Target Mapping (STTM)** | Senior Data Engineer | Lead Data Architect | Business Analysts, Source SMEs | QA Lead |
| **Data Governance & PII Masking Plan** | Data Steward / Architect | Chief Data Officer / CISO | Legal, Compliance | Enterprise Architects |
| **Disaster Recovery (DR) & Failover Plan** | Cloud Platform Architect | Lead Data Architect | SRE, Infrastructure Lead | Executive Sponsors |
| **Data Archiving & Purging (ILM) Policy** | Lead Data Architect | Data Governance Board | Legal, Compliance Lead | Storage Admins |
| **FinOps Cost Allocation Framework** | FinOps Lead / Architect | Engineering Director | Cloud Ops, Project Managers | Finance Team |
| **UAT Sign-off & Reconciliation Logs** | QA Lead / BI Lead | Business Product Owner | Senior Data Engineer | Executive Steering |
| **Production Operational Runbook** | DataOps Lead | Senior Data Engineer | Support Operations, SRE | Engineering Leadership |

### 14.2 Complete Production Operational Runbook Template

```markdown
# Operational Runbook: [PIPELINE_OR_DAG_NAME]

## 1. System Metadata & SLA Profile
- **Identifier:** DAG_FINANCIAL_SETTLEMENT_DAILY
- **Primary Target Tables:** DW_ANALYTICS.FACT_GL_BALANCES, DW_ANALYTICS.DIM_COST_CENTER
- **Criticality Tier:** Tier 1 (Critical Enterprise Financial Pipeline)
- **Production Delivery SLA:** Daily by 06:00 AM UTC
- **Notification Routing:** PagerDuty `Data-Platform-OnCall`, Slack `#data-ops-sev1`

## 2. Ingestion & Upstream Topology
- **Upstream Source Endpoint:** SAP S/4HANA via AWS DMS CDC to `s3://enterprise-prod-landing/sap_gl/`
- **Dependency Condition:** File flag `_READY_FOR_SETTLEMENT.txt` must arrive by 03:00 AM UTC
- **Execution Engine:** AWS MWAA Airflow 2.x triggering Snowflake Virtual Warehouse `WH_ETL_FINANCE_L`

## 3. Disaster Recovery & Failure Triage Workflows

### Incident 1: Upstream Ingestion Missing or Delayed
- **Symptom:** Upstream sensor times out waiting for SAP completion flag.
- **Diagnostic:** Check SAP RFC connection status and inspect AWS DMS replication task metrics in CloudWatch.
- **Remediation:** If SAP delayed, notify Financial Ops channel and trigger incident ticket to SAP Basis Team.

### Incident 2: High Reconciliation Delta Breach (> 0.05% Variance)
- **Symptom:** Task `reconciliation_audit_step` fails pipeline execution.
- **Diagnostic:** Run query: `SELECT * FROM DW_AUDIT.RECONCILIATION_LOG WHERE AUDIT_STATUS = 'FAILED' ORDER BY CHECK_TS DESC;`
- **Remediation:** 
  1. Verify if late-arriving GL transactions were posted out of order.
  2. If missing records, run recovery script: `CALL SP_RESYNC_GL_SETTLEMENT('YYYY-MM-DD');`.
  3. Do not release downstream BI semantic model until financial controller approves delta.

### Incident 3: Primary Region Outage / Failover Invocation
- **Symptom:** AWS `us-east-1` outage disrupts primary database ingestion and access.
- **Failover Action:** 
  1. Incident Commander declares regional failover.
  2. Execute Snowflake Failover promote command on secondary account:
     `ALTER FAILOVER GROUP ENTERPRISE_FAILOVER_GROUP PRIMARY;`
  3. Activate Route 53 failover DNS routing to point Power BI gateways to secondary regional endpoint.
  4. Enable and resume passive MWAA Airflow DAGs in DR region (`us-west-2`).
```
