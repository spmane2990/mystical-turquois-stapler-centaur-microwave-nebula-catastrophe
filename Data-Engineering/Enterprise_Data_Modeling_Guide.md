# Enterprise Data Modeling: Architecture, Methodologies & Implementation Guide

**Author:** Somnath Mane  
**Role:** Senior Data Engineer / Lead Data Architect  
**Domain Focus:** Enterprise Cloud Data Warehousing, Dimensional Modeling, Semantic Layers, and Lakehouse Architectures  
**Date:** September 2026  

---

## Executive Summary & Architectural Vision

Modern data systems require a disciplined data modeling foundation to bridge raw operational data stores with high-concurrency analytical engines and enterprise BI semantic layers. Whether provisioning data lakes on AWS (S3/Athena/Glue), high-performance analytical warehouses in Snowflake, or lakehouses on Azure Databricks (Delta Lake), sound data modeling ensures consistency, auditability, computational efficiency, and predictable business logic execution.

This document serves as a comprehensive technical reference covering foundational modeling paradigms, modern lakehouse design patterns, dimensional modeling strategies, schema evolution, physical warehouse optimization, semantic layer integration, and enterprise governance frameworks.

---

## 1. Conceptual, Logical, and Physical Data Modeling Framework

Enterprise data modeling progresses across three distinct abstraction tiers. Each tier serves a specific operational function and targets distinct stakeholder personas.

```
+-------------------------------------------------------------------------------+
|                           1. Conceptual Model                                 |
|      - High-level business entities (Customer, Product, Order, Claim)         |
|      - Business relationships (1:1, 1:N, M:N)                                 |
|      - Technology-agnostic, business-domain oriented                          |
+---------------------------------------+---------------------------------------+
                                        |
                                        v
+-------------------------------------------------------------------------------+
|                            2. Logical Model                                   |
|      - Normalized entities, complete enterprise attributes                    |
|      - Primary keys (PK), foreign keys (FK), business constraints             |
|      - Relationship cardinality, nullability, normalization (3NF/BCNF)        |
+---------------------------------------+---------------------------------------+
                                        |
                                        v
+-------------------------------------------------------------------------------+
|                            3. Physical Model                                  |
|      - Target engine specific (Snowflake, Databricks Delta, PostgreSQL, S3)   |
|      - Data types, physical storage format (Parquet, ORC), compression        |
|      - Clustering keys, partition keys, micro-partition pruning               |
|      - Surrogate keys vs Natural/Business keys, indexing strategies           |
+-------------------------------------------------------------------------------+
```

### Detailed Breakdown of Tiers

| Modeling Level | Purpose | Primary Audience | Key Deliverables & Artifacts | Primary Tech/Tools |
| :--- | :--- | :--- | :--- | :--- |
| **Conceptual** | Capture high-level scope, enterprise taxonomy, and domain boundaries. | Enterprise Architects, Business Analysts, Product Owners | Entity-Relationship Overview, Domain Context Diagrams, Business Glossary | Lucidchart, Miro, ERwin |
| **Logical** | Define normalized entities, business keys, validation constraints, and business associations. | Lead Data Engineers, Data Modelers, System Analysts | 3NF ERDs, Attribute Dictionary, Cardinality & Referential Integrity Rules | ERwin, ER/Studio, dbdocs, SqlDBM |
| **Physical** | Optimize data storage, query IOPS, micro-partitioning, and execution efficiency for specific engines. | Senior Data Engineers, DBA / Cloud Data Platform Leads | DDL scripts, Partitioning/Clustering specs, Liquibase/Flyway migrations | Snowflake SQL, Spark SQL, Terraform, Liquibase |

---

## 2. Core Data Modeling Methodologies & Trade-Off Analysis

Modern enterprise platforms rarely rely on a single modeling methodology across all layers. An architect must evaluate trade-offs based on ingestion frequency, query patterns, maintenance complexity, and storage efficiency.

```
                          Data Modeling Spectrum
                          
  Normalized / Agile                                              Denormalized / Speed
 [ 3NF / Inmon ] <-----> [ Data Vault 2.0 ] <-----> [ Kimball Star ] <-----> [ OBT / Lakehouse ]
  Enterprise Core         High-Audit Raw DW         BI Marts / OLAP           Ad-hoc / Feature Store
```

### Comparative Architectural Analysis

| Feature / Metric | Relational / 3NF (Inmon) | Dimensional / Star (Kimball) | Data Vault 2.0 (Linstedt) | One Big Table (OBT) / Lakehouse |
| :--- | :--- | :--- | :--- | :--- |
| **Primary Focus** | Single Version of Truth; complete operational normalization. | Query performance, analyst intuitiveness, BI semantic layers. | Agile enterprise data integration, full auditability, schema resilience. | Massive distributed scan speed, columnar read efficiency, ML features. |
| **Normalization Level** | 3rd Normal Form (3NF) or BCNF. | De-normalized dimensions around fact tables. | Hybrid: Normalized Hubs/Links, denormalized Satellites. | Fully de-normalized (nested/repeated structures allowed). |
| **Write / Load Performance** | High overhead due to cascade integrity checks and joins. | Fast parallel load into staging, surrogate key lookup overhead. | Extremely high: insert-only parallel loads, zero FK locking. | Very fast append/upsert via columnar files (Parquet). |
| **Read / Query Complexity** | High: requires multi-table relational joins. | Minimal: single-join star schema, star-join bitmap filters. | High: requires extensive point-in-time (PIT) and Bridge joins. | Ultra-low: zero joins, direct sequential columnar scans. |
| **Schema Evolution Cost** | High: schema updates require altering constraints and tables. | Medium: adding columns to dimensions/facts is straightforward. | Low: add new Satellites without disrupting existing pipeline flows. | Low: easily add metadata or optional columns via Parquet/Iceberg schema evolution. |
| **Best-Fit Domain** | Core enterprise operational data stores (ODS), transactional hubs. | Enterprise BI (Power BI, Tableau), multi-dimensional metrics, Marts. | Enterprise Core DWH integrating 10+ legacy/heterogeneous source systems. | Streaming clickstreams, large telemetry/IoT, Snowflake/Databricks feature stores. |

---

## 3. Dimensional Modeling Deep Dive (The Kimball Framework)

Dimensional modeling remains the gold standard for structuring consumable data marts and analytical semantic layers.

### 3.1 Anatomy of a Fact Table

Fact tables represent numerical measurements and business events occurring at a specific operational point in time.

*   **Fact Granularity (Grain):** The foundational definition of what an individual row in the fact table represents (e.g., *One row per individual prescription filled per pharmacy location per day*).
*   **Types of Measures:**
    *   **Additive:** Can be aggregated across all dimensions (e.g., `Sales_Amount`, `Quantity_Sold`).
    *   **Semi-Additive:** Can be aggregated across some dimensions but not time (e.g., `Account_Balance`, `Inventory_Levels`).
    *   **Non-Additive:** Cannot be meaningfully summed across any dimension; requires ratio or average recalculation (e.g., `Unit_Price`, `Profit_Margin_Pct`).

### 3.2 Fact Table Classification & Implementation Patterns

```
                                Fact Table Archetypes
                                
   Transaction Fact                Periodic Snapshot Fact          Accumulating Snapshot Fact
  +-------------------------+     +-------------------------+     +-------------------------+
  | Grain: 1 row per event  |     | Grain: 1 row per entity |     | Grain: 1 row per entity |
  | Time: Discrete point    |     |        per time period  |     |        lifecycle        |
  | Insert: Append-only     |     | Time: Periodic (M, W, D)|     | Update: Overwritten as  |
  | Example: Point of Sale, |     | Insert: Periodic append |     |         milestones hit  |
  | Claim Incurred          |     | Example: Monthly balance|     | Example: Order-to-Cash, |
  +-------------------------+     +-------------------------+     | Patient Journey         |
                                                                  +-------------------------+
```

#### Fact Table DDL Implementation (Snowflake Target)

```sql
-- Transaction Fact: Retail Order Lines
CREATE OR REPLACE TABLE DW_ANALYTICS.FACT_SALES_TRANSACTION (
    SALES_TRANSACTION_KEY   NUMBER(38,0) AUTOINCREMENT START 1 INCREMENT 1,
    DATE_KEY                NUMBER(8,0) NOT NULL,       -- FK to Dim_Date (YYYYMMDD)
    CUSTOMER_KEY            NUMBER(38,0) NOT NULL,      -- FK to Dim_Customer (Surrogate Key)
    PRODUCT_KEY             NUMBER(38,0) NOT NULL,      -- FK to Dim_Product
    STORE_KEY               NUMBER(38,0) NOT NULL,      -- FK to Dim_Store
    PROMOTION_KEY           NUMBER(38,0) NOT NULL,      -- FK to Dim_Promotion
    
    -- Degenerate Dimension
    ORDER_NUMBER            VARCHAR(64) NOT NULL,
    ORDER_LINE_NUMBER       NUMBER(5,0) NOT NULL,
    
    -- Numerical Metrics
    UNIT_PRICE              NUMBER(12,4) NOT NULL,
    ORDER_QUANTITY          NUMBER(10,2) NOT NULL,
    GROSS_REVENUE_AMT       NUMBER(18,2) NOT NULL,
    DISCOUNT_AMT            NUMBER(18,2) DEFAULT 0.00,
    NET_REVENUE_AMT         NUMBER(18,2) NOT NULL,
    TAX_AMT                 NUMBER(18,2) DEFAULT 0.00,
    EXTENDED_COST_AMT       NUMBER(18,2) NOT NULL,
    MARGIN_AMT              NUMBER(18,2) AS (NET_REVENUE_AMT - EXTENDED_COST_AMT),
    
    -- Ingestion Audit Metadata
    DW_LOAD_TIMESTAMP       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    DW_BATCH_ID             VARCHAR(64) NOT NULL,
    
    CONSTRAINT PK_FACT_SALES PRIMARY KEY (SALES_TRANSACTION_KEY)
)
CLUSTER BY (DATE_KEY, CUSTOMER_KEY);
```

### 3.3 Slowly Changing Dimensions (SCD) Implementation Mechanics

Handling dimension attribute mutability over time is critical to historical reporting integrity.

```
Type 0: Retain Original (Fixed: e.g., Date of Birth, Original System Start Date)
Type 1: Overwrite (No historical tracking: e.g., Fixing a phone number typo)
Type 2: Add New Row with Validity Ranges (Full historical fidelity)
Type 3: Add Previous Column (Tracks current and immediate previous state)
Type 4: Add Mini-Dimension / History Table (For high-frequency changes)
Type 6: Hybrid (1 + 2 + 3) -> Current flag + Historical records + Current value in all rows
```

#### SCD Type 2 Table Structure & Incremental Merge Pattern

```sql
-- Target Dimension Table
CREATE OR REPLACE TABLE DW_ANALYTICS.DIM_CUSTOMER (
    CUSTOMER_KEY        NUMBER(38,0) AUTOINCREMENT START 1 INCREMENT 1, -- Surrogate Key
    CUSTOMER_ID         VARCHAR(64) NOT NULL,                           -- Natural / Business Key
    FULL_NAME           VARCHAR(256) NOT NULL,
    EMAIL               VARCHAR(256),
    PHONE_NUMBER        VARCHAR(32),
    TIER_LEVEL          VARCHAR(32) NOT NULL,
    BILLING_STATE       VARCHAR(64) NOT NULL,
    POSTAL_CODE         VARCHAR(16),
    
    -- SCD Type 2 Tracking Metadata
    START_DATE          TIMESTAMP_NTZ NOT NULL,
    END_DATE            TIMESTAMP_NTZ DEFAULT TO_TIMESTAMP_NTZ('9999-12-31 23:59:59'),
    IS_CURRENT          BOOLEAN DEFAULT TRUE,
    RECORD_HASH         VARCHAR(64) NOT NULL, -- MD5 / SHA256 of tracked attributes
    
    CONSTRAINT PK_DIM_CUSTOMER PRIMARY KEY (CUSTOMER_KEY)
)
CLUSTER BY (CUSTOMER_ID, IS_CURRENT);

-- Production SCD Type 2 MERGE Script (Two-Pass Pattern)
-- Step 1: Identify changed records and insert expired versions
-- Step 2: Insert newly updated records as active (IS_CURRENT = TRUE)
MERGE INTO DW_ANALYTICS.DIM_CUSTOMER AS TGT
USING (
    -- Staged rows that have changed compared to current dimension records
    SELECT 
        SRC.CUSTOMER_ID,
        SRC.FULL_NAME,
        SRC.EMAIL,
        SRC.PHONE_NUMBER,
        SRC.TIER_LEVEL,
        SRC.BILLING_STATE,
        SRC.POSTAL_CODE,
        SRC.RECORD_HASH,
        CURRENT_TIMESTAMP() AS EFFECTIVE_START_DATE
    FROM STAGE.STG_CUSTOMER SRC
    JOIN DW_ANALYTICS.DIM_CUSTOMER CURR
      ON SRC.CUSTOMER_ID = CURR.CUSTOMER_ID
     AND CURR.IS_CURRENT = TRUE
    WHERE SRC.RECORD_HASH != CURR.RECORD_HASH
) AS CHANGED_SRC
ON TGT.CUSTOMER_ID = CHANGED_SRC.CUSTOMER_ID 
AND TGT.IS_CURRENT = TRUE

-- Expire existing current record
WHEN MATCHED THEN UPDATE SET
    TGT.END_DATE = CHANGED_SRC.EFFECTIVE_START_DATE,
    TGT.IS_CURRENT = FALSE;

-- Step 2: Insert the updated new versions AND newly created customers
INSERT INTO DW_ANALYTICS.DIM_CUSTOMER (
    CUSTOMER_ID, FULL_NAME, EMAIL, PHONE_NUMBER, TIER_LEVEL,
    BILLING_STATE, POSTAL_CODE, START_DATE, END_DATE, IS_CURRENT, RECORD_HASH
)
SELECT 
    SRC.CUSTOMER_ID, SRC.FULL_NAME, SRC.EMAIL, SRC.PHONE_NUMBER, SRC.TIER_LEVEL,
    SRC.BILLING_STATE, SRC.POSTAL_CODE,
    CURRENT_TIMESTAMP() AS START_DATE,
    TO_TIMESTAMP_NTZ('9999-12-31 23:59:59') AS END_DATE,
    TRUE AS IS_CURRENT,
    SRC.RECORD_HASH
FROM STAGE.STG_CUSTOMER SRC
LEFT JOIN DW_ANALYTICS.DIM_CUSTOMER CURR
  ON SRC.CUSTOMER_ID = CURR.CUSTOMER_ID
 AND CURR.IS_CURRENT = TRUE
WHERE CURR.CUSTOMER_ID IS NULL -- Brand new records
   OR CURR.RECORD_HASH != SRC.RECORD_HASH; -- Records modified in current run
```

---

## 4. Modern Lakehouse & Cloud Warehousing Architecture

Modern cloud data platforms synthesize data lake flexibility with data warehouse atomicity using ACID storage formats (Delta Lake, Apache Iceberg, Apache Hudi).

```
+-------------------------------------------------------------------------------+
|                            BRONZE LAYER (RAW)                                 |
| - Format: Delta / Parquet / Iceberg on S3 or ADLS Gen2                        |
| - Ingestion: EventBridge, Kafka, ADF, or SQS triggers into Auto Loader/Snowpipe|
| - Schema: Raw JSON payload, append-only, original metadata preserved           |
+---------------------------------------+---------------------------------------+
                                        |
                                        v
+-------------------------------------------------------------------------------+
|                           SILVER LAYER (CLEANSED)                             |
| - Format: ACID Parquet tables with enforced schemas                           |
| - Processing: Spark / Databricks / Snowflake Tasks & DBT                      |
| - Operations: De-duplication, SCD2 history, data cleansing, conformance       |
+---------------------------------------+---------------------------------------+
                                        |
                                        v
+-------------------------------------------------------------------------------+
|                            GOLD LAYER (CURATED)                               |
| - Format: Dimensional Star Schema, Aggregated Marts, or OBT                   |
| - Target: Power BI DirectQuery / Import, Tableau Extracts, ML Feature Store   |
| - Performance: Micro-partition clustering, Z-Ordering, Materialized Views    |
+-------------------------------------------------------------------------------+
```

### Modern Lakehouse Design Principles

1.  **ACID Transactional Guarantees:** Ensures concurrent writer isolation (optimistic concurrency control) without dirty reads.
2.  **Schema Enforcement and Schema Evolution:** Reject malformed records at Silver ingestion while permitting planned column additions:
    ```sql
    -- Spark SQL / Delta Lake Schema Evolution
    SET spark.databricks.delta.schema.autoMerge.enabled = true;
    ```
3.  **Storage / Compute Decoupling:** Data files reside on object storage (AWS S3, Azure ADLS Gen2); computational compute warehouses scale independently.
4.  **Time Travel and Zero-Copy Cloning:** Query historical snapshot states for audit reconciliation or regression rollbacks:
    ```sql
    -- Snowflake Zero-Copy Clone for Dev/QA Testing
    CREATE OR REPLACE TABLE DW_ANALYTICS.FACT_SALES_DEV 
    CLONE DW_ANALYTICS.FACT_SALES_TRANSACTION AT(OFFSET => -3600); -- 1 hour ago
    ```

---

## 5. Physical Modeling, Storage Optimization & Partitioning Strategies

Physical optimization dictates query SLA adherence, compute costs, and cloud credits consumption.

### 5.1 Partitioning vs. Clustering Mechanics

```
   Traditional Partitioning (Hive / File Directory)      Modern Micro-Partitioning (Snowflake / Delta)
  +-----------------------------------------------+     +-----------------------------------------------+
  | s3://bucket/fact_sales/year=2026/month=09/    |     | Natural 50-500MB immutable columnar files      |
  |  - Manual folder trees                        |     | Metadata min/max ranges per column per file   |
  |  - Risk of small-file problems                |     | Automated micro-partition pruning             |
  |  - Risk of data skew if cardinality is high   |     | Clustering keys applied when volume > 1 TB     |
  +-----------------------------------------------+     +-----------------------------------------------+
```

*   **Snowflake Micro-Partitioning:** Automatically segments data into columnar blocks (50–500MB uncompressed). Query pruning relies heavily on column min/max ranges. Use explicit `CLUSTER BY (Date_Key, Organization_Id)` only on multi-terabyte tables exhibiting scan skew.
*   **Delta Lake Liquid Clustering & Z-Ordering:** Replaces static Hive-style directory partitioning with multi-dimensional space-filling curve algorithms (Hilbert or Morton curves) to enable pruning across multiple independent query filters:
    ```sql
    -- Delta Lake Z-Ordering
    OPTIMIZE delta.`/mnt/gold/fact_sales`
    ZORDER BY (customer_key, date_key);
    ```

### 5.2 Optimizing Write Performance vs. Read Throughput

*   **File Sizing:** Target compaction to 128MB–1GB file sizes to prevent IOPS saturation from small-file proliferation.
*   **Column Projection:** In columnar architectures (Parquet, ORC, Snowflake FDN), prune queries at the select layer (`SELECT *` introduces significant unnecessary IO and memory bandwidth usage).
*   **Data Types Matter:** Enforce right-sized data types. Storing a 2-digit status code as `VARCHAR(16777216)` will not increase storage size in columnar formats with compression, but can degrade memory allocation during sort, windowing, and hash-join operations in execution memory.

---

## 6. Semantic Layer Integration (Power BI, Tableau, Universe)

The semantic layer abstracts complex underlying table joins, optimizes calculation pushdown, and enforces enterprise security.

```
                                 Semantic Layer Ecosystem
                                 
   Raw Analytical Tables                Semantic Abstraction                      BI Consumption
  +-----------------------+           +--------------------------+           +-----------------------+
  | Dim_Customer (SCD2)   | --------> | Power BI Semantic Model  | --------> | Executive Dashboards  |
  | Fact_Sales (Cluster)  |           | Tabular Engine / DAX     |           | Self-Service Explorer |
  | Dim_Date              |           | Relationships & Context  |           | Embedded Visuals      |
  +-----------------------+           +--------------------------+           +-----------------------+
```

### 6.1 Star Schema vs. Snowflake Schema in BI Engines

*   **Star Schema (Recommended):** Dimensions are denormalized. The BI tabular engine (Power BI VertiPaq) optimizes one-to-many single-hop relationships with maximum compression and memory locality.
*   **Snowflake Schema (Anti-Pattern in BI):** Normalizing sub-dimensions (e.g., `Product -> Subcategory -> Category`) forces multi-hop table navigation, degrading DAX/LOD execution speed and confusing business users in self-service reporting.

### 6.2 Advanced Semantic Layer Optimization

*   **DAX / Calculation Vectorization:** Leverage relationship filters instead of iterating row-by-row through `FILTER()`:
    ```dax
    // High-Performance DAX Measure Pattern
    Total_Net_Revenue_YTD = 
    CALCULATE(
        SUM(FACT_SALES_TRANSACTION[NET_REVENUE_AMT]),
        DATESYTD(DIM_DATE[CALENDAR_DATE]),
        DIM_CUSTOMER[TIER_LEVEL] IN {"Enterprise", "Premier"}
    )
    ```
*   **Composite Models & Aggregation Tables:** Combine DirectQuery for real-time transactional granularity with in-memory Import aggregation tables (e.g., pre-aggregated monthly summary facts) mapped seamlessly using Power BI Aggregations.
*   **Tableau LOD Expressions:** Anchor calculations across grains without altering visualization filters:
    ```
    // Tableau Fixed LOD: Cohort Base Value
    { FIXED [Customer_ID] : MIN([Order_Date]) }
    ```

---

## 7. Data Governance, Data Quality, and Testing Frameworks

An unmonitored data model deteriorates into a data swamp. Enterprise platforms require continuous validation frameworks.

```
                Continuous Data Model Quality Life Cycle
                
  +--------------------+      +--------------------+      +--------------------+
  | 1. Contract / Gate | ---> | 2. Pipeline Test   | ---> | 3. Production SLA  |
  | Great Expectations |      | DBT Test Assertions|      | Monte Carlo / Data |
  | OpenMetadata       |      | Reconciliation Run |      | Observability      |
  +--------------------+      +--------------------+      +--------------------+
```

### 7.1 Automated Data Quality Rules Matrix

| Check Domain | Test Category | Target Layer | SQL / DBT Assertion Example | Remediation / Action |
| :--- | :--- | :--- | :--- | :--- |
| **Referential Integrity** | Foreign Key Orphan Detection | Silver / Gold | `SELECT f.CUSTOMER_KEY FROM FACT_SALES f LEFT JOIN DIM_CUSTOMER d ON f.CUSTOMER_KEY = d.CUSTOMER_KEY WHERE d.CUSTOMER_KEY IS NULL;` | Route unassigned keys to surrogate `-1` (Unknown) and flag alerting DAG. |
| **Grain Uniqueness** | Primary Key Duplication | All Layers | `SELECT ORDER_NUMBER, ORDER_LINE_NUMBER, COUNT(*) FROM STG_SALES GROUP BY 1,2 HAVING COUNT(*) > 1;` | Fail pipeline run; route duplicate records to dead-letter storage. |
| **SCD2 Overlap** | Temporal Continuity | Silver / Dimensions | `SELECT CUSTOMER_ID FROM DIM_CUSTOMER WHERE IS_CURRENT = TRUE GROUP BY CUSTOMER_ID HAVING COUNT(*) > 1;` | Break build; invoke automated dimension reconciliation procedure. |
| **Value Domain Validity** | Range & Enum Check | Bronze / Silver | `SELECT COUNT(*) FROM FACT_SALES WHERE NET_REVENUE_AMT < 0 OR DISCOUNT_AMT < 0;` | Log warning; alert data steward if anomaly threshold exceeds 0.1%. |

### 7.2 Enterprise Security Modeling (RLS / CLS)

*   **Row-Level Security (RLS):** Filter visible rows based on identity and role token:
    ```sql
    -- Snowflake Row Access Policy Example
    CREATE OR REPLACE ROW ACCESS POLICY RAP_TERRITORY_FILTER
    AS (TERRITORY_CODE VARCHAR) RETURNS BOOLEAN ->
      CURRENT_ROLE() IN ('ENTERPRISE_ADMIN')
      OR EXISTS (
          SELECT 1 FROM SECURITY.USER_ROLE_MAPPING
          WHERE USER_NAME = CURRENT_USER()
            AND ASSIGNED_TERRITORY = TERRITORY_CODE
      );

    -- Apply to Fact Table
    ALTER TABLE DW_ANALYTICS.FACT_SALES_TRANSACTION 
    ADD ROW ACCESS POLICY RAP_TERRITORY_FILTER ON (STORE_KEY);
    ```
*   **Column-Level Security (CLS / Masking):** Protect PII/PHI (e.g., patient health identifiers, billing details) dynamically based on caller role without maintaining duplicated tables.

---

## 8. Real-World Case Studies Across Core Domains

### 8.1 Healthcare & Life Sciences: Longitudinal Patient Journey Model

```
                               Patient Journey Data Model
                               
   DIM_PATIENT (SCD Type 2)           FACT_CLINICAL_EVENT (Accumulating Snapshot)
  +--------------------------+       +---------------------------------------------+
  | PK: Patient_Key          |       | PK: Clinical_Event_Key                      |
  | NK: Enterprise_MPI_ID    | ----> | FK: Patient_Key                             |
  | Gender, Birth_Year       |       | FK: Provider_Key                            |
  | Chronic_Conditions_Hash  |       | FK: Diagnosis_Code_Key (ICD-10)             |
  | Start_Date, End_Date     |       | Initial_Consult_Date                        |
  | Is_Current               |       | Lab_Ordered_Date, Lab_Completed_Date        |
  +--------------------------+       | Prescription_Date, Dispense_Date            |
                                     | Therapy_Discontinuation_Date                |
                                     | Total_Days_On_Therapy                       |
                                     +---------------------------------------------+
```

*   **Business Challenge:** Track patient cohort progression through therapy stages, medication adherence, and clinical trial efficacy across 5+ source EHR and claims platforms.
*   **Architectural Strategy:** Implemented an **Accumulating Snapshot Fact Table** tied to an SCD2 Patient master record. Distinct milestone date fields capture the non-linear timeline from initial diagnosis to treatment outcome.
*   **Impact:** Reduced cohort analysis query execution times by 84% in Snowflake, eliminating expensive multi-table self-joins on historical claim tables.

### 8.2 Fast-Moving Consumer Goods (FMCG) / Retail: Net Revenue Management (NRM)

```
                            Retail NRM Analytical Engine
                            
       DIM_PRODUCT                     FACT_REVENUE_MANAGEMENT                DIM_PROMOTION
  +--------------------+              +-------------------------+          +--------------------+
  | PK: Product_Key    | <----------- | FK: Product_Key         | -------> | PK: Promotion_Key  |
  | SKU_Code, Brand    |              | FK: Promotion_Key       |          | Promo_Type, Mech   |
  | PPA_Pack_Format    |              | FK: Store_Key           |          | Discount_Depth     |
  | Baseline_Volume    |              | FK: Date_Key            |          +--------------------+
  +--------------------+              | Baseline_Sales_Units    |
                                      | Incremental_Promo_Units |
                                      | Trade_Investment_Cost   |
                                      | Net_Price_Realization   |
                                      +-------------------------+
```

*   **Business Challenge:** Quantify the price elasticity, trade promotion spend efficiency, and Price Pack Architecture (PPA) impacts across national supermarket chains.
*   **Architectural Strategy:** Deployed a **Periodic Daily Snapshot Fact Table** combined with an additive transaction stream. Fact entries break out baseline volume from incremental promotional lift, backed by an R-driven regression engine write-back pipeline.
*   **Semantic Layer Execution:** Delivered in Power BI utilizing DirectQuery over optimized Gold tables with VertiPaq imported aggregation tables for lightning-fast executive slice-and-dice.

---

## 9. Senior Data Engineer Checklist: Architecture & Review

Use this checklist during architecture design reviews, sprint planning, and pull request approvals:

* [ ] **Grain Explicitly Documented:** Is the grain defined in written plain language at the top of the table DDL?
* [ ] **Key Architecture Strategy:** Are surrogate integer/bigint keys implemented for dimensions instead of raw string business keys?
* [ ] **Null Handling in Foreign Keys:** Are null foreign keys converted to standard default surrogate keys (e.g., `-1 = 'Not Applicable'`, `-2 = 'Unknown'`) to prevent dropped rows in inner joins?
* [ ] **SCD Change Detection:** Is record hash calculation (MD5/SHA256) standardized across all tracked attributes to prevent false-positive SCD2 row proliferation?
* [ ] **Pruning & Clustering Aligned to Access Patterns:** Are micro-partitions / clustering keys aligned directly with high-frequency `WHERE` and `JOIN` filters?
* [ ] **Storage Decoupling & Compression Checked:** Are source data files converted to columnar Parquet/Iceberg formats with Snappy or ZSTD compression?
* [ ] **Semantic Relationship Cardinality:** Are relationships inside Power BI / Tableau strictly 1-to-Many single-directional where possible to prevent ambiguous path evaluation?
* [ ] **Data Quality Gate Automated:** Are uniqueness, nullability, and referential integrity tests automated within CI/CD or workflow DAG execution (Airflow/MWAA, DBT, or Great Expectations)?
