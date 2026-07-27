# Tasty Bytes Advanced Snowflake Engineering & Data Science Playbook

This repository contains a comprehensive guide and production-grade implementation scripts for building, managing, and optimizing an end-to-end data platform using Snowflake. Based on the **Tasty Bytes** global food truck network dataset, this document showcases advanced architectural patterns including zero-copy cloning, secure role-based access control (RBAC), semi-structured ingestion pipelines, Snowpark Python dataframes, Snowflake Cortex LLMs, and Snowpark Machine Learning (XGBoost).

---

## Table of Contents
1. [Database Initialization & Warehouse Architecture](#1-database-initialization--warehouse-architecture)
2. [Role-Based Access Control (RBAC) & Security](#2-role-based-access-control-rbac--security)
3. [Stages, External Tables & Continuous Ingestion (Snowpipe)](#3-stages-external-tables--continuous-ingestion-snowpipe)
4. [Semi-Structured Data Manipulation & Advanced Views](#4-semi-structured-data-manipulation--advanced-views)
5. [Time Travel & Zero-Copy Cloning](#5-time-travel--zero-copy-cloning)
6. [Programmability: UDFs & Stored Procedures (SQL & Python)](#6-programmability-udfs--stored-procedures-sql--python)
7. [Continuous Data Pipelines: Streams & Tasks (DAGs)](#7-continuous-data-pipelines-streams--tasks-dags)
8. [Snowpark Python API: DataFrames & Snowflake CLI](#8-snowpark-python-api-dataframes--snowflake-cli)
9. [Enterprise AI: Querying LLMs with Snowflake Cortex](#9-enterprise-ai-querying-llms-with-snowflake-cortex)
10. [Snowpark ML: Training & Deploying an XGBoost Model](#10-snowpark-ml-training--deploying-an-xgboost-model)
11. [Operational Dashboards: Streamlit in Snowflake](#11-operational-dashboards-streamlit-in-snowflake)

---

## 1. Database Initialization & Warehouse Architecture

Efficient resource optimization requires separating compute clusters dynamically based on operational workloads. Below, we initialize our environment and define standard and Python-optimized warehouses.

```sql
-- Switch to high-privilege role for initialization
USE ROLE SYSADMIN;

-- Create Database and Schemas
CREATE DATABASE IF NOT EXISTS tastybytes;
USE DATABASE tastybytes;
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS analytics;

-- Create SQL Analytics Warehouse with Auto-suspend and Multi-cluster scaling
CREATE OR REPLACE WAREHOUSE tasty_bi_wh WITH
    WAREHOUSE_SIZE = 'MEDIUM'
    WAREHOUSE_TYPE = 'STANDARD'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3
    SCALING_POLICY = 'ECONOMY'
    COMMENT = 'Warehouse for general BI queries and transformations';

-- Create Snowpark/Python-Optimized Warehouse for heavy ML memory workloads
CREATE OR REPLACE WAREHOUSE tasty_ml_wh WITH
    WAREHOUSE_SIZE = 'LARGE'
    WAREHOUSE_TYPE = 'SNOWPARK-OPTIMIZED'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Memory-intensive warehouse for Snowpark ML model training';
```

---

## 2. Role-Based Access Control (RBAC) & Security

To enforce the principle of least privilege, we build a custom role hierarchy where the functional developer role inherits lower-level data access privileges.

```sql
USE ROLE SECURITYADMIN;

-- Create Custom Roles
CREATE ROLE IF NOT EXISTS tasty_security_admin_role;
CREATE ROLE IF NOT EXISTS tasty_data_engineer_role;
CREATE ROLE IF NOT EXISTS tasty_data_scientist_role;

-- Establish Hierarchy: System Administrator inherits custom roles
GRANT ROLE tasty_data_engineer_role TO ROLE SYSADMIN;
GRANT ROLE tasty_data_scientist_role TO ROLE SYSADMIN;

-- Grant Object Privileges to Data Engineer
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON DATABASE tastybytes TO ROLE tasty_data_engineer_role;
GRANT ALL PRIVILEGES ON SCHEMA tastybytes.raw TO ROLE tasty_data_engineer_role;
GRANT ALL PRIVILEGES ON SCHEMA tastybytes.analytics TO ROLE tasty_data_engineer_role;
GRANT USAGE ON WAREHOUSE tasty_bi_wh TO ROLE tasty_data_engineer_role;
GRANT USAGE ON WAREHOUSE tasty_ml_wh TO ROLE tasty_data_engineer_role;

-- Grant Execution privileges for Tasks and Pipes
GRANT EXECUTE TASK ON ACCOUNT TO ROLE tasty_data_engineer_role;
GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE tasty_data_engineer_role;
```

---

## 3. Stages, External Tables & Continuous Ingestion (Snowpipe)

Ingesting raw telemetry and ordering logs from Cloud Storage involves creating file formats, configuring stages, defining external partitions, and setting up an automated Snowpipe file landing architecture.

```sql
USE ROLE tasty_data_engineer_role;
USE DATABASE tastybytes;
USE SCHEMA raw;

-- 1. File Format Configuration
CREATE OR REPLACE FILE FORMAT csv_json_wrapper_ff
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE;

CREATE OR REPLACE FILE FORMAT csv_compressed_ff
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    COMPRESSION = 'GZIP';

-- 2. Define Internal and External Stages
CREATE OR REPLACE STAGE raw_internal_stage
    FILE_FORMAT = csv_json_wrapper_ff
    COMMENT = 'Internal repository for incoming JSON payload files';

-- Note: External stage requires a pre-existing Cloud Storage Integration
CREATE OR REPLACE STAGE raw_external_aws_stage
    URL = 's3://sf-tastybytes-sub/order_logs/'
    COMMENT = 'External S3 Bucket landing zone';

-- 3. External Table with Directory Partitioning
CREATE OR REPLACE EXTERNAL TABLE ext_order_logs_raw (
    order_id NUMBER(38,0) AS (value:c1::number),
    truck_id NUMBER(38,0) AS (value:c2::number),
    customer_id NUMBER(38,0) AS (value:c3::number),
    order_ts TIMESTAMP_NTZ AS (value:c4::timestamp_ntz),
    order_amount NUMBER(10,2) AS (value:c5::number(10,2))
)
PARTITION BY (partition_date = to_date(substr(metadata$filename, 12, 10)))
LOCATION = @raw_external_aws_stage
FILE_FORMAT = csv_compressed_ff;

-- 4. Target Standard Table for Automated Ingestion
CREATE OR REPLACE TABLE order_payload_ingest (
    raw_payload VARIANT,
    ingested_at TIMESTAMP_LTZ DEFAULT current_timestamp()
);

-- 5. Create Snowpipe for Continuous Auto-Ingestion
CREATE OR REPLACE PIPE order_payload_snowpipe
    AUTO_INGEST = TRUE
    AWS_SNS_TOPIC = 'arn:aws:sns:us-east-1:123456789012:tastybytes-s3-ingest-topic'
    AS
    COPY INTO tastybytes.raw.order_payload_ingest(raw_payload)
    FROM @tastybytes.raw.raw_internal_stage
    FILE_FORMAT = (TYPE = 'JSON');
```

---

## 4. Semi-Structured Data Manipulation & Advanced Views

Modern telemetry files bundle contextual attributes inside rich JSON graphs. Below, we leverage `LATERAL FLATTEN` to unpack nested object hierarchies and cache them using regular and Materialized Views.

```sql
USE SCHEMA raw;

-- Insert Mock Complex Semi-Structured Data
INSERT INTO order_payload_ingest (raw_payload)
SELECT PARSE_JSON('{
    "order_id": 987654,
    "truck_metadata": { "truck_id": 42, "city": "Paris", "brand": "Crepes R Us" },
    "customer": { "customer_id": 8812, "loyalty_tier": "Gold" },
    "line_items": [
        { "menu_item_id": 101, "quantity": 2, "unit_price": 7.50 },
        { "menu_item_id": 104, "quantity": 1, "unit_price": 12.00 }
    ],
    "transaction_date": "2026-07-09T10:00:00Z"
}');

USE SCHEMA analytics;

-- Secure View exposing flattened order level details
CREATE OR REPLACE SECURE VIEW v_order_header_summary AS
SELECT
    raw_payload:order_id::NUMBER AS order_id,
    raw_payload:truck_metadata.truck_id::NUMBER AS truck_id,
    raw_payload:truck_metadata.city::VARCHAR AS destination_city,
    raw_payload:customer.customer_id::NUMBER AS customer_id,
    raw_payload:customer.loyalty_tier::VARCHAR AS customer_tier,
    TO_TIMESTAMP_NTZ(raw_payload:transaction_date::VARCHAR) AS transaction_timestamp
FROM tastybytes.raw.order_payload_ingest;

-- High-performance view parsing deep arrays using LATERAL FLATTEN
CREATE OR REPLACE VIEW v_order_line_item_details AS
SELECT
    h.order_id,
    h.destination_city,
    f.value:menu_item_id::NUMBER AS menu_item_id,
    f.value:quantity::NUMBER AS order_quantity,
    f.value:unit_price::NUMBER(10,2) AS item_price,
    (order_quantity * item_price) AS line_total
FROM v_order_header_summary h,
LATERAL FLATTEN(input => tastybytes.raw.order_payload_ingest.raw_payload, path => 'line_items') f;

-- Create Materialized View for performance optimization on aggregate metrics
CREATE OR REPLACE MATERIALIZED VIEW mv_city_revenue_tracker
COMMENT = 'Pre-computed materialized aggregation of total global operational revenues'
AS
SELECT 
    destination_city,
    COUNT(DISTINCT order_id) as total_order_count,
    SUM(line_total) as aggregate_revenue
FROM tastybytes.analytics.v_order_line_item_details
GROUP BY destination_city;
```

---

## 5. Time Travel & Zero-Copy Cloning

Snowflake's multi-cluster shared data architecture preserves historic state mutations without duplicating base micro-partitions.

```sql
USE SCHEMA analytics;

-- Create Base Operational Table for Active Modifiers
CREATE OR REPLACE TABLE operational_truck_fleet AS 
SELECT 
    raw_payload:truck_metadata.truck_id::NUMBER AS truck_id,
    raw_payload:truck_metadata.brand::VARCHAR AS franchise_brand,
    CURRENT_TIMESTAMP() AS last_updated_ts
FROM tastybytes.raw.order_payload_ingest;

-- Set explicit data retention time travel window (in days)
ALTER TABLE operational_truck_fleet SET DATA_RETENTION_TIME_IN_DAYS = 14;

-- Capture current Query ID for precise temporal point recovery
SET query_checkpoint_id = (SELECT last_query_id());

-- Simulating an Accidental Destructive Action
UPDATE operational_truck_fleet SET franchise_brand = 'MALICIOUS_OVERWRITE';

-- 1. Time Travel Querying (Viewing data BEFORE the bad update statement executed)
SELECT * FROM operational_truck_fleet BEFORE(STATEMENT => $query_checkpoint_id);

-- 2. Zero-Copy Cloning for Sandbox Environment
CREATE OR REPLACE TABLE operational_truck_fleet_dev_backup
    CLONE operational_truck_fleet BEFORE(STATEMENT => $query_checkpoint_id);
```

---

## 6. Programmability: UDFs & Stored Procedures (SQL & Python)

We leverage built-in language execution engines to implement both highly fast scalar math logic and multi-step transaction loops inside the warehouse.

```sql
USE SCHEMA analytics;

-- 1. SQL User-Defined Function (UDF): Scalar calculation
CREATE OR REPLACE FUNCTION udf_calculate_net_margin(gross_sales NUMBER(10,2), cost_basis NUMBER(10,2))
RETURNS NUMBER(10,2)
AS
$$
    (gross_sales - cost_basis) * 0.85
$$;

-- 2. Python User-Defined Function (UDF): Text Cleansing
CREATE OR REPLACE FUNCTION py_udf_normalize_city_names(city VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'clean_text'
AS
$$
def clean_text(city_string: str) -> str:
    if not city_string:
        return "UNKNOWN"
    return city_string.strip().upper()
$$;

-- 3. Python Stored Procedure: Execute transactional management routines
CREATE OR REPLACE PROCEDURE sp_purge_and_repartition_dev_tables(target_prefix VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'execute_purge'
AS
$$
import snowflake.snowpark as snowpark

def execute_purge(session: snowpark.Session, target_prefix: str) -> str:
    tables_df = session.sql(f"SHOW TABLES LIKE '{target_prefix}%'")
    results = tables_df.collect()
    
    deleted_counter = 0
    for row in results:
        table_name = row['name']
        session.sql(f"DROP TABLE IF EXISTS {table_name}").collect()
        deleted_counter += 1
        
    return f"Success! Purged {deleted_counter} temporary testing tables."
$$;
```

---

## 7. Continuous Data Pipelines: Streams & Tasks (DAGs)

By linking change-data-capture (CDC) objects with transaction triggers, we construct fully self-orchestrating operational DAG structures.

```sql
USE SCHEMA analytics;

-- Create an Append-Only Stream over raw ingest ingestion table
CREATE OR REPLACE STREAM order_ingest_stream ON TABLE tastybytes.raw.order_payload_ingest
    APPEND_ONLY = TRUE;

-- Create Base Target Analytical Destination Table
CREATE OR REPLACE TABLE core_order_fact (
    order_id NUMBER,
    truck_id NUMBER,
    revenue NUMBER(10,2),
    processed_by_task VARCHAR,
    dw_load_ts TIMESTAMP_NTZ
);

-- Root Master Gateway Task (Orchestrates execution window schedule)
CREATE OR REPLACE TASK tsk_process_order_stream_root
    WAREHOUSE = tasty_bi_wh
    SCHEDULE = 'USING CRON */5 * * * * UTC'
    WHEN SYSTEM$STREAM_HAS_DATA('order_ingest_stream')
AS
    INSERT INTO core_order_fact (order_id, truck_id, revenue, processed_by_task, dw_load_ts)
    SELECT 
        raw_payload:order_id::NUMBER,
        raw_payload:truck_metadata.truck_id::NUMBER,
        (raw_payload:customer.customer_id::NUMBER * 0.05)::NUMBER(10,2),
        'tsk_process_order_stream_root',
        CURRENT_TIMESTAMP()
    FROM order_ingest_stream;

-- Child Dependent Consumer Task
CREATE OR REPLACE TASK tsk_downstream_metrics_refresh
    WAREHOUSE = tasty_bi_wh
    AFTER tsk_process_order_stream_root
AS
    ALTER MATERIALIZED VIEW mv_city_revenue_tracker REFRESH;

-- Activate tasks in hierarchical sequence
ALTER TASK tsk_downstream_metrics_refresh RESUME;
ALTER TASK tsk_process_order_stream_root RESUME;
```

---

## 8. Snowpark Python API: DataFrames & Snowflake CLI

Using Python wrappers instead of native text queries streamlines pipeline abstractions for developers.

### Python Snowpark Orchestration Engine
```python
# pipeline_script.py
import snowflake.snowpark as snowpark
from snowflake.snowpark.functions import col, lit, when

def main(session: snowpark.Session) -> snowpark.DataFrame:
    raw_df = session.table("tastybytes.raw.order_payload_ingest")
    
    processed_df = raw_df.select(
        col("raw_payload")["order_id"].cast("int").alias("ORDER_ID"),
        col("raw_payload")["customer"]["loyalty_tier"].cast("string").alias("TIER")
    ).filter(col("TIER").is_not_null()).with_column("MARKETING_BONUS", 
                  when(col("TIER") == "Gold", lit(25))
                  .otherwise(lit(5)))
                  
    processed_df.write.mode("append").save_as_table("tastybytes.analytics.snowpark_loyalty_rewards")
    return processed_df
```

### Deployment Configuration via Snowflake CLI (`snowflake.yml`)
```yaml
definition_version: "1"
project_details:
  name: tastybytes_automation_framework
  description: "Deploys custom data engineering automation routines to Snowflake"
procedures:
  - name: sp_run_snowpark_pipeline
    signature: ""
    returns: string
    handler: "pipeline_script.main"
    runtime: "3.10"
    packages:
      - snowflake-snowpark-python
```
To deploy this package using the Snowflake CLI tool:
```bash
snow connection test
snow object deploy
snow procedure call "sp_run_snowpark_pipeline()"
```

---

## 9. Enterprise AI: Querying LLMs with Snowflake Cortex

Snowflake Cortex provides built-in machine learning and LLM capabilities directly inside SQL.

```sql
USE SCHEMA analytics;

-- Create unstructured comment testing ground
CREATE OR REPLACE TABLE customer_feedback_reviews (
    review_id INT,
    raw_review_text VARCHAR
);

INSERT INTO customer_feedback_reviews VALUES 
(1, 'The pulled pork tacos from the Nashville truck were absolutely sensational. Five stars!'),
(2, 'Extremely slow service at the London location. Waited 45 minutes for a cold burger.');

-- Execute Advanced Text Generation via Cortex Functions
CREATE OR REPLACE VIEW v_ai_enriched_feedback AS
SELECT 
    review_id,
    raw_review_text,
    SNOWFLAKE.CORTEX.SENTIMENT(raw_review_text) AS calculated_sentiment_score,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(raw_review_text, 'What menu item did they buy?') AS purchased_item,
    SNOWFLAKE.CORTEX.COMPLETE('mistral-large', 
        CONCAT('Summarize this menu review in one short sentence and translate to Spanish: ', raw_review_text)
    ) AS ai_spanish_summary
FROM customer_feedback_reviews;
```

---

## 10. Snowpark ML: Training & Deploying an XGBoost Model

We use the memory-optimized `tasty_ml_wh` cluster alongside the `snowflake-ml-python` ecosystem to train and register an XGBoost model.

```python
# ml_training_orchestrator.py
import snowflake.snowpark as snowpark
from snowflake.ml.modeling.xgboost import XGBRegressor
from snowflake.ml.registry import Registry

def train_and_register_xgboost_model(session: snowpark.Session) -> str:
    training_data_df = session.sql("""
        SELECT 
            TRUCK_ID, 
            COALESCE(RAW_PAYLOAD:customer.customer_id::INT, 0) as CUST_ID,
            RAW_PAYLOAD:order_id::INT as FEATURES_COUNT,
            RAW_PAYLOAD:line_items[0].unit_price::FLOAT as PRICE_POINT,
            RAW_PAYLOAD:line_items[0].quantity::FLOAT as TARGET_QUANTITY
        FROM tastybytes.raw.order_payload_ingest
    """)
    
    train_df, test_df = training_data_df.random_split(weights=[0.8, 0.2], seed=42)
    
    regressor = XGBRegressor(
        input_cols=["TRUCK_ID", "CUST_ID", "FEATURES_COUNT", "PRICE_POINT"],
        label_cols=["TARGET_QUANTITY"],
        output_cols=["PREDICTED_QUANTITY"]
    )
    
    regressor.fit(train_df)
    
    native_registry = Registry(session=session, database_name="tastybytes", schema_name="analytics")
    registered_model = native_registry.log_model(
        model=regressor,
        model_name="truck_demand_xgboost_model",
        version_name="v1_production",
        comment="XGBoost model predicting truck-level order volume quantities.",
        sample_input_data=train_df.limit(5)
    )
    return "Success"
```

---

## 11. Operational Dashboards: Streamlit in Snowflake

Streamlit in Snowflake allows you to deploy secure interactive data web apps directly inside your Snowflake environment.

```python
# streamlit_app.py
import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(layout="wide", page_title="Tasty Bytes Global Revenue Analytics Engine")
st.title("🚚 Tasty Bytes Global Executive Performance Dashboard")

session = get_active_session()

@st.cache_data(ttl=300)
def load_metrics_summary():
    query = "SELECT destination_city, total_order_count, aggregate_revenue FROM tastybytes.analytics.mv_city_revenue_tracker ORDER BY aggregate_revenue DESC;"
    return session.sql(query).to_pandas()

df = load_metrics_summary()

metric_col1, metric_col2 = st.columns(2)
with metric_col1:
    st.metric(label="Total Global Revenue Analyzed", value=f"${df['AGGREGATE_REVENUE'].sum():,.2f}")
with metric_col2:
    st.metric(label="Total Unique Food Truck Orders", value=f"{df['TOTAL_ORDER_COUNT'].sum():,}")

st.markdown("### Operational City Revenue Breakdown")
st.bar_chart(data=df, x="DESTINATION_CITY", y="AGGREGATE_REVENUE", color="#1f77b4")
```
