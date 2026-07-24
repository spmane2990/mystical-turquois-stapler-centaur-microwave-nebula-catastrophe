# Snowflake Course Notes

This document summarizes the main Snowflake concepts covered in the course in a compact, study-friendly format. It is intended to be used as a personal reference for core ideas, key SQL patterns, and practical notes.

## Core topics covered

- Table structures and micro-partitioning
- Permanent, temporary, and transient tables
- Time travel, retention, and recovering dropped tables
- Regular, secure, and materialized views
- Query performance tuning with caching, clustering, and search optimization
- Warehouse sizing, query acceleration, and query profiling
- Loading and querying Parquet and JSON data
- Recursive views, dynamic tables, and staging workflows
- Users, roles, and privilege management
- Specialized table types such as hybrid, external, Iceberg, dynamic, and interactive tables

## Course notes and key SQL snippets

### 1. Table structures in Snowflake

Snowflake stores data in a highly optimized way that is very different from traditional row-based databases. Instead of relying on a single large storage block, Snowflake automatically divides table data into many small internal units called micro-partitions. These micro-partitions are tiny, compressed, and columnar, which allows Snowflake to scan only the relevant data during query execution.

Why this matters:

- Queries can skip irrelevant micro-partitions, which improves performance.
- Snowflake stores metadata such as min/max values and distinct counts per column to support pruning.
- Columnar storage means only the columns needed by a query are read.

Tables can be created as permanent, temporary, or transient depending on how long the data should remain available and how much recovery support is needed.

Example snippet:

```sql
CREATE OR REPLACE TABLE IN_PRODUCTION (
    BRAND STRING,
    MODEL STRING,
    COLOR STRING,
    MEMORY STRING,
    STORAGE STRING,
    RATING FLOAT,
    LAUNCH_DATE DATE
);
```

Temporary table example:

```sql
CREATE OR REPLACE TEMPORARY TABLE IN_TESTING (
    BRAND STRING,
    MODEL STRING,
    COLOR STRING,
    MEMORY STRING,
    STORAGE STRING,
    RATING FLOAT,
    LAUNCH_DATE DATE
);
```

Transient table example:

```sql
CREATE OR REPLACE TRANSIENT TABLE IN_BETA (
    BRAND STRING,
    MODEL STRING,
    COLOR STRING,
    MEMORY STRING,
    STORAGE STRING,
    RATING FLOAT,
    LAUNCH_DATE DATE
);
```

### 2. Retention and time travel

Time travel is one of Snowflake’s most useful recovery features. It allows you to access historical versions of data as they existed at a previous point in time. This is especially useful for auditing, accidental updates, and restoring data after mistakes.

Key ideas:

- Retention period defines how far back historical data is preserved.
- Permanent tables usually support longer retention than temporary or transient tables.
- Time travel is not the same as fail-safe. Fail-safe is a separate recovery mechanism used for disaster recovery.
- If a table is dropped while time travel is enabled, it can often be restored using UNDROP TABLE.

Example snippet:

```sql
ALTER TABLE IN_PRODUCTION
SET DATA_RETENTION_TIME_IN_DAYS = 7;
```

Query historical data using an offset:

```sql
SELECT * FROM IN_PRODUCTION AT (OFFSET => -60 * 20);
```

Query historical data using a query ID:

```sql
SELECT last_query_id();
```

```sql
SELECT * FROM IN_PRODUCTION BEFORE(STATEMENT => '<query_id>');
```

This pattern is useful when you want to see the table as it existed immediately before a specific statement was executed. In the course example, the table initially contained OPPO and Google Pixel rows. After a delete and an update, the live table showed fewer rows and a renamed brand value. By querying before the earlier statement, the older values could still be recovered.

Query historical data using a timestamp:

```sql
SELECT CURRENT_TIMESTAMP();
```

```sql
SELECT * FROM IN_PRODUCTION AT (TIMESTAMP => '<timestamp>'::TIMESTAMP_TZ);
```

This method is useful when you want to inspect the state of the table at a specific time. In the course walkthrough, the current table was first observed, then a timestamp was captured, later changes were made, and the earlier state was retrieved by querying that saved timestamp.

Restore a dropped table:

```sql
UNDROP TABLE IN_PRODUCTION;
```

### 3. Views in Snowflake

Views are logical layers over existing tables. They allow you to define reusable SQL logic and present it as if it were a table. Views are helpful for abstraction, data simplification, and security because they let users access derived data without directly interacting with the underlying tables.

There are different types of views:

- Regular view: stores the query definition only. The underlying query is executed each time the view is used.
- Materialized view: stores the result set physically, making repeated reads faster, though it uses more storage.
- Secure view: hides the underlying query definition so users cannot see how the data is derived.

Regular view example:

```sql
CREATE OR REPLACE VIEW ORDER_SUMMARY AS
SELECT o.ord_id, o.date, c.cust_name, p.prod_name,
       (p.price * o.quantity) - (p.price * o.quantity) * (o.discount::FLOAT / 100) AS cost
FROM CUSTOMER_DETAILS c
JOIN ORDER_DETAILS o ON o.cust_id = c.cust_id
JOIN PRODUCT_INFO p ON p.prod_id = o.prod_id;
```

Materialized view example:

```sql
CREATE OR REPLACE MATERIALIZED VIEW HIGH_TAX_VIEW_MATERIALIZED AS
SELECT * FROM LINEITEM WHERE L_TAX > 0.05;
```

### 4. Query performance optimization

Snowflake performs many optimizations automatically, but there are also techniques you can explicitly use to improve performance for large workloads.

Main optimization approaches:

- Result caching: Snowflake can reuse previously executed query results if the same query is run again.
- Clustering: rows with similar values for a chosen clustering key are physically co-located, which helps queries that filter on that key.
- Search optimization: useful for selective point lookups where a query returns a small number of rows using equality filters.

Result caching example:

```sql
ALTER SESSION SET USE_CACHED_RESULT = TRUE;
```

Clustering example:

```sql
ALTER TABLE IOWA_SALES CLUSTER BY (COUNTY);
```

Search optimization example:

```sql
ALTER TABLE IOWA_SALES_OPTIMIZED ADD SEARCH OPTIMIZATION;
```

### 5. Semi-structured data

Semi-structured data is data that does not fit neatly into fixed rows and columns, such as JSON arrays, nested objects, or Parquet files. Snowflake handles this efficiently using VARIANT data types.

Why this is useful:

- You can ingest complex nested structures without first fully normalizing them.
- You can query specific fields inside JSON objects using dot notation.
- Arrays can be flattened into rows for easier analysis.

Parquet example:

```sql
COPY INTO CITIES
FROM (
    SELECT
        $1:continent::VARCHAR,
        $1:country:name::VARCHAR,
        $1:country:city::VARIANT
    FROM @CITIES_STAGE/cities.parquet
);
```

Flatten array example:

```sql
SELECT CONTINENT, COUNTRY, c.value::STRING AS CITY
FROM CITIES, LATERAL FLATTEN(INPUT => CITY) c;
```

JSON example:

```sql
COPY INTO ZOMATO_RESTAURANTS
FROM @ZOMATO_STAGE/zomato_data.json.gz
FILE_FORMAT = (
    TYPE = 'JSON',
    STRIP_OUTER_ARRAY = TRUE
);
```

### 6. Access control and privileges

Access control in Snowflake is based on roles rather than directly assigning permissions to every user. This makes administration simpler and more secure. A role can be granted specific privileges on warehouses, databases, schemas, and tables, and then assigned to users.

Best practices:

- Follow the principle of least privilege.
- Create custom roles for different job functions instead of giving everyone broad access.
- Grant only the minimum set of privileges needed to perform the task.

User and role example:

```sql
CREATE USER TestUser
PASSWORD = 'Test@1234'
COMMENT = 'This is a test user'
MUST_CHANGE_PASSWORD = FALSE;
```

Grant privileges example:

```sql
CREATE ROLE BASIC_ROLE;
GRANT ROLE BASIC_ROLE TO USER TestUser;
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE BASIC_ROLE;
GRANT USAGE ON DATABASE SALES_DB TO ROLE BASIC_ROLE;
GRANT USAGE ON SCHEMA SALES_DB.PUBLIC TO ROLE BASIC_ROLE;
GRANT SELECT ON TABLE SALES_DB.PUBLIC.IOWA_SALES TO ROLE BASIC_ROLE;
```

### 7. Warehouses, caching, and query acceleration

Warehouses are Snowflake’s compute resources. They determine how fast queries run and how much cost is incurred. Snowflake allows you to size warehouses differently, suspend or resume them automatically, and even enable query acceleration for large analytical workloads.

Key ideas:

- Larger warehouses usually improve performance, but also increase cost.
- Auto-suspend and auto-resume help control idle compute expenses.
- Result cache can make repeated queries very fast, while data cache can help when the same micro-partitions are reused.
- Query acceleration is useful when a query is large and benefits from extra parallelism.

Example snippet:

```sql
CREATE OR REPLACE WAREHOUSE PRACTICE_WH
WITH WAREHOUSE_SIZE='XSMALL'
AUTO_SUSPEND=10
AUTO_RESUME=TRUE;
```

```sql
CREATE OR REPLACE WAREHOUSE wh_with_qas
WITH WAREHOUSE_SIZE='SMALL'
ENABLE_QUERY_ACCELERATION=TRUE
QUERY_ACCELERATION_MAX_SCALE_FACTOR=64;
```

Query profiling is also an important practice. In Snowflake, the Query Profile shows where time is spent, such as table scans, joins, filters, and sorting. This helps you understand whether a query is I/O-bound, compute-bound, or affected by caching.

### 8. Recursive views, dynamic tables, and staging

The newer demos also introduce a few advanced patterns:

- Recursive views let you model hierarchical relationships such as manager-to-employee chains.
- Dynamic tables are continuously refreshed tables that maintain a near-real-time result based on source data.
- Stages are storage locations used to upload files before loading them into Snowflake tables.

Recursive view example:

```sql
CREATE OR REPLACE RECURSIVE VIEW EMPLOYEE_HIERARCHY
(emp_id, emp_name, emp_title, manager_id, level) AS (
    SELECT emp_id, emp_name, emp_title, manager_id, 1 AS level
    FROM EMPLOYEES
    WHERE manager_id IS NULL

    UNION ALL

    SELECT e.emp_id, e.emp_name, e.emp_title, e.manager_id, eh.level + 1 AS level
    FROM EMPLOYEES e
    JOIN EMPLOYEE_HIERARCHY eh
      ON e.manager_id = eh.emp_id
);
```

Dynamic table example:

```sql
CREATE OR REPLACE DYNAMIC TABLE sales_by_category
TARGET_LAG = '2 minute'
WAREHOUSE = PRACTICE_WH
REFRESH_MODE = auto
INITIALIZE = on_create
AS
SELECT category, SUM(price) AS total_price
FROM source_sales_raw
GROUP BY category;
```

Stage example:

```sql
CREATE OR REPLACE STAGE MISC_STAGE;
LIST @MISC_STAGE;
```

These features are especially useful in modern data engineering workflows where data freshness, lineage, and reusable transformations matter.

### 9. Specialized tables in Snowflake

In addition to standard Snowflake tables, the platform also supports specialized table types that are designed for very different workloads.

#### Hybrid tables

Hybrid tables are intended for transactional and operational workloads. They are optimized for row-based operations, row-level locking, and strong data integrity. They support primary keys, foreign keys, and unique constraints, which makes them useful for systems such as inventory updates, order processing, and session-state management.

Why they matter:

- They are better suited for OLTP-style workloads than the usual columnar analytical tables.
- They support ACID-style transactional behavior with concurrent updates.
- They can be joined with standard Snowflake tables in the same query.

Example:

```sql
CREATE HYBRID TABLE INVENTORY (
    PRODUCT_ID INT PRIMARY KEY,
    AVAILABLE_QTY INT NOT NULL,
    LAST_UPDATED TIMESTAMP_NTZ
);
```

```sql
INSERT INTO INVENTORY VALUES (101, 42, CURRENT_TIMESTAMP());

UPDATE INVENTORY
SET AVAILABLE_QTY = AVAILABLE_QTY - 1
WHERE PRODUCT_ID = 101;
```

#### External tables

External tables let Snowflake query files that live in external cloud storage such as Amazon S3, Azure Blob Storage, or Google Cloud Storage. The data remains outside Snowflake, but Snowflake creates a virtual table definition over those files so you can query them with SQL.

Important notes:

- They are typically read-only from the Snowflake side.
- They are useful for data lake access without physically loading everything into Snowflake.
- They work well with file formats such as CSV, JSON, and Parquet.

Example:

```sql
CREATE OR REPLACE EXTERNAL TABLE SALES_EXT (
    ORDER_ID INT AS (VALUE:order_id::INT),
    CUSTOMER_ID INT AS (VALUE:customer_id::INT),
    TOTAL_AMOUNT NUMBER AS (VALUE:total_amount::NUMBER)
)
WITH LOCATION = @sales_stage
FILE_FORMAT = (TYPE = 'JSON');
```

```sql
SELECT * FROM SALES_EXT WHERE TOTAL_AMOUNT > 1000;
```

#### Iceberg tables

Iceberg tables use the Apache Iceberg open table format. They are closely related to external tables because the data can live outside Snowflake in cloud storage, but they are more advanced because they use a structured Iceberg catalog and metadata layer. This makes them suitable for lakehouse-style architectures where Snowflake, Spark, Trino, or other engines need to work with the same table format.

Why they matter:

- They provide open-format interoperability with tools such as Spark and Trino.
- They support richer metadata and better pruning than simple external tables.
- They allow Snowflake to work with a managed, catalog-based table format while still keeping data in external storage.
- Unlike a basic external table, they can support more advanced table semantics and data management operations.

A simple comparison:

- External table: a read-focused SQL view over files in cloud storage.
- Iceberg table: a richer, catalog-driven table format that is still stored externally but offers more advanced features and interoperability.

##### 1. Snowflake as the Iceberg catalog

Use this when you want Snowflake to manage the Iceberg metadata and write the table files into an external volume.

Example:

```sql
CREATE OR REPLACE ICEBERG TABLE sales_iceberg_table
EXTERNAL_VOLUME = 's3_external_volume'
CATALOG = 'SNOWFLAKE'
BASE_LOCATION = 'analytics/sales'
PATH_LAYOUT = 'HIERARCHICAL'
TARGET_FILE_SIZE = '128MB'
AUTO_REFRESH = TRUE
COMMENT = 'Snowflake-managed Iceberg table in object storage';
```

Scenario:

- Your data is in Amazon S3.
- You want Snowflake to manage the Iceberg table metadata.
- You want Snowflake to query the table while keeping the files in object storage.

##### 2. External REST catalog

Use this when your Iceberg table is registered in a REST-based catalog such as Snowflake Open Catalog or another compatible service.

Example:

```sql
CREATE OR REPLACE ICEBERG TABLE sales_from_rest_catalog
EXTERNAL_VOLUME = 's3_external_volume'
CATALOG = 'my_rest_catalog_integration'
CATALOG_TABLE_NAME = 'sales_table'
CATALOG_NAMESPACE = 'analytics'
AUTO_REFRESH = TRUE
COMMENT = 'Iceberg table registered in an external REST catalog';
```

Scenario:

- Your organization already uses a REST catalog for table governance.
- You want Snowflake to access the table without copying it into Snowflake.
- The catalog service manages the table metadata remotely.

##### 3. Delta files in object storage

Use this when you already have Delta files in object storage and want Snowflake to expose them through the Iceberg table model.

Example:

```sql
CREATE OR REPLACE ICEBERG TABLE delta_sales
EXTERNAL_VOLUME = 's3_external_volume'
CATALOG = 'my_catalog_integration'
BASE_LOCATION = 'delta/sales'
AUTO_REFRESH = TRUE
COMMENT = 'Iceberg table created over Delta files in object storage';
```

Scenario:

- You have Delta-format files in S3.
- You want Snowflake to read them as Iceberg-compatible data without fully loading them into Snowflake.
- This is common in hybrid lakehouse environments.

##### 4. Existing Iceberg files in object storage

Use this when the Iceberg table already exists in storage and you want Snowflake to register it from the metadata file.

Example:

```sql
CREATE OR REPLACE ICEBERG TABLE iceberg_from_metadata
EXTERNAL_VOLUME = 's3_external_volume'
CATALOG = 'my_catalog_integration'
METADATA_FILE_PATH = 's3://my-bucket/iceberg-path/metadata/00000-abc123.metadata.json'
COMMENT = 'Iceberg table registered from an existing metadata file';
```

Scenario:

- A Spark job or another engine already created an Iceberg table in object storage.
- You want Snowflake to query that existing table without rebuilding it.
- This is useful for cross-engine analytics and data sharing.

##### 5. AWS Glue Catalog scenario

Use this when your Iceberg tables are managed by AWS Glue Catalog. Snowflake can connect to that catalog through a catalog integration and access the metadata from Glue.

Example:

```sql
CREATE OR REPLACE ICEBERG TABLE glue_catalog_sales
EXTERNAL_VOLUME = 's3_external_volume'
CATALOG = 'glue_catalog_integration'
CATALOG_TABLE_NAME = 'sales_table'
CATALOG_NAMESPACE = 'analytics'
AUTO_REFRESH = TRUE
COMMENT = 'Iceberg table discovered through AWS Glue Catalog';
```

Scenario:

- Your data is in Amazon S3.
- The table metadata is managed in AWS Glue Catalog.
- You want Snowflake to query the table as an Iceberg table without moving data into Snowflake.

Key takeaway:

- Snowflake as the catalog: Snowflake manages the Iceberg metadata.
- REST catalog: an external catalog service manages the metadata.
- Delta files: you are exposing Delta-backed data through the Iceberg table model.
- Metadata file path: you are registering an existing Iceberg table directly from storage.
- AWS Glue Catalog: you are integrating Snowflake with an AWS-based catalog ecosystem.

#### Dynamic tables

Dynamic tables are declarative data engineering objects. Instead of manually building a pipeline, you define the query, and Snowflake keeps the table refreshed automatically. They are very useful for near-real-time transformation pipelines.

Example:

```sql
CREATE OR REPLACE DYNAMIC TABLE SALES_BY_CATEGORY
TARGET_LAG = '2 minute'
WAREHOUSE = PRACTICE_WH
REFRESH_MODE = auto
AS
SELECT CATEGORY, SUM(PRICE) AS TOTAL_PRICE
FROM SOURCE_SALES
GROUP BY CATEGORY;
```

#### Interactive tables

Interactive tables are built for low-latency, high-concurrency serving workloads such as live dashboards and APIs. They are optimized for simple, selective queries rather than very large analytical scans.

Important notes:

- They are designed for sub-second or very fast response times.
- They require a mandatory CLUSTER BY clause.
- They should be queried using an interactive warehouse.

Example:

```sql
CREATE OR REPLACE INTERACTIVE TABLE DASHBOARD_ORDERS
CLUSTER BY (CUSTOMER_ID)
AS
SELECT CUSTOMER_ID, COUNT(*) AS ORDER_COUNT
FROM ORDERS
GROUP BY CUSTOMER_ID;
```

A good mental model is:

- Use standard tables for analytics.
- Use hybrid tables for transactional workloads.
- Use external and Iceberg tables when data lives in cloud storage or a lakehouse format.
- Use dynamic tables for continuously refreshed pipelines.
- Use interactive tables for user-facing, low-latency queries.

### 10. Stages, AWS integration, and loading different file formats

In Snowflake, a stage is a named location where files are stored before they are loaded into tables. Stages are commonly used with cloud storage such as Amazon S3, Azure Blob Storage, or Google Cloud Storage. Snowflake can access these files using a storage integration, which avoids hard-coding secrets and makes data loading more secure.

Key ideas:

- A stage is the bridge between external storage and Snowflake.
- A storage integration is used to securely connect Snowflake to cloud storage.
- File format objects define how Snowflake should interpret files such as CSV, JSON, or Parquet.
- Copy commands are used to load data from a stage into Snowflake tables.

#### Creating a stage

Example:

```sql
CREATE OR REPLACE STAGE aws_stage
URL = 's3://my-data-bucket/raw-data/'
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');
```

You can also create a named internal stage or use a Snowflake-managed stage for temporary file upload workflows.

#### Creating a storage integration for AWS

Example concept:

```sql
CREATE OR REPLACE STORAGE INTEGRATION s3_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/snowflake-role'
STORAGE_ALLOWED_LOCATIONS = ('s3://my-data-bucket/raw-data/');
```

This is useful because Snowflake can access S3 securely without embedding credentials directly in SQL.

#### File format objects

A file format object tells Snowflake how to read the file content.

CSV example:

```sql
CREATE OR REPLACE FILE FORMAT csv_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"';
```

JSON example:

```sql
CREATE OR REPLACE FILE FORMAT json_format
TYPE = 'JSON'
STRIP_OUTER_ARRAY = TRUE;
```

Parquet example:

```sql
CREATE OR REPLACE FILE FORMAT parquet_format
TYPE = 'PARQUET';
```

#### Loading CSV files

Example:

```sql
CREATE OR REPLACE TABLE customers (
    customer_id INT,
    customer_name STRING,
    city STRING
);

COPY INTO customers
FROM @aws_stage/customers.csv
FILE_FORMAT = (FORMAT_NAME = 'csv_format');
```

#### Loading JSON files

Example:

```sql
CREATE OR REPLACE TABLE orders_json (
    raw_data VARIANT
);

COPY INTO orders_json
FROM @aws_stage/orders.json
FILE_FORMAT = (FORMAT_NAME = 'json_format');
```

#### Loading Parquet files

Example:

```sql
CREATE OR REPLACE TABLE sales_parquet (
    order_id INT,
    sale_amount NUMBER,
    sale_date DATE
);

COPY INTO sales_parquet
FROM @aws_stage/sales.parquet
FILE_FORMAT = (FORMAT_NAME = 'parquet_format');
```

#### When to use which format

- CSV: simple flat files, exports, legacy systems.
- JSON: nested and semi-structured event data.
- Parquet: large analytic datasets where columnar storage and compression are beneficial.

This workflow is one of the most common Snowflake ingestion patterns: stage files in cloud storage, define a file format, and load them into tables with COPY INTO.

#### Sending emails from Snowflake

Snowflake can also be used for operational notifications. This is typically done with a notification integration and a task or stored procedure that triggers an alert when something important happens, such as a failed load, a threshold breach, or a daily summary.

Example concept:

```sql
CREATE OR REPLACE NOTIFICATION INTEGRATION email_notify
TYPE = EMAIL
ENABLED = TRUE
COMMENT = 'Notification integration for email alerts';
```

A typical workflow is:

```sql
CREATE OR REPLACE TASK daily_check
WAREHOUSE = PRACTICE_WH
SCHEDULE = 'USING CRON 0 8 * * * UTC'
AS
SELECT CURRENT_TIMESTAMP();
```

In practice, you would pair this with an alerting mechanism or a stored procedure that sends an email when a condition is met. This is useful for operational monitoring, ETL failure alerts, and business-rule notifications.

## Practical takeaways

- Start with table structures and retention before moving to views.
- Use views to simplify repeated logic and secure subsets of data.
- Use clustering and search optimization only when they clearly improve query performance.
- Prefer roles and least-privilege access for secure administration.
- Semi-structured data is best handled with VARIANT columns and flattening techniques.
- Time travel is a powerful recovery and auditing feature, but it should be configured thoughtfully based on storage and compliance needs.
- Warehouse sizing, query profiling, and query acceleration matter when you need predictable performance and cost control.
