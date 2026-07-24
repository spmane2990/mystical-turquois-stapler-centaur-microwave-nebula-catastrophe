# Snowflake Course Notes

This document summarizes the main Snowflake concepts covered in the course in a compact, study-friendly format. It is intended to be used as a personal reference for core ideas, key SQL patterns, and practical notes.

## Core topics covered

- Table structures and micro-partitioning
- Permanent, temporary, and transient tables
- Time travel, retention, and recovering dropped tables
- Regular, secure, and materialized views
- Query performance tuning with caching, clustering, and search optimization
- Loading and querying Parquet and JSON data
- Users, roles, and privilege management

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

Query historical data:

```sql
SELECT * FROM IN_PRODUCTION AT (OFFSET => -60 * 20);
```

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

## Practical takeaways

- Start with table structures and retention before moving to views.
- Use views to simplify repeated logic and secure subsets of data.
- Use clustering and search optimization only when they clearly improve query performance.
- Prefer roles and least-privilege access for secure administration.
- Semi-structured data is best handled with VARIANT columns and flattening techniques.
- Time travel is a powerful recovery and auditing feature, but it should be configured thoughtfully based on storage and compliance needs.
