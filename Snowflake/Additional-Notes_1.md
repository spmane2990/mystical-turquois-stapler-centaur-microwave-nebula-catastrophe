# Modern Data Engineering with Snowflake & DevOps

A comprehensive reference guide covering Snowflake architecture, data pipeline engineering (declarative and imperative), Database Change Management (DCM), continuous delivery (CI/CD), semi-structured data processing, and query performance optimization.

---

## Table of Contents

1. [Continuous Data Pipelines](https://www.google.com/search?q=%231-continuous-data-pipelines)
* [Declarative Pipelines: Dynamic Tables](https://www.google.com/search?q=%2311-declarative-pipelines-dynamic-tables)
* [Imperative Pipelines: Tasks, Streams, & DAGs](https://www.google.com/search?q=%2312-imperative-pipelines-tasks-streams--dags)
* [Declarative vs. Imperative Comparison](https://www.google.com/search?q=%2313-declarative-vs-imperative-comparison)


2. [DevOps in Data Engineering](https://www.google.com/search?q=%232-devops-in-data-engineering)
* [Core DevOps Pillars](https://www.google.com/search?q=%2321-core-devops-pillars)
* [Snowflake Git Integration Setup](https://www.google.com/search?q=%2322-snowflake-git-integration-setup)


3. [Database Change Management (DCM)](https://www.google.com/search?q=%233-database-change-management-dcm)
* [Imperative vs. Declarative Management](https://www.google.com/search?q=%2331-imperative-vs-declarative-management)
* [In-Place Upgrades with `CREATE OR ALTER](https://www.google.com/search?q=%2332-in-place-upgrades-with-create-or-alter)`


4. [Continuous Delivery (CI/CD) Pipeline](https://www.google.com/search?q=%234-continuous-delivery-cicd-pipeline)
* [Snowflake CLI (`snow git execute`)](https://www.google.com/search?q=%2341-snowflake-cli-snow-git-execute)
* [GitHub Actions Workflow](https://www.google.com/search?q=%2342-github-actions-workflow)


5. [Data Ingestion & Staging](https://www.google.com/search?q=%235-data-ingestion--staging)
* [Stage Categories & Iceberg Compatibility](https://www.google.com/search?q=%2351-stage-categories--iceberg-compatibility)
* [Local Uploads via `PUT` and `COPY INTO](https://www.google.com/search?q=%2352-local-uploads-via-put-and-copy-into)`


6. [Semi-Structured Data Processing (`LATERAL FLATTEN`)](https://www.google.com/search?q=%236-semi-structured-data-processing-lateral-flatten)
7. [Performance Tuning & Micro-Partition Clustering](https://www.google.com/search?q=%237-performance-tuning--micro-partition-clustering)

---

## 1. Continuous Data Pipelines

Snowflake continuous data pipelines ingest, transform, and move data in near real-time as soon as it arrives[cite: 1]. They fall into two main categories: **Declarative** and **Imperative**[cite: 1].

```
                           Continuous Data Pipelines
                                       |
                +----------------------+----------------------+
                |                                             |
   Declarative Pipelines                         Imperative Pipelines
   (Focus on desired outcome)                     (Focus on execution steps)
                |                                             |
         Dynamic Tables                                Tasks + Streams
  (Auto-managed lag & refresh)                 (Explicit CDC & Scheduling)

```

---

### 1.1 Declarative Pipelines: Dynamic Tables

Declarative pipelines focus on the **desired end outcome** rather than the procedural steps required to compute it[cite: 1]. In Snowflake, this is handled by **Dynamic Tables**[cite: 1]. You specify a SQL query and a target freshness window (`TARGET_LAG`), and Snowflake manages dependency tracking, background scheduling, and incremental materialization[cite: 1].

#### Refresh Modes

* **`AUTO` (Default):** Snowflake automatically selects between incremental and full refresh based on query complexity[cite: 1].
* **`INCREMENTAL`:** Calculates and merges only the specific delta changes in the source data since the last update, reducing compute costs[cite: 1].
* **`FULL`:** Re-executes the defining query completely, replacing existing results[cite: 1]. Used as a fallback for non-deterministic or unsupported transformation queries[cite: 1].

#### Initialization Options

* **`INITIALIZE = ON_CREATE`:** Populates data immediately upon table creation[cite: 1].
* **`INITIALIZE = ON_SCHEDULE`:** Defers initial population until the first scheduled refresh cycle within the `TARGET_LAG` window[cite: 1].

```sql
-- Source table
CREATE OR REPLACE TABLE SOURCE_SALES_RAW (
    SALE_ID INT,
    PRODUCT STRING,
    CATEGORY STRING,
    PRICE NUMBER(10, 2),
    SALE_DATE DATE
);

-- Declarative Dynamic Table
CREATE OR REPLACE DYNAMIC TABLE SALES_BY_CATEGORY
    TARGET_LAG = '2 minute'
    WAREHOUSE = PRACTICE_WH
    REFRESH_MODE = AUTO
    INITIALIZE = ON_CREATE
AS
SELECT 
    CATEGORY,
    SUM(PRICE) AS TOTAL_PRICE
FROM SOURCE_SALES_RAW
GROUP BY CATEGORY;

-- Managing Dynamic Tables
ALTER DYNAMIC TABLE SALES_BY_CATEGORY SUSPEND;
ALTER DYNAMIC TABLE SALES_BY_CATEGORY RESUME;

```

---

### 1.2 Imperative Pipelines: Tasks, Streams, & DAGs

Imperative pipelines specify **step-by-step actions** to transform data, giving engineers control over explicit dependencies, execution order, and custom procedural logic[cite: 1].

```
                               Imperative Processing Workflow
                                             |
[ Source Table ] ---> [ Stream (CDC Delta) ] ---> [ Root Task (Schedule/Cron) ]
                                                        |
                                            +-----------+-----------+
                                            |                       |
                                     [ Child Task A ]        [ Child Task B ]
                                     (AFTER Root)            (AFTER Root)

```

#### Streams (Change Data Capture)

A **Stream** records all Data Manipulation Language (DML) modifications (`INSERT`, `UPDATE`, `DELETE`) made to a source table[cite: 1]. It exposes system metadata columns:

* `METADATA$ACTION`: Indicates whether the operation was an `INSERT` or `DELETE`[cite: 1].
* `METADATA$ISUPDATE`: A boolean flag indicating if the row was modified as part of an `UPDATE` operation[cite: 1].
* `METADATA$ROW_ID`: Unique internal row identifier[cite: 1].

> **Stream Offset Offset Behavior:** Querying a stream in a basic `SELECT` statement does not advance its offset. The stream only consumes data and advances its tracking offset when utilized in a committed transaction (e.g., `INSERT`, `MERGE`, or `UPDATE`).

#### Tasks & Directed Acyclic Graphs (DAGs)

* **Tasks** execute scheduled SQL statements, stored procedures, or procedural code[cite: 1].
* A **DAG** chains tasks together using the `AFTER` clause. Only the **Root Task** defines a `SCHEDULE`. Child tasks execute automatically upon the successful completion of their predecessor task[cite: 1].
* **Conditional Execution:** The `WHEN SYSTEM$STREAM_HAS_DATA('stream_name')` clause ensures tasks run only when new delta records exist in the stream, preventing unnecessary compute expense[cite: 1].

```sql
-- 1. Create Source & Stream
CREATE OR REPLACE TABLE RAW_ORDERS (
    ORDER_ID INT,
    CUSTOMER_ID INT,
    AMOUNT NUMBER(10,2),
    STATUS STRING
);

CREATE OR REPLACE STREAM RAW_ORDERS_STREAM ON TABLE RAW_ORDERS;

-- 2. Target Tables
CREATE OR REPLACE TABLE ORDER_SUMMARY (
    ORDER_ID INT,
    CUSTOMER_ID INT,
    AMOUNT NUMBER(10,2),
    PROCESSED_AT TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE HIGH_VALUE_ORDERS (
    ORDER_ID INT,
    AMOUNT NUMBER(10,2)
);

-- 3. Root Task (Scheduled with CRON and conditional stream execution)
CREATE OR REPLACE TASK TASK_ROOT_INGEST
    WAREHOUSE = PRACTICE_WH
    SCHEDULE = 'USING CRON 0 6 * * * UTC'
    WHEN SYSTEM$STREAM_HAS_DATA('RAW_ORDERS_STREAM')
AS
INSERT INTO ORDER_SUMMARY (ORDER_ID, CUSTOMER_ID, AMOUNT, PROCESSED_AT)
SELECT ORDER_ID, CUSTOMER_ID, AMOUNT, CURRENT_TIMESTAMP()
FROM RAW_ORDERS_STREAM
WHERE METADATA$ACTION = 'INSERT';

-- 4. Child Task (Runs automatically AFTER Root Task)
CREATE OR REPLACE TASK TASK_CHILD_HIGH_VALUE
    WAREHOUSE = PRACTICE_WH
    AFTER TASK_ROOT_INGEST
AS
INSERT INTO HIGH_VALUE_ORDERS (ORDER_ID, AMOUNT)
SELECT ORDER_ID, AMOUNT
FROM ORDER_SUMMARY
WHERE AMOUNT >= 500.00;

-- 5. DAG Lifecycle Management (Resume Bottom-Up, Suspend Top-Down)
-- Enable Child tasks first, then Root
ALTER TASK TASK_CHILD_HIGH_VALUE RESUME;
ALTER TASK TASK_ROOT_INGEST RESUME;

-- Manually trigger Root Task immediately
EXECUTE TASK TASK_ROOT_INGEST;

```

---

### 1.3 Declarative vs. Imperative Comparison

| Feature | Declarative (Dynamic Tables) | Imperative (Tasks + Streams) |
| --- | --- | --- |
| **Model** | "What" result set you want materialized[cite: 1]. | "How" to transform and move data step-by-step[cite: 1]. |
| **Orchestration** | Fully automated by Snowflake engine[cite: 1]. | Explicitly built using Task DAGs[cite: 1]. |
| **Freshness Target** | Parameterized via `TARGET_LAG` (e.g., `'5 minute'`)[cite: 1]. | Managed via explicit Task `SCHEDULE` (Interval/CRON)[cite: 1]. |
| **Change Tracking** | Integrated dependency and lineage graph tracking[cite: 1]. | Manual Change Data Capture via Stream deltas[cite: 1]. |

---

## 2. DevOps in Data Engineering

DevOps in data engineering is a set of core philosophies, practices, and tooling designed to allow engineering teams to quickly, safely, and reliably deploy and evolve data pipelines at scale[cite: 1].

```
                 DevOps Pillars in Data Engineering
                                 |
     +-------------------+-------+-------+-------------------+
     |                   |               |                   |
Source Control    Declarative Code   Automation        Modern Tooling
 & Collaboration     Management        (CI/CD)        (CLIs, Integrations)

```

---

### 2.1 Core DevOps Pillars

1. **Source Control & Collaboration:** Storing pipeline code, database object definitions, and transformation logic in version control systems (e.g., Git/GitHub) to maintain a single source of truth and complete audit trail[cite: 1].
2. **Declarative Code Management:** Updating object definitions incrementally without complex, procedural migration scripts[cite: 1].
3. **Automation (CI/CD):** Testing and deploying pipeline modifications automatically across isolated development environments to maintain high uptime[cite: 1].
4. **Modern Tooling:** Streamlining workflows via developer CLI tools (`Snowflake CLI`) and automation integration platforms (`GitHub Actions`)[cite: 1].

---

### 2.2 Snowflake Git Integration Setup

Snowflake integrates directly with remote Git providers (such as GitHub), allowing a repository to be mounted natively inside Snowflake as an external stage-like object[cite: 1].

```
+------------------+         +----------------------------+         +---------------------------+
| GitHub Repository|  <--->  | API Integration & Secret   |  <--->  | Git Repository Object     |
| (Source Code)    |         | (Authentication & Domain)  |         | (Accessible as Stage)     |
+------------------+         +----------------------------+         +---------------------------+

```

```sql
-- Step 1: Store GitHub Personal Access Token (PAT) inside a Secret
CREATE OR REPLACE SECRET GITHUB_PAT
    TYPE = PLAINTEXT_PASSWORD
    USERNAME = 'your_github_username'
    PASSWORD = 'ghp_your_personal_access_token_here';

-- Step 2: Create API Integration targeting your GitHub profile domain
CREATE OR REPLACE API INTEGRATION GIT_API_INTEGRATION
    API_PROVIDER = GIT_HTTPS_API
    API_ALLOWED_PREFIXES = ('https://github.com/your_github_username')
    ALLOWED_AUTHENTICATION_SECRETS = (GITHUB_PAT)
    ENABLED = TRUE;

-- Step 3: Create native Git Repository Object
CREATE OR REPLACE GIT REPOSITORY COURSE_REPO.PUBLIC.ADVANCED_DATA_ENGINEERING_SNOWFLAKE
    API_INTEGRATION = GIT_API_INTEGRATION
    ORIGIN = 'https://github.com/your_github_username/advanced_data_engineering_snowflake.git'
    GIT_CREDENTIALS = COURSE_REPO.PUBLIC.GITHUB_PAT;

-- Step 4: Verify connection and list files
SHOW GIT REPOSITORIES;
LIST @COURSE_REPO.PUBLIC.ADVANCED_DATA_ENGINEERING_SNOWFLAKE/branches/main/;

```

---

## 3. Database Change Management (DCM)

Database Change Management (DCM)—also known as schema migration—is the practice of defining all database objects in code within a repository and deploying those objects to environments using automation tools[cite: 1].

---

### 3.1 Imperative vs. Declarative Management

```
Imperative DCM                            Declarative DCM
(Step-by-step migration scripts)          (Desired end-state definition)

[v1.sql] -> [v2.sql] -> [v3.sql]          +----------------------------------+
                                          | CREATE OR ALTER TABLE my_table ( |
   Requires strict sequence execution     |   id INT, name STRING, city INT  |
   High operational overhead & risks      | );                               |
                                          +----------------------------------+
                                           Snowflake computes & applies deltas

```

* **Imperative Approach:** Applies sequential migration scripts (`01.sql`, `02.sql`, `03.sql`) to transition database objects step-by-step[cite: 1]. This method can be error-prone, cumbersome to manage across multiple databases, and dependent on strict ordering[cite: 1].
* **Declarative Approach:** Defines the final desired end state of an object in a single source file[cite: 1]. The system automatically compares the target state against the existing state and applies necessary updates[cite: 1].

---

### 3.2 In-Place Upgrades with `CREATE OR ALTER`

The `CREATE OR ALTER` SQL command allows Snowflake objects (such as tables) to be updated declaratively and idempotently[cite: 1]. It creates the object if it does not exist or updates it in-place without dropping the object[cite: 1].

```sql
-- Initial Definition
CREATE OR ALTER TABLE STAGING_TASTY_BYTES.RAW_POS.COUNTRY (
    COUNTRY_ID NUMBER(18,0),
    COUNTRY STRING
);

-- Evolved Definition: Adds 'CITY_ID' in-place preserving existing table data & grants
CREATE OR ALTER TABLE STAGING_TASTY_BYTES.RAW_POS.COUNTRY (
    COUNTRY_ID NUMBER(18,0),
    COUNTRY STRING,
    CITY_ID NUMBER(19,0)
);

```

> **Warning:** Modifying a table definition with `CREATE OR ALTER` by removing a column will drop that column and its contents[cite: 1]. Dropped data can be restored using **Time Travel**[cite: 1].

---

## 4. Continuous Delivery (CI/CD) Pipeline

Continuous Delivery (CD) automates deploying source-controlled code to dedicated staging and production environments[cite: 1].

```
                       CI/CD Deployment Workflow
                                   |
[ Push / Pull Request ] ---> [ GitHub Actions Pipeline ]
                                   |
                     Installs Snowflake CLI & Credentials
                                   |
                 Calls 'snow git execute' with Params
                                   |
               +-------------------+-------------------+
               |                                       |
    [ Target: STAGING Database ]           [ Target: PROD Database ]

```

---

### 4.1 Snowflake CLI (`snow git execute`)

The `Snowflake CLI` provides command-line tools to execute files or directory trees directly from Snowflake Git repository objects while interpolating environment variables[cite: 1].

```bash
# Execute staging environment data setup with variable parameterization
snow git execute @COURSE_REPO.PUBLIC.ADVANCED_DATA_ENGINEERING_SNOWFLAKE/branches/main/module1/hamburg_weather/pipeline/data/load_tasty_bytes.sql \
  -D "env='STAGING'" \
  --database=COURSE_REPO \
  --schema=PUBLIC

# Deploy production pipeline objects
snow git execute @COURSE_REPO.PUBLIC.ADVANCED_DATA_ENGINEERING_SNOWFLAKE/branches/main/module1/hamburg_weather/pipeline/objects/ \
  -D "env='PROD'" \
  --database=COURSE_REPO \
  --schema=PUBLIC

```

---

### 4.2 GitHub Actions Workflow

This workflow (`.github/workflows/main.yaml`) deploys code into Snowflake `STAGING` or `PROD` databases automatically whenever changes are pushed or merged into `staging` or `main` branches[cite: 1].

```yaml
name: Snowflake Continuous Delivery

on:
  push:
    branches: [ staging, main ]
  pull_request:
    types: [ closed ]
    branches: [ staging, main ]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout Source Code
        uses: actions/checkout@v3

      - name: Install Snowflake CLI
        uses: Snowflake-Labs/snowflake-cli-action@v1

      - name: Set Environment Target
        run: |
          if [ "${{ github.ref_name }}" = "staging" ]; then
            echo "DEPLOY_ENV=STAGING" >> $GITHUB_ENV
          elif [ "${{ github.ref_name }}" = "main" ]; then
            echo "DEPLOY_ENV=PROD" >> $GITHUB_ENV
          fi

      - name: Deploy Pipeline Objects via Snowflake CLI
        env:
          SNOWFLAKE_CONNECTIONS_DEFAULT_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_CONNECTIONS_DEFAULT_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
          SNOWFLAKE_CONNECTIONS_DEFAULT_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
        run: |
          snow git execute @COURSE_REPO.PUBLIC.ADVANCED_DATA_ENGINEERING_SNOWFLAKE/branches/${{ github.ref_name }}/module1/hamburg_weather/pipeline/data/load_tasty_bytes.sql \
            -D "env='${{ env.DEPLOY_ENV }}'"

```

---

## 5. Data Ingestion & Staging

Stages serve as intermediate locations for files before loading them into relational tables[cite: 1].

```
                                Stage Categories
                                       |
             +-------------------------+-------------------------+
             |                                                   |
      Internal Stages                                     External Stages
 (Snowflake-managed storage)                      (AWS S3, Azure Blob, GCS)
 - User (`@~`), Table (`@%`), Named (`@stage`)     - Required for Iceberg Tables

```

---

### 5.1 Stage Categories & Iceberg Compatibility

| Stage Category | Description | Iceberg Table Support |
| --- | --- | --- |
| **Internal Stages** | Managed within Snowflake internal cloud storage (User `@~`, Table `@%`, or Named `@stage`)[cite: 1]. Access governed via RBAC[cite: 1]. | **Not Supported** (Iceberg requires open external format structures)[cite: 1]. |
| **External Stages** | References external cloud storage buckets (AWS S3, Azure Blob, GCS) using Storage Integrations[cite: 1]. | **Required** (Supports Apache Iceberg open table metadata & Parquet format)[cite: 1]. |

---

### 5.2 Local Uploads via `PUT` and `COPY INTO`

Files on a developer endpoint can be staged via `PUT` commands and loaded using `COPY INTO` with error handling flags[cite: 1].

```sql
-- Step 1: Define an Internal Stage
CREATE OR REPLACE STAGE MY_LOCAL_STAGE;

-- Step 2: Upload local file (Executed in SnowSQL CLI)
-- PUT file:///tmp/sales_data.csv @MY_LOCAL_STAGE AUTO_COMPRESS=TRUE;

-- Step 3: Inspect files inside the stage
LIST @MY_LOCAL_STAGE;

-- Step 4: Load into target table with error handling options
COPY INTO INGESTED_SALES
FROM @MY_LOCAL_STAGE/sales_data.csv.gz
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE'; 
-- Options for ON_ERROR: 'CONTINUE' | 'SKIP_FILE' | 'ABORT_STATEMENT'

```

---

## 6. Semi-Structured Data Processing (`LATERAL FLATTEN`)

Semi-structured JSON datasets are ingested into `VARIANT` columns[cite: 1]. Specific properties are accessed using dot/bracket notation, and nested arrays are expanded into relational rows using `LATERAL FLATTEN`[cite: 1].

```sql
-- Step 1: Create landing table
CREATE OR REPLACE TABLE RAW_ZOMATO_JSON (
    SRC_VARIANT VARIANT
);

-- Step 2: Insert sample JSON
INSERT INTO RAW_ZOMATO_JSON
SELECT PARSE_JSON('{
  "city": "Pune",
  "restaurants": [
    {"name": "Spicy Bite", "rating": 4.5},
    {"name": "Urban Cafe", "rating": 4.2}
  ]
}');

-- Step 3: Query attributes and flatten nested JSON array
SELECT 
    SRC_VARIANT:city::STRING AS CITY_NAME,
    f.value:name::STRING AS RESTAURANT_NAME,
    f.value:rating::FLOAT AS RATING
FROM RAW_ZOMATO_JSON,
LATERAL FLATTEN(INPUT => SRC_VARIANT:restaurants) f;

```

---

## 7. Performance Tuning & Micro-Partition Clustering

Snowflake stores table data in compressed, columnar micro-partitions[cite: 1]. Over time, table modifications can cause key ranges to scatter across micro-partitions[cite: 1]. The `SYSTEM$CLUSTERING_INFORMATION` function measures partition health and key overlaps[cite: 1].

```sql
-- Analyze table clustering efficiency on specific key columns
SELECT SYSTEM$CLUSTERING_INFORMATION('IOWA_SALES', '(COUNTY)');

```

### Interpreting Output Metrics

```json
{
  "cluster_by_keys" : "LINEAR(COUNTY)",
  "total_partition_count" : 1250,
  "total_constant_partition_count" : 800,
  "average_overlaps" : 2.14,
  "average_depth" : 3.45,
  "partition_depth_histogram" : {
    "00000" : 800,
    "00001" : 300,
    "00002" : 100,
    "00003" : 50
  }
}

```

* **`total_partition_count`:** Total micro-partitions constituting the table[cite: 1].
* **`total_constant_partition_count`:** Count of micro-partitions where clustering key values do not overlap with any other partitions[cite: 1]. *(Higher is better)*[cite: 1].
* **`average_overlaps`:** Average number of distinct partitions overlapping a shared value range for the given key[cite: 1]. *(Lower is better; 0 represents no overlap)*[cite: 1].
* **`average_depth`:** Average partition depth that must be scanned to find key values[cite: 1]. *(Lower is better)*[cite: 1].
* **`partition_depth_histogram`:** Shows partition distribution across depth buckets[cite: 1]. Well-clustered tables have the majority of partitions in lower depth bins (e.g., `00000`–`00001`)[cite: 1].