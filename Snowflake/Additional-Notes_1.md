# Modern Data Engineering with Snowflake, DevOps, & Observability

A comprehensive reference guide covering Snowflake architecture, continuous data engineering (declarative and imperative), automated ingestion via Snowpipe, Database Change Management (DCM), continuous delivery (CI/CD), semi-structured data processing, query performance tuning, and pipeline observability through Snowflake Trail.

---

## Table of Contents

- [1. Continuous Data Pipelines](#1-continuous-data-pipelines)
  - [1.1 Declarative Pipelines: Dynamic Tables](#11-declarative-pipelines-dynamic-tables)
  - [1.2 Imperative Pipelines: Streams, Tasks, & DAGs](#12-imperative-pipelines-streams-tasks--dags)
  - [1.3 Deep-Dive: Streams Mechanics & Pipeline Use Cases](#13-deep-dive-streams-mechanics--pipeline-use-cases)
  - [1.4 Automated Continuous Ingestion: Snowpipe](#14-automated-continuous-ingestion-snowpipe)
  - [1.5 Pipeline Architecture Comparison](#15-pipeline-architecture-comparison)
- [2. DevOps in Data Engineering](#2-devops-in-data-engineering)
  - [2.1 Core DevOps Pillars](#21-core-devops-pillars)
  - [2.2 Snowflake Git Integration Setup](#22-snowflake-git-integration-setup)
- [3. Database Change Management (DCM)](#3-database-change-management-dcm)
  - [3.1 Imperative vs. Declarative Management](#31-imperative-vs-declarative-management)
  - [3.2 In-Place Upgrades with `CREATE OR ALTER`](#32-in-place-upgrades-with-create-or-alter)
- [4. Continuous Delivery (CI/CD) Pipeline](#4-continuous-delivery-cicd-pipeline)
  - [4.1 Snowflake CLI (`snow git execute`)](#41-snowflake-cli-snow-git-execute)
  - [4.2 GitHub Actions Workflow](#42-github-actions-workflow)
- [5. Data Pipeline Observability (Snowflake Trail)](#5-data-pipeline-observability-snowflake-trail)
  - [5.1 Core Observability Pillars](#51-core-observability-pillars)
  - [5.2 Snowflake Trail Framework & OpenTelemetry Standard](#52-snowflake-trail-framework--opentelemetry-standard)
  - [5.3 Event Tables Setup](#53-event-tables-setup)
  - [5.4 Logging Implementation](#54-logging-implementation)
  - [5.5 Tracing Implementation](#55-tracing-implementation)
  - [5.6 Alerts & Notification Integrations](#56-alerts--notification-integrations)
  - [5.7 Native & Third-Party Telemetry Tools](#57-native--third-party-telemetry-tools)
- [6. Data Ingestion & Staging](#6-data-ingestion--staging)
  - [6.1 Stage Categories & Iceberg Compatibility](#61-stage-categories--iceberg-compatibility)
  - [6.2 Local Uploads via `PUT` and `COPY INTO`](#62-local-uploads-via-put-and-copy-into)
- [7. Semi-Structured Data Processing (`LATERAL FLATTEN`)](#7-semi-structured-data-processing-lateral-flatten)
- [8. Performance Tuning & Micro-Partition Clustering](#8-performance-tuning--micro-partition-clustering)

---

## 1. Continuous Data Pipelines

Snowflake continuous data pipelines automate ingestion, transformation, and data delivery in near real-time. Pipelines follow two main engineering models: **Declarative** and **Imperative**.

```
                           Continuous Data Pipelines
                                       |
        +------------------------------+------------------------------+
        |                              |                              |
Declarative Pipelines          Imperative Pipelines         Continuous Ingestion
(What result you want)        (How to step-by-step)            (Micro-batching)
        |                              |                              |
 Dynamic Tables                 Tasks + Streams                    Snowpipe
 (Auto-managed)            (CDC metadata & DAGs)           (Auto-ingest via SNS/SQS)

```

---

### 1.1 Declarative Pipelines: Dynamic Tables

Declarative pipelines focus on defining the **desired end state** using standard SQL queries. Snowflake automatically manages execution scheduling, multi-table dependency tracking, and incremental processing based on a specified freshness lag (`TARGET_LAG`).

#### Refresh Modes

- **`AUTO` (Default):** Snowflake evaluates the underlying query structure and dynamically selects the most cost-effective refresh strategy (`INCREMENTAL` or `FULL`).
- **`INCREMENTAL`:** Calculates only the changed row deltas since the prior execution run and merges them into the destination table, reducing compute duration.
- **`FULL`:** Fully re-evaluates the query definition and overwrites the dynamic table. Acts as a fallback for queries containing non-deterministic logic or unsupported functions.

#### Initialization Strategies

- **`INITIALIZE = ON_CREATE`:** Triggers immediate materialization upon creation so data is available instantly.
- **`INITIALIZE = ON_SCHEDULE`:** Defers initial execution until the first scheduled cycle within the configured `TARGET_LAG` window.

```sql
-- Source raw table
CREATE OR REPLACE TABLE SOURCE_SALES_RAW (
    SALE_ID INT,
    PRODUCT STRING,
    CATEGORY STRING,
    PRICE NUMBER(10, 2),
    SALE_DATE DATE
);

-- Declarative Dynamic Table definition
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

-- Operational Management Controls
ALTER DYNAMIC TABLE SALES_BY_CATEGORY SUSPEND;
ALTER DYNAMIC TABLE SALES_BY_CATEGORY RESUME;

```

---

### 1.2 Imperative Pipelines: Streams, Tasks, & DAGs

Imperative pipelines require data engineers to define the explicit step-by-step logic, schedule triggers, dependency graphs, and Change Data Capture (CDC) processing rules.

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

#### Tasks & Directed Acyclic Graphs (DAGs)

- **Tasks:** Execute scheduled SQL statements, stored procedures, or procedural blocks.
- **DAGs:** Form multi-step pipelines by linking child tasks to predecessor tasks using the `AFTER` statement. Only the **Root Task** defines a `SCHEDULE`.
- **Conditional Execution:** Using `WHEN SYSTEM$STREAM_HAS_DATA('stream_name')` ensures a task executes only when unprocessed delta records are detected, eliminating idle compute expenses.

```sql
-- 1. Create Source Table & CDC Stream
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

-- 3. Root Task (Scheduled via CRON; checks stream status)
CREATE OR REPLACE TASK TASK_ROOT_INGEST
    WAREHOUSE = PRACTICE_WH
    SCHEDULE = 'USING CRON 0 6 * * * UTC'
    WHEN SYSTEM$STREAM_HAS_DATA('RAW_ORDERS_STREAM')
AS
INSERT INTO ORDER_SUMMARY (ORDER_ID, CUSTOMER_ID, AMOUNT, PROCESSED_AT)
SELECT ORDER_ID, CUSTOMER_ID, AMOUNT, CURRENT_TIMESTAMP()
FROM RAW_ORDERS_STREAM
WHERE METADATA$ACTION = 'INSERT';

-- 4. Child Task (Executes automatically AFTER Root Task)
CREATE OR REPLACE TASK TASK_CHILD_HIGH_VALUE
    WAREHOUSE = PRACTICE_WH
    AFTER TASK_ROOT_INGEST
AS
INSERT INTO HIGH_VALUE_ORDERS (ORDER_ID, AMOUNT)
SELECT ORDER_ID, AMOUNT
FROM ORDER_SUMMARY
WHERE AMOUNT >= 500.00;

-- 5. Task DAG Lifecycle Management
-- Tasks are created suspended by default. Enable child tasks BEFORE root task.
ALTER TASK TASK_CHILD_HIGH_VALUE RESUME;
ALTER TASK TASK_ROOT_INGEST RESUME;

-- Manually trigger Root Task immediately
EXECUTE TASK TASK_ROOT_INGEST;

```

---

### 1.3 Deep-Dive: Streams Mechanics & Pipeline Use Cases

A **Stream** is a lightweight Change Data Capture (CDC) object in Snowflake that records all Data Manipulation Language (DML) modifications (`INSERT`, `UPDATE`, `DELETE`) made to a source table, view, or stage since a specific point in time.

```
+------------------+         +-------------------------------+         +-----------------------+
|  Source Table    |  --->   |       Snowflake Stream        |  --->   | Task / MERGE Pipeline |
|  (Raw Ingestion) |         | (Metadata Deltas & Changes)   |         | (Incremental Load)    |
+------------------+         +-------------------------------+         +-----------------------+

```

#### Key Mechanics of Streams

1. **Offset Tracking:** A stream creates a transactional offset pointer anchored to the source table's micro-partition version. It does not duplicate raw physical data; instead, it tracks micro-partition version changes, preserving storage efficiency.
2. **System Metadata Columns:** When queried, a stream presents the delta view alongside three system-managed metadata attributes:

- `METADATA$ACTION`: Identifies the operational delta—`INSERT` or `DELETE`.
- `METADATA$ISUPDATE`: Returns `TRUE` if the change was part of an `UPDATE` command (represented in the stream as a `DELETE` of the old state followed by an `INSERT` of the new state).
- `METADATA$ROW_ID`: Represents a unique, immutable row identity key used to track rows across updates.

3. **Transaction Offset Consumption:** Reading a stream via a standard `SELECT` query does **not** advance its transactional offset. The offset moves forward only when the stream data is successfully consumed in a committed DML transaction (such as `INSERT INTO ... SELECT`, `MERGE`, or `CREATE TABLE AS SELECT`). If the downstream DML statement fails or rolls back, the stream offset remains unchanged.

#### Types of Streams

- **Standard Streams:** Tracks all DML changes (`INSERT`, `UPDATE`, `DELETE`) on regular tables, views, or external tables.
- **Append-Only Streams:** Tracks `INSERT` operations only. Ideal for immutable append-only workloads (e.g., event logs, IoT feeds), reducing evaluation overhead by ignoring `UPDATE` and `DELETE` activity.
- **Insert-Only Streams:** Used exclusively on external tables to track newly added files and records.

#### Primary Pipeline Use Cases for Streams

- **Incremental Micro-Batching:** Avoids costly full-table scans by processing only net-new change data downstream.
- **SCD Type 2 Dimension Processing:** Pairs with `MERGE INTO` SQL statements in scheduled tasks to track historical dimension changes efficiently.
- **Cost Minimization via Conditional Tasks:** Combined with `SYSTEM$STREAM_HAS_DATA('stream_name')`, downstream processing compute (warehouses) is triggered only when actionable data changes exist.

#### Complete Stream-Driven MERGE Pipeline Example

```sql
-- Step 1: Base table receiving raw transactional data
CREATE OR REPLACE TABLE RAW_TRANSACTIONS (
    TXN_ID INT,
    CUST_ID INT,
    AMOUNT NUMBER(10,2),
    STATUS STRING,
    UPDATED_AT TIMESTAMP_NTZ
);

-- Step 2: Create a Standard Stream on the raw table
CREATE OR REPLACE STREAM RAW_TXN_STREAM ON TABLE RAW_TRANSACTIONS;

-- Step 3: Target Analytical Table
CREATE OR REPLACE TABLE CURATED_TRANSACTIONS (
    TXN_ID INT PRIMARY KEY,
    CUST_ID INT,
    AMOUNT NUMBER(10,2),
    STATUS STRING,
    LAST_PROCESSED_AT TIMESTAMP_NTZ
);

-- Step 4: Scheduled Task executing incremental CDC MERGE logic
CREATE OR REPLACE TASK PROCESS_TXN_STREAM_TASK
    WAREHOUSE = PRACTICE_WH
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('RAW_TXN_STREAM')
AS
MERGE INTO CURATED_TRANSACTIONS T
USING (
    SELECT
        TXN_ID,
        CUST_ID,
        AMOUNT,
        STATUS,
        METADATA$ACTION,
        METADATA$ISUPDATE
    FROM RAW_TXN_STREAM
) S
ON T.TXN_ID = S.TXN_ID
-- Handle UPDATES and INSERTS
WHEN MATCHED AND S.METADATA$ACTION = 'INSERT' AND S.METADATA$ISUPDATE = TRUE THEN
    UPDATE SET
        T.AMOUNT = S.AMOUNT,
        T.STATUS = S.STATUS,
        T.LAST_PROCESSED_AT = CURRENT_TIMESTAMP()
-- Handle DELETES
WHEN MATCHED AND S.METADATA$ACTION = 'DELETE' AND S.METADATA$ISUPDATE = FALSE THEN
    DELETE
-- Handle NEW INSERTS
WHEN NOT MATCHED AND S.METADATA$ACTION = 'INSERT' THEN
    INSERT (TXN_ID, CUST_ID, AMOUNT, STATUS, LAST_PROCESSED_AT)
    VALUES (S.TXN_ID, S.CUST_ID, S.AMOUNT, S.STATUS, CURRENT_TIMESTAMP());

-- Enable the Task
ALTER TASK PROCESS_TXN_STREAM_TASK RESUME;

```

---

### 1.4 Automated Continuous Ingestion: Snowpipe

**Snowpipe** is Snowflake's native, serverless continuous data ingestion service. It continuously loads files into target tables as soon as they are uploaded to an internal or external stage.

```
[ Cloud Storage / Stage ] ---> [ Storage Notification (SNS/SQS) ] ---> [ Snowpipe ] ---> [ Target Table ]

```

#### Key Architecture Principles

- **Serverless Compute:** Uses Snowflake-managed compute resources rather than user-configured virtual warehouses, charging only for exact usage.
- **Event-Driven Loading:** Utilizes cloud storage notification events (AWS SQS/SNS, Azure Event Grid, or GCP Pub/Sub) to auto-trigger ingestion routines in near real-time.
- **Pipe Object:** Wraps a customized `COPY INTO` SQL command inside an automated pipe object.

```sql
-- Step 1: Create a target landing table
CREATE OR REPLACE TABLE STAGED_SERVER_LOGS (
    LOG_TIMESTAMP TIMESTAMP_NTZ,
    DEVICE_ID STRING,
    MESSAGE STRING,
    ERROR_CODE INT
);

-- Step 2: Define an External Stage
CREATE OR REPLACE STAGE LOGS_EXTERNAL_STAGE
    URL = 's3://my-organization-logs-bucket/raw/'
    STORAGE_INTEGRATION = S3_STORAGE_INT
    FILE_FORMAT = (TYPE = 'JSON');

-- Step 3: Create Snowpipe with embedded COPY INTO statement
CREATE OR REPLACE PIPE AUTO_INGEST_LOGS_PIPE
    AUTO_INGEST = TRUE
AS
COPY INTO STAGED_SERVER_LOGS
FROM (
    SELECT
        $1:timestamp::TIMESTAMP_NTZ,
        $1:device_id::STRING,
        $1:message::STRING,
        $1:error_code::INT
    FROM @LOGS_EXTERNAL_STAGE
);

-- Step 4: Show Pipe status and obtain Notification Channel ARN for Cloud Config
SHOW PIPES;
SELECT SYSTEM$PIPE_STATUS('AUTO_INGEST_LOGS_PIPE');

-- Pause or Resume Pipe operational execution
ALTER PIPE AUTO_INGEST_LOGS_PIPE SET PIPE_EXECUTION_PAUSED = TRUE;
ALTER PIPE AUTO_INGEST_LOGS_PIPE SET PIPE_EXECUTION_PAUSED = FALSE;

```

---

### 1.5 Pipeline Architecture Comparison

| Feature               | Dynamic Tables                   | Tasks & Streams                      | Snowpipe                             |
| --------------------- | -------------------------------- | ------------------------------------ | ------------------------------------ |
| **Pipeline Type**     | Declarative Transformation       | Imperative Orchestration             | Continuous Ingestion                 |
| **Primary Focus**     | Complex SQL transform layers     | Procedural step workflows / CDC      | Micro-batch raw file ingestion       |
| **Compute Model**     | User Virtual Warehouse           | User Virtual Warehouse               | Serverless (Snowflake-managed)       |
| **Trigger Mechanism** | Target Lag Window (`TARGET_LAG`) | Time Interval, CRON, or `AFTER` task | Event Notifications (SQS/SNS/PubSub) |
| **Orchestration**     | Engine-managed graph             | Explicit Task DAGs                   | Single-statement ingestion wrapper   |

---

## 2. DevOps in Data Engineering

DevOps in data engineering provides best practices and tooling to rapidly deploy, manage, and scale reliable data pipelines.

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

1. **Source Control & Collaboration:** Storing pipeline code, database object definitions, and transformation scripts in Git to maintain an audit trail and single source of truth.
2. **Declarative Code Management:** Updating database objects using single-source definitions rather than managing ordered procedural migration scripts.
3. **Automation (CI/CD):** Testing and deploying pipeline changes automatically across isolated development environments (`STAGING`, `PROD`).
4. **Modern Tooling:** Utilizing developer interfaces (`Snowflake CLI`) and automation providers (`GitHub Actions`) to automate lifecycle tasks.

---

### 2.2 Snowflake Git Integration Setup

Snowflake integrates with remote Git providers (such as GitHub) by mounting a repository natively as a stage-like object in Snowflake.

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

Database Change Management (DCM) is the practice of defining database objects in code within a repository and deploying those objects (and changes) using automated tools.

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

- **Imperative Approach:** Applies sequential migration scripts (`01.sql`, `02.sql`, `03.sql`) to transition database objects step-by-step. This method can be error-prone, cumbersome to manage across multiple databases, and dependent on strict ordering.
- **Declarative Approach:** Defines the final desired state of an object in a single source-controlled file. The system compares the desired state against the target database and applies only the required updates.

---

### 3.2 In-Place Upgrades with `CREATE OR ALTER`

The `CREATE OR ALTER` SQL command enables declarative database change management in Snowflake. It creates the object if it does not exist, or updates it in-place without dropping the object.

```sql
-- Initial Definition
CREATE OR ALTER TABLE STAGING_TASTY_BYTES.RAW_POS.COUNTRY (
    COUNTRY_ID NUMBER(18,0),
    COUNTRY STRING
);

-- Evolved Definition: Adds 'CITY_ID' in-place, preserving data, tags, policies, and grants
CREATE OR ALTER TABLE STAGING_TASTY_BYTES.RAW_POS.COUNTRY (
    COUNTRY_ID NUMBER(18,0),
    COUNTRY STRING,
    CITY_ID NUMBER(19,0)
);

```

> **Warning:** Omitting an existing column in a `CREATE OR ALTER` statement will cause Snowflake to drop that column and its contents. Dropped data can be restored using **Time Travel**.

---

## 4. Continuous Delivery (CI/CD) Pipeline

Continuous Delivery (CD) automates deploying source-controlled code to dedicated staging and production environments.

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

The `Snowflake CLI` executes files or directory trees directly from Snowflake Git repository objects while interpolating runtime parameters.

```bash
# Execute staging environment setup with variable parameterization
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

This workflow (`.github/workflows/main.yaml`) deploys code into Snowflake `STAGING` or `PROD` databases automatically whenever changes are pushed or merged into `staging` or `main` branches.

```yaml
name: Snowflake Continuous Delivery

on:
  push:
    branches: [staging, main]
  pull_request:
    types: [closed]
    branches: [staging, main]

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

## 5. Data Pipeline Observability (Snowflake Trail)

Observability enables data engineers to identify runtime issues, isolate root causes, and monitor performance health across continuous data pipelines. **Snowflake Trail** is Snowflake's native observability framework designed to standardize telemetry collection and monitoring.

```
                    Snowflake Trail Observability Architecture
                                        |
      +---------------------------------+---------------------------------+
      |                                 |                                 |
 [ Event Tables ]              [ System Metrics ]             [ Alerts & Actions ]
(OpenTelemetry Logs & Traces) (Resource / Warehouse Use)     (Notification Integrations)
      |                                                                   |
 Vendor-Neutral Standard                                     Email / Slack / Webhooks / Queues

```

---

### 5.1 Core Observability Pillars

- **Logs:** Immutable, timestamped event records capturing discrete occurrences (start, completion, failures) within code or procedure executions.
- **Traces:** Detailed, itemized journeys of requests moving through a system. Traces track execution timing, steps, spans, and parent-child dependencies.
- **Metrics:** Aggregated numeric measurements representing system health, resource consumption, latency distributions, and throughput.

---

### 5.2 Snowflake Trail Framework & OpenTelemetry Standard

**Snowflake Trail** unifies native telemetry objects—Event Tables, Alerts, and Notification Integrations—into a cohesive framework.

- **OpenTelemetry Adherence:** Snowflake Trail is built directly on the **OpenTelemetry (OTel)** open standard. OpenTelemetry provides a vendor-neutral, industry-standard specification for data schemas and telemetry capture.
- **Interoperability:** Because Snowflake Trail complies with OpenTelemetry standards, telemetry generated in Snowflake can easily integrate into third-party observability dashboards without complex transformation layers.

---

### 5.3 Event Tables Setup

**Event Tables** are specialized database objects used to capture logs and traces emitted by stored procedures, UDFs, and Snowflake applications. They automatically structure incoming data using OpenTelemetry-compliant schema columns.

```sql
-- Step 1: Create a custom Event Table in target database/schema
CREATE OR REPLACE EVENT TABLE STAGING_TASTY_BYTES.TELEMETRY.PIPELINE_EVENTS;

-- Step 2: Register the table as the active Event Table for the account
ALTER ACCOUNT SET EVENT_TABLE = STAGING_TASTY_BYTES.TELEMETRY.PIPELINE_EVENTS;

-- Step 3: Inspect OpenTelemetry standard columns
DESCRIBE TABLE STAGING_TASTY_BYTES.TELEMETRY.PIPELINE_EVENTS;

```

---

### 5.4 Logging Implementation

Snowflake supports five standard log levels: `DEBUG`, `INFO`, `WARN`, `ERROR`, and `FATAL`. Setting a log level establishes a minimum severity threshold for telemetry capture.

```sql
-- Set Account-level Log Severity Threshold
ALTER ACCOUNT SET LOG_LEVEL = INFO;

```

#### Python Handler Example with Embedded Logging

```sql
CREATE OR REPLACE PROCEDURE PROCESS_ORDER_HEADERS_STREAM_PROC()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'logging')
HANDLER = 'run'
AS
$$
import logging
import snowflake.snowpark as snowpark

# Instantiate standard Python logger
logger = logging.getLogger("pipeline_logger")

def run(session: snowpark.Session):
    logger.info("Starting process_order_headers_stream procedure.")

    try:
        # Fetch stream count
        df = session.table("RAW_POS.ORDER_HEADER_STREAM")
        count = df.count()

        logger.info(f"Found {count} orders in stream to process.")

        # Transformation execution logic here...

        logger.info("Procedure completed successfully.")
        return "SUCCESS"
    except Exception as e:
        logger.error(f"Execution failed with exception: {str(e)}")
        raise e
$$;

-- Querying Captured Log Telemetry from Event Table
SELECT
    TIMESTAMP,
    RESOURCE_ATTRIBUTES['snowflake.object.name']::STRING AS OBJECT_NAME,
    RECORD['severity_text']::STRING AS LOG_LEVEL,
    VALUE::STRING AS LOG_MESSAGE
FROM STAGING_TASTY_BYTES.TELEMETRY.PIPELINE_EVENTS
WHERE RECORD_TYPE = 'log'
ORDER BY TIMESTAMP DESC;

```

---

### 5.5 Tracing Implementation

Traces capture execution hierarchies and timing distributions using **Spans** (units of work), **Trace Events** (moments in time), and **Attributes** (key-value tags).

```sql
-- Enable Tracing for the Session
ALTER SESSION SET TRACE_LEVEL = ALWAYS;

```

#### Python Handler Example with OpenTelemetry Traces

```sql
CREATE OR REPLACE PROCEDURE PROCESS_ORDER_HEADERS_TRACE_PROC()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-snowpark-python', 'snowflake-telemetry-python')
HANDLER = 'run'
AS
$$
import uuid
from snowflake import telemetry
import snowflake.snowpark as snowpark

def run(session: snowpark.Session):
    # Set custom trace correlation identifier
    trace_id = str(uuid.uuid4())

    # Tag root span attributes
    telemetry.set_span_attribute("procedure.name", "PROCESS_ORDER_HEADERS_TRACE_PROC")
    telemetry.set_span_attribute("execution.id", trace_id)

    # Add a discrete Trace Event
    telemetry.add_event("query_begin", {"description": "Querying raw order stream"})

    df = session.table("RAW_POS.ORDER_HEADER_STREAM")
    records_count = df.count()

    telemetry.add_event("query_complete", {"records_found": records_count})
    return "SUCCESS"
$$;

-- Querying Span Telemetry from Event Table
SELECT
    TIMESTAMP,
    RECORD['name']::STRING AS SPAN_NAME,
    RECORD_ATTRIBUTES['procedure.name']::STRING AS PROC_NAME,
    RECORD_ATTRIBUTES['execution.id']::STRING AS TRACE_ID
FROM STAGING_TASTY_BYTES.TELEMETRY.PIPELINE_EVENTS
WHERE RECORD_TYPE LIKE 'span%'
ORDER BY TIMESTAMP DESC;

```

---

### 5.6 Alerts & Notification Integrations

**Alerts** evaluate data conditions on a schedule, while **Notification Integrations** dispatch automated alerts to external endpoints (Email, Webhooks/Slack, or Cloud Queues).

```sql
-- Step 1: Create an Email Notification Integration
CREATE OR REPLACE NOTIFICATION INTEGRATION EMAIL_NOTIFICATION_INT
    TYPE = EMAIL
    ENABLED = TRUE
    ALLOWED_RECIPIENTS = ('data_team_lead@company.com');

-- Step 2: Create a Stored Procedure to send HTML alert emails
CREATE OR REPLACE PROCEDURE NOTIFY_DATA_QUALITY_TEAM()
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    CALL SYSTEM$SEND_EMAIL(
        'EMAIL_NOTIFICATION_INT',
        'data_team_lead@company.com',
        'Data Pipeline Alert: NULL Values Detected',
        'Warning: Unprocessed NULL values detected in RAW_POS.ORDER_HEADER.'
    );
    RETURN 'Notification Sent';
END;
$$;

-- Step 3: Create a Snowflake Alert object
CREATE OR REPLACE ALERT ORDER_DATA_QUALITY_ALERT
    WAREHOUSE = PRACTICE_WH
    SCHEDULE = '1 MINUTE'
    IF (EXISTS (
        SELECT 1 FROM RAW_POS.ORDER_HEADER WHERE ORDER_ID IS NULL
    ))
    THEN
        CALL NOTIFY_DATA_QUALITY_TEAM();

-- Resume the Alert (Alerts are created suspended by default)
ALTER ALERT ORDER_DATA_QUALITY_ALERT RESUME;

-- Suspend or Drop Alert
ALTER ALERT ORDER_DATA_QUALITY_ALERT SUSPEND;
DROP ALERT ORDER_DATA_QUALITY_ALERT;

```

---

### 5.7 Native & Third-Party Telemetry Tools

Because Snowflake Trail adopts standard OpenTelemetry schemas, recorded telemetry data can be exported to third-party monitoring platforms (e.g., Datadog, Grafana, PagerDuty) or visualized natively using Snowflake Dashboards, Snowsight monitoring, and System Views.

---

## 6. Data Ingestion & Staging

Stages serve as intermediate locations for files before loading them into relational tables.

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

### 6.1 Stage Categories & Iceberg Compatibility

| Stage Category      | Description                                                                                                              | Iceberg Table Support                                                        |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------- |
| **Internal Stages** | Managed within Snowflake internal cloud storage (User `@~`, Table `@%`, or Named `@stage`). Access is governed via RBAC. | **Not Supported** (Iceberg requires open external format structures).        |
| **External Stages** | References external cloud storage buckets (AWS S3, Azure Blob, GCS) using Storage Integrations.                          | **Required** (Supports Apache Iceberg open table metadata & Parquet format). |

---

### 6.2 Local Uploads via `PUT` and `COPY INTO`

Files on a developer endpoint can be staged via `PUT` commands and loaded using `COPY INTO` with error handling flags.

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

## 7. Semi-Structured Data Processing (`LATERAL FLATTEN`)

Semi-structured JSON datasets are ingested into `VARIANT` columns. Specific properties are accessed using dot/bracket notation, and nested arrays are expanded into relational rows using `LATERAL FLATTEN`.

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

## 8. Performance Tuning & Micro-Partition Clustering

Snowflake stores table data in compressed, columnar micro-partitions. Over time, table modifications can cause key ranges to scatter across micro-partitions. The `SYSTEM$CLUSTERING_INFORMATION` function measures partition health and key overlaps.

```sql
-- Analyze table clustering efficiency on specific key columns
SELECT SYSTEM$CLUSTERING_INFORMATION('IOWA_SALES', '(COUNTY)');

```

### Interpreting Output Metrics

```json
{
  "cluster_by_keys": "LINEAR(COUNTY)",
  "total_partition_count": 1250,
  "total_constant_partition_count": 800,
  "average_overlaps": 2.14,
  "average_depth": 3.45,
  "partition_depth_histogram": {
    "00000": 800,
    "00001": 300,
    "00002": 100,
    "00003": 50
  }
}
```

- **`total_partition_count`:** Total micro-partitions constituting the table.
- **`total_constant_partition_count`:** Count of micro-partitions where clustering key values do not overlap with any other partitions. _(Higher is better)_.
- **`average_overlaps`:** Average number of distinct partitions overlapping a shared value range for the given key. _(Lower is better; 0 represents no overlap)_.
- **`average_depth`:** Average partition depth that must be scanned to find key values. _(Lower is better)_.
- **`partition_depth_histogram`:** Shows partition distribution across depth buckets. Well-clustered tables have the majority of partitions in lower depth bins (e.g., `00000`–`00001`).
