# A Three-Level Observability Maturity Framework for Snowflake

---

## Executive Overview & Architectural Philosophy

The observability landscape in modern data platforms has evolved far beyond basic cron job and task execution monitoring. With the emergence of AI-enabled data pipelines, real-time analytics, and strict regulatory compliance requirements across enterprise sectors, data engineering teams face a multi-dimensional matrix of operational constraints:

- **Freshness SLAs:** Ensuring end-to-end data availability guarantees.
- **Quality SLAs:** Enforcing semantic and structural integrity at scale.
- **Cost Allocation & FinOps:** Granular cost attribution across business units, workloads, and compute models.
- **AI Traceability:** Full auditability of model outputs, token consumption, agent decisions, and safety guardrails.

Snowflake provides a comprehensive, native stack of observability tools. However, many enterprise features remain underutilized or siloed. This technical documentation defines a **Three-Level Observability Maturity Framework** that systematically maps Snowflake’s native capabilities—such as Event Tables, Data Metric Functions (DMFs), System Alerts, Task DAGs, Streams, and Cortex AI Observability—into an actionable operational paradigm.

```text
+-----------------------------------------------------------------------------------+
|                         SNOWFLAKE OBSERVABILITY FRAMEWORK                         |
+-----------------------------------------------------------------------------------+
| LEVEL 3: AI OBSERVABILITY & TRACEABILITY                                          |
| - Token & Credit Billing (CORTEX_AI_FUNCTIONS_USAGE_HISTORY)                      |
| - Span-Level Agent Tracing & Tool Auditing (AI_OBSERVABILITY_EVENTS)              |
| - Safety & Injection Detection (CORTEX_AI_GUARDRAILS_USAGE_HISTORY)               |
| - Classification Drift & Semantic Shift Tracking                                  |
+-----------------------------------------------------------------------------------+
| LEVEL 2: DATA QUALITY ENFORCEMENT & FINOPS COST ATTRIBUTION                      |
| - Native System & Custom Data Metric Functions (DMFs) vs dbt/Great Expectations   |
| - Full Taxonomy: Accuracy, Uniqueness, Volume, Freshness, Statistics              |
| - Granular Failed-Row Extraction via SYSTEM$DATA_METRIC_SCAN                     |
| - Automated Anomaly Detection over DMF Historical Records                         |
| - Compute & Serverless FinOps (Warehouse Metering, DMF Costs, Resource Monitors)  |
+-----------------------------------------------------------------------------------+
| LEVEL 1: PIPELINE HEALTH & RELIABILITY FOUNDATIONS                                |
| - OpenTelemetry-compliant Event Tables (Telemetry & Logs)                         |
| - CDC Lag Tracking via Streams & SYSTEM$STREAM_HAS_DATA()                         |
| - Orchestrated Task DAGs & Blast-Radius Containment                               |
| - Native System Alerts & Multi-Channel Webhook Notifications                      |
+-----------------------------------------------------------------------------------+

```

---

## Monitoring vs. Observability: Key Differences

Understanding the operational boundary between monitoring and observability is critical for enterprise data architecture:

| Dimension              | Pipeline Monitoring (Legacy)                            | Data & AI Observability (Modern Native)                                      |
| ---------------------- | ------------------------------------------------------- | ---------------------------------------------------------------------------- |
| **Primary Scope**      | Binary state checking (_"Did job X succeed or fail?"_)  | State, data context, cause, and downstream impact analysis.                  |
| **Failure Scope**      | Isolated task level.                                    | Full lineage, pipeline DAG topology, and blast radius.                       |
| **Data Integrity**     | Manual queries or downstream error reports from users.  | Continuous, automated metric tracking (Freshness, Nulls, Duplicates, Drift). |
| **Cost Visibility**    | Monthly macro-level account invoices.                   | Real-time FinOps down to query tags, serverless DMFs, and LLM token usage.   |
| **Resolution Pathway** | Manual log inspection across disparate log aggregators. | Native OpenTelemetry event spans, stream lag metrics, and row-level scans.   |

---

## Level 1: Pipeline Health & Execution Reliability

### Core Capabilities & Architecture

Level 1 establishes basic pipeline health guarantees. It addresses: _Did jobs run successfully, on time, and within expected durations?_

1. **Event Tables (OpenTelemetry Native):** When linked to an account or database, Snowflake automatically captures unstructured logs, unhandled exceptions, and telemetry spans from Tasks, Stored Procedures, and UDFs. Telemetry events adhere to OpenTelemetry standard schemas.
2. **Streams & Change Data Capture (CDC):** Streams act as continuous delta tracking mechanisms and pipeline lag indicators. Paired with `SYSTEM$STREAM_HAS_DATA()`, pipelines execute conditionally, eliminating idle polling costs.
3. **Task Directed Acyclic Graphs (DAGs):** Structural task dependencies prevent cascading failures; if a parent task fails, downstream child tasks are automatically skipped.
4. **Native Alerts:** Near-real-time event monitors evaluate underlying event tables or operational views, triggering immediate multi-channel notifications (Slack, PagerDuty, Webhooks, Email).

### Operational Definition of "Good" at Level 1

- Every pipeline failure is detected and alerted within **5 minutes**.
- Downstream execution is blocked automatically upon parent failure.
- Root-cause telemetry (query ID, error stack) is logged and notified with zero external software agents.

### Complete Implementation Script (Level 1)

```sql
-- ============================================================================
-- LEVEL 1: PIPELINE HEALTH IMPLEMENTATION
-- Setup telemetry, event tables, conditional task execution, and alerts.
-- ============================================================================

CREATE DATABASE IF NOT EXISTS OBS_DEMO_DB;
CREATE SCHEMA IF NOT EXISTS OBS_DEMO_DB.PIPELINE_HEALTH;
USE SCHEMA OBS_DEMO_DB.PIPELINE_HEALTH;

-- 1. Create and Configure Account/Database Event Table
CREATE EVENT TABLE IF NOT EXISTS OBS_DEMO_DB.PIPELINE_HEALTH.ACCOUNT_EVENTS;
ALTER DATABASE OBS_DEMO_DB SET EVENT_TABLE = OBS_DEMO_DB.PIPELINE_HEALTH.ACCOUNT_EVENTS;
ALTER DATABASE OBS_DEMO_DB SET LOG_LEVEL = INFO;
ALTER DATABASE OBS_DEMO_DB SET TRACE_LEVEL = ALWAYS;

-- 2. Target Tables and CDC Stream
CREATE OR REPLACE TABLE RAW_TRANSACTIONS (
    TRANSACTION_ID STRING,
    CUSTOMER_ID STRING,
    AMOUNT NUMBER(12, 2),
    TRANSACTION_TS TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE STAGED_TRANSACTIONS (
    TRANSACTION_ID STRING,
    CUSTOMER_ID STRING,
    AMOUNT NUMBER(12, 2),
    LOAD_TS TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE ANALYTICS_SUMMARY (
    SUMMARY_DATE DATE,
    TOTAL_TX_COUNT NUMBER,
    TOTAL_AMOUNT NUMBER(15, 2)
);

CREATE OR REPLACE STREAM RAW_TX_STREAM ON TABLE RAW_TRANSACTIONS;

-- 3. Stored Procedure with Native Logging & Tracing
CREATE OR REPLACE PROCEDURE PROCESS_TRANSACTIONS_PROC()
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('snowflake-telemetry-python')
HANDLER = 'run'
AS
$$
import logging
from snowflake import telemetry

def run(session):
    logging.info("Starting ingestion processing for RAW_TX_STREAM...")

    # Add custom OpenTelemetry trace attribute
    telemetry.set_span_attribute("pipeline.stage", "staging_ingestion")

    try:
        query = "INSERT INTO STAGED_TRANSACTIONS SELECT TRANSACTION_ID, CUSTOMER_ID, AMOUNT, CURRENT_TIMESTAMP() FROM RAW_TX_STREAM;"
        res = session.sql(query).collect()
        logging.info("Successfully staged delta rows.")
        return "SUCCESS"
    except Exception as e:
        logging.error(f"Error encountered during staging execution: {str(e)}")
        telemetry.add_event("pipeline_failure", {"error.message": str(e)})
        raise e
$$;

-- 4. Orchestrated Task DAG with Conditional Execution
CREATE OR REPLACE TASK ROOT_INGEST_TASK
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('RAW_TX_STREAM')
AS
    CALL PROCESS_TRANSACTIONS_PROC();

CREATE OR REPLACE TASK CHILD_ANALYTICS_TASK
    WAREHOUSE = COMPUTE_WH
    AFTER ROOT_INGEST_TASK
AS
    INSERT INTO OBS_DEMO_DB.PIPELINE_HEALTH.ANALYTICS_SUMMARY
    SELECT DATE(LOAD_TS), COUNT(*), SUM(AMOUNT)
    FROM STAGED_TRANSACTIONS
    GROUP BY 1;

ALTER TASK CHILD_ANALYTICS_TASK RESUME;
ALTER TASK ROOT_INGEST_TASK RESUME;

-- 5. System Alert for Task Failures via Event Table Monitoring
CREATE OR REPLACE ALERT ALERT_TASK_FAILURE
    WAREHOUSE = COMPUTE_WH
    SCHEDULE = '2 MINUTE'
    IF (
        EXISTS (
            SELECT 1
            FROM OBS_DEMO_DB.PIPELINE_HEALTH.ACCOUNT_EVENTS
            WHERE RECORD_TYPE = 'log'
              AND VALUE:severity::STRING IN ('ERROR', 'FATAL')
              AND TIMESTAMP >= DATEADD('minute', -5, CURRENT_TIMESTAMP())
        )
    )
    THEN
        CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
            'Pipeline execution failed! Check event table for details.',
            'text/plain'
        );

ALTER ALERT ALERT_TASK_FAILURE RESUME;

```

---

## Level 2: Data Quality Enforcement & FinOps Cost Attribution

### Core Capabilities & Architecture

Level 2 upgrades monitoring from structural pipeline execution to semantic data correctness and FinOps cost transparency: _Is the data correct, complete, and fresh? And who is paying for it?_

#### 1. Data Metric Functions (DMFs) vs. External Tools

Data Metric Functions are Snowflake’s native alternative to external data quality frameworks like **dbt tests** or **Great Expectations**. Unlike third-party tools that require dedicated orchestration servers or compute instances:

- **Serverless Execution:** DMFs operate independently of user-managed virtual warehouses, running on Snowflake's serverless compute layer.
- **Declarative Schedules:** Execution intervals are specified directly on table objects.
- **Historical Quality Record:** Outputs persist automatically into system views, creating an audit record for data quality tracking over time.

#### 2. Native System DMF Taxonomy

Snowflake provides built-in system DMFs covering common data quality dimensions:

| Quality Dimension           | System Data Metric Functions (DMFs)               | Operational Purpose                                                              |
| --------------------------- | ------------------------------------------------- | -------------------------------------------------------------------------------- |
| **Accuracy / Completeness** | `NULL_COUNT`, `BLANK_COUNT`, `INVALID_JSON_COUNT` | Detects missing values, whitespace-only strings, or malformed JSON payloads.     |
| **Uniqueness**              | `DUPLICATE_COUNT`, `UNIQUE_COUNT`                 | Ensures key integrity and flags row duplication.                                 |
| **Volume**                  | `ROW_COUNT`                                       | Tracks table growth trends and flags unexpected bulk deletions or empty loads.   |
| **Freshness & Schema**      | `FRESHNESS`, `SCHEMA_CHANGE_COUNT`                | Verifies SLA compliance on load times and tracks structural DDL modifications.   |
| **Statistics**              | `AVG`, `MEDIAN`, `STDDEV`, `APPROX_QUANTILE`      | Monitors numeric distributions to detect unexpected metric variance or outliers. |

#### 3. Custom DMFs, Row Extraction & Anomaly Detection

- **Custom DMFs:** Used to encode domain business rules (e.g., enforcing financial threshold limits or regulatory pattern formats).
- **Granular Failure Extraction (`SYSTEM$DATA_METRIC_SCAN`):** Returns the specific failing rows rather than aggregate metrics, allowing teams to locate and fix bad records directly.
- **Automated Anomaly Detection:** Uses historical DMF metric records to detect when a value deviates from predicted ranges—flagging anomalies even if they fall within broad static thresholds.

#### 4. FinOps Cost Attribution Matrix

- **`QUERY_TAG`:** Session-level tags that flow into query history, enabling cost allocation by team, project, or workflow.
- **`WAREHOUSE_METERING_HISTORY`:** Measures virtual warehouse credit consumption over time.
- **`DATA_QUALITY_MONITORING_USAGE_HISTORY`:** Tracks serverless compute consumed specifically by DMF quality checks.
- **Resource Monitors:** Hard guardrails that suspend warehouses when budgets are exceeded.

### Operational Definition of "Good" at Level 2

- Every critical table has freshness and completeness checks attached.
- Quality violations trigger automated alerts within **5 minutes** of DML execution.
- Failed records are extractable for triage via automated scripts.
- Every team knows their monthly compute and data quality monitoring spend.

### Complete Implementation Script (Level 2)

```sql
-- ============================================================================
-- LEVEL 2: DATA QUALITY & FINOPS IMPLEMENTATION
-- Setup System & Custom DMFs, failed-row extraction, and FinOps queries.
-- ============================================================================

USE SCHEMA OBS_DEMO_DB.PIPELINE_HEALTH;

-- 1. Create Data Table & Set Query Tags for Cost Allocation
ALTER SESSION SET QUERY_TAG = '{"team": "finance", "project": "revenue_recon"}';

CREATE OR REPLACE TABLE FINANCIAL_RECORDS (
    RECORD_ID STRING PRIMARY KEY,
    ACCOUNT_NUMBER STRING,
    PAYLOAD_JSON STRING,
    TRANSACTION_AMOUNT NUMBER(15, 2),
    LAST_UPDATED TIMESTAMP_NTZ
);

-- 2. Create Custom Data Metric Function (DMF) for Domain Validation
CREATE OR REPLACE DATA METRIC FUNCTION DMF_NEGATIVE_AMOUNT_COUNT(
    ARG_TABLE TABLE(TRANSACTION_AMOUNT NUMBER(15, 2))
)
RETURNS NUMBER
AS
$$
    SELECT COUNT(*)
    FROM ARG_TABLE
    WHERE TRANSACTION_AMOUNT < 0
$$;

-- 3. Attach System & Custom DMFs to Table
ALTER TABLE FINANCIAL_RECORDS SET DATA_METRIC_SCHEDULE = '15 MINUTE';

ALTER TABLE FINANCIAL_RECORDS ADD DATA METRIC FUNCTION
    -- Accuracy / Completeness
    SNOWFLAKE.CORE.NULL_COUNT ON (ACCOUNT_NUMBER),
    SNOWFLAKE.CORE.BLANK_COUNT ON (ACCOUNT_NUMBER),
    SNOWFLAKE.CORE.INVALID_JSON_COUNT ON (PAYLOAD_JSON),
    -- Uniqueness & Volume
    SNOWFLAKE.CORE.DUPLICATE_COUNT ON (RECORD_ID),
    SNOWFLAKE.CORE.ROW_COUNT ON (),
    -- Freshness & Schema
    SNOWFLAKE.CORE.FRESHNESS ON (LAST_UPDATED),
    -- Custom Business Rule
    DMF_NEGATIVE_AMOUNT_COUNT ON (TRANSACTION_AMOUNT);

-- 4. Query Historical Data Quality Results
SELECT
    SCHEDULED_TIME,
    MEASUREMENT_TIME,
    METRIC_NAME,
    VALUE
FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.DATA_METRIC_FUNCTION_REFERENCES(
    REF_ENTITY_NAME => 'OBS_DEMO_DB.PIPELINE_HEALTH.FINANCIAL_RECORDS',
    REF_ENTITY_DOMAIN => 'TABLE'
))
ORDER BY MEASUREMENT_TIME DESC;

-- 5. FinOps Cost Attribution Dashboards (Unified Account Consumption)
-- Virtual Warehouse Usage by Tag
SELECT
    H.WAREHOUSE_NAME,
    PARSE_JSON(H.QUERY_TAG):team::STRING AS TEAM,
    SUM(H.CREDITS_USED_CLOUD_SERVICES + H.EXECUTION_TIME) AS APPROX_COST_SCORE
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY H
WHERE H.START_TIME >= DATEADD('day', -30, CURRENT_TIMESTAMP())
GROUP BY 1, 2;

-- Data Metric Monitoring Serverless Cost
SELECT
    START_TIME,
    END_TIME,
    ENTITY_NAME,
    CREDITS_USED
FROM SNOWFLAKE.ACCOUNT_USAGE.DATA_QUALITY_MONITORING_USAGE_HISTORY
WHERE START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC;

```

---

## Level 3: AI Observability & Governance

### Core Capabilities & Architecture

Level 3 addresses modern Generative AI and LLM workloads: _What did the AI decide, why, at what cost, and is it still performing as expected?_

1. **Token-Level Billing Granularity (`CORTEX_AI_FUNCTIONS_USAGE_HISTORY`):** Tracks function calls (`AI_CLASSIFY`, `AI_COMPLETE`, `AI_EXTRACT`), model used, query ID, user, token counts (input/output), and exact credit consumption with max 5-minute latency.
2. **Span-Level Agent Tracing (`AI_OBSERVABILITY_EVENTS` via `SNOWFLAKE.LOCAL`):** OpenTelemetry-native execution traces for Cortex Agents and Cortex Code. Captures LLM planning decisions, tool selection and execution (Cortex Search, Cortex Analyst, SQL execution), response generation, and user feedback (thumbs up/down).
3. **Prompt Safety & Injection Guardrails (`CORTEX_AI_GUARDRAILS_USAGE_HISTORY`):** Monitors the prompt injection detection layer. Every request to a Cortex Agent is scanned, recording flagged requests, tool types involved, and security signals for auditing.
4. **Model Drift Detection:** Implemented pattern on top of inference logging by tracking classification distributions over time (e.g., ratio of positive/negative/neutral sentiment labels week-over-week) to detect data drift, concept drift, or model degradation.

### Operational Definition of "Good" at Level 3

- Every AI function call is logged with inputs, outputs, and token consumption.
- Token spend is bounded by budget guardrails and automated alert thresholds.
- Classification distribution is tracked weekly with automated drift alerts.
- Guardrail activations are routinely reviewed by security teams.

### Complete Implementation Script (Level 3)

```sql
-- ============================================================================
-- LEVEL 3: AI OBSERVABILITY IMPLEMENTATION
-- Log LLM classification outputs, monitor token usage, and detect drift.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS OBS_DEMO_DB.AI_OBSERVABILITY;
USE SCHEMA OBS_DEMO_DB.AI_OBSERVABILITY;

-- 1. Table for Inbound Unstructured Feedback & Classification
CREATE OR REPLACE TABLE CUSTOMER_FEEDBACK (
    FEEDBACK_ID STRING,
    FEEDBACK_TEXT STRING,
    CLASSIFIED_SENTIMENT STRING,
    PROCESSED_TS TIMESTAMP_NTZ
);

-- 2. Process Feedback using Snowflake Cortex AI & Capture Metadata
INSERT INTO CUSTOMER_FEEDBACK (FEEDBACK_ID, FEEDBACK_TEXT, CLASSIFIED_SENTIMENT, PROCESSED_TS)
SELECT
    'FB_101',
    'The product speed is incredible, but customer support response was slightly delayed.',
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT('The product speed is incredible, but customer support response was slightly delayed.', ['Positive', 'Negative', 'Neutral']):label::STRING,
    CURRENT_TIMESTAMP();

-- 3. Token & Credit Usage Audit across Cortex AI Functions
SELECT
    START_TIME,
    FUNCTION_NAME,
    MODEL_NAME,
    USER_NAME,
    TOKEN_COUNT_INPUT,
    TOKEN_COUNT_OUTPUT,
    CREDITS_USED
FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY
WHERE START_TIME >= DATEADD('day', -7, CURRENT_TIMESTAMP())
ORDER BY START_TIME DESC;

-- 4. LLM Classification Drift Tracking Query (Weekly Distribution Analysis)
WITH WEEKLY_DISTRIBUTION AS (
    SELECT
        DATE_TRUNC('week', PROCESSED_TS) AS EVAL_WEEK,
        CLASSIFIED_SENTIMENT,
        COUNT(*) AS TOTAL_COUNT,
        RATIO_TO_REPORT(COUNT(*)) OVER (PARTITION BY DATE_TRUNC('week', PROCESSED_TS)) AS PROPORTION
    FROM CUSTOMER_FEEDBACK
    GROUP BY 1, 2
)
SELECT
    CURRENT_WEEK.EVAL_WEEK AS CURRENT_WEEK,
    PREV_WEEK.EVAL_WEEK AS PRIOR_WEEK,
    CURRENT_WEEK.CLASSIFIED_SENTIMENT,
    CURRENT_WEEK.PROPORTION AS CURRENT_PROP,
    PREV_WEEK.PROPORTION AS PRIOR_PROP,
    ABS(CURRENT_WEEK.PROPORTION - COALESCE(PREV_WEEK.PROPORTION, 0)) AS DRIFT_DELTA
FROM WEEKLY_DISTRIBUTION CURRENT_WEEK
LEFT JOIN WEEKLY_DISTRIBUTION PREV_WEEK
    ON CURRENT_WEEK.CLASSIFIED_SENTIMENT = PREV_WEEK.CLASSIFIED_SENTIMENT
   AND PREV_WEEK.EVAL_WEEK = DATEADD('week', -1, CURRENT_WEEK.EVAL_WEEK)
WHERE CURRENT_WEEK.EVAL_WEEK = DATE_TRUNC('week', CURRENT_DATE());

```

---

## Progression Roadmap & Implementation Strategy

Organizations should implement this maturity model incrementally:

```text
[Level 1: Pipeline Health] ---> [Level 2: Data Quality & FinOps] ---> [Level 3: AI Observability]
 (Fix Pipeline Mechanics)       (Ensure Data Trust & Costs)          (Govern Generative AI)

```

1. **Level 1 to Start:** Poor pipelines render downstream quality checks useless (as they assess outdated data) and make AI lineage irrelevant (since jobs fail before execution).
2. **Level 2 Next:** Quality control establishes the baseline trust needed before expanding into AI functionality. Flaws in underlying data carry forward into AI models trained or prompted with that data.
3. **Level 3 at AI Deployment:** Deploy full AI observability when moving LLM applications to production, avoiding unnecessary compute and engineering costs prior to deployment.

---

## Competitive Differentiation: Why Native Snowflake Observability Wins

- **Unified Schema & Telemetry:** Event tables store pipeline telemetry, UDF performance metrics, and AI agent traces within the same native schema.
- **Single Alerting Engine:** System alerts respond to task failures, data quality violations, and AI budget threshold breaches using a uniform mechanism.
- **Integrated FinOps:** `ACCOUNT_USAGE` views offer an integrated billing system across virtual warehouses, storage, serverless DMF execution, and AI tokens.
- **Serverless Isolation:** Serverless execution for DMFs, tasks, and alerts ensures observability workloads do not compete with production compute.
- **Zero Third-Party Overhead:** Eliminates the typical anti-pattern of maintaining a separate observability platform with distinct security guarantees, access control systems, and pricing models.

---

## Deployment Repository & Resources

A complete reference implementation containing working SQL scripts and dashboards is available on GitHub:
