# Scenario 10: Lakehouse FinOps & Automated Cost Governance

This design establishes an automated **FinOps and Cost Governance Platform** spanning a multi-cloud data infrastructure (**AWS, Databricks, and Snowflake**). It replaces retrospective, end-of-month cloud billing surprises with **near-real-time telemetry ingestion**, automated **runaway compute circuit breakers**, and granular, tag-based business cost attribution.

---

## 1. System Requirements & FinOps KPIs

- **Granular Attribution:** $100\%$ of cloud spend mapped to specific cost centers (Engineering Domains, Business Units, Client Tiers).
- **Automated Circuit Breaking:**
- Detect and terminate rogue/unbounded queries in Snowflake ($> 45\text{ mins}$ or $> \$100$ estimated cost).
- Auto-terminate idle Databricks clusters ($< 15\text{ mins}$ idle) and block non-spot on-demand scale-out during dev hours.

- **Storage Tiering Optimization:** Automate lifecycle transitions across S3 tiers (Standard $\to$ Infrequent Access $\to$ Glacier Instant Retrieval) based on access heatmaps, targeting a $35\text{--}50\%$ storage bill reduction.
- **Data Freshness SLA:** FinOps telemetry visible in analytical dashboards within **$< 2\text{ hours}$** of execution.

---

## 2. End-to-End FinOps Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│ TELEMETRY INGESTION                                                                             │
│                                                                                                 │
│  [ AWS Billing ]                  [ Databricks System Tables ]        [ Snowflake Account ]     │
│   - Cost & Usage Report (CUR 2.0)  - system.billing.usage              - ACCOUNT_USAGE views    │
│   - Hourly Parquet dump to S3      - DBU consumption & job metadata    - QUERY_HISTORY, METERING│
└───────────────┬─────────────────────────────────┬─────────────────────────────────┬─────────────┘
                │                                 │                                 │
                ▼                                 ▼                                 ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│ FINOPS DATA LAKEHOUSE (Amazon S3 + AWS Glue Catalog)                                            │
│                                                                                                 │
│   s3://finops-lakehouse/cur_raw/       s3://finops-lakehouse/databricks/  s3://finops-lakehouse/sf/ │
└───────────────────────────────────────────────┬─────────────────────────────────────────────────┘
                                                │
                                                ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│ PROCESSING & NORMALIZATION LAYER (Amazon Athena + AWS Glue / MWAA)                              │
│                                                                                                 │
│   - Unifies diverse billing units (AWS $, Databricks DBUs, Snowflake Credits)                   │
│   - Normalizes schema to FOCUS (FinOps Open Cost and Usage Specification) standard               │
│   - Attaches corporate cost-center tags & metadata catalogs                                     │
└───────────────────────────────────────────────┬─────────────────────────────────────────────────┘
                                                │
                                                ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│ ACTIVE ENFORCEMENT & OBSERVABILITY                                                              │
│                                                                                                 │
│   ┌───────────────────────────────────┐               ┌─────────────────────────────────────┐   │
│   │ Event-Driven Automation (Lambda)  │               │ FinOps Analytical Dashboards        │   │
│   │ - Kills runaway Snowflake queries │               │ - Amazon Athena / QuickSight / BI   │   │
│   │ - Shuts down idle Spark clusters  │               │ - Unit economics ($ per ETL pipeline)│   │
│   │ - Enforces S3 Lifecycle rules     │               │ - Real-time budget burn tracking    │   │
│   └───────────────────────────────────┘               └─────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘

```

---

## 3. Telemetry Ingestion & Cost Normalization (The FOCUS Model)

Cloud vendors quantify consumption using incompatible metrics: AWS bills in USD, Databricks bills in Databricks Units (DBUs), and Snowflake bills in Compute Credits.

To evaluate total cost of ownership (TCO), the platform ingests raw logs and normalizes them into the **FinOps Open Cost and Usage Specification (FOCUS)** standard.

### Core Telemetry Streams

1. **AWS Cost and Usage Report (CUR 2.0):** Emitted hourly into S3 as Snappy-compressed Parquet, partitioned by billing period.
2. **Databricks System Tables (`system.billing.usage`):** Captures cluster IDs, workspace IDs, SKU names, run IDs, and DBU counts.
3. **Snowflake Account Usage:** Exported to S3 stage via a lightweight scheduled task querying `SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY` and `QUERY_HISTORY`.

### Unified FOCUS Normalization Query (Athena SQL)

```sql
CREATE OR REPLACE VIEW finops_lakehouse.curated.focus_cost_and_usage AS

-- 1. Snowflake Compute & Storage Cost (Converted from Credits to USD)
SELECT
    DATE_TRUNC('hour', start_time) AS charge_period_start,
    'Snowflake' AS provider_name,
    'Enterprise Data Warehouse' AS service_category,
    warehouse_name AS resource_id,
    COALESCE(tag_cost_center, 'Unallocated') AS cost_center,
    'Compute' AS charge_subcategory,
    credits_used AS consumed_quantity,
    'Credits' AS consumed_unit,
    credits_used * 3.00 AS effective_cost_usd -- Assuming contract rate of $3.00/credit
FROM finops_lakehouse.raw.snowflake_metering_history

UNION ALL

-- 2. Databricks DBU Consumption
SELECT
    DATE_TRUNC('hour', usage_start_time) AS charge_period_start,
    'Databricks' AS provider_name,
    'Lakehouse Spark Compute' AS service_category,
    cluster_id AS resource_id,
    COALESCE(custom_tags['CostCenter'], 'Unallocated') AS cost_center,
    sku_name AS charge_subcategory,
    dbus AS consumed_quantity,
    'DBUs' AS consumed_unit,
    dbus * 0.40 AS effective_cost_usd -- Contractual blended rate per DBU
FROM finops_lakehouse.raw.databricks_billing_usage

UNION ALL

-- 3. Raw AWS Infrastructure (Compute, S3, Data Transfer)
SELECT
    line_item_usage_start_date AS charge_period_start,
    'AWS' AS provider_name,
    line_item_product_code AS service_category,
    line_item_resource_id AS resource_id,
    COALESCE(resource_tags_user_cost_center, 'Unallocated') AS cost_center,
    line_item_operation AS charge_subcategory,
    line_item_usage_amount AS consumed_quantity,
    pricing_unit AS consumed_unit,
    line_item_unblended_cost AS effective_cost_usd
FROM finops_lakehouse.raw.aws_cur_reports
WHERE line_item_unblended_cost > 0.0;

```

---

## 4. Automated Circuit Breakers & Resource Governance

Reactive weekly reports do not stop a malformed cartesian product query from burning tens of thousands of dollars overnight. Active programmatic guards prevent cost overruns.

### A. Snowflake Runaway Query Guard Dog (Python Lambda)

A Lambda function runs on an EventBridge schedule every **10 minutes**, querying operational metadata for queries exceeding time thresholds or memory limits, and systematically aborting them.

````python
import os
import snowflake.connector
import urllib3
import json

http = urllib3.PoolManager()
SLACK_WEBHOOK = os.environ['SLACK_FINOPS_ALERTS']

def lambda_handler(event, context):
    ctx = snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        warehouse='FINOPS_MONITOR_WH'
    )
    cs = ctx.cursor()

    # Identify queries active > 45 minutes on Ad-Hoc/Analytics Warehouses
    query = """
    SELECT
        query_id,
        user_name,
        warehouse_name,
        execution_status,
        DATEDIFF('minute', start_time, CURRENT_TIMESTAMP()) AS duration_minutes,
        query_text
    FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_WAREHOUSE(
        WAREHOUSE_NAME => 'ANALYTICS_ADHOC_WH',
        END_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
    ))
    WHERE execution_status = 'RUNNING'
      AND duration_minutes > 45;
    """

    cs.execute(query)
    rogue_queries = cs.fetchall()

    for row in rogue_queries:
        q_id, user, wh, _, duration, text = row

        # Abort the runaway execution
        cs.execute(f"SELECT SYSTEM$CANCEL_QUERY('{q_id}');")

        # Dispatch instant incident notification to FinOps channel
        alert_payload = {
            "text": f":warning: *FinOps Circuit Breaker: Runaway Query Aborted* :warning:\n"
                    f"*Query ID:* `{q_id}` | *User:* `{user}` | *Warehouse:* `{wh}`\n"
                    f"*Duration:* `{duration} mins` (Limit: 45 mins)\n"
                    f"*SQL Prefix:* ```{text[:200]}...```"
        }
        http.request('POST', SLACK_WEBHOOK, body=json.dumps(alert_payload), headers={'Content-Type': 'application/json'})

    cs.close()
    ctx.close()
    return {"status": "SUCCESS", "killed_count": len(rogue_queries)}

````

### B. Snowflake Autonomous Suspension Policies

Prevent idle warehouse billing by aggressively lowering the auto-suspend window on ad-hoc and ETL clusters:

```sql
-- Production ETL: 1 minute idle shutdown (resumes instantly on task execution)
ALTER WAREHOUSE etl_loading_wh SET
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  STATEMENT_TIMEOUT_IN_SECONDS = 3600; -- Hard cap at 1 hour

-- Ad-hoc BI / Data Analyst: 2 minutes idle shutdown
ALTER WAREHOUSE analytics_adhoc_wh SET
  AUTO_SUSPEND = 120
  AUTO_RESUME = TRUE
  STATEMENT_TIMEOUT_IN_SECONDS = 2700; -- Hard cap at 45 mins

```

### C. Databricks Spot Orchestration & Auto-Termination

To prevent development clusters from running through weekends:

- **Cluster Policies:** Enforce strict policy constraints on non-production workspaces via Unity Catalog:
- `autotermination_minutes`: Fixed value of `15`.
- `spark_version`: Limited to LTS runtimes.
- `azure_attributes.spot_instances` or `aws_attributes.spot_bid_price_percent`: Mandatory `100%` Spot/Preemptible instance usage for non-driver worker nodes. Drivers remain On-Demand to prevent cluster evictions on spot loss.

---

## 5. Storage Tiering & S3 Lifecycle Automation

Storage cost growth in Lakehouses is primarily driven by old data remaining in `S3 Standard` long after query frequency drops.

```
Access Heatmap by Age:
[ Day 0 - 30 ]  ────────────────► Standard Tier ($0.023 / GB)  [Daily active ETL & Dashboards]
[ Day 31 - 90 ] ────────────────► Infrequent Access ($0.0125 / GB)  [Weekly backfills & audits]
[ Day 91 - 365 ] ───────────────► Glacier Instant Retrieval ($0.004 / GB)  [Rare ad-hoc queries]
[ Day 366+ ]    ────────────────► Glacier Deep Archive ($0.00099 / GB)  [Regulatory compliance]

```

### Automated Lifecycle Ruleset (Terraform / S3 Policy)

```json
{
  "Rules": [
    {
      "ID": "LakehouseS3TieringLifecycle",
      "Status": "Enabled",
      "Filter": {
        "Prefix": "lakehouse/curated/"
      },
      "Transitions": [
        {
          "Days": 30,
          "StorageClass": "STANDARD_IA"
        },
        {
          "Days": 90,
          "StorageClass": "GLACIER_IR"
        },
        {
          "Days": 365,
          "StorageClass": "DEEP_ARCHIVE"
        }
      ],
      "NoncurrentVersionTransitions": [
        {
          "NoncurrentDays": 7,
          "StorageClass": "GLACIER_IR"
        }
      ],
      "NoncurrentVersionExpiration": {
        "NoncurrentDays": 30
      },
      "AbortIncompleteMultipartUpload": {
        "DaysAfterInitiation": 2
      }
    }
  ]
}
```

- **Important Lakehouse Guardrail:** Avoid applying lifecycle transitions to the `_delta_log/` or `metadata/` folders. Parquet data files can transition down to `STANDARD_IA` or `GLACIER_IR`, but transaction logs must remain in `S3 Standard` to avoid high transition and read-request fees during schema evaluation and checkpoint parsing.

---

## 6. Unit Economics: Calculating Cost per Pipeline Run

FinOps maturity relies on understanding unit cost economics: e.g., _"How much does the daily SCD2 Customer Dimension update cost per customer record?"_

### Pipeline Unit Cost Formula

$$\text{Cost Per Record} = \frac{\text{MWAA DAG Execution Cost} + \text{Databricks Worker DBUs/Compute} + \text{Snowflake Merge Credits}}{\text{Total Rows Successfully Ingested}}$$

```sql
-- Athena Unit Economics Query
WITH pipeline_spend AS (
    SELECT
        DATE(charge_period_start) AS run_date,
        cost_center,
        resource_id,
        SUM(effective_cost_usd) AS daily_compute_cost
    FROM finops_lakehouse.curated.focus_cost_and_usage
    WHERE resource_id IN ('scd2_customer_cluster', 'CUSTOMER_DW_WH')
    GROUP BY 1, 2, 3
),
pipeline_volume AS (
    SELECT
        CAST(_ingested_at AS DATE) AS run_date,
        COUNT(*) AS rows_processed
    FROM "enterprise_dw"."gold"."dim_customer"
    GROUP BY 1
)
SELECT
    s.run_date,
    s.cost_center,
    s.daily_compute_cost,
    v.rows_processed,
    (s.daily_compute_cost / NULLIF(v.rows_processed, 0)) * 1000 AS cost_per_1k_records_usd
FROM pipeline_spend s
JOIN pipeline_volume v ON s.run_date = v.run_date
ORDER BY s.run_date DESC;

```

---

## 7. Failure Modes & Edge Case Matrix

| FinOps Risk Vector                       | Real-World Impact                                                                                                                                                                                     | Engineering Mitigation                                                                                                                                                                                                                                                  |
| ---------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Glacier Retrieval Shock**              | An analyst runs an unpruned full-scan query targeting an Iceberg table whose older Parquet files transitioned to Glacier Deep Archive, incurring high retrieval fees and failing with timeout errors. | Tables backed by archive tiers must be registered as **Archive Tables** with partition pruning enforced. Queries attempting to scan historical partitions $> 90\text{ days}$ must specify an explicit date predicate or route through an asynchronous restore pipeline. |
| **Tagged Cost Drift**                    | Engineering teams provision clusters or S3 buckets without the required `CostCenter` or `Environment` tags, landing spend in the `Unallocated` bucket.                                                | Enforce **AWS SCPs (Service Control Policies)** and **Databricks Cluster Policies** that reject resource creation calls missing mandatory tags: `CostCenter`, `Owner`, `Environment`.                                                                                   |
| **Snowflake Warehouse Over-Scaling**     | An engineer spins up an `X-Large` warehouse ($16\text{ credits/hour}$) for a quick test and forgets to scale down or suspend it.                                                                      | Implement an automated Lambda monitor inspecting `WAREHOUSES` view metadata. Warehouses sized $> \text{Medium}$ running continuously $> 2\text{ hours}$ trigger Slack notifications and an automated downscale via `ALTER WAREHOUSE ... SET WAREHOUSE_SIZE = 'SMALL'`.  |
| **S3 Incomplete Multipart Upload Leaks** | Failed large-file Spark/Glue ingestion attempts leave multi-gigabyte uncommitted binary parts on S3, continuing to incur standard storage charges invisibly.                                          | Enforce an S3 Lifecycle Rule globally: `AbortIncompleteMultipartUpload = 2 days`. AWS automatically purges orphaned chunks after 48 hours.                                                                                                                              |

---

### Complete Data Engineering System Design Portfolio (10/10)

1. **Legacy Informatica to Databricks/Snowflake Migration** (Architecture, CDC, Dual-Run Reconciliation)
2. **Serverless Lakehouse Ingestion** (S3, Lambda, Glue, Athena, Small File Compaction)
3. **Multi-Engine Lakehouse Coexistence** (Databricks, Snowflake Iceberg Tables, Delta UniForm, MWAA)
4. **Cross-Cloud Incremental CDC Pipeline** (Azure to Snowflake on AWS, Snowpipe, Streams & Tasks)
5. **Automated Data Quality & Quarantine Pipeline** (Auto Loader, DLT, Great Expectations, Athena, Lambda)
6. **GDPR/CCPA "Right to be Forgotten" at Scale** (Delta Deletion Vectors, Snowflake Data Masking, WORM Audit)
7. **Multi-Region Disaster Recovery (DR)** (S3 RTC, Snowflake Failover Groups, MWAA Active-Passive, RPO $< 15\text{m}$)
8. **Real-Time Streaming Lakehouse** (Event Hubs/Kinesis, Spark Structured Streaming, Snowpipe Streaming, Dynamic Tables)
9. **Data Mesh Architecture** (Federated Products, Delta Sharing, Central Contracts, MWAA Dependency Sensors)
10. **Lakehouse FinOps & Cost Governance** (FOCUS Schema, Runaway Query Breakers, S3 Tiering, Unit Economics)
