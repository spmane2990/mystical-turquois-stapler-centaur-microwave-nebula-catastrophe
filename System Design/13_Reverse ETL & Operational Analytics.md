# Scenario 13: Reverse ETL & Operational Analytics

This scenario addresses closing the feedback loop between the data warehouse and frontline business operations. High-value data science and business intelligence outputs—such as customer churn risk scores, lead propensity indices, and account health metrics—are computed in **Snowflake** or **Databricks**, but frontline revenue and support teams need them surfaced directly inside SaaS operational systems (**Salesforce CRM**, **HubSpot**, **Zendesk**).

---

## 1. System Requirements & Operational Constraints

- **Throughput & Freshness:** Process $500{,}000+$ mutated entity records daily with a sync latency of **$< 15$ minutes** from warehouse score generation to CRM visibility.
- **Third-Party API Rate Limits:** Operational CRMs enforce strict call quotas (e.g., Salesforce limits standard REST calls to $100{,}000$ per 24-hour rolling window; HubSpot enforces burst rates like $100\text{ req/10 sec}$). Naive unthrottled point-to-point scripts trigger HTTP `429 Too Many Requests` and lead to IP bans.
- **Delta-Only Replication:** Resyncing complete multi-million-row dimensions daily is strictly prohibited; only net-new mutations must be extracted.
- **Idempotency & Replayability:** Network dropped connections or CRM maintenance periods must not duplicate updates or create split-brain customer states.
- **Audit & Visibility Loop:** Downstream sync receipts (`status_code`, `crm_record_id`, `delivered_at`, `error_message`) must be ingested back into the data lake to give analysts visibility into sync health via **Amazon Athena**.

---

## 2. End-to-End Architectural Blueprint

```
[ Snowflake Enterprise DW / Databricks Gold ]
  - Dimension: enterprise_dw.gold.dim_customer_features
  - Stream: customer_features_cdc_stream (Change Detection)
                 │
                 │ (Trigger: Scheduled MWAA / Snowflake Task)
                 ▼
[ Delta Extraction & Formatting Layer ]
  - Extracted Mutations (JSON payload batch)
  - Pre-signed S3 Export: s3://reverse-etl-stage/outbound/
                 │
                 │ (S3 Event Notification / Polling)
                 ▼
[ Decoupling & Buffering: Amazon SQS (FIFO Queue) ]
  - Groups messages by crm_domain (MessageGroupId = "salesforce")
  - Enforces deduplication ID based on hash(user_id, updated_at)
                 │
                 ▼
[ Rate-Limiting Dispatcher: AWS Lambda + Amazon ElastiCache (Redis) ]
  - Sliding Window / Leaky Bucket enforces CRM API ceilings
  - Assembles bulk REST batches (e.g., 200 records per PATCH call)
                 │
                 ├────────────────────────────────────────┐
                 ▼ (Bulk HTTPS PATCH)                     ▼ (Exhausted Retries)
[ Third-Party Operational APIs ]             [ Dead-Letter Queue (SQS DLQ) ]
  - Salesforce Bulk API v2 / REST               - Quarantines unresolvable payloads
  - HubSpot CRM Contacts API                    - Lambda alerts on queue depth
                 │
                 │ (Delivery Logs & HTTP Status Codes)
                 ▼
[ Ingestion Back-Loop: S3 Delivery Audit Zone ]
  s3://reverse-etl-stage/audit_logs/YYYY/MM/DD/*.parquet
                 │
                 ▼
[ Operational Monitoring: AWS Glue Catalog & Amazon Athena ]
  Data Analysts query: "Which customer lead scores failed to push to CRM today?"

```

---

## 3. Warehouse Change Detection (Snowflake Streams)

To avoid heavy full-table comparisons, attach an append-only change-tracking stream to the dimensional table producing the business scores.

### 1. Table & Stream DDL

```sql
-- Target dimensional mart updated by ML/ETL pipelines
CREATE OR REPLACE TABLE enterprise_dw.gold.dim_customer_features (
    account_id VARCHAR(64) PRIMARY KEY,
    crm_lead_id VARCHAR(64),
    churn_risk_score NUMBER(5, 2),
    health_tier VARCHAR(16),
    propensity_to_upsell FLOAT,
    feature_updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Stream to capture changes
CREATE OR REPLACE STREAM enterprise_dw.gold.stream_customer_feature_deltas
ON TABLE enterprise_dw.gold.dim_customer_features;

```

### 2. Delta Export Task with Row Deduplication

If multiple updates touch the same customer within a single batch window, only the latest state should be exported. The extraction query formats rows into CRM-ready JSON strings:

```sql
CREATE OR REPLACE TASK enterprise_dw.gold.task_export_reverse_etl_deltas
WAREHOUSE = 'REVERSE_ETL_WH'
SCHEDULE = '10 MINUTE'
WHEN SYSTEM$STREAM_HAS_DATA('enterprise_dw.gold.stream_customer_feature_deltas')
AS
COPY INTO @stage_reverse_etl_outbound/batch_
FROM (
    WITH ranked_deltas AS (
        SELECT
            account_id,
            crm_lead_id,
            churn_risk_score,
            health_tier,
            propensity_to_upsell,
            feature_updated_at,
            METADATA$ACTION AS delta_action,
            ROW_NUMBER() OVER (
                PARTITION BY account_id
                ORDER BY feature_updated_at DESC
            ) AS rank_idx
        FROM enterprise_dw.gold.stream_customer_feature_deltas
        WHERE crm_lead_id IS NOT NULL -- Exclude records not tied to CRM entities
    )
    SELECT
        OBJECT_CONSTRUCT(
            'external_id', crm_lead_id,
            'attributes', OBJECT_CONSTRUCT(
                'Churn_Score__c', churn_risk_score,
                'Account_Health__c', health_tier,
                'Upsell_Propensity__c', propensity_to_upsell,
                'Analytics_Synced_At__c', CURRENT_TIMESTAMP()
            )
        )::STRING AS payload
    FROM ranked_deltas
    WHERE rank_idx = 1
      AND delta_action = 'INSERT' -- Captures row insertions and current post-images of updates
)
FILE_FORMAT = (TYPE = 'JSON')
HEADER = FALSE
OVERWRITE = FALSE;

```

---

## 4. Buffering & Rate-Limiting Dispatcher (Lambda + Redis)

Third-party APIs quickly drop connections if slammed with hundreds of concurrent requests. AWS Lambda consumes from the **SQS FIFO Queue**, checks rate quotas in **Redis**, and flushes records in batches conforming to downstream bulk API interfaces.

### Rate-Limiter & Bulk Dispatcher Implementation

```python
import os
import json
import boto3
import urllib3
import redis

# Initialize Redis client for token bucket state tracking
redis_client = redis.Redis(
    host=os.environ['REDIS_HOST'],
    port=int(os.environ['REDIS_PORT']),
    decode_responses=True
)

s3_client = boto3.client('s3')
http = urllib3.PoolManager()

SALESFORCE_INSTANCE_URL = os.environ['SF_INSTANCE_URL']
SF_ACCESS_TOKEN = os.environ['SF_ACCESS_TOKEN']
AUDIT_BUCKET = os.environ['AUDIT_BUCKET']

# Salesforce Bulk Limits: max 200 records per composite batch request
BATCH_SIZE_LIMIT = 200
# Rate limit: Max 20 requests per second
MAX_REQUESTS_PER_SECOND = 20

def acquire_rate_limit_token(client_key: str, limit_per_sec: int) -> bool:
    """Token bucket implementation via atomic Redis INCR/EXPIRE."""
    current_sec_key = f"ratelimit:{client_key}:{int(os.time())}"
    current_usage = redis_client.incr(current_sec_key)
    if current_usage == 1:
        redis_client.expire(current_sec_key, 2)
    return current_usage <= limit_per_sec

def lambda_handler(event, context):
    records_to_sync = []
    receipts = []

    for record in event['Records']:
        body = json.loads(record['body'])
        records_to_sync.append(body)

    if not records_to_sync:
        return {"status": "NO_DATA"}

    # Guard API ceiling
    if not acquire_rate_limit_token("salesforce_bulk", MAX_REQUESTS_PER_SECOND):
        # Throwing an exception forces SQS to retry with exponential backoff & jitter
        raise Exception("CRM API Rate Limit Threshold Reached - Backing off SQS worker.")

    # Prepare Composite API payload for Salesforce
    composite_records = [
        {
            "method": "PATCH",
            "url": f"/services/data/v59.0/sobjects/Account/External_ID__c/{item['external_id']}",
            "referenceId": item['external_id'],
            "body": item['attributes']
        }
        for item in records_to_sync[:BATCH_SIZE_LIMIT]
    ]

    payload = {"allOrNone": False, "compositeRequest": composite_records}

    # Execute downstream write
    response = http.request(
        "POST",
        f"{SALESFORCE_INSTANCE_URL}/services/data/v59.0/composite",
        headers={
            "Authorization": f"Bearer {SF_ACCESS_TOKEN}",
            "Content-Type": "application/json"
        },
        body=json.dumps(payload)
    )

    resp_data = json.loads(response.data.decode('utf-8'))

    # Collect individual response statuses
    for result in resp_data.get('compositeResponse', []):
        receipts.append({
            "external_id": result['referenceId'],
            "http_status": result['httpStatusCode'],
            "synced_at": os.time(),
            "error": result.get('body') if result['httpStatusCode'] >= 400 else None
        })

    # Persist execution receipts back to S3 for Lakehouse auditability
    batch_id = context.aws_request_id
    s3_client.put_object(
        Bucket=AUDIT_BUCKET,
        Key=f"audit_logs/year={os.strftime('%Y')}/month={os.strftime('%m')}/day={os.strftime('%d')}/receipt_{batch_id}.json",
        Body=json.dumps(receipts)
    )

    return {"status": "SUCCESS", "processed": len(receipts)}

```

---

## 5. Audit Ingestion & Operational Observability (Amazon Athena)

Operational teams need visibility when customer scores fail to reach the CRM (e.g., deleted CRM lead records or field permission errors). Audit files emitted by the Lambda dispatcher are cataloged in the **AWS Glue Data Catalog**.

```
[ S3 Audit Zone ] ──► [ AWS Glue Data Catalog ] ──► [ Amazon Athena SQL ] ──► [ Operational BI ]

```

### 1. Athena External Audit Table DDL

```sql
CREATE EXTERNAL TABLE IF NOT EXISTS operational_dw.reverse_etl_audit (
    external_id STRING,
    http_status INT,
    synced_at DOUBLE,
    error STRING
)
PARTITIONED BY (
    year STRING,
    month STRING,
    day STRING
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://reverse-etl-stage/audit_logs/';

```

### 2. Operational Health Diagnostic Query

Identifies broken records and high-frequency error codes:

```sql
SELECT
    http_status,
    error,
    COUNT(*) AS failed_sync_count,
    ARRAY_AGG(external_id) AS sample_failed_ids
FROM operational_dw.reverse_etl_audit
WHERE http_status >= 400
  AND year = '2026' AND month = '09'
GROUP BY http_status, error
ORDER BY failed_sync_count DESC;

```

---

## 6. Failure Modes, Dead-Letter Queues & Circuit Breaking

```
[ Incoming SQS Message ]
           │
           ▼
    [ Worker Lambda ]
           │
     (Fails 5 times)
           │
           ▼
[ Dead-Letter Queue (DLQ) ]
           │
           ├────────────────────────────┐
           ▼                            ▼
[ CloudWatch Alarm ]          [ Automated Lambda Pauser ]
  Metric: ApproximateNumberOf   Triggers: Disables Snowflake Export Task
  MessagesVisible > 50          Prevents accumulating undeliverable stream backlog

```

| Failure Vector                 | Operational Impact                                                                                                               | Engineering Mitigation                                                                                                                                                                                                    |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Salesforce Service Outage**  | CRM endpoints return `503 Service Unavailable` or connection timeouts.                                                           | Lambda checks endpoint health before pulling large SQS batches. SQS triggers an exponential backoff with jitter up to the max visibility timeout ($15\text{ mins}$).                                                      |
| **Downstream Schema Changes**  | CRM Administrator renames or deletes custom fields (e.g., `Churn_Score__c`), causing 100% of writes to return `400 Bad Request`. | If the DLQ receives $> 50$ failed messages in a 5-minute rolling window, a CloudWatch alarm triggers an automated Lambda that halts the Snowflake extraction task to stop stream consumption until schemas re-align.      |
| **Duplicate Message Delivery** | Network drop after CRM accepts write, but before Lambda completes message deletion in SQS.                                       | Use **SQS FIFO** with a unique `MessageDeduplicationId` calculated as `SHA256(external_id + feature_updated_at)`. SQS automatically deduplicates repeated message arrivals within a 5-minute window.                      |
| **Access Token Invalidation**  | OAuth token expires or refresh token credentials fail during off-peak hours.                                                     | Store refresh tokens in **AWS Secrets Manager**; the Lambda execution environment intercepts `401 Unauthorized` responses, executes an atomic token refresh, caches the new bearer token in Redis, and retries the batch. |

---

## 7. Operational Architecture Summary Matrix

| Component                   | Technology                  | Responsibility                                                                   |
| --------------------------- | --------------------------- | -------------------------------------------------------------------------------- |
| **Change Detection**        | Snowflake Streams           | Captures delta changes on analytical feature tables without full-table re-scans. |
| **Export Staging**          | Snowflake Tasks + Amazon S3 | Deduplicates multi-update bursts and unloads lightweight JSON payloads.          |
| **Buffering & Concurrency** | Amazon SQS (FIFO)           | Decouples warehouse batch exports from real-time CRM API consumption ceilings.   |
| **Rate Throttling**         | AWS Lambda + Redis          | Enforces composite batch sizes and strict per-second request rate limits.        |
| **Audit & Feedback**        | AWS Glue + Amazon Athena    | Ingests downstream HTTP receipt logs for debugging and data observability.       |
