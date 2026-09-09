# Scenario 7: Multi-Region Active-Passive Disaster Recovery & Business Continuity

This design details an enterprise-grade Active-Passive Disaster Recovery (DR) solution for an end-to-end Lakehouse and Enterprise Data Warehouse platform. It covers regional recovery from catastrophic outages (e.g., `us-east-1` losing network, compute, or control planes) with a verified **Recovery Point Objective (RPO) < 15 minutes** and **Recovery Time Objective (RTO) < 60 minutes**.

---

## 1. Core DR Metrics & Architectural Boundaries

```
[ Primary: AWS us-east-1 / Snowflake AWS_US_EAST_1 ]  ──(Active)──► Serving All Users / Pipelines
                         │
      Replication Stream │ (S3 CRR + Catalog Sync + Snowflake Replication Group)
      RPO < 15 Minutes   ▼
[ Secondary: AWS us-west-2 / Snowflake AWS_US_WEST_2 ] ──(Standby)──► Read-Only / Cold Workers

```

- **Target RPO (< 15 min):** Maximum acceptable data loss window during a hard crash. Driven by asynchronous cross-region storage replication and warehouse log shipping.
- **Target RTO (< 60 min):** Maximum permissible duration to detect a disaster, shift DNS/client traffic, promote read-replicas, and unpause orchestration pipelines.
- **Blast Radius Strategy:** Complete regional decoupling. The secondary region maintains independent IAM roles, AWS KMS Customer Managed Keys (CMKs), VPC topologies, and Snowflake accounts.

---

## 2. End-to-End System Replication Topology

```
+──────────────────────────────────────────────────────────+
| PRIMARY: AWS us-east-1                                   |
|                                                          |
|  [ Ingestion Producers ]                                 |
|             │                                            |
|             ▼                                            |
|  [ S3 Lakehouse Primary ] ──────(S3 RTC Replication)─────┼──────────────┐
|             │                                            |              │
|             ▼                                            |              │
|  [ AWS Glue / Unity Catalog ] ──(Catalog Sync / Event)───┼──────┐       │
|             │                                            |      │       │
|             ▼                                            |      │       │
|  [ MWAA Primary (Active) ]                               |      │       │
|             │                                            |      │       │
|             ▼                                            |      │       │
|  [ Snowflake Primary Account ]                           |      │       │
|    - Failover Group: Primary                             |      │       │
|    - Read/Write Workloads                                |      │       │
+─────────────────────────────┬────────────────────────────+      │       │
                              │ (Auto-Replication)                │       │
                              ▼                                   │       │
+──────────────────────────────────────────────────────────+      │       │
| SECONDARY: AWS us-west-2 (STANDBY)                       |      │       │
|                                                          |      │       │
|  [ S3 Lakehouse Secondary ] ◄────────────────────────────┼──────┼───────┘
|             │                                            |      │
|             ▼                                            |      │
|  [ AWS Glue / Unity Catalog Standby ] ◄──────────────────┼──────┘
|             │                                            |
|             ▼                                            |
|  [ Snowflake Secondary Account ]                         |
|    - Failover Group: Secondary (Read-Only Replicated)    |
|    - Suspended Warehouses                                |
|             ▲                                            |
|             │ (Promoted on Failover)                     |
|  [ MWAA Standby (DAGs Paused / Inactive State) ]         |
+──────────────────────────────────────────────────────────+

```

---

## 3. Storage Layer Replication (S3, Delta, & Catalogs)

### S3 Replication Time Control (RTC) & Metadata Consistency

Standard S3 Cross-Region Replication (CRR) does not provide SLA-backed guarantees. To honor the **RPO < 15 minutes**, enable **S3 Replication Time Control (RTC)**.

- **RTC SLA:** Replicates 99.99% of new objects within 15 minutes, with CloudWatch metrics tracking `ReplicationLatency` and `BytesPendingReplication`.
- **KMS Key Decoupling:** Replicating encrypted objects across regions requires multi-region keys (MRKs) or automatic envelope re-encryption using the destination region's CMK:

```json
{
  "Rules": [
    {
      "ID": "LakehouseSyncWithRTC",
      "Status": "Enabled",
      "Priority": 1,
      "SourceSelectionCriteria": {
        "SseKmsEncryptedObjects": { "Status": "Enabled" }
      },
      "Destination": {
        "Bucket": "arn:aws:s3:::enterprise-lakehouse-us-west-2",
        "EncryptionConfiguration": {
          "ReplicaKmsKeyID": "arn:aws:kms:us-west-2:123456789012:key/mrk-west-key"
        },
        "ReplicationTime": {
          "Status": "Enabled",
          "Time": { "Minutes": 15 }
        },
        "Metrics": {
          "Status": "Enabled",
          "EventThreshold": { "Minutes": 15 }
        }
      }
    }
  ]
}
```

### Delta Lake & Iceberg Parity Guarantees

- **The Log Ordering Problem:** If data Parquet files replicate before or out-of-sync with `_delta_log/*.json` commits, reads in the secondary region risk failing or reading partial state.
- **Solution:** Because S3 RTC processes events with near-zero skew, target snapshots remain self-contained. Readers in the standby region target the last fully closed Delta checkpoint rather than tailing transient files.

### Glue Data Catalog Cross-Region Synchronization

Replicate the catalog using **EventBridge & Lambda Sync**:

1. An update to the primary Glue Catalog fires an EventBridge rule (`Glue Data Catalog Table State Change`).
2. A lightweight Lambda ships the partition/schema DDL to the target region:

```python
# Glue Catalog Sync Handler (Runs in us-west-2)
import boto3

glue_client = boto3.client('glue', region_name='us-west-2')

def sync_table_definition(event, context):
    table_input = event['detail']['tableInput']
    db_name = event['detail']['databaseName']

    # Rewrites storage location from us-east-1 to us-west-2 bucket
    table_input['StorageDescriptor']['Location'] = table_input['StorageDescriptor']['Location'].replace(
        'enterprise-lakehouse-us-east-1',
        'enterprise-lakehouse-us-west-2'
    )
    glue_client.update_table(DatabaseName=db_name, TableInput=table_input)

```

---

## 4. Snowflake Cross-Cloud & Cross-Region Business Continuity

Snowflake natively handles warehouse and database redundancy through **Failover Groups**. This replicates databases, users, roles, network policies, and resource monitors.

### 1. Primary Account Setup (Account: `EAST_ORG.PROD_EAST`)

```sql
-- Create Failover Group covering Databases, Shares, Security, and Governance
CREATE FAILOVER GROUP enterprise_failover_group
  OBJECT_TYPES = USERS, ROLES, WAREHOUSES, RESOURCE MONITORS, DATABASES
  ALLOWED_DATABASES = ENTERPRISE_DW, GOLD_MARTS
  ALLOWED_ACCOUNTS = WEST_ORG.PROD_WEST
  REPLICATION_SCHEDULE = '10 MINUTE'; -- Satisfies RPO < 15 min

```

### 2. Standby Account Initialization (Account: `WEST_ORG.PROD_WEST`)

```sql
-- Bootstrap the secondary replica in us-west-2
CREATE FAILOVER GROUP enterprise_failover_group
  AS REPLICA OF EAST_ORG.PROD_EAST.enterprise_failover_group;

-- Manually verify initial synchronization
ALTER FAILOVER GROUP enterprise_failover_group REFRESH;

```

### 3. Monitoring Replication Lag

Run this check periodically via a health-check monitor:

```sql
SELECT
    name,
    primary,
    sync_status,
    last_refresh_scheduled_time,
    last_refresh_completed_time,
    DATEDIFF('minute', last_refresh_completed_time, CURRENT_TIMESTAMP()) AS lag_in_minutes
FROM TABLE(INFORMATION_SCHEMA.FAILOVER_GROUP_PROGRESS('enterprise_failover_group'))
WHERE lag_in_minutes > 12; -- Alert trigger before hitting 15 min RPO limit

```

---

## 5. Orchestration Failover Strategy: MWAA (Airflow)

Running two active MWAA environments running identical DAG schedules causes **split-brain executions**, non-deterministic dual-writes, and duplicated costs.

### Active-Passive Orchestration Model

- **Primary Region (`us-east-1`):** MWAA environment is fully active, with schedules running normally.
- **Secondary Region (`us-west-2`):** MWAA environment is provisioned with minimum worker capacity ($1\text{ worker}$), but all DAGs are placed in an **unpaused/disabled** state (`is_paused_upon_creation = True` in `airflow.cfg`).
- **Metadata State Synchronization:** The secondary Airflow relies on identical DAG code synced continuously from the deployment Git repository via AWS CodePipeline/GitHub Actions. It does **not** mirror the operational MySQL/Postgres metadata database directly, as dirty in-flight task execution states should not carry over across a hard failover.

---

## 6. Automated Failover Orchestration (The Failover Runbook)

When an outage is declared, the failover process is executed via a single automated AWS Step Function (`Execute-Disaster-Recovery-Plan`) to minimize human error and hit the **RTO < 60 min** target.

```
[ CloudWatch / Route 53 Health Check Detects Outage ]
                         │
                         ▼
        [ Trigger PagerDuty + SRE Incident ]
                         │
                         ▼
     [ Step Function: Regional Failover Workflow ]
                         │
       ┌─────────────────┼─────────────────┐
       ▼                 ▼                 ▼
[ Step 1: Promote  [ Step 2: Unpause  [ Step 3: Shift Client ]
  Snowflake to       Secondary MWAA    DNS / Ingress Point   ]
  Primary Role ]     Airflow DAGs ]    (Route 53 CNAME)      ]
       │                 │                 │
       └─────────────────┼─────────────────┘
                         ▼
           [ Step 4: Health Validation ]
             - Run smoke test queries
             - Verify fresh data writes
                         │
                         ▼
       [ Mark Outage Remediated (< 60 Min) ]

```

### Step 1: Promote Snowflake Standby to Primary

Executed via a Lambda running against the secondary account:

```sql
-- Run in WEST_ORG.PROD_WEST
ALTER FAILOVER GROUP enterprise_failover_group PRIMARY;

```

- **Effect:** Secondary databases transition immediately from `READ ONLY` to `READ/WRITE`. All users and roles are immediately authenticated with identical privileges.

### Step 2: Reroute Application & BI Endpoints (Client Redirection)

Avoid hardcoding region-specific Snowflake URLs in Looker, Tableau, or client services. Utilize **Snowflake Organization URLs**:

- Standard Connection: `https://<org_name>-<connection_name>.snowflakecomputing.com`
- Failover execution redirects the client connection name instantly without modifying credentials or client connection strings:

```sql
ALTER CONNECTION enterprise_bi_conn
ENABLE FAILOVER TO ACCOUNTS WEST_ORG.PROD_WEST;

```

### Step 3: Unpause Standby MWAA Airflow DAGs

A Python Lambda script runs an API call against the secondary MWAA environment to unpause critical production DAGs:

```python
import boto3
import requests
import json

mwaa_client = boto3.client('mwaa', region_name='us-west-2')

def activate_standby_mwaa(event, context):
    # Obtain CLI/Web token for secondary MWAA
    response = mwaa_client.create_cli_token(Name='mwaa-lakehouse-west')
    token = response['CliToken']
    url = f"https://{response['WebServerHostname']}/aws_mwaa/cli"

    # Critical data pipeline DAG IDs to unpause
    core_dags = ['scd2_sales_mart', 'vendor_ingest_curated', 'finance_gold_sync']

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'text/plain'
    }

    for dag_id in core_dags:
        # Executes: airflow dags unpause <dag_id>
        body = f"dags unpause {dag_id}"
        r = requests.post(url, headers=headers, data=body)
        print(f"Unpaused {dag_id}: status={r.status_code}")

```

---

## 7. Failback Mechanics (Returning to Primary)

Once the cloud vendor resolves the outage in `us-east-1`, failback must proceed cautiously to prevent overwriting new data generated in the secondary region during the incident.

```
+-------------------------------------------------------------------------+
| 1. Re-establish Replication from Secondary to Primary:                  |
|    - Configure S3 RTC from us-west-2 back to us-east-1.                 |
|    - Issue ALTER FAILOVER GROUP ... REFRESH in us-east-1.               |
+-------------------------------------------------------------------------+
                                     |
                                     v
+-------------------------------------------------------------------------+
| 2. Drain and Pause Standby Pipelines:                                   |
|    - Pause all DAGs on MWAA in us-west-2.                               |
|    - Wait for all active Snowflake transactions to commit.              |
+-------------------------------------------------------------------------+
                                     |
                                     v
+-------------------------------------------------------------------------+
| 3. Execute Final State Synchronization:                                 |
|    - Run a final explicit ALTER FAILOVER GROUP REFRESH in us-east-1.     |
+-------------------------------------------------------------------------+
                                     |
                                     v
+-------------------------------------------------------------------------+
| 4. Demote Secondary and Restore Primary:                                |
|    - Run ALTER FAILOVER GROUP ... PRIMARY in us-east-1.                 |
|    - Shift Snowflake Client Connection URL back to us-east-1.           |
|    - Unpause primary MWAA Airflow DAG schedules.                        |
+-------------------------------------------------------------------------+

```

---

## 8. Failure Modes & Edge Case Matrix

| Risk Scenario                    | Impact                                                                                    | Mitigation Strategy                                                                                                                                                                               |
| -------------------------------- | ----------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Split-Brain Writes**           | Both regions accept writes, creating irreconcilable database divergences.                 | Strict primary assignment: Only one region owns the Primary Failover Group at any time. The secondary remains hardware read-only until an explicit `PRIMARY` promotion command finishes.          |
| **Replication Queue Saturation** | Influx of large files exceeds network pipes; replication lag spikes to $> 15\text{ min}$. | CloudWatch alarms on S3 RTC metric `BytesPendingReplication`. If backlog crosses 12 minutes of lag, automatically throttle low-priority batch ingestion to preserve bandwidth for CDC/Gold files. |
| **Orphaned Airflow State**       | Standby Airflow starts processing old runs, firing duplicate notifications/loads.         | DAGs rely on idempotent `MERGE INTO` operations and strict execution date parameters (`data_interval_start`). DAG backfills are disabled by default (`catchup=False`).                            |
| **Silent IAM Drift**             | Secondary region fails during cutover due to missing or outdated IAM permissions.         | Infrastructure as Code (IaC) via Terraform/Terragrunt. All IAM roles, KMS keys, and S3 policies are defined in a unified module and applied simultaneously to both regions in CI/CD.              |
