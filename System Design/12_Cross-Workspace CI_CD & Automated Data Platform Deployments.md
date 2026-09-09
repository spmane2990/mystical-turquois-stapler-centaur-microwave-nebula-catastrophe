# Scenario 12: Cross-Workspace CI/CD & Automated Data Platform Deployments

This design details an enterprise-grade GitOps and Continuous Integration / Continuous Deployment (CI/CD) architecture for a multi-cloud data platform. It eliminates manual notebook updates, prevents uncoordinated schema drift, and enables automated testing and deployment across **Development (Dev)**, **Staging (Stage)**, and **Production (Prod)** tiers.

The architecture coordinates:

- **Databricks Asset Bundles (DABs)** for PySpark pipelines, Delta Live Tables (DLT), and compute definitions.
- **Schemachange** for version-controlled, idempotent Snowflake declarative migrations.
- **AWS MWAA (Airflow)** for DAG validation, dependency checks, and automated syncing.
- **GitHub Actions / Azure DevOps** as the unified execution orchestrator with ephemeral test environments.

---

## 1. System Requirements & Quality Gates

- **Zero Manual Interventions:** No interactive production runs from notebooks or direct DDL execution from personal user accounts.
- **Hermetic Ephemeral Testing:** Every Pull Request (PR) automatically deploys changes to an isolated, short-lived environment to run integration tests before code review approval.
- **Idempotent Database Migrations:** All Snowflake DDL/DML migrations must be versioned, forward-only, trackable, and safe against partial rollbacks.
- **Zero-Downtime Pipeline Deployment:** Deploying a new pipeline version must not interrupt in-flight batch processes or drop streaming state checkpoints.
- **Automated Rollback & Audit Trail:** Infrastructure state tracked completely in Git, backed by cryptographically signed commit trees and deployment logs.

---

## 2. End-to-End GitOps Architecture

```
[ Developer Workspace ]
   - Feature branch development
   - Local pre-commit linting (sqlfluff, flake8, black)
            │
            │ (git push origin feature/JIRA-1234)
            ▼
[ Pull Request Pipeline (GitHub Actions / Azure DevOps) ]
   1. Static Analysis: Linting, AST parsing, Unit tests (pytest)
   2. Ephemeral Environment Creation:
      - Snowflake: Clone target schema via Zero-Copy Clone
      - Databricks: Deploy ephemeral DAB to Dev workspace
      - MWAA: Run DAG import integrity parser
   3. Integration Test Execution against sample datasets
   4. Teardown Ephemeral Resources
            │
            │ (PR Approved & Merged to 'main')
            ▼
[ Staging Deployment Pipeline ]
   1. Schemachange executes migrations on STG_DW
   2. DABs deploys workflows to Staging Databricks Workspace
   3. Sync DAGs to Staging S3 MWAA bucket
   4. Run end-to-end regression validation suite
            │
            │ (Automated or Manual Release Tag: v2.4.1)
            ▼
[ Production Deployment Pipeline ]
   1. Schemachange: Production DDL migrations with change-lock
   2. Databricks: Blue/Green DAB workflow pointer swap
   3. MWAA: Canary deploy DAGs to Production S3 bucket
   4. Post-deploy health-check telemetry monitoring

```

---

## 3. Snowflake Declarative Migrations (Schemachange)

Instead of running ad-hoc SQL worksheets, database modifications are version-controlled using **Schemachange** (a lightweight database change management tool designed for Snowflake).

### Directory Hierarchy

```
snowflake_migrations/
├── V1.1.0__create_core_schemas.sql
├── V1.1.1__add_customer_segment_column.sql
├── V1.1.2__create_orders_merge_task.sql
├── R__recreate_reporting_views.sql        # Repeatable migration (runs on change)
└── A__grant_bi_analyst_permissions.sql    # Always-run migration

```

### Schemachange CI/CD Execution Command

```bash
# Executed via service principal credentials inside GitHub Actions
schemachange -f ./snowflake_migrations \
  -a "${SNOWFLAKE_ACCOUNT}" \
  -u "${SNOWFLAKE_DEPLOYER_USER}" \
  -r "SYSADMIN" \
  -w "DEPLOYMENT_WH" \
  -d "${TARGET_DATABASE}" \
  -c "${TARGET_DATABASE}.CHANGE_TRACKING.SCHEMACHANGE_HISTORY" \
  --create-change-history-table

```

### Automated Ephemeral PR Testing via Zero-Copy Clone

To test schema migrations without duplicating petabytes of data:

```sql
-- GitHub Actions creates an instant, zero-cost sandbox on PR open
CREATE DATABASE pr_1234_sandbox CLONE enterprise_dw;

-- Schemachange applies migration scripts against the sandbox
-- ... runs automated integration tests ...

-- GitHub Actions drops the sandbox upon PR merge/close
DROP DATABASE pr_1234_sandbox;

```

---

## 4. Databricks Infrastructure as Code: Databricks Asset Bundles (DABs)

Databricks Asset Bundles package code (notebooks, Python packages), cluster configs, and workflow definitions into a declarative YAML specification deployed deterministically across workspaces.

### Bundle Configuration (`databricks.yml`)

```yaml
bundle:
  name: enterprise_lakehouse_pipelines

include:
  - resources/*.yml

targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://adb-dev.azuredatabricks.net
    variables:
      bronze_bucket: s3://enterprise-lakehouse-dev-bronze

  stage:
    mode: production
    workspace:
      host: https://adb-stage.azuredatabricks.net
    variables:
      bronze_bucket: s3://enterprise-lakehouse-stage-bronze

  prod:
    mode: production
    workspace:
      host: https://adb-prod.azuredatabricks.net
    variables:
      bronze_bucket: s3://enterprise-lakehouse-prod-bronze
```

### Job Resource Definition (`resources/etl_workflow.yml`)

```yaml
resources:
  jobs:
    daily_customer_scd2:
      name: "[${bundle.target}] Daily Customer SCD2 Pipeline"
      job_clusters:
        - job_cluster_key: worker_pool
          new_cluster:
            spark_version: 14.3.x-scala2.12
            node_type_id: Standard_D8s_v5
            autoscale:
              min_workers: 2
              max_workers: 8
            azure_attributes:
              availability: SPOT_WITH_FALLBACK_AZURE
      tasks:
        - task_key: run_scd2_merge
          job_cluster_key: worker_pool
          python_wheel_task:
            package_name: lakehouse_transforms
            entry_point: scd2_runner
            parameters: ["--source", "${var.bronze_bucket}/customers/"]
      permissions:
        - level: CAN_MANAGE
          group_name: data-platform-engineers
```

### Deployment Commands

```bash
# Validates syntax, schemas, and resource definitions
databricks bundle validate -t prod

# Deploys the code, wheels, and schedules to the target workspace
databricks bundle deploy -t prod

```

---

## 5. MWAA Airflow DAG Integrity & Synchronization Pipeline

A common production outage occurs when a DAG with syntax or import errors lands in the S3 DAG folder, crashing the MWAA scheduler parse loop.

### 1. Pre-Deployment Static DAG Parser (`test_dag_integrity.py`)

Run inside GitHub Actions on every pull request:

```python
import pytest
from airflow.models import DagBag

def test_dag_import_errors():
    """Verify that all Airflow DAGs parse without Python syntax or import exceptions."""
    dag_bag = DagBag(dag_folder="./dags", include_examples=False)

    assert len(dag_bag.import_errors) == 0, (
        f"Airflow DAG import failure: {dag_bag.import_errors}"
    )

def test_dag_tags_and_sla():
    """Enforce data governance standards: owner, retries, and timeout configs."""
    dag_bag = DagBag(dag_folder="./dags", include_examples=False)

    for dag_id, dag in dag_bag.dags.items():
        assert dag.default_args.get("owner"), f"{dag_id} missing 'owner' definition."
        assert dag.default_args.get("retries", 0) >= 1, f"{dag_id} must have retries >= 1."
        assert dag.default_args.get("execution_timeout"), f"{dag_id} missing 'execution_timeout'."

```

### 2. Idempotent S3 Sync with Exclusion Rules

Deploy clean DAGs using the AWS CLI:

```bash
aws s3 sync ./dags s3://mwaa-prod-lakehouse-environment/dags/ \
  --delete \
  --exclude "*.pyc" \
  --exclude "*__pycache__*" \
  --exclude ".pytest_cache/*" \
  --exclude "*.md"

```

---

## 6. Complete GitHub Actions Production Release Workflow

`.github/workflows/deploy_production.yml`:

```yaml
name: Production Release Pipeline

on:
  push:
    tags:
      - "v*.*.*"

jobs:
  validate_and_test:
    name: Lint & Unit Tests
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.10"

      - name: Install Linting & Testing Tools
        run: |
          pip install flake8 pytest sqlfluff apache-airflow==2.8.1

      - name: Run SQLFluff (Snowflake Dialect)
        run: sqlfluff lint ./snowflake_migrations --dialect snowflake

      - name: Run Python Linters
        run: flake8 ./dags ./src --max-line-length=120

      - name: Execute DAG Integrity Checks
        run: pytest tests/test_dag_integrity.py

  deploy_snowflake:
    name: Apply Snowflake Migrations
    needs: validate_and_test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install Schemachange
        run: pip install schemachange
      - name: Run Deployments
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.PROD_SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_DEPLOYER_USER: ${{ secrets.PROD_SNOWFLAKE_USER }}
          SNOWFLAKE_PASSWORD: ${{ secrets.PROD_SNOWFLAKE_PW }}
        run: |
          schemachange -f ./snowflake_migrations \
            -a "$SNOWFLAKE_ACCOUNT" -u "$SNOWFLAKE_DEPLOYER_USER" \
            -r "SYSADMIN" -w "DEPLOYMENT_WH" -d "ENTERPRISE_DW" \
            -c "ENTERPRISE_DW.CHANGE_TRACKING.SCHEMACHANGE_HISTORY" \
            --create-change-history-table

  deploy_databricks:
    name: Deploy Databricks DABs
    needs: validate_and_test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Install Databricks CLI
        run: curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
      - name: Deploy Production Bundle
        env:
          DATABRICKS_HOST: ${{ secrets.PROD_DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.PROD_DATABRICKS_TOKEN }}
        run: |
          databricks bundle validate -t prod
          databricks bundle deploy -t prod

  deploy_mwaa:
    name: Sync DAGs to AWS MWAA
    needs: [deploy_snowflake, deploy_databricks]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Configure AWS Credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: us-east-1
      - name: Sync Production S3 DAG Bucket
        run: |
          aws s3 sync ./dags s3://mwaa-prod-lakehouse-environment/dags/ --delete --exclude "*__pycache__*"
```

---

## 7. Failure Modes & Mitigations

| Failure Vector                               | Production Impact                                                                                                                      | Engineering Mitigation                                                                                                                                                                                                                       |
| -------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Failed Middle Migration Script**           | Migration halts halfway through execution, leaving the database schema in an unknown state.                                            | Schemachange records execution status inside `SCHEMACHANGE_HISTORY`. Every script must be written to run atomically within a transaction block (`BEGIN ... COMMIT`). If a step fails, the entire script rolls back and aborts the CI/CD run. |
| **DAB Overwriting Active Streaming Jobs**    | Deploying a bundle while a Spark Structured Streaming job runs could wipe the current checkpoint folder.                               | Enforce persistent, external checkpoint paths outside bundle deployment paths (`s3://persistent-checkpoints/...`), decoupling runtime state storage from deployed bundle versions.                                                           |
| **Airflow Worker Parsing Desynchronization** | Some workers parse updated DAGs from S3 faster than others, creating non-deterministic task execution across multi-node MWAA clusters. | Avoid dynamic cross-file dependencies inside DAG definitions. Keep helper classes self-contained or deploy them as versioned wheel packages into the MWAA `plugins.zip` or `requirements.txt`.                                               |
| **Secret Exfiltration via PR Workflows**     | Forked or rogue PRs attempt to print or export cloud credentials.                                                                      | Set up **Environment Protection Rules** in GitHub Actions/Azure DevOps. Secrets are restricted to protected branches (`main`, `release/*`) and require mandatory, independent code reviews before pipeline triggers.                         |

---
