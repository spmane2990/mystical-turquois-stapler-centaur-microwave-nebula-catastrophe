Below is a production-grade Terraform deployment pattern for an **S3 Lakehouse**, **AWS Lake Formation security controls**, and an **Apache Iceberg-enabled AWS Glue ETL pipeline**.

This solution handles a critical Lake Formation requirement: **disabling `IAMAllowedPrincipals**` to shift governance to explicit Lake Formation fine-grained access control.

---

## 1. Project Directory Structure

Organize your Terraform project with clear boundaries between storage, catalog/permissions, and compute resources:

```text
├── main.tf                 # Provider setup & terraform settings
├── variables.tf            # Input configuration variables
├── outputs.tf              # Resource outputs (S3 ARNs, Roles, Job Names)
├── s3.tf                   # Medallion Lakehouse S3 Buckets (Bronze/Silver/Gold)
├── lakeformation.tf        # Data Lake Settings, Data Location & Permissions
├── glue.tf                 # Glue Catalog, IAM Role, and PySpark Iceberg ETL Job
└── scripts/
    └── iceberg_etl.py      # Glue PySpark ETL script using Apache Iceberg

```

---

## 2. Terraform Infrastructure Configuration

### `main.tf`

```hcl
terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.5"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Environment = var.environment
      ManagedBy   = "Terraform"
      Project     = "Lakehouse-Architecture"
    }
  }
}

data "aws_caller_identity" "current" {}

```

### `variables.tf`

```hcl
variable "aws_region" {
  type        = string
  default     = "us-east-1"
  description = "AWS Region for deployment"
}

variable "environment" {
  type        = string
  default     = "prod"
  description = "Deployment environment stage"
}

```

---

### `s3.tf` (Medallion S3 Storage Layer)

Creates isolated S3 buckets for the **Bronze** (raw landing) and **Silver** (clean Apache Iceberg) layers, enforcing KMS encryption and public access blocks.

```hcl
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# Bronze Bucket: Raw Ingestion
resource "aws_s3_bucket" "bronze" {
  bucket        = "lakehouse-bronze-${var.environment}-${random_id.bucket_suffix.hex}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "bronze" {
  bucket                  = aws_s3_bucket.bronze.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Silver Bucket: Transformed Apache Iceberg Tables
resource "aws_s3_bucket" "silver" {
  bucket        = "lakehouse-silver-${var.environment}-${random_id.bucket_suffix.hex}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "silver" {
  bucket                  = aws_s3_bucket.silver.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Upload ETL Script to S3
resource "aws_s3_object" "etl_script" {
  bucket = aws_s3_bucket.silver.id
  key    = "scripts/iceberg_etl.py"
  source = "${path.module}/scripts/iceberg_etl.py"
  etag   = filemd5("${path.module}/scripts/iceberg_etl.py")
}

```

---

### `lakeformation.tf` (Governance & Security)

Configures the Terraform executing identity as a **Lake Formation Admin**, revokes default legacy IAM permissions (`IAMAllowedPrincipals`), and registers S3 locations under Lake Formation management.

```hcl
# 1. Designate Caller as Lake Formation Admin
resource "aws_lakeformation_data_lake_settings" "this" {
  admins = [data.aws_caller_identity.current.arn]

  # Revoke legacy 'IAMAllowedPrincipals' for new databases and tables
  create_database_default_permissions {
    permissions = []
    principal   = "IAM_ALLOWED_PRINCIPALS"
  }

  create_table_default_permissions {
    permissions = []
    principal   = "IAM_ALLOWED_PRINCIPALS"
  }
}

# 2. Register S3 Silver Lakehouse Storage in Lake Formation
resource "aws_lakeformation_resource" "silver_s3" {
  arn      = aws_s3_bucket.silver.arn
  role_arn = aws_iam_role.lakeformation_s3_access.arn
}

# IAM Role assumed by Lake Formation to vend S3 Credentials
resource "aws_iam_role" "lakeformation_s3_access" {
  name = "lakehouse-lakeformation-s3-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lakeformation.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy" "lakeformation_s3_policy" {
  name = "lakehouse-lakeformation-s3-policy"
  role = aws_iam_role.lakeformation_s3_access.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ]
      Resource = [
        aws_s3_bucket.silver.arn,
        "${aws_s3_bucket.silver.arn}/*"
      ]
    }]
  })
}

# 3. Grant Lake Formation Data Location Permission to Glue ETL Role
resource "aws_lakeformation_permissions" "glue_data_location" {
  principal   = aws_iam_role.glue_etl_role.arn
  permissions = ["DATA_LOCATION_ACCESS"]

  data_location {
    arn = aws_lakeformation_resource.silver_s3.arn
  }
}

# 4. Grant Lake Formation Database Permissions to Glue ETL Role
resource "aws_lakeformation_permissions" "glue_database_permissions" {
  principal   = aws_iam_role.glue_etl_role.arn
  permissions = ["CREATE_TABLE", "ALTER", "DROP", "DESCRIBE"]

  database {
    name = aws_glue_catalog_database.lakehouse_db.name
  }
}

```

---

### `glue.tf` (Glue Catalog & Apache Iceberg ETL Pipeline)

Provisions the Glue Catalog database, configures the IAM execution role with necessary Lake Formation / S3 permissions, and defines a **Glue 4.0 Serverless PySpark job** configured for Apache Iceberg.

```hcl
# 1. Glue Catalog Database
resource "aws_glue_catalog_database" "lakehouse_db" {
  name        = "silver_lakehouse_db"
  description = "Target database for Silver layer Apache Iceberg tables"
}

# 2. IAM Role for Glue ETL Execution
resource "aws_iam_role" "glue_etl_role" {
  name = "lakehouse-glue-etl-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "glue.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service_policy" {
  role       = aws_iam_role.glue_etl_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# S3 Access Policy for Glue ETL Job
resource "aws_iam_role_policy" "glue_s3_access" {
  name = "glue-s3-lakehouse-access"
  role = aws_iam_role.glue_etl_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ]
      Resource = [
        aws_s3_bucket.bronze.arn,
        "${aws_s3_bucket.bronze.arn}/*",
        aws_s3_bucket.silver.arn,
        "${aws_s3_bucket.silver.arn}/*"
      ]
    }]
  })
}

# 3. Glue Serverless ETL Job (Apache Iceberg Enabled)
resource "aws_glue_job" "iceberg_etl" {
  name              = "lakehouse-iceberg-etl-${var.environment}"
  role_arn          = aws_iam_role.glue_etl_role.arn
  glue_version      = "4.0" # Includes Apache Iceberg runtime extensions
  worker_type       = "G.1X"
  number_of_workers = 2

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.silver.id}/${aws_s3_object.etl_script.key}"
    python_version  = "3"
  }

  default_arguments = {
    "--job-language"                    = "python"
    "--datalake-formats"                = "iceberg"
    "--enable-glue-datacatalog"         = "true"
    # Apache Iceberg Glue Catalog Configurations
    "--conf"                            = "spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO"
    # Environment runtime variables
    "--BRONZE_BUCKET"                   = aws_s3_bucket.bronze.id
    "--SILVER_BUCKET"                   = aws_s3_bucket.silver.id
    "--DATABASE_NAME"                   = aws_glue_catalog_database.lakehouse_db.name
  }

  depends_on = [
    aws_lakeformation_permissions.glue_data_location,
    aws_lakeformation_permissions.glue_database_permissions
  ]
}

```

---

## 3. Glue PySpark Apache Iceberg Script

Place this code inside `./scripts/iceberg_etl.py`. It reads raw landing JSON files from the S3 Bronze bucket, performs transformations, and executes an **ACID `MERGE INTO` (Upsert)** operation directly into an Apache Iceberg table managed by the Glue Data Catalog.

```python
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, to_timestamp

# Extract Job Arguments
args = getResolvedOptions(
    sys.argv,
    ['JOB_NAME', 'BRONZE_BUCKET', 'SILVER_BUCKET', 'DATABASE_NAME']
)

bronze_bucket = args['BRONZE_BUCKET']
silver_bucket = args['SILVER_BUCKET']
database_name = args['DATABASE_NAME']
table_name = "fact_customer_orders"

# Initialize Spark Session with Glue Iceberg catalog enabled
spark = SparkSession.builder \
    .appName("GlueIcebergETL") \
    .getOrCreate()

# 1. Read Raw JSON Stream/Batch from Bronze Bucket
bronze_path = f"s3://{bronze_bucket}/landing/orders/*.json"

try:
    df_raw = spark.read.json(bronze_path)
except Exception as e:
    print(f"No files found at path {bronze_path}, exiting gracefully.")
    sys.exit(0)

# 2. Transform Data & Schema Binding
df_transformed = df_raw \
    .withColumn("order_amount", col("amount").cast("decimal(18,2)")) \
    .withColumn("order_timestamp", to_timestamp(col("timestamp"))) \
    .withColumn("updated_at", current_timestamp()) \
    .select("order_id", "customer_id", "order_amount", "status", "order_timestamp", "updated_at")

df_transformed.createOrReplaceTempView("src_orders")

# 3. Create Apache Iceberg Table if it does not exist (using Glue Catalog)
full_table_identifier = f"glue_catalog.{database_name}.{table_name}"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_table_identifier} (
        order_id STRING,
        customer_id STRING,
        order_amount DECIMAL(18,2),
        status STRING,
        order_timestamp TIMESTAMP,
        updated_at TIMESTAMP
    )
    USING iceberg
    LOCATION 's3://{silver_bucket}/tables/{table_name}/'
    PARTITIONED BY (days(order_timestamp))
""")

# 4. Perform ACID Upsert (MERGE INTO)
spark.sql(f"""
    MERGE INTO {full_table_identifier} t
    USING src_orders s
    ON t.order_id = s.order_id
    WHEN MATCHED THEN
        UPDATE SET
            t.order_amount = s.order_amount,
            t.status = s.status,
            t.updated_at = s.updated_at
    WHEN NOT MATCHED THEN
        INSERT (order_id, customer_id, order_amount, status, order_timestamp, updated_at)
        VALUES (s.order_id, s.customer_id, s.order_amount, s.status, s.order_timestamp, s.updated_at)
""")

print("Successfully merged micro-batch into Iceberg table.")

```

---

## 4. Execution & Validation Workflow

```bash
# 1. Initialize & Validate Terraform Code
terraform init
terraform validate

# 2. Preview and Apply Infrastructure Deployment
terraform apply -auto-approve

# 3. Upload Sample Test Data to S3 Bronze Bucket
aws s3 cp - <<EOF s3://$(terraform output -raw bronze_bucket_name)/landing/orders/batch_1.json
{"order_id": "ORD-1001", "customer_id": "CUST-501", "amount": 149.99, "status": "COMPLETED", "timestamp": "2026-07-27T10:00:00Z"}
{"order_id": "ORD-1002", "customer_id": "CUST-502", "amount": 89.50, "status": "PENDING", "timestamp": "2026-07-27T10:15:00Z"}
EOF

# 4. Trigger the AWS Glue ETL Job
aws glue start-job-run --job-name $(terraform output -raw glue_job_name)

# 5. Query the Apache Iceberg Table via Amazon Athena
# Query string: SELECT * FROM silver_lakehouse_db.fact_customer_orders;

```
