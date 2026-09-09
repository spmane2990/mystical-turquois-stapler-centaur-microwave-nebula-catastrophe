# Scenario 14: Real-Time Data Masking & Tokenization for Multi-Tenant SaaS

This design establishes an isolated, compliant, multi-tenant Lakehouse and Data Warehouse architecture for a B2B SaaS platform. It handles ingestion across thousands of corporate tenants while guaranteeing **zero cross-tenant data leakage**, preventing analytical access to cleartext PII/PCI/PHI, and enforcing **cryptographic tokenization** before data ever lands in open Lakehouse formats.

---

## 1. System Requirements & Threat Model

- **Multi-Tenant Scale:** $5{,}000+$ independent corporate tenants operating within a shared storage and compute Lakehouse infrastructure.
- **Tenant Isolation SLA:** Absolute isolation. Under no condition can a query executed by Tenant A (or an analyst querying on behalf of Tenant A) scan, filter, or infer data belonging to Tenant B.
- **Pre-Landing Tokenization:** Sensitive identifiers (SSN, credit cards, IBANs, national IDs) must be tokenized at the edge before writing to **Amazon S3 / Azure ADLS Gen2** or entering **Databricks Bronze**. Cleartext sensitive values must never touch disk unencrypted.
- **Format-Preserving Encryption (FPE):** Tokenized fields must retain their original schema format, type, and length (e.g., a 16-digit credit card number tokenizes into an algorithmic 16-digit surrogate string) so that downstream schemas, analytical transforms, and masking rules execute without breaking types.
- **Dynamic Analytics De-Tokenization:** Authorized compliance officers can decrypt tokens on-the-fly inside **Snowflake** using localized role-based access without moving datasets into cleartext staging zones.

---

## 2. End-to-End Architectural Blueprint

```
[ Multi-Tenant Application Egress / Webhooks / Agents ]
                            │
                            ▼
┌────────────────────────────────────────────────────────┐
│ Edge Tokenization & Ingestion Gateway                  │
│   - AWS API Gateway / NLB                              │
│   - Stateless Lambda Tokenization Microservice         │
│   - AWS KMS / HashiCorp Vault (Envelope Encryption)    │
│   - Vaultless Format-Preserving Encryption (FF1/FPE)   │
└───────────────────────────┬────────────────────────────┘
                            │ (Tokenized Payload: S3 Bucket)
                            ▼
┌────────────────────────────────────────────────────────┐
│ Immutable Multi-Tenant Lakehouse (Databricks)          │
│   - Bronze/Silver Parquet: Contains only FPE tokens    │
│   - Tenant isolation via Hive Partitioning:            │
│     s3://lakehouse/curated/tenant_id={UUID}/...        │
└───────────────────────────┬────────────────────────────┘
                            │ (Snowpipe / External Tables)
                            ▼
┌────────────────────────────────────────────────────────┐
│ Enterprise Serving Layer (Snowflake Multi-Tenant DW)   │
│   - Row-Level Security (RLS) via Row Access Policies   │
│   - Column-Level Security (CLS) via Masking Policies   │
│   - Secure User-Defined Functions (UDFs) for FPE       │
│     De-Tokenization (KMS External Network Access)      │
└────────────────────────────────────────────────────────┘

```

---

## 3. Edge Tokenization Engine (AWS Lambda + KMS Envelope Encryption)

Traditional cryptographic hashing (like SHA-256) is **non-reversible** (breaking operational workflows that must contact customers) and vulnerable to dictionary attacks on low-cardinality fields like phone numbers. Standard AES-256-GCM produces binary ciphertext with expanded lengths, breaking strict VARCHAR/INTEGER schema definitions.

This tier uses **NIST SP 800-38G Format-Preserving Encryption (FF1 Mode)** powered by envelope encryption:

```
[ Raw Record: SSN = "123-45-6789" ]
                  │
                  ▼
   [ AWS KMS (Customer Managed Key) ]
                  │
                  │ (Decrypt Tenant Data Encryption Key - DEK)
                  ▼
   [ Local Memory FF1 Encryptor ] ──► [ Tokenized Output: "847-19-2041" ]
                  │
                  ▼
  (Retains standard SSN regex & length; cleartext purged from memory)

```

### Stateless Tokenization Lambda Handler

```python
import os
import json
import boto3
import base64
from pyffx import String # FF1 Format-Preserving Encryption implementation

kms_client = boto3.client('kms', region_name='us-east-1')
s3_client = boto3.client('s3')

ENCRYPTED_DEK_CACHE = {} # Memory cache for encrypted tenant data keys
RAW_LANDING_BUCKET = os.environ['TOKENIZED_LANDING_BUCKET']

def get_tenant_cipher(tenant_id: str):
    """Retrieve or decrypt the tenant-specific encryption key via AWS KMS."""
    if tenant_id not in ENCRYPTED_DEK_CACHE:
        # Generate or decrypt a Tenant Data Key using the Root Key Management Service
        response = kms_client.generate_data_key(
            KeyId=os.environ['KMS_ROOT_KEY_ARN'],
            EncryptionContext={'TenantId': tenant_id},
            KeySpec='AES_256'
        )
        ENCRYPTED_DEK_CACHE[tenant_id] = response['Plaintext']

    # Initialize FF1 FPE cipher for 9-digit numeric formats (SSN style)
    dek = ENCRYPTED_DEK_CACHE[tenant_id]
    return String(dek, alphabet='0123456789', length=9)

def lambda_handler(event, context):
    processed_records = []

    for record in event['Records']:
        payload = json.loads(record['body'])
        tenant_id = payload['tenant_id']
        raw_ssn = payload['ssn'].replace('-', '') # E.g., '123456789'

        cipher = get_tenant_cipher(tenant_id)
        tokenized_ssn = cipher.encrypt(raw_ssn)

        # Inject formatted surrogate back into payload
        payload['ssn'] = f"{tokenized_ssn[:3]}-{tokenized_ssn[3:5]}-{tokenized_ssn[5:]}"
        payload['_is_tokenized'] = True

        processed_records.append(payload)

    # Persist tokenized partition directly to S3
    batch_id = context.aws_request_id
    s3_client.put_object(
        Bucket=RAW_LANDING_BUCKET,
        Key=f"curated/tenant_id={tenant_id}/batch_{batch_id}.json",
        Body=json.dumps(processed_records)
    )

    return {"status": "SUCCESS", "records_tokenized": len(processed_records)}

```

---

## 4. Multi-Tenant Storage Isolation (Databricks / S3)

To balance performance with security, partition Lakehouse data physically by `tenant_id` at the directory level while applying metadata governance via **Unity Catalog**:

```
s3://enterprise-lakehouse-curated/
    ├── tenant_id=tenant_a/
    │   ├── data_part_001.parquet (Encrypted with KMS Key A)
    │   └── _delta_log/
    └── tenant_id=tenant_b/
        ├── data_part_001.parquet (Encrypted with KMS Key B)
        └── _delta_log/

```

- **Storage Isolation:** Use S3 Bucket Policies and IAM Session Policies with conditions on `${aws:PrincipalTag/TenantId}`. An application node processing Tenant A cannot physically read prefixes labeled `tenant_id=tenant_b/`.
- **Delta Lake Table Design:** Under Unity Catalog, define an isolated database per tenant (`prod_catalog.tenant_a.transactions`) or a unified catalog schema enforced by dynamic views.

---

## 5. Warehouse Isolation: Snowflake Row-Level & Column-Level Security

When data is loaded into Snowflake, analytical queries from multiple client applications run against shared tables. Zero cross-tenant leakage is enforced by combining **Row Access Policies (RAP)** with **Tag-Based Dynamic Masking**.

```
                           [ Incoming Query ]
                                   │
                                   ▼
             ┌───────────────────────────────────────────┐
             │ Snowflake Row Access Policy (RAP)         │
             │ Evaluates: CURRENT_ROLE() & Tenant Context │
             └─────────────────────┬─────────────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    ▼                             ▼
       [ Matched Tenant Rows Only ]    [ Filtered Out (Hidden) ]
                    │
                    ▼
             ┌───────────────────────────────────────────┐
             │ Snowflake Dynamic Masking Policy          │
             │ Checks: Does user have DECRYPT privilege? │
             └─────────────────────┬─────────────────────┘
                                   │
                    ┌──────────────┴──────────────┐
                    ▼                             ▼
        [ De-Tokenized PII String ]     [ Masked Token: ******* ]

```

### Step 1: Mapping User Sessions to Tenants

Create an immutable entitlements mapping table:

```sql
CREATE OR REPLACE TABLE governance.tenant_entitlements (
    snowflake_role VARCHAR(64),
    tenant_id VARCHAR(64),
    clearance_level VARCHAR(32) -- 'AUDITOR', 'OPERATOR', 'ADMIN'
);

-- Seed mapping: Internal support or external tenant user
INSERT INTO governance.tenant_entitlements VALUES
('ROLE_TENANT_ACME', 'tenant_acme_uuid_101', 'OPERATOR'),
('ROLE_TENANT_GLOBEX', 'tenant_globex_uuid_202', 'OPERATOR'),
('ROLE_GLOBAL_AUDITOR', '*', 'AUDITOR');

```

### Step 2: Implement Row Access Policy (Tenant Boundary Enforcement)

```sql
CREATE OR REPLACE ROW ACCESS POLICY governance.tenant_isolation_policy
AS (record_tenant_id VARCHAR) RETURNS BOOLEAN ->
    -- 1. Full bypass for break-glass administrative role
    CURRENT_ROLE() = 'ACCOUNTADMIN'
    -- 2. Global auditors can scan all rows
    OR EXISTS (
        SELECT 1 FROM governance.tenant_entitlements e
        WHERE e.snowflake_role = CURRENT_ROLE()
          AND e.tenant_id = '*'
    )
    -- 3. Tenant-scoped users can scan ONLY their matching rows
    OR EXISTS (
        SELECT 1 FROM governance.tenant_entitlements e
        WHERE e.snowflake_role = CURRENT_ROLE()
          AND e.tenant_id = record_tenant_id
    );

-- Apply the policy to the shared transactional fact table
ALTER TABLE enterprise_dw.finance.tenant_transactions
ADD ROW ACCESS POLICY governance.tenant_isolation_policy ON (tenant_id);

```

### Step 3: Column Masking & De-Tokenization on the Fly

Tokens stored in the database are masked by default. Authorized roles trigger the decryptor function via **Snowflake External Network Access**:

```sql
-- Dynamic Masking Policy on Tokenized SSN
CREATE OR REPLACE MASKING POLICY governance.mask_tokenized_ssn
AS (token_val STRING) RETURNS STRING ->
    CASE
        -- Fully unmasked/de-tokenized only for authorized compliance officers
        WHEN CURRENT_ROLE() IN ('LEGAL_COMPLIANCE', 'INTERNAL_AUDIT') THEN
            governance.decrypt_ff1_ssn(token_val) -- Secure UDF calling KMS
        -- Masked view for operational users
        WHEN CURRENT_ROLE() LIKE 'ROLE_TENANT_%' THEN
            CONCAT('***-**-', RIGHT(token_val, 4))
        -- Completely redacted for standard analytics roles
        ELSE '***REDACTED***'
    END;

-- Attach masking policy
ALTER TABLE enterprise_dw.finance.tenant_transactions
MODIFY COLUMN ssn SET MASKING POLICY governance.mask_tokenized_ssn;

```

---

## 6. Secure Cryptographic De-Tokenization (Snowflake External Network Access)

To prevent embedding long-lived encryption keys inside database scripts, Snowflake calls an external Lambda decryptor via **Snowflake External Access Integration**:

```sql
-- 1. Create a secure Network Rule pointing to the internal KMS De-Tokenization Gateway
CREATE OR REPLACE NETWORK RULE governance.kms_detokenize_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  VALUE_LIST = ('kms-gateway.internal.enterprise.com:443');

-- 2. Establish External Access Integration
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION kms_detokenize_integration
  ALLOWED_NETWORK_RULES = (governance.kms_detokenize_network_rule)
  ENABLED = TRUE;

-- 3. Define the Secure Decryption UDF
CREATE OR REPLACE SECURE FUNCTION governance.decrypt_ff1_ssn(encrypted_token STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
HANDLER = 'decrypt_payload'
EXTERNAL_ACCESS_INTEGRATIONS = (kms_detokenize_integration)
AS
$$
import urllib3
import json

http = urllib3.PoolManager()

def decrypt_payload(encrypted_token):
    if not encrypted_token:
        return None

    url = "https://kms-gateway.internal.enterprise.com/detokenize"
    req_body = json.dumps({"token": encrypted_token})

    # Secure intra-VPC call to decrypt using Hardware Security Module (HSM)
    res = http.request("POST", url, body=req_body, headers={'Content-Type': 'application/json'})
    return json.loads(res.data.decode('utf-8'))['cleartext']
$$;

```

---

## 7. Performance Impact & Micro-Partition Pruning

Applying Row Access Policies on multi-tenant tables can degrade performance if the table is improperly clustered, forcing full table scans across all tenants.

```
Without Clustering on tenant_id:
[ Query: SELECT * WHERE tenant_id = 'A' ]
  ├── Scans Micro-Partition 1 (Contains A, B, C) -> Scanned!
  ├── Scans Micro-Partition 2 (Contains B, C, D) -> Scanned!
  └── Scans Micro-Partition 3 (Contains A, D)    -> Scanned!
  (Result: Scanned 100% of micro-partitions)

With Clustering Key (tenant_id, transaction_date):
[ Query: SELECT * WHERE tenant_id = 'A' ]
  ├── Micro-Partition 1 (Only Tenant A) -> Scanned!
  ├── Micro-Partition 2 (Only Tenant B) -> Skipped! (Pruned)
  └── Micro-Partition 3 (Only Tenant C) -> Skipped! (Pruned)
  (Result: Scanned only 33% of storage, 3x performance boost)

```

- **Mandatory Clustering:** Always define the first clustering key in Snowflake as `tenant_id`:

```sql
ALTER TABLE enterprise_dw.finance.tenant_transactions
CLUSTER BY (tenant_id, transaction_date);

```

- **Performance Benefit:** Even though the query hits a shared physical table, Snowflake's metadata engine eliminates partitions belonging to other tenants during compile time, preventing noisy-neighbor compute spikes.

---

## 8. Failure Modes & Mitigations

| Failure Vector                             | Security & Architectural Impact                                                                                                  | Mitigation Strategy                                                                                                                                                                                                        |
| ------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Cross-Tenant Leakage via Lateral Joins** | A complex analytical query joins a multi-tenant table to an un-partitioned staging table, exposing unauthorized rows.            | Enforce Row Access Policies globally at the schema level; every derived view or downstream mart must inherit the base table's RAP via Snowflake's automatic policy inheritance.                                            |
| **KMS Request Throttling under Load**      | High-concurrency batch de-tokenization hits KMS API request ceilings ($10{,}000\text{ req/sec}$ limit), failing batch queries.   | Implement **Local Key Caching** inside the decryption engine with a 5-minute TTL. Encrypt/decrypt operations use locally cached data keys (DEKs) wrapped by KMS root keys, cutting external API calls by $> 99\%$.         |
| **Format-Preserving Encryption Collision** | Low-cardinality values produce ciphertext overlaps across distinct tenants.                                                      | Encrypt using **Tweakable Ciphers**: Use the `tenant_id` string as an initialization tweak parameter in the FF1 algorithm. Identical cleartext SSNs across Tenant A and Tenant B generate completely distinct ciphertexts. |
| **Orphaned Tokenization State**            | Ingestion pipeline fails halfway through tokenization, leaving an inconsistent mix of cleartext and tokenized fields in staging. | Pre-commit validation step: Run a regex-based structural audit task. Any row where `_is_tokenized != TRUE` is halted, isolated into a quarantine S3 bucket, and barred from Bronze/Silver promotion.                       |
