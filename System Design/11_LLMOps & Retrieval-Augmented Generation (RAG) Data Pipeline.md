# Scenario 11: LLMOps & Retrieval-Augmented Generation (RAG) Data Pipeline

This design establishes an enterprise-grade data preparation and indexing platform for Generative AI. It transforms unstructured documents (PDFs, TIFF scans, audio call transcripts, markdown runbooks) into chunked, enriched, and cryptographically verified vector embeddings. The resulting vectors are indexed into **Snowflake Cortex Search** and **Databricks Vector Search**, governed by strict Document-Level Access Control (DLAC) and orchestrated by **AWS MWAA**.

---

## 1. System Requirements & GenAI Data SLOs

- **Scale & Throughput:** Ingest $200{,}000+$ multi-page technical documents, legal contracts, and call center transcripts daily ($1\text{--}2\text{ TB/day}$).
- **Index Freshness SLA:** Newly finalized documents landing in S3 must be parsed, vectorized, and queryable in RAG retrieval endpoints within **$< 15$ minutes**.
- **Information Retrieval Quality:** Prevent text truncation across tables/headers; maintain parent-child document lineage to feed LLMs with context windows ($4\text{k--}32\text{k}$ tokens).
- **Document-Level Access Control (DLAC):** Zero cross-department information leakage. Vector lookups by an engineer must never surface internal HR compensation documents or restricted legal briefs.
- **Embedding Drift Mitigation:** Support zero-downtime, blue/green re-indexing when upgrading vector models (e.g., migrating from `text-embedding-3-small` to `text-embedding-3-large` or an open-source model).

---

## 2. End-to-End RAG Architecture

```
[ Unstructured Sources: PDFs, Legal Scans, Call Center Audio ]
                            │
                            ▼
[ Ingestion & Landing: Amazon S3 (Raw Documents Bucket) ]
   s3://enterprise-rag-raw/{department}/{year}/{month}/{uuid}.pdf
                            │
                            │ (S3 ObjectCreated EventBridge Notification)
                            ▼
[ Document Extraction & OCR: AWS Lambda / Amazon Textract ]
   - Async PDF parser / Layout-aware OCR
   - Extracts layout boundaries, Markdown tables, and text
                            │
                            ▼
[ Staging Zone: S3 Extracted Markdown/JSON ]
   s3://enterprise-rag-stage/parsed/{doc_id}.json
                            │
                            ▼
[ Distributed Chunking & Embedding: Databricks PySpark ]
   - Recursive character chunking with semantic heading preservation
   - Token-length boundary enforcement
   - Distributed GPU / Model Inference (MLflow / Hugging Face / vLLM)
   - Generates Dense Vectors (e.g., 1536-dimensional float arrays)
                            │
       ┌────────────────────┴────────────────────┐
       ▼                                         ▼
[ Curated Delta Lakehouse Layer ]       [ Snowflake Cortex Vector Engine ]
  - Chunk ID + Parent Document Lineage    - Tables with native VECTOR data type
  - Text Chunk Payload                    - Cortex Vector Search Service
  - Embedding Vector (Dense Array)        - Row-Level Access Policies (DLAC)
  - Metadata: Department, Auth Clearance  - Hybrid Search (Vector + Keyword)
                            │
                            ▼
[ Orchestration & Governance: AWS MWAA (Airflow) ]
  - Triggers daily chunk compaction & re-indexing
  - Monitors embedding drift and orphan chunk cleanup

```

---

## 3. Layout-Aware Document Extraction (AWS Textract + Lambda)

Naive text splitters destroy the semantic meaning of tables, multi-column articles, and headers. The extraction step converts binary documents into **layout-aware markdown**.

```python
import json
import boto3
import os

textract_client = boto3.client('textract', region_name='us-east-1')
s3_client = boto3.client('s3')
PARSED_STAGE_BUCKET = os.environ['STAGE_BUCKET']

def lambda_handler(event, context):
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = record['s3']['object']['key']
        doc_id = key.split('/')[-1].replace('.pdf', '')
        department = key.split('/')[0]

        # Start asynchronous layout and table extraction
        response = textract_client.start_document_analysis(
            DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}},
            FeatureTypes=['TABLES', 'LAYOUT']
        )
        job_id = response['JobId']

        # Store intermediate tracking payload in DynamoDB/SQS for completion polling
        payload = {
            "job_id": job_id,
            "doc_id": doc_id,
            "department": department,
            "s3_raw_source": f"s3://{bucket}/{key}"
        }
        # Once textract completes, table cells are serialized into HTML/Markdown
        # e.g., <table><tr><td>Revenue</td><td>$1.2M</td></tr></table>
        # Saved to: s3://enterprise-rag-stage/parsed/{doc_id}.json

```

---

## 4. Chunking Strategy & Lineage Preservation

Choosing the wrong chunking pattern causes RAG systems to return fragmented facts or hallucinate due to missing context.

| Strategy                         | Chunk Size & Overlap                               | Best For                                             | Risks / Trade-offs                                       |
| -------------------------------- | -------------------------------------------------- | ---------------------------------------------------- | -------------------------------------------------------- |
| **Fixed-Size Chunking**          | 512 tokens, 50-token overlap                       | Quick baselines, uniform text.                       | Splits sentences, numbers, and SQL snippets mid-thought. |
| **Semantic / Markdown Chunking** | Variable (200–1000 tokens), breaks on `## Headers` | Technical documentation, user runbooks, policy docs. | Uneven vector densities; requires clean source markdown. |
| **Hierarchical (Parent-Child)**  | **Child:** 200 tokens (for embedding lookup).<br>  |

<br>**Parent:** 2000 tokens (retrieved for LLM context). | High-precision retrieval across complex contracts. | Requires maintaining two sets of IDs and slightly higher storage footprint. |

### Distributed Chunking Implementation in PySpark

```python
from pyspark.sql import functions as F
from pyspark.sql.types import *
import tiktoken

# Define schema for chunks
chunk_schema = ArrayType(StructType([
    StructField("chunk_id", StringType(), False),
    StructField("chunk_index", IntegerType(), False),
    StructField("chunk_text", StringType(), False),
    StructField("token_count", IntegerType(), False)
]))

def recursive_token_chunker(text: str, doc_id: str, max_tokens: int = 512, overlap: int = 64):
    enc = tiktoken.get_encoding("cl100k_base")
    tokens = enc.encode(text)
    chunks = []
    start = 0
    idx = 0

    while start < len(tokens):
        end = min(start + max_tokens, len(tokens))
        chunk_tokens = tokens[start:end]
        chunk_text = enc.decode(chunk_tokens)

        chunks.append({
            "chunk_id": f"{doc_id}#c{idx}",
            "chunk_index": idx,
            "chunk_text": chunk_text,
            "token_count": len(chunk_tokens)
        })

        if end == len(tokens):
            break
        start += (max_tokens - overlap)
        idx += 1

    return chunks

chunk_udf = F.udf(recursive_token_chunker, chunk_schema)

# Process raw staged files
staged_docs_df = spark.read.json("s3://enterprise-rag-stage/parsed/*.json")

chunked_df = staged_docs_df.withColumn(
    "chunks",
    chunk_udf(F.col("extracted_markdown"), F.col("doc_id"))
).select(
    F.col("doc_id"),
    F.col("department"),
    F.col("classification_level"),
    F.explode("chunks").alias("chunk")
).select(
    F.col("doc_id"),
    F.col("department"),
    F.col("classification_level"),
    F.col("chunk.chunk_id").alias("chunk_id"),
    F.col("chunk.chunk_index").alias("chunk_index"),
    F.col("chunk.chunk_text").alias("chunk_text"),
    F.col("chunk.token_count").alias("token_count"),
    F.current_timestamp().alias("processed_at")
)

```

---

## 5. Distributed Vector Embedding Generation (Databricks)

Computing vector embeddings sequentially via external REST endpoints for $200\text{k}$ documents bottlenecks on HTTP connection latency and rate limits. The compute tier uses **PySpark with GPU worker nodes** or micro-batched **MLflow Model Serving** to calculate embeddings in parallel.

```python
from pyspark.sql.functions import pandas_udf
import pandas as pd
from sentence_transformers import SentenceTransformer

# Broadcast embedding model name to workers
MODEL_NAME = "BAAI/bge-large-en-v1.5" # 1024-dimensional dense vectors

@pandas_udf(ArrayType(FloatType()))
def compute_embeddings_gpu(batch_iter: pd.Series) -> pd.Series:
    # Model loaded once per executor GPU worker
    model = SentenceTransformer(MODEL_NAME, device="cuda")
    embeddings = []

    # Process text in worker memory batches
    for texts in batch_iter:
        vectors = model.encode(texts.tolist(), normalize_embeddings=True, show_progress_bar=False)
        embeddings.append(vectors.tolist())

    return pd.Series(embeddings)

# Generate dense vector column
vectorized_df = chunked_df.withColumn("embedding", compute_embeddings_gpu(F.col("chunk_text")))

# Write to curated Gold Delta Table with Deletion Vectors
(
    vectorized_df.write
    .format("delta")
    .mode("append")
    .option("delta.enableDeletionVectors", "true")
    .saveAsTable("enterprise_lakehouse_gold.rag_knowledge_base")
)

```

---

## 6. Hybrid Indexing & Document-Level Access Control (Snowflake Cortex)

Vectors and metadata are synced to **Snowflake** to power Cortex Vector Search. Because enterprise search involves sensitive data, access is governed using **Row-Level Access Policies (RAP)** based on the querying user's authorization claims.

### 1. Snowflake Table DDL with Native Vector Type

```sql
CREATE OR REPLACE TABLE enterprise_dw.rag.document_embeddings (
    chunk_id VARCHAR(128) PRIMARY KEY,
    doc_id VARCHAR(64),
    department VARCHAR(32),
    classification_level VARCHAR(16), -- 'PUBLIC', 'INTERNAL', 'RESTRICTED'
    chunk_text STRING,
    embedding VECTOR(FLOAT, 1024),
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

```

### 2. Document-Level Access Control Policy (DLAC)

```sql
-- Create Row Access Policy checking user department claims
CREATE OR REPLACE ROW ACCESS POLICY enterprise_dw.rag.rag_security_policy
AS (department_tag VARCHAR, classification_level VARCHAR) RETURNS BOOLEAN ->
    CURRENT_ROLE() = 'ACCOUNTADMIN'
    OR (
        classification_level IN ('PUBLIC', 'INTERNAL')
        AND CURRENT_ROLE() IN ('DATA_ANALYST', 'ENGINEERING_USER')
    )
    OR (
        classification_level = 'RESTRICTED'
        AND CURRENT_ROLE() = 'LEGAL_COMPLIANCE'
        AND department_tag = 'LEGAL'
    );

-- Apply policy to the embedding table
ALTER TABLE enterprise_dw.rag.document_embeddings
ADD ROW ACCESS POLICY enterprise_dw.rag.rag_security_policy
ON (department, classification_level);

```

### 3. Cortex Hybrid Retrieval Query

Combines vector cosine similarity with full-text keyword search (`BM25`) directly in SQL:

```sql
-- User runs a question via RAG retrieval query
WITH vector_search AS (
    SELECT
        chunk_id,
        chunk_text,
        VECTOR_COSINE_SIMILARITY(
            embedding,
            SNOWFLAKE.CORTEX.EMBED_TEXT_1024('bge-large-en-v1.5', 'What are our standard liabilities in vendor agreements?')
        ) AS similarity_score
    FROM enterprise_dw.rag.document_embeddings
    ORDER BY similarity_score DESC
    LIMIT 20
)
SELECT
    chunk_id,
    chunk_text,
    similarity_score
FROM vector_search
WHERE similarity_score >= 0.75;

```

---

## 7. Model Versioning & Zero-Downtime Re-Embedding

Embedding models evolve rapidly. When upgrading the embedding model, changing the dimensionality or internal weights invalidates all existing cosine distances.

```
[ Active Production Index: bge-large-en-v1.5 (1024 dim) ]  <-- Serves Live RAG Queries
                               │
                (Airflow triggers backfill DAG)
                               │
                               ▼
[ Blue/Green Index Build: text-embedding-3-large (3072 dim) ]
   - Reprocesses Delta Bronze/Silver parsed documents
   - Generates new embeddings into: document_embeddings_v2
   - Validates recall and latency benchmarks
                               │
                               ▼
[ Zero-Downtime Atomic Cutover ]
   ALTER TABLE enterprise_dw.rag.document_embeddings SWAP WITH document_embeddings_v2;

```

- **Lineage Tracking:** Store `embedding_model_id` directly in the Delta Lake transaction metadata.
- **Orchestration via MWAA:** An Airflow DAG reads the extraction catalog, runs the chunking & embedding pipeline against the new model, validates similarity distribution checks, and executes an atomic metadata pointer swap in Snowflake Cortex with zero client downtime.

---

## 8. Failure Modes & Mitigations

| Failure Vector                          | Production Impact                                                                                        | Engineering Mitigation                                                                                                                                          |
| --------------------------------------- | -------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **OCR Hallucinations on Scans**         | Garbled characters generate noisy embeddings, poisoning similarity rankings.                             | Use confidence score thresholds from Textract. Any page with mean block confidence $< 85\%$ is flagged and routed to human-in-the-loop review via Amazon A2I.   |
| **Token Truncation Failures**           | Sentences or tables exceed embedding model context bounds, dropping critical tail context.               | Enforce rigid pre-embedding checks in PySpark using `tiktoken`. Hard-truncate and sub-chunk with overlaps before invoking the vector model.                     |
| **Document Deletion Desynchronization** | A document is deleted from S3, but its vector chunks linger in the index, returning deleted PII to LLMs. | Integrate with **Scenario 6 Deletion Vectors**: Deletion of a parent `doc_id` cascades an immediate delete across `rag.document_embeddings WHERE doc_id = ...`. |
| **Cosine Drift Across Chunk Lengths**   | Shorter chunks appear artificially closer in vector space than longer contextual chunks.                 | Normalize all embedding vectors ($L_2$ norm) prior to persistence. Use Snowflake's `VECTOR_COSINE_SIMILARITY` or dot product on unit-normalized vectors.        |
