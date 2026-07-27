# Comprehensive Developer Guide: Autonomous Agentic AI & RAG with Snowflake Cortex

This guide synthesizes all foundational paradigms, architectural patterns, and production implementations for building autonomous AI systems with **Snowflake Cortex**. It covers:

1. **Autonomous Agents vs. Assistants & Tool Orchestration**
2. **Text-to-SQL & Semantic Models (Cortex Analyst)**
3. **Document Ingestion & RAG (Cortex Search)**
4. **Agent Monitoring, Observability & Prompt Engineering**
5. **Extensibility & Model Context Protocol (MCP)**
6. **Multi-Page Interactive Streamlit Application**

---

## 1. The Paradigm Shift: Assistants vs. Autonomous Agents

```
TRADITIONAL DIRECTED ASSISTANTS                AUTONOMOUS AGENTIC AI (CORTEX AGENTS)
┌───────────────────────────────┐              ┌─────────────────────────────────────────────────┐
│ User: "Run this query/prompt" │              │ User: "Identify reasons for Q2 churn & draft    │
└──────────────┬────────────────┘              │       resolution emails for top accounts."      │
               │                               └────────────────────────┬────────────────────────┘
               ▼                                                        │
┌───────────────────────────────┐                                       ▼
│ Model returns immediate response│              ┌─────────────────────────────────────────────────┐
│ without independent planning. │              │ 1. INTELLIGENT PLANNING: Break goal into tasks   │
└───────────────────────────────┘              │ 2. TOOL CALLING: Access Analyst & Search        │
                                               │ 3. MEMORY & REFLECTION: Evaluate context & retry │
                                               │ 4. AUTONOMOUS EXECUTION: Deliver end result     │
                                               └─────────────────────────────────────────────────┘

```

- **Directed AI Assistants (Ask-and-Receive)**: Require human intervention at every step. They take a prompt and execute a single generation or pre-defined SQL call.
- **Autonomous AI Agents (Goal-Oriented)**: Operate with **autonomy, planning, memory, and tool usage**. Given a high-level goal, an agent formulates execution plans, calls appropriate data tools (structured or unstructured), evaluates intermediate results, and iteratively works toward the objective.

### Core Architectural Pillars

1. **Intelligent Planning**: Deconstructs complex queries into sub-tasks (e.g., retrieving sales metrics via SQL before searching transcript logs for qualitative explanations).
2. **Tool Calling & Execution**: Dynamically interfaces with **Cortex Analyst** (for structured metric aggregation) and **Cortex Search** (for unstructured document retrieval).
3. **Contextual Memory & Reflection**: Maintains conversation state across multi-turn sessions, identifies missing information, and refines query strategies autonomously.

---

## 2. Infrastructure Setup & Data Pipeline

This SQL pipeline sets up the environment, stages data, parses PDFs into layout-aware Markdown, chunks text recursively, and loads structured financial/sales datasets.

```sql
-- ============================================================================
-- 1. ENVIRONMENT & WAREHOUSE CONFIGURATION
-- ============================================================================
CREATE DATABASE IF NOT EXISTS CORTEX_AGENT_PRODUCTION_DB;
CREATE SCHEMA IF NOT EXISTS CORTEX_AGENT_PRODUCTION_DB.SALES_INTELLIGENCE;
USE DATABASE CORTEX_AGENT_PRODUCTION_DB;
USE SCHEMA SALES_INTELLIGENCE;

CREATE OR REPLACE WAREHOUSE CORTEX_AGENT_WH WITH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE CORTEX_AGENT_WH;

-- ============================================================================
-- 2. STAGE DEFINITION & UNSTRUCTURED INGESTION (PDFs)
-- ============================================================================
CREATE STAGE IF NOT EXISTS TRANSCRIPTS_STAGE
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- Extract raw text from uploaded call transcripts using Layout mode
CREATE OR REPLACE TABLE PARSED_TRANSCRIPTS AS
SELECT
    relative_path AS file_name,
    TO_VARCHAR(
        SNOWFLAKE.CORTEX.PARSE_DOCUMENT(
            @TRANSCRIPTS_STAGE,
            relative_path,
            {'mode': 'LAYOUT'}
        ):content
    ) AS parsed_text
FROM DIRECTORY(@TRANSCRIPTS_STAGE)
WHERE relative_path LIKE '%.pdf';

-- Recursive character chunking for vector search (1,800 chars, 250 overlap)
CREATE OR REPLACE TABLE CHUNKED_TRANSCRIPTS (
    file_name VARCHAR,
    chunk VARCHAR
);

INSERT INTO CHUNKED_TRANSCRIPTS (file_name, chunk)
SELECT
    file_name,
    c.value AS chunk
FROM
    PARSED_TRANSCRIPTS,
    LATERAL FLATTEN(
        input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
            parsed_text,
            'markdown',
            1800,
            250
        )
    ) c;

-- Search Service over transcripts
CREATE OR REPLACE CORTEX SEARCH SERVICE TRANSCRIPT_SEARCH_SERVICE
    ON chunk
    WAREHOUSE = CORTEX_AGENT_WH
    TARGET_LAG = '1 minute'
    EMBEDDING_MODEL = 'snowflake-arctic-embed-l-v2.0'
    AS (
        SELECT file_name, chunk
        FROM CHUNKED_TRANSCRIPTS
    );

-- ============================================================================
-- 3. STRUCTURED TABLES FOR CORTEX ANALYST
-- ============================================================================
CREATE OR REPLACE TABLE CUSTOMER_DIM (
    customer_id INT,
    customer_name VARCHAR(100),
    industry VARCHAR(50)
);

CREATE OR REPLACE TABLE PRODUCT_DIM (
    product_id INT,
    product_line VARCHAR(50)
);

CREATE OR REPLACE TABLE SALES_PERFORMANCE (
    sales_id INT,
    customer_id INT,
    product_id INT,
    sales_rep VARCHAR(50),
    stage VARCHAR(30),
    deal_amount FLOAT,
    is_won BOOLEAN,
    close_date DATE
);

-- Search Service for Literal Matching in Semantic Dimensions
CREATE OR REPLACE CORTEX SEARCH SERVICE PRODUCT_LINE_SEARCH_SERVICE
    ON product_dimension
    WAREHOUSE = CORTEX_AGENT_WH
    TARGET_LAG = '1 hour'
    AS (
        SELECT DISTINCT product_line AS product_dimension FROM PRODUCT_DIM
    );

```

---

## 3. Semantic Specification for Structured Metrics

Save this specification as `sales_intelligence_model.yaml` in your Snowflake Stage (`@TRANSCRIPTS_STAGE/sales_intelligence_model.yaml`).

```yaml
name: Sales_Intelligence_Semantic_Model
description: Structured sales metrics, win rates, and deal pipeline dimensions.

tables:
  - name: product
    base_table:
      database: CORTEX_AGENT_PRODUCTION_DB
      schema: SALES_INTELLIGENCE
      table: PRODUCT_DIM
    primary_key:
      columns: [product_id]
    dimensions:
      - name: product_id
        expr: product_id
        data_type: number
      - name: product_line
        expr: product_line
        cortex_search_service_name: PRODUCT_LINE_SEARCH_SERVICE
        synonyms: ["category", "offering", "product family"]
        data_type: varchar

  - name: customer
    base_table:
      database: CORTEX_AGENT_PRODUCTION_DB
      schema: SALES_INTELLIGENCE
      table: CUSTOMER_DIM
    primary_key:
      columns: [customer_id]
    dimensions:
      - name: customer_id
        expr: customer_id
        data_type: number
      - name: customer_name
        expr: customer_name
        synonyms: ["account", "client"]
        data_type: varchar

  - name: sales_performance
    base_table:
      database: CORTEX_AGENT_PRODUCTION_DB
      schema: SALES_INTELLIGENCE
      table: SALES_PERFORMANCE
    primary_key:
      columns: [sales_id]
    time_dimensions:
      - name: close_date
        expr: close_date
        data_type: date
    dimensions:
      - name: sales_rep
        expr: sales_rep
        synonyms: ["account executive", "rep", "owner"]
        data_type: varchar
      - name: stage
        expr: stage
        data_type: varchar
      - name: is_won
        expr: is_won
        data_type: boolean
    measures:
      - name: total_deal_amount
        expr: deal_amount
        synonyms: ["revenue", "pipeline value", "booking amount"]
        default_aggregation: sum
        data_type: number
      - name: win_rate
        expr: AVG(CASE WHEN is_won THEN 1.0 ELSE 0.0 END)
        description: Ratio of closed won deals over total closed opportunities.
        synonyms: ["conversion rate", "win ratio"]
        data_type: number

relationships:
  - name: sales_to_product
    left_table: sales_performance
    right_table: product
    relationship_columns:
      - left_column: product_id
        right_column: product_id
    join_type: left_outer
    relationship_type: many_to_one

  - name: sales_to_customer
    left_table: sales_performance
    right_table: customer
    relationship_columns:
      - left_column: customer_id
        right_column: customer_id
    join_type: left_outer
    relationship_type: many_to_one

custom_instructions: "Always default to calculating win rates as a percentage formatted value when requested."

verified_queries:
  - name: "win_rate_by_product_line"
    question: "What is the win rate by product line?"
    verified_at: 1738020395
    verified_by: "james_dev_advocate"
    sql: "
      SELECT
        p.product_line,
        AVG(CASE WHEN s.is_won THEN 1.0 ELSE 0.0 END) * 100 AS win_rate_percentage,
        SUM(s.deal_amount) AS total_pipeline
      FROM CORTEX_AGENT_PRODUCTION_DB.SALES_INTELLIGENCE.SALES_PERFORMANCE s
      LEFT JOIN CORTEX_AGENT_PRODUCTION_DB.SALES_INTELLIGENCE.PRODUCT_DIM p
        ON s.product_id = p.product_id
      GROUP BY p.product_line
      ORDER BY win_rate_percentage DESC;
    "

```

---

## 4. Production Prompt Engineering & Orchestration Strategy

Agent reliability is dictated by the **Three Pillars of Agent Instructions**:

```
                              ┌─────────────────────────────────────────┐
                              │            AGENT INSTRUCTION            │
                              │               HIERARCHY                 │
                              └────────────────────┬────────────────────┘
                                                   │
         ┌─────────────────────────────────────────┼─────────────────────────────────────────┐
         │                                         │                                         │
         ▼                                         ▼                                         ▼
┌─────────────────────────────────┐   ┌─────────────────────────────────┐   ┌─────────────────────────────────┐
│   1. ORCHESTRATION INSTRUCTIONS │   │    2. RESPONSE INSTRUCTIONS     │   │     3. SYSTEM BOUNDARIES        │
│  (Tool Selection & Hierarchies) │   │ (Output Formatting & Citations) │   │ (Scope Guardrails & Compliance) │
└─────────────────────────────────┘   └─────────────────────────────────┘   └─────────────────────────────────┘

```

```python
ORCHESTRATION_INSTRUCTIONS = """
You are an autonomous Sales Intelligence Agent for Snowflake.
Your primary objective is to evaluate deal progression, calculate metrics, and extract customer call transcript insights.

TOOL SELECTION RULES:
1. For quantitative metrics, win rates, revenue totals, or rep performance:
   --> ALWAYS call `cortex_analyst_tool` FIRST.
2. For qualitative questions regarding customer objections, sentiment, feature requests, or competitive mentions:
   --> ALWAYS call `cortex_search_tool` FIRST.
3. For hybrid queries (e.g., "Why is the win rate low for Product X?"):
   --> STEP 1: Query `cortex_analyst_tool` to compute the quantitative metric.
   --> STEP 2: Use the output labels to query `cortex_search_tool` to retrieve relevant transcript passages.
   --> STEP 3: Combine both data sources into a unified response.
"""

RESPONSE_INSTRUCTIONS = """
1. Structure all executive answers using Markdown headings, key bullet points, and summary metrics.
2. Every claim sourced from transcript calls MUST include an explicit source citation in brackets (e.g., [File: transcript_2026_q1.pdf]).
3. Format numerical metrics cleanly (e.g., Currency as $XX,XXX, Win rates as XX.X%).
"""

SYSTEM_BOUNDARY_INSTRUCTIONS = """
1. Do not invent or hallucinate metrics or transcripts outside the scope of retrieved tool results.
2. If a query falls outside sales, customer transcripts, or pipeline metrics, reply: "I am authorized to assist only with sales metrics and transcript intelligence."
"""

```

---

## 5. Agent Monitoring, Debugging & Systematic Iteration Loop

Production agent deployments require continuous observability to debug planning failures, improper tool selection, or incorrect outputs.

```
       ┌───────────────────────────────────────────────────────────────┐
       │                   1. MONITOR INVOCATION TRACES                │
       │     (Read from SNOWFLAKE.LOCAL.AI_OBSERVABILITY_EVENTS)       │
       └───────────────────────────────┬───────────────────────────────┘
                                       │
                                       ▼
       ┌───────────────────────────────────────────────────────────────┐
       │                  2. IDENTIFY FAILURE ROOT CAUSE               │
       │ ┌──────────────────────┬────────────────────────────────────┐ │
       │ │ Tool Selection Issue  │ Update Orchestration Instructions   │ │
       │ │ Poor Output Formatting│ Update Response Instructions        │ │
       │ │ Hallucination/Out-Scope│ Update System Boundaries           │ │
       │ └──────────────────────┴────────────────────────────────────┘ │
       └───────────────────────────────┬───────────────────────────────┘
                                       │
                                       ▼
       ┌───────────────────────────────────────────────────────────────┐
       │                   3. RETEST, VERIFY & DEPLOY                  │
       │       (Run test suites against updated agent instruction)     │
       └───────────────────────────────────────────────────────────────┘

```

### Accessing AI Observability Traces via SQL

Snowflake records event traces for Cortex Agents inside the Event Table infrastructure:

```sql
-- Query agent execution traces for debugging tool calls and planning latency
SELECT
    TIMESTAMP,
    RECORD_ATTRIBUTES['app.name']::STRING AS agent_name,
    RECORD_ATTRIBUTES['db.system']::STRING AS target_system,
    RECORD_ATTRIBUTES['cortex.tool.selected']::STRING AS tool_called,
    RECORD_ATTRIBUTES['cortex.planning.step']::STRING AS execution_plan,
    RECORD_ATTRIBUTES['cortex.response.duration_ms']::INT AS duration_ms
FROM SNOWFLAKE.LOCAL.AI_OBSERVABILITY_EVENTS
WHERE RECORD_ATTRIBUTES['app.name'] = 'sales_intelligence_agent'
ORDER BY TIMESTAMP DESC
LIMIT 50;

```

---

## 6. Model Context Protocol (MCP) Integration

The **Model Context Protocol (MCP)** is an open standard that allows Cortex Agents to connect securely to external clients and environments (such as **Cursor**, **Claude Desktop**, or custom tools).

```
┌───────────────────────────┐           MCP Standard (JSON-RPC)           ┌───────────────────────────┐
│     EXTERNAL MCP CLIENT   │ ◄─────────────────────────────────────────► │ SNOWFLAKE MANAGED MCP     │
│  (e.g., Cursor IDE, Slack)│                                             │         SERVER            │
└───────────────────────────┘                                             └─────────────┬─────────────┘
                                                                                        │
                                                                                        ▼
                                                                          ┌───────────────────────────┐
                                                                          │       CORTEX AGENT        │
                                                                          │ (Analyst + Search Tools)  │
                                                                          └───────────────────────────┘

```

### Managed MCP Server Configuration (`mcp_config.json`)

To connect external development environments like **Cursor** to your Snowflake Managed MCP Server, configure the client JSON manifest as follows:

```json
{
  "mcpServers": {
    "snowflake-sales-agent": {
      "command": "npx",
      "args": [
        "-y",
        "@snowflake/mcp-server-snowflake",
        "--account",
        "<YOUR_SNOWFLAKE_ACCOUNT_LOCATOR>",
        "--user",
        "<YOUR_USER_NAME>",
        "--role",
        "CORTEX_USER_ROLE",
        "--warehouse",
        "CORTEX_AGENT_WH",
        "--database",
        "CORTEX_AGENT_PRODUCTION_DB",
        "--schema",
        "SALES_INTELLIGENCE",
        "--agent",
        "sales_intelligence_agent"
      ],
      "env": {
        "SNOWFLAKE_AUTHENTICATOR": "externalbrowser"
      }
    }
  }
}
```

---

## 7. Multi-Page Production Streamlit Application

This Streamlit application combines **Structured Metrics Querying**, **Unstructured Transcript RAG Search**, and the **Autonomous Orchestration Agent**.

```python
# ============================================================================
# STREAMLIT IN SNOWFLAKE: PRODUCTION SALES INTELLIGENCE AGENT APP
# ============================================================================
import json
import textwrap
import _snowflake
import pandas as pd
import streamlit as st
from snowflake.core import Root
from snowflake.snowpark.session import Session
from snowflake.snowpark.context import get_active_session
from snowflake.cortex import complete

# ----------------------------------------------------------------------------
# 1. PAGE SETUP & SESSION INITIALIZATION
# ----------------------------------------------------------------------------
st.set_page_config(page_title="Snowflake Autonomous Agent Center", layout="wide")
session = get_active_session()

API_ENDPOINT = "/api/v2/cortex/analyst/message"
API_TIMEOUT = 50000

# Session states
if "agent_history" not in st.session_state:
    st.session_state.agent_history = []
if "analyst_payload" not in st.session_state:
    st.session_state.analyst_payload = []

# ----------------------------------------------------------------------------
# 2. TOOL MODULE 1: STRUCTURED METRICS (CORTEX ANALYST)
# ----------------------------------------------------------------------------
def call_cortex_analyst(payload):
    request_body = {
        "messages": payload,
        "semantic_model_file": '@"CORTEX_AGENT_PRODUCTION_DB"."SALES_INTELLIGENCE"."TRANSCRIPTS_STAGE"/sales_intelligence_model.yaml'
    }
    resp = _snowflake.send_snow_api_request("POST", API_ENDPOINT, {}, {}, request_body, None, API_TIMEOUT)
    return json.loads(resp["content"])

def render_analyst_view():
    st.header("📊 Sales Metrics & Win Rates (Structured Data)")
    st.caption("Driven by Cortex Analyst & Semantic View Definitions")

    user_query = st.text_input("Ask a quantitative sales question:", "What is the win rate by product line?")

    if st.button("Query Metrics", type="primary"):
        payload = [{"role": "user", "content": [{"type": "text", "text": user_query}]}]
        with st.spinner("Analyzing data models..."):
            res = call_cortex_analyst(payload)

        if "message" in res:
            contents = res["message"].get("content", [])
            for item in contents:
                if item.get("type") == "text":
                    st.info(item["text"])
                elif item.get("type") == "sql":
                    sql_stmt = item["statement"]
                    with st.expander("Generated SQL", expanded=True):
                        st.code(sql_stmt, language="sql")
                    df = session.sql(sql_stmt.replace(";", "")).to_pandas()
                    st.dataframe(df)

# ----------------------------------------------------------------------------
# 3. TOOL MODULE 2: UNSTRUCTURED RAG (CORTEX SEARCH)
# ----------------------------------------------------------------------------
class CortexSearchRetriever:
    def __init__(self, snowpark_session: Session, limit: int = 3):
        self._session = snowpark_session
        self._limit = limit

    def retrieve(self, query: str):
        root = Root(self._session)
        service = (
            root.databases["CORTEX_AGENT_PRODUCTION_DB"]
            .schemas["SALES_INTELLIGENCE"]
            .cortex_search_services["TRANSCRIPT_SEARCH_SERVICE"]
        )
        resp = service.search(query=query, columns=["chunk", "file_name"], limit=self._limit)
        return resp.results if resp.results else []

def render_search_view():
    st.header("📄 Customer Call Transcripts Search (Unstructured Data)")
    st.caption("Hybrid Semantic + Keyword Vector Search via Cortex Search")

    retriever = CortexSearchRetriever(session)
    search_query = st.text_input("Search customer transcripts:", "What concerns were raised about pricing or integration?")

    if st.button("Search Transcripts", type="primary"):
        with st.spinner("Searching vector index..."):
            results = retriever.retrieve(search_query)
            for i, r in enumerate(results):
                with st.expander(f"Result {i+1} - Source: {r.get('file_name', 'Unknown')}"):
                    st.write(r["chunk"])

# ----------------------------------------------------------------------------
# 4. TOOL MODULE 3: AUTONOMOUS ORCHESTRATION AGENT
# ----------------------------------------------------------------------------
def run_autonomous_agent(user_goal: str):
    retriever = CortexSearchRetriever(session, limit=3)

    # STEP 1: Determine Tool Plan using Prompt Instructions
    orchestration_prompt = f"""
    You are an Autonomous Orchestrator Agent.
    Goal: "{user_goal}"

    Formulate a step-by-step plan:
    - Step 1: Does this require structured metrics? (Yes/No)
    - Step 2: Does this require searching unstructured call transcripts? (Yes/No)
    Explain tool sequence.
    """

    plan_response = complete("claude-3-5-sonnet", orchestration_prompt)
    st.markdown("### 🧠 Agent Execution Plan")
    st.info(plan_response)

    # STEP 2: Execute Tool Operations
    structured_context = ""
    unstructured_context = ""

    # Query Analyst if quantitative terms exist
    if any(k in user_goal.lower() for k in ["win rate", "metric", "revenue", "pipeline", "rep", "performance"]):
        with st.spinner("Executing Tool 1: Cortex Analyst..."):
            analyst_res = call_cortex_analyst([{"role": "user", "content": [{"type": "text", "text": user_goal}]}])
            if "message" in analyst_res:
                for item in analyst_res["message"].get("content", []):
                    if item.get("type") == "sql":
                        df = session.sql(item["statement"].replace(";", "")).to_pandas()
                        structured_context = df.to_markdown(index=False)
                        st.markdown("**Tool Output (Cortex Analyst):**")
                        st.dataframe(df)

    # Query Search Service if qualitative terms exist
    if any(k in user_goal.lower() for k in ["concern", "customer", "transcript", "objection", "reason", "why"]):
        with st.spinner("Executing Tool 2: Cortex Search..."):
            docs = retriever.retrieve(user_goal)
            unstructured_context = "\n---\n".join([f"[Source: {d.get('file_name')}] {d['chunk']}" for d in docs])
            st.markdown("**Tool Output (Cortex Search):**")
            with st.expander("Retrieved Passages"):
                st.write(unstructured_context)

    # STEP 3: Synthesize Final Intelligence Response
    final_synthesis_prompt = f"""
    System Instructions: Combine structured metrics and unstructured passages into a coherent response.

    Structured Data Context:
    {structured_context if structured_context else "N/A"}

    Unstructured Transcript Context:
    {unstructured_context if unstructured_context else "N/A"}

    User Query: {user_goal}

    Generate a detailed response adhering to formatting instructions and cite document sources.
    """

    with st.spinner("Synthesizing autonomous insights..."):
        stream = complete("claude-3-5-sonnet", final_synthesis_prompt, stream=True)
        st.markdown("### 🎯 Final Sourced Intelligence Answer")
        final_answer = st.write_stream(stream)

    return final_answer

def render_agent_view():
    st.header("🤖 Autonomous Sales Intelligence Agent")
    st.caption("Autonomously orchestrates between Cortex Analyst and Search")

    for msg in st.session_state.agent_history:
        st.chat_message(msg["role"]).write(msg["content"])

    user_input = st.chat_input("E.g., What is our win rate by product line, and what objections are customers raising in calls?")
    if user_input:
        st.chat_message("user").write(user_input)
        st.session_state.agent_history.append({"role": "user", "content": user_input})

        answer = run_autonomous_agent(user_input)
        st.session_state.agent_history.append({"role": "assistant", "content": answer})

# ----------------------------------------------------------------------------
# 5. NAVIGATION ROUTER
# ----------------------------------------------------------------------------
navigation_pages = {
    "Autonomous Agent Hub": render_agent_view,
    "Structured Metrics (Cortex Analyst)": render_analyst_view,
    "Transcript RAG (Cortex Search)": render_search_view
}

selected_page = st.sidebar.radio("Navigate Application Modules", list(navigation_pages.keys()))
navigation_pages[selected_page]()

```

---

## 8. Summary Comparison Matrix

| Component              | Cortex Analyst (Text-to-SQL)           | Cortex Search (RAG)                   | Cortex Autonomous Agent                 |
| ---------------------- | -------------------------------------- | ------------------------------------- | --------------------------------------- |
| **Input Data**         | Relational tables (Facts & Dimensions) | Unstructured files (PDFs, Docs, Logs) | Multi-modal context & tool outputs      |
| **Primary Artifact**   | YAML Semantic Data Model               | Hybrid Search Index                   | Orchestration & Boundary Instructions   |
| **Operational Output** | Deterministic SQL & DataFrames         | Contextually relevant passage chunks  | Sourced, end-to-end business insights   |
| **Key Advantage**      | High precision on structured numbers   | Flexible retrieval over text          | Full autonomy across diverse data tools |
| **Monitoring Tool**    | Query History Logs                     | Target Lag Sync State                 | `AI_OBSERVABILITY_EVENTS` Event Table   |
