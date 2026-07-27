# Upstream Data Architecture Guide: Migrating Tableau Calculations to SQL & Tableau Prep

_A Production Engineering Guide for Moving VizQL Computation to Database Views and ETL Pipelines_

---

## 1. Architectural Strategy: Why Push Calculations Upstream?

Pushing complex calculations from Tableau’s runtime engine (VizQL / Hyper) upstream into SQL database views or ETL workflows (**Tableau Prep Builder**) significantly optimizes overall system architecture.

```
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                                PERFORMANCE & SCALABILITY                               │
│                                                                                        │
│   BEFORE (VizQL Runtime Engine Heavy)                                                  │
│   ┌──────────────────────┐    Calculates LODs/SLAs    ┌────────────────────────┐   │
│   │ RAW FACT/DIM TABLES  ├───────────────────────────►│ TABLEAU DESKTOP/SERVER │   │
│   │ (Un-aggregated Rows) │    On-the-fly Every Render  │ (VizQL CPU Spikes)     │   │
│   └──────────────────────┘                            └────────────────────────┘   │
│                                                                                        │
│   AFTER (Upstream Pre-computation via SQL/Prep)                                        │
│   ┌──────────────────────┐    Pre-calculated Attributes┌────────────────────────┐   │
│   │ SQL VIEW / PREP FLOW ├───────────────────────────►│ TABLEAU DESKTOP/SERVER │   │
│   │ (Materialized/Indexed)│   Instant Column Vector   │ (Fast Scene Rendering) │   │
│   └──────────────────────┘                            └────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────────────┘

```

### Key Engineering Benefits

1. **Eliminates VizQL CPU Spikes:** `FIXED` Level of Detail (LOD) expressions generate temporary `GROUP BY` subqueries at runtime. Pre-calculating them eliminates dynamic subquery overhead.
2. **Standardizes Business Logic Across Tools:** Moving logic to SQL database views ensures Tableau, Power BI, Python scripts, and SQL reports consume identical metric definitions.
3. **Optimizes Indexing & Materialization:** SQL engines can leverage materialized views, index structures, and partition strategies that runtime Tableau expressions cannot utilize.

---

## 2. Migration 1: Cohort Retention & Lifetime Value (LTV)

### Tableau LOD Expression Logic (Runtime)

In Tableau, customer cohort analysis relies on nested Level of Detail expressions:

- `[Customer_First_Purchase_Month]` = `{ FIXED [Customer ID] : MIN(DATETRUNC('month', [Order Date])) }`
- `[Cohort_Customer_Count]` = `{ FIXED [Customer_First_Purchase_Month] : COUNTD([Customer ID]) }`

---

### Upstream Pattern A: Native ANSI-SQL View Implementation

Using SQL Window Functions (`MIN() OVER(...)` and `COUNT(DISTINCT) OVER(...)`) allows the database to calculate cohort attributes in a single pass.

```sql
CREATE OR REPLACE VIEW vw_customer_cohort_ltv AS
WITH customer_first_orders AS (
    SELECT
        customer_id,
        order_id,
        order_date,
        sales,
        -- Upstream equivalent of { FIXED [Customer ID] : MIN(DATETRUNC('month', [Order Date])) }
        DATE_TRUNC('month', MIN(order_date) OVER(PARTITION BY customer_id)) AS cohort_month
    FROM fact_sales
),
cohort_sizes AS (
    SELECT
        cfo.*,
        -- Upstream equivalent of { FIXED [Customer_First_Purchase_Month] : COUNTD([Customer ID]) }
        DENSE_RANK() OVER(PARTITION BY cohort_month ORDER BY customer_id)
            + DENSE_RANK() OVER(PARTITION BY cohort_month ORDER BY customer_id DESC) - 1 AS cohort_customer_count
    FROM customer_first_orders cfo
)
SELECT
    customer_id,
    order_id,
    order_date,
    sales,
    cohort_month,
    cohort_customer_count,
    -- Calculate elapsed months upstream
    (EXTRACT(YEAR FROM order_date) - EXTRACT(YEAR FROM cohort_month)) * 12 +
    (EXTRACT(MONTH FROM order_date) - EXTRACT(MONTH FROM cohort_month)) AS cohort_elapsed_months
FROM cohort_sizes;

```

---

### Upstream Pattern B: Tableau Prep Builder Workflow

1. **Add Fixed LOD Aggregation Node:** Isolate Customer First Order.
1. Connect to `fact_sales`.
1. Add an **Aggregate** step. Group by `Customer ID`, set field `Order Date` to **Minimum** (`Min Order Date`).
1. Rename output to `Customer_First_Purchase_Date`.

1. **Add Clean Step for Cohort Month:** Date Truncation & Calculations.
1. Add a **Clean Step** following the aggregate node.
1. Create calculated field: `Cohort_Month` = `DATE_TRUNC('month', Customer_First_Purchase_Date)`.

1. **Join Back to Primary Stream:** Re-attaching Cohort Attributes.
1. Add a **Join** step connecting the primary `fact_sales` stream with the Clean Step.
1. Join Clause: `fact_sales.Customer ID` = `Clean_Step.Customer ID`.

1. **Calculate Cohort Sizes:** Second Aggregate & Join.
1. Add a second **Aggregate** step grouped by `Cohort_Month` using `COUNTD(Customer ID)` to output `Cohort_Customer_Count`.
1. Join this back to the main flow on `Cohort_Month`.

---

## 3. Migration 2: Complex SLA Net Working Business Hours

### Tableau Runtime Calculation (Runtime)

The Tableau expression calculates working minutes between `Created_Timestamp` and `Resolved_Timestamp`, excluding weekends and non-business hours (08:00 to 18:00).

---

### Upstream Pattern A: Production SQL User-Defined Function (UDF) or View

Implementing SLA calculations in SQL leverages a calendar reference table or direct mathematical date differences for optimal efficiency.

```sql
CREATE OR REPLACE VIEW vw_ticket_sla_analytics AS
WITH ticket_prep AS (
    SELECT
        ticket_id,
        created_timestamp,
        resolved_timestamp,
        -- Extract start and end dates
        CAST(created_timestamp AS DATE) AS start_date,
        CAST(resolved_timestamp AS DATE) AS end_date,
        -- Work day boundary timestamps
        CAST(created_timestamp AS DATE) + TIME '08:00:00' AS start_day_open,
        CAST(created_timestamp AS DATE) + TIME '18:00:00' AS start_day_close,
        CAST(resolved_timestamp AS DATE) + TIME '08:00:00' AS end_day_open,
        CAST(resolved_timestamp AS DATE) + TIME '18:00:00' AS end_day_close
    FROM fact_tickets
),
ticket_minute_calc AS (
    SELECT
        ticket_id,
        created_timestamp,
        resolved_timestamp,
        start_date,
        end_date,
        -- Calculate Same-Day SLA Minutes
        CASE
            WHEN start_date = end_date THEN
                GREATEST(0, EXTRACT(EPOCH FROM (
                    LEAST(resolved_timestamp, start_day_close) - GREATEST(created_timestamp, start_day_open)
                )) / 60)
            ELSE
                -- Multi-Day Resolution: Day 1 Minutes + Intermediary Business Days (600 mins/day) + Final Day Minutes
                GREATEST(0, EXTRACT(EPOCH FROM (start_day_close - GREATEST(created_timestamp, start_day_open))) / 60)
                +
                GREATEST(0, (
                    (end_date - start_date - 1)
                    - (2 * (EXTRACT(WEEK FROM end_date) - EXTRACT(WEEK FROM start_date)))
                ) * 600)
                +
                GREATEST(0, EXTRACT(EPOCH FROM (LEAST(resolved_timestamp, end_day_close) - end_day_open)) / 60)
        END AS sla_net_business_minutes
    FROM ticket_prep
)
SELECT
    ticket_id,
    created_timestamp,
    resolved_timestamp,
    ROUND(sla_net_business_minutes, 2) AS sla_net_business_minutes,
    ROUND(sla_net_business_minutes / 60.0, 2) AS sla_net_business_hours
FROM ticket_minute_calc;

```

---

### Upstream Pattern B: Tableau Prep Script Step (Python / R)

For complex date operations that are difficult to express in native Prep GUI blocks, use a **Script Step** invoking Python (`pandas`):

```python
import pandas as pd
import numpy as np

def transform(df):
    """
    Tableau Prep Script Step Entry Point
    Calculates SLA business hours between created_timestamp and resolved_timestamp
    """
    # Convert to datetime
    df['created_timestamp'] = pd.to_datetime(df['created_timestamp'])
    df['resolved_timestamp'] = pd.to_datetime(df['resolved_timestamp'])

    # Calculate total working hours using custom business hour range
    # 08:00 to 18:00 = 10 Hours daily
    def calc_sla_hours(row):
        start = row['created_timestamp']
        end = row['resolved_timestamp']

        if pd.isnull(start) or pd.isnull(end) or start > end:
            return 0.0

        # Generate range of business days excluding weekends (5=Sat, 6=Sun)
        bus_days = pd.date_range(start.date(), end.date(), freq='B')

        if len(bus_days) == 0:
            return 0.0

        if len(bus_days) == 1:
            # Same day SLA
            day_open = pd.Timestamp.combine(start.date(), pd.Timestamp("08:00:00").time())
            day_close = pd.Timestamp.combine(start.date(), pd.Timestamp("18:00:00").time())

            effective_start = max(start, day_open)
            effective_end = min(end, day_close)

            return max(0.0, (effective_end - effective_start).total_seconds() / 3600.0)

        # Multi-day SLA
        # Day 1
        d1_open = pd.Timestamp.combine(start.date(), pd.Timestamp("08:00:00").time())
        d1_close = pd.Timestamp.combine(start.date(), pd.Timestamp("18:00:00").time())
        d1_hours = max(0.0, (d1_close - max(start, d1_open)).total_seconds() / 3600.0)

        # Intermediate days (10 hours per business day)
        inter_hours = max(0, len(bus_days) - 2) * 10.0

        # Final day
        dn_open = pd.Timestamp.combine(end.date(), pd.Timestamp("08:00:00").time())
        dn_close = pd.Timestamp.combine(end.date(), pd.Timestamp("18:00:00").time())
        dn_hours = max(0.0, (min(end, dn_close) - dn_open).total_seconds() / 3600.0)

        return d1_hours + inter_hours + dn_hours

    df['sla_net_business_hours'] = df.apply(calc_sla_hours, axis=1)
    return df

def get_output_schema():
    """
    Defines output schema requirements for Tableau Prep
    """
    return pd.DataFrame({
        'ticket_id': prep_string(),
        'created_timestamp': prep_datetime(),
        'resolved_timestamp': prep_datetime(),
        'sla_net_business_hours': prep_decimal()
    })

```

---

## 4. Migration 3: Outlier Profiling & Regional Standard Deviation

### Tableau Runtime Calculation (Runtime)

- `{ FIXED [Region] : AVG([Sales]) }`
- `{ FIXED [Region] : STDEV([Sales]) }`
- `IF ([Sales] - [Regional_Mean_Sales]) / [Regional_StdDev_Sales] > 3.5 THEN TRUE ELSE FALSE END`

---

### Upstream Pattern A: SQL Window Function Implementation

```sql
CREATE OR REPLACE VIEW vw_sales_outlier_profiling AS
WITH regional_stats AS (
    SELECT
        transaction_id,
        region,
        customer_id,
        sales,
        -- Calculate regional mean and standard deviation upstream using SQL window functions
        AVG(sales) OVER(PARTITION BY region) AS regional_mean_sales,
        STDDEV_SAMP(sales) OVER(PARTITION BY region) AS regional_stddev_sales
    FROM fact_sales_transactions
)
SELECT
    transaction_id,
    region,
    customer_id,
    sales,
    ROUND(regional_mean_sales, 2) AS regional_mean_sales,
    ROUND(regional_stddev_sales, 2) AS regional_stddev_sales,
    -- Compute Z-Score upstream
    ROUND((sales - regional_mean_sales) / NULLIF(regional_stddev_sales, 0), 4) AS z_score,
    -- Flag Outliers
    CASE
        WHEN (sales - regional_mean_sales) / NULLIF(regional_stddev_sales, 0) > 3.5 THEN TRUE
        ELSE FALSE
    END AS is_outlier_flag
FROM regional_stats;

```

---

## 5. Architectural Comparison & Strategy Matrix

| Metric / Consideration  | Runtime Tableau Expressions (VizQL)            | Database Views (SQL)                              | Tableau Prep Flows                                |
| ----------------------- | ---------------------------------------------- | ------------------------------------------------- | ------------------------------------------------- |
| **Execution Point**     | Client Desktop / Server rendering phase        | On-demand or materialized by DB engine            | Scheduled ETL pre-processing                      |
| **Maintenance Burden**  | High; tied to individual workbook components   | Low; centralized in data warehouse                | Moderate; managed via Tableau Conductor           |
| **Concurrency Scaling** | Poor; every user interaction triggers re-calc  | High; scales with RDBMS / Data Warehouse capacity | High; output materializes to `.hyper` extracts    |
| **Best Used For**       | Dynamic ad-hoc filtering and quick prototyping | Enterprise dashboards with standardized metrics   | Workflows requiring multi-source joins & cleaning |
