# The Look E-Commerce Analytics Project

## 📘 Overview
This project demonstrates how **data quality** and **actionable analytics** enable an organization to make informed, impactful decisions. Using a fictitious e-commerce company — **The Look** — we explore a complete data pipeline that ingests, transforms, and analyzes operational data to optimize business margins through insights.

The Look aims to improve profitability by **understanding its current and historical performance** across areas such as user behavior, orders, inventory, and product management. This repository showcases best practices in **data engineering**, **data modeling**, and **data quality monitoring**.

---

## 🧠 Purpose
The goal is to simulate how organizations (or individuals) make data-informed decisions:
- Gather **accurate, consistent, and complete data**.
- Transform data into **actionable insights**.
- Use dashboards to **interpret patterns and drive improvement**.

This mirrors how individuals make choices using personal, medical, and scientific observations to improve their health — The Look uses data to improve its margins.

---

## 🏗️ Project Architecture
### Data Source
- **GCP BigQuery Dataset:** [The Look E-Commerce Public Dataset](https://console.cloud.google.com/marketplace/product/bigquery-public-data/thelook-ecommerce)
- **Entities:** Distribution Centers, Events, Inventory Items, Orders, Products, Users

### Ingestion Pipeline
#### 1. BigQuery → Raw Layer (AWS S3)
- **Script:** `ingest_bigquery_to_raw.py`
- **Engine:** Databricks Job running PySpark on a Spark Cluster
- **Features:**
  - Incremental ingestion with **high-watermark cursors**
  - Append-only Parquet storage (partitioned by date and timestamp)
  - Atomic and idempotent ingestion
  - Schema-stable and backfill-aware

#### 2. Raw Layer → Bronze Layer (Databricks Auto Loader)
- **Script:** `raw_to_bronze_autoloader.py`
- **Features:**
  - Exactly-once ingestion with checkpointing
  - Automatic schema evolution
  - Managed through Databricks Unity Catalog
  - Reads directly from S3 via Unity Catalog Volume

---

## 🧩 Data Transformation
### Layers
1. **Bronze:** Raw ingested data.
2. **Silver:** Cleaned and conformed data using **dbt Core**.
3. **Gold:** Business-ready, analytics-optimized data marts.

### dbt Model Strategy
- **Staging (Silver):** Deduplication, normalization, and schema consistency.
- **Intermediate (Silver):** Conformed data models with standardized business logic.
- **Marts (Gold):** Dimensional and fact tables for analytics and dashboards.

---

## ⚙️ dbt Project Highlights
- Incremental models with **MERGE upserts**
- Schema evolution with `on_schema_change: sync_all_columns`
- **SCD2 (Slowly Changing Dimensions)** for Users, Products, and Distribution Centers
- **Elementary Monitoring** integration for data quality checks
- **Semantic Layer** to ensure consistent business metrics

---

## 📊 Dashboards and Reporting
Dashboards are built in **Databricks** on top of the Gold (marts) models, providing:
- **Funnel Overview**
- **Funnel Channel Drilldown**
- **Funnel Leaks**
- **Channel Profitability**

Each visual is **interpretable** and **action-oriented**:
> Example: If Organic Traffic has a product_to_cart_vs_median of 0.75, that means Organic sessions have 25% fewer product-to-cart conversions than average, prompting actions like improving organic conversion paths or reallocating marketing spend.

---

## 📈 Data Quality Framework
Ensures that The Look’s data represents reality across six core dimensions:
1. **Accuracy**
2. **Completeness**
3. **Consistency**
4. **Validity**
5. **Uniqueness**
6. **Timeliness**

### Actionable Data
Good quality data must also be **understandable** and **actionable**, leading directly to business improvements.

---

## 🧠 Monitoring & Observability (Elementary)
- **Freshness SLAs:** Layer-dependent (hours for staging, days for marts)
- **Volume & Schema Drift Detection**
- **Column-level Anomalies** (price, conversion rates, margins)
- **Alerts via Slack (#data-quality-alerts)**

---

## 🧱 Project Structure
```
the-look-analytics/
│
├── ingestion/
│   ├── ingest_bigquery_to_raw.py
│   └── raw_to_bronze_autoloader.py
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   ├── seeds/
│   ├── macros/
│   ├── snapshots/
│   └── tests/
│
├── dashboards/
│   ├── funnel_overview.dbx
│   ├── funnel_by_channel.dbx
│   └── channel_profitability.dbx
│
└── monitoring/
    └── elementary.yml
```

---

## 🧭 Summary
This project exemplifies **modern data stack practices** for an analytics-driven organization:
- Incremental ingestion (BigQuery → S3)
- Auto Loader for exactly-once ingestion
- dbt for transformation and lineage
- SCD2 dimensional modeling
- Actionable dashboards
- End-to-end observability with Elementary

It serves as a **portfolio-ready template** for showcasing **data engineering**, **dbt modeling**, and **data quality monitoring** skills in Databricks.

---

## 🏁 Run Instructions
```bash
# Step 1: Ingest BigQuery → Raw
python ingestion/ingest_bigquery_to_raw.py

# Step 2: Promote RAW → Bronze
python ingestion/raw_to_bronze_autoloader.py

# Step 3: Transform in dbt
dbt run -s staging+ intermediate+ marts

# Step 4: Run tests & docs
dbt test
dbt docs generate && dbt docs serve
```

---

## 📬 Contact
For questions or contributions, reach out at:
📧 analytics@thelook.io

---

> _“Truth drives improvement — data drives truth.”_
