# 🎮 GameLens — Video Game Analytics Platform

**GameLens** is an analytics platform that collects data from **Steam**, **YouTube** to create insightful dashboards for tracking **game popularity, audience sentiment, and market trends**.  

---

## Features
- **Daily data collection** from multiple sources (Steam API, YouTube API, Metacritic/IGDB).
- **Multi-layer Data Lake**: Raw → Silver → Golden layers in **Parquet** with partitioning.
- **Analytical layer**:
  - **AWS Athena** for aggregations and heavy computations.
  - **ClickHouse** for fast BI queries and multidimensional slicing.
- **Interactive dashboards** in **Apache Superset** (read-only access for viewers).
- **AI-powered insights**: daily analysis and automatically generated recommendations using **OpenAI**.
- **Forecasting**: predictive models to estimate future game popularity based on trends.
- **Monitoring & Alerts**: **Prometheus + Grafana** dashboards and Slack/Email notifications.

---

## Architecture

[Steam / YouTube API]  <br>
↓ (Airflow DAG: daily at night)  <br>
[S3 Data Lake: Raw Zone (JSON/Parquet)]  <br>
↓ (ETL: cleaning, normalization)  <br>
[S3 Silver Layer: Standardized data (Parquet, partitioned)]  <br>
↓ (Athena: aggregations, computations)  <br>
[S3 Golden Layer: Aggregated data]  <br>
↓ (Load)  <br>
[ClickHouse (Golden DB on Hetzner)]  <br>
↓  <br>
[Superset (dashboards, read‑only access)]

---

## Tech Stack

### **Data Collection & Processing**
- **Python**: requests, pandas, pyarrow, clickhouse-driver
- **Airflow**: DAGs for daily fetch & ETL
- **Great Expectations**: data validation & quality checks

### **Storage & Analytics**
- **AWS S3**: Data Lake (Raw/Silver/Golden)
- **Parquet**: storage format with partitioning
- **AWS Athena**: SQL analytics over the Data Lake
- **ClickHouse**: fast OLAP storage for BI queries

### **BI & Interface**
- **Apache Superset**: interactive dashboards (read-only for viewers)

### **Monitoring & Alerts**
- **Prometheus + Grafana**: pipeline & DB monitoring
- **Slack/Email alerts**: notifications for failures

### **ML & AI**
- **OpenAI API**: automatically generated daily insights
- **scikit-learn / Prophet**: trend forecasting models

### **Infrastructure**
- **Hetzner VPS**: hosting for ClickHouse, Airflow, and Superset
- **Docker**: containerized services
- **GitHub Actions**: CI/CD for pipelines
- **Terraform (optional)**: infrastructure automation

---

## Data Lake Layers
- **Raw Layer**: raw API data (JSON/Parquet).
- **Silver Layer**: cleaned and standardized data.
- **Golden Layer**: aggregated, analytics-ready data for BI and dashboards.
