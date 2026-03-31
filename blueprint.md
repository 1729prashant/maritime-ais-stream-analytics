# 🚢 Maritime AIS Stream Analytics - Project Blueprint

A **production-style, real-time data engineering platform** for ingesting, processing, and analyzing AIS ship tracking data.

---

## 🧭 0. High-Level Mental Model
This is a **streaming data platform**. 

Data Source → Ingestion → Processing → Storage → Warehouse → Analytics → Dashboard

Everything else (IaC, monitoring, security) sits **across all layers**.

---

## 1. Data Collection (Generation Layer)

### Goal
Get real-time ship data from an external system.

### Source
- AISStream API (WebSocket)

### Process
- Connect to WebSocket
- Filter by region (reduces cost & complexity)
- Receive JSON events:
  - ship_id (MMSI)
  - latitude / longitude
  - speed
  - timestamp
  - what else ??

**Design decision:**  
You are NOT generating data — you are consuming it.
Generation = External API (AISStream)


### Monitoring & Debugging
- Log connection status
- Track dropped messages
- Retry on disconnect

### IaC
- Store API keys securely (env variables / secrets manager)

---

## 2. Data Preparation & Cleaning

### 2a. Service Selection (Ingestion Layer)

**Goal:** Move data from API → system

**Tools:** Kafka (or Redpanda)

**Flow:** AISStream → Python Producer → Kafka Topic

**Topic Name:** `ship_positions_raw`

**Design Choices:**
- Partition by ship_id (optional)
- JSON format initially

**Monitoring:**
- Kafka lag
- Message throughput
- Producer failures

**IaC:**
- Kafka setup via Docker Compose (simple)
- Later: Terraform if cloud Kafka

---

### 2b. Data Processing (Transformation Layer)

**Goal:** Clean + enrich streaming data

**Tools:** Spark Structured Streaming

**Processing Steps:**
- **Clean**
  - remove null coordinates
  - validate lat/lon ranges
  - deduplicate
- **Enrich**
  - compute speed buckets
  - detect moving vs stationary
  - detect port proximity (optional)

**Output Streams:**
clean_ship_positions
ship_activity_metrics


**Monitoring:**
- Failed records count
- Processing latency
- Checkpointing status

**IaC:**
- Spark job config (Dockerized)
- Storage paths predefined

---

### 2c. Data Storage (Serving Layer)

#### Data Lake (Raw + Processed)
**Goal:** Store historical data cheaply

**Tool:** GCS / S3

**Format:** Parquet (compressed, efficient)

**Structure:**
/ships/
/raw/year=2026/month=03/day=26/
/processed/year=2026/month=03/day=26/



**Why this matters:**
- Reproducibility
- Replayability
- Cheap storage

**Monitoring:**
- File sizes
- Partition correctness
- Write failures

**IaC:**
- Bucket creation
- Lifecycle rules (delete old data)

#### Data Warehouse

**Goal:** Serve analytics queries

**Tool:** BigQuery

**Tables:**
- `ship_positions` → timestamp (partitioned), ship_id (clustered)
- `ship_activity_daily` → aggregated metrics

**Optimization:**
- Partition by timestamp
- Cluster by ship_id

**Monitoring:**
- Query performance
- Cost usage

**IaC:**
- Dataset creation
- Table schemas

---

## 3. Data Quality Checks

**Goal:** Ensure data reliability

**Tool:** dbt

**Tests:**
- **Basic:** ship_id NOT NULL, timestamp NOT NULL
- **Advanced:** lat [-90,90], lon [-180,180]
- **Business:** speed >= 0

**Where it runs:** After data lands in warehouse

**Monitoring:**
- Test failures
- Anomaly spikes

**IaC:**
- dbt project config
- Version-controlled tests

---

## 4. Data Visualization & Analysis

**Goal:** Make insights usable

**Tool:** Streamlit

**Dashboard Tiles:**
- **Categorical:** Top ports / regions by ship count
- **Temporal:** Ship activity over time

**Bonus:** Map visualization (ship positions)

**Monitoring:**
- Dashboard uptime
- Query latency

**IaC:**
- Deployment config (optional)

---

## 5. Data Engineering Lifecycle

### Generation
AISStream (external API)

### Ingestion
Python Producer → Kafka

### Transformation
Spark Streaming → cleaned + enriched data
dbt → warehouse transformations

### Serving
Data Lake (historical)
BigQuery (analytics)

### Analytics
Streamlit Dashboard


---

## 6. Cross-Cutting Concerns

**Security**
- API keys in env variables
- IAM roles for storage + BigQuery
- No hardcoded secrets

**Data Management**
- Partitioning strategy
- Schema evolution handling
- Retention policies

**DataOps**
- Version control (Git)
- Reproducible pipelines
- Automated runs (later: CI/CD)

**Data Architecture**
- Lake → warehouse pattern
- Streaming + batch hybrid
- Separation of raw vs processed

**Orchestration**
- Airflow / Prefect (later stage)
- Schedule batch jobs
- Manage dependencies

**Software Engineering**
- Modular code: ingestion/, processing/, warehouse/
- Logging everywhere
- Config-driven pipelines

---

## 7. Final Mental Model

> A **streaming data platform with layered architecture**, not a script that pulls data and plots charts.

---

## 8. Next Steps

- Build a **step-by-step plan (Day 1 → Day 7)**
- Decide **exact tech stack** (Kafka vs Redpanda, Airflow vs Prefect)
