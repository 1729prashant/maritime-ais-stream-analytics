```mermaid
flowchart LR

%% =======================
%% GENERATION
%% =======================
A["AISStream API\nReal-time Ship Data"]

%% =======================
%% INGESTION
%% =======================
B["Python Producer"]
C["Kafka Topic: ship_positions_raw"]

%% =======================
%% PROCESSING
%% =======================
D["Spark Structured Streaming"]
E["Cleaned Stream: ship_positions_clean"]
F["Aggregated Metrics: ship_activity"]

%% =======================
%% STORAGE - DATA LAKE
%% =======================
G["Data Lake (GCS/S3)\nParquet Partitioned"]

%% =======================
%% STORAGE - WAREHOUSE
%% =======================
H["BigQuery Data Warehouse"]

%% =======================
%% TRANSFORMATIONS
%% =======================
I["dbt Models: Staging -> Marts"]

%% =======================
%% ANALYTICS
%% =======================
J["Streamlit Dashboard"]

%% =======================
%% ORCHESTRATION
%% =======================
K["Airflow / Prefect"]

%% =======================
%% FLOW
%% =======================
A --> B --> C --> D
D --> E --> G
D --> F --> G
G --> H --> I --> J

%% =======================
%% ORCHESTRATION LINKS
%% =======================
K --> B
K --> D
K --> I

%% =======================
%% OPTIONAL STYLING (GitHub ignores classDef, but safe to keep)
%% =======================
