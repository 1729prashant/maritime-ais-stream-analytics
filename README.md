```
docker compose up -d
uv run python main.py                                 # producer
uv run python -m ingestion.consumers.kafka_consumer   # consumer
uv run streamlit run dashboard/app.py                 # visualise
```