This project ingests breweries data from the Open Brewery DB API and saves it into a local data lake.

Architecture:
- Bronze: Raw API stored as NDJSON
- Silver: Curated dataset stored as Parquet, partitioned by location (country and state)
- Gold: Aggregated dataset with brewery counts per type and location


Execution order:
1. Open Brewery DB API     
2. Bronze (Raw NDJSON)
3. Silver (Partitioned Parquet by country/state)
4. Gold (Aggregated Parquet Dataset)

Running the Project (Local)

1. Install Dependencies:
```
pip install -r requirements.txt
pip install -e .
```

2. Run the Pipeline:
python -m breweries_pipeline.cli all --max-pages 2 --per-page 50 --log-level INFO

Output:

```
data/
  bronze/
    breweries_"date".ndjson

  silver/
    country=.../state=.../.../.parquet

  gold/
    breweries_aggregated.parquet
```

Running Tests:
pytest -q

Data Quality (Silver Layer):
- Required columns 
- Not-null checks 
- Unique id 

If validation fails, the pipeline stops with the error information.

To run using Docker:
docker compose -f docker/docker-compose.yml up --build

API Reference:
https://www.openbrewerydb.org/

-- Production Considerations --

In a production environment this pipeline would be orchestrated using Apache Airflow. Each stage of the pipeline (Bronze, Silver, and Gold) could be executed as independent tasks within an Airflow DAG, allowing scheduling, dependency management, monitoring, and retries also containerization could be extended so that each pipeline layer runs in its own container. 

Although the current implementation uses Pandas due to the relatively small dataset and local execution environment, the architecture was designed so that the transformation layer could be easily migrated to Spark in a production scenario with larger data volumes.

Monitoring and alerting could be implemented through Airflow logs and integrations with monitoring platforms such as Datadog or similar observability tools.
