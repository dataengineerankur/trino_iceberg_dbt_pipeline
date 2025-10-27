# Trino + Iceberg + Kafka + dbt demo (with simple web UI)

This project spins up a small lakehouse stack on your laptop using Docker:
- Trino (SQL query engine)
- MinIO (S3-compatible object storage)
- Hive Metastore (Iceberg metadata)
- Kafka + Zookeeper (event stream)
- A simple Flask web UI to talk to Trino and send test events to Kafka
- dbt project to model data and sync Kafka events into Iceberg

## Prerequisites
- Docker Desktop (or Docker + Docker Compose)
- macOS/Linux/Windows

## What gets started
- Trino at http://localhost:8080
- MinIO Console at http://localhost:9001 (user/pass: `minioadmin` / `minioadmin`)
- Hive Metastore on port 9083 (internal)
- Kafka broker on `kafka:9092` inside the compose network
- Web UI at http://localhost:5000 (after you start it)

## Start the core stack (Trino, MinIO, Hive, Kafka)
```bash
cd trino_iceberg_dbt_pipeline/trino
docker compose up -d --build
```
- Wait ~30–60 seconds for services to become healthy.
- Trino UI: open http://localhost:8080
- MinIO Console: open http://localhost:9001 (login: `minioadmin` / `minioadmin`)

## Start the web UI
```bash
cd ../webapp
docker compose up -d --build
# Web UI: http://localhost:5000
```
The web UI lets you:
- Test connection to Trino
- Run simple SQL queries
- Send a test event into Kafka (topic `events_topic`)

Note: The webapp uses the existing `trino_default` Docker network defined by the Trino compose.

## dbt project
There are two dbt folders under `dbt/`.
- `dbt/iceberg_project` contains models/macros that read from Kafka via Trino and write into Iceberg tables.
- Helpful docs live in `dbt/KAFKA_TO_ICEBERG.md` and `dbt/iceberg_project/README.md`.

Before running dbt, ensure Trino is up.
```bash
cd ../dbt/iceberg_project
# Preview connection settings in dbt_project.yml and profiles.yml
# Then run a model (first run creates the Iceberg table)
dbt run --models kafka_to_iceberg.events_streaming

# Re-run to process any new Kafka messages
# (model is incremental)
dbt run --models kafka_to_iceberg.events_streaming

# Optional tests
dbt test
```

## Sending a test event to Kafka
You can do this from the web UI (recommended), or manually inside the Kafka container:
```bash
docker exec -i kafka bash -lc 'echo "{\"id\":\"1\",\"type\":\"click\"}" | kafka-console-producer --broker-list kafka:9092 --topic events_topic'
```

## Stopping everything
```bash
# From the trino folder (core services)
docker compose down

# From the webapp folder
cd ../webapp && docker compose down
```

## Troubleshooting
- Trino not reachable: wait a bit, then check logs `docker compose logs -f trino` from the `trino` folder.
- MinIO login: default `minioadmin` / `minioadmin`.
- Web UI can’t connect to Trino: confirm `TRINO_HOST=trino` in `webapp/docker-compose.yml` and Trino is up.
- dbt errors: ensure Trino is running; see `dbt/profiles.yml` for connection config and `dbt/logs/dbt.log` for details.

## Folder map 
- `trino/`: compose and config for Trino, MinIO, Hive Metastore, Kafka
- `webapp/`: small Flask app + compose to query Trino and push Kafka events
- `dbt/iceberg_project/`: dbt models/macros to land Kafka events into Iceberg
- `dbt/KAFKA_TO_ICEBERG.md`: extra notes on syncing Kafka → Iceberg
