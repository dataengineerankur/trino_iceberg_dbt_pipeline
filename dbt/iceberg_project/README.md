# Iceberg Project - DBT Models

This project contains DBT models for the Trino-Iceberg-Kafka data pipeline.

## Models

### Kafka to Iceberg

The `kafka_to_iceberg` models provide an incremental ETL process that synchronizes data from Kafka topics to Iceberg tables.

#### events_streaming

This model incrementally reads data from the Kafka `events_topic` and writes it to an Iceberg table. It:

1. Tracks the last processed Kafka offset in the target table
2. Only processes new messages that appeared after the last processed offset
3. Extracts and parses JSON data from Kafka messages
4. Stores the data in Iceberg format with partition information

## Running the Models

To run the Kafka to Iceberg synchronization:

```bash
# One-time initial load
dbt run --models kafka_to_iceberg.events_streaming

# Subsequent incremental loads
dbt run --models kafka_to_iceberg.events_streaming
```

The model will automatically detect the last processed offset and only process new messages.

## Testing

The project includes tests to verify the Kafka to Iceberg synchronization:

```bash
# Run all tests
dbt test

# Run tests for specific models
dbt test --models kafka_to_iceberg.events_streaming
```

## Scheduling Incremental Loads

For production use, schedule the incremental model to run at regular intervals:

```bash
# Example cron job (run every 5 minutes)
*/5 * * * * cd /path/to/dbt/iceberg_project && dbt run --models kafka_to_iceberg.events_streaming
```

Alternatively, use a workflow orchestration tool like Airflow to schedule and monitor the runs.

## Custom Macros

The project includes helpful macros for working with Kafka data:

- `get_last_kafka_offset`: Retrieves the last processed Kafka offset from a table
- `test_kafka_incremental_load`: Tests if all messages have been processed correctly