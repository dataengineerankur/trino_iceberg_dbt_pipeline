# Kafka to Iceberg Synchronization

This document describes three approaches for syncing data from Kafka topics to Iceberg tables:

## 1. DBT Model Approach (Currently Has Connection Issues)

The DBT approach uses incremental models to track and sync data from Kafka to Iceberg:

```
dbt/iceberg_project/
├── models/
│   ├── kafka_to_iceberg/
│   │   ├── events_streaming.sql     # Incremental model
│   │   └── source.yml               # Source definitions
└── macros/
    └── kafka_helpers.sql            # Helper macros
```

### Key Features

- Incremental materialization with merge strategy
- Tracks Kafka offsets to only process new records
- Parses JSON from Kafka messages
- Maintains metadata (partitions, offsets)

### Usage

Run the model to sync data:

```bash
cd dbt/iceberg_project
dbt run --models kafka_to_iceberg.events_streaming
```

However, this approach is currently experiencing connection issues with the Hive Metastore.

## 2. DBT Macro Approach (Workaround for Connection Issues)

This approach uses DBT macros to execute direct SQL rather than going through DBT's materialization system:

```
dbt/iceberg_project/
└── macros/
    └── kafka_to_iceberg_sync.sql    # Direct SQL macro
```

### Key Features

- Direct SQL execution
- Creates Iceberg table if it doesn't exist
- Tracks last processed Kafka offset
- Processes records in batches
- Handles incremental sync logic

### Usage

You can run the macro directly:

```bash
cd dbt/iceberg_project
dbt run-operation kafka_to_iceberg_sync --args '{"kafka_topic": "kafka.default.events_topic", "target_table": "iceberg.default.raw_events_streaming", "batch_size": 1000}'
```

Or use the provided script for scheduled runs:

```bash
# Run once
./sync_kafka_to_iceberg.sh --once

# Run continuously (every minute by default)
./sync_kafka_to_iceberg.sh
```

## 3. View + Macro Approach 

This approach tries to avoid Hive Metastore connection issues by separating the Kafka reading (view) from the Iceberg writing (direct SQL):

```
dbt/iceberg_project/
├── models/
│   └── kafka_to_iceberg/
│       └── kafka_view.sql           # Simple view on Kafka
└── macros/
    └── direct_kafka_to_iceberg.sql  # Direct SQL macro for Iceberg
```

### Key Features

- Uses simple view model without incremental features
- Avoids Hive Metastore dependencies in the model
- Uses direct SQL operations for table creation and data loading
- Maintains offset tracking for incremental loading

### Usage

You can run each component separately:

```bash
# First create the view (this avoids Hive Metastore connections)
dbt run --models kafka_to_iceberg.kafka_view

# Then run the macro to sync data
dbt run-operation direct_kafka_to_iceberg --args '{"target_table": "iceberg.default.raw_events_streaming", "limit": 1000}'
```

Or use the provided script:

```bash
# Run once
./sync_direct.sh --once

# Run continuously (every minute by default)
./sync_direct.sh
```

## 4. Pure SQL Approach (Ultimate Fallback Solution)

This approach completely bypasses dbt's model system and uses only a macro with direct SQL statements:

```
dbt/iceberg_project/
└── macros/
    └── direct_sql_sync.sql          # Direct SQL-only macro
```

### Key Features

- No models or views - just pure SQL
- Reads directly from Kafka in the macro
- Creates Iceberg table directly
- Handles offset tracking in SQL
- Minimal interaction with Hive Metastore
- Most reliable with severe connection issues

### Usage

```bash
# Run the SQL macro directly
dbt run-operation direct_sql_sync --args '{"kafka_source": "kafka.default.events_topic", "target_table": "iceberg.default.raw_events_streaming", "limit": 1000}'
```

Or use the dedicated script:

```bash
# Run once
./direct_sql_sync.sh --once

# Run continuously
./direct_sql_sync.sh
```

## Troubleshooting

If you encounter Hive Metastore connection issues when running the DBT model:

1. Try the View + Macro approach (method 3), which is specifically designed to work around Hive Metastore issues
2. Check Trino's connectivity to the Hive Metastore
3. Verify network connectivity between services

The connection error may look like:
```
TrinoExternalError(type=EXTERNAL, name=HIVE_METASTORE_ERROR, message="172.20.0.2:9083: java.net.SocketTimeoutException: Read timed out", query_id=20250318_001310_00003_egrtd)
```

## Next Steps

1. Resolve Hive Metastore connection issues for the DBT model approach
2. Add more robust error handling to the direct approach
3. Implement monitoring for the sync process
4. Add schema evolution handling