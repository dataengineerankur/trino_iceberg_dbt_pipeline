#!/bin/bash

# Script to sync data from Kafka to Iceberg using a direct SQL approach
# This bypasses the dbt materialization system to avoid Hive Metastore issues

DBT_PROJECT_DIR="/Users/ankurchopra/repo_projects/projects/trino_iceberg_dbt_pipeline/dbt/iceberg_project"
TARGET_TABLE="iceberg.default.raw_events_streaming"
BATCH_SIZE=1000
INTERVAL_SECONDS=60
LOG_FILE="/Users/ankurchopra/repo_projects/projects/trino_iceberg_dbt_pipeline/dbt/direct_sync.log"

# Create log file if it doesn't exist
touch "$LOG_FILE"

echo "Starting Direct Kafka to Iceberg sync at $(date)" | tee -a "$LOG_FILE"

# Function to run the sync
run_sync() {
  echo "Running sync at $(date)" | tee -a "$LOG_FILE"
  
  # First run the view model to make sure it's up to date
  cd "$DBT_PROJECT_DIR" && \
  dbt run --models kafka_to_iceberg.kafka_view | tee -a "$LOG_FILE"
  
  # Then run the macro to sync data
  cd "$DBT_PROJECT_DIR" && \
  dbt run-operation direct_kafka_to_iceberg \
    --args "{\"target_table\": \"$TARGET_TABLE\", \"limit\": $BATCH_SIZE}" \
    | tee -a "$LOG_FILE"
    
  echo "Completed sync at $(date)" | tee -a "$LOG_FILE"
  echo "----------------------------------------" | tee -a "$LOG_FILE"
}

# Run once or continuously
if [ "$1" == "--once" ]; then
  echo "Running sync once" | tee -a "$LOG_FILE"
  run_sync
else
  echo "Running sync continuously every $INTERVAL_SECONDS seconds" | tee -a "$LOG_FILE"
  while true; do
    run_sync
    sleep $INTERVAL_SECONDS
  done
fi