#!/bin/bash

# Script to sync data from Kafka to Iceberg using dbt macros
# This avoids the dbt materialization system which seems to have issues with the Hive Metastore

# Configuration
DBT_PROJECT_DIR="/Users/ankurchopra/repo_projects/projects/trino_iceberg_dbt_pipeline/dbt/iceberg_project"
KAFKA_TOPIC="kafka.default.events_topic"
TARGET_TABLE="iceberg.default.raw_events_streaming"
BATCH_SIZE=1000
INTERVAL_SECONDS=60  # Run every minute
LOG_FILE="/Users/ankurchopra/repo_projects/projects/trino_iceberg_dbt_pipeline/dbt/kafka_sync.log"

# Create log file if it doesn't exist
touch "$LOG_FILE"

echo "Starting Kafka to Iceberg sync process at $(date)" | tee -a "$LOG_FILE"
echo "Will sync from $KAFKA_TOPIC to $TARGET_TABLE every $INTERVAL_SECONDS seconds" | tee -a "$LOG_FILE"

# Function to run the sync
run_sync() {
  echo "Running sync at $(date)" | tee -a "$LOG_FILE"
  
  # Run the dbt macro
  cd "$DBT_PROJECT_DIR" && \
  dbt run-operation kafka_to_iceberg_sync \
    --args "{\"kafka_topic\": \"$KAFKA_TOPIC\", \"target_table\": \"$TARGET_TABLE\", \"batch_size\": $BATCH_SIZE}" \
    | tee -a "$LOG_FILE"
    
  echo "Completed sync at $(date)" | tee -a "$LOG_FILE"
  echo "-----------------------------------------" | tee -a "$LOG_FILE"
}

# Run once or continuously based on command line argument
if [ "$1" == "--once" ]; then
  echo "Running sync once" | tee -a "$LOG_FILE"
  run_sync
else
  echo "Running sync continuously every $INTERVAL_SECONDS seconds" | tee -a "$LOG_FILE"
  # Run continuously
  while true; do
    run_sync
    sleep $INTERVAL_SECONDS
  done
fi