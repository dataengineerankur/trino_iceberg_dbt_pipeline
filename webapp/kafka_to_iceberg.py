#!/usr/bin/env python3

import trino
import time
import argparse
import logging
from datetime import datetime
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('kafka_to_iceberg')

def get_trino_connection(host='localhost', port=8080, user='trino', catalog='iceberg', schema='default'):
    """Create and return a Trino connection"""
    try:
        conn = trino.dbapi.connect(
            host=host,
            port=port,
            user=user,
            catalog=catalog,
            schema=schema,
        )
        return conn
    except Exception as e:
        logger.error(f"Error connecting to Trino: {str(e)}")
        return None

def create_target_table_if_not_exists(cursor, target_table):
    """Create the target Iceberg table if it doesn't exist"""
    try:
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            id INTEGER,
            name VARCHAR,
            timestamp TIMESTAMP(6),
            offset BIGINT,
            partition_id BIGINT,
            ingest_time TIMESTAMP
        )
        """)
        logger.info(f"Ensured target table {target_table} exists")
    except Exception as e:
        logger.error(f"Error creating target table: {str(e)}")
        raise

def get_last_offset(cursor, target_table):
    """Get the highest offset from the target table"""
    try:
        cursor.execute(f"SELECT COALESCE(MAX(offset), -1) FROM {target_table}")
        row = cursor.fetchone()
        last_offset = row[0] if row else -1
        logger.info(f"Last processed offset: {last_offset}")
        return last_offset
    except Exception as e:
        logger.error(f"Error retrieving last offset: {str(e)}")
        return -1  # Start from the beginning if we can't determine the last offset

def ingest_from_kafka(cursor, kafka_topic, target_table, batch_size=1000):
    """Ingest data from Kafka to Iceberg"""
    last_offset = get_last_offset(cursor, target_table)
    
    try:
        # Get new records from Kafka
        query = f"""
        INSERT INTO {target_table}
        SELECT 
            CAST(json_extract_scalar(json_parse(_message), '$.id') AS INTEGER) AS id,
            CAST(json_extract_scalar(json_parse(_message), '$.name') AS VARCHAR) AS name,
            CAST(json_extract_scalar(json_parse(_message), '$.timestamp') AS TIMESTAMP(6)) AS timestamp,
            _partition_offset AS offset,
            _partition_id AS partition_id,
            CURRENT_TIMESTAMP AS ingest_time
        FROM kafka.default.{kafka_topic}
        WHERE _partition_offset > {last_offset}
        LIMIT {batch_size}
        """
        
        logger.info(f"Executing query: {query}")
        cursor.execute(query)
        
        # Get number of rows affected
        cursor.execute(f"SELECT COUNT(*) FROM {target_table} WHERE offset > {last_offset}")
        row = cursor.fetchone()
        rows_inserted = row[0] if row else 0
        
        logger.info(f"Inserted {rows_inserted} new records into {target_table}")
        return rows_inserted
    except Exception as e:
        logger.error(f"Error ingesting data: {str(e)}")
        return 0

def main(args):
    """Main function to run the Kafka to Iceberg ingestion"""
    logger.info(f"Starting Kafka to Iceberg ingestion job")
    logger.info(f"Parameters: host={args.host}, port={args.port}, user={args.user}")
    logger.info(f"Source: kafka.default.{args.kafka_topic}, Target: {args.target_table}")
    
    conn = get_trino_connection(
        host=args.host,
        port=args.port,
        user=args.user,
        catalog=args.catalog,
        schema=args.schema
    )
    
    if not conn:
        logger.error("Failed to connect to Trino. Exiting.")
        return 1
    
    try:
        with conn.cursor() as cursor:
            # Create target table if it doesn't exist
            create_target_table_if_not_exists(cursor, args.target_table)
            
            # Run in continuous mode if requested
            if args.continuous:
                logger.info(f"Running in continuous mode with {args.interval}s interval")
                while True:
                    start_time = time.time()
                    rows = ingest_from_kafka(cursor, args.kafka_topic, args.target_table, args.batch_size)
                    
                    # If running as a service, log in a rotating manner
                    if rows > 0:
                        logger.info(f"{datetime.now().strftime('%H:%M:%S')} - Ingested {rows} new records")
                    
                    # Calculate sleep time to maintain the interval
                    elapsed = time.time() - start_time
                    sleep_time = max(0.1, args.interval - elapsed)
                    time.sleep(sleep_time)
            else:
                # One-time execution
                rows = ingest_from_kafka(cursor, args.kafka_topic, args.target_table, args.batch_size)
                logger.info(f"Ingested {rows} new records in one-time execution mode")
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    finally:
        if conn:
            conn.close()
    
    return 0

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest data from Kafka to Iceberg")
    parser.add_argument("--host", default=os.environ.get("TRINO_HOST", "localhost"), help="Trino host")
    parser.add_argument("--port", type=int, default=int(os.environ.get("TRINO_PORT", "8080")), help="Trino port")
    parser.add_argument("--user", default=os.environ.get("TRINO_USER", "trino"), help="Trino user")
    parser.add_argument("--catalog", default=os.environ.get("TRINO_CATALOG", "iceberg"), help="Trino catalog")
    parser.add_argument("--schema", default=os.environ.get("TRINO_SCHEMA", "default"), help="Trino schema")
    parser.add_argument("--kafka-topic", default="events_topic", help="Kafka topic name")
    parser.add_argument("--target-table", default="iceberg.default.events_streaming", help="Target Iceberg table name")
    parser.add_argument("--batch-size", type=int, default=1000, help="Number of records to process in a batch")
    parser.add_argument("--continuous", action="store_true", help="Run in continuous mode")
    parser.add_argument("--interval", type=float, default=5.0, help="Polling interval in seconds (for continuous mode)")
    
    args = parser.parse_args()
    exit(main(args))