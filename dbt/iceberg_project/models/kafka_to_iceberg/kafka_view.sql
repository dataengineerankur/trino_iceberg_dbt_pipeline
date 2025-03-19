-- This is a simple view model that only reads from Kafka
-- It doesn't use any incremental functionality or Hive Metastore connections

{{ config(
    materialized='view'
) }}

SELECT
    CAST(JSON_EXTRACT_SCALAR(CAST(message AS VARCHAR), '$.id') AS BIGINT) AS id,
    JSON_EXTRACT_SCALAR(CAST(message AS VARCHAR), '$.name') AS name,
    CAST(JSON_EXTRACT_SCALAR(CAST(message AS VARCHAR), '$.created_at') AS TIMESTAMP) AS created_at,
    _kafka_partition,
    _kafka_offset,
    _timestamp
FROM kafka.default.events_topic
WHERE JSON_EXTRACT_SCALAR(CAST(message AS VARCHAR), '$.id') IS NOT NULL