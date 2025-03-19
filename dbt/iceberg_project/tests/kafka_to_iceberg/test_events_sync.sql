-- Test to verify that all Kafka events have been properly processed
-- This test will pass if all valid Kafka messages have been processed into the Iceberg table

WITH kafka_messages AS (
    SELECT
        CAST(JSON_EXTRACT_SCALAR(CAST(message AS VARCHAR), '$.id') AS BIGINT) AS id,
        _kafka_offset
    FROM {{ source('kafka_events', 'events_topic') }}
    WHERE JSON_EXTRACT_SCALAR(CAST(message AS VARCHAR), '$.id') IS NOT NULL
),

iceberg_records AS (
    SELECT
        id,
        _kafka_offset
    FROM {{ ref('events_streaming') }}
),

-- Find any Kafka messages with valid IDs that haven't been processed
unprocessed_messages AS (
    SELECT 
        k.id,
        k._kafka_offset
    FROM kafka_messages k
    LEFT JOIN iceberg_records i ON k.id = i.id
    WHERE i.id IS NULL
)

-- If this query returns any rows, the test will fail
SELECT 
    id,
    _kafka_offset,
    'Message with ID ' || id || ' at offset ' || _kafka_offset || ' was not processed' AS error_message
FROM unprocessed_messages
LIMIT 10