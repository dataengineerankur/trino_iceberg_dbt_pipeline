







    


WITH kafka_data AS (
    SELECT
        CAST(JSON_EXTRACT_SCALAR(CAST(_message AS VARCHAR), '$.id') AS BIGINT) AS id,
        JSON_EXTRACT_SCALAR(CAST(_message AS VARCHAR), '$.name') AS name,
        CAST(JSON_EXTRACT_SCALAR(CAST(_message AS VARCHAR), '$.created_at') AS TIMESTAMP(6)) AS created_at,
        _partition_id,
        _partition_offset
    FROM "kafka"."default"."events_topic"
    
    WHERE _partition_offset > 6
    
)

SELECT
    id,
    name,
    created_at,
    _partition_id,
    _partition_offset,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(6)) AS processed_at
FROM kafka_data
WHERE id IS NOT NULL