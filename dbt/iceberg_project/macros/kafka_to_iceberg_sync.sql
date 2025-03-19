{% macro kafka_to_iceberg_sync(kafka_topic, target_table, batch_size=1000) %}

{# This macro syncs data from a Kafka topic to an Iceberg table #}
{# It can be run using `dbt run-operation kafka_to_iceberg_sync --args '{"kafka_topic": "kafka.default.events_topic", "target_table": "iceberg.default.raw_events_streaming", "batch_size": 1000}' #}
{# The advantage is it runs the raw SQL directly rather than using dbt's materialization system #}

{% set create_table_if_not_exists %}
CREATE TABLE IF NOT EXISTS {{ target_table }} (
    id BIGINT,
    name VARCHAR,
    created_at TIMESTAMP,
    _kafka_partition INTEGER,
    _kafka_offset BIGINT,
    processed_at TIMESTAMP
)
WITH (
    format = 'PARQUET',
    partitioning = ARRAY['day(processed_at)']
)
{% endset %}

{% do run_query(create_table_if_not_exists) %}
{% do log("Created table if it didn't exist: " ~ target_table, info=True) %}

{% set max_offset_query %}
SELECT COALESCE(MAX(_kafka_offset), -1) as max_offset
FROM {{ target_table }}
{% endset %}

{% set max_offset_result = run_query(max_offset_query) %}
{% if execute %}
    {% set last_offset = max_offset_result.columns['max_offset'][0] %}
{% else %}
    {% set last_offset = -1 %}
{% endif %}

{% do log("Last processed Kafka offset: " ~ last_offset, info=True) %}

{% set insert_new_records %}
INSERT INTO {{ target_table }}
WITH kafka_data AS (
    SELECT
        CAST(JSON_EXTRACT_SCALAR(CAST(message AS VARCHAR), '$.id') AS BIGINT) AS id,
        JSON_EXTRACT_SCALAR(CAST(message AS VARCHAR), '$.name') AS name,
        CAST(JSON_EXTRACT_SCALAR(CAST(message AS VARCHAR), '$.created_at') AS TIMESTAMP) AS created_at,
        _kafka_partition,
        _kafka_offset,
        CURRENT_TIMESTAMP AS processed_at
    FROM {{ kafka_topic }}
    WHERE _kafka_offset > {{ last_offset }}
    ORDER BY _kafka_offset
    LIMIT {{ batch_size }}
)
SELECT 
    id,
    name,
    created_at,
    _kafka_partition,
    _kafka_offset,
    processed_at
FROM kafka_data
WHERE id IS NOT NULL
{% endset %}

{% do run_query(insert_new_records) %}
{% do log("Inserted new records from Kafka into " ~ target_table, info=True) %}

{% set count_query %}
SELECT COUNT(*) as cnt FROM {{ target_table }}
{% endset %}

{% set count_result = run_query(count_query) %}
{% if execute %}
    {% set record_count = count_result.columns['cnt'][0] %}
{% else %}
    {% set record_count = 0 %}
{% endif %}

{% do log("Total records in " ~ target_table ~ ": " ~ record_count, info=True) %}

{# Return success message #}
{{ return("Successfully synced data from " ~ kafka_topic ~ " to " ~ target_table) }}

{% endmacro %}