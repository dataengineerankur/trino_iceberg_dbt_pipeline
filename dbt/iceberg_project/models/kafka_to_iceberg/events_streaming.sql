{{ config(
    materialized='incremental',
    unique_key='id', 
    incremental_strategy='merge'
) }}

{% set target_table = this %}

{% set max_offset_query %}
    {% if is_incremental() %}
        SELECT COALESCE(MAX(_partition_offset), -1) as max_offset
        FROM {{ target_table }}
    {% else %}
        SELECT -1 as max_offset
    {% endif %}
{% endset %}

{% set max_offset_result = run_query(max_offset_query) %}
{% if execute %}
    {% set last_offset = max_offset_result.columns['max_offset'][0] %}
{% else %}
    {% set last_offset = -1 %}
{% endif %}

WITH kafka_data AS (
    SELECT
        CAST(JSON_EXTRACT_SCALAR(CAST(_message AS VARCHAR), '$.id') AS BIGINT) AS id,
        JSON_EXTRACT_SCALAR(CAST(_message AS VARCHAR), '$.name') AS name,
        CAST(JSON_EXTRACT_SCALAR(CAST(_message AS VARCHAR), '$.created_at') AS TIMESTAMP(6)) AS created_at,
        _partition_id,
        _partition_offset
    FROM {{ source('kafka_events', 'events_topic') }}
    {% if is_incremental() %}
    WHERE _partition_offset > {{ last_offset }}
    {% endif %}
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