{% macro get_last_kafka_offset(target_table) %}
    {# Retrieves the last processed Kafka offset from the target table #}
    {% set max_offset_query %}
        SELECT COALESCE(MAX(_kafka_offset), -1) as max_offset
        FROM {{ target_table }}
    {% endset %}
    
    {% set result = run_query(max_offset_query) %}
    
    {% if execute %}
        {% set last_offset = result.columns['max_offset'][0] %}
        {{ return(last_offset) }}
    {% else %}
        {{ return(-1) }}
    {% endif %}
{% endmacro %}

{% macro test_kafka_incremental_load(model) %}
    {# Test to verify the incremental model is correctly loading data from Kafka #}
    {# This test only runs on the specified model #}
    
    {% set target_table = ref(model) %}
    
    WITH kafka_count AS (
        SELECT COUNT(*) AS kafka_messages
        FROM {{ source('kafka_events', 'events_topic') }}
    ),
    table_count AS (
        SELECT COUNT(*) AS loaded_messages
        FROM {{ target_table }}
    )
    
    SELECT
        kafka_count.kafka_messages,
        table_count.loaded_messages,
        CASE 
            WHEN kafka_count.kafka_messages > table_count.loaded_messages THEN 
                'Kafka has ' || (kafka_count.kafka_messages - table_count.loaded_messages) || ' unprocessed messages'
            ELSE 'All messages processed'
        END AS status
    FROM kafka_count, table_count
    
{% endmacro %}