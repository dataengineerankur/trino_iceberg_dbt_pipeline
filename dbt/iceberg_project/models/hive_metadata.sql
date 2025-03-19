{{ config(
    materialized='view'
) }}

-- This query should work if we can access Hive metadata
SELECT 
    table_catalog,
    table_schema,
    table_name
FROM hive.information_schema.tables
LIMIT 5