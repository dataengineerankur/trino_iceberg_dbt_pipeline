{{ config(materialized='table') }}

SELECT
  id,
  name,
  CAST(created_at AS timestamp) AS event_time
FROM iceberg.default.raw_events
WHERE id IS NOT NULL;