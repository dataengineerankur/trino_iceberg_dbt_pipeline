{{ config(
    materialized='view'
) }}

-- Simple diagnostic query to test basic functionality
SELECT 
    1 as test_column