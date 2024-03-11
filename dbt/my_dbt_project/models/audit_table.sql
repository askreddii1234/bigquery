-- models/audit_table.sql

{{ config(materialized='incremental', unique_key='invocation_id') }}

WITH raw_data AS (
  -- Use a macro to load the run_results.json content into a variable
  SELECT '{{ load_json('target/run_results.json') }}' as json_content
),
parsed_data AS (
  -- Parse the JSON to extract necessary fields
  SELECT
    -- Assuming you have a macro to parse json, e.g. `parse_json_field`
    {{ parse_json_field(json_content, 'invocation_id') }} as invocation_id,
    {{ parse_json_field(json_content, 'status') }} as status,
    {{ parse_json_field(json_content, 'start_time') }} as start_time,
    {{ parse_json_field(json_content, 'end_time') }} as end_time,
    -- Extract other fields as needed
  FROM raw_data
)

SELECT
  invocation_id,
  status,
  start_time,
  end_time
  -- Include other fields
FROM parsed_data
