-- This is a dbt model to flatten nested JSON data in BigQuery incrementally every hour
WITH control_table AS (
    -- Get the latest extraction status from the control table
    SELECT MAX(end_time) AS last_extraction_time
    FROM {{ source('your_dataset', 'control_table') }}
    WHERE status = 'completed'
),
source_data AS (
    -- Select new or updated records from the source table based on the control table
    SELECT 
        record_id,
        updated_at,
        JSON_EXTRACT(json_field, '$.name') AS name,
        JSON_EXTRACT(json_field, '$.address.city') AS address_city,
        JSON_EXTRACT(json_field, '$.address.zip') AS address_zip,
        JSON_EXTRACT_ARRAY(json_field, '$.contacts') AS contacts,
        JSON_EXTRACT_ARRAY(json_field, '$.skills') AS skills,
        JSON_EXTRACT(json_field, '$.product.name') AS product_name,
        JSON_EXTRACT(json_field, '$.product.details.brand') AS product_details_brand,
        JSON_EXTRACT(json_field, '$.product.details.specs.ram') AS product_details_specs_ram,
        JSON_EXTRACT(json_field, '$.product.details.specs.storage') AS product_details_specs_storage,
        JSON_EXTRACT_ARRAY(json_field, '$.hobbies') AS hobbies
    FROM {{ source('your_dataset', 'your_source_table') }}, control_table
    WHERE updated_at > COALESCE(last_extraction_time, TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR))
),
flattened_data AS (
    -- Flatten the arrays within the JSON data
    SELECT
        record_id,
        updated_at,
        name,
        address_city,
        address_zip,
        contact.value AS contact,
        contact_index,
        skill.value AS skill,
        skill_index,
        product_name,
        product_details_brand,
        product_details_specs_ram,
        product_details_specs_storage,
        hobby.value AS hobby,
        hobby_index,
        DATE(updated_at) AS partition_date
    FROM source_data
    LEFT JOIN UNNEST(contacts) WITH OFFSET AS contact_index ON TRUE
    LEFT JOIN UNNEST(skills) WITH OFFSET AS skill_index ON TRUE
    LEFT JOIN UNNEST(hobbies) WITH OFFSET AS hobby_index ON TRUE
)

-- Insert the flattened data into the target table to ensure incremental updates
BEGIN
  DECLARE success BOOLEAN DEFAULT FALSE;

  BEGIN
    INSERT INTO {{ target('your_dataset', 'your_target_table') }} PARTITION BY partition_date (
        record_id,
        updated_at,
        name,
        address_city,
        address_zip,
        contact,
        contact_index,
        skill,
        skill_index,
        product_name,
        product_details_brand,
        product_details_specs_ram,
        product_details_specs_storage,
        hobby,
        hobby_index,
        partition_date
    )
    SELECT
        record_id,
        updated_at,
        name,
        address_city,
        address_zip,
        contact,
        contact_index,
        skill,
        skill_index,
        product_name,
        product_details_brand,
        product_details_specs_ram,
        product_details_specs_storage,
        hobby,
        hobby_index,
        partition_date
    FROM flattened_data;

    SET success = TRUE;
  EXCEPTION WHEN ERROR THEN
    SET success = FALSE;
  END;

  -- Update the control table with the extraction end time and batch_id only for successful runs
  IF success THEN
    INSERT INTO {{ target('your_dataset', 'control_table') }} (batch_id, end_time, status)
    VALUES (GENERATE_UUID(), CURRENT_TIMESTAMP(), 'completed');
  ELSE
    INSERT INTO {{ target('your_dataset', 'control_table') }} (batch_id, end_time, status)
    VALUES (GENERATE_UUID(), CURRENT_TIMESTAMP(), 'failed');
  END IF;
END;

-- Control Table Schema:
-- control_table (
--     batch_id STRING,       -- Unique identifier for each batch run
--     end_time TIMESTAMP,    -- Timestamp indicating when the extraction finished
--     status STRING          -- Status of the extraction ('completed', 'failed', etc.)
-- )

-- Target Table Schema:
-- your_target_table (
--     record_id STRING,               -- Unique identifier for each record
--     updated_at TIMESTAMP,           -- Timestamp of when the record was last updated
--     name STRING,                    -- Flattened field from JSON
--     address_city STRING,            -- Flattened field from JSON
--     address_zip STRING,             -- Flattened field from JSON
--     contact STRING,                 -- Flattened contact information
--     contact_index INT64,            -- Index of the contact in the original array
--     skill STRING,                   -- Flattened skill information
--     skill_index INT64,              -- Index of the skill in the original array
--     product_name STRING,            -- Flattened product name
--     product_details_brand STRING,   -- Flattened product brand
--     product_details_specs_ram STRING, -- Flattened product RAM specification
--     product_details_specs_storage STRING, -- Flattened product storage specification
--     hobby STRING,                   -- Flattened hobby information
--     hobby_index INT64,              -- Index of the hobby in the original array
--     partition_date DATE             -- Partition field for efficient querying
-- )
