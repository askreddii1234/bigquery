-- 1. First, let's implement this with raw SQL for BigQuery

-- Create the staging table
CREATE OR REPLACE TABLE `project_id.dataset_id.stg_contracts` (
    contract_id INT64 NOT NULL,
    territory_id INT64 NOT NULL,
    external_contract_id STRING NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    interest_date DATE NOT NULL,
    effective_from_date DATE NOT NULL,
    effective_to_date DATE NOT NULL
);

-- Create the consolidated staging table
CREATE OR REPLACE TABLE `project_id.dataset_id.stg_contracts_consolidated` (
    contract_id INT64 NOT NULL,
    territory_id INT64 NOT NULL,
    external_contract_id STRING NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    interest_date DATE NOT NULL,
    effective_from_date DATE NOT NULL,
    effective_to_date DATE NOT NULL
);

-- Create the final table with contract_identifier
CREATE OR REPLACE TABLE `project_id.dataset_id.contracts_final` (
    contract_identifier INT64 NOT NULL,
    contract_id INT64 NOT NULL,
    territory_id INT64 NOT NULL,
    external_contract_id STRING NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    interest_date DATE NOT NULL,
    effective_from_date DATE NOT NULL,
    effective_to_date DATE NOT NULL
);

-- Create mapping table for contract_identifiers
CREATE OR REPLACE TABLE `project_id.dataset_id.contract_identifier_map` (
    contract_id INT64 NOT NULL,
    territory_id INT64 NOT NULL,
    contract_identifier INT64 NOT NULL
);

-- Add primary key constraint using clustering
ALTER TABLE `project_id.dataset_id.contract_identifier_map`
CLUSTER BY contract_id, territory_id;

-- Insert sample data
INSERT INTO `project_id.dataset_id.stg_contracts` VALUES
    (1, 100, 'EXT1001', 500.00, '2023-03-15', '2022-01-01', '2022-12-31'),
    (1, 100, 'EXT1001', 500.00, '2023-03-15', '2023-01-01', '9999-12-31'),
    (2, 100, 'EXT1002', 750.00, '2023-03-15', '2022-01-01', '2022-12-31'),
    (2, 100, 'EXT1002', 750.00, '2023-03-15', '2023-01-01', '9999-12-31'),
    (1, 101, 'EXT1011', 600.00, '2023-03-15', '2022-01-01', '2022-12-31'),
    (1, 101, 'EXT1011', 600.00, '2023-03-15', '2023-01-01', '9999-12-31'),
    -- Introduce a gap to see if our consolidation handles it
    (2, 101, 'EXT1012', 900.00, '2023-03-15', '2022-01-01', '2022-06-30'),
    (2, 101, 'EXT1012', 900.00, '2023-03-15', '2022-08-01', '9999-12-31'),
    -- Introduce overlapping periods to see if our consolidation handles it
    (1, 102, 'EXT1021', 1000.00, '2023-03-15', '2022-01-01', '2022-12-31'),
    (1, 102, 'EXT1021', 1000.00, '2023-03-15', '2022-07-01', '9999-12-31'),
    (2, 102, 'EXT1022', 1200.00, '2023-03-15', '2022-01-01', '2022-12-31'),
    (2, 102, 'EXT1022', 1200.00, '2023-03-15', '2023-01-01', '9999-12-31');

-- Procedure to consolidate records and perform SCD Type 2 processing
CREATE OR REPLACE PROCEDURE `project_id.dataset_id.consolidate_and_process_contracts`()
BEGIN
    -- Step 1: Consolidate records with identical attributes but different date ranges
    DELETE FROM `project_id.dataset_id.stg_contracts_consolidated` WHERE 1=1;
    
    INSERT INTO `project_id.dataset_id.stg_contracts_consolidated`
    WITH grouped_contracts AS (
        SELECT 
            contract_id,
            territory_id,
            external_contract_id,
            amount,
            interest_date,
            -- Group by everything except date ranges
            ARRAY_AGG(STRUCT(effective_from_date, effective_to_date) ORDER BY effective_from_date) AS date_ranges
        FROM `project_id.dataset_id.stg_contracts`
        GROUP BY 
            contract_id,
            territory_id,
            external_contract_id,
            amount,
            interest_date
    ),
    
    -- Handle date range consolidation with a custom approach
    consolidated_dates AS (
        SELECT
            contract_id,
            territory_id,
            external_contract_id,
            amount,
            interest_date,
            date_ranges,
            (SELECT MIN(dr.effective_from_date) FROM UNNEST(date_ranges) AS dr) AS min_from_date,
            (SELECT MAX(dr.effective_to_date) FROM UNNEST(date_ranges) AS dr) AS max_to_date
        FROM grouped_contracts
    ),
    
    -- Check for continuity and gaps
    date_analysis AS (
        SELECT
            contract_id,
            territory_id,
            external_contract_id,
            amount,
            interest_date,
            date_ranges,
            min_from_date,
            max_to_date,
            -- If dates are continuous (no gaps > 1 day), we can consolidate
            (SELECT 
                LOGICAL_AND(
                    EXISTS(
                        SELECT 1 
                        FROM UNNEST(date_ranges) AS next_dr
                        WHERE DATE_ADD(dr.effective_to_date, INTERVAL 1 DAY) >= next_dr.effective_from_date
                        AND dr.effective_to_date < next_dr.effective_from_date
                    ) OR dr.effective_to_date = '9999-12-31' OR 
                    NOT EXISTS(
                        SELECT 1 
                        FROM UNNEST(date_ranges) AS check_dr
                        WHERE check_dr.effective_from_date > dr.effective_to_date
                    )
                )
             FROM UNNEST(date_ranges) AS dr
            ) AS can_consolidate
        FROM consolidated_dates
    )
    
    -- Output the final consolidated records
    SELECT
        contract_id,
        territory_id,
        external_contract_id,
        amount,
        interest_date,
        CASE 
            -- Only consolidate if the periods are continuous or overlapping
            WHEN can_consolidate THEN min_from_date
            ELSE dr.effective_from_date
        END AS effective_from_date,
        CASE 
            WHEN can_consolidate THEN max_to_date
            ELSE dr.effective_to_date
        END AS effective_to_date
    FROM date_analysis,
    UNNEST(
        CASE
            -- If we can consolidate, just return a single record
            WHEN can_consolidate THEN [STRUCT(min_from_date AS effective_from_date, max_to_date AS effective_to_date)]
            -- Otherwise, keep the original date ranges
            ELSE date_ranges
        END
    ) AS dr;

    -- Step 2: Assign contract_identifiers for any new contract combinations
    INSERT INTO `project_id.dataset_id.contract_identifier_map` (contract_id, territory_id, contract_identifier)
    WITH new_unique_contracts AS (
        SELECT DISTINCT s.contract_id, s.territory_id
        FROM `project_id.dataset_id.stg_contracts_consolidated` s
        LEFT JOIN `project_id.dataset_id.contract_identifier_map` m 
            ON s.contract_id = m.contract_id AND s.territory_id = m.territory_id
        WHERE m.contract_identifier IS NULL
    ),
    current_max AS (
        SELECT IFNULL(MAX(contract_identifier), 0) as max_id
        FROM `project_id.dataset_id.contract_identifier_map`
    )
    SELECT 
        contract_id, 
        territory_id,
        max_id + ROW_NUMBER() OVER (ORDER BY contract_id, territory_id)
    FROM new_unique_contracts, current_max;
    
    -- Step 3: Handle SCD Type 2 updates
    MERGE `project_id.dataset_id.contracts_final` T
    USING (
        SELECT 
            m.contract_identifier,
            s.contract_id,
            s.territory_id,
            s.external_contract_id,
            s.amount,
            s.interest_date,
            s.effective_from_date,
            s.effective_to_date,
            CURRENT_DATE() as current_date
        FROM `project_id.dataset_id.stg_contracts_consolidated` s
        JOIN `project_id.dataset_id.contract_identifier_map` m 
            ON s.contract_id = m.contract_id AND s.territory_id = m.territory_id
    ) S
    ON T.contract_identifier = S.contract_identifier 
        AND T.effective_to_date = '9999-12-31'
        AND (
            T.external_contract_id != S.external_contract_id OR
            T.amount != S.amount OR
            T.interest_date != S.interest_date OR
            T.effective_from_date != S.effective_from_date
        )
    WHEN MATCHED THEN
        UPDATE SET effective_to_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
    
    -- Insert new versions for updated records
    INSERT INTO `project_id.dataset_id.contracts_final` (
        contract_identifier,
        contract_id,
        territory_id,
        external_contract_id,
        amount,
        interest_date,
        effective_from_date,
        effective_to_date
    )
    SELECT 
        m.contract_identifier,
        s.contract_id,
        s.territory_id,
        s.external_contract_id,
        s.amount,
        s.interest_date,
        CURRENT_DATE(),
        '9999-12-31'
    FROM `project_id.dataset_id.stg_contracts_consolidated` s
    JOIN `project_id.dataset_id.contract_identifier_map` m 
        ON s.contract_id = m.contract_id AND s.territory_id = m.territory_id
    JOIN `project_id.dataset_id.contracts_final` f 
        ON m.contract_identifier = f.contract_identifier
    WHERE f.effective_to_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);
    
    -- Step 4: Insert completely new records
    INSERT INTO `project_id.dataset_id.contracts_final` (
        contract_identifier,
        contract_id,
        territory_id,
        external_contract_id,
        amount,
        interest_date,
        effective_from_date,
        effective_to_date
    )
    SELECT 
        m.contract_identifier,
        s.contract_id,
        s.territory_id,
        s.external_contract_id,
        s.amount,
        s.interest_date,
        s.effective_from_date,
        s.effective_to_date
    FROM `project_id.dataset_id.stg_contracts_consolidated` s
    JOIN `project_id.dataset_id.contract_identifier_map` m 
        ON s.contract_id = m.contract_id AND s.territory_id = m.territory_id
    LEFT JOIN (
        SELECT DISTINCT contract_identifier 
        FROM `project_id.dataset_id.contracts_final`
    ) f 
        ON m.contract_identifier = f.contract_identifier
    WHERE f.contract_identifier IS NULL;
END;

-- Execute the procedure for initial load
CALL `project_id.dataset_id.consolidate_and_process_contracts`();

-- Sample daily update
BEGIN TRANSACTION;

-- Clear staging for new daily data
DELETE FROM `project_id.dataset_id.stg_contracts` WHERE 1=1;

-- Insert new daily data
INSERT INTO `project_id.dataset_id.stg_contracts` VALUES
    -- Update to an existing contract (amount changed)
    (1, 100, 'EXT1001', 550.00, '2023-03-16', '2023-01-01', '9999-12-31'),
    -- New contract
    (3, 100, 'EXT1003', 800.00, '2023-03-16', '2023-01-01', '9999-12-31'),
    -- Contract with multiple time periods that should be consolidated
    (3, 101, 'EXT1013', 650.00, '2023-03-16', '2023-01-01', '2023-06-30'),
    (3, 101, 'EXT1013', 650.00, '2023-03-16', '2023-07-01', '2023-12-31'),
    (3, 101, 'EXT1013', 650.00, '2023-03-16', '2024-01-01', '9999-12-31');

-- Execute the consolidation and processing procedure
CALL `project_id.dataset_id.consolidate_and_process_contracts`();

COMMIT;

-- Query to view the consolidated records
SELECT * FROM `project_id.dataset_id.stg_contracts_consolidated`;

-- Query to view the final SCD Type 2 records
SELECT * FROM `project_id.dataset_id.contracts_final`
ORDER BY contract_id, territory_id, effective_from_date;

-- 2. Now, let's implement the same concept using dbt

-- models/staging/stg_contracts.sql
/*
SELECT
    contract_id,
    territory_id,
    external_contract_id,
    amount,
    interest_date,
    effective_from_date,
    CASE 
        WHEN effective_to_date = '9999-12-31' THEN DATE('9999-12-31')
        ELSE effective_to_date
    END as effective_to_date,
    CURRENT_TIMESTAMP() as loaded_at
FROM {{ source('raw_data', 'contracts_raw') }}
*/

-- models/intermediate/int_contracts_consolidated.sql
/*
{{
    config(
        materialized='table',
    )
}}

WITH grouped_contracts AS (
    SELECT 
        contract_id,
        territory_id,
        external_contract_id,
        amount,
        interest_date,
        -- Group by everything except date ranges
        ARRAY_AGG(STRUCT(effective_from_date, effective_to_date) ORDER BY effective_from_date) AS date_ranges
    FROM {{ ref('stg_contracts') }}
    GROUP BY 
        contract_id,
        territory_id,
        external_contract_id,
        amount,
        interest_date
),

-- First pass: get min/max dates
consolidated_dates AS (
    SELECT
        contract_id,
        territory_id,
        external_contract_id,
        amount,
        interest_date,
        date_ranges,
        (SELECT MIN(dr.effective_from_date) FROM UNNEST(date_ranges) AS dr) AS min_from_date,
        (SELECT MAX(dr.effective_to_date) FROM UNNEST(date_ranges) AS dr) AS max_to_date
    FROM grouped_contracts
),

-- Second pass: analyze if dates can be consolidated
date_analysis AS (
    SELECT
        contract_id,
        territory_id,
        external_contract_id,
        amount,
        interest_date,
        date_ranges,
        min_from_date,
        max_to_date,
        -- Check if the ranges are continuous (no gaps > 1 day)
        (SELECT 
            LOGICAL_AND(
                EXISTS(
                    SELECT 1 
                    FROM UNNEST(date_ranges) AS next_dr
                    WHERE DATE_ADD(dr.effective_to_date, INTERVAL 1 DAY) >= next_dr.effective_from_date
                    AND dr.effective_to_date < next_dr.effective_from_date
                ) OR dr.effective_to_date = '9999-12-31' OR 
                NOT EXISTS(
                    SELECT 1 
                    FROM UNNEST(date_ranges) AS check_dr
                    WHERE check_dr.effective_from_date > dr.effective_to_date
                )
            )
         FROM UNNEST(date_ranges) AS dr
        ) AS can_consolidate
    FROM consolidated_dates
)

-- Final output: consolidated records where possible
SELECT
    contract_id,
    territory_id,
    external_contract_id,
    amount,
    interest_date,
    CASE 
        -- Only consolidate if the periods are continuous or overlapping
        WHEN can_consolidate THEN min_from_date
        ELSE dr.effective_from_date
    END AS effective_from_date,
    CASE 
        WHEN can_consolidate THEN max_to_date
        ELSE dr.effective_to_date
    END AS effective_to_date
FROM date_analysis,
UNNEST(
    CASE
        -- If we can consolidate, just return a single record
        WHEN can_consolidate THEN [STRUCT(min_from_date AS effective_from_date, max_to_date AS effective_to_date)]
        -- Otherwise, keep the original date ranges
        ELSE date_ranges
    END
) AS dr
*/

-- models/intermediate/int_contract_identifiers.sql
-- (same as previous dbt example, but using the consolidated model as input)

-- models/marts/dim_contracts.sql
-- (same as previous dbt example, but using the consolidated model for source)

-- Additional tests for data quality (in dbt)
/*
version: 2

models:
  - name: int_contracts_consolidated
    description: Consolidated contract records
    tests:
      - dbt_utils.expression_is_true:
          expression: "effective_from_date <= effective_to_date"
          severity: error
      - dbt_utils.recency:
          field: loaded_at
          datepart: day
          interval: 1
    columns:
      - name: contract_id
        tests:
          - not_null
      - name: territory_id
        tests:
          - not_null
      - name: effective_from_date
        tests:
          - not_null
      - name: effective_to_date
        tests:
          - not_null
*/
