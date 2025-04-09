-- 1. Create the staging table structure
CREATE OR REPLACE TABLE `project_id.dataset_id.stg_contracts` (
    contract_id INT64 NOT NULL,
    territory_id INT64 NOT NULL,
    external_contract_id STRING NOT NULL,
    amount NUMERIC(10, 2) NOT NULL,
    interest_date DATE NOT NULL,
    effective_from_date DATE NOT NULL,
    effective_to_date DATE NOT NULL
);

-- 2. Create the final table with contract_identifier as surrogate key
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

-- 3. Create a table to maintain the mapping of contract_id + territory_id to contract_identifier
CREATE OR REPLACE TABLE `project_id.dataset_id.contract_identifier_map` (
    contract_id INT64 NOT NULL,
    territory_id INT64 NOT NULL,
    contract_identifier INT64 NOT NULL
);

-- Add primary key constraint using clustering
ALTER TABLE `project_id.dataset_id.contract_identifier_map`
CLUSTER BY contract_id, territory_id;

-- 4. Create a sequence for contract_identifier (using a table in BigQuery)
CREATE OR REPLACE TABLE `project_id.dataset_id.contract_identifier_seq` (
    next_val INT64 NOT NULL
);

-- Initialize the sequence with 1
INSERT INTO `project_id.dataset_id.contract_identifier_seq` (next_val) VALUES (1);

-- 5. Initial Load Process
BEGIN TRANSACTION;

-- Load sample data into staging table
INSERT INTO `project_id.dataset_id.stg_contracts` 
VALUES
    (1, 100, 'EXT1001', 500.00, '2023-03-15', '2022-01-01', '2022-12-31'),
    (1, 100, 'EXT1001', 500.00, '2023-03-15', '2023-01-01', '9999-12-31'),
    (2, 100, 'EXT1002', 750.00, '2023-03-15', '2022-01-01', '2022-12-31'),
    (2, 100, 'EXT1002', 750.00, '2023-03-15', '2023-01-01', '9999-12-31'),
    (1, 101, 'EXT1011', 600.00, '2023-03-15', '2022-01-01', '2022-12-31'),
    (1, 101, 'EXT1011', 600.00, '2023-03-15', '2023-01-01', '9999-12-31'),
    (2, 101, 'EXT1012', 900.00, '2023-03-15', '2022-01-01', '2022-12-31'),
    (2, 101, 'EXT1012', 900.00, '2023-03-15', '2023-01-01', '9999-12-31'),
    (1, 102, 'EXT1021', 1000.00, '2023-03-15', '2022-01-01', '2022-12-31'),
    (1, 102, 'EXT1021', 1000.00, '2023-03-15', '2023-01-01', '9999-12-31'),
    (2, 102, 'EXT1022', 1200.00, '2023-03-15', '2022-01-01', '2022-12-31'),
    (2, 102, 'EXT1022', 1200.00, '2023-03-15', '2023-01-01', '9999-12-31');

-- Process: Assign contract_identifier for each unique contract_id + territory_id pair
-- In BigQuery, we can use ROW_NUMBER() window function
INSERT INTO `project_id.dataset_id.contract_identifier_map` (contract_id, territory_id, contract_identifier)
WITH unique_contracts AS (
    SELECT DISTINCT contract_id, territory_id
    FROM `project_id.dataset_id.stg_contracts`
)
SELECT 
    contract_id, 
    territory_id,
    ROW_NUMBER() OVER (ORDER BY contract_id, territory_id) AS contract_identifier
FROM unique_contracts;

-- Load data into the final table with assigned contract_identifiers
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
FROM `project_id.dataset_id.stg_contracts` s
JOIN `project_id.dataset_id.contract_identifier_map` m 
    ON s.contract_id = m.contract_id AND s.territory_id = m.territory_id;

COMMIT;

-- 6. Create a stored procedure for daily loads in BigQuery
CREATE OR REPLACE PROCEDURE `project_id.dataset_id.daily_load_contracts`()
BEGIN
    -- Step 1: Assign contract_identifiers to any new contract_id + territory_id combinations
    INSERT INTO `project_id.dataset_id.contract_identifier_map` (contract_id, territory_id, contract_identifier)
    WITH new_unique_contracts AS (
        SELECT DISTINCT s.contract_id, s.territory_id
        FROM `project_id.dataset_id.stg_contracts` s
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

    -- Step 2: Handle updates (SCD Type 2)
    -- First, identify records that need to be updated using MERGE statement
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
        FROM `project_id.dataset_id.stg_contracts` s
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
    FROM `project_id.dataset_id.stg_contracts` s
    JOIN `project_id.dataset_id.contract_identifier_map` m 
        ON s.contract_id = m.contract_id AND s.territory_id = m.territory_id
    JOIN `project_id.dataset_id.contracts_final` f 
        ON m.contract_identifier = f.contract_identifier
    WHERE f.effective_to_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY);

    -- Step 3: Insert completely new records
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
    FROM `project_id.dataset_id.stg_contracts` s
    JOIN `project_id.dataset_id.contract_identifier_map` m 
        ON s.contract_id = m.contract_id AND s.territory_id = m.territory_id
    LEFT JOIN (
        SELECT DISTINCT contract_identifier 
        FROM `project_id.dataset_id.contracts_final`
    ) f 
        ON m.contract_identifier = f.contract_identifier
    WHERE f.contract_identifier IS NULL;
END;

-- 7. Daily Load Example
BEGIN TRANSACTION;

-- Clear staging for new daily data
DELETE FROM `project_id.dataset_id.stg_contracts` WHERE 1=1;

-- For testing, insert some sample daily data including updates and new records
INSERT INTO `project_id.dataset_id.stg_contracts` VALUES
-- Update to an existing contract (amount changed from 500.00 to 550.00)
(1, 100, 'EXT1001', 550.00, '2023-03-16', '2023-01-01', '9999-12-31'),
-- Update to another existing contract (amount changed from 900.00 to 950.00)
(2, 101, 'EXT1012', 950.00, '2023-03-16', '2023-01-01', '9999-12-31'),
-- New contract
(3, 100, 'EXT1003', 800.00, '2023-03-16', '2023-01-01', '9999-12-31'),
-- New contract with different territory
(3, 102, 'EXT1023', 1100.00, '2023-03-16', '2023-01-01', '9999-12-31');

-- Execute the stored procedure
CALL `project_id.dataset_id.daily_load_contracts`();

COMMIT;

-- 8. Sample queries for BigQuery

-- Query 1: Get all current active records
SELECT * FROM `project_id.dataset_id.contracts_final` 
WHERE effective_to_date = '9999-12-31';

-- Query 2: View the contract_identifier mapping
SELECT * FROM `project_id.dataset_id.contract_identifier_map`;

-- Query 3: View history of a specific contract
SELECT * FROM `project_id.dataset_id.contracts_final` 
WHERE contract_id = 1 AND territory_id = 100
ORDER BY effective_from_date;

-- Query 4: Get total number of contracts by territory with NUMERIC formatting
SELECT 
    territory_id, 
    COUNT(DISTINCT contract_identifier) as contract_count
FROM `project_id.dataset_id.contracts_final`
WHERE effective_to_date = '9999-12-31'
GROUP BY territory_id
ORDER BY territory_id;

-- Query 5: Get total amount by territory for active contracts
SELECT 
    territory_id, 
    SUM(amount) as total_amount,
    FORMAT("$%',.2f", SUM(amount)) as formatted_amount
FROM `project_id.dataset_id.contracts_final`
WHERE effective_to_date = '9999-12-31'
GROUP BY territory_id
ORDER BY territory_id;

-- Query 6: Check contract history with change tracking
WITH contract_history AS (
    SELECT 
        contract_identifier,
        contract_id,
        territory_id,
        amount,
        effective_from_date,
        effective_to_date,
        LAG(amount) OVER (PARTITION BY contract_identifier ORDER BY effective_from_date) as previous_amount
    FROM `project_id.dataset_id.contracts_final`
)
SELECT 
    contract_identifier,
    contract_id,
    territory_id,
    amount,
    previous_amount,
    CASE 
        WHEN previous_amount IS NULL THEN 'New Record'
        WHEN amount != previous_amount THEN 'Amount Changed'
        ELSE 'Other Change'
    END as change_type,
    effective_from_date,
    effective_to_date
FROM contract_history
ORDER BY contract_identifier, effective_from_date;

-- 9. BigQuery-specific optimizations

-- Add partitioning for better performance on large tables
ALTER TABLE `project_id.dataset_id.contracts_final`
    SET OPTIONS (
      partition_by = DATE(effective_from_date),
      require_partition_filter = false
    );

-- Add clustering for faster lookups
ALTER TABLE `project_id.dataset_id.contracts_final`
    SET OPTIONS (
      clustering = ['contract_id', 'territory_id']
    );

-- Create a view for active contracts
CREATE OR REPLACE VIEW `project_id.dataset_id.active_contracts` AS
SELECT *
FROM `project_id.dataset_id.contracts_final`
WHERE effective_to_date = '9999-12-31';
