-- 1. Create the staging table structure
CREATE TABLE stg_contracts (
    contract_id INTEGER NOT NULL,
    territory_id INTEGER NOT NULL,
    external_contract_id TEXT NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    interest_date DATE NOT NULL,
    effective_from_date DATE NOT NULL,
    effective_to_date DATE NOT NULL
);

-- 2. Create the final table with contract_identifier as surrogate key
CREATE TABLE contracts_final (
    contract_identifier INTEGER PRIMARY KEY,
    contract_id INTEGER NOT NULL,
    territory_id INTEGER NOT NULL,
    external_contract_id TEXT NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    interest_date DATE NOT NULL,
    effective_from_date DATE NOT NULL,
    effective_to_date DATE NOT NULL
);

-- 3. Create a table to maintain the mapping of contract_id + territory_id to contract_identifier
CREATE TABLE contract_identifier_map (
    contract_id INTEGER NOT NULL,
    territory_id INTEGER NOT NULL,
    contract_identifier INTEGER NOT NULL,
    PRIMARY KEY (contract_id, territory_id)
);

-- 4. Create a sequence for contract_identifier (In SQLite, we simulate using a table)
CREATE TABLE contract_identifier_seq (
    next_val INTEGER NOT NULL DEFAULT 1
);

-- Initialize the sequence with 1
INSERT INTO contract_identifier_seq (next_val) VALUES (1);

-- 5. Initial Load Process
-- Note: In SQLite, we'll use the sequence table directly rather than a function

-- Procedure for initial load
BEGIN TRANSACTION;

-- Load sample data into staging table
INSERT INTO stg_contracts VALUES
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
WITH unique_contracts AS (
    SELECT DISTINCT contract_id, territory_id
    FROM stg_contracts
)
INSERT INTO contract_identifier_map (contract_id, territory_id, contract_identifier)
SELECT 
    contract_id, 
    territory_id,
    ROW_NUMBER() OVER (ORDER BY contract_id, territory_id) AS contract_identifier
FROM unique_contracts;

-- Load data into the final table with assigned contract_identifiers
INSERT INTO contracts_final (
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
FROM stg_contracts s
JOIN contract_identifier_map m ON s.contract_id = m.contract_id AND s.territory_id = m.territory_id;

COMMIT;

-- 6. Daily Load Process
-- For daily loads, we need to handle:
-- - New contract_id + territory_id combinations
-- - Updates to existing contracts (SCD Type 2)

-- Procedure for daily load
BEGIN TRANSACTION;

-- Clear staging for new daily data
DELETE FROM stg_contracts;

-- Simulate loading new daily data
-- Here we'd typically have an INSERT or COPY operation to load new data

-- For testing, insert some sample daily data including updates and new records
INSERT INTO stg_contracts VALUES
-- Update to an existing contract (amount changed from 500.00 to 550.00)
(1, 100, 'EXT1001', 550.00, '2023-03-16', '2023-01-01', '9999-12-31'),
-- Update to another existing contract (amount changed from 900.00 to 950.00)
(2, 101, 'EXT1012', 950.00, '2023-03-16', '2023-01-01', '9999-12-31'),
-- New contract
(3, 100, 'EXT1003', 800.00, '2023-03-16', '2023-01-01', '9999-12-31'),
-- New contract with different territory
(3, 102, 'EXT1023', 1100.00, '2023-03-16', '2023-01-01', '9999-12-31');

-- Step 1: Assign contract_identifiers to any new contract_id + territory_id combinations
WITH new_unique_contracts AS (
    SELECT DISTINCT s.contract_id, s.territory_id
    FROM stg_contracts s
    LEFT JOIN contract_identifier_map m ON s.contract_id = m.contract_id AND s.territory_id = m.territory_id
    WHERE m.contract_identifier IS NULL
)
INSERT INTO contract_identifier_map (contract_id, territory_id, contract_identifier)
SELECT 
    contract_id, 
    territory_id,
    (SELECT MAX(contract_identifier) FROM contract_identifier_map) + ROW_NUMBER() OVER (ORDER BY contract_id, territory_id)
FROM new_unique_contracts;

-- Step 2: Handle updates (SCD Type 2)
-- First, identify records that need to be updated (comparing staging vs current active records)
WITH updates AS (
    SELECT 
        s.contract_id,
        s.territory_id,
        f.contract_identifier,
        s.external_contract_id AS new_external_contract_id,
        f.external_contract_id AS old_external_contract_id,
        s.amount AS new_amount,
        f.amount AS old_amount,
        s.interest_date AS new_interest_date,
        f.interest_date AS old_interest_date,
        s.effective_from_date AS new_from_date,
        s.effective_to_date AS new_to_date
    FROM stg_contracts s
    JOIN contract_identifier_map m ON s.contract_id = m.contract_id AND s.territory_id = m.territory_id
    JOIN contracts_final f ON m.contract_identifier = f.contract_identifier
    WHERE f.effective_to_date = '9999-12-31'
    AND (
        s.external_contract_id != f.external_contract_id OR
        s.amount != f.amount OR
        s.interest_date != f.interest_date OR
        s.effective_from_date != f.effective_from_date OR
        s.effective_to_date != f.effective_to_date
    )
)
-- Update the end date for affected records
UPDATE contracts_final
SET 
    effective_to_date = CURRENT_DATE - 1
WHERE contract_identifier IN (SELECT contract_identifier FROM updates)
AND effective_to_date = '9999-12-31';

-- Insert new versions for updated records
INSERT INTO contracts_final (
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
    CURRENT_DATE,
    '9999-12-31'
FROM stg_contracts s
JOIN contract_identifier_map m ON s.contract_id = m.contract_id AND s.territory_id = m.territory_id
JOIN contracts_final f ON m.contract_identifier = f.contract_identifier
WHERE f.effective_to_date = CURRENT_DATE - 1;

-- Step 3: Insert completely new records
INSERT INTO contracts_final (
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
FROM stg_contracts s
JOIN contract_identifier_map m ON s.contract_id = m.contract_id AND s.territory_id = m.territory_id
LEFT JOIN (SELECT DISTINCT contract_identifier FROM contracts_final) f 
ON m.contract_identifier = f.contract_identifier
WHERE f.contract_identifier IS NULL;

COMMIT;

-- 7. Queries to test the solution

-- Query 1: Get all current active records
SELECT * FROM contracts_final WHERE effective_to_date = '9999-12-31';

-- Query 2: View the contract_identifier mapping
SELECT * FROM contract_identifier_map;

-- Query 3: View history of a specific contract
SELECT * FROM contracts_final 
WHERE contract_id = 1 AND territory_id = 100
ORDER BY effective_from_date;

-- Notes for SQLite adaptation:
-- 1. SQLite doesn't support stored procedures or functions directly
-- 2. The BEGIN TRANSACTION/COMMIT blocks show logical separation
-- 3. ROW_NUMBER() requires SQLite 3.25.0 or later
-- 4. Use real Date functions for SQLite as needed
-- 5. For production, adapt the logic to proper procedures or scripts

-- 8. Additional sample queries for testing

-- Query 4: Get total number of contracts by territory
SELECT territory_id, COUNT(DISTINCT contract_identifier) as contract_count
FROM contracts_final
WHERE effective_to_date = '9999-12-31'
GROUP BY territory_id;

-- Query 5: Get total amount by territory for active contracts
SELECT territory_id, SUM(amount) as total_amount
FROM contracts_final
WHERE effective_to_date = '9999-12-31'
GROUP BY territory_id;

-- Query 6: Check contract history (all versions)
SELECT 
    contract_identifier,
    contract_id,
    territory_id,
    amount,
    effective_from_date,
    effective_to_date
FROM contracts_final
ORDER BY contract_identifier, effective_from_date;
