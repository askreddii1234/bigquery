-- dbt Project Structure
-- project_root/
-- ├── dbt_project.yml
-- ├── models/
-- │   ├── schema.yml
-- │   ├── sources.yml
-- │   ├── staging/
-- │   │   └── stg_contracts.sql
-- │   ├── intermediate/
-- │   │   └── int_contract_identifiers.sql
-- │   └── marts/
-- │       └── dim_contracts.sql
-- └── macros/
--     └── generate_contract_identifier.sql

-- 1. dbt_project.yml
/*
name: 'contract_data_warehouse'
version: '1.0.0'
config-version: 2

profile: 'bigquery'

model-paths: ["models"]
analysis-paths: ["analyses"]
test-paths: ["tests"]
seed-paths: ["seeds"]
macro-paths: ["macros"]

target-path: "target"
clean-targets:
  - "target"
  - "dbt_packages"

models:
  contract_data_warehouse:
    staging:
      +materialized: view
    intermediate:
      +materialized: table
    marts:
      +materialized: incremental
      +incremental_strategy: merge
*/

-- 2. models/sources.yml
/*
version: 2

sources:
  - name: raw_data
    database: "{{ var('raw_database', target.database) }}"
    schema: "{{ var('raw_schema', 'raw_data') }}"
    tables:
      - name: contracts_raw
        columns:
          - name: contract_id
            description: Contract ID
          - name: territory_id
            description: Territory ID
          - name: external_contract_id
            description: External Contract ID
          - name: amount
            description: Contract Amount
          - name: interest_date
            description: Interest Date
          - name: effective_from_date
            description: Effective From Date
          - name: effective_to_date
            description: Effective To Date
*/

-- 3. models/schema.yml
/*
version: 2

models:
  - name: stg_contracts
    description: Staged contract data
    columns:
      - name: contract_id
        description: Contract ID
        tests:
          - not_null
      - name: territory_id
        description: Territory ID
        tests:
          - not_null
      - name: external_contract_id
        description: External Contract ID
        tests:
          - not_null
      - name: amount
        description: Contract Amount
        tests:
          - not_null
      - name: interest_date
        description: Interest Date
        tests:
          - not_null
      - name: effective_from_date
        description: Effective From Date
        tests:
          - not_null
      - name: effective_to_date
        description: Effective To Date
        tests:
          - not_null

  - name: int_contract_identifiers
    description: Contract identifier mapping
    columns:
      - name: contract_id
        description: Contract ID
        tests:
          - not_null
      - name: territory_id
        description: Territory ID
        tests:
          - not_null
      - name: contract_identifier
        description: Surrogate key for contracts
        tests:
          - not_null
          - unique
    tests:
      - unique:
          columns: ['contract_id', 'territory_id']

  - name: dim_contracts
    description: Contract dimension table with SCD Type 2
    columns:
      - name: contract_sk
        description: Surrogate key for the contract record
        tests:
          - not_null
          - unique
      - name: contract_identifier
        description: Surrogate key for the contract entity
        tests:
          - not_null
      - name: contract_id
        description: Contract ID
        tests:
          - not_null
      - name: territory_id
        description: Territory ID
        tests:
          - not_null
      - name: amount
        description: Contract Amount
      - name: is_current
        description: Flag indicating if this is the current record
    tests:
      - dbt_utils.equal_rowcount:
          compare_model: ref('stg_contracts')
*/

-- 4. models/staging/stg_contracts.sql
-- This model cleans and standardizes the source data
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

-- 5. macros/generate_contract_identifier.sql
-- This macro generates the surrogate key logic
/*
{% macro generate_contract_identifier() %}
    FARM_FINGERPRINT(CONCAT(CAST(contract_id AS STRING), '-', CAST(territory_id AS STRING)))
{% endmacro %}
*/

-- 6. models/intermediate/int_contract_identifiers.sql
-- This model creates the contract identifier mapping
WITH current_identifiers AS (
    SELECT DISTINCT
        contract_id,
        territory_id
    FROM {{ ref('stg_contracts') }}
),

{% if is_incremental() %}
existing_identifiers AS (
    SELECT 
        contract_id,
        territory_id,
        contract_identifier
    FROM {{ this }}
),

new_identifiers AS (
    SELECT 
        current_identifiers.contract_id,
        current_identifiers.territory_id
    FROM current_identifiers
    LEFT JOIN existing_identifiers
        ON current_identifiers.contract_id = existing_identifiers.contract_id
        AND current_identifiers.territory_id = existing_identifiers.territory_id
    WHERE existing_identifiers.contract_identifier IS NULL
),

max_identifier AS (
    SELECT COALESCE(MAX(contract_identifier), 0) as max_id
    FROM existing_identifiers
),

new_with_ids AS (
    SELECT
        contract_id,
        territory_id,
        max_id + ROW_NUMBER() OVER (ORDER BY contract_id, territory_id) as contract_identifier
    FROM new_identifiers
    CROSS JOIN max_identifier
),

unioned AS (
    SELECT * FROM existing_identifiers
    UNION ALL
    SELECT * FROM new_with_ids
)
{% else %}
unioned AS (
    SELECT
        contract_id,
        territory_id,
        ROW_NUMBER() OVER (ORDER BY contract_id, territory_id) as contract_identifier
    FROM current_identifiers
)
{% endif %}

SELECT
    contract_id,
    territory_id,
    contract_identifier
FROM unioned

-- 7. models/marts/dim_contracts.sql
-- This model implements the SCD Type 2 logic
{{
    config(
        unique_key='contract_sk',
        on_schema_change='sync_all_columns',
    )
}}

WITH staged_contracts AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['contract_id', 'territory_id', 'effective_from_date']) }} as contract_sk,
        s.contract_id,
        s.territory_id,
        s.external_contract_id,
        s.amount,
        s.interest_date,
        s.effective_from_date,
        s.effective_to_date,
        i.contract_identifier,
        s.loaded_at
    FROM {{ ref('stg_contracts') }} s
    INNER JOIN {{ ref('int_contract_identifiers') }} i
        ON s.contract_id = i.contract_id
        AND s.territory_id = i.territory_id
),

{% if is_incremental() %}
current_contracts AS (
    SELECT
        contract_sk,
        contract_id,
        territory_id,
        external_contract_id,
        amount,
        interest_date,
        effective_from_date,
        effective_to_date,
        contract_identifier
    FROM {{ this }}
    WHERE effective_to_date = '9999-12-31'
),

updated_contracts AS (
    SELECT
        current_contracts.contract_sk,
        current_contracts.contract_id,
        current_contracts.territory_id,
        current_contracts.external_contract_id,
        current_contracts.amount,
        current_contracts.interest_date,
        current_contracts.effective_from_date,
        DATE_SUB(CURRENT_DATE, INTERVAL 1 DAY) as effective_to_date,
        current_contracts.contract_identifier
    FROM current_contracts
    INNER JOIN staged_contracts
        ON current_contracts.contract_identifier = staged_contracts.contract_identifier
        AND current_contracts.effective_to_date = '9999-12-31'
        AND (
            current_contracts.external_contract_id != staged_contracts.external_contract_id OR
            current_contracts.amount != staged_contracts.amount OR
            current_contracts.interest_date != staged_contracts.interest_date
        )
),

new_versions AS (
    SELECT
        staged_contracts.contract_sk,
        staged_contracts.contract_id,
        staged_contracts.territory_id,
        staged_contracts.external_contract_id,
        staged_contracts.amount,
        staged_contracts.interest_date,
        CURRENT_DATE as effective_from_date,
        staged_contracts.effective_to_date,
        staged_contracts.contract_identifier
    FROM staged_contracts
    INNER JOIN updated_contracts
        ON staged_contracts.contract_identifier = updated_contracts.contract_identifier
        AND staged_contracts.effective_to_date = '9999-12-31'
),

unchanged_current_contracts AS (
    SELECT
        current_contracts.contract_sk,
        current_contracts.contract_id,
        current_contracts.territory_id,
        current_contracts.external_contract_id,
        current_contracts.amount,
        current_contracts.interest_date,
        current_contracts.effective_from_date,
        current_contracts.effective_to_date,
        current_contracts.contract_identifier
    FROM current_contracts
    LEFT JOIN updated_contracts
        ON current_contracts.contract_sk = updated_contracts.contract_sk
    WHERE updated_contracts.contract_sk IS NULL
),

new_contracts AS (
    SELECT
        staged_contracts.contract_sk,
        staged_contracts.contract_id,
        staged_contracts.territory_id,
        staged_contracts.external_contract_id,
        staged_contracts.amount,
        staged_contracts.interest_date,
        staged_contracts.effective_from_date,
        staged_contracts.effective_to_date,
        staged_contracts.contract_identifier
    FROM staged_contracts
    LEFT JOIN current_contracts
        ON staged_contracts.contract_identifier = current_contracts.contract_identifier
    WHERE current_contracts.contract_identifier IS NULL
),

historical_contracts AS (
    SELECT
        contract_sk,
        contract_id,
        territory_id,
        external_contract_id,
        amount,
        interest_date,
        effective_from_date,
        effective_to_date,
        contract_identifier
    FROM {{ this }}
    WHERE effective_to_date != '9999-12-31'
),

combined AS (
    SELECT * FROM unchanged_current_contracts
    UNION ALL
    SELECT * FROM updated_contracts
    UNION ALL
    SELECT * FROM new_versions
    UNION ALL
    SELECT * FROM new_contracts
    UNION ALL
    SELECT * FROM historical_contracts
)
{% else %}
combined AS (
    SELECT * FROM staged_contracts
)
{% endif %}

SELECT
    contract_sk,
    contract_identifier,
    contract_id,
    territory_id,
    external_contract_id,
    amount,
    interest_date,
    effective_from_date,
    effective_to_date,
    CASE
        WHEN effective_to_date = '9999-12-31' THEN TRUE
        ELSE FALSE
    END as is_current
FROM combined

-- 8. Example dbt run command in CI/CD pipeline
/*
dbt run --models marts.dim_contracts
dbt test --models marts.dim_contracts
*/

-- 9. Example dbt Exposures for documentation
/*
version: 2

exposures:
  - name: contract_analytics_dashboard
    type: dashboard
    maturity: high
    url: https://lookerstudio.google.com/u/0/reporting/contract-analytics
    description: >
      Contract analytics dashboard showing contract amounts by territory
      with historical tracking of changes.
      
    depends_on:
      - ref('dim_contracts')
      
    owner:
      name: Analytics Team
      email: analytics@example.com
*/
