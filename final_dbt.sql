Key Features of This Solution
1. Intelligent Date Range Consolidation
The solution doesn't blindly consolidate every set of records with the same attributes. Instead, it:

Groups records by all attributes except date ranges
Analyzes the date ranges to check if they're continuous or overlapping
Only consolidates when there are no significant gaps (>1 day)
Handles special cases like the '9999-12-31' end date

2. Handles Various Date Scenarios
The code handles several challenging date range scenarios:

Continuous periods: Records with end and start dates that align perfectly (e.g., 2022-12-31 followed by 2023-01-01)
Overlapping periods: Records where date ranges overlap are merged
Gapped periods: Records with significant gaps between them remain separate
Special end dates: The '9999-12-31' infinity date is properly handled

3. Two Implementation Options
Raw SQL for BigQuery

Uses BigQuery's powerful array functions to handle the date consolidation logic
Implements a stored procedure that:

Consolidates records with the same attributes
Assigns identifiers to new contract/territory combinations
Performs SCD Type 2 processing on the consolidated records
Inserts completely new records



dbt Implementation

Structured in the dbt modular pattern with staging, intermediate, and marts layers
Leverages dbt's testing capabilities to ensure data quality
Follows best practices for incremental processing

How It Works

Record Grouping: Records are first grouped by all attributes except dates
Date Analysis: The date ranges are analyzed to determine if they can be consolidated

This uses a complex but robust algorithm to determine continuity


Consolidation: Where possible, records are consolidated into a single record
SCD Type 2 Processing: Standard SCD Type 2 logic is applied to these consolidated records

Benefits of This Approach

Data Volume Reduction: Significantly reduces the number of records in your final table
Query Performance: Fewer records means faster query performance
Data Clarity: Avoids artificial fragmentation of continuous periods
Flexibility: The consolidation logic can be tuned to your specific business rules

Implementation Notes
For the BigQuery implementation:

Replace project_id.dataset_id with your actual project and dataset names
Adjust the data types and column names if needed
The procedure can be scheduled to run daily through Cloud Scheduler

For the dbt implementation:

Integrate into your existing dbt project structure
Adjust the source references to match your dbt project configuration
Add appropriate tests to ensure data quality



final version :


-- Complete dbt Project Structure for Consolidated SCD Type 2

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

vars:
  active_to_date: "9999-12-31"

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

-- 2. packages.yml
/*
packages:
  - package: dbt-labs/dbt_utils
    version: 0.9.2
*/

-- 3. models/sources.yml
/*
version: 2

sources:
  - name: raw_data
    database: "{{ var('raw_database', target.database) }}"
    schema: "{{ var('raw_schema', 'raw_data') }}"
    tables:
      - name: contracts_raw
        description: Raw contract data with possible fragmented effective date ranges
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
            description: External Contract ID reference number
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
            description: Date when the contract version becomes effective
            tests:
              - not_null
          - name: effective_to_date
            description: Date when the contract version expires
            tests:
              - not_null
        loaded_at_field: "_ingestion_timestamp"
        freshness:
          warn_after: {count: 24, period: hour}
          error_after: {count: 48, period: hour}
*/

-- 4. models/staging/stg_contracts.sql
{{
    config(
        materialized='view'
    )
}}

SELECT
    contract_id,
    territory_id,
    external_contract_id,
    CAST(amount AS NUMERIC) as amount,
    CAST(interest_date AS DATE) as interest_date,
    CAST(effective_from_date AS DATE) as effective_from_date,
    CASE 
        WHEN CAST(effective_to_date AS STRING) = '9999-12-31' THEN DATE('9999-12-31')
        ELSE CAST(effective_to_date AS DATE)
    END as effective_to_date,
    {{ dbt_utils.current_timestamp() }} as loaded_at
FROM {{ source('raw_data', 'contracts_raw') }}

-- 5. models/intermediate/int_contracts_consolidated.sql
{{
    config(
        materialized='table',
        cluster_by=['contract_id', 'territory_id']
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
                ) OR dr.effective_to_date = '{{ var("active_to_date") }}' OR 
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
    END AS effective_to_date,
    can_consolidate AS was_consolidated,
    ARRAY_LENGTH(date_ranges) AS original_record_count,
    {{ dbt_utils.current_timestamp() }} as processed_at
FROM date_analysis,
UNNEST(
    CASE
        -- If we can consolidate, just return a single record
        WHEN can_consolidate THEN [STRUCT(min_from_date AS effective_from_date, max_to_date AS effective_to_date)]
        -- Otherwise, keep the original date ranges
        ELSE date_ranges
    END
) AS dr

-- 6. models/intermediate/int_contract_identifiers.sql
{{
    config(
        materialized='table',
        unique_key=['contract_id', 'territory_id']
    )
}}

WITH current_identifiers AS (
    SELECT DISTINCT
        contract_id,
        territory_id
    FROM {{ ref('int_contracts_consolidated') }}
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
{{
    config(
        materialized='incremental',
        unique_key=['contract_sk'],
        on_schema_change='sync_all_columns',
        cluster_by=['contract_identifier', 'effective_from_date']
    )
}}

WITH staged_contracts AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['c.contract_id', 'c.territory_id', 'c.effective_from_date']) }} as contract_sk,
        c.contract_id,
        c.territory_id,
        c.external_contract_id,
        c.amount,
        c.interest_date,
        c.effective_from_date,
        c.effective_to_date,
        i.contract_identifier,
        c.processed_at
    FROM {{ ref('int_contracts_consolidated') }} c
    INNER JOIN {{ ref('int_contract_identifiers') }} i
        ON c.contract_id = i.contract_id
        AND c.territory_id = i.territory_id
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
    WHERE effective_to_date = '{{ var("active_to_date") }}'
),

-- Find records that need to be expired (values have changed)
updated_contracts AS (
    SELECT
        current_contracts.contract_sk,
        current_contracts.contract_id,
        current_contracts.territory_id,
        current_contracts.external_contract_id,
        current_contracts.amount,
        current_contracts.interest_date,
        current_contracts.effective_from_date,
        DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) as effective_to_date,
        current_contracts.contract_identifier
    FROM current_contracts
    INNER JOIN staged_contracts
        ON current_contracts.contract_identifier = staged_contracts.contract_identifier
        AND current_contracts.effective_to_date = '{{ var("active_to_date") }}'
        AND (
            current_contracts.external_contract_id != staged_contracts.external_contract_id OR
            current_contracts.amount != staged_contracts.amount OR
            current_contracts.interest_date != staged_contracts.interest_date
        )
),

-- Create new current versions
new_versions AS (
    SELECT
        staged_contracts.contract_sk,
        staged_contracts.contract_id,
        staged_contracts.territory_id,
        staged_contracts.external_contract_id,
        staged_contracts.amount,
        staged_contracts.interest_date,
        CURRENT_DATE() as effective_from_date,
        staged_contracts.effective_to_date,
        staged_contracts.contract_identifier
    FROM staged_contracts
    INNER JOIN updated_contracts
        ON staged_contracts.contract_identifier = updated_contracts.contract_identifier
),

-- Keep existing current records that haven't changed
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

-- Identify completely new records (new contract_identifiers)
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

-- Get historical records that don't need to change
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
    WHERE effective_to_date != '{{ var("active_to_date") }}'
),

-- Combine all the record sets
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
-- Initial load just takes all the staged contracts
combined AS (
    SELECT * FROM staged_contracts
)
{% endif %}

-- Final output with current flag
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
        WHEN effective_to_date = '{{ var("active_to_date") }}' THEN TRUE
        ELSE FALSE
    END as is_current
FROM combined

-- 8. models/schema.yml
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
          - dbt_utils.expression_is_true:
              expression: "date(effective_from_date) <= date(effective_to_date)"
      - name: effective_to_date
        description: Effective To Date
        tests:
          - not_null
      - name: loaded_at
        description: Timestamp when data was loaded
        tests:
          - not_null

  - name: int_contracts_consolidated
    description: Consolidated contract records
    tests:
      - dbt_utils.expression_is_true:
          expression: "date(effective_from_date) <= date(effective_to_date)"
          severity: error
    columns:
      - name: contract_id
        tests:
          - not_null
      - name: territory_id
        tests:
          - not_null
      - name: was_consolidated
        description: Flag indicating if records were consolidated
      - name: original_record_count
        description: Number of records that were consolidated
      - name: effective_from_date
        tests:
          - not_null
      - name: effective_to_date
        tests:
          - not_null
      - name: processed_at
        tests:
          - not_null
          - dbt_utils.recency:
              field: processed_at
              datepart: day
              interval: 1

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
      - dbt_utils.expression_is_true:
          expression: "(is_current AND effective_to_date = '9999-12-31') OR (NOT is_current AND effective_to_date != '9999-12-31')"
          severity: error
*/

-- 9. macros/get_contract_hash.sql
/*
{% macro get_contract_hash(contract_id, territory_id) %}
    FARM_FINGERPRINT(CONCAT(CAST({{ contract_id }} AS STRING), '-', CAST({{ territory_id }} AS STRING)))
{% endmacro %}
*/

-- 10. analyses/consolidated_impact_analysis.sql
/*
-- This analysis helps understand the impact of consolidation
WITH before_consolidation AS (
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT CONCAT(contract_id, '-', territory_id)) as unique_contracts
    FROM {{ ref('stg_contracts') }}
),

after_consolidation AS (
    SELECT 
        COUNT(*) as total_records,
        COUNT(DISTINCT CONCAT(contract_id, '-', territory_id)) as unique_contracts,
        SUM(CASE WHEN was_consolidated THEN 1 ELSE 0 END) as consolidated_records,
        SUM(original_record_count) as original_record_count
    FROM {{ ref('int_contracts_consolidated') }}
)

SELECT
    before_consolidation.total_records as before_record_count,
    after_consolidation.total_records as after_record_count,
    before_consolidation.total_records - after_consolidation.total_records as records_reduced,
    ROUND(100.0 * (before_consolidation.total_records - after_consolidation.total_records) / before_consolidation.total_records, 2) as percent_reduction,
    after_consolidation.consolidated_records as consolidated_contract_count
FROM before_consolidation, after_consolidation
*/

-- 11. seeds/territory_reference.csv
/*
territory_id,territory_name,region,country
100,Northeast,East,USA
101,Southeast,East,USA
102,Midwest,Central,USA
103,Southwest,West,USA
104,West,West,USA
105,Canada East,North,Canada
106,Canada West,North,Canada
*/

-- 12. models/marts/fct_contract_metrics.sql
{{
    config(
        materialized='table',
        partition_by={
            "field": "effective_date",
            "data_type": "date",
            "granularity": "month"
        }
    )
}}

-- This fact table contains monthly metrics for active contracts
WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="month",
        start_date="cast('2022-01-01' as date)",
        end_date="date_add(current_date(), interval 3 month)"
    )
    }}
),

-- Format dates for easier joining
dates AS (
    SELECT 
        date_day as effective_date,
        DATE_TRUNC(date_day, MONTH) as effective_month,
        DATE_SUB(DATE_ADD(DATE_TRUNC(date_day, MONTH), INTERVAL 1 MONTH), INTERVAL 1 DAY) as month_end_date
    FROM date_spine
),

-- Get active contracts for each month
contracts_by_month AS (
    SELECT
        d.effective_month,
        d.effective_date,
        c.contract_identifier,
        c.contract_id,
        c.territory_id,
        c.external_contract_id,
        c.amount,
        c.interest_date
    FROM dates d
    INNER JOIN {{ ref('dim_contracts') }} c
        ON d.effective_date >= c.effective_from_date
        AND d.effective_date <= c.effective_to_date
),

-- Add territory information
contracts_with_territory AS (
    SELECT
        c.*,
        t.territory_name,
        t.region,
        t.country
    FROM contracts_by_month c
    LEFT JOIN {{ ref('territory_reference') }} t
        ON c.territory_id = t.territory_id
)

-- Calculate metrics
SELECT
    effective_month,
    effective_date,
    territory_id,
    territory_name,
    region,
    country,
    COUNT(DISTINCT contract_identifier) as contract_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MIN(amount) as min_amount,
    MAX(amount) as max_amount
FROM contracts_with_territory
GROUP BY 1, 2, 3, 4, 5, 6

-- 13. profiles.yml (not stored in repo, used locally)
/*
contract_data_warehouse:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: your-project-id
      dataset: your_dataset_dev
      threads: 4
      timeout_seconds: 300
      location: US
      priority: interactive
    prod:
      type: bigquery
      method: service-account
      project: your-project-id
      dataset: your_dataset_prod
      threads: 8
      timeout_seconds: 600
      keyfile: /path/to/service/account/keyfile.json
      location: US
      priority: interactive
*/

-- 14. Example usage in CI/CD pipeline (.github/workflows/dbt.yml)
/*
name: dbt CI/CD Pipeline

on:
  push:
    branches: [ main ]
  pull_request:
    branches: [ main ]
  schedule:
    - cron: '0 5 * * *' # Run daily at 5 AM UTC

jobs:
  dbt:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Setup Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.9'
          
      - name: Install dependencies
        run: |
          pip install dbt-bigquery==1.3.0
          dbt deps
          
      - name: Run dbt tests
        env:
          BIGQUERY_TOKEN: ${{ secrets.BIGQUERY_TOKEN }}
        run: |
          dbt test
          
      - name: Run dbt models
        env:
          BIGQUERY_TOKEN: ${{ secrets.BIGQUERY_TOKEN }}
        run: |
          dbt run
          
      - name: Generate dbt documentation
        env:
          BIGQUERY_TOKEN: ${{ secrets.BIGQUERY_TOKEN }}
        run: |
          dbt docs generate
          
      - name: Upload documentation artifact
        uses: actions/upload-artifact@v2
        with:
          name: dbt-docs
          path: target/
*/

-- 15. Example airflow/dagster workflow (not part of dbt project)
/*
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5)
}

dag = DAG(
    'contract_etl_pipeline',
    default_args=default_args,
    description='Extract, transform and load contract data',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False
)

extract_task = BashOperator(
    task_id='extract_contract_data',
    bash_command='python /path/to/extract_script.py',
    dag=dag
)

dbt_run_task = BashOperator(
    task_id='dbt_run',
    bash_command='cd /path/to/dbt/project && dbt run --models marts.dim_contracts+',
    dag=dag
)

dbt_test_task = BashOperator(
    task_id='dbt_test',
    bash_command='cd /path/to/dbt/project && dbt test --models marts.dim_contracts+',
    dag=dag
)

extract_task >> dbt_run_task >> dbt_test_task
*/
