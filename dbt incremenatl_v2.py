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


+++++++++++++

    -- This is a dbt model to flatten nested JSON data in BigQuery, running daily with transactional consistency
BEGIN TRANSACTION;

WITH control_table AS (
    -- Fetch the latest completed batch end_time
    SELECT MAX(end_time) AS last_extraction_time
    FROM {{ source('your_dataset', 'control_table') }}
    WHERE status = 'completed'
),
source_data AS (
    -- Select new or updated records from the source table
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
        JSON_EXTRACT_ARRAY(json_field, '$.hobbies') AS hobbies,
        '{{ run_started_at }}' AS batch_id  -- Add batch_id for auditing
    FROM {{ source('your_dataset', 'your_source_table') }}, control_table
    WHERE updated_at > COALESCE(last_extraction_time, TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY))
),
deduplicated_source_data AS (
    -- Deduplicate records to get the latest version for each record_id
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY updated_at DESC) AS row_num
        FROM source_data
    )
    WHERE row_num = 1  -- Keep only the latest record per record_id
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
        DATE(updated_at) AS partition_date,
        batch_id
    FROM deduplicated_source_data
    LEFT JOIN UNNEST(contacts) WITH OFFSET AS contact_index ON TRUE
    LEFT JOIN UNNEST(skills) WITH OFFSET AS skill_index ON TRUE
    LEFT JOIN UNNEST(hobbies) WITH OFFSET AS hobby_index ON TRUE
)

-- Insert the flattened data into the target table
INSERT INTO {{ target('your_dataset', 'target_table') }} PARTITION BY partition_date (
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
    partition_date,
    batch_id,
    processed_at
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
    partition_date,
    batch_id,
    CURRENT_TIMESTAMP() AS processed_at
FROM flattened_data;

-- Update the control table with batch status and record count
INSERT INTO {{ target('your_dataset', 'control_table') }} (
    batch_id,
    start_time,
    end_time,
    status,
    processed_record_count
)
VALUES (
    '{{ run_started_at }}',
    TIMESTAMP('{{ run_started_at }}'),
    CURRENT_TIMESTAMP(),
    'completed',
    (SELECT COUNT(*) FROM flattened_data)
);

COMMIT TRANSACTION;

+++++++++++
CREATE TABLE your_dataset.control_table (
    batch_id STRING,                 -- Unique identifier for each batch run
    start_time TIMESTAMP,            -- Timestamp when the batch started
    end_time TIMESTAMP,              -- Timestamp when the batch ended
    status STRING,                   -- Status of the batch ('completed', 'failed')
    processed_record_count INT64,    -- Number of records processed in the batch
    error_message STRING             -- Error message (if applicable, for failed batches)
);


CREATE TABLE your_dataset.target_table (
    record_id STRING,                      -- Unique identifier for each record
    updated_at TIMESTAMP,                  -- Timestamp of the record update in the source table
    name STRING,                           -- Flattened field from JSON
    address_city STRING,                   -- Flattened field from JSON
    address_zip STRING,                    -- Flattened field from JSON
    contact STRING,                        -- Flattened contact information
    contact_index INT64,                   -- Index of the contact in the original array
    skill STRING,                          -- Flattened skill information
    skill_index INT64,                     -- Index of the skill in the original array
    product_name STRING,                   -- Flattened product name
    product_details_brand STRING,          -- Flattened product brand
    product_details_specs_ram STRING,      -- Flattened product RAM specification
    product_details_specs_storage STRING,  -- Flattened product storage specification
    hobby STRING,                          -- Flattened hobby
    hobby_index INT64,                     -- Index of the hobby in the original array
    partition_date DATE,                   -- Partition field for efficient querying
    batch_id STRING,                       -- Audit column for batch traceability
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()  -- Timestamp of when the record was processed
) PARTITION BY partition_date;


BEGIN TRANSACTION;

-- Handle the case where the control table is empty
WITH control_table AS (
    SELECT 
        target_table,
        COALESCE(MAX(end_time), TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 YEAR)) AS last_extraction_time
    FROM `your_project.your_dataset.control_table`
    GROUP BY target_table
),

-- Process data for table1
source_data_table1 AS (
    SELECT 
        record_id,
        updated_at,
        json_field,
        'table1' AS target_table
    FROM `your_project.your_dataset.your_source_table`
    LEFT JOIN control_table
    ON control_table.target_table = 'table1'
    WHERE updated_at > COALESCE(
        (SELECT last_extraction_time FROM control_table WHERE target_table = 'table1'),
        TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 YEAR)
    )
    AND JSON_EXTRACT(json_field, '$.name') IS NOT NULL -- Condition specific to table1
),

-- Process data for table2
source_data_table2 AS (
    SELECT 
        record_id,
        updated_at,
        json_field,
        'table2' AS target_table
    FROM `your_project.your_dataset.your_source_table`
    LEFT JOIN control_table
    ON control_table.target_table = 'table2'
    WHERE updated_at > COALESCE(
        (SELECT last_extraction_time FROM control_table WHERE target_table = 'table2'),
        TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 YEAR)
    )
    AND JSON_EXTRACT(json_field, '$.product') IS NOT NULL -- Condition specific to table2
),

-- Flatten data for table1
flattened_data_table1 AS (
    SELECT
        record_id,
        updated_at,
        JSON_EXTRACT(json_field, '$.name') AS name,
        JSON_EXTRACT(json_field, '$.address.city') AS address_city,
        JSON_EXTRACT(json_field, '$.address.zip') AS address_zip,
        DATE(updated_at) AS partition_date,
        '{{ run_started_at }}' AS batch_id
    FROM source_data_table1
),

-- Flatten data for table2
flattened_data_table2 AS (
    SELECT
        record_id,
        updated_at,
        JSON_EXTRACT(json_field, '$.product.name') AS product_name,
        JSON_EXTRACT(json_field, '$.product.details.brand') AS product_details_brand,
        JSON_EXTRACT(json_field, '$.product.details.specs.ram') AS product_details_specs_ram,
        JSON_EXTRACT(json_field, '$.product.details.specs.storage') AS product_details_specs_storage,
        DATE(updated_at) AS partition_date,
        '{{ run_started_at }}' AS batch_id
    FROM source_data_table2
)

-- Insert data into table1
INSERT INTO `your_project.your_dataset.table1` PARTITION BY partition_date (
    record_id,
    updated_at,
    name,
    address_city,
    address_zip,
    partition_date,
    batch_id
)
SELECT
    record_id,
    updated_at,
    name,
    address_city,
    address_zip,
    partition_date,
    batch_id
FROM flattened_data_table1;

-- Insert data into table2
INSERT INTO `your_project.your_dataset.table2` PARTITION BY partition_date (
    record_id,
    updated_at,
    product_name,
    product_details_brand,
    product_details_specs_ram,
    product_details_specs_storage,
    partition_date,
    batch_id
)
SELECT
    record_id,
    updated_at,
    product_name,
    product_details_brand,
    product_details_specs_ram,
    product_details_specs_storage,
    partition_date,
    batch_id
FROM flattened_data_table2;

-- Update control table for table1
INSERT INTO `your_project.your_dataset.control_table` (
    target_table,
    batch_id,
    start_time,
    end_time,
    status,
    processed_record_count
)

+++++

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),  # Adjust as needed
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def trigger_flask_etl(**kwargs):
    """
    Trigger the Flask service to execute the ETL process.
    """
    # Extract the 'env' parameter from DAG params
    env = kwargs['params']['env']
    flask_url = f"http://<flask-service-url>/incremental?env={env}"  # Replace <flask-service-url>

    try:
        response = requests.get(flask_url)
        response.raise_for_status()
        print(f"ETL process triggered successfully for {env}: {response.json()}")
    except requests.exceptions.RequestException as e:
        raise Exception(f"Failed to trigger Flask service: {e}")

with DAG(
    'trigger_flask_etl',
    default_args=default_args,
    description='Trigger Flask ETL service from Cloud Composer',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    params={'env': 'dev'},  # Default environment
) as dag:

    trigger_task = PythonOperator(
        task_id='trigger_flask_etl_task',
        python_callable=trigger_flask_etl,
        provide_context=True
    )

    trigger_task


+++

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import requests

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),  # Adjust as needed
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Function to trigger the Flask ETL service
def trigger_flask_etl(**kwargs):
    """
    Trigger the Flask service to execute the ETL process.
    """
    # Extract the 'env' parameter from DAG params
    env = kwargs['params']['env']
    flask_url = f"http://<flask-service-url>/incremental?env={env}"  # Replace <flask-service-url>

    try:
        response = requests.get(flask_url)
        response.raise_for_status()
        # Store the success message in XCom
        kwargs['ti'].xcom_push(key='etl_status', value='success')
        print(f"ETL process triggered successfully for {env}: {response.json()}")
    except requests.exceptions.RequestException as e:
        # Store the failure message in XCom
        kwargs['ti'].xcom_push(key='etl_status', value='failure')
        raise Exception(f"Failed to trigger Flask service: {e}")

# Function to log the status of the previous task
def log_status(**kwargs):
    """
    Logs the status of the ETL trigger task.
    """
    # Retrieve the status from XCom
    etl_status = kwargs['ti'].xcom_pull(task_ids='trigger_flask_etl_task', key='etl_status')
    if etl_status == 'success':
        print("ETL job completed successfully!")
    else:
        print("ETL job failed. Check the logs for details.")

# Define the DAG
with DAG(
    'trigger_flask_etl_with_status',
    default_args=default_args,
    description='Trigger Flask ETL service and log status from Cloud Composer',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    params={'env': 'dev'},  # Default environment
) as dag:

    # Task to trigger the Flask ETL service
    trigger_task = PythonOperator(
        task_id='trigger_flask_etl_task',
        python_callable=trigger_flask_etl,
        provide_context=True
    )

    # Task to log the status of the previous task
    log_status_task = PythonOperator(
        task_id='log_status_task',
        python_callable=log_status,
        provide_context=True
    )

    # Define task dependencies
    trigger_task >> log_status_task

SELECT 
    'table1' AS target_table,
    '{{ run_started_at }}',
    TIMESTAMP('{{ run_started_at }}'),
    CURRENT_TIMESTAMP(),
    'completed',
    COUNT(*) AS processed_record_count
FROM flattened_data_table1;

-- Update control table for table2
INSERT INTO `your_project.your_dataset.control_table` (
    target_table,
    batch_id,
    start_time,
    end_time,
    status,
    processed_record_count
)
SELECT 
    'table2' AS target_table,
    '{{ run_started_at }}',
    TIMESTAMP('{{ run_started_at }}'),
    CURRENT_TIMESTAMP(),
    'completed',
    COUNT(*) AS processed_record_count
FROM flattened_data_table2;

COMMIT TRANSACTION;

CREATE TABLE your_project.your_dataset.control_table (
    table_name STRING,               -- Target table name
    batch_id STRING,                 -- Unique identifier for each batch run
    start_time TIMESTAMP,            -- Timestamp when the batch processing started
    end_time TIMESTAMP,              -- Timestamp when the batch processing ended
    status STRING,                   -- Status of the batch ('completed', 'failed')
    processed_record_count INT64     -- Number of records processed in the batch
);


new learning!:

import json
import time
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.utils.dates import days_ago
from datetime import datetime
import requests
import os

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Function to load configuration based on the environment
def load_config(env):
    config_path = os.getenv('CONFIG_FILE_PATH', './config.json')  # Path to the config file
    with open(config_path, 'r') as config_file:
        config = json.load(config_file)
    return config[env]

# Function to trigger a single dbt model
def trigger_dbt_model(model, profile, flask_url, **kwargs):
    """
    Trigger the Flask service to execute a dbt model and store status in XCom.
    """
    model_url = f"{flask_url}/run_dbt_model?profile={profile}&model={model}"
    try:
        response = requests.get(model_url)
        response.raise_for_status()
        print(f"dbt model '{model}' executed successfully: {response.json()}")
        # Push success status to XCom
        kwargs['ti'].xcom_push(key=f'{model}_status', value='success')
    except requests.exceptions.RequestException as e:
        print(f"Failed to execute dbt model '{model}': {e}")
        # Push failure status to XCom
        kwargs['ti'].xcom_push(key=f'{model}_status', value='failure')
        raise Exception(f"Failed to execute dbt model '{model}': {e}")

# Function to orchestrate model execution with a 2-minute delay
def execute_dbt_models_with_delay(**kwargs):
    """
    Load configuration, execute dbt models sequentially, and add a delay between executions.
    """
    env = kwargs['params']['env']
    config = load_config(env)
    flask_url = config['flask_url']
    profile = config['profile']
    models = config['models']

    for model in models:
        print(f"Triggering model: {model}")
        # Trigger the model execution
        trigger_dbt_model(model, profile, flask_url, **kwargs)
        
        # Wait for 2 minutes before executing the next model
        print(f"Waiting for 2 minutes before executing the next model...")
        time.sleep(120)  # 120 seconds = 2 minutes

# Function to log the execution status of all models
def log_dbt_execution_status(**kwargs):
    """
    Retrieve and log the execution status of each model from XCom.
    """
    env = kwargs['params']['env']
    config = load_config(env)
    models = config['models']

    for model in models:
        status = kwargs['ti'].xcom_pull(key=f'{model}_status')
        print(f"Model '{model}' execution status: {status}")

# Define the new DAG
with DAG(
    'run_dbt_models_with_status_and_delay',
    default_args=default_args,
    description='Run dbt models sequentially with status tracking and delay',
    schedule_interval=None,
    catchup=False,
    params={'env': 'dev'},  # Default environment
) as dbt_dag:

    # Sensor to wait for the first DAG to complete successfully
    wait_for_etl_dag = ExternalTaskSensor(
        task_id='wait_for_etl_dag',
        external_dag_id='trigger_flask_etl_with_status',  # First DAG's ID
        external_task_id=None,
        mode='poke',
        timeout=600,
        poke_interval=30,
    )

    # Task to execute dbt models sequentially with delay
    execute_models_task = PythonOperator(
        task_id='execute_models_with_delay',
        python_callable=execute_dbt_models_with_delay,
        provide_context=True,
    )

    # Task to log the status of dbt models
    log_status_task = PythonOperator(
        task_id='log_dbt_execution_status',
        python_callable=log_dbt_execution_status,
        provide_context=True,
    )

    wait_for_etl_dag >> execute_models_task >> log_status_task


++++++++++++++

from flask import Flask, request, jsonify
import os
import subprocess
import logging

app = Flask(__name__)

# Set up logging for 100% traceability
logging.basicConfig(
    filename='dbt_command_execution.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# GCP Project mapping
gcp_project_ids = {
    'bld': 'abcd',
    'int': 'efgh'
}

def execute_dbt_command(command, env, profile_name, folder_name=None, model_name=None):
    """Execute dbt command for all models in folder or specific model."""
    gcp_project_id = gcp_project_ids.get(env)

    if not gcp_project_id:
        logging.error(f"Invalid environment: {env}")
        return jsonify({"error": "Invalid environment or GCP project_id not found"}), 400

    env_vars = {
        'DBT_TARGET_ENVIRONMENT': env,
        'GCP_PROJECT_ID': gcp_project_id
    }

    try:
        # Construct dbt command
        if folder_name and not model_name:
            # Run all models in folder
            command_to_run = f"dbt {command} --models {folder_name} --profile {profile_name} --profiles-dir /home/appuser/.dbt --target {env}"
            logging.info(f"Running dbt command for all models in folder: {folder_name}")
        elif folder_name and model_name:
            # Run a specific model in a folder
            command_to_run = f"dbt {command} --models {folder_name}/{model_name} --profile {profile_name} --profiles-dir /home/appuser/.dbt --target {env}"
            logging.info(f"Running dbt command for model {model_name} in folder {folder_name}")
        else:
            # Default behavior
            command_to_run = f"dbt {command} --profile {profile_name} --profiles-dir /home/appuser/.dbt --target {env}"
            logging.info("Running default dbt command")

        # Execute command
        output = subprocess.check_output(
            ['/bin/bash', '-c', command_to_run],
            env={**env_vars, **os.environ},
            stderr=subprocess.STDOUT
        )

        logging.info(f"Command succeeded: {command_to_run}")
        return jsonify({"status": "success", "output": output.decode()}), 200

    except subprocess.CalledProcessError as e:
        error_output = e.output.decode()
        logging.error(f"Command failed: {command_to_run} | Error: {error_output}")

        # Determine status code based on errors
        if "Compilation Error" in error_output:
            status_code = 400
        elif "Test Failure" in error_output:
            status_code = 422
        else:
            status_code = 500

        return jsonify({"status": "error", "error": error_output}), status_code

@app.route('/<command>-dbt/<env>/<profile_name>/<folder_name>', defaults={'model_name': None}, methods=['GET'])
@app.route('/<command>-dbt/<env>/<profile_name>/<folder_name>/<model_name>', methods=['GET'])
def handle_dbt_command(command, env, profile_name, folder_name, model_name):
    """API Endpoint to trigger dbt commands with profile name."""
    if command not in ["run", "snapshot", "debug", "test", "compile"]:
        logging.error(f"Invalid command: {command}")
        return jsonify({"error": "Invalid command"}), 400

    return execute_dbt_command(command, env, profile_name, folder_name, model_name)

if __name__ == '__main__':
    app.run(debug=True)
