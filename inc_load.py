
# flatten_json_bigquery.py
from google.cloud import bigquery
import json

# Function to flatten nested JSON objects
def flatten_json(nested_json, parent_key='', sep='_'):
    """Flatten a nested JSON object."""
    items = []
    for k, v in nested_json.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_json(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for i, item in enumerate(v):
                items.extend(flatten_json(item, f"{new_key}{sep}{i}", sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

# Function to incrementally flatten JSON data in BigQuery
def flatten_table_incrementally():
    project_id = "your_project_id"
    dataset_id = "your_dataset_id"
    source_table_id = "your_source_table_id"
    target_table_id = "your_target_table_id"  # Static target table

    client = bigquery.Client()

    # Define source and target table references
    source_table_ref = f"{project_id}.{dataset_id}.{source_table_id}"
    target_table_ref = f"{project_id}.{dataset_id}.{target_table_id}"

    # Query to get new or updated records from the source table
    query = f"""
        SELECT *
        FROM `{source_table_ref}`
        WHERE updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    """

    query_job = client.query(query)
    results = query_job.result()

    rows_to_insert = []

    # Flatten each row's JSON field and prepare for insertion
    for row in results:
        json_data = json.loads(row['json_field'])  # Assuming 'json_field' contains the JSON data
        flattened_data = flatten_json(json_data)
        flattened_data.update({
            'record_id': row['record_id'],
            'updated_at': row['updated_at']
        })
        rows_to_insert.append(flattened_data)

    if not rows_to_insert:
        print("No new data to insert.")
        return

    # Update the target table schema if new fields are found
    table = client.get_table(target_table_ref)
    existing_fields = {field.name for field in table.schema}
    new_fields = []

    for key in rows_to_insert[0].keys():
        if key not in existing_fields:
            new_fields.append(bigquery.SchemaField(key, "STRING"))

    if new_fields:
        updated_schema = table.schema + new_fields
        table.schema = updated_schema
        client.update_table(table, ['schema'])
        print(f"Updated table schema with new fields: {[field.name for field in new_fields]}")

    # Insert flattened rows into the target table
    errors = client.insert_rows_json(target_table_ref, rows_to_insert)
    if errors:
        print(f"Encountered errors while inserting rows: {errors}")
    else:
        print("Data successfully inserted.")

# dag_flatten_json_bigquery.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from flatten_json_bigquery import flatten_table_incrementally

# Define the DAG for Cloud Composer
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'flatten_json_bigquery_dag',
    default_args=default_args,
    description='A DAG to flatten JSON data in BigQuery incrementally every hour',
    schedule_interval=timedelta(hours=1),
    catchup=False,
)

flatten_task = PythonOperator(
    task_id='flatten_table_incrementally',
    python_callable=flatten_table_incrementally,
    dag=dag,
)

flatten_task

# Explanation:
# 1. The `flatten_json` function is used to convert nested JSON objects into a flat structure. It recursively traverses the JSON to create a dictionary with flattened keys.
# 2. The `flatten_table_incrementally` function queries records from the source table that have been updated within the last hour, flattens their JSON fields, and inserts the flattened data into the target table.
# 3. The target table (`target_table_id`) is static, meaning it will not be recreated each time. Instead, if new fields are found in the JSON, the table schema is updated accordingly.
# 4. If new fields are added to the JSON data in the future, the script will automatically detect these fields and update the target table schema to include them.
# 5. The script runs hourly, which means it processes records updated within the past hour. This is done using the `updated_at` timestamp to ensure only new or changed records are processed, avoiding reprocessing of older data.
# 6. The DAG (`dag_flatten_json_bigquery.py`) in Airflow schedules the `flatten_table_incrementally` function to run every hour using `PythonOperator`.

# Example Data:
# - The source table contains nested JSON data under the `json_field` column. Each row also has a `record_id` and `updated_at` field.
# - After flattening, the data is stored in the target table with a flat structure, making it easier to query and analyze.

# Source Table Data (source_table_id):
# | record_id | updated_at          | json_field                                                                                           |
# |-----------|---------------------|------------------------------------------------------------------------------------------------------|
# | 1         | 2024-11-28T10:00:00 | {"name": "John", "address": {"city": "NYC", "zip": "10001"}, "contacts": [{"type": "email", "value": "john@example.com"}, {"type": "phone", "value": "123-456-7890"}]} |
# | 2         | 2024-11-28T11:00:00 | {"name": "Jane", "skills": ["Python", "SQL"], "address": {"city": "LA", "zip": "90001"}}                                       |
# | 3         | 2024-11-28T12:00:00 | {"product": {"name": "Laptop", "details": {"brand": "Dell", "specs": {"ram": "16GB", "storage": "512GB SSD"}}}}                |
# | 4         | 2024-11-27T09:00:00 | {"name": "Alice", "hobbies": ["reading", "cycling"], "address": {"city": "SF", "zip": "94105"}}                                |

# Flattened Data in Target Table (target_table_id):
# | record_id | updated_at          | name  | address_city | address_zip | contacts_0_type | contacts_0_value     | contacts_1_type | contacts_1_value | skills_0 | skills_1 | product_name | product_details_brand | product_details_specs_ram | product_details_specs_storage | hobbies_0 | hobbies_1 |
# |-----------|---------------------|-------|--------------|-------------|----------------|----------------------|----------------|-----------------|----------|----------|--------------|-----------------------|--------------------------|------------------------------|-----------|-----------|
# | 1         | 2024-11-28T10:00:00 | John  | NYC          | 10001       | email          | john@example.com     | phone          | 123-456-7890    |          |          |              |                       |                          |                              |           |           |
# | 2         | 2024-11-28T11:00:00 | Jane  | LA           | 90001       |                |                      |                |                 | Python   | SQL      |              |                       |                          |                              |           |           |
# | 3         | 2024-11-28T12:00:00 |       |              |             |                |                      |                |                 |          |          | Laptop       | Dell                  | 16GB                     | 512GB SSD                    |           |           |

# Incremental Explanation
# - The script runs hourly to pick up records where the `updated_at` timestamp is within the last hour.
# - For example, if the script runs at 1 PM, it will process records updated between 12 PM and 1 PM.
# - This ensures the process is incremental, avoiding reprocessing of older records, and only focusing on the latest changes within the last hour.
# - If new fields are added to the JSON data in the future, the script will automatically update the target table schema to include these new fields.
 explain this code in step by step

