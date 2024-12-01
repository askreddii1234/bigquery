from google.cloud import spanner
from google.cloud import bigquery
from google.api_core import retry
import json
import os
from google.cloud.spanner_v1 import param_types

# Configuration settings for different environments
CONFIG = {
    "dev": {
        "GCP_PROJECT_ID": "your-gcp-project-id-dev",
        "SPANNER_INSTANCE_ID": "your-spanner-instance-id-dev",
        "SPANNER_DATABASE_ID": "your-spanner-database-id-dev",
        "BIGQUERY_DATASET": "your-bigquery-dataset-dev",
        "BIGQUERY_TABLE": "your-bigquery-table-dev"
    },
    "int": {
        "GCP_PROJECT_ID": "your-gcp-project-id-int",
        "SPANNER_INSTANCE_ID": "your-spanner-instance-id-int",
        "SPANNER_DATABASE_ID": "your-spanner-database-id-int",
        "BIGQUERY_DATASET": "your-bigquery-dataset-int",
        "BIGQUERY_TABLE": "your-bigquery-table-int"
    },
    "prod": {
        "GCP_PROJECT_ID": "your-gcp-project-id-prod",
        "SPANNER_INSTANCE_ID": "your-spanner-instance-id-prod",
        "SPANNER_DATABASE_ID": "your-spanner-database-id-prod",
        "BIGQUERY_DATASET": "your-bigquery-dataset-prod",
        "BIGQUERY_TABLE": "your-bigquery-table-prod"
    }
}

# Initialize Spanner and BigQuery clients
def get_spanner_client(project_id):
    return spanner.Client(project=project_id)

def get_bigquery_client(project_id):
    return bigquery.Client(project=project_id)

def fetch_data_from_spanner(instance_id, database_id, spanner_table, project_id):
    # Connect to Spanner database
    spanner_client = get_spanner_client(project_id)
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    # Query to fetch all data from the Spanner table
    with database.snapshot() as snapshot:
        results = snapshot.execute_sql(f"SELECT * FROM {spanner_table}")
        rows = [dict(row) for row in results]
    return rows

def truncate_bigquery_table(dataset, table, project_id):
    bigquery_client = get_bigquery_client(project_id)
    table_id = f"{project_id}.{dataset}.{table}"
    try:
        query = f"TRUNCATE TABLE `{table_id}`"
        query_job = bigquery_client.query(query)
        query_job.result()  # Wait for query completion
        print(f"Table {table_id} truncated successfully.")
    except Exception as e:
        print(f"Table {table_id} does not exist or could not be truncated. Skipping truncation.")

def load_data_to_bigquery(rows, dataset, table, project_id):
    # Define BigQuery table reference
    bigquery_client = get_bigquery_client(project_id)
    table_ref = bigquery_client.dataset(dataset).table(table)

    # Batch processing logic
    BATCH_SIZE = 50  # Batch size of 50 records
    for i in range(0, len(rows), BATCH_SIZE):
        batch = rows[i:i + BATCH_SIZE]

        # Prepare rows for BigQuery insertion
        formatted_rows = []
        for row in batch:
            formatted_row = {}
            for key, value in row.items():
                # Convert Spanner types to BigQuery compatible types
                if isinstance(value, str):
                    try:
                        # Attempt to parse JSON strings to preserve nested structure
                        parsed_value = json.loads(value)
                        if isinstance(parsed_value, (dict, list)):
                            formatted_row[key] = parsed_value  # Keep as dict or list for BigQuery JSON
                        else:
                            formatted_row[key] = value
                    except json.JSONDecodeError:
                        formatted_row[key] = value
                elif isinstance(value, spanner.CommittedTimestamp):
                    formatted_row[key] = value.isoformat()
                elif isinstance(value, spanner.Date):
                    formatted_row[key] = value.isoformat()
                elif isinstance(value, spanner.Timestamp):
                    formatted_row[key] = value.isoformat()
                else:
                    formatted_row[key] = value
            formatted_rows.append(formatted_row)

        # Insert batch into BigQuery
        errors = bigquery_client.insert_rows_json(table_ref, formatted_rows, retry=retry.Retry(deadline=120))
        if errors:
            print(f"Errors occurred while inserting batch {i // BATCH_SIZE + 1}: {errors}")
        else:
            print(f"Batch {i // BATCH_SIZE + 1} inserted successfully.")

def migrate_data(environment):
    if environment not in CONFIG:
        return {'error': 'Invalid environment specified'}

    # Get configuration for the specified environment
    config = CONFIG[environment]

    # Truncate BigQuery table before loading data
    truncate_bigquery_table(config['BIGQUERY_DATASET'], config['BIGQUERY_TABLE'], config['GCP_PROJECT_ID'])

    # Fetch data from Spanner
    rows = fetch_data_from_spanner(config['SPANNER_INSTANCE_ID'], config['SPANNER_DATABASE_ID'], "your_spanner_table", config['GCP_PROJECT_ID'])

    # Load data into BigQuery
    load_data_to_bigquery(rows, config['BIGQUERY_DATASET'], config['BIGQUERY_TABLE'], config['GCP_PROJECT_ID'])

    return {'message': 'Data migration completed successfully'}
