from google.cloud import bigquery

def get_table_row_count(project_id, dataset_id, table_id):
    # Initialize the BigQuery client
    client = bigquery.Client(project=project_id)
    
    # Construct the table reference
    table_ref = bigquery.dataset.DatasetReference(project_id, dataset_id).table(table_id)
    
    # Get the table
    table = client.get_table(table_ref)
    
    # Print the number of rows in the table
    print(f"Number of rows in table {table_id}: {table.num_rows}")

# Replace with your actual project, dataset, and table IDs
project_id = "YOUR_PROJECT_ID"
dataset_id = "YOUR_DATASET_ID"
table_id = "YOUR_TABLE_ID"

# Call the function
get_table_row_count(project_id, dataset_id, table_id)
