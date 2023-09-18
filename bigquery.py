import csv
from collections import defaultdict
from google.cloud import bigquery
import os
from datetime import datetime
from google.oauth2 import service_account
import json
# credentials = service_account.Credentials.from_service_account_file('/content/service-explore-labs-383615-5118216a9657.json')

# Load the configuration
with open('config.json', 'r') as file:
    config = json.load(file)

# Use the configuration in the script
project_id = config['project_id']
dataset_id = config['dataset_id']
#credentials_path = config['credentials_path']
service_account_name = config['service_account_name']
input_file = config['input_file']
#credentials = service_account.Credentials.from_service_account_file(credentials_path)

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_name

#client = bigquery.Client(credentials= credentials,project=project_id)
client = bigquery.Client(project=project_id)

# Specify your dataset
# dataset_id = "loony_dataset"  # Replace with your dataset ID

# Ensure the dataset exists; if not, create it
dataset_ref = client.dataset(dataset_id)
try:
    client.get_dataset(dataset_ref)
except Exception as e:
    dataset = bigquery.Dataset(dataset_ref)
    client.create_dataset(dataset)
    print(f"Dataset {dataset_id} created.")

# Read the input file
with open('input.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip the header row

    # Process the data
    data = defaultdict(list)
    column_counter = defaultdict(int)
    headers = defaultdict(list)

    for row in reader:
        if not row:  # Skip empty rows
            continue

        table_name = row[0].replace(' ', '_').lower()  # Convert spaces to underscores and make lowercase
        column_name = row[1].replace(' ', '_').upper()  # Convert spaces to underscores and make uppercase for column name

        # Increment the counter for the column_name and format it
        column_counter[column_name] += 1
        code = f"{row[2]}{column_counter[column_name]:03d}"

        name = row[3]
        description = row[3]
        effective_from_date = datetime.strptime("01-Jan-2001", "%d-%b-%Y").strftime("%Y-%m-%d")
        effective_to_date = datetime.strptime("31-Jan-9999", "%d-%b-%Y").strftime("%Y-%m-%d")
        # effective_from_date = "01-Jan-01"
        # effective_to_date = "01-Jan-99"

        data[table_name].append([code, name, description, effective_from_date, effective_to_date])

        # Store header for each table
        if table_name not in headers:
            headers[table_name] = [column_name, "name", "description", "effective_from_date", "effective_to_date"]

# Write the output files and load them into BigQuery
for table_name, rows in data.items():
    table_name_raw = f"{table_name}_raw"
    file_path = f'{table_name_raw}.csv'

    # file_path = f'{table_name}.csv'
    
    # Write to CSV
    with open(file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers[table_name])  # Write header
        for row in rows:
            writer.writerow(row)

    # Check if the table exists, if not create it
    table_ref = dataset_ref.table(table_name_raw)
    try:
        client.get_table(table_ref)
        print(f"Table {table_name_raw} already exists in dataset {dataset_id}. Will truncate and reload.")
    except Exception as e:
        # Create the table based on headers
        schema = [
            bigquery.SchemaField(headers[table_name][0], "STRING", mode="REQUIRED"),
            bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("description", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("effective_from_date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("effective_to_date", "DATE", mode="REQUIRED")
        ]
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Table {table_name_raw} created in dataset {dataset_id}.")

    # Load data from CSV to BigQuery
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1,write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    with open(file_path, "rb") as file:
        job = client.load_table_from_file(file, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f"Loaded data from {file_path} into {dataset_id}:{table_name_raw}.")

print("Files created and loaded into BigQuery successfully!")
