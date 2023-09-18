import csv
from collections import defaultdict
from google.cloud import bigquery
import logging

# Set up logging
logging.basicConfig(filename='script_log.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

def cast_value(value, bq_type):
    """Cast the value to the specified BigQuery data type."""
    try:
        if value is None or value == "":
            return None
        if bq_type == "STRING":
            return str(value)
        elif bq_type == "INTEGER":
            return int(value)
        elif bq_type == "FLOAT":
            return float(value)
        elif bq_type == "DATE":
            return str(value)  # Assuming the date is already in 'YYYY-MM-DD' format in CSV
        # Add more types as needed
        else:
            return value  # Return original value if type is not recognized
    except Exception as e:
        logging.error(f"Error casting value {value} to {bq_type}: {e}")
        return None

# Initialize the BigQuery client
client = bigquery.Client()
dataset_id = "your_dataset_id"  # Replace with your dataset ID

# Read and Process the Input File
with open('input_file.csv', 'r') as file:
    ...
    # (rest of the reading and processing code remains unchanged)
    ...

# Write the output files and attempt to load them into BigQuery
for table_name, rows in data.items():
    file_path = f'{table_name}.csv'
    table_ref = dataset_ref.table(table_name)
    
    try:
        table = client.get_table(table_ref)  # Fetch table metadata
        table_schema = table.schema

        # Cast the data based on the BigQuery table schema
        casted_rows = []
        for row in rows:
            casted_row = [cast_value(val, field.field_type) for val, field in zip(row, table_schema)]
            casted_rows.append(casted_row)

        # Write casted data to CSV
        with open(file_path, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(headers[table_name])  # Write header
            for row in casted_rows:
                writer.writerow(row)

        # Load data from CSV to BigQuery
        job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1)
        with open(file_path, "rb") as file:
            job = client.load_table_from_file(file, table_ref, job_config=job_config)
        job.result()  # Wait for the job to complete
        logging.info(f"Loaded data from {file_path} into {dataset_id}:{table_name}.")
    except Exception as e:
        logging.error(f"Error processing table {table_name}: {e}")
        print(f"Table {table_name} had an error. Check the logs for more details.")

print("Script execution completed! Check the log for details.")
