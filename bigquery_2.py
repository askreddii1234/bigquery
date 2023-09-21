!pip install google-cloud-storage google-cloud-bigquery

import google.cloud.storage
from google.cloud import bigquery

# Initialize GCS and BigQuery clients
storage_client = google.cloud.storage.Client()
bq_client = bigquery.Client()

# Try listing datasets
try:
    datasets = list(bq_client.list_datasets())
    print("Datasets in project:")
    for dataset in datasets:
        print(dataset.dataset_id)
except Exception as e:
    print(f"Error when listing datasets: {e}")

# Step 1: List files in the specified "folder" in the GCS bucket
bucket_name = 'cloudskool'
folder_name = 'ingress/'  # Note the trailing slash to ensure it's a folder

bucket = storage_client.get_bucket(bucket_name)
blobs = bucket.list_blobs(prefix=folder_name)

# Filter files based on your condition (if any)
filtered_files = list(blobs)

# Step 3 and 4: Load files into BigQuery with truncation and print row counts
for blob in filtered_files:
    table_name = blob.name.split('/')[-1].split('.')[0]  # Extracting table name from the file name
    
    # Print and check derived table name
    print(f"Derived table name for blob {blob.name}: {table_name}")
    if not table_name:
        print(f"Skipping blob {blob.name} due to invalid derived table name.")
        continue

    dataset_ref = bq_client.dataset('loon_bucket_bq')
    table_ref = dataset_ref.table(table_name)
    
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV  # Set source format to CSV
    job_config.autodetect = True
    job_config.skip_leading_rows = 1  # Skip header row in CSV
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE  # Truncate table before loading
    
    load_job = bq_client.load_table_from_uri(
        f'gs://{bucket_name}/{blob.name}', table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to finish
    
    # Fetch job statistics
    job_stats = load_job._properties['statistics']['load']
    rows_loaded = job_stats['outputRows']

    # Fetch table statistics
    table = bq_client.get_table(table_ref)
    table_rows = table.num_rows

    # Print the number of records loaded and the number of rows in the table
    print(f"For file {blob.name}:")
    print(f"Number of records loaded from source: {rows_loaded}")
    print(f"Number of rows in BigQuery table {table_name}: {table_rows}")
    print("----")
