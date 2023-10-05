from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from io import BytesIO

# Load credentials from the service account file
credentials = service_account.Credentials.from_service_account_file(
    '/content/content/service-explore-labs-383615-7de7261b9ce5.json')

# Initialize GCS and BigQuery clients using the credentials
storage_client = storage.Client(credentials=credentials)
bq_client = bigquery.Client(credentials=credentials)

# Preprocess the dates in the DataFrame
def preprocess_dates_in_dataframe(df):
    df['effective_to_date'] = df['effective_to_date'].replace('31-Dec-9999', '9999-12-31')
    return df

# Try listing datasets
try:
    datasets = list(bq_client.list_datasets())
    print("Datasets in project:")
    for dataset in datasets:
        print(dataset.dataset_id)
except Exception as e:
    print(f"Error when listing datasets: {e}")

# Step 1: List files in the specified "folder" in the GCS bucket
bucket_name = 'cloudskool1'
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

    dataset_ref = bq_client.dataset('gdm')
    table_ref = dataset_ref.table(table_name)
    print(f"Truncating and loading data into BigQuery table {table_name}...")

    # Read the CSV data from the GCS blob into a pandas DataFrame
    blob_data = BytesIO(blob.download_as_bytes())
    df = pd.read_csv(blob_data)
    
    # Preprocess the dates in the DataFrame
    df = preprocess_dates_in_dataframe(df)

    # Load the DataFrame into BigQuery
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.autodetect = True
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    load_job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()
    
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


++++++++++++++

from flask import Flask, request, jsonify, render_template_string
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from io import BytesIO

app = Flask(__name__)

# Initialize GCS and BigQuery clients using the credentials
credentials = service_account.Credentials.from_service_account_file(
    '/content/content/service-explore-labs-383615-7de7261b9ce5.json')
storage_client = storage.Client(credentials=credentials)
bq_client = bigquery.Client(credentials=credentials)

def preprocess_dates_in_dataframe(df):
    df['effective_to_date'] = df['effective_to_date'].replace('31-Dec-9999', '9999-12-31')
    return df

@app.route('/load-data', methods=['GET', 'POST'])
def load_data():
    if request.method == 'POST':
        file_name = request.form.get('file_name')
        
        if not file_name:
            return "File name not provided", 400
        
        bucket_name = 'cloudskool1'
        blob = storage_client.get_bucket(bucket_name).blob(f"ingress/{file_name}")
        
        table_name = file_name.split('.')[0]
        dataset_ref = bq_client.dataset('gdm')
        table_ref = dataset_ref.table(table_name)
        
        # Read the CSV data from the GCS blob into a pandas DataFrame
        blob_data = BytesIO(blob.download_as_bytes())
        df = pd.read_csv(blob_data)
        
        # Preprocess the dates in the DataFrame
        df = preprocess_dates_in_dataframe(df)

        # Load the DataFrame into BigQuery
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.autodetect = True
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

        load_job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        load_job.result()
        
        return f"Data from {file_name} loaded successfully!"

    # If it's a GET request, render the form
    return render_template_string("""
        <form method="post">
            <label for="file_name">File Name:</label>
            <input type="text" id="file_name" name="file_name" required>
            <input type="submit" value="Load Data">
        </form>
    """)

if __name__ == '__main__':
    app.run(debug=True, port=8090)


++++++++++++++++++++++++++++=


from flask import Flask, request, jsonify, render_template_string
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
from io import BytesIO

app = Flask(__name__)

# Initialize GCS and BigQuery clients using the credentials
credentials = service_account.Credentials.from_service_account_file(
    '/content/content/service-explore-labs-383615-7de7261b9ce5.json')
storage_client = storage.Client(credentials=credentials)
bq_client = bigquery.Client(credentials=credentials)

def preprocess_dates_in_dataframe(df):
    df['effective_to_date'] = df['effective_to_date'].replace('31-Dec-9999', '9999-12-31')
    return df

@app.route('/load-data', methods=['GET', 'POST'])
def load_data():
    if request.method == 'POST':
        file_name = request.form.get('file_name')
        
        if not file_name:
            return "File name not provided", 400
        
        bucket_name = 'cloudskool1'
        blob = storage_client.get_bucket(bucket_name).blob(f"ingress/{file_name}")
        
        table_name = file_name.split('.')[0]
        dataset_ref = bq_client.dataset('gdm')
        table_ref = dataset_ref.table(table_name)
        
        # Read the CSV data from the GCS blob into a pandas DataFrame
        blob_data = BytesIO(blob.download_as_bytes())
        df = pd.read_csv(blob_data)
        
        # Preprocess the dates in the DataFrame
        df = preprocess_dates_in_dataframe(df)

        # Load the DataFrame into BigQuery
        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.autodetect = True
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

        load_job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        load_job.result()
        
        return f"Data from {file_name} loaded successfully!"

    # If it's a GET request, render the form
    return render_template_string("""
        <form method="post">
            <label for="file_name">File Name:</label>
            <input type="text" id="file_name" name="file_name" required>
            <input type="submit" value="Load Data">
        </form>
    """)

@app.route('/query-data', methods=['GET', 'POST'])
def query_data():
    if request.method == 'POST':
        sql_query = request.form.get('sql_query')
        
        if not sql_query:
            return "SQL query not provided", 400
        
        # Execute the query using BigQuery client
        query_job = bq_client.query(sql_query)
        results = query_job.result()
        
        # Convert results into a list of dictionaries
        rows = [dict(row.items()) for row in results]

        return jsonify(rows)

    # If it's a GET request, render the form
    return render_template_string("""
        <form method="post">
            <label for="sql_query">SQL Query:</label>
            <textarea id="sql_query" name="sql_query" required></textarea>
            <input type="submit" value="Execute Query">
        </form>
    """)

if __name__ == '__main__':
    app.run(debug=True, port=8090)




