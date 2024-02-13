To include start and end time logging for your Dataflow job, you can modify the Python script to log the current time at the beginning and end of the data processing. This helps in monitoring the duration of your job execution. Here's how you can adjust the bigquery_to_gcs.py script to include these timestamps.

Updated Python Script with Start and End Time Logging
Below is the modified version of the script. It logs the start time right before the pipeline starts and the end time after the pipeline finishes.

python
Copy code
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
from datetime import datetime

class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--output', type=str, help='Output GCS path prefix')

def run():
    options = MyOptions()

    # Log start time
    start_time = datetime.now()
    logging.info(f"Job started at {start_time}")

    p = beam.Pipeline(options=options)

    # Your BigQuery SQL query
    query = 'SELECT table_catalog, table_schema, table_name, column_name, is_nullable, data_type FROM your_project_id.your_dataset_id.INFORMATION_SCHEMA.COLUMNS'

    def format_result(element):
        return ','.join([str(element[k]) for k in element.keys()])

    (p 
     | 'ReadFromBigQuery' >> beam.io.Read(beam.io.BigQuerySource(query=query, use_standard_sql=True))
     | 'FormatToCSV' >> beam.Map(format_result)
     | 'WriteToGCS' >> beam.io.WriteToText(options.output, file_name_suffix='.csv', shard_name_template=''))

    result = p.run()
    result.wait_until_finish()

    # Log end time
    end_time = datetime.now()
    logging.info(f"Job finished at {end_time}")

    # Calculate and log duration
    duration = end_time - start_time
    logging.info(f"Total processing time: {duration}")

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
Execution Command
Execute your Dataflow job with the same command as before, making sure to replace the placeholders appropriately:

bash
Copy code
python bigquery_to_gcs.py \
    --runner DataflowRunner \
    --project your-project-id \
    --temp_location gs://your-bucket/temp \
    --output YOUR_GCS_PATH \
    --region your-region
This script logs the start time and end time of the job in the Google Cloud logging system, allowing you to track how long the job took to complete. You can view these logs in the Google Cloud Console under the Logging section, filtering by your Dataflow job's name.
