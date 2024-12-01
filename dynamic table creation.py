from google.cloud import bigquery

# Initialize the BigQuery client
client = bigquery.Client()

# Set the project, dataset, and table details
project_id = "your_project"
dataset_id = "your_dataset"
table_name = "your_table_name"

# Query INFORMATION_SCHEMA to get column details
query = f"""
SELECT column_name, data_type, is_nullable
FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
WHERE table_name = '{table_name}'
ORDER BY ordinal_position;
"""

query_job = client.query(query)
results = query_job.result()

# Start constructing the CREATE TABLE statement
create_table_ddl = f"CREATE TABLE `{project_id}.{dataset_id}.new_table_name` (\n"

# Iterate through the columns and add to the DDL statement
for row in results:
    column_name = row.column_name
    data_type = row.data_type
    is_nullable = row.is_nullable

    # Handle NULLABLE constraint
    nullable_constraint = "NOT NULL" if is_nullable == "NO" else ""

    create_table_ddl += f"    {column_name} {data_type} {nullable_constraint},\n"

# Remove the trailing comma and close the statement
create_table_ddl = create_table_ddl.rstrip(",\n") + "\n);"

# Print the CREATE TABLE DDL
print(create_table_ddl)
