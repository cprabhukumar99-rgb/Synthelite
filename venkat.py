from google.cloud import bigquery

# Initialize client
client = bigquery.Client()

# Define your details
project_id = "your-project-id"
dataset_id = "your_dataset"
table_id = "your_table"

# GCS file path
uri = "gs://your-bucket-name/your-file.csv"

# Full table reference
table_ref = f"{project_id}.{dataset_id}.{table_id}"

# Job configuration
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,  # Change if JSON/Parquet
    skip_leading_rows=1,  # Skip header row
    autodetect=True  # Automatically detect schema
)

# Load data from GCS to BigQuery
load_job = client.load_table_from_uri(
    uri,
    table_ref,
    job_config=job_config
)

# Wait for job to complete
load_job.result()

# Print result
table = client.get_table(table_ref)
print(f"Loaded {table.num_rows} rows into {table_ref}")