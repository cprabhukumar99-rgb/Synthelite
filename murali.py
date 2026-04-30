from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime
import pendulum

utc = pendulum.timezone("UTC")

with DAG(
    dag_id="gcs_to_bigquery_daily1122",
    start_date=datetime(2026, 4, 26, tzinfo=utc),
    schedule="30 10 * * *",   # Runs at 4:00 PM IST
    catchup=False,
    tags=["gcs", "bigquery"],
) as dag:

    load_csv = GCSToBigQueryOperator(
        task_id="load_csv_to_bq",
        bucket="nagasai005-airflow-bucket",
        source_objects=["input/emp.csv"],
        destination_project_dataset_table="nagasai005.emp_dataset.emp_table",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
        schema_fields=[
            {"name": "emp_no", "type": "INTEGER"},
            {"name": "emp_name", "type": "STRING"},
            {"name": "salary", "type": "INTEGER"},
            {"name": "company", "type": "STRING"},
            {"name": "experience", "type": "INTEGER"},
        ],
    )

    load_csv