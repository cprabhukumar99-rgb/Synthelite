from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime
import pendulum

# Set timezone to India
local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    "owner": "prabhu",
    "start_date": datetime(2026, 4, 27, tzinfo=local_tz),
}

with DAG(
    dag_id="gcs_to_bigquery_daily",
    default_args=default_args,
    schedule="0 10 * * *",   # 10 AM IST
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
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime
import pendulum

local_tz = pendulum.timezone("Asia/Kolkata")

default_args = {
    "owner": "prabhu",
    "start_date": datetime(2026, 4, 27, tzinfo=local_tz),
}

with DAG(
    dag_id="gcs_to_bigquery_daily",
    default_args=default_args,
    schedule="0 14 * * *",   # 2 PM IST
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