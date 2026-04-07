from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

# ---------------- DAG ----------------
with DAG(
    dag_id="gcs_to_bq_merge_pipeline",
    start_date=datetime(2026, 4, 7),
    schedule_interval=None,
    catchup=False,
) as dag:

    # ---------------- STEP 1: Load CSV to STAGING ----------------
    load_to_staging = GCSToBigQueryOperator(
        task_id="load_to_staging",

        bucket="nagasai55",   # ✅ correct bucket name
        source_objects=["aades.csv"],  # ✅ only file name

        destination_project_dataset_table="project-5ca53396-d9bc-483a-a4b.nagasai456.zzztable",

        schema_fields=[
            {"name": "emp_no", "type": "INTEGER"},
            {"name": "emp_name", "type": "STRING"},
            {"name": "salary", "type": "INTEGER"},
            {"name": "company", "type": "STRING"},
        ],

        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        autodetect=False,
    )

    # ---------------- STEP 2: MERGE ----------------
    merge_to_final = BigQueryInsertJobOperator(
        task_id="merge_to_final",
        configuration={
            "query": {
                "query": """
                MERGE `project-5ca53396-d9bc-483a-a4b.nagasai456.finaltable2` T
                USING `project-5ca53396-d9bc-483a-a4b.nagasai456.zzztable` S
                ON T.emp_no = S.emp_no

                WHEN MATCHED THEN
                  UPDATE SET
                    emp_name = S.emp_name,
                    salary = S.salary,
                    company = S.company

                WHEN NOT MATCHED THEN
                  INSERT (emp_no, emp_name, salary, company)
                  VALUES (S.emp_no, S.emp_name, S.salary, S.company)
                """,
                "useLegacySql": False,
            }
        },
    )

    # ---------------- FLOW ----------------
    load_to_staging >> merge_to_final