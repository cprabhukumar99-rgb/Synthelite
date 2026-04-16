"""
Apache Airflow DAG example - Simple ETL workflow
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 1, 1),
}

# Define the DAG
dag = DAG(
    'etl_pipeline_dag',
    default_args=default_args,
    description='A simple ETL pipeline DAG',
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False,
)


# Define Python functions for tasks
def extract_data():
    """Extract data from source"""
    print("Extracting data...")
    return "Data extracted successfully"


def transform_data():
    """Transform the extracted data"""
    print("Transforming data...")
    return "Data transformed successfully"


def load_data():
    """Load data to destination"""
    print("Loading data...")
    return "Data loaded successfully"


def validate_data():
    """Validate the loaded data"""
    print("Validating data...")
    return "Data validation completed"


# Define tasks
task_extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

task_transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

task_load = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)

task_validate = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

task_cleanup = BashOperator(
    task_id='cleanup',
    bash_command='echo "Cleaning up temporary files"',
    dag=dag,
)

# Set task dependencies (workflow order)
task_extract >> task_transform >> task_load >> task_validate >> task_cleanup

if __name__ == "__main__":
    dag.cli()
