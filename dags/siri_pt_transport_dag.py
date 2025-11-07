from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

# Passed as an argument for the instance 'dag' of the class 'DAG'
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

common_environment = {
    'AWS_ACCESS_KEY_ID': '{{ var.value.aws_access_key_id }}',
    'AWS_SECRET_ACCESS_KEY': '{{ var.value.aws_secret_access_key }}',
    'API_KEY': '{{ var.value.siri_api_key }}',
    'S3_BUCKET_NAME': '{{ var.value.s3_bucket_name }}',
    'AWS_REGION': '{{ var.value.aws_region }}'
}

with DAG(
    dag_id='siri_pt_daily_etl',
    default_args=default_args,
    description='Daily ETL for Swiss SIRI-PT transport data.',
    schedule='@daily',
    catchup=False,
    tags=['siri-pt', 'etl', 'docker'],
) as dag:

    extract_task = DockerOperator(
        task_id='extract_raw_data',
        image='my-siri-etl:latest',
        auto_remove='success',
        environment=common_environment,
        command='python siri_etl.py extract',
        network_mode='bridge',
        mount_tmp_dir=False
    )

    transform_task = DockerOperator(
        task_id='transform_to_parquet',
        image='my-siri-etl:latest',
        auto_remove='success',
        environment=common_environment,
        command='python siri_etl.py transform',
        network_mode='bridge',
        mount_tmp_dir=False
    )

    # Ensures that transform_task only runs if extract_task succeeds
    extract_task >> transform_task
