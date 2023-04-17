import os
import csv
import tempfile
from datetime import timedelta
from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook



default_args = {
    'owner': 'bina',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DATA_SOURCE_FILE = 'data/national-nutrition-survey-carbohydrate-intake-among-adult-singaporeans/national-nutrition-survey-carbohydrate-intake-by-gender-and-age-group.csv'

@task
def extract_data(source_file: str) -> str:
    gcs_hook = GCSHook()
    bucket_name = 'asia-southeast1-airflow3107-aeb72f53-bucket'
    local_source_file = '/tmp/national_nutrition.csv'
    gcs_hook.download(bucket_name, source_file, local_source_file)

    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.close()

    with open(local_source_file, 'r') as infile, open(temp_file.name, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        header = next(reader)
        writer.writerow(header)

        for row in reader:
            writer.writerow(row)

    return temp_file.name

@task
def upload_file_to_gcs(local_file: str, remote_file_path: str) -> str:
    gcs_hook = GCSHook()
    bucket_name = 'asia-southeast1-airflow3107-aeb72f53-bucket'
    remote_file = f'{remote_file_path}/{os.path.basename(local_file)}'
    gcs_hook.upload(bucket_name, remote_file, local_file)
    return remote_file


with DAG(
    'national_nutrition',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='0 0 1 1 *',  # Run once a year on January 1st at 00:00
    tags=['project']
) as dag:
    dag.doc_md = "extract national nutrition data from gcs and load to data big query (datawarehouse)"

    extract_data_task = extract_data(source_file=DATA_SOURCE_FILE)

    upload_data_task = upload_file_to_gcs(local_file=extract_data_task, remote_file_path='data/national-nutrition-survey-carbohydrate-intake-among-adult-singaporeans')

    load_data = GCSToBigQueryOperator(
        task_id='load_to_bq',
        bucket='asia-southeast1-airflow3107-aeb72f53-bucket',
        source_objects=[upload_data_task],
        field_delimiter=',',
        destination_project_dataset_table='project.national_nutrition',
        skip_leading_rows=1,
        schema_fields=[
            {'name': 'year', 'type': 'INTEGER'},
            {'name': 'gender', 'type': 'STRING'},
            {'name': 'age_group', 'type': 'STRING'},
            {'name': 'mean', 'type': 'FLOAT'},
            {'name': 'standard_error_of_mean', 'type': 'FLOAT'},
            {'name': '5th_percentile', 'type': 'FLOAT'},
            {'name': '10th_percentile', 'type': 'FLOAT'},
            {'name': '25th_percentile', 'type': 'FLOAT'},
            {'name': '50th_percentile', 'type': 'FLOAT'},
            {'name': '75th_percentile', 'type': 'FLOAT'},
            {'name': '90th_percentile', 'type': 'FLOAT'},
            {'name': '95th_percentile', 'type': 'FLOAT'}
        ],
        write_disposition='WRITE_TRUNCATE',  # Truncate the existing data in the table
    )

    extract_data_task >> upload_data_task >> load_data
