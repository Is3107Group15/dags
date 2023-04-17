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

DATA_SOURCE_FILE = 'data/government-health-expenditure/government-health-expenditure.csv'

@task
def extract_data(source_file: str) -> str:
    gcs_hook = GCSHook()
    bucket_name = 'asia-southeast1-airflow3107-aeb72f53-bucket'
    local_source_file = '/tmp/health_expenditure.csv'
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


@task
def filter_data_by_year(source_file: str, min_year: int = 2010) -> str:
    temp_file = tempfile.NamedTemporaryFile(delete=False)
    temp_file.close()

    with open(source_file, 'r') as infile, open(temp_file.name, 'w', newline='') as outfile:
        reader = csv.reader(infile)
        writer = csv.writer(outfile)
        
        header = next(reader)
        writer.writerow(header)

        for row in reader:
            year = int(row[0])
            if year >= min_year:
                writer.writerow(row)

    return temp_file.name

with DAG(
    'government_health_expenditure_2010_onwards',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='0 0 1 1 *',  # Run once a year on January 1st at 00:00
    tags=['project']
) as dag:
    dag.doc_md = "extract health expenditure data from gcs and load to data big query (datawarehouse)"

    extract_data_task = extract_data(source_file=DATA_SOURCE_FILE)

    filter_data_task = filter_data_by_year(source_file=extract_data_task)

    remote_filtered_data = f'data/government-health-expenditure/filtered/{filter_data_task}'
    upload_data_task = upload_file_to_gcs(local_file=filter_data_task, remote_file_path='data/government-health-expenditure/filtered')


    load_data = GCSToBigQueryOperator(
        task_id='load_to_bq',
        bucket='asia-southeast1-airflow3107-aeb72f53-bucket',
        source_objects=[upload_data_task],
        field_delimiter=',',
        destination_project_dataset_table='project.health_expenditure',
        skip_leading_rows=1,
        schema_fields=[
            {'name': 'financial_year', 'type': 'INTEGER'},
            {'name': 'operating_expenditure', 'type': 'INTEGER'},
            {'name': 'development_expenditure', 'type': 'INTEGER'},
            {'name': 'government_health_expenditure', 'type': 'FLOAT'},
            {'name': 'percentage_gdp', 'type': 'FLOAT'}
        ],
        write_disposition='WRITE_TRUNCATE',  # Truncate the existing data in the table
    )

    extract_data_task >> filter_data_task >> upload_data_task >> load_data