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
    'owner': 'clare',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

DATA_SOURCE_FILE = 'data/vaccination-and-immunisation-of-students/vaccination-and-immunisation-of-students-annual.csv'

@task
def extract_data(source_file: str) -> str:
    gcs_hook = GCSHook()
    bucket_name = 'asia-southeast1-airflow3107-aeb72f53-bucket'
    local_source_file = '/tmp/vaccination-of-students.csv' 
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
    'vaccination_of_students',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='0 0 1 1 *',  # Run once a year on January 1st at 00:00
    tags=['project']
) as dag:
    dag.doc_md = "extract vaccination of students data from gcs and load to data big query (datawarehouse)"

    extract_data_task = extract_data(source_file=DATA_SOURCE_FILE)

    upload_data_task = upload_file_to_gcs(local_file=extract_data_task, remote_file_path='data/vaccination-and-immunisation-of-students')

    load_data = GCSToBigQueryOperator(
        task_id='load_to_bq',
        bucket='asia-southeast1-airflow3107-aeb72f53-bucket',
        source_objects=[upload_data_task],
        field_delimiter=',',
        destination_project_dataset_table='project.vaccination-and-immunisation-of-students',
        skip_leading_rows=1,
        schema_fields=[
            {'name': 'year', 'type': 'INTEGER'},
            {'name': 'vaccination_type', 'type': 'STRING'},
            {'name': 'no_of_doses_in_thousands', 'type': 'FLOAT'},
            
        ],
        write_disposition='WRITE_TRUNCATE',  # Truncate the existing data in the table
    )

    extract_data_task >> upload_data_task >> load_data