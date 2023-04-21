#Since this data of the number of those using sports facillities only has a supporting role
# to complement our main findings, we downloaded the data as a CSV file directly from the website source
# (https://tablebuilder.singstat.gov.sg/table/TS/M890361) and used Airflow to upload this data to BigQuery.



import requests,os
from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.email_operator import EmailOperator
import csv
from datetime import timedelta

default_args = {
    'owner': 'sansuthn',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'sports_booking_data_to_bq',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='0 0 1 1 *',  # Run once a year on January 1st at 00:00
    tags=['project']
) as dag:
    dag.doc_md = "extract data of number of sport facilities users data from gcs and load to data big query (datawarehouse)"

    load_data = GCSToBigQueryOperator(
        task_id='load_to_bq',
        bucket='asia-southeast1-airflow3107-aeb72f53-bucket',
        source_objects=['data/totalsportsbooking.csv'],
        field_delimiter =',',
        destination_project_dataset_table='project.sportsusersdata',
        skip_leading_rows=1,
        schema_fields=[
            {'name': 'Year', 'type': 'INTEGER'},
            {'name': 'Total Bookings', 'type': 'INTEGER'},
        ],
        write_disposition='WRITE_TRUNCATE',
    )

    load_data