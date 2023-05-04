from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator, 
    BigQueryDeleteTableOperator,
)
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta


GCS_BUCKET_EXPRESS = Variable.get("GCS_BUCKET_EXPRESS")
PROJECT_ID = Variable.get("PROJECT_ID")

DESTINATION_SCHEMA = [
    {"name": "uid_txn", "type": "STRING", "mode": "REQUIRED"},
    {"name": "speed", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "currenct_datetime", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "road_name", "type": "STRING", "mode": "REQUIRED"}
]

default_args = {
    'owner': 'express_sensor',
    'depends_on_past': True,
    'start_date': datetime.today() - timedelta(days=2),
    'is_paused_upon_creation' : False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(   'clean_json_to_bigquery',
            schedule_interval="@daily",
            default_args=default_args
    ) as dags:

    START = DummyOperator(task_id="start")
    END = DummyOperator(task_id="end")

    gcs_to_temp = GCSToBigQueryOperator(
        task_id='gcs_to_temptable',
        gcp_conn_id = "GCP_ZOOMCAMP_CONNECTION",
        bucket = GCS_BUCKET_EXPRESS,
        source_objects = ["raw_express_sensor/*.gz"],
        schema_fields = DESTINATION_SCHEMA,
        destination_project_dataset_table = f"{PROJECT_ID}.express_way.temp",
        source_format='NEWLINE_DELIMITED_JSON',
        compression='GZIP',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
    )

    create_partition_cluster_table = BigQueryInsertJobOperator(
        task_id='create_raw_table',
        gcp_conn_id= "GCP_ZOOMCAMP_CONNECTION",
        configuration={
        "query": {
            "query": f"select uid_txn, road_name, TIMESTAMP_MILLIS(currenct_datetime) AS currenct_datetime, speed  from {PROJECT_ID}.express_way.temp",
            "useLegacySql": False,
            "writeDisposition": "WRITE_TRUNCATE",
            'destinationTable': {
                'projectId': f"{PROJECT_ID}",
                'datasetId': "express_way",
                'tableId': "express_raw" 
                },
            'clustering': {
                "fields" : [
                    "road_name"
                    ]
                },
            'timePartitioning' : {
                "type": "DAY",
                "field": "currenct_datetime"
                }
            }
        }
    )

    delete_temp_table = BigQueryDeleteTableOperator(
    task_id="delete_temp_table",
    gcp_conn_id= "GCP_ZOOMCAMP_CONNECTION",
    deletion_dataset_table=f"{PROJECT_ID}.express_way.temp",
    )

START >> gcs_to_temp >> create_partition_cluster_table >> delete_temp_table >> END