    
"""
    This DAG is used to create an analysis table for Brazil Ecommerce dataset.
    It uses Dataproc to run a PySpark job that creates an analysis table.
"""
import os

import pendulum

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.task_group import TaskGroup


DAG_ID="brazil_ecommerce_analysis" 
SRC_TABLE = "brazil_cumulative"
OUTPUT_TABLE = "brazil_analysis"
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET_NAME = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("GCP_BQ_DATASET")
PROJECT_REGION = os.environ.get("GCP_PROJECT_REGION")
CLUSTER_NAME = os.environ.get("GCP_CLUSTER_NAME")
TEMP_BUCKET = os.environ.get("GCP_TEMP_BUCKET")


with DAG(
    dag_id=DAG_ID,
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    end_date=pendulum.datetime(2026, 1, 30, tz="UTC"),
    default_args = {
        "email": ["your@email.com"],
        "email_on_failure": True,
        "email_on_retry": False,
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
    },
    catchup=False,
    max_active_runs=1,
    tags=["brazil", "ecommerce", "analysis", "dataproc"],
    doc_md=__doc__,
) as dag:

    check_brazil_data = BigQueryTableExistenceSensor(
        task_id=f"check_bq_table",
        table_id=SRC_TABLE,
        project_id=PROJECT_ID,
        dataset_id=BIGQUERY_DATASET,
        poke_interval=60,
        timeout=60*60,
        mode="reschedule",
    )
    
    load_script_gsc = LocalFilesystemToGCSOperator(
        task_id="load_script_gsc",
        src="/opt/airflow/dags/scripts/brazil_ecommerce_analysis.py",
        dst="code/brazil_ecommerce_analysis.py",
        bucket=BUCKET_NAME,
    )
    
    create_analysis_table = DataprocSubmitJobOperator(
        task_id="create_analysis_table",
        region=PROJECT_REGION,
        project_id=PROJECT_ID,
        gcp_conn_id="google_cloud_dataproc",
        job={
            'reference': {'project_id': PROJECT_ID},
            'placement': {'cluster_name': CLUSTER_NAME},
            'pyspark_job': {
                'main_python_file_uri': f'gs://{BUCKET_NAME}/code/{DAG_ID}.py',
                'args': [
                    f'--src_table={SRC_TABLE}',
                    f'--project_id={PROJECT_ID}',
                    f'--dataset_id={BIGQUERY_DATASET}',
                    f'--output_table={OUTPUT_TABLE}',
                    f'--temp_bucket={TEMP_BUCKET}'
                ]
            }
        },
        trigger_rule="all_success",
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=PROJECT_REGION
    )

    check_brazil_data >> load_script_gsc >> create_analysis_table >> delete_cluster
