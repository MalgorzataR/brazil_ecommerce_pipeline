"""
    This DAG is used to create a cumulative table for Brazil Ecommerce dataset.
    It uses Dataproc to run a PySpark job that creates a cumulative table.
"""
import os

import pendulum
from google.api_core.retry import Retry

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.task_group import TaskGroup


DAG_ID = "brazil_ecommerce_cumulative"
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET_NAME = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("GCP_BQ_DATASET")
PROJECT_REGION = os.environ.get("GCP_PROJECT_REGION")
CLUSTER_NAME = os.environ.get("GCP_CLUSTER_NAME")
TEMP_BUCKET = os.environ.get("GCP_TEMP_BUCKET")
OUTPUT_TABLE = "brazil_cumulative"

BRAZIL_SRC_TABLES = [
    "order_items",
    "order_payments",
    "orders",
    "products",
    "category"
]

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "e2-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
    },
    "worker_config": {
        "num_instances": 0,
    },
    "software_config": {
        "properties": {
            "dataproc:dataproc.allow.zero.workers": "true"
        }
    }
}


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
    tags=["brazil", "ecommerce", "cumulative", "dataproc"],
    doc_md=__doc__,
) as dag:

    load_script_gsc = LocalFilesystemToGCSOperator(
        task_id="load_script_gsc",
        src=f"/opt/airflow/dags/scripts/{DAG_ID}.py",
        dst=f"code/{DAG_ID}.py",
        bucket=BUCKET_NAME,
    )

    task_groups = []
    for table in BRAZIL_SRC_TABLES:
        with TaskGroup(group_id=f"{table}_data_check") as tg:
            check_brazil_data = BigQueryTableExistenceSensor(
                task_id=f"check_{table}_bq_table",
                table_id=table,
                project_id=PROJECT_ID,
                dataset_id=BIGQUERY_DATASET,
                poke_interval=60,
                timeout=60*60,
                mode="reschedule",
            )
        task_groups.append(tg)
 
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=PROJECT_REGION,
        cluster_name=CLUSTER_NAME,
        retry=Retry(maximum=100.0, initial=10.0, multiplier=1.0),
        num_retries_if_resource_is_not_ready=3,
    )

    create_cumulative_table = DataprocSubmitJobOperator(
        task_id="create_cumulative_table",
        region=PROJECT_REGION,
        project_id=PROJECT_ID,
        gcp_conn_id="google_cloud_dataproc",
        job={
            'reference': {'project_id': PROJECT_ID},
            'placement': {'cluster_name': CLUSTER_NAME},
            'pyspark_job': {
                'main_python_file_uri': f'gs://{BUCKET_NAME}/code/{DAG_ID}.py',
                'args': [
                    f'--project_id={PROJECT_ID}',
                    f'--dataset_id={BIGQUERY_DATASET}',
                    f'--output_table={OUTPUT_TABLE}',
                    f'--temp_bucket={TEMP_BUCKET}'
                ]
            }
        },
        trigger_rule="all_success",
    )
    
    load_script_gsc >> task_groups
    task_groups >> create_cluster
    create_cluster >> create_cumulative_table
