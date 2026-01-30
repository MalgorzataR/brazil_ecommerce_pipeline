"""
    This DAG is used to download and ingest a Brazil Ecommerce dataset from Kaggle.
    Data is stored in GCS and BigQuery.
"""
import os
import pendulum

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.task_group import TaskGroup

from custom_sensors.brazil_sensor import BrazilSensorOperator


dataset_file = "brazil_dataset"
dataset_url = f"https://www.kaggle.com/api/v1/datasets/download/olistbr/brazilian-ecommerce"
path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET_NAME = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("GCP_BQ_DATASET")

BRAZIL_FILES = {
    "customers": "olist_customers_dataset",
    "geolocation": "olist_geolocation_dataset",
    "order_items": "olist_order_items_dataset",
    "order_payments": "olist_order_payments_dataset",
    "order_reviews": "olist_order_reviews_dataset",
    "orders": "olist_orders_dataset",
    "products": "olist_products_dataset",
    "sellers": "olist_sellers_dataset",
    "category": "product_category_name_translation"
}

SCHEMAS = {
    "category": [
    {'name': 'product_category_name', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'product_category_name_english', 'type': 'STRING', 'mode': 'NULLABLE'},
    ]
}


with DAG(
    dag_id="brazil_ecommerce_ingestion",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    end_date=pendulum.datetime(2026, 1, 30, tz="UTC"),
    default_args = {
        "email": ["your@email.com"],
        "email_on_failure": True,
        "email_on_retry": False,
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 4,
        "retry_delay": pendulum.duration(minutes=5),
    },
    catchup=False,
    max_active_runs=1,
    tags=["data-ingestion", "brazil", "ecommerce", "loading"],
    doc_md=__doc__,
) as dag:

    # Sensor task to check for the availability of Brazil e-commerce data before proceeding with downstream tasks.
    check_for_brazil_data = BrazilSensorOperator(
        task_id="check_for_brazil_data",
        base_url=dataset_url,
        poke_interval=60,
        timeout=60*60,
        mode="reschedule",
    )
    
    wget_brazil_data = BashOperator(
        task_id="wget_brazil_data",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}.zip "
    )

    unzip_brazil_data = BashOperator(
        task_id="unzip_brazil_data",
        bash_command=f"unzip -o {path_to_local_home}/{dataset_file}.zip -d {path_to_local_home}"
    )

    taskgroups = []
    for name, file_name in BRAZIL_FILES.items():
        with TaskGroup(group_id=name) as tg_dataset:
            upload_csv_to_gcs = LocalFilesystemToGCSOperator(
            task_id=f"upload_csv_to_gcs_{name}",
            src=f"{path_to_local_home}/{file_name}.csv",
            dst=f"raw/{file_name}.csv",
            bucket=BUCKET_NAME
            )

            load_data_to_bq = GCSToBigQueryOperator(
                task_id=f"load_{name}_data_to_bq",
                bucket=BUCKET_NAME,
                source_objects=[f"raw/{file_name}.csv"],
                destination_project_dataset_table=f"{PROJECT_ID}.{BIGQUERY_DATASET}.{name}",
                schema_fields=SCHEMAS.get(name),
                autodetect=False if name in SCHEMAS.keys() else True,
                source_format="CSV",
                field_delimiter=",",
                quote_character='"',
                allow_quoted_newlines=True,
                write_disposition="WRITE_TRUNCATE",
                create_disposition="CREATE_IF_NEEDED",
            )

        taskgroups.append(tg_dataset)

    clean_up_local = BashOperator(
        task_id="clean_up_local",
        bash_command=f"rm {path_to_local_home}/*.csv | rm {path_to_local_home}/*.zip",
        trigger_rule="all_done",
    )

    check_for_brazil_data >> wget_brazil_data >> unzip_brazil_data
    unzip_brazil_data >> taskgroups
    taskgroups >> clean_up_local
