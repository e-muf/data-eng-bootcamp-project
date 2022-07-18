import os
from datetime import timedelta

from airflow import DAG
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

args = {
  "owner": "emanuel-dev",
  "retries": 1,
  "retray_delay": timedelta(minutes=5),
  'start_date': days_ago(1)
}

# Environment variables
GCS_PROJECT_BUCKET = os.getenv('GCS_SOURCE_DATA_BUCKET')

# DAG variables
parent_dag = "load_transform_dag"

# Macros
execution_date_nodash = "{{ ds_nodash }}"

with DAG(
  dag_id = "create_dwh_bq_dag",
  default_args = args,
  schedule_interval = "0 5 * * *",
) as dag:
  finish_workload = DummyOperator(
    task_id = "finish_workload"
  )

  input_sensor = GoogleCloudStorageObjectSensor(
    task_id = "sensor_task",
    bucket = GCS_PROJECT_BUCKET,
    object = f"data/signal/{parent_dag}/{execution_date_nodash}/_SUCCESS",
    mode = "poke",
    poke_interval = 60,
    timeout = 60 * 60 * 24 * 7
  )

  input_sensor >> finish_workload