import os
from datetime import timedelta
from queue import Empty
from urllib.parse import quote_plus

from numpy import empty

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcp_sql_operator import CloudSQLExecuteQueryOperator, CloudSqlInstanceImportOperator
from airflow.providers.google.cloud.operators.dataproc import (
  ClusterGenerator,
  DataprocCreateClusterOperator,
  DataprocDeleteClusterOperator,
  DataprocSubmitJobOperator
)
from airflow.utils.dates import days_ago

args = {
  "owner": "emanuel-dev",
  "retries": 1,
  "retray_delay": timedelta(minutes=5),
  'start_date': days_ago(1)
}

GCP_PROJECT_ID =  os.getenv('GCP_PROJECT')
GCP_REGION = "us-central1"
GCS_PROJECT_BUCKET = os.getenv('GCS_SOURCE_DATA_BUCKET')
GCSQL_POSTGRES_DATABASE_NAME = os.getenv('GCSQL_POSTGRES_DATABASE_NAME')
GCSQL_POSTGRES_INSTANCE_NAME_QUERY = os.getenv('GCSQL_POSTGRES_INSTANCE_NAME_QUERY')

CLUSTER_NAME = 'ephemeral-spark-cluster-{{ ds_nodash }}'


user_purchase_source_objet = "data/user_purchase.csv"
pyspark_job_object = "code/transform_reviews.py"
schema_table_name = "movies_schema.user_purchase"

SQL = f"""
DROP SCHEMA IF EXISTS movies_schema CASCADE;
CREATE SCHEMA IF NOT EXISTS movies_schema;
CREATE TABLE IF NOT EXISTS {schema_table_name} (
  invoice_number varchar(10),
  stock_code varchar(20),
  detail varchar(1000),
  quantity int,
  invoice_date timestamp,
  unit_price numeric(8,3),
  customer_id int,
  country varchar(20)
);
"""

import_body = {
  "importContext": {
    "database": GCSQL_POSTGRES_DATABASE_NAME,
    "fileType": "csv",
    "uri": f'gs://{GCS_PROJECT_BUCKET}/{user_purchase_source_objet}',
    "csvImportOptions": {
      "table": schema_table_name
    }
  }
}

PYSPARK_JOB = {
  "reference": {"project_id": GCP_PROJECT_ID},
  "placement": {"cluster_name": CLUSTER_NAME},
  "pyspark_job": {
    "main_python_file_uri": f"gs://{GCS_PROJECT_BUCKET}/{pyspark_job_object}",
    "jar_file_uris": [
      f"gs://{GCS_PROJECT_BUCKET}/resources/postgresql-42.4.0.jar"
    ]
  }
}

CLUSTER_CONFIG = ClusterGenerator(
  project_id = GCP_PROJECT_ID,
  cluster_name = CLUSTER_NAME,
  master_machine_type = "n1-standard-2",
  worker_machine_type = "n1-standard-2",
  num_workers = 2,
  master_disk_type = "pd-standard",
  master_disk_size = 30,
  worker_disk_type = "pd-standard",
  worker_disk_size = 30,
  init_actions_uris = ["gs://goog-dataproc-initialization-actions-us-central1/python/pip-install.sh"],
  metadata = {'PIP_PACKAGES': 'google-cloud-secret-manager'},
  service_account_scopes = ["https://www.googleapis.com/auth/cloud-platform"]
).make()

with DAG (
  dag_id = "load_data",
  default_args = args,
  schedule_interval = "0 5 * * *",
) as dag:
  dag_start = DummyOperator(
    task_id = "start_workload"
  )

  dag_end = DummyOperator(
    task_id = "end_workload"
  )

  ddl_user_purchase_task = CloudSQLExecuteQueryOperator(
    gcp_cloudsql_conn_id = 'public_postgres_tcp',
    sql = SQL,
    task_id = "create_user_purchase_table"
  )

  sql_import_task = CloudSqlInstanceImportOperator(
    body = import_body,
    instance = GCSQL_POSTGRES_INSTANCE_NAME_QUERY,
    task_id = 'gcs_to_cloudsql'
  )

  create_cluster = DataprocCreateClusterOperator(
    task_id = "create_dataproc_cluster",
    project_id = GCP_PROJECT_ID,
    cluster_config = CLUSTER_CONFIG,
    region = GCP_REGION,
    cluster_name = CLUSTER_NAME
  )

  pyspark_task = DataprocSubmitJobOperator(
    task_id = "submit_pyspark_job",
    job = PYSPARK_JOB,
    location = GCP_REGION,
    project_id = GCP_PROJECT_ID
  )

  delete_cluster = DataprocDeleteClusterOperator(
    task_id = "delete_dataproc_cluster",
    project_id = GCP_PROJECT_ID,
    cluster_name = CLUSTER_NAME,
    region = GCP_REGION
  )

  (dag_start >> 
    [ddl_user_purchase_task, create_cluster] >> sql_import_task >>
    pyspark_task >> delete_cluster >> 
  dag_end)
