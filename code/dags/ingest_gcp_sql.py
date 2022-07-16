import os
from datetime import timedelta
from queue import Empty
from urllib.parse import quote_plus

from numpy import empty

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.gcp_sql_operator import CloudSQLExecuteQueryOperator, CloudSqlInstanceImportOperator
from airflow.utils.dates import days_ago

args = {
  "owner": "emanuel-dev",
  "retries": 1,
  "retray_delay": timedelta(minutes=5),
  'start_date': days_ago(1)
}

GCP_PROJECT_ID =  os.getenv('GCP_PROJECT')
GCS_SOURCE_DATA_BUCKET = os.getenv('GCS_SOURCE_DATA_BUCKET')
GCSQL_POSTGRES_DATABASE_NAME = os.getenv('GCSQL_POSTGRES_DATABASE_NAME')
GCSQL_POSTGRES_INSTANCE_NAME_QUERY = os.getenv('GCSQL_POSTGRES_INSTANCE_NAME_QUERY')

user_purchase_source_objet = "data/user_purchase.csv"
schema_table_name = "movies_schema.user_purchase"

SQL = f"""
CREATE SCHEMA movies_schema;
CREATE TABLE {schema_table_name} (
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
    "uri": f'gs://{GCS_SOURCE_DATA_BUCKET}/{user_purchase_source_objet}',
    "csvImportOptions": {
      "table": schema_table_name
    }
  }
}

with DAG (
  dag_id = "load_data",
  default_args = args,
  schedule_interval = "0 5 * * *",
) as dag:
  dag_start = DummyOperator(
    task_id = "dag_start"
  )

  dag_end = DummyOperator(
    task_id = "dag_end"
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

  dag_start >> ddl_user_purchase_task >> sql_import_task >> dag_end
