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

conn_settings = Variable.get("connection_settings", deserialize_json=True)
GCP_PROJECT_ID =  os.getenv('GCP_PROJECT')
GCSQL_POSTGRES_PUBLIC_PORT = 5432
GCS_SOURCE_DATA_BUCKET = conn_settings['GCS_PROJECT_BUCKET']
GCSQL_POSTGRES_USER = conn_settings['GCSQL_POSTGRES_USER']
GCSQL_POSTGRES_PASSWORD = conn_settings['GCSQL_POSTGRES_PASSWORD']
GCSQL_POSTGRES_PUBLIC_IP = conn_settings['GCSQL_POSTGRES_PUBLIC_IP']
GCP_REGION = conn_settings['GCP_REGION']
GCSQL_POSTGRES_INSTANCE_NAME_QUERY = conn_settings['GCSQL_POSTGRES_INSTANCE_NAME_QUERY']
GCSQL_POSTGRES_DATABASE_NAME = conn_settings['GCSQL_POSTGRES_DATABASE_NAME']

postgres_kwargs = dict(
  user = quote_plus(GCSQL_POSTGRES_USER),
  password = quote_plus(GCSQL_POSTGRES_PASSWORD),
  public_port = GCSQL_POSTGRES_PUBLIC_PORT,
  public_ip = quote_plus(GCSQL_POSTGRES_PUBLIC_IP),
  project_id = quote_plus(GCP_PROJECT_ID),
  location = quote_plus(GCP_REGION),
  instance = quote_plus(GCSQL_POSTGRES_INSTANCE_NAME_QUERY),
  database = quote_plus(GCSQL_POSTGRES_DATABASE_NAME),
)

os.environ['AIRFLOW_CONN_PUBLIC_POSTGRES_TCP'] = (
  "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?"
  "database_type=postgres&"
  "project_id={project_id}&"
  "location={location}&"
  "instance={instance}&"
  "use_proxy=False&"
  "use_ssl=False".format(**postgres_kwargs)
)

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
