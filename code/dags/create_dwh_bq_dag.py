import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.sensors.gcs_sensor import GoogleCloudStorageObjectSensor
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.utils.dates import days_ago

args = {
  "owner": "emanuel-dev",
  "retries": 1,
  "retray_delay": timedelta(minutes=5),
  'start_date': days_ago(1)
}

# Environment variables
GCP_PROJECT_ID =  os.getenv('GCP_PROJECT')
GCS_PROJECT_BUCKET = os.getenv('GCS_SOURCE_DATA_BUCKET')
BQ_RAW_DATASET = os.getenv('BQ_RAW_DATASET')
BQ_DWH_DATASET = os.getenv('BQ_DWH_DATASET')

# DAG variables
parent_dag = "load_transform_dag"

# Macros
execution_date_nodash = "{{ ds_nodash }}"

# Movie review
bq_moview_review_table_name = "classified_movie_review"
bq_moview_review_table_id = f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{bq_moview_review_table_name}"

# Review logs
bq_review_log_table_name = "review_logs"
bq_review_log_table_id = f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{bq_review_log_table_name}"

# User purchase
bq_user_purchase_table_name = "user_purchase"
bq_user_purchase_table_id = f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{bq_user_purchase_table_name}"

# US regions
bq_us_regions_table_name = "us_regions"
bq_us_regions_table_id = f"{GCP_PROJECT_ID}.{BQ_RAW_DATASET}.{bq_us_regions_table_name}"

#DWH
bq_dim_devices_table_name = "dim_devices"
bq_dim_devices_table_id = f"{GCP_PROJECT_ID}.{BQ_DWH_DATASET}.{bq_dim_devices_table_name}"

bq_dim_os_table_name = "dim_os"
bq_dim_os_table_id = f"{GCP_PROJECT_ID}.{BQ_DWH_DATASET}.{bq_dim_os_table_name}"

bq_dim_location_table_name = "dim_location"
bq_dim_location_table_id = f"{GCP_PROJECT_ID}.{BQ_DWH_DATASET}.{bq_dim_location_table_name}"

bq_dim_browser_table_name = "dim_browser"
bq_dim_browser_table_id = f"{GCP_PROJECT_ID}.{BQ_DWH_DATASET}.{bq_dim_browser_table_name}"

bq_dim_date_table_name = "dim_date"
bq_dim_date_table_id = f"{GCP_PROJECT_ID}.{BQ_DWH_DATASET}.{bq_dim_date_table_name}"

bq_fact_movie_analytics_table_name = "fact_movie_analytics"
bq_fact_movie_analytics_table_id = f"{GCP_PROJECT_ID}.{BQ_DWH_DATASET}.{bq_fact_movie_analytics_table_name}"

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

  gcs_to_bq_movie_review = GoogleCloudStorageToBigQueryOperator(
    task_id = "gcs_to_bq_movie_review",
    bucket = GCS_PROJECT_BUCKET,
    source_objects = ["job-results/reviews/*.parquet"],
    source_format = "parquet",
    destination_project_dataset_table = bq_moview_review_table_id,
    write_disposition = "WRITE_TRUNCATE"
  )

  gcs_to_bq_review_logs = GoogleCloudStorageToBigQueryOperator(
    task_id = "gcs_to_bq_review_logs",
    bucket = GCS_PROJECT_BUCKET,
    source_objects = ["job-results/logs/*.parquet"],
    source_format = "parquet",
    destination_project_dataset_table = bq_review_log_table_id,
    write_disposition = "WRITE_TRUNCATE"
  )

  gcs_to_bq_user_purchase = GoogleCloudStorageToBigQueryOperator(
    task_id = "gcs_to_bq_user_purchase",
    bucket = GCS_PROJECT_BUCKET,
    source_objects = ["job-results/user_purchase/*.parquet"],
    source_format = "parquet",
    destination_project_dataset_table = bq_user_purchase_table_id,
    write_disposition = "WRITE_TRUNCATE"
  )

  gcs_to_bq_us_regions = GoogleCloudStorageToBigQueryOperator(
    task_id = "gcs_to_bq_us_regions",
    bucket = GCS_PROJECT_BUCKET,
    source_objects = ["data/us_regions.csv"],
    schema_fields = [
      {"name": "state", "type": "STRING", "mode": "NULLABLE"},
      {"name": "state_code", "type": "STRING", "mode": "NULLABLE"},
      {"name": "region", "type": "STRING", "mode": "NULLABLE"},
      {"name": "division", "type": "STRING", "mode": "NULLABLE"}
    ],
    source_format = "csv",
    destination_project_dataset_table = bq_us_regions_table_id,
    write_disposition = "WRITE_TRUNCATE"
  )

  # Load DWH tables
  dwh_dim_devices = BigQueryOperator(
    task_id = "dwh_dim_devices",
    sql = f"""SELECT ROW_NUMBER() OVER() as id_dim_devices, *
              FROM (
                SELECT DISTINCT device
                FROM {bq_review_log_table_id}
              ) td;""",
    destination_dataset_table =  bq_dim_devices_table_id,
    write_disposition = "WRITE_TRUNCATE",
    create_disposition = "CREATE_IF_NEEDED",
    use_legacy_sql = False,
    priority = "BATCH"
  )

  dwh_dim_os = BigQueryOperator(
    task_id = "dwh_dim_os",
    sql = f"""SELECT ROW_NUMBER() OVER() as id_dim_os, *
              FROM (
                SELECT DISTINCT os
                FROM {bq_review_log_table_id}
              ) tos;""",
    destination_dataset_table =  bq_dim_os_table_id,
    write_disposition = "WRITE_TRUNCATE",
    create_disposition = "CREATE_IF_NEEDED",
    use_legacy_sql = False,
    priority = "BATCH"
  )

  dwh_dim_location = BigQueryOperator(
    task_id = "dwh_dim_location",
    sql = f"""SELECT ROW_NUMBER() OVER() as id_dim_location, *
              FROM (
                SELECT DISTINCT location, region
                FROM `raw_movies.review_logs` logs
                LEFT JOIN `raw_movies.us_regions` regions
                ON logs.location = regions.state
              ) tloc;""",
    destination_dataset_table =  bq_dim_location_table_id,
    write_disposition = "WRITE_TRUNCATE",
    create_disposition = "CREATE_IF_NEEDED",
    use_legacy_sql = False,
    priority = "BATCH"
  )

  dwh_dim_browser = BigQueryOperator(
    task_id = "dwh_dim_browser",
    sql = f"""SELECT ROW_NUMBER() OVER() as id_dim_browser, *
              FROM (
                SELECT DISTINCT browser
                FROM {bq_review_log_table_id}
              ) tbrowser;""",
    destination_dataset_table =  bq_dim_browser_table_id,
    write_disposition = "WRITE_TRUNCATE",
    create_disposition = "CREATE_IF_NEEDED",
    use_legacy_sql = False,
    priority = "BATCH"
  )

  dwh_dim_date = BigQueryOperator(
    task_id = "dwh_dim_date",
    sql = f"""SELECT 
                ROW_NUMBER() OVER() as id_dim_date, log_date,
                EXTRACT(DAY FROM log_date) as day,
                EXTRACT(MONTH FROM log_date) as month,
                EXTRACT(YEAR FROM log_date) as year,
                CASE
                  WHEN log_date BETWEEN DATE(2021, 3, 21) AND DATE(2021, 6, 21) THEN "spring"
                  WHEN log_date BETWEEN DATE(2021, 6, 22) AND DATE(2021, 9, 23) THEN "summer"
                  WHEN log_date BETWEEN DATE(2021, 9, 24) AND DATE(2021, 12, 21) THEN "autumn"
                  ELSE "winter"
                END AS season
              FROM (
                SELECT DISTINCT log_date
                FROM {bq_review_log_table_id}
              ) tdate;""",
    destination_dataset_table =  bq_dim_date_table_id,
    write_disposition = "WRITE_TRUNCATE",
    create_disposition = "CREATE_IF_NEEDED",
    use_legacy_sql = False,
    priority = "BATCH"
  )

  dwh_fact_movie_analytics = BigQueryOperator(
    task_id = "dwh_fact_movie_analytics",
    sql = f"""SELECT 
                GENERATE_UUID() AS id_fact_movie_analytics,
                (SELECT id_dim_date 
                FROM {bq_dim_date_table_id} dim_date 
                WHERE logs.log_date = dim_date.log_date) id_dim_date,
                (SELECT id_dim_location
                FROM {bq_dim_location_table_id} dim_location
                WHERE dim_location.location = logs.location) id_dim_location,
                (SELECT id_dim_devices
                FROM {bq_dim_devices_table_id} dim_device
                WHERE dim_device.device = logs.device) id_dim_devices,
                (SELECT id_dim_os
                FROM {bq_dim_os_table_id} dim_os
                WHERE dim_os.os = logs.os) id_dim_os,
                (SELECT id_dim_browser
                FROM {bq_dim_browser_table_id} dim_browser
                WHERE dim_browser.browser = logs.browser) id_dim_browser,
                SUM(reviews.positive_review) review_score,
                COUNT(reviews.review_id) review_count
              FROM {bq_moview_review_table_id} reviews
              JOIN {bq_review_log_table_id} logs
              ON reviews.review_id = logs.review_id
              GROUP BY id_dim_date, id_dim_location, id_dim_devices, id_dim_os, id_dim_browser;""",
    destination_dataset_table =  bq_fact_movie_analytics_table_id,
    write_disposition = "WRITE_TRUNCATE",
    create_disposition = "CREATE_IF_NEEDED",
    use_legacy_sql = False,
    priority = "BATCH"
  )

  input_sensor >> gcs_to_bq_movie_review >> dwh_fact_movie_analytics
  input_sensor >> gcs_to_bq_user_purchase >> dwh_fact_movie_analytics
  input_sensor >> gcs_to_bq_us_regions >> dwh_fact_movie_analytics
  input_sensor >> gcs_to_bq_review_logs >> [dwh_dim_devices, dwh_dim_os, dwh_dim_location, dwh_dim_date, dwh_dim_browser] >> dwh_fact_movie_analytics
  dwh_fact_movie_analytics >> finish_workload

