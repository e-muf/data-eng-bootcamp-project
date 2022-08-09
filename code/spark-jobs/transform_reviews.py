# %%
import os
from google.cloud import secretmanager
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, array_contains, expr, current_timestamp
from pyspark.ml.feature import Tokenizer, StopWordsRemover

# %%
spark = SparkSession.builder \
  .appName('transform_data') \
  .getOrCreate()

client = secretmanager.SecretManagerServiceClient()
# %%
GCP_PROJECT_BUCKET = 'gs://data-bootcamp-project-357503-bucket'

def get_secret_data(secret_id, version_id, project_id = "data-bootcamp-project-357503", client = client):
  secret_detail = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
  response = client.access_secret_version(request={"name": secret_detail})
  return response

movie_review_df = spark.read.csv(
  f"{GCP_PROJECT_BUCKET}/data/movie_review.csv",
  header = True,
  inferSchema = True
)

log_reviews_df = spark.read.csv(
  f"{GCP_PROJECT_BUCKET}/data/log_reviews.csv",
  header = True,
  inferSchema = True
)

jdbc_url = get_secret_data("postgres_conn_id", 1).payload.data.decode("UTF-8")
pg_user = get_secret_data("pg_user", 1).payload.data.decode("UTF-8")
pg_password = get_secret_data("pg_password", 1).payload.data.decode("UTF-8")

# %%
tokenizer = Tokenizer(inputCol="review_str", outputCol="review_tokenized")
remover = StopWordsRemover(inputCol="review_tokenized", outputCol="review_words", locale="en_US")

# %%
reviews_tokenized_df = tokenizer.transform(movie_review_df)
reviews = remover.transform(reviews_tokenized_df)
reviews = reviews.withColumn("positive_review", 
                             expr('''array_contains(review_words, "good") OR 
                             array_contains(review_words, "funny")'''))
reviews = reviews.withColumn("inserted_date", current_timestamp())
reviews = reviews.select(col("id_review").alias("review_id"),
                         col("cid").alias("customer_id"),
                         col("inserted_date"),
                         when(col("positive_review") == True, 1)
                         .otherwise(0).alias("positive_review"))
# %%
logs_df = log_reviews_df.selectExpr(
    "array(id_review) review_id",
    "xpath(log, './reviewlog/log/logDate/text()') logDate",
    "xpath(log, './reviewlog/log/device/text()') device",
    "xpath(log, './reviewlog/log/location/text()') location",
    "xpath(log, './reviewlog/log/os/text()') os",
    "xpath(log, './reviewlog/log/ipAddress/text()') ip",
    "xpath(log, './reviewlog/log/phoneNumber/text()') phone_number",
).selectExpr(
    "explode(arrays_zip(review_id, logDate, device, location, os, ip, phone_number)) logs"
).select(to_date(col('logs.logDate'), 'MM-dd-yyyy').alias("log_date"), "logs.*").drop("logDate")

logs_df = logs_df.withColumn("browser",
  when((col("device") == "Google Android") | (col("device") == "Microsoft Windows"), "Google Chrome")
  .when((col("device") == "Linux"), "Mozilla Firefox")
  .when((col("device") == "Apple MacOS") | (col("device") == "Apple iOS"), "Safari")
  .otherwise("Opera")
)

user_purchase_df = spark.read.jdbc(
    jdbc_url, "movies_schema.user_purchase", properties={
        "user": pg_user,
        "password": pg_password,
        "driver": "org.postgresql.Driver"
    }
)

reviews.write.save(f'{GCP_PROJECT_BUCKET}/job-results/reviews/', mode='overwrite')
logs_df.write.save(f'{GCP_PROJECT_BUCKET}/job-results/logs/', mode='overwrite')
user_purchase_df.write.save(f"{GCP_PROJECT_BUCKET}/job-results/user_purchase", mode="overwrite")
