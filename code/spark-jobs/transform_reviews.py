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

# %%
GCP_PROJECT_BUCKET = 'gs://dataeng-proj-bucket'

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
                         col("cid").alias("user_id"),
                         col("inserted_date"),
                         when(col("positive_review") == True, 1)
                         .otherwise(0).alias("positive_review"))
# %%
logs_df = log_reviews_df.selectExpr(
    "xpath(log, './reviewlog/log/logDate/text()') logDate",
    "xpath(log, './reviewlog/log/device/text()') device",
    "xpath(log, './reviewlog/log/location/text()') location",
    "xpath(log, './reviewlog/log/os/text()') os",
    "xpath(log, './reviewlog/log/ipAddress/text()') ip",
    "xpath(log, './reviewlog/log/phoneNumber/text()') phone_number",
).selectExpr(
    "explode(arrays_zip(logDate, device, location, os, ip, phone_number)) logs"
).select(to_date(col('logs.logDate'), 'MM-dd-yyyy').alias("log_date"), "logs.*").drop("logDate")
logs_df.show(10)

reviews.write.save(f'{GCP_PROJECT_BUCKET}/job-result/reviews/', mode='overwrite')
logs_df.write.save(f'{GCP_PROJECT_BUCKET}/job-result/logs/', mode='overwrite')
