import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.1.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 pyspark-shell'
from ast import literal_eval
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("SSKafka") \
    .getOrCreate()
    
# default for startingOffsets is "latest", but "earliest" allows rewind for missed alerts    
dsraw = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("subscribe", "test") \
  .option("startingOffsets", "earliest") \
  .load()

ds = dsraw.selectExpr("CAST(value AS STRING)")

alertQuery = ds \
        .writeStream \
        .queryName("qalerts")\
        .format("memory")\
        .start()

alerts = spark.sql("select * from qalerts")
print("Some of the records streamed are :", alerts.head(5))
print(alerts.count())