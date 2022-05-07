from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, LongType
from pyspark.ml.feature import Normalizer, StandardScaler
import random
import sys, socket, uuid, json, random, time, os, pyspark.pandas as ps
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from pyspark.sql import functions as F
import time

KAFKA_HOST = '0.0.0.0:29092'
conf = {'bootstrap.servers': KAFKA_HOST,
        'group.id': "FraudLoginDetection",
        'auto.offset.reset': 'smallest'}

TOPIC_SERVER_LOGS = 'server_logs'
TOPIC_ALERTS = 'alerts'
kafka_topic_name = "server_logs"
kafka_bootstrap_servers = '0.0.0.0:29092'

spark = (SparkSession
         .builder
         .master('local')
         .appName('FraudLoginDetection')
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
         .getOrCreate())

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
  .option("subscribe", kafka_topic_name) \
  .load()


df = df.selectExpr("CAST(value AS STRING)", "timestamp")
print(type(df))
# df2 = df.select("key", df.struct(
#         df.value[0].alias("value1"),
#         df.value[1].alias("value2"),
#         df.value[2].alias("value3"),
#         df.value[3].alias("value4"),
#         df.value[4].alias("value5")
#     ).alias("value"))

loginData_schema = StructType() \
        .add("UUID", StringType())\
            .add("Time", LongType())\
                .add("Country", StringType())\
                    .add("EventType", StringType())\
                        .add("AccountID", StringType())

transaction_detail_df2 = df.select(explode(split(df.value, ',')).alias('transaction_detail'))

df = df.select(explode(split(df.value, ',')).alias('transaction_detail'))

posts_stream = df.writeStream.trigger(processingTime='1 seconds')\
        .outputMode('Append')\
            .option("truncate", "false")\
                .format("console")\
                    .start()

posts_stream.awaitTermination()