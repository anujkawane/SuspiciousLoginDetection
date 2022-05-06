import sys
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from pyspark.sql import SparkSession

# Spark session & context
from pyspark.streaming import StreamingContext

spark = (SparkSession
         .builder
         .master('local')
         .appName('FraudLoginDetection')
         # Add kafka package
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
         .getOrCreate())

sc = spark.sparkContext

ssc = StreamingContext(sc, 2)

KAFKA_HOST = '0.0.0.0:29092'
conf = {'bootstrap.servers': KAFKA_HOST,
        'group.id': "foo",
        'auto.offset.reset': 'smallest'}

TOPIC_SERVER_LOGS = 'server_logs'
TOPIC_ALERTS = 'alerts'

consumer = Consumer(conf)
producer = Producer(conf)

running = True
MIN_COMMIT_COUNT = 20

df = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", "0.0.0.0:29092") # kafka server
  .option("subscribe", "server_logs") # topic
  .option("startingOffsets", "earliest") # start from beginning
  .load())
#
# from pyspark.sql.types import StringType
#
# # Convert binary to string key and value
# df1 = (df
#     .withColumn("key", df["key"].cast(StringType()))
#     .withColumn("value", df["value"].cast(StringType())))
#
# print(df1['value'])


def msg_process(msg):
    message = msg.value()
    # message = json.loads(message)
    # prediction = predict(trained_model, message)[0]
    # if(prediction == 1):
    #     messageToSend = {'request_id': message['request_id'], 'fraud': 'True'}
    # else:
    #     messageToSend = {'request_id': message['request_id'], 'fraud': 'False'}

    print(message)

    if "10" in str(message):
        producer.produce(TOPIC_ALERTS,message, callback=acked)
        producer.flush()

def consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)
        msg_count = 0
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
                msg_count += 1
                if msg_count % MIN_COMMIT_COUNT == 0:
                    consumer.commit(asynchronous=True)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

# def predict(model, msg):
#     msg = pd.json_normalize(msg['data'])
#     return model.predict((msg[input_features]))

if __name__ == '__main__':
        consume_loop(consumer, [TOPIC_SERVER_LOGS])