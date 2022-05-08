from pyspark.sql import SparkSession
from pyspark.ml.feature import Normalizer, StandardScaler
import sys, socket, json, time, pyspark.pandas as ps
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
# from pyspark.sql import functions as F
import time

KAFKA_HOST = 'localhost:29092'
TOPIC_SERVER_LOGS = 'server_logs'
TOPIC_ALERTS = 'alerts'

conf = {'bootstrap.servers': KAFKA_HOST,
        'client.id': socket.gethostname(),
        'group.id': "FraudLoginDetection",
        'auto.offset.reset': 'smallest'}

spark = (SparkSession
         .builder
         .master('local')
         .appName('FraudLoginDetection')
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1")
         .getOrCreate())

df = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", KAFKA_HOST)\
    .option("subscribe", TOPIC_SERVER_LOGS)\
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 1) \
    .option("group.id", "2") \
    .option("auto.offset.reset", "earliest") \
    .load()

PANDA = ps.DataFrame({'USER_ID': ['91669', '35004', '83542', '95642'],
        'LOCATION': ['INDIA', 'USA', 'USA', 'INDIA'],
        'DEVICE_TYPE': ['ANDROID', 'ANDROID', 'ANDROID', 'ANDROID'],
        'TIMESTAMP': ['1651970897','1651897851', '1651897852', '1651897853'],
        })

geoLocationMap = ps.DataFrame({'IP': ['1', '2', '3', '4'],
        'LOCATION': ['USA', 'USA', 'USA', 'INDIA']
        })

producer = Producer(conf)

def updateTimeStamp(user_details,current_logged_time):
    global PANDA
    PANDA = PANDA[PANDA['USER_ID'] != user_details['USER_ID'].to_list()[0] ]
    user_details = user_details.iloc[0,:].to_list()
    user_details[-1]  = str(current_logged_time)
    # user_details = ps.Series(user_details)
    user_details = ps.DataFrame({'USER_ID': [user_details[0]],
                  'LOCATION': [user_details[1]],
                  'DEVICE_TYPE': [user_details[2]],
                  'TIMESTAMP': [user_details[3]],
                  })
    PANDA = PANDA.append(user_details,ignore_index= True)

def getLocationFromIP(ip_address):
    locationIp = geoLocationMap[geoLocationMap['IP'] == ip_address]
    location = str(locationIp['LOCATION'].values[0])
    print("Current Location ",location)
    return location

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

def processData(request):
    print(request)
    list = request['value'].split(",")
    print(list)
    UUID = list[0]
    TIME = list[1]
    IPv4 = list[2]
    DEVICE_TYPE = list[3]
    USER_ID = list[4]

    user_login = PANDA[PANDA['USER_ID'].str.contains(USER_ID)]
    if len(user_login) > 0:
        last_logged_time = int(user_login['TIMESTAMP'].values[0])
        current_logged_time = int(TIME)
        if current_logged_time - last_logged_time < 10 * 60:
            last_location = user_login['LOCATION'].values[0]
            current_logged_location = getLocationFromIP(IPv4)
            if last_location == current_logged_location:
                last_Logged_device = user_login['DEVICE_TYPE'].values[0]
                current_logged_device = DEVICE_TYPE
                if last_Logged_device == current_logged_device:
                    updateTimeStamp(user_login, current_logged_time)
                    print("Valid Login within timestamp -> ", request)
                else:
                    produceToAlerts(request)
            else:
                produceToAlerts(request)
        else:
            updateTimeStamp(user_login, current_logged_time)
            print("Valid Login after timestamp-> ", request)
        return '1'

def convert_string_to_json(row) :
    request = json.loads(row)
    data = request['data']
    data['request_id'] = request['request_id']
    return data

def create_df(batch_df) :
    value_series = batch_df['value'].to_list()
    value_list = []
    for value in value_series :
        value_list.append({"value" : json.loads(value)})
    return ps.DataFrame(value_list)


def produceToAlerts(message):
    producer.produce(TOPIC_ALERTS, json.dumps(message).encode('utf-8'), callback=acked)
    producer.flush()
    print("\033[1;31;40m -- PRODUCER: Sent message with id {}".format(message))
    producer.poll(1)



def readData(batch_df, batch_id):
    batch = batch_df.to_pandas_on_spark()
    if batch['value'].size > 0:
        input_df = create_df(batch)
        input_df.apply(processData,axis =1)

posts_stream = df.writeStream.foreachBatch(readData).start().awaitTermination()
