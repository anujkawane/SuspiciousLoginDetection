from pyspark.sql import SparkSession
from pyspark.ml.feature import Normalizer, StandardScaler
import sys, socket, json, os, time, pyspark.pandas as ps
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
import time

KAFKA_HOST = 'localhost:29092'
TOPIC_SERVER_LOGS = 'server_logs'
TOPIC_ALERTS = 'alerts'
PATH = os.getcwd()+ '/DataFiles'
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

User_Login_History = ps.read_csv(PATH + '/User_login_history.csv')
IPv4_Location_Mapping = ps.read_csv(PATH + '/IPv4_Location_Mapping.csv')

df = spark.readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", KAFKA_HOST)\
    .option("subscribe", TOPIC_SERVER_LOGS)\
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 1) \
    .option("group.id", "2") \
    .option("auto.offset.reset", "earliest") \
    .load()

producer = Producer(conf)

def updateTimeStamp(user_details,current_logged_time):
    global User_Login_History
    User_Login_History = User_Login_History[User_Login_History['UserID'] != user_details['UserID'].to_list()[0] ]
    user_details = user_details.iloc[0,:].to_list()
    user_details[-1]  = str(current_logged_time)
    user_details = ps.DataFrame({'UserID': [user_details[0]],
                  'Location': [user_details[1]],
                  'DeviceType': [user_details[2]],
                  'Time': [user_details[3]],
                  })
    User_Login_History = User_Login_History.append(user_details,ignore_index= True)

def getLocationFromIP(ip_address):
    locationIp = IPv4_Location_Mapping[IPv4_Location_Mapping['IP'] == ip_address]
    Country = str(locationIp['Country'].values[0])
    State = str(locationIp['State'].values[0])
    City = str(locationIp['city'].values[0])
    return City, State, Country

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

def processData(request):
    list = request['value'].split(",")
    RequestID = list[0]
    TIME = list[1]
    IPv4 = list[2]
    DEVICE_TYPE = list[3]
    USER_ID = list[4]

    user_login = User_Login_History[User_Login_History['UserID'] == USER_ID]

    if len(user_login) > 0:
        last_logged_time = int(user_login['Time'].values[0])
        current_logged_time = int(TIME)
        if current_logged_time - last_logged_time < 40000 * 60:
            last_location = user_login['Location'].values[0]
            current_logged_location= getLocationFromIP(IPv4)
            if last_location in current_logged_location:
                last_Logged_device = user_login['DeviceType'].values[0].split(",")
                current_logged_device = DEVICE_TYPE
                if current_logged_device in last_Logged_device:
                    print("Valid Login -> ", request.values[0])
                    updateTimeStamp(user_login, current_logged_time)
                else:
                    produceToAlerts(request.values[0]+",DeviceAlert"+","+' '.join(current_logged_location))
            else:
                produceToAlerts(request.values[0]+",SignInAlert"+","+' '.join(current_logged_location))
        else:
            last_location = user_login['Location'].values[0]
            current_logged_location = getLocationFromIP(IPv4)
            if last_location not in current_logged_location:
                last_Logged_device = user_login['DeviceType'].values[0].split(",")
                current_logged_device = DEVICE_TYPE
                if current_logged_device in last_Logged_device:
                    print("Valid Login -> ", request.values[0])
                    updateTimeStamp(user_login, current_logged_time)
                else:
                    produceToAlerts(request.values[0] + ",SignInAlert" + "," + ' '.join(current_logged_location))
            else:
                print("Valid Login -> ", request.values[0])
                updateTimeStamp(user_login, current_logged_time)

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
