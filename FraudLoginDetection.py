from pyspark.sql import SparkSession
from pyspark.ml.feature import Normalizer, StandardScaler
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
import sys, socket, json, os, time, pyspark.pandas as ps

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
countConsumed = 0

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
    location_ip_df = IPv4_Location_Mapping[IPv4_Location_Mapping['IP'] == ip_address]
    country = str(location_ip_df['Country'].values[0])
    state = str(location_ip_df['State'].values[0])
    city = str(location_ip_df['city'].values[0])
    return city, state, country

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

def processData(request):
    global countConsumed
    countConsumed = countConsumed + 1
    userlog_list = request['value'].split(",")
    # RequestID = userlog_list[0]
    time = userlog_list[1]
    ipv4 = userlog_list[2]
    device_type = userlog_list[3]
    user_id = userlog_list[4]
    user_login_data = User_Login_History[User_Login_History['UserID'] == user_id]

    if len(user_login_data) > 0:
        last_logged_time = int(user_login_data['Time'].values[0])
        current_logged_time = int(time)
        if current_logged_time - last_logged_time < 40000 * 60:
            last_location = user_login_data['Location'].values[0]
            current_logged_location= getLocationFromIP(ipv4)
            if last_location in current_logged_location:
                last_Logged_device = user_login_data['DeviceType'].values[0].split(",")
                current_logged_device = device_type
                if current_logged_device in last_Logged_device:
                    print("Valid Login -> ", request.values[0])
                    # updateTimeStamp(user_login_data, current_logged_time)
                else:
                    produceToAlerts(request.values[0]+",DeviceAlert"+","+' '.join(current_logged_location))
            else:
                produceToAlerts(request.values[0]+",SignInAlert"+","+' '.join(current_logged_location))
        else:
            last_location = user_login_data['Location'].values[0]
            current_logged_location = getLocationFromIP(ipv4)
            if last_location not in current_logged_location:
                last_Logged_device = user_login_data['DeviceType'].values[0].split(",")
                current_logged_device = device_type
                if current_logged_device in last_Logged_device:
                    print("Valid Login -> ", request.values[0])
                    # updateTimeStamp(user_login_data, current_logged_time)
                else:
                    produceToAlerts(request.values[0] + ",SignInAlert" + "," + ' '.join(current_logged_location))
            else:
                print("Valid Login -> ", request.values[0])
                # updateTimeStamp(user_login_data, current_logged_time)
    print("FINAL CONSUMED COUNT", countConsumed)

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

def readData(batch_df, batch_id):
    batch = batch_df.to_pandas_on_spark()
    if batch['value'].size > 0:
        input_df = create_df(batch)
        print('Processed')
        input_df.apply(processData,axis =1)

posts_stream = df.writeStream.foreachBatch(readData).start().awaitTermination()
