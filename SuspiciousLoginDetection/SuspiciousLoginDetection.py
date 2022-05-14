from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType
from pyspark.sql.functions import col, when
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
import sys, socket, json, os, time, math, pyspark.pandas as ps

KAFKA_HOST = 'localhost:9092'
TOPIC_SERVER_LOGS = 'server_logs'
TOPIC_ALERTS = 'alerts'
PATH = os.path.dirname(os.getcwd())+ '/DataFiles'
print(PATH)
AUTO_LOGOUT_TIME_MINUTES = 10
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

UserDetails = ps.read_csv(PATH + '/User_login_history.csv')
IPv4_Location_Mapping = ps.read_csv(PATH + '/IPv4_Location_Mapping.csv')

# Add purpose of this DF
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_HOST) \
    .option("subscribe", TOPIC_SERVER_LOGS) \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 1) \
    .option("group.id", "2") \
    .option("auto.offset.reset", "smallest") \
    .load()

schema = StructType([
  StructField('UserID', StringType(), True),
  StructField('LastLoginTime', StringType(), True)
  ])

UserLoginStatus = spark.createDataFrame([], schema)

producer = Producer(conf)
countConsumed = 0

def updateTimeStamp(userID, current_logged_time):
    global UserLoginStatus
    user_login_data = UserLoginStatus.filter(UserLoginStatus.UserID == userID).collect()
    if len(user_login_data) > 0:
        UserLoginStatus = UserLoginStatus.withColumn("LastLoginTime", when(col("UserID") == userID, current_logged_time).otherwise(col("LastLoginTime")))
    else:
        newRow = UserLoginStatus.collect()
        newRow.append({'UserID': userID, 'LastLoginTime': current_logged_time})
        UserLoginStatus = spark.createDataFrame(newRow, schema)


def deleteRecordsBeforeTime():
    global UserLoginStatus
    value = math.ceil(time.time()) - (AUTO_LOGOUT_TIME_MINUTES * 60)
    UserLoginStatus = UserLoginStatus.where(col("LastLoginTime") > value)


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
    requestData = request.values[0].asDict()
    global UserLoginStatus
    deleteRecordsBeforeTime()
    global countConsumed
    countConsumed = countConsumed + 1


    time = requestData['Timestamp']
    current_logged_time = int(time)
    ipv4 = requestData['currentIP']
    device_type = requestData['DeviceType']
    user_id = requestData['UserID']

    userData = UserDetails[UserDetails['UserID'] == user_id]
    userLastLogin = UserLoginStatus.filter(UserLoginStatus.UserID == user_id).collect()

    if len(userLastLogin) > 0:
        last_location = userData['Location'].values[0]
        current_logged_location = getLocationFromIP(ipv4)
        if last_location in current_logged_location:
            last_Logged_device = userData['DeviceType'].values[0].split(",")
            current_logged_device = device_type
            if current_logged_device in last_Logged_device:
                requestData['Alert'] = 'No'
                produceToAlerts(requestData)
                updateTimeStamp(user_id, current_logged_time)
            else:
                requestData['Alert'] = 'NewDeviceSignIn'
                requestData['currentLocation'] = current_logged_location
                produceToAlerts(requestData)
        else:
            requestData['Alert'] = 'SignInAlert'
            requestData['currentLocation'] = current_logged_location
            produceToAlerts(requestData)
    else:
        last_location = userData['Location'].values[0]
        current_logged_location = getLocationFromIP(ipv4)
        if last_location not in current_logged_location:
            last_Logged_device = userData['DeviceType'].values[0].split(",")
            current_logged_device = device_type
            if current_logged_device in last_Logged_device:
                requestData['Alert'] = 'No'
                produceToAlerts(requestData)
                updateTimeStamp(user_id, current_logged_time)
            else:
                requestData['Alert'] = 'NewDeviceSignIn/SignInAlert'
                requestData['currentLocation'] = current_logged_location
                produceToAlerts(requestData)
        else:
            requestData['Alert'] = 'No'
            produceToAlerts(requestData)
            updateTimeStamp(user_id, current_logged_time)


def convert_string_to_json(row):
    request = json.loads(row)
    data = request['data']
    data['request_id'] = request['request_id']
    return data


def create_df(batch_df):
    value_series = batch_df['value'].to_list()
    value_list = []
    for value in value_series:
        value_list.append({"value": json.loads(value)})
    return ps.DataFrame(value_list)

def produceToAlerts(message):
    producer.produce(TOPIC_ALERTS, json.dumps(message).encode('utf-8'), callback=acked)
    producer.flush()
    print("\033[1;31;40m -- PRODUCER: Sent message with id {}".format(message))

def readData(batch_df, batch_id):
    batch = batch_df.to_pandas_on_spark()
    if batch['value'].size > 0:
        input_df = create_df(batch)
        input_df.apply(processData, axis=1)

posts_stream = df.writeStream.foreachBatch(readData).start().awaitTermination()
