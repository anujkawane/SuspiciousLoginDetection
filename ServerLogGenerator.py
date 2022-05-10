from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import socket, uuid, json, random, time, os, pyspark.pandas as ps
from ServerLogEnum import ServerLogEnum
import math

PATH = os.getcwd()+ '/DataFiles'

KAFKA_HOST = 'localhost:29092'
conf = {'bootstrap.servers': KAFKA_HOST,
        'client.id': socket.gethostname(),
        'group.id': "FraudLoginDetection",
        'auto.offset.reset': 'smallest'}

TOPIC_SERVER_LOGS = 'server_logs'

deviceTypes = ['Android', 'IOS', 'Windows', 'Mac OS']
User_Login_History = ps.read_csv(PATH + '/User_login_history.csv')
IPv4_Location_Mapping = ps.read_csv(PATH + '/IPv4_Location_Mapping.csv')

def good_data():
    randomNum = random.randint(0, len(User_Login_History))
    mainRandom = User_Login_History.iloc[randomNum]
    userId = str(mainRandom['UserID'])
    curr_uuid = str(uuid.uuid4())
    location = mainRandom['Location']
    ip_good_df = IPv4_Location_Mapping[IPv4_Location_Mapping['city'] == location]['IP']
    len_ip_good = len(ip_good_df)
    ip_good = str(ip_good_df.iloc[random.randint(0, len_ip_good - 1)])
    device = str(random.choice(mainRandom['DeviceType'].split(',')))
    curr_time = str(math.ceil(time.time()))

    goodLog = ServerLogEnum(curr_uuid,
                  curr_time,
                  ip_good,
                  device,
                  userId)
    return goodLog

def bad_data():
    randomNum = random.randint(0, len(User_Login_History))
    mainRandom = User_Login_History.iloc[randomNum]
    userId = str(mainRandom['UserID'])
    curr_uuid = str(uuid.uuid4())
    len_ip_bad = len(IPv4_Location_Mapping)
    ip_bad = IPv4_Location_Mapping['IP'].iloc[random.randint(0, len_ip_bad - 1)]
    device = str(random.choice(deviceTypes))
    curr_time = str(math.ceil(time.time()))

    badLog = ServerLogEnum(curr_uuid,
                            curr_time,
                            ip_bad,
                            device,
                            userId)
    return badLog

def getServerLog(choice):
    log = None
    if("GOOD" in choice):
        log = good_data()
    else:
        log = bad_data()
    return log.returnCommaSeparated()

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

def produce():
    producer = Producer(conf)
    datalist = ["GOOD", "BAD"]
    for i in range(100):
        choice = random.choices(datalist, weights=(80, 20))
        data = None
        if "GOOD" in choice:
            data = getServerLog("GOOD")
        else:
            data = getServerLog("BAD")
        producer.produce(TOPIC_SERVER_LOGS, json.dumps(data).encode('utf-8'), callback=acked)
        producer.flush()

        print("\033[1;31;40m -- PRODUCER: Sent message with id {}".format(data))
        producer.poll(1)

if __name__ == '__main__':
    produce()