from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import socket, uuid, json, random, time, os, math, pyspark.pandas as ps
from ServerLog import ServerLog

DATA_PATH = os.path.dirname(os.getcwd())+ '/DataFiles'
KAFKA_HOST = 'localhost:9092'
conf = {'bootstrap.servers': KAFKA_HOST,
        'client.id': socket.gethostname(),
        'group.id': "FraudLoginDetection",
        'auto.offset.reset': 'smallest'}

User_Login_History = ps.read_csv(DATA_PATH + '/User_login_history.csv')
IPv4_Location_Mapping = ps.read_csv(DATA_PATH + '/IPv4_Location_Mapping.csv')

TOPIC_SERVER_LOGS = 'server_logs'

DEVICE_TYPES = ['Android', 'IOS', 'Windows', 'Mac OS', ]
DATA_LIST = ["GOOD", "BAD"]

NUMBER_OF_LOGS = 30

def getServerLog(choice):

    random_num = random.randint(0, len(User_Login_History)-1)
    user_data_df = User_Login_History.iloc[random_num]
    user_id = str(user_data_df['UserID'])
    curr_uuid = str(uuid.uuid4())
    current_time = str(math.ceil(time.time()))
    if choice == 'GOOD':
        location = user_data_df['Location']
        ip_good_df = IPv4_Location_Mapping[IPv4_Location_Mapping['city'] == location]['IP']
        ip = str(ip_good_df.iloc[random.randint(0, len(ip_good_df) - 1)])
        device_choice = str(random.choice(user_data_df['DeviceType'].split(',')))
    else:
        ip = IPv4_Location_Mapping['IP'].iloc[random.randint(0, len(IPv4_Location_Mapping) - 1)]
        device_choice = str(random.choice(DEVICE_TYPES))

    generate_log = ServerLog(curr_uuid,
                             current_time,
                             ip,
                             device_choice,
                             user_id)
    return generate_log.toJSON()

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

def produce():
    producer = Producer(conf)
    for i in range(NUMBER_OF_LOGS):
        choice = random.choices(DATA_LIST, weights=(80, 20))
        data = getServerLog(choice[0])
        if(i == NUMBER_OF_LOGS-1):
            data['isLast'] = 'True'
        else:
            data['isLast'] = 'False'
        producer.produce(TOPIC_SERVER_LOGS, json.dumps(data).encode('utf-8'), callback=acked)
        producer.flush()
        print("\033[1;31;40m -- PRODUCER: Sent message with id {}".format(data))
        producer.poll(0.2)
        # print("CURRENT PRODUCED COUNT", i)
    print("FINAL PRODUCED COUNT", i)

if __name__ == '__main__':
    produce()