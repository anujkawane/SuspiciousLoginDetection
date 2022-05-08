from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import socket, uuid, json, random, time, os, pyspark.pandas as ps
from ServerLogEnum import ServerLogEnum

PATH = os.getcwd()+ '/DataFiles/geoip2-ipv4_csv.csv'

KAFKA_HOST = 'localhost:29092'
conf = {'bootstrap.servers': KAFKA_HOST,
        'client.id': socket.gethostname(),
        'group.id': "FraudLoginDetection",
        'auto.offset.reset': 'smallest'}

TOPIC_SERVER_LOGS = 'server_logs'

ipv4 = ["1", "2", "3", "4","5"]
deviceType = ["ANDROID","ANDROID","ANDROID"]
user_id = ['91669', '35004', '83542', '95642']

def getServerLog():
    # serverObj = ServerLogEnum(str(uuid.uuid4()),
    #                           str(int(time.time())),
    #                           str(ipv4[random.randint(0, len(ipv4)-1)]),
    #                           str(deviceType[random.randint(0, len(deviceType)-1)]),
    #                           str(user_id[random.randint(0, len(user_id)-1)]))

    serverObj = ServerLogEnum(str(uuid.uuid4()),
                              str(int(time.time())),
                              str(4),
                              str("ANDROID"),
                              str("91669"))

    print(serverObj.returnCommaSeparated)
    return serverObj.returnCommaSeparated()


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print()
        print("Message produced: %s" % (str(msg)))

def start_producing():
    producer = Producer(conf)
    for i in range(5):
        data = getServerLog()
        print(data)
        producer.produce(TOPIC_SERVER_LOGS, json.dumps(data).encode('utf-8'), callback=acked)
        producer.flush()

        # print("\033[1;31;40m -- PRODUCER: Sent message with id {}".format(data))
        producer.poll(1)

    # Push to CSV

if __name__ == '__main__':
    start_producing()
