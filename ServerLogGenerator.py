from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import socket, uuid, json, random, time, os, pyspark.pandas as ps
from ServerLogEnum import ServerLogEnum

PATH = os.getcwd()+ '/DataFiles/geoip2-ipv4_csv.csv'

KAFKA_HOST = '0.0.0.0:29092'
conf = {'bootstrap.servers': KAFKA_HOST,
        'client.id': socket.gethostname(),
        'group.id': "FraudLoginDetection",
        'auto.offset.reset': 'smallest'}

# random = ''
country = ["USA", "IN", "UK", "CA", "AU", "DE", "ES", "FR", "NL", "SG", "RU", "JP", "BR", "CN", "O"]
eventType = ["click", "purchase", "login", "log-out", "delete-account", "create-account", "update-settings", "other"]

def getServerLog():
    serverObj = ServerLogEnum(str(uuid.uuid4()),
                              str(time.time()),
                              str(country[random.randint(0, len(country)-1)]),
                              str(eventType[random.randint(0, len(eventType)-1)]),
                              str(random.randint(10000,99999)))

    print(serverObj.returnCommaSeparated)
    return serverObj.returnCommaSeparated()



TOPIC_SERVER_LOGS = 'server_logs'

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print()
        # print("Message produced: %s" % (str(msg)))

def start_producing():
    producer = Producer(conf)
    df = ps.read_csv(PATH)
    df.dropna()

    # generate dtaftame for csv push
    for i in range(5):

        data = getServerLog()
        # print(data)
        # Data Generation Logic
        # print(getServerLog())
        # type(data)

        # push to df
        print(data)
        producer.produce(TOPIC_SERVER_LOGS, json.dumps(data).encode('utf-8'), callback=acked)
        producer.flush()

        # print("\033[1;31;40m -- PRODUCER: Sent message with id {}".format(data))
        producer.poll(1)

    # Push to CSV



if __name__ == '__main__':
    start_producing()

