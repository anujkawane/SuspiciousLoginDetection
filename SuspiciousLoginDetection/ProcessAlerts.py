import math
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
import json, time, socket, sys

KAFKA_HOST = 'localhost:9092'
TOPIC_SERVER_LOGS = 'server_logs'
TOPIC_ALERTS = 'alerts'

conf = {'bootstrap.servers': KAFKA_HOST,
        'client.id': socket.gethostname(),
        'group.id': 'FraudLoginDetection',
        'auto.offset.reset': 'smallest'}   # latest, earliest

consumer = Consumer(conf)
count = 0
latency = 0
started_at = 0

def processMessage(msg):
    processed_data = json.loads(msg.value())

    global count
    global latency
    global started_at
    count += 1

    if count == 1:
        started_at = int(processed_data['Timestamp'])
    latency += (math.ceil(time.time()) - int(processed_data['Timestamp']))
    if(processed_data['isLast'] == 'True'):
        totalExecutionDuration =  math.ceil(time.time()) - started_at
        print('Throughput of the pipeline = ', totalExecutionDuration/count,' requests/sec')
        print('Latency', int(latency / count))

    if(processed_data['Alert'] != "No"):
       print(printMessage(processed_data))


def printMessage(processed_data):
    message = "Hi " + processed_data['UserID'] + ", \n" \
            "We detected a sign-in to your account.\n" \
            "When: " + time.strftime('%A, %B %-d, %Y %H:%M:%S',time.localtime(int(processed_data['Timestamp']))) + "\n" \
            "Device: " + processed_data['DeviceType'] + "\n" \
            "Location: " + ' '.join(processed_data['currentLocation']) + "\n" \
            "IP: " + processed_data['currentIP'] + "\n" \
            "Alert Type: " + processed_data['Alert'] + "\n" \
            "If this was you, you can disregard this message. Otherwise, please let us know."
    print(message + '\n')

def consume(consumer, topics):
    try:
        consumer.subscribe(topics)
        while True:
            msg = consumer.poll()
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
                processMessage(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

if __name__ == '__main__':
        consume(consumer, [TOPIC_ALERTS])