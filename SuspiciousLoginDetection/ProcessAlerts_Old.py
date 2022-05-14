import math

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
import json, time, socket, sys

KAFKA_HOST = 'localhost:29092'
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
def msg_process(msg):
    global count
    global latency
    global started_at
    count += 1
    alert_log = json.loads(msg.value()).split(",")
    if count == 1:
        started_at = int(alert_log[1])
    latency += (math.ceil(time.time()) - int(alert_log[1]))
    if(alert_log[5] == 'True'):
        totalExecutionDuration =  math.ceil(time.time()) - started_at
        print("TOTAL COUNT", count)
        print('Throughput of the pipeline is', count / totalExecutionDuration,' requests/second')
        print('Average Latency', latency / count)

    # if(alert_log[6] != "validLogin"):
    #    print(printMessage(alert_log))


def printMessage(alert_log):
    message = "Hi " + alert_log[4] + ", \n" \
            "We detected a sign-in to your account.\n" \
            "When: " + time.strftime('%A, %B %-d, %Y %H:%M:%S',time.localtime(int(alert_log[1]))) + "\n" \
            "Device: " + alert_log[3] + "\n" \
            "Location: " + alert_log[-1] + "\n" \
            "IP: " + alert_log[2] + "\n" \
            "Alert Type: " + alert_log[6] + "\n" \
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
                msg_process(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

if __name__ == '__main__':
        consume(consumer, [TOPIC_ALERTS])