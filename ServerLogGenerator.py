from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import socket
import uuid
import json

KAFKA_HOST = '0.0.0.0:29092'
conf = {'bootstrap.servers': KAFKA_HOST,
        'client.id': socket.gethostname(),
        'group.id': "foo",
        'auto.offset.reset': 'smallest'}

TOPIC_SERVER_LOGS = 'server_logs'
TOPIC_ALERTS = 'alerts'

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

def start_producing():
    producer = Producer(conf)
    for i in range(1000):
        message_id = str(uuid.uuid4())

        producer.produce(TOPIC_SERVER_LOGS, json.dumps("Hello").encode('utf-8'), callback=acked)
        producer.flush()

        print("\033[1;31;40m -- PRODUCER: Sent message with id {}".format(message_id))
        producer.poll(1)

if __name__ == '__main__':
    start_producing()