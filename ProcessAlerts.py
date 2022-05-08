import json
import socket
import joblib
import sys
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
import json
import pandas as pd

KAFKA_HOST = 'localhost:29092'
TOPIC_ALERTS = 'alerts'
conf = {'bootstrap.servers': KAFKA_HOST,
        'group.id': "foo",
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)
running = True

def msg_process(msg):
    message = msg.value()
    message = json.loads(message)
    print(message)

def consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)
        msg_count = 0
        while running:
            msg = consumer.poll(timeout=1.0)
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
                msg_count += 1
                if msg_count % MIN_COMMIT_COUNT == 0:
                    consumer.commit(asynchronous=True)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

if __name__ == '__main__':
        consume_loop(consumer, [TOPIC_ALERTS])