import json, time, socket, sys
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer


KAFKA_HOST = 'localhost:29092'
TOPIC_SERVER_LOGS = 'server_logs'
TOPIC_ALERTS = 'alerts'

conf = {'bootstrap.servers': KAFKA_HOST,
        'client.id': socket.gethostname(),
        'group.id': "FraudLoginDetection",
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)

running = True

def msg_process(msg):
    list = json.loads(msg.value()).split(",")
    print(list)
    message = "Hi " + list[4] + ", \n" \
                "We detected a sign-in to your account.\n" \
                "When: " + time.strftime('%A, %B %-d, %Y %H:%M:%S',time.localtime(int(list[1]))) + "\n" \
                "Device: " + list[3] + "\n" \
                "Location: " + list[-1] + "\n" \
                "IP: " + list[2] + "\n" \
                "Alert Type: "+ list[5]+"\n" \
                "If this was you, you can disregard this message. Otherwise, please let us know."
    print(message)
    print("\n")


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
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

if __name__ == '__main__':
        consume_loop(consumer, [TOPIC_ALERTS])