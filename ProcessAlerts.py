import sys
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

KAFKA_HOST = '0.0.0.0:29092'
conf = {'bootstrap.servers': KAFKA_HOST,
        'group.id': "foo",
        'auto.offset.reset': 'smallest'}

TOPIC_ALERTS = 'alerts'

consumer = Consumer(conf)

running = True

def msg_process(msg):
    message = msg.value()
    # message = json.loads(message)
    # prediction = predict(trained_model, message)[0]
    # if(prediction == 1):
    #     messageToSend = {'request_id': message['request_id'], 'fraud': 'True'}
    # else:
    #     messageToSend = {'request_id': message['request_id'], 'fraud': 'False'}

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
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

if __name__ == '__main__':
        consume_loop(consumer, [TOPIC_ALERTS])