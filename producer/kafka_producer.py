from kafka import KafkaProducer
import sys, os

# producer obj
def get_kafka_producer(server):
    producer = KafkaProducer(bootstrap_servers=server, \
                            value_serializer=lambda x: x.encode('utf-8'), \
                            key_serializer=lambda x: x.encode('utf-8'))
    return producer

# producing async
def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

def _produce_msg(producer: KafkaProducer, topic, key, msg):
    try:
        producer.send(topic=topic, value=msg, key=key)
        print (msg + "is produced")
        # Wait up to 1 second for events. Callbacks will be invoked during
        # this method call if the message is acknowledged.
        # producer.poll(1)
    except:
        print("Unexpected error:", sys.exc_info()[0])
        raise

