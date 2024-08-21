import json
import logging
from confluent_kafka import Consumer, KafkaError


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration for the Kafka Consumer
conf = {
    'bootstrap.servers': 'localhost:9092',  
    'group.id': 'my_group',                 
    'auto.offset.reset': 'smallest'      
}

# Here I am creating a Kafka Consumer instance with the above configuration
consumer = Consumer(conf)

running = True

def msg_process(msg):
    """
    Processes a Kafka message.

    This function decodes the message value from UTF-8 and converts it from JSON format 
    to a Python dictionary. It then logs the received record.

    """
    record = json.loads(msg.value().decode('utf-8'))
    logger.info(f"Received record: {record}")


def consume_msg(consumer, topics=["person_info"]):
    """
    Consumes messages from specified Kafka topics.

    This function subscribes the consumer to the provided list of topics and continuously 
    polls for messages.

    """
    global running
    try:
        # Subscribe to the specified topics
        consumer.subscribe(topics)

        while running:
            # Poll for messages with a timeout of 1 second
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                # Handle specific Kafka errors
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.info("End of partition reached")
                else:
                    logger.error("Error occurred: %s", msg.error())
            else:
                # Process the message if no errors occurred
                msg_process(msg)
    finally:
        consumer.close()

# Start consuming messages
consume_msg(consumer)