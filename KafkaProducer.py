import logging
from confluent_kafka import Producer
import socket
import random
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration for the Kafka Producer
conf = {
    'bootstrap.servers': 'localhost:9092',  
    'client.id': socket.gethostname()
}

# Here I am creating a Kafka Producer instance with the above configuration
producer = Producer(conf)

# prepairing set of data to Produce
first_names = [
    'John', 'Jane', 'Alice', 'Bob', 'Charlie', 
    'Diana', 'Edward', 'Fiona', 'George', 'Hannah'
]

last_names = [
    'Smith', 'Johnson', 'Williams', 'Brown', 
    'Jones', 'Garcia', 'Miller', 'Davis', 
    'Rodriguez', 'Martinez'
]

# Herr i am generating list of 50 random records with first and last names
records = [
    {'first_name': random.choice(first_names), 
     'last_name': random.choice(last_names)} 
    for _ in range(50)
]

def produce_msg(records):
    """
    Produces messages to a Kafka topic.

    This function takes a list of records, converts each record to JSON, 
    and sends it to the Kafka topic 'my_records'.

    """
    try:
        # Iterate over each record and produce it to the Kafka topic
        for record in records:
            producer.produce(
                topic="person_info",     
                key='Arun@1997',             
                value=json.dumps(record).encode('utf-8')
            )
    except Exception as e:
        logger.error("Error occurred while producing message: %s", e)

    finally:
        producer.flush()

# Call the produce_msg function to send the records to Kafka
produce_msg(records)
