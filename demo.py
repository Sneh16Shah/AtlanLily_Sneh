import random
import time
import logging
from confluent_kafka import Producer

# Configure logging to write to the file immediately without buffering
logging.basicConfig(filename='producer_logs.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'metadata-producer',
    'acks': 1,
    'linger.ms': 0,
}

producer = Producer(kafka_config)

topic = 'metadata-changes'


def generate_random_metadata_event(id):
    entity_id = str(id)
    event_type = random.choice(['update', 'create', 'delete'])
    metadata_info = f'Random metadata event for entity {entity_id}'
    return {
        'entity_id': entity_id,
        'event_type': event_type,
        'metadata_info': metadata_info,
    }


for i in range(100000):
    metadata_event = generate_random_metadata_event(i)
    producer.produce(topic, key='metadata_key', value=str(metadata_event))
    producer.flush()

    # Open the file each time to write immediately without buffering
    with open('producer_logs.log', 'a') as log_file:
        log_file.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - INFO - Generated entry {i} - Metadata Event: {metadata_event}\n")
    
    print("Generated entry", i)
    time.sleep(1)

# Flushing any remaining messages
producer.flush()

logging.info("Produced 50 random metadata events.")
print("Produced 50 random metadata events.")
