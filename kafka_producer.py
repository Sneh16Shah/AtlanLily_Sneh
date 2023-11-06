import random
import time
from confluent_kafka import Producer


kafka_config = {
    'bootstrap.servers': 'localhost:9092',  
    'client.id': 'metadata-producer',
    'acks': 1,  
    'linger.ms': 0,  
}

producer = Producer(kafka_config)

topic = 'metadata-changes'

def generate_random_metadata_event(id):
    entity_id = str(id)#str(random.randint(1, 1000))
    event_type = random.choice(['update', 'create', 'delete'])
    metadata_info = f'Random metadata event for entity {entity_id}'
    return {
        'entity_id': entity_id,
        'event_type': event_type,
        'metadata_info': metadata_info,
    }

for _ in range(100000):
    metadata_event = generate_random_metadata_event(_)
    producer.produce(topic, key='metadata_key', value=str(metadata_event))
    producer.flush()
    print("Generated entry {}".format(_))
    time.sleep(1) 

print("Produced 50 random metadata events.")
