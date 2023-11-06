from confluent_kafka import Consumer, KafkaError
import time

kafka_config = {
    'bootstrap.servers': 'localhost:9092',  
    'group.id': 'metadata-consumer-group',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(kafka_config)

topic = 'metadata-changes'

consumer.subscribe([topic])

events_to_process = 500  

event_count = 0

while event_count < events_to_process:
    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue

        else:
            print(f"Error: {msg.error()}")
            break
    
    print(f"Received message: {msg.value()}")
    
    event_count += 1

consumer.close()
