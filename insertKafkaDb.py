import psycopg2
from confluent_kafka import Consumer, KafkaError

kafka_config = {
    'bootstrap.servers': 'localhost:9092',  
    'group.id': 'metadata-consumer-group',
    'auto.offset.reset': 'earliest'
}

db_params = {
    "host": "localhost",  
    "database": "metadata_db",  
    "user": "postgres",  
    "password": "123456"  
}

consumer = Consumer(kafka_config)

topic = 'metadata-changes'

consumer.subscribe([topic])

events_to_process = 500  

event_count = 0

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

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

    metadata_event = eval(msg.value()) 

    insert_query = """
    INSERT INTO metadata_events (entity_id, event_type, metadata_info)
    VALUES (%(entity_id)s, %(event_type)s, %(metadata_info)s)
    """

    cursor.execute(insert_query, metadata_event)
    conn.commit()
    print("inserted entry number {}".format(event_count))

    event_count += 1

    #time.sleep(1)  

conn.commit()
cursor.close()
conn.close()

consumer.close()
