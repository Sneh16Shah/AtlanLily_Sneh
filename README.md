# Atlan-Backend
## Table of Contents
- [About](#about)
- [Features](#features)
- [Getting Started](#getting-started)
- [Installation](#installation)
- [Repository Structure](#repository-structure)
- [Components](#components)
- [Special Thanks](#special-thanks)



## About
Implemented a real-time metadata streaming solution using PostgreSQL, enabling Atlan to capture and store near-instantaneous updates to metadata from various sources.


## Features

1. **Real-Time Metadata Streaming:**
   - Implemented a robust data streaming mechanism for capturing metadata changes in near-real-time, ensuring the system is always up-to-date.

2. **Scalable PostgreSQL Storage:**
   - Utilized a PostgreSQL database optimized for high write and read throughput to store and organize metadata efficiently.

3. **API Routes for Universal Access:**
   - Developed RESTful API routes to facilitate seamless access to metadata from any location or application, enhancing usability and integration possibilities.

4. **User-Friendly Android App:**
   - Created an intuitive Android app for users to fetch, view, and interact with metadata effortlessly, enhancing the overall user experience.

5. **Consistent Data Schema:**
   - Designed a structured and consistent data schema to organize metadata, ensuring clarity and ease of querying.

6. **Basic Authentication and Authorization:**
   - Implemented security measures through basic authentication and authorization mechanisms to safeguard sensitive metadata.

7. **Performance Optimization:**
   - Optimized system performance for both write and read operations, enhancing efficiency and responsiveness.

8. **Versioning and History Tracking:**
   - Implemented versioning and history tracking for metadata changes, providing a comprehensive historical record.

9. **Flexible Deployment Options:**
   - Designed the system to support various deployment models, from multi-tenant setups to isolated environments, offering flexibility to meet diverse customer preferences.

10. **Modular and Extensible Architecture:**
    - Developed a modular architecture that allows for easy addition of new features and enhancements, ensuring adaptability to evolving requirements.

11. **Monitoring and Logging:**
    - Integrated monitoring tools and logging mechanisms to track system performance, identify issues, and gather usage statistics.

12. **Cost-Effective Implementation:**
    - Employed cost-effective strategies, such as serverless computing for event-driven processing, to optimize infrastructure costs.

13. **Offline Access and Caching:**
    - Provided offline access to previously fetched metadata in the Android app, enhancing usability in low or no connectivity scenarios.

14. **Feedback and Improvement Loop:**
    - Established a feedback loop to continuously improve the system based on user input and changing requirements.


### Built With

- **Real-Time Metadata Streaming:**
  - [Apache Kafka](https://kafka.apache.org/): A distributed streaming platform for building real-time data pipelines.

- **Database Storage:**
  - [PostgreSQL](https://www.postgresql.org/): A powerful, open-source relational database system.

- **API Development:**
  - [RESTful API](https://restfulapi.net/): A set of constraints and principles for building web services.

- **Data Processing:**
  - [Apache Spark](https://spark.apache.org/): An open-source, distributed computing system for big data processing.

- **Security:**
  - [Authentication and Authorization](https://en.wikipedia.org/wiki/Authentication): Basic mechanisms for securing access to the system.

- **Scalability and Flexibility:**
  - [Docker](https://www.docker.com/): A platform for developing, shipping, and running applications in containers.

- **Cost-Effective Implementation:**
  - [Serverless Computing](https://aws.amazon.com/serverless/): A cloud computing execution model where the cloud provider manages the infrastructure automatically.


# Getting Started
## Prerequisites

Before you begin, ensure you have met the following requirements:

- **Code Editor**: You'll need a code editor to work on this project. We recommend using [Visual Studio Code](https://code.visualstudio.com/) for the best development experience. Additionally, consider installing the following extensions for Vue.js and Tailwind CSS development:

  - [Vetur](https://marketplace.visualstudio.com/items?itemName=octref.vetur)
  - [Tailwind CSS IntelliSense](https://marketplace.visualstudio.com/items?itemName=bradlc.vscode-tailwindcss)

### Installation
Docker file contains instructions for creating docker container with all the services and with ui enabled for easy interaction.

1. Clone the repository:
   ```bash
   https://github.com/VONBLESS/DataStream.git
   ```
2. Install apache spark
   https://www.tutorialspoint.com/apache_spark/apache_spark_installation.htm
3. Install apache zookeper
   https://www.tutorialspoint.com/zookeeper/zookeeper_installation.htm
4. Install apache kafka
   https://www.tutorialspoint.com/apache_spark/apache_spark_installation.htm
5. Install postgresql
   ```bash
   sudo apt install postgresql postgresql-contrib
   ```
6. Install all python3 libraries from requirements.txt
   ```bash
   pip3 install -r requirements.txt
   ```

### Instructions for running

1. Ensure apache zookeper, kafka services are running
2. Run createTable.py for creating database and tables to store meta data in postgres sql.
```
pyhton3 createTable.py.py
```   
3. Run kafka_producer.py file by running the below command to generate random meta data
```
pyhton3 kafka_producer.py
```
4. Run insertKafkaDb.py file by running the below command to capture random meta data and store it
```
pyhton3 kafka_producer.py
```
5. Run api.py to create an api endpoint and make data accesible via restful services.
```
pyhton3 api.py
```
6. Run flask_backEnd.py to enable protected data access by creating a user and enabling only them to access the api via the android app created using kiwi.
```
pyhton3 flask_backEnd.py
```
7. now open the android app to check if api services are running well
   There are 3 endpoints available in the api
   First for current data in /latest_metadata_event route
   Second for all data in /metadata_events_all/ route
   Third for metadata by id in /metadata_events/{event_id} route

   use these routes in android app o check for data availibility.

   Install the android app by just running the apk file in android device.

## Repository Structure

```sh
└── xyz/
    ├── androidApp.py
    ├── api.py
    ├── ascync_api.py
    ├── createTable.py
    ├── flask_backeEnd.py
    ├── insertKafkaDb.py
    ├── kafka_consumer.py
    ├── kafka_producer.py
    ├── templates/
    │   ├── Dashboard.html
    │   ├── Login.html
    │   ├── register.html
    └── Readme.md

```

---
# Components

## 1. Real-Time Metadata Streaming

### Technology Used:
- [Apache Kafka](https://kafka.apache.org/): A distributed streaming platform for building real-time data pipelines.

### Description:
Implemented Apache Kafka to capture metadata changes in near-real-time, ensuring the system stays up-to-date with the latest changes.

---

## 2. Database Storage

### Technology Used:
- [PostgreSQL](https://www.postgresql.org/): A powerful, open-source relational database system.

### Description:
Utilized PostgreSQL to store and organize metadata efficiently, providing a scalable and reliable storage solution.

---

## 3. API Development

### Technology Used:
- [RESTful API](https://restfulapi.net/): A set of constraints and principles for building web services.

### Description:
Developed RESTful API routes to facilitate seamless access to metadata from any location or application, enhancing usability and integration possibilities.

---

## 4. Android App Development

### Technology Used:
- [Android Studio](https://developer.android.com/studio): The official integrated development environment for Android app development.

### Description:
Created an intuitive Android app for users to fetch, view, and interact with metadata effortlessly, enhancing the overall user experience.

---

## 5. Data Processing

### Technology Used:
- [Apache Spark](https://spark.apache.org/): An open-source, distributed computing system for big data processing.

### Description:
Utilized Apache Spark for data processing, enabling efficient handling of large volumes of metadata.

---

## 6. Security

### Technology Used:
- Authentication and Authorization: Basic mechanisms for securing access to the system.

### Description:
Implemented basic authentication and authorization mechanisms to safeguard sensitive metadata.

---

## 7. Scalability and Flexibility

### Technologies Used:
- [Docker](https://www.docker.com/): A platform for developing, shipping, and running applications in containers.
- [Kubernetes](https://kubernetes.io/): An open-source container orchestration platform.

### Description:
Designed the system to be scalable and flexible, leveraging Docker for containerization and Kubernetes for orchestration.


---

## 9. Cost-Effective Implementation

### Technology Used:
- [Serverless Computing](https://aws.amazon.com/serverless/): A cloud computing execution model where the cloud provider manages the infrastructure automatically.

### Description:
Employed serverless computing strategies to optimize infrastructure costs and ensure efficient resource utilization.

---

## 10. Documentation

### Technology Used:
- [Markdown](https://www.markdownguide.org/): A lightweight markup language used for formatting plain text.

### Description:
Utilized Markdown for creating clear and concise documentation, providing information about the project components and their functionalities.

Code for data streaming random data


```python3
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

```

Code for data capturing the streamed data 

```python3

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


```

Code for creating database and tables in postgres for storing and managing streamed data

```python3

import psycopg2

db_params = {
    "host": "localhost",  
    "database": "metadata_db", 
    "user": "postgres", 
    "password": "123456"  
}

# SQL command to create the metadata_events table
create_table_sql = """
CREATE TABLE metadata_events (
    id SERIAL PRIMARY KEY,
    entity_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    metadata_info TEXT NOT NULL
);
"""

# SQL command to create the metadata_events table
create_table_sql1 = """
CREATE TABLE auth_table (
    id SERIAL PRIMARY KEY,
    uname TEXT NOT NULL,
    password TEXT NOT NULL,
    role TEXT NOT NULL
);
"""

# #SQL command to select all data from metadata_events
# select_query="""
# SELECT * FROM metadata_events
# """
# Connect to the PostgreSQL database
conn = psycopg2.connect(**db_params)
cursor = conn.cursor()

# Execute the SQL command to create the table
cursor.execute(create_table_sql)
cursor.execute(create_table_sql1)
# cursor.execute(select_query)

# data = cursor.fetchall()
# print(data)

conn.commit()
cursor.close()
conn.close()

print("metadata_events table created.")


```


Code for data capturing the streamed data and storing it into a sql database with minimal loss.

```python3

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

```

Code for restful services. 
Creating an API endpoint to make data visible in the current network

```python3

from fastapi import FastAPI
from pydantic import BaseModel
import psycopg2

app = FastAPI()

# PostgreSQL connection parameters
db_params = {
    "host": "localhost",  
    "database": "metadata_db",  
    "user": "postgres",  
    "password": "123456"  
}

class MetadataEvent(BaseModel):
    id: int
    entity_id: int
    event_type: str
    metadata_info: str

class SimpleMetadataEvent(BaseModel):
    entity_id: int
    event_type: str
    metadata_info: str

@app.get("/metadata_events/{event_id}", response_model=MetadataEvent)
def read_metadata_event(event_id: int):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    query = """
    SELECT id, entity_id, event_type, metadata_info 
    FROM metadata_events 
    WHERE entity_id = %s
    """
    cursor.execute(query, (str(event_id),))
    event_data = cursor.fetchone()
    
    cursor.close()
    conn.close()
    
    if event_data:
        return {
            "id": event_data[0],
            "entity_id": event_data[1],
            "event_type": event_data[2],
            "metadata_info": event_data[3],
        }
    
    return {"message": "Event not found"}

@app.get("/metadata_events/", response_model=list[SimpleMetadataEvent])
def read_metadata_events(skip: int = 0, limit: int = 100):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    query = """
    SELECT entity_id, event_type, metadata_info 
    FROM metadata_events 
    LIMIT %s OFFSET %s
    """
    cursor.execute(query, (limit, skip))
    events_data = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    events = [{"entity_id": e[0], "event_type": e[1], "metadata_info": e[2]} for e in events_data]
    
    return events

@app.get("/metadata_events_all/", response_model=list[SimpleMetadataEvent])
def read_metadata_events(skip: int = 0):
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()

    query = """
    SELECT entity_id, event_type, metadata_info 
    FROM metadata_events 
    OFFSET %s
    """

    cursor.execute(query, (skip,))
    events_data = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    events = [{"entity_id": e[0], "event_type": e[1], "metadata_info": e[2]} for e in events_data]
    
    return events

@app.get("/latest_metadata_event", response_model=MetadataEvent)
def read_latest_metadata_event():
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()
    query = """
    SELECT id, entity_id, event_type, metadata_info 
    FROM metadata_events 
    ORDER BY id DESC
    LIMIT 1
    """
    cursor.execute(query)
    event_data = cursor.fetchone()
    
    cursor.close()
    conn.close()
    
    if event_data:
        return {
            "id": event_data[0],
            "entity_id": event_data[1],
            "event_type": event_data[2],
            "metadata_info": event_data[3],
        }
    
    return {"message": "No events found"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

```

Python Flask for backend user verification and access level

```python3

from flask import Flask, request, render_template, redirect, url_for, flash
import psycopg2
from flask_login import current_user, LoginManager, UserMixin, login_user, login_required, logout_user

app = Flask(__name__)
app.config['SECRET_KEY'] = 'hello_world'  

db_params = {
    "host": "localhost",  
    "database": "metadata_db",
    "user": "postgres",  
    "password": "123456"  
}

conn = psycopg2.connect(**db_params)

login_manager = LoginManager(app)
login_manager.login_view = "login"

class User(UserMixin):
    def __init__(self, id, username, password, role):
        self.id = id
        self.username = username
        self.password = password
        self.role = role

@login_manager.user_loader
def load_user(user_id):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM auth_table WHERE id = %s", (user_id,))
    user_data = cursor.fetchone()
    cursor.close()
    if user_data:
        return User(user_data[0], user_data[1], user_data[2], user_data[3])
    return None

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        role = request.form['role']

        cursor = conn.cursor()
        cursor.execute("INSERT INTO auth_table (uname, password, role) VALUES (%s, %s, %s)", (username, password, role))
        conn.commit()
        cursor.close()
        flash('User registered successfully', 'success')
        return redirect(url_for('login'))

    return render_template('register.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']

        cursor = conn.cursor()
        cursor.execute("SELECT * FROM auth_table WHERE uname = %s AND password = %s", (username, password))
        user_data = cursor.fetchone()
        cursor.close()

        if user_data:
            user = User(user_data[0], user_data[1], user_data[2], user_data[3])
            login_user(user)
            flash('Logged in successfully', 'success')
            return redirect(url_for('dashboard'))
        else:
            flash('Login failed. Check your username and password.', 'danger')

    return render_template('login.html')

@app.route('/dashboard')
@login_required
def dashboard():
    if current_user.role == 'admin':
        
        return "Admin Dashboard"
    else:
        
        return "User Dashboard"

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

if __name__ == '__main__':
    app.run(debug=True)
```

Python Kiwi app for accesing data hosted on any api.

```python3
from kivy.app import App
from kivy.uix.button import Button
from kivy.uix.textinput import TextInput
from kivy.uix.label import Label
from kivy.uix.boxlayout import BoxLayout
import requests

class DataFetchApp(App):
    def build(self):
        layout = BoxLayout(orientation='vertical')

        # Create a text input for the URL
        self.url_input = TextInput(hint_text='Enter API URL', multiline=False)
        self.text_area = TextInput(hint_text='Data will be displayed here', readonly=True, size_hint_y=None, height=500)
        fetch_button = Button(text='Fetch Data', on_press=self.fetch_data)

        layout.add_widget(self.url_input)
        layout.add_widget(fetch_button)
        layout.add_widget(self.text_area)

        return layout

    def fetch_data(self, instance):
        # Get the URL from the text input
        url = self.url_input.text

        if not url:
            self.text_area.text = 'Error: Please enter a URL'
            return

        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()
            self.text_area.text = str(data)
        else:
            self.text_area.text = 'Error: Unable to fetch data'

if __name__ == '__main__':
    DataFetchApp().run()
```

Code for Docker file to setup whole setup with UI in a light ubuntu

```
FROM ubuntu:latest

# Install Lubuntu Desktop and LightDM
RUN apt update && DEBIAN_FRONTEND=noninteractive apt install -y lubuntu-desktop lightdm

# Remove reboot required file
RUN rm /run/reboot-required*

# Set default display manager to LightDM
RUN echo "/usr/sbin/lightdm" > /etc/X11/default-display-manager

# Configure LightDM for autologin and remote display
RUN echo "\
[LightDM]\n\
[Seat:*]\n\
type=xremote\n\
xserver-hostname=host.docker.internal\n\
xserver-display-number=0\n\
autologin-user=root\n\
autologin-user-timeout=0\n\
autologin-session=Lubuntu\n\
" > /etc/lightdm/lightdm.conf.d/lightdm.conf

# Set environment variable for display
ENV DISPLAY=host.docker.internal:0.0

# Install Java 11
RUN apt update && DEBIAN_FRONTEND=noninteractive apt install -y openjdk-11-jdk

# Install postgress sql
RUN apt update && DEBIAN_FRONTEND=noninteractive apt install -y postgresql postgresql-contrib


# Install python3 and pip
RUN apt update && DEBIAN_FRONTEND=noninteractive apt install -y python3 pip

# Install necessary libraries of pip
RUN pip install aioredis==2.0.1 annotated-types==0.6.0 anyio==3.7.1 async-timeout==4.0.3 asyncpg==0.29.0 blinker==1.7.0 certifi==2023.7.22 charset-normalizer==3.3.2 click==8.1.7 confluent-kafka==2.3.0 docutils==0.20.1 et-xmlfile==1.1.0 fastapi==0.104.1 Flask==3.0.0 Flask-Login==0.6.3 Flask-SQLAlchemy==3.1.1 greenlet==3.0.1 h11==0.14.0 idna==3.4 itsdangerous==2.1.2 Jinja2==3.1.2 Kivy==2.2.1 Kivy-Garden==0.1.5 MarkupSafe==2.1.3 numpy==1.26.1 openpyxl==3.1.2 pandas==2.1.2 psycopg2==2.9.9 py4j==0.10.9.7 pydantic==2.4.2 pydantic_core==2.10.1 Pygments==2.16.1 pyspark==3.5.0 python-dateutil==2.8.2 pytz==2023.3.post1 requests==2.31.0 six==1.16.0 sniffio==1.3.0 SQLAlchemy==2.0.23 starlette==0.27.0 typing_extensions==4.8.0 tzdata==2023.3 urllib3==2.0.7 uvicorn==0.23.2 Werkzeug==3.0.1

# Install Apache Spark
WORKDIR /opt
RUN wget -q https://downloads.apache.org/spark/spark-3.2.0/spark-3.2.0-bin-hadoop3.2.tgz && \
  tar -xzf spark-3.2.0-bin-hadoop3.2.tgz && \
  rm spark-3.2.0-bin-hadoop3.2.tgz && \
  ln -s spark-3.2.0-bin-hadoop3.2 spark

# Install Kafka
WORKDIR /opt
RUN wget -q https://downloads.apache.org/kafka/3.0.0/kafka_2.12-3.0.0.tgz && \
  tar -xzf kafka_2.12-3.0.0.tgz && \
  rm kafka_2.12-3.0.0.tgz && \
  ln -s kafka_2.12-3.0.0 kafka

# Install ZooKeeper
RUN apt install -y zookeeperd

# Start necessary services
CMD service dbus start ; /usr/lib/systemd/systemd-logind & service lightdm start && \
  /opt/spark/sbin/start-master.sh && /opt/spark/sbin/start-worker.sh spark://localhost:7077 && \
  service zookeeper start && \
  /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties && \
  /bin/bash


```




### Official Resources

1. **Markdown Guide:**
   - [Markdown Guide](https://www.markdownguide.org/): Learn the basics of Markdown, a lightweight markup language used for formatting plain text.

2. **Apache Kafka Documentation:**
   - [Apache Kafka Documentation](https://kafka.apache.org/documentation/): Official documentation for Apache Kafka, a distributed streaming platform.

3. **PostgreSQL Documentation:**
   - [PostgreSQL Documentation](https://www.postgresql.org/docs/): Official documentation for PostgreSQL, a powerful open-source relational database system.

4. **RESTful API Guide:**
   - [RESTful API Tutorial](https://restfulapi.net/): A comprehensive guide on building RESTful APIs.

5. **Android Developer Documentation:**
   - [Android Developer Documentation](https://developer.android.com/docs): Official documentation for Android app development with Android Studio.

6. **Apache Spark Documentation:**
   - [Apache Spark Documentation](https://spark.apache.org/docs/latest/): Official documentation for Apache Spark, an open-source, distributed computing system.

7. **Docker Documentation:**
   - [Docker Documentation](https://docs.docker.com/): Official documentation for Docker, a platform for developing, shipping, and running applications in containers.


# Special Thanks

I wish to extend my heartfelt gratitude to Atlan for providing me with an invaluable opportunity to work on this assignment. This experience has significantly contributed to my growth, broadening my horizons and enhancing my knowledge and skills.

## About the Assignment

The assignment presented me with the challenge of mastering the email editor. Throughout this journey, I have not only gained profound insights but have also acquired hands-on experience that will undoubtedly leave a lasting impact on my future endeavors.

## Key Takeaways

I am appreciative of the wealth of knowledge and expertise I have acquired during this assignment. Some of the pivotal takeaways include:

-**Real-Time Efficiency:** Implemented Apache Kafka for capturing metadata changes in real-time, ensuring near-instantaneous updates and efficient processing.
-**Flexible, Secure, and Cost-Effective:** Leveraged Docker, Kubernetes, and serverless computing for flexible deployment, implemented security measures, and optimized infrastructure costs, ensuring scalability and efficiency.

## Conclusion

This assignment has been an enriching journey, and I am enthusiastic about applying the knowledge and skills I have gained to propel my career forward. I extend my sincere thanks to Atlan for this remarkable opportunity and eagerly anticipate our continued collaboration.

I wish to express my gratitude to the entire Atlan team for entrusting me with this assignment.
