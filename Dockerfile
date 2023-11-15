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
RUN apt update && DEBIAN_FRONTEND=noninteractive apt install -y postgresql postgresql-contrib libpq-dev git


# Install python3 and pip
RUN apt update && DEBIAN_FRONTEND=noninteractive apt install -y python3 pip

# Install necessary libraries of pip
RUN pip install aioredis==2.0.1 annotated-types==0.6.0 anyio==3.7.1 async-timeout==4.0.3 asyncpg==0.29.0 blinker==1.7.0 certifi==2023.7.22 charset-normalizer==3.3.2 click==8.1.7 confluent-kafka==2.3.0 docutils==0.20.1 et-xmlfile==1.1.0 fastapi==0.104.1 Flask==3.0.0 Flask-Login==0.6.3 Flask-SQLAlchemy==3.1.1 greenlet==3.0.1 h11==0.14.0 idna==3.4 itsdangerous==2.1.2 Jinja2==3.1.2 Kivy==2.2.1 Kivy-Garden==0.1.5 MarkupSafe==2.1.3 numpy==1.26.1 openpyxl==3.1.2 pandas==2.1.2 psycopg2==2.9.9 py4j==0.10.9.7 pydantic==2.4.2 pydantic_core==2.10.1 Pygments==2.16.1 pyspark==3.5.0 python-dateutil==2.8.2 pytz==2023.3.post1 requests==2.31.0 six==1.16.0 sniffio==1.3.0 SQLAlchemy==2.0.23 starlette==0.27.0 typing_extensions==4.8.0 tzdata==2023.3 urllib3==2.0.7 uvicorn==0.23.2 Werkzeug==3.0.1

# Install Apache Spark
WORKDIR /opt
RUN wget -q https://dlcdn.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
  tar -xzf spark-3.5.0-bin-hadoop3.tgz && \
  rm spark-3.5.0-bin-hadoop3.tgz && \
  ln -s spark-3.5.0-bin-hadoop3 spark

# Install Kafka
WORKDIR /opt
RUN wget -q https://downloads.apache.org/kafka/3.6.0/kafka-3.6.0-src.tgz && \
  tar -xzf kafka-3.6.0-src.tgz && \
  rm kafka-3.6.0-src.tgz && \
  ln -s kafka-3.6.0-src kafka

# Install ZooKeeper
RUN apt install -y zookeeperd

# Start necessary services
CMD service dbus start ; /usr/lib/systemd/systemd-logind & service lightdm start && \
  /opt/spark/sbin/start-master.sh && /opt/spark/sbin/start-worker.sh spark://localhost:7077 && \
  service zookeeper start && \
  /opt/kafka/bin/kafka-server-start.sh -daemon /opt/kafka/config/server.properties && \
  /bin/bash

