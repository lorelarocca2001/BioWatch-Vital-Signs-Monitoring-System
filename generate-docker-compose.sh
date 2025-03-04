#!/bin/bash

if [ -f .env ]; then
  export $(cat .env | grep -v '^#' | xargs)
else
  echo ".env file not found!"
  exit 1
fi

NUM_PATIENTS=$NUM_PATIENTS
PATIENT_IDS=($(echo $PATIENT_IDS | tr ',' ' '))

cat > docker-compose.yml <<EOL
services:
  fluentd:
    build: fluentd
    hostname: fluentd
    container_name: fluentd
    command: "-c /fluentd/etc/fluentd.conf"
    volumes:
      - ./fluentd/conf:/fluentd/etc
    ports:
      - "9880:9880"
    depends_on:
      topics:
        condition: service_completed_successfully
EOL

for i in $(seq 1 $NUM_PATIENTS); do
  PATIENT_ID=${PATIENT_IDS[$i-1]}
  
  cat >> docker-compose.yml <<EOL

  vitaldbscraper_patient_${i}:
    build: python
    hostname: vitaldbscraper
    container_name: vitaldbscraper${i}
    environment:
      - PYTHONBUFFERED=1
      - PYTHON_APP=vitaldbscraper.py
      - PATIENT_ID=${PATIENT_ID}
    volumes:
      - ./python/bin/:/usr/src/app/bin
    depends_on:
      - fluentd
EOL
done

cat >> docker-compose.yml <<EOL

  broker:
    image: apache/kafka:latest
    hostname: broker
    container_name: broker
    ports:
      - '9092:9092'
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
      KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT_HOST://broker:9092,PLAINTEXT://broker:19092'
      KAFKA_PROCESS_ROLES: 'broker,controller'
      KAFKA_CONTROLLER_QUORUM_VOTERS: '1@broker:29093'
      KAFKA_LISTENERS: 'CONTROLLER://:29093,PLAINTEXT_HOST://:9092,PLAINTEXT://:19092'
      KAFKA_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
      KAFKA_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'
      CLUSTER_ID: '4L6g3nShT-eMCtK--X86sw'
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_LOG_DIRS: '/tmp/kraft-combined-logs'

  topics:
    image: apache/kafka:latest
    hostname: topics
    container_name: topics
    command: > 
      bash -c "
      /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:9092 --list | grep -w vitaldb ||
      /opt/kafka/bin/kafka-topics.sh --create --topic vitaldb --bootstrap-server broker:9092
      "
    depends_on:
      - broker
EOL

cat >> docker-compose.yml <<EOL

  spark:
    image: tap:spark
    hostname: spark
    container_name: spark
    mem_limit: 1GB
    env_file: ".env"
    volumes:
      - ./spark/code/:/opt/tap/
      - ./spark/dataset:/tmp/dataset
      - sparklibs:/tmp/.ivy2
    command: > 
      /opt/spark/bin/spark-submit --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp" --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.elasticsearch:elasticsearch-spark-30_2.12:8.16.2  /opt/tap/vitaldbtap.py
    depends_on:
      topics:
        condition: service_completed_successfully
EOL

cat >> docker-compose.yml <<EOL

volumes:
  sparklibs:
EOL

echo "Il file docker-compose.yml è stato generato con successo."

