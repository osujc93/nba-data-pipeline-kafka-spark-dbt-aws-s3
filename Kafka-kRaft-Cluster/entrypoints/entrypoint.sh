#!/bin/bash

/opt/kafka/bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" -c /opt/kafka/config/server.properties --ignore-formatted

echo "===== Kafka Configuration ====="
cat /opt/kafka/config/server.properties

echo "KAFKA_JMX_OPTS: $KAFKA_JMX_OPTS"
echo "KAFKA_OPTS: $KAFKA_OPTS"
echo "JMX_PORT: $JMX_PORT"
echo "KAFKA_JMX_PORT: $KAFKA_JMX_PORT"

echo "Starting Kafka in KRaft mode..."

exec /opt/kafka/bin/kafka-server-start.sh /opt/kafka/config/server.properties

