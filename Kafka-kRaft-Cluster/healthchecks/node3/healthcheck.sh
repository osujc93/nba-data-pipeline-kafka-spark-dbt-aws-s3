#!/bin/bash

LOGFILE="/var/log/kafka/kafka_healthcheck.log"

sudo touch "$LOGFILE"
sudo chmod 644 "$LOGFILE"

exec > >(sudo tee -a "$LOGFILE") 2>&1

log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - $1"
}

IPS=172.16.10.3

PORTS=9093

CLIENT_PROPS="/opt/kafka/config/client.properties"
PRODUCER_PROPS="/opt/kafka/config/producer.properties"
CONSUMER_PROPS="/opt/kafka/config/consumer.properties"

TOPICS=("your_topic_name")
CONSUMER_GROUPS=("your_consumer_group")


check_port() {
  local IP=$1
  local PORT=$2
  log "Checking if port $PORT is open on $IP..."
  if bash -c "echo > /dev/tcp/$IP/$PORT" 2>/dev/null; then
    log "Port $PORT is open on $IP"
  else
    log "Port $PORT is closed on $IP"
    exit 1
  fi
}

check_kafka() {
  local IP=$1
  local PORT=$2
  log "Performing Kafka checks on $IP:$PORT..."

  TEST_TOPIC="healthcheck-topic-$(date +%s)"
  log "Creating test topic '$TEST_TOPIC'..."

  if sudo /opt/kafka/bin/kafka-topics.sh \
    --create \
    --topic "$TEST_TOPIC" \
    --bootstrap-server "$IP:$PORT" \
    --command-config "$PRODUCER_PROPS" \
    --replication-factor 1 \
    --partitions 1; then
    log "Test topic '$TEST_TOPIC' created successfully."
  else
    log "Failed to create test topic '$TEST_TOPIC'."
    exit 1
  fi

  log "Producing test message to topic '$TEST_TOPIC'..."
  if echo "healthcheck-message" | sudo /opt/kafka/bin/kafka-console-producer.sh \
    --broker-list "$IP:$PORT" \
    --topic "$TEST_TOPIC" \
    --producer.config "$PRODUCER_PROPS"; then
    log "Test message produced successfully."
  else
    log "Failed to produce test message."
    exit 1
  fi

  log "Consuming the test message from topic '$TEST_TOPIC'..."
  if sudo /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server "$IP:$PORT" \
    --topic "$TEST_TOPIC" \
    --from-beginning \
    --max-messages 1 \
    --consumer.config "$CONSUMER_PROPS"; then
    log "Test message consumed successfully."
  else
    log "Failed to consume test message."
    exit 1
  fi

  log "Deleting test topic '$TEST_TOPIC'..."
  if sudo /opt/kafka/bin/kafka-topics.sh \
    --delete \
    --topic "$TEST_TOPIC" \
    --bootstrap-server "$IP:$PORT" \
    --command-config "$PRODUCER_PROPS"; then
    log "Test topic '$TEST_TOPIC' deleted successfully."
  else
    log "Failed to delete test topic '$TEST_TOPIC'."
    exit 1
  fi

  log "Kafka checks passed on $IP:$PORT"
}


log "=== Kafka Health Check Started ==="

for index in "${!IPS[@]}"; do
  IP=${IPS[$index]}
  PORT=${PORTS[$index]}
  log "---------------------------------------------"
  log "Performing health checks for $IP:$PORT"
  log "---------------------------------------------"

  check_port "$IP" "$PORT"
  check_kafka "$IP" "$PORT"
done

log "All health checks completed successfully."
log "=== Kafka Health Check Finished ==="

exit 0
