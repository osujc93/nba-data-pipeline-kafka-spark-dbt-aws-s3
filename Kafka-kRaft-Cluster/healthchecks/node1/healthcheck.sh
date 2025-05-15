#!/bin/bash

LOGFILE="/var/log/kafka/kafka_healthcheck.log"
CONTROLLER_PORT="${CONTROLLER_PORT:-9091}"

sudo touch "$LOGFILE"
sudo chmod 644 "$LOGFILE"

exec > >(sudo tee -a "$LOGFILE") 2>&1

log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log "=== Controller Health Check Started (port=$CONTROLLER_PORT) ==="

CMD="/opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-controller kafka1:$CONTROLLER_PORT describe --status"

log "Running: $CMD"
OUTPUT=$($CMD 2>&1)
RET=$?

if [ $RET -ne 0 ]; then
  log "Failed to run kafka-metadata-quorum.sh: $OUTPUT"
  exit 1
fi

echo "$OUTPUT" | grep -Eq 'Leader|Follower'
if [ $? -ne 0 ]; then
  log "Controller is not in Leader/Follower state. Output was:"
  log "$OUTPUT"
  exit 1
fi

log "Controller is healthy (found Leader or Follower in output)."
log "=== Controller Health Check Finished ==="

exit 0
